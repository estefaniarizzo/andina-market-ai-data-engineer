"""Generate coherent synthetic data for Andina Market and insert it directly into Azure SQL.

Volume (configurable below): ~2,000 customers, 40 products, ~12,000 orders,
~25,000 order lines, ~13,000 payments and 1,500 support tickets over 18 months.

Realism and edge cases that the pipeline must handle:
- Seasonality (Black Friday / December peak, weekends) and business growth.
- Heavy-tailed customer activity (a few frequent buyers, many one-off buyers).
- Order status consistent with the order age (recent orders are still `created`/`paid`).
- Payments updated after creation: rejected-then-approved retries, refunds on
  cancelled orders, pending payments on new orders and a few duplicate charges.
- Historic prices: some products got more expensive, old order lines keep the old price.
- Inactive products that only appear in historic orders.
- Soft-deleted customers (`is_deleted = 1`) that still own historic orders.
- Dirty-but-valid values the quality layer must flag or normalize: emails with
  uppercase/whitespace, malformed emails, missing phones, empty ticket bodies and
  orders whose header total does not match the sum of their lines.
- Support tickets with text coherent with the customer's real order, product and city.

IDs are inserted explicitly (IDENTITY_INSERT) so the dataset is reproducible
with the fixed SEED and references between tables are known in advance.
"""

import bisect
import random
import unicodedata
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from faker import Faker

from db import get_connection
from synthetic_catalog import (
    CHANNELS,
    CHANNELS_BY_SEGMENT,
    COUNTRIES,
    INACTIVE_SKUS,
    PAYMENT_METHODS_BY_COUNTRY,
    PRICE_INCREASE_SKUS,
    PRODUCT_CATALOG,
    SEGMENTS,
    STORE_PAYMENT_METHODS,
    TICKET_TEMPLATES,
)

SEED = 2026
NUM_CUSTOMERS = 2000
NUM_ORDERS = 12000
NUM_TICKETS = 1500
HISTORY_DAYS = 540

NUM_TOTAL_MISMATCH_ORDERS = 15
NUM_DUPLICATE_CHARGES = 8
NUM_INVALID_EMAILS = 3
NUM_UNNORMALIZED_EMAILS = 6
NUM_EMPTY_TICKET_BODIES = 2
SOFT_DELETED_CUSTOMER_RATE = 0.01

random.seed(SEED)
fake = Faker("es_CO")
Faker.seed(SEED)

NOW = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
START = NOW - timedelta(days=HISTORY_DAYS)
EMAIL_DOMAINS = ["example.com", "example.net", "example.org", "correo.example"]


def money(value) -> Decimal:
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def random_between(start: datetime, end: datetime) -> datetime:
    if end <= start:
        return start
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def slug(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return "".join(ch for ch in normalized.lower() if ch.isalnum())


def ms(value: datetime) -> datetime:
    """Cap at NOW (no future timestamps) and add milliseconds, like SYSUTCDATETIME() in DATETIME2(3)."""
    return min(value, NOW).replace(microsecond=random.randint(0, 999) * 1000)


# ---------------------------------------------------------------------------
# Generators (pure Python, no database access)
# ---------------------------------------------------------------------------

def build_customers():
    countries = list(COUNTRIES)
    weights = [COUNTRIES[c]["weight"] for c in countries]
    customers = []

    for customer_id in range(1, NUM_CUSTOMERS + 1):
        country = random.choices(countries, weights=weights, k=1)[0]
        # More recent sign-ups than old ones (growing business).
        days_ago = int(HISTORY_DAYS * 1.6 * random.random() ** 1.4) + 1
        created_at = ms(NOW - timedelta(days=days_ago, minutes=random.randint(0, 1439)))
        first_name, last_name = fake.first_name(), fake.last_name()
        email = f"{slug(first_name)}.{slug(last_name)}{customer_id}@{random.choice(EMAIL_DOMAINS)}"
        segment = random.choices(*SEGMENTS, k=1)[0]

        customers.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": None if random.random() < 0.08 else fake.phone_number()[:30],
            "city": random.choice(COUNTRIES[country]["cities"]),
            "country": country,
            "segment": segment,
            "signup_date": created_at.date(),
            "created_at": created_at,
            "updated_at": created_at,
            "is_deleted": False,
            # Heavy-tailed purchase propensity (not stored in the source).
            "activity": random.lognormvariate(0, 1.1),
        })

    dirty = random.sample(customers, NUM_INVALID_EMAILS + NUM_UNNORMALIZED_EMAILS)
    for customer in dirty[:NUM_INVALID_EMAILS]:
        customer["email"] = customer["email"].split("@")[0] + "@"
    for customer in dirty[NUM_INVALID_EMAILS:]:
        customer["email"] = f"  {customer['email'].upper()} "

    for customer in random.sample(customers, int(NUM_CUSTOMERS * SOFT_DELETED_CUSTOMER_RATE)):
        customer["is_deleted"] = True
        customer["updated_at"] = ms(random_between(customer["created_at"] + timedelta(days=30), NOW))
        customer["activity"] *= 0.3
    return customers


def build_products():
    products = []
    for product_id, (sku, name, category, price, warranty, description) in enumerate(PRODUCT_CATALOG, start=1):
        created_at = ms(START - timedelta(days=random.randint(30, 200)))
        product = {
            "product_id": product_id,
            "sku": sku,
            "product_name": name,
            "category": category,
            "unit_price": price,
            "old_price": price,
            "price_change_at": None,
            "product_description": (
                f"{description} Garantía de {warranty} meses." if warranty else
                f"{description} Producto sin garantía de fabricante; aplica derecho de retracto."
            ),
            "product_status": "active",
            "inactive_since": None,
            "created_at": created_at,
            "updated_at": created_at,
            "is_deleted": False,
            "popularity": random.lognormvariate(0, 0.8),
        }
        if sku in PRICE_INCREASE_SKUS:
            product["old_price"] = money((price / Decimal("1.10")).quantize(Decimal("100")))
            product["price_change_at"] = ms(NOW - timedelta(days=random.randint(60, 200)))
            product["updated_at"] = product["price_change_at"]
        if sku in INACTIVE_SKUS:
            product["product_status"] = "inactive"
            product["inactive_since"] = ms(NOW - timedelta(days=random.randint(90, 250)))
            product["updated_at"] = product["inactive_since"]
        products.append(product)
    return products


def day_weights():
    days, weights = [], []
    for offset in range(HISTORY_DAYS):
        day = START + timedelta(days=offset)
        weight = 0.7 + 0.6 * offset / HISTORY_DAYS  # growth
        if (day.month == 11 and day.day >= 20) or (day.month == 12 and day.day <= 24):
            weight *= 1.8
        if day.weekday() >= 5:
            weight *= 1.25
        days.append(day)
        weights.append(weight)
    return days, weights


def order_status_for(order_date: datetime, channel: str):
    """Return (status, updated_at) consistent with the age of the order."""
    if channel == "store":
        if random.random() < 0.02:
            return "cancelled", order_date + timedelta(minutes=random.randint(5, 60))
        return "delivered", order_date

    age = NOW - order_date
    if random.random() < 0.06 and age > timedelta(hours=6):
        return "cancelled", random_between(order_date + timedelta(hours=1), min(NOW, order_date + timedelta(days=5)))
    if age < timedelta(days=1):
        if random.random() < 0.4:
            return "created", order_date
        return "paid", order_date + timedelta(minutes=random.randint(2, 30))
    if age < timedelta(days=3):
        status = random.choices(["paid", "shipped"], weights=[0.3, 0.7], k=1)[0]
    elif age < timedelta(days=7):
        status = random.choices(["shipped", "delivered"], weights=[0.4, 0.6], k=1)[0]
    else:
        status = "delivered"
    offsets = {"paid": timedelta(minutes=random.randint(2, 30)),
               "shipped": timedelta(days=1, hours=random.randint(0, 12)),
               "delivered": timedelta(days=random.randint(2, 7), hours=random.randint(0, 12))}
    return status, min(NOW, order_date + offsets[status])


def payment_method_for(country: str, channel: str) -> str:
    if channel == "store":
        return random.choices(*STORE_PAYMENT_METHODS, k=1)[0]
    methods, weights = PAYMENT_METHODS_BY_COUNTRY.get(country, PAYMENT_METHODS_BY_COUNTRY["DEFAULT"])
    return random.choices(methods, weights=weights, k=1)[0]


def build_orders(customers, products):
    days, weights = day_weights()
    # Customers sorted by sign-up so each order only picks customers that already existed.
    by_signup = sorted(customers, key=lambda c: c["created_at"])
    signup_times = [c["created_at"] for c in by_signup]
    cumulative, running = [], 0.0
    for customer in by_signup:
        running += customer["activity"]
        cumulative.append(running)
    orders, items, payments = [], [], []
    order_item_id = payment_id = 0

    for order_id in range(1, NUM_ORDERS + 1):
        day = random.choices(days, weights=weights, k=1)[0]
        order_date = ms(day + timedelta(minutes=random.randint(6 * 60, 23 * 60 + 59)))
        order_date = min(order_date, NOW - timedelta(hours=2))
        eligible = max(1, bisect.bisect_right(signup_times, order_date))
        customer = by_signup[bisect.bisect_left(cumulative, random.uniform(0, cumulative[eligible - 1]))]
        if order_date < customer["created_at"]:
            order_date = ms(random_between(customer["created_at"], NOW - timedelta(minutes=5)))
        if customer["is_deleted"] and order_date > customer["updated_at"]:
            order_date = ms(random_between(customer["created_at"], customer["updated_at"]))

        channel = random.choices(CHANNELS, weights=CHANNELS_BY_SEGMENT[customer["segment"]], k=1)[0]
        status, updated_at = order_status_for(order_date, channel)

        available = [p for p in products if p["inactive_since"] is None or p["inactive_since"] > order_date]
        chosen = random.sample(
            available,
            k=min(len(available), random.choices([1, 2, 3, 4], weights=[0.45, 0.30, 0.17, 0.08], k=1)[0]),
        )
        total = Decimal("0.00")
        for product in chosen:
            price = (product["old_price"]
                     if product["price_change_at"] and order_date < product["price_change_at"]
                     else product["unit_price"])
            quantity = random.choices([1, 2, 3], weights=[0.75, 0.18, 0.07], k=1)[0]
            order_item_id += 1
            items.append((order_item_id, order_id, product["product_id"], quantity, price,
                          order_date, order_date, False))
            total += price * quantity

        orders.append({
            "order_id": order_id, "customer_id": customer["customer_id"], "order_date": order_date,
            "sales_channel": channel, "order_status": status, "order_total": money(total),
            "created_at": order_date, "updated_at": ms(updated_at), "is_deleted": False,
            "country": customer["country"], "city": customer["city"],
            "product_name": chosen[0]["product_name"],
        })

        method = payment_method_for(customer["country"], channel)
        amount = money(total)

        def add_payment(status_, created, updated, payment_date, method_=method):
            nonlocal payment_id
            payment_id += 1
            payments.append((payment_id, order_id, method_, status_, amount, payment_date,
                             created, updated, False))

        if status == "created":
            add_payment("pending", order_date, order_date, None)
        elif status == "cancelled":
            if random.random() < 0.5:
                add_payment("rejected", order_date, ms(order_date + timedelta(minutes=1)), None)
            else:
                approved_at = ms(order_date + timedelta(minutes=random.randint(2, 30)))
                add_payment("refunded", order_date, ms(updated_at), approved_at)
        else:
            attempt_at = order_date
            if channel != "store" and random.random() < 0.10:
                add_payment("rejected", attempt_at, ms(attempt_at + timedelta(minutes=1)), None,
                            method_=payment_method_for(customer["country"], channel))
                attempt_at = ms(attempt_at + timedelta(minutes=random.randint(2, 15)))
            approved_at = ms(attempt_at + timedelta(minutes=random.randint(1, 20)))
            if channel == "store":
                approved_at = order_date
            add_payment("approved", attempt_at, approved_at, approved_at)

    # Source-system bug: header total does not match the lines (flagged as a warning).
    for order in random.sample([o for o in orders if o["order_status"] == "delivered"], NUM_TOTAL_MISMATCH_ORDERS):
        order["order_total"] = money(order["order_total"] + random.choice([5000, 7900, 12000]))

    # Duplicate charges: a second approved payment for the same delivered order.
    approved = [p for p in payments if p[3] == "approved" and p[2] != "cash"]
    for original in random.sample(approved, NUM_DUPLICATE_CHARGES):
        payment_id += 1
        duplicate_at = ms(original[5] + timedelta(seconds=random.randint(5, 90)))
        payments.append((payment_id, original[1], original[2], "approved", original[4], duplicate_at,
                         original[6], duplicate_at, False))

    return orders, items, payments


def build_tickets(customers, orders):
    orders_by_customer = {}
    for order in orders:
        orders_by_customer.setdefault(order["customer_id"], []).append(order)
    customers_by_id = {c["customer_id"]: c for c in customers}
    buyers = list(orders_by_customer)
    non_buyers = [c["customer_id"] for c in customers if c["customer_id"] not in orders_by_customer]
    order_subjects = [s for s in TICKET_TEMPLATES if s != "No puedo aplicar mi cupón"]
    tickets = []

    for ticket_id in range(1, NUM_TICKETS + 1):
        if random.random() < 0.85 or not non_buyers:
            customer_id = random.choice(buyers)
            order = random.choice(orders_by_customer[customer_id])
            subject = random.choice(order_subjects)
            if order["order_status"] == "cancelled" and random.random() < 0.6:
                subject = "Reembolso no recibido"
            created_at = ms(random_between(order["order_date"] + timedelta(hours=1),
                                           min(NOW, order["order_date"] + timedelta(days=12))))
        else:
            customer_id, order = random.choice(non_buyers), None
            subject = "No puedo aplicar mi cupón"
            created_at = ms(random_between(customers_by_id[customer_id]["created_at"], NOW))

        customer = customers_by_id[customer_id]
        priority_weights, templates = TICKET_TEMPLATES[subject]
        context = {
            "order_id": order["order_id"] if order else "N/A",
            "order_date": order["order_date"].strftime("%d/%m/%Y") if order else "",
            "product": order["product_name"] if order else "el producto",
            "channel": {"web": "web", "app": "app", "store": "tienda"}[order["sales_channel"]] if order else "app",
            "city": customer["city"],
        }
        body = random.choice(templates).format(**context)
        priority = random.choices(["low", "medium", "high", "urgent"], weights=priority_weights, k=1)[0]

        age = NOW - created_at
        if age < timedelta(days=2):
            status = random.choices(["open", "in_progress"], weights=[0.6, 0.4], k=1)[0]
        else:
            status = random.choices(["open", "in_progress", "resolved", "closed"],
                                    weights=[0.05, 0.07, 0.38, 0.50], k=1)[0]
        updated_at = created_at if status == "open" else ms(
            random_between(created_at + timedelta(hours=1), min(NOW, created_at + timedelta(days=10))))
        tickets.append((ticket_id, customer_id, subject, body, status, priority, created_at, updated_at, False))

    for index in random.sample(range(len(tickets)), NUM_EMPTY_TICKET_BODIES):
        tickets[index] = tickets[index][:3] + ("   ",) + tickets[index][4:]
    return tickets


# ---------------------------------------------------------------------------
# Database load
# ---------------------------------------------------------------------------

def bulk_insert(cursor, table, columns, rows):
    placeholders = ", ".join("?" for _ in columns)
    cursor.execute(f"SET IDENTITY_INSERT {table} ON;")
    cursor.fast_executemany = True
    batch_size = 2000
    for start in range(0, len(rows), batch_size):
        cursor.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders});",
            rows[start:start + batch_size],
        )
    cursor.execute(f"SET IDENTITY_INSERT {table} OFF;")
    print(f"- {table}: {len(rows)} filas")


def main():
    customers = build_customers()
    products = build_products()
    orders, items, payments = build_orders(customers, products)
    tickets = build_tickets(customers, orders)

    connection = get_connection()
    cursor = connection.cursor()
    try:
        print("Limpiando datos previos...")
        for table in ["dbo.SupportTickets", "dbo.Payments", "dbo.OrderItems",
                      "dbo.Orders", "dbo.Products", "dbo.Customers"]:
            cursor.execute(f"DELETE FROM {table};")

        print("Insertando datos sintéticos:")
        bulk_insert(cursor, "dbo.Customers",
                    ["customer_id", "first_name", "last_name", "email", "phone", "city", "country",
                     "segment", "signup_date", "created_at", "updated_at", "is_deleted"],
                    [(c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], c["city"],
                      c["country"], c["segment"], c["signup_date"], c["created_at"], c["updated_at"],
                      c["is_deleted"]) for c in customers])
        bulk_insert(cursor, "dbo.Products",
                    ["product_id", "sku", "product_name", "category", "unit_price", "product_description",
                     "product_status", "created_at", "updated_at", "is_deleted"],
                    [(p["product_id"], p["sku"], p["product_name"], p["category"], p["unit_price"],
                      p["product_description"], p["product_status"], p["created_at"], p["updated_at"],
                      p["is_deleted"]) for p in products])
        bulk_insert(cursor, "dbo.Orders",
                    ["order_id", "customer_id", "order_date", "sales_channel", "order_status", "order_total",
                     "created_at", "updated_at", "is_deleted"],
                    [(o["order_id"], o["customer_id"], o["order_date"], o["sales_channel"], o["order_status"],
                      o["order_total"], o["created_at"], o["updated_at"], o["is_deleted"]) for o in orders])
        bulk_insert(cursor, "dbo.OrderItems",
                    ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
                     "created_at", "updated_at", "is_deleted"], items)
        bulk_insert(cursor, "dbo.Payments",
                    ["payment_id", "order_id", "payment_method", "payment_status", "amount", "payment_date",
                     "created_at", "updated_at", "is_deleted"], payments)
        bulk_insert(cursor, "dbo.SupportTickets",
                    ["ticket_id", "customer_id", "subject", "ticket_body", "ticket_status", "priority",
                     "created_at", "updated_at", "is_deleted"], tickets)

        connection.commit()
        print("\nCarga sintética completada correctamente.")
    except Exception:
        connection.rollback()
        print("\nOcurrió un error durante la carga; se hizo rollback.")
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
