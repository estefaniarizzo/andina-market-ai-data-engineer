"""Simulate one business cycle of changes in Azure SQL to demonstrate incremental loads.

Every change sets `updated_at = SYSUTCDATETIME()`, exactly like the operational
application would, so the next incremental export only picks up these rows:

- Order lifecycle: created -> paid, paid -> shipped, shipped -> delivered.
- Payments: pending -> approved, pending -> rejected + new approved retry,
  approved -> refunded when an order is cancelled (the case the challenge warns about).
- Customers: segment upgrades (tracked as SCD2 in Silver), a soft delete and new sign-ups.
- Products: price increase and a product discontinued (soft delete).
- New orders with their lines and payments; tickets resolved and new tickets.

Usage:
    python sql/05_simulate_source_changes.py [--seed 7]
"""

import argparse
import random

from db import get_connection
from synthetic_catalog import COUNTRIES, PAYMENT_METHODS_BY_COUNTRY

NOW = "SYSUTCDATETIME()"


def ids(cursor, query, limit):
    cursor.execute(query.format(limit=limit))
    return [row[0] for row in cursor.fetchall()]


def run(cursor, rng):
    summary = {}

    # --- Order lifecycle and payments ------------------------------------------------
    created = ids(cursor, "SELECT TOP {limit} order_id FROM dbo.Orders WHERE order_status = 'created' "
                          "ORDER BY order_id", 40)
    retries = max(1, len(created) // 4) if created else 0
    to_retry, to_approve = created[:retries], created[retries:]
    for order_id in to_approve:
        cursor.execute(f"UPDATE dbo.Payments SET payment_status = 'approved', payment_date = {NOW}, "
                       f"updated_at = {NOW} WHERE order_id = ? AND payment_status = 'pending';", order_id)
    for order_id in to_retry:
        cursor.execute(f"UPDATE dbo.Payments SET payment_status = 'rejected', updated_at = {NOW} "
                       "WHERE order_id = ? AND payment_status = 'pending';", order_id)
        cursor.execute(
            "INSERT INTO dbo.Payments (order_id, payment_method, payment_status, amount, payment_date) "
            f"SELECT order_id, 'credit_card', 'approved', order_total, {NOW} FROM dbo.Orders WHERE order_id = ?;",
            order_id)
    if created:
        placeholders = ",".join("?" for _ in created)
        cursor.execute(f"UPDATE dbo.Orders SET order_status = 'paid', updated_at = {NOW} "
                       f"WHERE order_id IN ({placeholders});", created)
    summary["pagos pending -> approved"] = len(to_approve)
    summary["pagos rechazados + reintento aprobado"] = len(to_retry)

    cancelled = ids(cursor, "SELECT TOP {limit} order_id FROM dbo.Orders WHERE order_status = 'paid' "
                            "AND order_id NOT IN (SELECT order_id FROM dbo.Payments WHERE payment_status <> 'approved') "
                            "ORDER BY order_date DESC", 5)
    for order_id in cancelled:
        cursor.execute(f"UPDATE dbo.Orders SET order_status = 'cancelled', updated_at = {NOW} WHERE order_id = ?;",
                       order_id)
        cursor.execute(f"UPDATE dbo.Payments SET payment_status = 'refunded', updated_at = {NOW} "
                       "WHERE order_id = ? AND payment_status = 'approved';", order_id)
    summary["pedidos cancelados con reembolso"] = len(cancelled)

    cursor.execute(f"UPDATE dbo.Orders SET order_status = 'delivered', updated_at = {NOW} "
                   "WHERE order_id IN (SELECT TOP 60 order_id FROM dbo.Orders WHERE order_status = 'shipped' "
                   "ORDER BY order_date);")
    summary["pedidos shipped -> delivered"] = cursor.rowcount
    cursor.execute(f"UPDATE dbo.Orders SET order_status = 'shipped', updated_at = {NOW} "
                   "WHERE order_id IN (SELECT TOP 50 order_id FROM dbo.Orders WHERE order_status = 'paid' "
                   "ORDER BY order_date);")
    summary["pedidos paid -> shipped"] = cursor.rowcount


    # --- Customers -------------------------------------------------------------------
    cursor.execute(f"UPDATE dbo.Customers SET segment = 'Premium', updated_at = {NOW} WHERE customer_id IN ("
                   "SELECT TOP 25 c.customer_id FROM dbo.Customers c JOIN dbo.Orders o ON o.customer_id = c.customer_id "
                   "WHERE c.segment = 'Regular' AND c.is_deleted = 0 GROUP BY c.customer_id "
                   "ORDER BY SUM(o.order_total) DESC);")
    summary["clientes Regular -> Premium (SCD2)"] = cursor.rowcount
    cursor.execute(f"UPDATE dbo.Customers SET segment = 'Business', updated_at = {NOW} WHERE customer_id IN ("
                   "SELECT TOP 3 customer_id FROM dbo.Customers WHERE segment = 'Premium' AND is_deleted = 0 "
                   "ORDER BY customer_id);")
    summary["clientes Premium -> Business (SCD2)"] = cursor.rowcount
    cursor.execute(f"UPDATE dbo.Customers SET is_deleted = 1, updated_at = {NOW} WHERE customer_id = ("
                   "SELECT MAX(customer_id) FROM dbo.Customers WHERE is_deleted = 0);")
    summary["clientes dados de baja (soft delete)"] = cursor.rowcount

    new_customer_ids = []
    countries = list(COUNTRIES)
    for index in range(20):
        country = rng.choice(countries)
        cursor.execute(
            "INSERT INTO dbo.Customers (first_name, last_name, email, phone, city, country, segment, signup_date) "
            "OUTPUT INSERTED.customer_id VALUES (?, ?, ?, ?, ?, ?, 'Regular', CAST(SYSUTCDATETIME() AS DATE));",
            f"Nuevo{index}", "Cliente", f"nuevo.cliente{rng.randint(100000, 999999)}@example.com",
            None, rng.choice(COUNTRIES[country]["cities"]), country)
        new_customer_ids.append((cursor.fetchone()[0], country))
    summary["clientes nuevos"] = len(new_customer_ids)

    # --- Products --------------------------------------------------------------------
    cursor.execute(f"UPDATE dbo.Products SET unit_price = ROUND(unit_price * 1.08, -2), updated_at = {NOW} "
                   "WHERE sku IN ('TEC-001', 'HOG-001');")
    summary["productos con aumento de precio"] = cursor.rowcount
    cursor.execute(f"UPDATE dbo.Products SET product_status = 'discontinued', is_deleted = 1, updated_at = {NOW} "
                   "WHERE sku = 'LIB-001';")
    summary["productos descontinuados"] = cursor.rowcount

    # --- New orders --------------------------------------------------------------------
    cursor.execute("SELECT product_id, unit_price FROM dbo.Products WHERE product_status = 'active' AND is_deleted = 0;")
    products = cursor.fetchall()
    cursor.execute("SELECT TOP 130 customer_id, country FROM dbo.Customers WHERE is_deleted = 0 ORDER BY NEWID();")
    buyers = [tuple(row) for row in cursor.fetchall()] + new_customer_ids
    for customer_id, country in buyers:
        lines = rng.sample(products, k=rng.choice([1, 1, 2, 3]))
        quantities = [rng.choice([1, 1, 1, 2]) for _ in lines]
        total = sum(product.unit_price * quantity for product, quantity in zip(lines, quantities))
        cursor.execute("INSERT INTO dbo.Orders (customer_id, order_date, sales_channel, order_status, order_total) "
                       "OUTPUT INSERTED.order_id VALUES (?, SYSUTCDATETIME(), ?, 'paid', ?);",
                       customer_id, rng.choice(["web", "app"]), total)
        order_id = cursor.fetchone()[0]
        cursor.executemany("INSERT INTO dbo.OrderItems (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?);",
                           [(order_id, p.product_id, q, p.unit_price) for p, q in zip(lines, quantities)])
        methods, weights = PAYMENT_METHODS_BY_COUNTRY.get(country, PAYMENT_METHODS_BY_COUNTRY["DEFAULT"])
        cursor.execute("INSERT INTO dbo.Payments (order_id, payment_method, payment_status, amount, payment_date) "
                       f"VALUES (?, ?, 'approved', ?, {NOW});",
                       order_id, rng.choices(methods, weights=weights, k=1)[0], total)
    summary["pedidos nuevos"] = len(buyers)

    # --- Support tickets ---------------------------------------------------------------
    cursor.execute(f"UPDATE dbo.SupportTickets SET ticket_status = 'resolved', updated_at = {NOW} "
                   "WHERE ticket_id IN (SELECT TOP 30 ticket_id FROM dbo.SupportTickets "
                   "WHERE ticket_status IN ('open', 'in_progress') ORDER BY created_at);")
    summary["tickets resueltos"] = cursor.rowcount
    for order_id in cancelled:
        cursor.execute(
            "INSERT INTO dbo.SupportTickets (customer_id, subject, ticket_body, ticket_status, priority) "
            "SELECT customer_id, 'Reembolso no recibido', "
            "CONCAT('Cancelé el pedido #', order_id, ' y todavía no veo el reembolso en mi tarjeta.'), 'open', 'high' "
            "FROM dbo.Orders WHERE order_id = ?;", order_id)
    summary["tickets nuevos"] = len(cancelled)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    connection = get_connection()
    cursor = connection.cursor()
    try:
        summary = run(cursor, random.Random(args.seed))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    print("Cambios simulados en la fuente:")
    for change, count in summary.items():
        print(f"- {change}: {count}")


if __name__ == "__main__":
    main()
