import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pyodbc
from dotenv import load_dotenv
from faker import Faker


load_dotenv()

SEED = 2026
NUM_CUSTOMERS = 60
NUM_ORDERS = 120
NUM_TICKETS = 40

random.seed(SEED)
fake = Faker("es_CO")
Faker.seed(SEED)

COUNTRIES = {
    "CO": {
        "cities": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga"],
        "weight": 0.55,
    },
    "MX": {
        "cities": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla"],
        "weight": 0.18,
    },
    "CL": {
        "cities": ["Santiago", "Valparaíso", "Concepción"],
        "weight": 0.10,
    },
    "PE": {
        "cities": ["Lima", "Arequipa", "Cusco"],
        "weight": 0.10,
    },
    "AR": {
        "cities": ["Buenos Aires", "Córdoba", "Rosario"],
        "weight": 0.07,
    },
}

PRODUCT_CATALOG = [
    ("TEC-001", "Audífonos inalámbricos Andina Wave", "Tecnología", Decimal("159900.00"),
     "Audífonos Bluetooth con estuche de carga."),
    ("TEC-002", "Teclado mecánico Compact Pro", "Tecnología", Decimal("289900.00"),
     "Teclado mecánico compacto con iluminación RGB."),
    ("TEC-003", "Mouse ergonómico Flow", "Tecnología", Decimal("89900.00"),
     "Mouse inalámbrico ergonómico para trabajo."),
    ("TEC-004", "Cargador USB-C 65W", "Tecnología", Decimal("119900.00"),
     "Cargador rápido compatible con USB-C."),
    ("TEC-005", "Smartwatch Active 2", "Tecnología", Decimal("249900.00"),
     "Reloj inteligente para actividad y notificaciones."),
    ("HOG-001", "Licuadora Compacta 1.5L", "Hogar", Decimal("139900.00"),
     "Licuadora de vaso de 1.5 litros."),
    ("HOG-002", "Set de sábanas Queen algodón", "Hogar", Decimal("129900.00"),
     "Juego de sábanas de algodón tamaño Queen."),
    ("HOG-003", "Lámpara de escritorio LED", "Hogar", Decimal("79900.00"),
     "Lámpara LED ajustable con tres intensidades."),
    ("HOG-004", "Botella térmica 750 ml", "Hogar", Decimal("69900.00"),
     "Botella de acero inoxidable."),
    ("HOG-005", "Organizador modular 6 cubos", "Hogar", Decimal("109900.00"),
     "Organizador modular para hogar."),
    ("MOD-001", "Mochila urbana impermeable", "Moda", Decimal("149900.00"),
     "Mochila impermeable con espacio para portátil."),
    ("MOD-002", "Chaqueta liviana unisex", "Moda", Decimal("189900.00"),
     "Chaqueta para clima variable."),
    ("MOD-003", "Tenis deportivos Run", "Moda", Decimal("229900.00"),
     "Tenis para caminata y entrenamiento."),
    ("MOD-004", "Gorra ajustable clásica", "Moda", Decimal("49900.00"),
     "Gorra de algodón ajustable."),
    ("MOD-005", "Billetera minimalista", "Moda", Decimal("59900.00"),
     "Billetera compacta con protección RFID."),
    ("BEL-001", "Protector solar SPF 50", "Belleza", Decimal("64900.00"),
     "Protector solar facial de amplio espectro."),
    ("BEL-002", "Kit hidratación facial", "Belleza", Decimal("119900.00"),
     "Kit facial de limpieza e hidratación."),
    ("BEL-003", "Secador de cabello Ion", "Belleza", Decimal("159900.00"),
     "Secador con tecnología iónica."),
    ("DEP-001", "Mat de yoga antideslizante", "Deportes", Decimal("89900.00"),
     "Tapete de yoga de 6 mm."),
    ("DEP-002", "Mancuernas ajustables 10 kg", "Deportes", Decimal("199900.00"),
     "Par de mancuernas para entrenamiento."),
    ("DEP-003", "Bandas elásticas set x5", "Deportes", Decimal("49900.00"),
     "Set de bandas de resistencia."),
    ("LIB-001", "Agenda semanal 2026", "Papelería", Decimal("39900.00"),
     "Agenda semanal y mensual."),
    ("LIB-002", "Cuaderno punteado A5", "Papelería", Decimal("29900.00"),
     "Cuaderno para notas y bullet journal."),
    ("LIB-003", "Set de marcadores pastel", "Papelería", Decimal("35900.00"),
     "Set de seis marcadores pastel."),
]

CHANNELS = ["web", "app", "store"]
PAYMENT_METHODS = ["credit_card", "debit_card", "pse", "cash", "wallet"]
SUPPORT_SUBJECTS = [
    "No recibí confirmación de mi pedido",
    "Solicitud de cambio de producto",
    "Producto recibido con defecto",
    "Consulta sobre tiempos de entrega",
    "No puedo aplicar mi cupón",
    "Solicitud de devolución",
    "Cobro duplicado en mi tarjeta",
    "Actualización de dirección de entrega",
]


def utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def random_past_datetime(days_back):
    return utc_now_naive() - timedelta(
        days=random.randint(1, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def get_connection():
    required = [
        "AZURE_SQL_SERVER",
        "AZURE_SQL_DATABASE",
        "AZURE_SQL_USERNAME",
        "AZURE_SQL_PASSWORD",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan valores en .env: {', '.join(missing)}")

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{os.environ['AZURE_SQL_SERVER']},1433;"
        f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
        f"UID={os.environ['AZURE_SQL_USERNAME']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
    )

    return pyodbc.connect(connection_string, timeout=60)


def delete_existing_data(cursor):
    cursor.execute("DELETE FROM dbo.SupportTickets;")
    cursor.execute("DELETE FROM dbo.Payments;")
    cursor.execute("DELETE FROM dbo.OrderItems;")
    cursor.execute("DELETE FROM dbo.Orders;")
    cursor.execute("DELETE FROM dbo.Products;")
    cursor.execute("DELETE FROM dbo.Customers;")


def insert_customers(cursor):
    country_codes = list(COUNTRIES.keys())
    country_weights = [COUNTRIES[country]["weight"] for country in country_codes]
    rows = []

    for index in range(NUM_CUSTOMERS):
        country = random.choices(country_codes, weights=country_weights, k=1)[0]
        created_at = random_past_datetime(720)
        first_name = fake.first_name()
        last_name = fake.last_name()
        segment = random.choices(
            ["Regular", "Premium", "Business"],
            weights=[0.72, 0.23, 0.05],
            k=1,
        )[0]

        rows.append(
            (
                first_name,
                last_name,
                f"{first_name.lower()}.{last_name.lower()}.{index + 1}@example.andina",
                fake.phone_number()[:30],
                random.choice(COUNTRIES[country]["cities"]),
                country,
                segment,
                created_at.date(),
                created_at,
                created_at,
                False,
            )
        )

    cursor.fast_executemany = True
    cursor.executemany(
        """
        INSERT INTO dbo.Customers (
            first_name, last_name, email, phone, city, country, segment,
            signup_date, created_at, updated_at, is_deleted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )


def insert_products(cursor):
    rows = []

    for index, (sku, name, category, price, description) in enumerate(PRODUCT_CATALOG):
        created_at = random_past_datetime(540)
        product_status = "inactive" if index == len(PRODUCT_CATALOG) - 1 else "active"

        rows.append(
            (
                sku,
                name,
                category,
                price,
                description,
                product_status,
                created_at,
                created_at,
                False,
            )
        )

    cursor.fast_executemany = True
    cursor.executemany(
        """
        INSERT INTO dbo.Products (
            sku, product_name, category, unit_price, product_description,
            product_status, created_at, updated_at, is_deleted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )


def fetch_ids_and_prices(cursor):
    cursor.execute("SELECT customer_id FROM dbo.Customers WHERE is_deleted = 0;")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT product_id, unit_price
        FROM dbo.Products
        WHERE is_deleted = 0;
        """
    )
    product_prices = {row[0]: Decimal(str(row[1])) for row in cursor.fetchall()}

    return customer_ids, product_prices


def insert_orders_items_payments(cursor, customer_ids, product_prices):
    for order_number in range(NUM_ORDERS):
        created_at = random_past_datetime(365)
        customer_id = random.choice(customer_ids)
        channel = random.choices(CHANNELS, weights=[0.50, 0.35, 0.15], k=1)[0]
        order_status = random.choices(
            ["created", "paid", "shipped", "delivered", "cancelled"],
            weights=[0.03, 0.07, 0.15, 0.68, 0.07],
            k=1,
        )[0]

        selected_product_ids = random.sample(
            list(product_prices.keys()),
            k=random.randint(1, 3),
        )

        item_rows = []
        order_total = Decimal("0.00")

        for product_id in selected_product_ids:
            quantity = random.randint(1, 3)
            unit_price = product_prices[product_id]
            order_total += unit_price * quantity
            item_rows.append((product_id, quantity, unit_price))

        cursor.execute(
            """
            INSERT INTO dbo.Orders (
                customer_id, order_date, sales_channel, order_status, order_total,
                created_at, updated_at, is_deleted
            )
            OUTPUT INSERTED.order_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            customer_id,
            created_at,
            channel,
            order_status,
            order_total,
            created_at,
            created_at,
            False,
        )
        order_id = cursor.fetchone()[0]

        cursor.fast_executemany = True
        cursor.executemany(
            """
            INSERT INTO dbo.OrderItems (
                order_id, product_id, quantity, unit_price,
                created_at, updated_at, is_deleted
            )
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            [
                (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    created_at,
                    created_at,
                    False,
                )
                for product_id, quantity, unit_price in item_rows
            ],
        )

        if order_status == "cancelled":
            payment_status = random.choice(["rejected", "refunded"])
            payment_date = None
        elif order_status == "created":
            payment_status = "pending"
            payment_date = None
        else:
            payment_status = "approved"
            payment_date = created_at + timedelta(minutes=random.randint(2, 30))

        cursor.execute(
            """
            INSERT INTO dbo.Payments (
                order_id, payment_method, payment_status, amount, payment_date,
                created_at, updated_at, is_deleted
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            order_id,
            random.choice(PAYMENT_METHODS),
            payment_status,
            order_total,
            payment_date,
            created_at,
            created_at,
            False,
        )

        if (order_number + 1) % 20 == 0:
            print(f"  Pedidos procesados: {order_number + 1}/{NUM_ORDERS}")


def insert_support_tickets(cursor, customer_ids):
    rows = []

    for _ in range(NUM_TICKETS):
        created_at = random_past_datetime(180)
        subject = random.choice(SUPPORT_SUBJECTS)
        ticket_status = random.choices(
            ["open", "in_progress", "resolved", "closed"],
            weights=[0.18, 0.12, 0.35, 0.35],
            k=1,
        )[0]
        priority = random.choices(
            ["low", "medium", "high", "urgent"],
            weights=[0.20, 0.52, 0.22, 0.06],
            k=1,
        )[0]

        rows.append(
            (
                random.choice(customer_ids),
                subject,
                (
                    f"{subject}. {fake.paragraph(nb_sentences=3)} "
                    "Mensaje generado como dato sintético para Andina Market."
                ),
                ticket_status,
                priority,
                created_at,
                created_at,
                False,
            )
        )

    cursor.fast_executemany = True
    cursor.executemany(
        """
        INSERT INTO dbo.SupportTickets (
            customer_id, subject, ticket_body, ticket_status, priority,
            created_at, updated_at, is_deleted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )


def apply_controlled_changes(cursor):
    now = utc_now_naive()

    cursor.execute(
        """
        UPDATE TOP (1) dbo.Customers
        SET segment = 'Premium', updated_at = ?;
        """,
        now,
    )

    cursor.execute(
        """
        UPDATE TOP (1) dbo.Payments
        SET payment_status = 'approved',
            payment_date = COALESCE(payment_date, ?),
            updated_at = ?
        WHERE payment_status = 'pending';
        """,
        now,
        now,
    )

    cursor.execute(
        """
        UPDATE TOP (1) dbo.Products
        SET product_status = 'discontinued',
            is_deleted = 1,
            updated_at = ?
        WHERE product_status = 'inactive';
        """,
        now,
    )


def print_counts(cursor):
    table_names = [
        "Customers",
        "Products",
        "Orders",
        "OrderItems",
        "Payments",
        "SupportTickets",
    ]

    print("\nRegistros insertados:")
    for table_name in table_names:
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name};")
        print(f"- {table_name}: {cursor.fetchone()[0]}")


def main():
    connection = get_connection()
    connection.autocommit = False
    cursor = connection.cursor()

    try:
        print("Limpiando datos previos...")
        delete_existing_data(cursor)
        connection.commit()

        print("Insertando clientes...")
        insert_customers(cursor)
        connection.commit()

        print("Insertando productos...")
        insert_products(cursor)
        connection.commit()

        customer_ids, product_prices = fetch_ids_and_prices(cursor)

        print("Insertando pedidos, líneas y pagos...")
        insert_orders_items_payments(cursor, customer_ids, product_prices)
        connection.commit()

        print("Insertando tickets de soporte...")
        insert_support_tickets(cursor, customer_ids)
        connection.commit()

        print("Aplicando cambios controlados...")
        apply_controlled_changes(cursor)
        connection.commit()

        print_counts(cursor)
        print("\nCarga sintética completada correctamente.")

    except Exception:
        try:
            connection.rollback()
        except pyodbc.Error:
            pass
        print("\nOcurrió un error durante la carga.")
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()