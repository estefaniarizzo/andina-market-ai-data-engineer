import csv
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyodbc
from dotenv import load_dotenv


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "staging"

SOURCE_TABLES = [
    ("customers", "dbo.Customers", "customer_id"),
    ("products", "dbo.Products", "product_id"),
    ("orders", "dbo.Orders", "order_id"),
    ("order_items", "dbo.OrderItems", "order_item_id"),
    ("payments", "dbo.Payments", "payment_id"),
    ("support_tickets", "dbo.SupportTickets", "ticket_id"),
]


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


def serialize_csv_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def export_table(cursor, file_name, source_table, order_column):
    output_file = OUTPUT_DIR / f"{file_name}.csv"

    cursor.execute(f"SELECT * FROM {source_table} ORDER BY {order_column};")
    columns = [column[0] for column in cursor.description]
    count = 0

    with output_file.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)

        while True:
            rows = cursor.fetchmany(500)
            if not rows:
                break

            writer.writerows(
                [[serialize_csv_value(value) for value in row] for row in rows]
            )
            count += len(rows)

    print(f"- {source_table}: {count} rows -> {output_file.name}")
    return count


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    connection = get_connection()
    cursor = connection.cursor()

    try:
        print(f"Exporting Azure SQL source tables to:\n{OUTPUT_DIR}\n")

        for file_name, source_table, order_column in SOURCE_TABLES:
            export_table(cursor, file_name, source_table, order_column)

        print("\nExport completed successfully.")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()