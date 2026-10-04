"""Azure SQL connection helper shared by the seed, simulation and export scripts.

Credentials are read from environment variables (or a local `.env` file that
is excluded from Git). `AZURE_SQL_TRUST_SERVER_CERTIFICATE=yes` is only meant
for local testing against a SQL Server container; never use it with Azure.
"""

import os

import pyodbc
from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARIABLES = [
    "AZURE_SQL_SERVER",
    "AZURE_SQL_DATABASE",
    "AZURE_SQL_USERNAME",
    "AZURE_SQL_PASSWORD",
]


def get_connection(autocommit: bool = False) -> pyodbc.Connection:
    missing = [key for key in REQUIRED_VARIABLES if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno / .env: {', '.join(missing)}")

    trust_certificate = os.getenv("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "no").lower()
    port = os.getenv("AZURE_SQL_PORT", "1433")

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{os.environ['AZURE_SQL_SERVER']},{port};"
        f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
        f"UID={os.environ['AZURE_SQL_USERNAME']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        "Encrypt=yes;"
        f"TrustServerCertificate={'yes' if trust_certificate == 'yes' else 'no'};"
        "Connection Timeout=60;"
    )
    return pyodbc.connect(connection_string, timeout=60, autocommit=autocommit)
