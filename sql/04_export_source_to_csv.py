"""Incremental export from Azure SQL to CSV files for the Unity Catalog landing Volume.

Why files instead of JDBC: Databricks Free Edition could not open a JDBC
connection to Azure SQL (serverless egress restrictions), so this script runs
outside Databricks and drops files that Auto Loader ingests incrementally.

Incremental strategy (per table):
- Low watermark  = last exported `updated_at` (state file) minus a lookback
  window, to catch rows committed late with an older `updated_at`.
- High watermark = SYSUTCDATETIME() captured once at the start of the run, so
  the window is closed and repeatable: updated_at > low AND updated_at <= high.
- Rows re-exported by the lookback are harmless: Bronze is append-only and
  Silver keeps the latest version per key with a deterministic MERGE.
- The state advances only after every table was written (and uploaded, when
  --upload is used). A failed run is simply re-executed.

Usage:
    python sql/04_export_source_to_csv.py --mode full                  # first load
    python sql/04_export_source_to_csv.py                              # incremental
    python sql/04_export_source_to_csv.py --upload --volume-path /Volumes/workspace/bronze/landing
"""

import argparse
import csv
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "staging"
STATE_FILE = PROJECT_ROOT / "data" / "state" / "export_watermarks.json"
DEFAULT_VOLUME_PATH = os.getenv("LANDING_VOLUME_PATH", "/Volumes/workspace/bronze/landing")
WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

SOURCE_TABLES = [
    ("customers", "dbo.Customers", "customer_id"),
    ("products", "dbo.Products", "product_id"),
    ("orders", "dbo.Orders", "order_id"),
    ("order_items", "dbo.OrderItems", "order_item_id"),
    ("payments", "dbo.Payments", "payment_id"),
    ("support_tickets", "dbo.SupportTickets", "ticket_id"),
]


def serialize_csv_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def export_table(cursor, run_id, file_name, source_table, order_column, low, high):
    table_dir = OUTPUT_DIR / file_name
    table_dir.mkdir(parents=True, exist_ok=True)
    output_file = table_dir / f"{file_name}_{run_id}.csv"

    query = f"SELECT * FROM {source_table} WHERE updated_at <= ?"
    params = [high]
    if low is not None:
        query += " AND updated_at > ?"
        params.append(low)
    cursor.execute(query + f" ORDER BY updated_at, {order_column};", params)

    columns = [column[0] for column in cursor.description]
    updated_at_index = columns.index("updated_at")
    count, max_updated_at = 0, None

    with output_file.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        while True:
            rows = cursor.fetchmany(2000)
            if not rows:
                break
            writer.writerows([[serialize_csv_value(value) for value in row] for row in rows])
            count += len(rows)
            max_updated_at = rows[-1][updated_at_index]

    if count == 0:
        output_file.unlink()
        output_file = None
    print(f"- {source_table}: {count} filas (> {low} y <= {high})"
          + (f" -> {output_file.relative_to(PROJECT_ROOT)}" if output_file else ""))
    return output_file, count, max_updated_at


def ensure_volume(client, volume_path):
    """Create the target schema and managed Volume on first use (/Volumes/<catalog>/<schema>/<volume>)."""
    from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists
    from databricks.sdk.service.catalog import VolumeType

    parts = volume_path.strip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise ValueError(f"--volume-path debe ser /Volumes/<catalogo>/<esquema>/<volume>: {volume_path}")
    catalog, schema, volume = parts[1:4]
    try:
        client.schemas.create(name=schema, catalog_name=catalog)
    except (AlreadyExists, ResourceAlreadyExists):
        pass
    try:
        client.volumes.create(catalog_name=catalog, schema_name=schema, name=volume, volume_type=VolumeType.MANAGED)
    except (AlreadyExists, ResourceAlreadyExists):
        pass


def upload_files(files, volume_path):
    from databricks.sdk import WorkspaceClient  # auth: DATABRICKS_HOST/TOKEN or ~/.databrickscfg

    client = WorkspaceClient()
    ensure_volume(client, volume_path)
    for local_file in files:
        relative = local_file.relative_to(OUTPUT_DIR).as_posix()
        with local_file.open("rb") as content:
            client.files.upload(f"{volume_path}/{relative}", content, overwrite=False)
        print(f"  subido {volume_path}/{relative}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--lookback-minutes", type=int, default=10)
    parser.add_argument("--upload", action="store_true", help="Sube los archivos al Volume con el SDK de Databricks")
    parser.add_argument("--volume-path", default=DEFAULT_VOLUME_PATH)
    args = parser.parse_args()

    state = {} if args.mode == "full" else load_state()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]

    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT SYSUTCDATETIME();")
        high = cursor.fetchone()[0]
        print(f"Run {run_id} | modo={args.mode} | high watermark={high}\n")

        written, manifest_tables, new_state = [], [], dict(state)
        for file_name, source_table, order_column in SOURCE_TABLES:
            previous = state.get(source_table)
            low = (datetime.strptime(previous, WATERMARK_FORMAT) - timedelta(minutes=args.lookback_minutes)
                   if previous else None)
            output_file, count, max_updated_at = export_table(
                cursor, run_id, file_name, source_table, order_column, low, high)
            if output_file:
                written.append(output_file)
            if max_updated_at is not None:
                new_state[source_table] = max_updated_at.strftime(WATERMARK_FORMAT)
            manifest_tables.append({
                "source_table": source_table, "rows": count,
                "low_watermark": low.isoformat(sep=" ") if low else None,
                "high_watermark": high.isoformat(sep=" "),
                "file": output_file.relative_to(OUTPUT_DIR).as_posix() if output_file else None,
            })
    finally:
        cursor.close()
        connection.close()

    manifest = OUTPUT_DIR / "_manifests" / f"{run_id}.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({"run_id": run_id, "mode": args.mode, "tables": manifest_tables}, indent=2),
                        encoding="utf-8")

    if args.upload:
        print(f"\nSubiendo a {args.volume_path} ...")
        upload_files(written + [manifest], args.volume_path)

    save_state(new_state)
    print(f"\nExport completado. Manifest: {manifest.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
