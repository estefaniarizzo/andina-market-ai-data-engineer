"""Run Bronze -> Silver -> Gold locally with Apache Spark + Delta Lake (no Databricks needed).

Uses the same package code as the Databricks Job. Differences, by design:
- the catalog is `spark_catalog` (Hive metastore in ./data/local_lakehouse);
- Bronze uses Spark's file streaming source instead of Auto Loader (`cloudFiles`
  only exists in Databricks), with the same checkpoint semantics.

Usage:
    python scripts/run_local_pipeline.py --reset          # rebuild from data/staging
    python scripts/run_local_pipeline.py                  # incremental (new files only)
    python scripts/run_local_pipeline.py --inject-bad-rows # add a corrupt file to demo quarantine

Delta jars: set DELTA_JARS=/path/delta-spark_2.12-3.2.0.jar,/path/delta-storage-3.2.0.jar to use local
jars; otherwise they are downloaded from Maven by delta-spark.
"""

import argparse
import shutil
import sys
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from andina_pipeline import gold, pipeline  # noqa: E402
from andina_pipeline.config import LakehouseConfig  # noqa: E402
from andina_pipeline.spark_session import local_spark  # noqa: E402

LOCAL_ROOT = PROJECT_ROOT / "data" / "local_lakehouse"

BAD_ORDERS_CSV = """order_id,customer_id,order_date,sales_channel,order_status,order_total,created_at,updated_at,is_deleted
990001,1,2026-01-15 10:00:00.000,web,delivered,-5000.00,2026-01-15 10:00:00.000,2026-01-15 10:00:00.000,false
990002,99999999,2026-01-15 11:00:00.000,app,paid,120000.00,2026-01-15 11:00:00.000,2026-01-15 11:00:00.000,false
990003,2,15/01/2026,web,lost,80000.00,2026-01-15 12:00:00.000,not-a-date,false
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--inject-bad-rows", action="store_true")
    parser.add_argument("--max-error-rate", type=float, default=0.05)
    parser.add_argument("--skip-gold", action="store_true")
    args = parser.parse_args()

    if args.reset and LOCAL_ROOT.exists():
        shutil.rmtree(LOCAL_ROOT)
    if args.inject_bad_rows:
        bad_file = PROJECT_ROOT / "data" / "staging" / "orders" / f"orders_bad_{uuid4().hex[:6]}.csv"
        bad_file.write_text(BAD_ORDERS_CSV, encoding="utf-8")
        print(f"Archivo corrupto agregado: {bad_file.relative_to(PROJECT_ROOT)}")

    spark = local_spark(warehouse_dir=str(LOCAL_ROOT / "warehouse"))
    cfg = LakehouseConfig(catalog="spark_catalog", landing_root=str(PROJECT_ROOT / "data" / "staging"),
                          checkpoint_root=str(LOCAL_ROOT / "checkpoints"))
    run_id = str(uuid4())
    pipeline.ensure_schemas(spark, cfg)

    print("\n== Bronze ==")
    for r in pipeline.run_bronze(spark, cfg, run_id, use_autoloader=False):
        print(f"{r['entity']:<16} filas nuevas={r['rows_read']:<6} watermark={r['watermark']}")

    print("\n== Silver ==")
    result = pipeline.run_silver(spark, cfg, run_id, args.max_error_rate)
    for s in result["entities"]:
        print(f"{s['entity']:<16} procesadas={s['rows_processed']:<6} cuarentena={s['rows_quarantined']:<3} "
              f"historial+={s['history_rows']:<5} fallos={s['rule_failures']}")
    print("\nChecks con hallazgos:")
    for c in result["checks"]:
        if c["failed_rows"]:
            print(f"- [{c['severity']}] {c['table_name']}.{c['check_name']}: {c['failed_rows']}/{c['total_rows']}")

    if not args.skip_gold:
        print("\n== Gold (opcional) ==")
        print(gold.publish(spark, cfg))
    spark.stop()


if __name__ == "__main__":
    main()
