# Arquitectura de ingesta — Nivel 1

## Implementación realizada

```text
┌─────────────────────────────────────────────────────────────────────┐
│                  FUENTE TRANSACCIONAL — Azure SQL Database           │
│ dbo.Customers  dbo.Products  dbo.Orders  dbo.OrderItems              │
│ dbo.Payments   dbo.SupportTickets   (updated_at indexado, is_deleted)│
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ sql/04_export_source_to_csv.py (pyodbc)
                                   │ updated_at > watermark − lookback
                                   │ AND updated_at <= high_watermark (fijo al inicio)
                                   │ watermark avanza solo si todo terminó bien
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ UC Volume <catalog>.<prefix>bronze.landing                           │
│   customers/customers_<run_id>.csv ... payments/payments_<run_id>.csv │
│   _manifests/<run_id>.json  (rango de watermarks, filas, archivos)    │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ Job: bronze_ingest (notebooks/01_ingest_bronze)
                                   │ Auto Loader cloudFiles · availableNow
                                   │ checkpoint + schemaLocation por tabla
                                   │ addNewColumns · _rescued_data · append-only
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Bronze Delta <prefix>bronze.<tabla>  — todas las versiones recibidas  │
│ _source_system | _source_table | _source_file | _source_file_modified_at│
│ _ingestion_run_id | _ingested_at                                      │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ Job: silver_transform → gold_publish (opcional)
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ <prefix>control                                                       │
│ ingestion_watermarks · silver_progress · data_quality_results         │
│ quarantine_records · Volume checkpoints                               │
└─────────────────────────────────────────────────────────────────────┘
```

Entornos (Bundle `databricks.yml`): `dev` → `dev_*`, `staging` → `stg_*`, `prod` → sin prefijo.

## Arquitectura objetivo de producción

```text
Azure SQL Database
        │
        │ JDBC incremental
        │ Watermark: updated_at
        │ Secrets: Azure Key Vault / secret scope
        │ Connectivity: Private Endpoint or allowlisted egress
        ▼
Databricks Job / Workflow
        │
        ├── Lectura incremental con límites inferior y superior
        ├── Validación de esquema
        ├── Reintentos y alertas
        └── MERGE idempotente a Bronze Delta
                │
                ▼
     Bronze Delta → Silver → consumo analítico / ML
```

La demostración usa una exportación batch controlada porque Databricks Free Edition presentó restricciones de red para completar la sesión JDBC. La fuente de verdad sigue siendo Azure SQL. En producción, la ingesta usaría JDBC incremental directo con secretos gestionados y conectividad privada o con IPs de salida permitidas.

## Diseño propuesto: streaming de clickstream

```text
App móvil
    │ eventos JSON:
    │ event_id, event_time, customer_id, session_id,
    │ event_type, product_id, channel
    ▼
Azure Event Hubs / Kafka
    ▼
Databricks Structured Streaming
    │ checkpoint persistente
    │ deduplicación por event_id
    │ watermark sobre event_time
    ▼
Bronze Delta → Silver (sesiones y calidad) → consumo BI / ML
```

- Trigger propuesto: procesamiento cada 1 minuto, ajustable al SLA.
- El checkpoint permite tolerancia a fallos y evita reprocesar eventos ya confirmados.
- El watermark permite gestionar eventos tardíos dentro de una ventana definida.
- La deduplicación por `event_id` evita contar varias veces un mismo evento.

## Diseño propuesto: SAP ECC on-premise

```text
SAP ECC on-premise
    │
    │ SAP SLT / SAP Data Services / middleware de integración
    │ CDC o extracción programada
    ▼
Azure Data Lake Storage Gen2 o Azure Event Hubs
    │
    │ VPN o ExpressRoute
    │ Managed Identity + Azure Key Vault
    ▼
Databricks Bronze Delta → Silver → consumo analítico / ML
```

- SAP no se implementó porque el reto solicita solamente una propuesta de diseño.
- La integración se desacopla de SAP para evitar que el procesamiento analítico dependa de consultas directas al ERP.
- Se conservarían claves de negocio, timestamps de cambio y metadatos de origen para garantizar trazabilidad.