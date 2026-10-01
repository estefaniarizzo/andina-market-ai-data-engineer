# Arquitectura de ingesta — Nivel 1

## Implementación realizada

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         FUENTE TRANSACCIONAL                        │
│                         Azure SQL Database                          │
│                                                                     │
│ dbo.Customers      dbo.Products        dbo.Orders                   │
│ dbo.OrderItems     dbo.Payments        dbo.SupportTickets           │
│                                                                     │
│ Datos sintéticos insertados directamente mediante Python + pyodbc   │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   │ Exportación batch controlada
                                   │ Python + pyodbc
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Staging local temporal                                  │
│                     data/staging/*.csv                                      │
│                                                                             │
│ customers | products | orders | order_items | payments | support_tickets    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   │ Carga manual al Volume
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Databricks Free Edition                           │
│                                                                     │
│ Unity Catalog: workspace                                            │
│ Schema Bronze: workspace.bronze                                     │
│ Volume: workspace.bronze.landing                                    │
│                                                                     │
│ CSV landing → notebooks/01_ingest_bronze                            │
│              → tablas Delta Bronze                                  │
│                                                                     │
│ Bronze metadata:                                                    │
│ _source_system | _source_table | _source_file                       │
│ _ingestion_run_id | _ingested_at                                    │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   │ Auditoría y control
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      workspace.control                              │
│                                                                     │
│ ingestion_watermarks                                                │
│ - source_table                                                      │
│ - last_successful_watermark                                         │
│ - last_run_id                                                       │
│ - rows_read                                                         │
│ - run_status                                                        │
│ - updated_at                                                        │
└─────────────────────────────────────────────────────────────────────┘
```

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