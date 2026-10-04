# Diseño: streaming de clickstream y SAP ECC on-premise

Ambos componentes son **diseño**, como pide el reto. La muestra de eventos está en
`data/samples/clickstream_events.jsonl`, generada con `scripts/generate_clickstream.py`.

## 1. Clickstream de la app

### Muestra entregada

Hay 3.392 eventos de 1.200 sesiones (`product_view` → `add_to_cart` → `purchase`) sobre el mismo catálogo e IDs de cliente que Azure SQL, así que se pueden cruzar con Silver.

```json
{"event_id": "c8183dad-...", "event_time": "2026-06-12T18:04:14.504Z", "event_type": "product_view",
 "customer_id": 1827, "session_id": "s-1599145f5806102f", "product_id": 36, "sku": "JUG-002",
 "category": "Juguetes", "price": 129900.0, "quantity": null, "channel": "web", "device": "mobile_web"}
```

La muestra incluye a propósito los problemas típicos de streaming:

| Caso | En la muestra | Tratamiento |
|---|---|---|
| Entrega *at-least-once* | 33 eventos con `event_id` duplicado | `dropDuplicatesWithinWatermark("event_id")` |
| Eventos tardíos | ~0,5 % con `event_time` horas antes de su posición | watermark de 2 horas; lo más tardío va a una tabla de tardíos |
| Sesiones anónimas | `customer_id = null` hasta el login en checkout | se conserva `session_id`; se asocia el cliente al cerrar la sesión |
| Evolución de esquema | `campaign` solo existe en `app_version = 5.3.0` | schema hints + `addNewColumns`, campo opcional |

### Arquitectura

```text
App web / móvil (SDK de eventos)
        │ JSON, at-least-once
        ▼
Azure Event Hubs (endpoint Kafka)  ── 1 partición por cada ~1 MB/s; clave = session_id
        │
        ▼
Databricks Structured Streaming (Job continuo o trigger 1 min)
        │  checkpoint en Volume  → exactly-once hacia Delta
        ▼
bronze.clickstream_events  (payload crudo + offset, partición, _ingested_at)
        │  parseo con esquema explícito, dedupe por event_id,
        │  watermark 2 h sobre event_time
        ▼
silver.clickstream_events  → silver.sessions (ventana de sesión 30 min)
        │
        ▼
Gold: embudo de conversión por producto/canal, features de ML (vistas recientes, carrito abandonado)
```

### Decisiones

- **Fuente:** Event Hubs con interfaz Kafka (`spark.readStream.format("kafka")`). Es administrado en Azure y permite cambiar a Kafka sin reescribir el código.
- **Bronze guarda el payload crudo** (`value` como string) más `topic/partition/offset`. Si cambia el parseo se puede reprocesar.
- **Deduplicación y tardíos:** `withWatermark("event_time", "2 hours")` + `dropDuplicatesWithinWatermark(["event_id"])`. El estado queda acotado a 2 horas. Los eventos más tardíos se guardan en Bronze y se recuperan con un batch diario de reconciliación.
- **Trigger:** cada 1 minuto. Es suficiente para recomendaciones y monitoreo, y cuesta menos que el modo continuo. Si el SLA fuera horario, el mismo código corre con `availableNow` como Job.
- **Calidad:** se aplican las mismas reglas como código (`event_type` en dominio, `product_id` existente en `silver.products`, `price >= 0`). Los inválidos van a cuarentena.
- **Privacidad:** `customer_id` es un ID interno, sin PII en los eventos. La IP o el dispositivo se seudonimizan antes de Bronze si se capturan.

### Esbozo de código

```python
events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "<namespace>.servicebus.windows.net:9093")
          .option("subscribe", "clickstream")
          .option("kafka.sasl.jaas.config", dbutils.secrets.get("andina", "eventhub-jaas"))
          .load())
parsed = (events.select(F.from_json(F.col("value").cast("string"), CLICK_SCHEMA).alias("e"), "offset")
          .select("e.*", "offset")
          .withColumn("event_time", F.to_timestamp("event_time"))
          .withWatermark("event_time", "2 hours")
          .dropDuplicatesWithinWatermark(["event_id"]))
(parsed.writeStream.option("checkpointLocation", "/Volumes/workspace/control/checkpoints/clickstream")
       .trigger(processingTime="1 minute").toTable("workspace.silver.clickstream_events"))
```

## 2. SAP ECC on-premise (pedidos B2B)

### Restricciones

El ERP está on-premise y es crítico: no se le pueden lanzar consultas analíticas pesadas. Los datos de pedidos B2B viven en tablas SAP (`VBAK`/`VBAP` cabecera y posición de pedido, `KNA1` clientes, `MARA` materiales) y cambian todo el día.

### Arquitectura propuesta

```text
SAP ECC (on-prem)
   │  SAP SLT (replicación trigger-based, CDC casi en tiempo real)
   │  — alternativa: extractores ODP/ODQ vía Azure Data Factory (SAP CDC connector)
   ▼
Self-hosted Integration Runtime / SLT server  (en la red on-prem)
   │  VPN site-to-site o ExpressRoute; sin exponer SAP a internet
   ▼
ADLS Gen2 landing (parquet/CSV por tabla + operación I/U/D + timestamp de cambio)
   │
   ▼
Databricks: el MISMO patrón de este proyecto
   Auto Loader → bronze.sap_vbak, bronze.sap_vbap ... (append-only, con operación CDC)
   → Silver: MERGE por llave SAP (VBELN, POSNR), aplicando deletes; SCD2 en clientes
   → mapeo a modelo canónico: silver.orders con source_system = 'sap_ecc'
```

### Decisiones

- **Por qué SLT/ODP y no JDBC al ERP:** la extracción la gestiona SAP, respeta la carga del sistema y entrega cambios (CDC) con la operación (insert/update/delete). El watermark por `updated_at` no sirve en SAP porque no todas las tablas tienen una fecha de modificación confiable.
- **Seguridad:** usuario técnico de SAP con permisos mínimos, credenciales en Azure Key Vault, Managed Identity para ADLS y permisos de Unity Catalog por esquema.
- **Modelo canónico:** las tablas SAP se traducen al mismo modelo de Silver (`orders`, `order_items`, `customers`) con `source_system` y la llave original (`VBELN`). Así Gold no necesita saber de qué sistema viene cada pedido.
- **Calidad específica:** conversión de unidades y monedas (`WAERK`), ceros a la izquierda en llaves SAP y fechas `00000000`. Se aplican con el mismo mecanismo de reglas y cuarentena.
- **Frecuencia:** SLT casi en tiempo real hacia la landing. Databricks lo procesa con el Job horario o con `availableNow` cada 15 minutos si el negocio B2B lo necesita.
