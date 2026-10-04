# Andina Market AI — Reto Técnico Data Engineer

Plataforma de datos para Andina Market, un e-commerce ficticio de supermercado que opera en Latinoamérica por web, app móvil y tiendas físicas.

Lleva los datos transaccionales de Azure SQL Database a Databricks (Unity Catalog + Delta Lake) de forma **incremental**, conserva el **historial de cambios**, aplica **calidad de datos como código** y deja un modelo listo para analítica y machine learning.

## Alcance

| Nivel | Estado |
|---|---|
| **Nivel 1 — Ingesta** (obligatorio) | Implementado: export incremental por watermark, Auto Loader con checkpoints, evolución de esquema, trazabilidad, Job + Bundle dev/staging/prod. Streaming y SAP como diseño, con muestra de clickstream. |
| **Nivel 2 — Transformación y modelado** (obligatorio) | Implementado: tipado, deduplicación determinista, `MERGE`, SCD2, reglas de calidad como código con cuarentena y corte del pipeline. |
| Nivel 3 — Gold (opcional) | Extensión mínima: modelo dimensional de ventas, sin dashboard. |
| Feature Store, RAG, agentes GenAI | Fuera de alcance. |

Las decisiones y sus alternativas están en [docs/architecture-decisions.md](docs/architecture-decisions.md).

## Arquitectura

```text
Azure SQL Database (6 tablas, updated_at + is_deleted)
    │  sql/04_export_source_to_csv.py
    │  incremental: updated_at > (watermark − lookback) AND updated_at <= high_watermark
    ▼
UC Volume  <catalog>.<prefix>bronze.landing/<tabla>/<tabla>_<run_id>.csv   (+ _manifests/)
    │  Auto Loader (cloudFiles, availableNow) · checkpoint por tabla · addNewColumns
    ▼
Bronze Delta   append-only, todo string, metadatos de linaje → historial completo de versiones
    │  solo filas nuevas (_ingested_at > control.silver_progress)
    │  tipado · dedupe determinista · reglas de calidad
    ├──────────────► control.quarantine_records   (filas con error, JSON original + motivo)
    ▼
Silver Delta   <tabla> (SCD1, MERGE)  +  <tabla>_history (SCD2: customers, products, orders, payments)
    │                                     control.data_quality_results · control.ingestion_watermarks
    ▼
Gold Delta (opcional)  dim_customers, dim_products, dim_date, fact_sales, agg_daily_sales
```

Orquestación: Job `bronze_ingest → silver_transform → gold_publish`, definido en [databricks.yml](databricks.yml) y [resources/andina_lakehouse_job.yml](resources/andina_lakehouse_job.yml).

Diagramas: [arquitectura de ingesta — Nivel 1](diagrams/architecture-level-1.md) · [modelo de datos — Nivel 2](diagrams/data-model-level-2.md) · [streaming y SAP](docs/streaming-and-sap-design.md).

### Por qué archivos y no JDBC directo

Databricks Free Edition (serverless) no pudo completar la sesión JDBC hacia Azure SQL por restricciones de red. El export corre fuera de Databricks y deja archivos en un Volume, que Auto Loader ingiere de forma incremental. Así las credenciales de Azure SQL nunca entran a Databricks. En producción, con red privada, se usaría JDBC/Lakeflow Connect con secret scope y el resto del pipeline no cambia.

## Fuente transaccional y datos sintéticos

Azure SQL con `dbo.Customers`, `dbo.Products`, `dbo.Orders`, `dbo.OrderItems`, `dbo.Payments` y `dbo.SupportTickets` (PK, FK, CHECK, `created_at`, `updated_at`, `is_deleted`, índice en `updated_at`). DDL: [sql/01_create_source_schema.sql](sql/01_create_source_schema.sql).

`sql/02_seed_source_data.py` genera datos reproducibles (semilla fija) con `fast_executemany`:

| Tabla | Filas | Qué incluye |
|---|---|---|
| Customers | 2.000 | 5 países, ciudades ponderadas, segmentos Regular/Premium/Business, emails en mayúsculas o con espacios, algunos inválidos, teléfonos nulos, bajas lógicas |
| Products | 40 | catálogo coherente por categoría, productos descontinuados, aumentos de precio |
| Orders | 12.000 | 540 días con estacionalidad y crecimiento, canal según segmento, todos los estados; 15 pedidos con total distinto a sus líneas (a propósito) |
| OrderItems | ~22.400 | 1 a 5 líneas por pedido, precio vigente a la fecha del pedido |
| Payments | ~13.000 | métodos por país, pendientes, rechazados con reintento aprobado, reembolsos de cancelados, 8 cobros duplicados (a propósito) |
| SupportTickets | 1.500 | asunto y texto coherentes con un pedido o producto real, algunos tickets vacíos |

`sql/05_simulate_source_changes.py` aplica cambios operativos como los de un día real para demostrar la carga incremental: pagos `pending → approved`, rechazos con reintento, reembolsos, pedidos `paid → shipped → delivered`, cancelaciones, clientes `Regular → Premium → Business`, bajas lógicas, altas, cambios de precio y tickets nuevos o resueltos.

## Nivel 1 — Ingesta

### Export incremental (`sql/04_export_source_to_csv.py`)

1. Fija el límite superior `high = SYSUTCDATETIME()` al inicio, para que la ventana no se mueva durante la corrida.
2. Por tabla lee `updated_at > low AND updated_at <= high`, con `low = último watermark − lookback` (10 min por defecto). El lookback cubre transacciones confirmadas tarde. Las filas que repite se resuelven en Silver.
3. Escribe `landing/<tabla>/<tabla>_<run_id>.csv` y un manifiesto `_manifests/<run_id>.json` con rango, filas y archivos.
4. Con `--upload` sube los archivos al Volume (Databricks SDK).
5. El watermark (`data/state/export_watermarks.json`) **solo avanza si todo terminó bien**. Si la corrida falla, la siguiente repite la misma ventana.

`dbo.Payments` se extrae por `updated_at` y no por fecha de creación, porque su estado cambia después de creado el pedido. Cada versión queda en Bronze.

### Bronze (`src/andina_pipeline/bronze.py`, notebook `01_ingest_bronze`)

- **Auto Loader** (`cloudFiles`, CSV) con `trigger(availableNow=True)`: procesa solo los archivos nuevos y termina, ideal para un Job programado en serverless.
- **Checkpoint y schema location** por tabla en el Volume `<prefix>control.checkpoints`: cada archivo se ingiere exactamente una vez, aun con reintentos.
- **Append-only**: no hay `overwrite`. Bronze guarda todas las versiones de cada registro y permite reconstruir Silver.
- **Evolución de esquema**: `schemaEvolutionMode=addNewColumns` + `mergeSchema`. El Job reintenta Bronze una vez, porque Auto Loader se detiene al ver una columna nueva. Los valores que no encajan van a `_rescued_data`. Si falta una columna del contrato, el pipeline falla; si hay una nueva, se reporta y no llega a Silver hasta agregarla al contrato.
- **Linaje**: `_source_system`, `_source_table`, `_source_file`, `_source_file_modified_at`, `_ingestion_run_id`, `_ingested_at`.
- **Control**: `control.ingestion_watermarks` registra por corrida y tabla las filas leídas y el mayor `updated_at` disponible.

### Streaming y SAP (diseño)

Muestra en [data/samples/clickstream_events.jsonl](data/samples/clickstream_events.jsonl): 3.392 eventos `product_view`, `add_to_cart` y `purchase`, con duplicados, eventos tardíos, sesiones anónimas y un campo nuevo. Se regenera con `scripts/generate_clickstream.py`. El diseño con Event Hubs + Structured Streaming y el de SAP ECC con SLT/ODP están en [docs/streaming-and-sap-design.md](docs/streaming-and-sap-design.md).

## Nivel 2 — Transformación y modelado

### Capas

| Capa | Responsabilidad |
|---|---|
| Bronze | Réplica cruda, append-only y trazable de cada versión recibida |
| Silver | Estado actual tipado y validado (SCD1) + historial (SCD2) + cuarentena |
| Gold (opcional) | Modelo dimensional de ventas |

### Silver (`src/andina_pipeline/silver.py`, notebook `02_transform_silver`)

1. **Incremental**: solo filas de Bronze con `_ingested_at` mayor al registrado en `control.silver_progress`.
2. **Contrato y tipado** ([config.py](src/andina_pipeline/config.py)): IDs enteros, montos `DECIMAL(18,2)`, timestamps con o sin milisegundos (`try_to_timestamp`), normalización de textos, emails, códigos de país y dominios.
3. **Deduplicación determinista**: última versión por llave según `updated_at DESC, _ingested_at DESC, _source_file DESC`.
4. **Calidad como código** ([quality.py](src/andina_pipeline/quality.py)), ver abajo.
5. **SCD1**: `MERGE` en `silver.<tabla>` que solo actualiza si la versión entrante no es más vieja (no retrocede con datos tardíos ni con el lookback).
6. **SCD2**: `silver.<tabla>_history` con `valid_from`, `valid_to` e `is_current`. Columnas rastreadas:
   - `customers`: segmento, email, ciudad, país, baja.
   - `payments`: estado, monto, fecha, baja.
   - `orders`: estado, total, baja.
   - `products`: precio, estado, baja.

   Un solo `MERGE` cierra la versión vigente e inserta la nueva. Funciona con varias versiones de la misma llave en un lote y es idempotente ante reprocesos.

Pagos se procesan después de pedidos, y pedidos después de clientes, para que la integridad referencial se valide contra Silver ya actualizado.

### Calidad de datos

| Severidad | Efecto | Ejemplos |
|---|---|---|
| `error` (fila) | a `control.quarantine_records` con el JSON original y los motivos | llave o `updated_at` nulo, fecha imposible de convertir, estado fuera de dominio, cantidad ≤ 0, monto negativo, `line_total ≠ quantity × unit_price`, `payment_date` nulo en pago `approved`/`refunded`, FK huérfana |
| `warn` (fila) | se conserva, marcada en `_dq_warnings` | email inválido, ticket vacío, fecha futura |
| umbral | si los `error` superan `max_error_rate` (5 % en prod) **el lote no se escribe y la tarea falla** | fuente rota o cambio de formato |
| `critical` (tabla) | el Job falla | llave duplicada en Silver, más de una versión vigente en SCD2 |
| `warn` (tabla) | se reporta | total del pedido ≠ suma de líneas, cobros duplicados o mayores al total, pedidos pagados o despachados sin pago aprobado |

`payment_date` es obligatorio solo para pagos `approved`/`refunded`: en `pending`/`rejected` es nulo por diseño. Los productos descontinuados siguen siendo válidos para ventas históricas. Todos los resultados quedan en `control.data_quality_results`.

### Organización y particionamiento

Tablas Delta por capa en Unity Catalog (`<catalog>.<prefix>{bronze,silver,gold,control}`). No hay partición física: con este volumen solo crearía archivos pequeños. En producción se usaría liquid clustering o partición por fecha en hechos grandes, más `OPTIMIZE` o predictive optimization.

### Gold (opcional)

`fact_sales` tiene una fila por línea de pedido y conserva cancelados con `is_revenue = false`. `agg_daily_sales` y el KPI suman solo pedidos no cancelados y sin baja lógica.

## Cómo ejecutarlo

### 1. Requisitos

- Python 3.10+, ODBC Driver 18 for SQL Server, `pip install -r requirements.txt`.
- Azure SQL Database y Databricks (Free Edition sirve) con Unity Catalog.
- Databricks CLI ≥ 0.218 para el Bundle (`databricks auth login --host <workspace-url>`).
- Copiar `.env.example` a `.env` y completar credenciales (no se versiona).

### 2. Fuente (Azure SQL)

```bash
# Ejecutar sql/01_create_source_schema.sql en la base (Query editor del portal o sqlcmd)
python sql/02_seed_source_data.py                      # carga sintética (borra y recarga)
```

### 3. Despliegue del Job (Bundle)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev     # sube notebooks + src/ y crea el Job "[dev <usuario>] andina-lakehouse-dev"
```

| Target | Esquemas | Landing Volume | Schedule |
|---|---|---|---|
| `dev` | `workspace.dev_bronze`, `dev_silver`, ... | `/Volumes/workspace/dev_bronze/landing` | pausado |
| `staging` | `workspace.stg_*` | `/Volumes/workspace/stg_bronze/landing` | pausado |
| `prod` | `workspace.bronze`, ... | `/Volumes/workspace/bronze/landing` | cada hora |

La primera ejecución del Job crea los esquemas y los Volumes (`landing`, `checkpoints`).

### 4. Carga inicial e incremental

```bash
# Carga inicial completa → Volume del target
python sql/04_export_source_to_csv.py --mode full --upload --volume-path /Volumes/workspace/dev_bronze/landing
databricks bundle run -t dev andina_lakehouse_job

# Simular un día de cambios y cargar solo lo que cambió
python sql/05_simulate_source_changes.py
python sql/04_export_source_to_csv.py --upload --volume-path /Volumes/workspace/dev_bronze/landing
databricks bundle run -t dev andina_lakehouse_job
```

Los notebooks `02_transform_silver` y `01_ingest_bronze` muestran al final ejemplos de historial: un cliente que cambió de segmento y un pago `pending → approved`. Para reconstruir todo, usar el parámetro `full_refresh=true` del Job.

> Si antes se cargaron CSV sueltos en la raíz del Volume `landing` (versión anterior), hay que borrarlos. Auto Loader lee `landing/<tabla>/`.

### 5. Ejecución local (sin Databricks)

El mismo paquete `src/andina_pipeline` corre en Spark local con Delta. Cambia Auto Loader por el lector de archivos de Structured Streaming (mismo checkpoint y append):

```bash
python scripts/run_local_pipeline.py --reset            # Bronze → Silver → Gold desde data/staging
python scripts/run_local_pipeline.py                    # incremental: solo archivos nuevos
python scripts/run_local_pipeline.py --inject-bad-rows  # demuestra cuarentena y corte por umbral
python -m pytest -q                                     # 18 pruebas (contrato, Silver, SCD2, Bronze, Gold)
```

Spark local necesita Java 17. Si no hay acceso a Maven, se pueden pasar los JAR de Delta con `DELTA_JARS=/ruta/delta-spark_2.12-3.2.0.jar,/ruta/delta-storage-3.2.0.jar`.

## Evidencia de pruebas locales

Corrida de punta a punta con SQL Server 2022 en Docker (en lugar de Azure SQL; mismo dialecto T-SQL) y Spark 3.5 + Delta 3.2 local:

| Paso | Resultado |
|---|---|
| Seed | 2.000 clientes, 40 productos, 12.000 pedidos, 22.409 líneas, 12.974 pagos, 1.500 tickets |
| Export full + pipeline `--reset` | Bronze = Silver = filas fuente. Hallazgos `warn` esperados: 3 emails inválidos, 2 tickets vacíos, 15 pedidos con total ≠ líneas, 8 cobros duplicados |
| Simulación + export incremental | solo cambios: 49 clientes, 4 productos, 268 pedidos, 275 líneas, 164 pagos, 36 tickets |
| Pipeline incremental | Bronze agrega solo esos archivos (checkpoint). Silver procesa solo esas filas: +48 versiones SCD2 de clientes, +237 de pedidos, +163 de pagos. Las tablas actuales siguen con una fila por llave |
| `--inject-bad-rows` (umbral 5 %) | `DataQualityError: orders: 3/3 filas con errores (100.0%) superan el umbral` → no se escribe nada |
| Reintento con `--max-error-rate 1.0` | las 3 filas van a `control.quarantine_records` con sus motivos: total negativo, cliente huérfano, fecha imposible de convertir y estado fuera de dominio |
| `pytest` | 18 pruebas pasan |

Ejemplo de historial en `silver.payments_history`: un pago con estado `approved` y luego `refunded`, con `valid_to` de la primera versión igual a `valid_from` de la segunda. En `silver.customers_history`: clientes `Regular → Premium` y `Premium → Business`.

En Spark local, el metastore Derby muestra un `ERROR HiveAlterHandler` al reescribir las tablas Gold. Es ruido del metastore local: las tablas Delta se escriben bien y en Databricks (Unity Catalog) no ocurre.

**No probado en este entorno:** Auto Loader (`cloudFiles` solo existe en Databricks) y el despliegue del Bundle contra un workspace real. El YAML se validó contra el JSON schema oficial de la Databricks CLI.

## Estructura del repositorio

```text
sql/                    DDL, seed sintético, simulación de cambios, export incremental, conexión
src/andina_pipeline/    config (contrato), bronze, silver, quality, gold, pipeline, spark_session
notebooks/              notebooks delgados del Job (importan src/)
resources/              definición del Job del Bundle
databricks.yml          Bundle con targets dev / staging / prod
scripts/                ejecución local y generador de clickstream
data/samples/           clickstream_events.jsonl
tests/                  pytest con Spark + Delta local
diagrams/, docs/        arquitectura, modelo, ADRs, streaming/SAP, supuestos
```

## Supuestos y limitaciones

Supuestos completos en [docs/assumptions.md](docs/assumptions.md). Principales limitaciones:

- El export corre fuera de Databricks por la restricción de red de Free Edition. En producción se programaría (ADF, cron o un Job con conectividad privada).
- El watermark por `updated_at` no detecta borrados físicos. La fuente usa bajas lógicas; para borrados físicos haría falta Change Tracking o CDC.
- La historia SCD2 empieza en la primera carga. Una versión que llega más vieja que la vigente queda en Bronze, pero no se reinserta en el historial.
- En Free Edition solo existe el catálogo `workspace`, por eso los entornos se separan con prefijo de esquema.

## Uso de IA

El código del pipeline, los notebooks, las pruebas y gran parte de la documentación fueron generados con asistencia de IA. Yo configuré la infraestructura en Azure —incluyendo acceso de red y los identificadores de conexión—, desplegué y ejecuté la solución en Databricks, y revisé los resultados.

En una segunda iteración usé un agente de IA (Devin) para revisar la entrega contra el enunciado y completar los niveles 1 y 2: carga incremental, Auto Loader, SCD2, calidad con cuarentena, Bundle, clickstream y pruebas. Lo validó localmente con SQL Server en Docker y Spark + Delta.

Esta entrega refleja una solución construida con apoyo de IA, que posteriormente configuré, ejecuté y comprendí para poder explicarla.

## Próximos pasos

- Conectividad privada y JDBC o Lakeflow Connect directo, con secret scope, eliminando el paso de archivos.
- Alertas del Job (email o Slack) y un dashboard sobre `control.data_quality_results`.
- Implementar el streaming de clickstream y la integración SAP descritos.
- Dashboard de KPIs sobre Gold.
