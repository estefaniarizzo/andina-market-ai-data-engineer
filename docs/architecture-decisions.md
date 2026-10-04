# Decisiones de arquitectura (ADR)

Formato corto: contexto → decisión → alternativas descartadas → consecuencias.

## ADR-01. Exportación incremental a archivos en vez de JDBC directo

- **Contexto:** Databricks Free Edition (solo serverless) no pudo abrir la sesión JDBC hacia Azure SQL por restricciones de red/egreso.
- **Decisión:** un exportador Python (`sql/04_export_source_to_csv.py`) consulta Azure SQL por `updated_at` y deja un CSV por tabla y por ejecución en el Volume `landing/<tabla>/`, más un manifiesto JSON con el rango de watermarks.
- **Alternativas:** JDBC directo desde Databricks (bloqueado en el entorno; es la opción objetivo en producción con Private Endpoint y secret scope); Azure Data Factory / Lakeflow Connect (servicio adicional y costo, sin aporte para el reto).
- **Consecuencias:** las credenciales nunca entran a Databricks; el exportador corre fuera de Databricks y necesita un programador (cron, ADF o un Job con conectividad). La ventana es reproducible porque el límite superior se fija al inicio.

## ADR-02. Watermark por `updated_at` con límite superior y lookback

- **Decisión:** `updated_at > (último_watermark − lookback) AND updated_at <= SYSUTCDATETIME()_inicio`. Lookback por defecto de 10 minutos. El estado avanza solo si se escribieron (y subieron) todas las tablas.
- **Por qué el lookback:** una transacción larga puede confirmar filas con un `updated_at` anterior al límite superior de la corrida previa; sin margen se perderían.
- **Consecuencias:** el lookback reexporta algunas filas. No es un problema porque Bronze es append-only y Silver deduplica por `(llave, updated_at)` y hace `MERGE` solo si la versión entrante es más nueva o igual.
- **Limitación conocida:** las bajas físicas no se detectan con watermark; la fuente usa bajas lógicas (`is_deleted`). Para bajas físicas haría falta CDC (Change Tracking/CDC de SQL Server).

## ADR-03. Bronze con Auto Loader, append-only y checkpoint

- **Decisión:** Auto Loader (`cloudFiles`) con `trigger(availableNow=True)`, checkpoint y schema location por tabla en el Volume `control.checkpoints`. Todas las columnas como texto. Solo `append`.
- **Alternativas:** `COPY INTO` (idempotente, pero menos control de evolución de esquema y sin `_rescued_data` integrado); `spark.read` + `overwrite` (versión original: perdía la historia de cambios y reprocesaba todo).
- **Consecuencias:** cada archivo se procesa exactamente una vez; Bronze conserva todas las versiones de cada registro (p. ej. un pago `pending` y luego `approved`), lo que permite reconstruir Silver desde cero.

## ADR-04. Evolución de esquema controlada

- **Decisión:** `schemaEvolutionMode=addNewColumns` + `mergeSchema` en Bronze; el Job reintenta una vez la tarea Bronze porque Auto Loader se detiene al detectar una columna nueva y continúa al reiniciar. Los valores que no encajan van a `_rescued_data`.
- **Silver aislado:** Silver selecciona explícitamente las columnas del contrato (`config.py`). Una columna nueva aparece en Bronze pero no llega a Silver hasta que se agrega al contrato. Si falta una columna del contrato, `run_bronze` falla (`contract_check`).
- **Alternativa descartada:** `overwriteSchema=true` (versión original): aceptaba cualquier cambio sin control.

## ADR-05. Silver incremental por `_ingested_at` y `MERGE` (SCD1)

- **Decisión:** Silver procesa solo las filas de Bronze con `_ingested_at` mayor al último valor guardado en `control.silver_progress`. Luego: tipado, deduplicación determinista, reglas de calidad y `MERGE` con condición `s.updated_at >= t.updated_at`.
- **Alternativas:** `foreachBatch` sobre un stream de Bronze (válido, pero en serverless/Spark Connect la función se serializa y depende de que el paquete sea importable en el servidor); Change Data Feed (requiere habilitarlo en cada tabla). Se eligió lo más simple y explicable.
- **Idempotencia:** si una ejecución falla después del `MERGE` pero antes de registrar el progreso, la siguiente reprocesa el lote sin efectos: el `MERGE` no retrocede versiones y SCD2 ignora versiones no más nuevas que la vigente.
- **Deduplicación determinista:** `row_number()` por llave ordenado por `updated_at DESC, _ingested_at DESC, _source_file DESC`. La versión original usaba `dropDuplicates`, que no garantiza cuál fila sobrevive.

## ADR-06. Historial con SCD Tipo 2

- **Decisión:** tablas `silver.<entidad>_history` con `valid_from`, `valid_to`, `is_current` y `_row_hash` de las columnas rastreadas:
  - `customers`: `segment`, `email`, `city`, `country`, `is_deleted` (el reto pregunta explícitamente por el cambio de segmento).
  - `payments`: `payment_status`, `amount`, `payment_date`, `is_deleted` (el reto advierte que el estado del pago cambia después).
  - `orders`: `order_status`, `order_total`, `is_deleted`; `products`: `unit_price`, `product_status`, `is_deleted`.
- **Implementación:** un único `MERGE` atómico (cierra la versión vigente e inserta las nuevas). Soporta varias versiones de la misma llave en un lote y colapsa versiones consecutivas sin cambios en columnas rastreadas.
- **Consecuencias:** la tabla actual (SCD1) sirve para operación y la histórica para análisis "as-of" (p. ej. ventas por el segmento que tenía el cliente el día de la compra). La historia empieza en la primera carga: los cambios anteriores a la primera exportación no existen en la fuente.
- **Limitación:** una versión que llega más vieja que la vigente se ignora en SCD2 (queda en Bronze). Con CDC real se podría reinsertar en su posición.

## ADR-07. Calidad de datos como código, cuarentena y corte del pipeline

- **Decisión:** reglas declarativas en `quality.py` (expresión SQL + severidad):
  - `error` → la fila va a `control.quarantine_records` con el registro original en JSON y el motivo; no llega a Silver.
  - `warn` → la fila se conserva y se marca en `_dq_warnings` (p. ej. email inválido, ticket vacío).
  - Umbral `max_error_rate` (5 % en prod): si se supera, el lote no se escribe y la tarea falla.
  - Checks de tabla `critical` (llave duplicada, más de una versión vigente en SCD2) detienen el Job después de escribir.
- **Reglas condicionales:** `payment_date` solo es obligatorio para pagos `approved`/`refunded`. Antes era obligatorio siempre y daba falsos positivos en pagos pendientes o rechazados.
- **Reglas de negocio de tabla (warn):** total del pedido contra suma de líneas, cobros duplicados o mayores al total, pedidos despachados sin pago aprobado.
- **Operación:** si un lote se detiene por el umbral, el responsable revisa la causa. Puede corregir la fuente y reexportar, o reejecutar con un umbral mayor para enviar esas filas a cuarentena de forma consciente.

## ADR-08. Orquestación con Databricks Asset Bundle

- **Decisión:** `databricks.yml` + `resources/andina_lakehouse_job.yml`: Job `bronze_ingest → silver_transform → gold_publish`, horario (pausado fuera de prod), `max_concurrent_runs: 1`, reintento en Bronze y parámetros `catalog`, `schema_prefix`, `max_error_rate` y `full_refresh`.
- **Entornos:** `dev` (modo development, prefijo `dev_`), `staging` (`stg_`), `prod` (sin prefijo, schedule activo, `run_as` de service principal recomendado). En Free Edition solo existe el catálogo `workspace`, por eso los entornos se aíslan con prefijo de esquema. Con más catálogos, se cambia la variable `catalog` por target.
- **Código compartido:** los notebooks son delgados e importan `src/andina_pipeline`, que también se ejecuta localmente (`scripts/run_local_pipeline.py`) y en `pytest`.

## ADR-09. Sin particionamiento físico

Con unas 25 mil líneas de pedido, particionar crea archivos pequeños sin beneficio. En producción: liquid clustering o partición por fecha en hechos grandes (`order_date`), `OPTIMIZE` programado y predictive optimization en Unity Catalog.

## ADR-10. Gold opcional, ingresos sin cancelados

Gold se mantiene como extensión. `agg_daily_sales` y el KPI solo suman líneas con `is_revenue = true` (pedido no cancelado y sin baja lógica). `fact_sales` conserva los cancelados para análisis de cancelaciones.
