# Andina Market AI — Reto Técnico Data Engineer

Proyecto de plataforma de datos para Andina Market, un e-commerce ficticio de supermercado que opera en Latinoamérica mediante canales web, app móvil y tiendas físicas.

El objetivo es llevar datos transaccionales desde Azure SQL Database a Databricks, aplicar controles de calidad y dejar un modelo de datos listo para analítica y machine learning.

## Nivel alcanzado

El alcance principal implementado corresponde a los niveles núcleo del reto:

- **Nivel 1 — Ingesta de datos**
- **Nivel 2 — Transformación y modelado**

Como extensión técnica mínima se materializó una capa Gold de ventas, pero no se implementó dashboard ni los niveles opcionales de Feature Store, RAG o agentes GenAI. Se priorizó profundidad, trazabilidad, calidad de datos y capacidad de sustentar cada decisión.

## Arquitectura

La solución usa una arquitectura medallion con Unity Catalog y Delta Lake:

```text
Azure SQL Database
    ↓
Exportación batch controlada a CSV
    ↓
Unity Catalog Volume
    ↓
Bronze Delta: réplica cruda y trazable
    ↓
Silver Delta: datos tipados, limpios y validados
    ↓
Gold Delta: modelo dimensional mínimo de ventas
```

Diagramas:

- [Arquitectura de ingesta — Nivel 1](diagrams/architecture-level-1.md)
- [Modelo de datos resultante — Nivel 2](diagrams/data-model-level-2.md)

## Fuente transaccional

La fuente de verdad es Azure SQL Database. Se desplegó una base de datos propia y se pobló directamente mediante Python y `pyodbc` con datos sintéticos coherentes.

Las tablas fuente son:

| Tabla Azure SQL | Contenido |
|---|---|
| `dbo.Customers` | Datos de clientes y segmentación |
| `dbo.Products` | Catálogo de productos |
| `dbo.Orders` | Pedidos y su estado |
| `dbo.OrderItems` | Líneas de cada pedido |
| `dbo.Payments` | Pagos, método, monto y estado |
| `dbo.SupportTickets` | Tickets de soporte con texto libre |

## Nivel 1 — Ingesta

### Implementación realizada

La demostración realiza una carga inicial controlada:

1. Un script Python consulta Azure SQL mediante `pyodbc`.
2. El script exporta cada tabla fuente a un CSV temporal en `data/staging/`.
3. Los archivos se cargan a `workspace.bronze.landing`, un Unity Catalog Volume.
4. El notebook `01_ingest_bronze` escribe tablas Delta en `workspace.bronze`.
5. Cada ejecución registra su estado en `workspace.control.ingestion_watermarks`.

Las tablas Bronze creadas son:

- `workspace.bronze.customers`
- `workspace.bronze.products`
- `workspace.bronze.orders`
- `workspace.bronze.order_items`
- `workspace.bronze.payments`
- `workspace.bronze.support_tickets`

Bronze conserva los campos de origen como texto y añade los siguientes metadatos:

- `_source_system`
- `_source_table`
- `_source_file`
- `_ingestion_run_id`
- `_ingested_at`

### Estrategia incremental de producción

La primera carga es completa. Para producción, las cargas posteriores usarían `updated_at` como watermark, con límite inferior y superior para evitar perder cambios durante la ejecución:

```sql
WHERE updated_at > :last_successful_watermark
  AND updated_at <= :current_upper_bound
```

`dbo.Payments` usa `updated_at`, no solamente la fecha de creación, porque el estado de un pago puede cambiar después de que se crea un pedido.

La frecuencia inicial propuesta es cada hora para entidades transaccionales. La frecuencia se ajustaría según el SLA, volumen y costo de cómputo.

### Trazabilidad, idempotencia y fallos

La tabla `workspace.control.ingestion_watermarks` almacena tabla fuente, watermark, ID de ejecución, filas leídas, estado y timestamp.

La demo usa `overwrite` para producir snapshots completos e idempotentes: ejecutar el notebook nuevamente reemplaza el snapshot Bronze en lugar de duplicar registros. En producción se usaría `MERGE` sobre Delta Lake, con una llave de negocio y watermark, para aplicar inserciones y actualizaciones incrementales sin duplicación.

En producción, un Job/Workflow ejecutaría los notebooks en orden, con reintentos, alertas ante fallo y actualización del watermark únicamente después de una ejecución exitosa.

### Evolución de esquema

En la demo, Bronze conserva datos crudos y la estructura se sobrescribe de forma controlada debido al volumen reducido. En producción:

- Se compararía el schema entrante con el schema Delta esperado antes de transformar.
- Las columnas nuevas compatibles se permitirían en Bronze y se registrarían en auditoría.
- Cambios de tipo, eliminación o renombrado de columnas se enviarían a revisión antes de afectar Silver.
- Las transformaciones Silver seleccionarían explícitamente las columnas esperadas para aislar a consumidores de cambios no validados.

### Restricción del entorno

Azure SQL fue desplegada, poblada y validada como fuente real. Se intentó conectividad JDBC directa desde Databricks Free Edition, pero el compute serverless no completó la sesión JDBC por restricciones de red/egreso.

Para no guardar credenciales dentro de notebooks y mantener la demostración reproducible, se implementó una exportación batch controlada mediante Python y `pyodbc` hacia CSV temporales. Los CSV se cargan en un Unity Catalog Volume y luego se convierten a tablas Delta Bronze.

La arquitectura objetivo de producción mantiene JDBC incremental directo, secretos en Azure Key Vault o secret scope y conectividad privada o con IPs de salida permitidas.

### Diseño propuesto para streaming

El streaming no se implementó, conforme al alcance solicitado. Para eventos de clickstream de la app móvil se propone:

```text
App móvil → Azure Event Hubs o Kafka → Databricks Structured Streaming
→ Bronze Delta → Silver → BI / ML
```

Cada evento tendría `event_id`, `event_time`, `customer_id`, `session_id`, `event_type`, `product_id` y `channel`.

- La deduplicación se realizaría por `event_id`.
- Se usaría watermark sobre `event_time` para gestionar eventos tardíos.
- Los checkpoints persistentes permitirían recuperarse de fallos sin reprocesar eventos confirmados.
- El trigger inicial sería cada minuto, ajustable al SLA.

### Diseño propuesto para SAP ECC

SAP ECC on-premise no se implementó porque el reto solicita únicamente diseño. La propuesta es:

```text
SAP ECC → SAP SLT / SAP Data Services / middleware
→ Azure Data Lake Storage Gen2 o Azure Event Hubs
→ Databricks Bronze → Silver → consumo analítico / ML
```

La conectividad usaría VPN o ExpressRoute, identidad administrada, Azure Key Vault y permisos de Unity Catalog. Se conservarían claves de negocio, timestamps de cambio y metadatos de origen.

## Nivel 2 — Transformación y modelado

### Responsabilidad de las capas

| Capa | Responsabilidad |
|---|---|
| Bronze | Conserva la réplica cruda y trazable de la fuente |
| Silver | Aplica tipos, normalización, deduplicación y reglas de calidad |
| Gold | Publica un modelo dimensional mínimo de ventas para consumo analítico |

### Capa Silver

El notebook `02_transform_silver` crea estas tablas Delta:

- `workspace.silver.customers`
- `workspace.silver.products`
- `workspace.silver.orders`
- `workspace.silver.order_items`
- `workspace.silver.payments`
- `workspace.silver.support_tickets`

Transformaciones implementadas:

- Conversión de IDs a tipos numéricos.
- Conversión de fechas y timestamps.
- Conversión de montos a `DECIMAL(18,2)`.
- Normalización de textos, correos, estados, prioridades, métodos de pago y canales.
- Deduplicación por llave primaria.
- Conservación de `is_deleted` para borrado lógico.
- Preservación de metadatos de origen y creación de `_transformation_run_id` y `_transformed_at`.

### Calidad de datos

Los resultados se guardan en `workspace.control.data_quality_results`.

Validaciones implementadas:

- Valores nulos en campos obligatorios.
- Unicidad de llaves primarias.
- Integridad referencial:
  - `orders.customer_id → customers.customer_id`
  - `order_items.order_id → orders.order_id`
  - `order_items.product_id → products.product_id`
  - `payments.order_id → orders.order_id`
  - `support_tickets.customer_id → customers.customer_id`

La integridad de productos valida existencia histórica del `product_id`; un producto `discontinued` puede seguir asociado a pedidos anteriores y no constituye una referencia inválida.

### Cambios en el tiempo

La demostración utiliza SCD Tipo 1 en Silver: la tabla representa el estado más reciente de cada entidad por su llave de negocio, y el campo `updated_at` permite detectar modificaciones.

Para producción, los atributos históricos relevantes —por ejemplo, el segmento del cliente— se modelarían con SCD Tipo 2 en una dimensión histórica. Esto agregaría una surrogate key, `effective_from`, `effective_to` e `is_current`, permitiendo consultar el valor vigente en cada momento.

Los pagos se capturan con `updated_at` porque su estado puede cambiar posteriormente. El borrado lógico se conserva mediante `is_deleted`.

### Organización y particionamiento

Las tablas se almacenan en Delta Lake y se organizan por capa dentro de Unity Catalog.

No se aplicó particionamiento físico debido al volumen sintético reducido: particionar tablas pequeñas agrega archivos y complejidad sin mejorar rendimiento. En producción, las tablas grandes de hechos se particionarían por fecha (`order_date` o `payment_date`) y se evaluaría `OPTIMIZE` / Z-Ordering según los patrones reales de consulta.

### Extensión Gold

Como extensión mínima se crearon:

- `workspace.gold.dim_customers`
- `workspace.gold.dim_products`
- `workspace.gold.dim_date`
- `workspace.gold.fact_sales`
- `workspace.gold.agg_daily_sales`

`fact_sales` tiene una fila por línea de pedido. Se conecta conceptualmente con clientes, productos y fechas. `agg_daily_sales` agrega pedidos, unidades e ingresos por fecha, categoría y canal.

Esta extensión demuestra cómo los datos Silver pueden materializarse para análisis sin reemplazar el modelo operacional validado de Silver.

## Reproducibilidad

### Requisitos

- Azure SQL Database.
- Python 3.10 o superior.
- ODBC Driver 18 for SQL Server.
- Dependencias Python del proyecto.
- Databricks Free Edition con Unity Catalog habilitado.

### Variables de entorno

Crear un archivo `.env` en la raíz del proyecto, sin versionarlo:

```env
AZURE_SQL_SERVER=tu-servidor.database.windows.net
AZURE_SQL_DATABASE=tu-base-de-datos
AZURE_SQL_USERNAME=tu-usuario
AZURE_SQL_PASSWORD=tu-password
```

### Ejecución

1. Crear las tablas fuente ejecutando los scripts DDL de `sql/`.
2. Generar e insertar los datos sintéticos directamente en Azure SQL.
3. Exportar la fuente a staging:

```bash
python sql/04_export_source_to_csv.py
```

4. Cargar los seis CSV de `data/staging/` al Volume `workspace.bronze.landing`.
5. Ejecutar, en este orden, los notebooks:

   - `01_ingest_bronze.py`
   - `02_transform_silver.py`
   - `03_publish_gold.py` (extensión no obligatoria)

## Estructura del repositorio

```text
sql/                  DDL, generación de datos y exportación Azure SQL
notebooks/            Notebooks de ingesta y transformación
diagrams/             Diagramas de arquitectura y modelo de datos
docs/                 Decisiones, evidencias y material de sustentación
data/staging/         Archivos temporales locales, excluidos de Git
README.md             Documentación principal
```

## Supuestos y limitaciones

- Los datos son sintéticos y se generaron de forma coherente con las relaciones del modelo.
- La demostración usa una carga batch manual controlada por las restricciones de red de Databricks Free Edition.
- La muestra de datos no tiene volumen suficiente para justificar particionamiento físico.
- Azure SQL y Databricks Free Edition se usaron para controlar costos del entorno.
- La operación productiva requeriría secretos gestionados, conectividad privada, un Job/Workflow, alertas y carga incremental mediante `MERGE`.
- El archivo `.env` y los CSV de staging no se versionan porque pueden contener credenciales o datos temporales.

## Uso de IA

El código del pipeline, los notebooks, las pruebas y gran parte de la documentación fueron generados con asistencia de IA. Yo configuré la infraestructura en Azure —incluyendo acceso de red y los identificadores de conexión—, desplegué y ejecuté la solución en Databricks, y revisé los resultados.

Esta entrega refleja una solución construida con apoyo de IA, que posteriormente configuré, ejecuté y comprendí para poder explicarla.

## Próximos pasos

- Orquestar los notebooks con Databricks Jobs/Workflows.
- Implementar JDBC incremental directo con secretos gestionados y conectividad segura.
- Implementar SCD Tipo 2 para atributos de cliente que requieran historial.
- Añadir monitoreo, alertas y manejo de errores centralizado.
- Construir un dashboard con KPIs de ingresos, pedidos, ticket promedio y ventas por canal.
- Extender la solución a Feature Store, RAG y agentes GenAI cuando el alcance y el volumen lo justifiquen.