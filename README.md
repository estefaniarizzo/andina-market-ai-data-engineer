# Andina Market AI — Reto Data Engineer

Plataforma de datos para Andina Market, una empresa colombiana de e-commerce de supermercado. La solución ingiere datos desde Azure SQL Database hacia Databricks usando una arquitectura medallion y prepara las capas Bronze, Silver y Gold para analítica, BI y machine learning.

## Alcance completado

### Nivel 1 — Ingesta

- Azure SQL Database desplegada y poblada como fuente transaccional.
- Ingesta inicial de seis tablas fuente: `Customers`, `Products`, `Orders`, `OrderItems`, `Payments` y `SupportTickets`.
- Ingesta batch implementada desde Azure SQL hacia Databricks Bronze.
- Tablas Delta creadas en Unity Catalog bajo `workspace.bronze`.
- Metadatos de ingesta y trazabilidad por ejecución implementados.
- Tabla de control y watermarks creada en `workspace.control.ingestion_watermarks`.
- Arquitectura documentada, incluyendo ingesta incremental, streaming e integración con SAP.

## Arquitectura

```text
Azure SQL Database
        ↓
Exportación batch controlada / diseño JDBC incremental
        ↓
Databricks Unity Catalog
        ↓
Tablas Delta Bronze
        ↓
Silver: validación y estandarización
        ↓
Gold: modelo dimensional
        ↓
BI / ML
```

## Tablas Bronze

| Tabla Bronze | Tabla fuente |
|---|---|
| `workspace.bronze.customers` | `dbo.Customers` |
| `workspace.bronze.products` | `dbo.Products` |
| `workspace.bronze.orders` | `dbo.Orders` |
| `workspace.bronze.order_items` | `dbo.OrderItems` |
| `workspace.bronze.payments` | `dbo.Payments` |
| `workspace.bronze.support_tickets` | `dbo.SupportTickets` |

Cada tabla Bronze conserva los campos crudos de la fuente y añade metadatos de ingesta:

- `_source_system`
- `_source_table`
- `_source_file`
- `_ingestion_run_id`
- `_ingested_at`

## Estrategia de ingesta incremental

La estrategia de producción utiliza `updated_at` como watermark incremental:

```sql
WHERE updated_at > :last_successful_watermark
  AND updated_at <= :current_upper_bound
```

La tabla `dbo.Payments` se extrae usando `updated_at` porque el estado de un pago puede cambiar después de su creación. Las eliminaciones lógicas se representan mediante `is_deleted`, de modo que las bajas viajan correctamente en cargas incrementales.

La tabla `workspace.control.ingestion_watermarks` registra:

- tabla fuente
- último watermark exitoso
- ID de ejecución de ingesta
- filas leídas
- estado de la ejecución
- fecha y hora de actualización

## Limitación del entorno y estrategia de demo

Azure SQL fue desplegada, poblada y validada. La conectividad JDBC desde Databricks Free Edition alcanzó el servidor y la base de datos, pero el compute serverless no completó la sesión JDBC debido a las restricciones de red/egreso del entorno gratuito.

Para evitar incluir credenciales en los notebooks y lograr una demo reproducible, la demostración implementada usa una exportación batch controlada desde Azure SQL mediante Python y `pyodbc` hacia archivos CSV temporales. Estos archivos se suben a un Unity Catalog Volume y se cargan en tablas Delta Bronze.

La arquitectura objetivo de producción sigue siendo ingesta incremental directa por JDBC desde Azure SQL, con credenciales almacenadas en un secret scope o Azure Key Vault, extracción por watermark, reintentos y conectividad privada o con IPs de salida permitidas.

## Diseño de streaming

Para eventos de clickstream desde la app móvil, la arquitectura de producción sería:

```text
App móvil
  → Azure Event Hubs / Kafka
  → Databricks Structured Streaming
  → Bronze Delta
  → Silver: sesionización y validación
  → Gold: métricas y features de ML
```

Cada evento incluiría un `event_id` único, `event_time`, `customer_id`, `session_id`, `event_type`, `product_id` y `channel`. La deduplicación usaría `event_id`; los eventos tardíos se manejarían con watermarks y checkpoints.

## Diseño de integración SAP ECC

Para SAP ECC on-premise, el patrón recomendado es una replicación desacoplada:

```text
SAP ECC
  → SAP SLT / middleware de integración
  → Azure Data Lake Storage / Event Hubs
  → Databricks Bronze
  → Silver y Gold
```

La conectividad usaría VPN o ExpressRoute, identidad administrada, secretos en Azure Key Vault y permisos de Unity Catalog. Los datos de proveedores y órdenes de compra conservarían claves de negocio, timestamps de cambio y metadatos de origen para trazabilidad.

## Estructura del repositorio

```text
sql/                  Scripts DDL, generación de datos y exportación desde Azure SQL
notebooks/            Notebooks de ingesta y transformación en Databricks
docs/                 Decisiones de arquitectura y diseño
diagrams/             Diagramas de arquitectura
data/staging/         Archivos temporales locales; no se versionan
```
## Capa Silver

La capa Silver estandariza y valida los datos ingeridos desde Bronze. Las tablas se almacenan bajo `workspace.silver`:

- `workspace.silver.customers`
- `workspace.silver.products`
- `workspace.silver.orders`
- `workspace.silver.order_items`
- `workspace.silver.payments`
- `workspace.silver.support_tickets`

Las transformaciones implementadas incluyen:

- Conversión de identificadores a tipos numéricos.
- Conversión de fechas y timestamps a sus tipos correspondientes.
- Conversión de valores monetarios a `DECIMAL(18,2)`.
- Normalización de estados, prioridades, métodos de pago y canales de venta.
- Eliminación de espacios en campos de texto y normalización de correos electrónicos a minúsculas.
- Deduplicación de registros por llave primaria.
- Preservación de los metadatos de origen y adición de metadatos de transformación.

Cada tabla Silver incluye:

- `_source_system`
- `_source_table`
- `_transformation_run_id`
- `_transformed_at`

Los resultados de calidad se registran en `workspace.control.data_quality_results`. Esta tabla incluye validaciones de unicidad de llave primaria y valores nulos en campos obligatorios, junto con el número de filas evaluadas, filas fallidas, estado de la validación y fecha de ejecución.

## Próximos pasos

- Construir la capa Gold con tablas dimensionales y de hechos.
- Implementar métricas de negocio para BI, como ingresos, ticket promedio y tasa de conversión.
- Crear features para modelos de ML, incluyendo churn, valor del cliente y recomendaciones.
- Añadir orquestación, reintentos y alertas para las ejecuciones de ingesta y transformación.
- Implementar ingesta incremental JDBC con watermarks y secretos gestionados.