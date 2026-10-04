# Modelo de datos resultante — Nivel 2

Este diagrama representa el modelo relacional normalizado disponible en la capa Silver. La capa Silver contiene datos tipados, normalizados, deduplicados y validados antes de su consumo analítico o de machine learning.

```text
                                  ┌───────────────────────────┐
                                  │ silver.customers          │
                                  │ PK customer_id            │
                                  │ first_name, last_name     │
                                  │ email, city, country      │
                                  │ segment, signup_date      │
                                  │ is_deleted                │
                                  └─────────────┬─────────────┘
                                                │ 1
                           ┌────────────────────┴────────────────────┐
                           │                                         │
                           │ 1                                       │ 1
                           ▼                                         ▼
            ┌───────────────────────────┐             ┌───────────────────────────┐
            │ silver.orders             │             │ silver.support_tickets    │
            │ PK order_id               │             │ PK ticket_id              │
            │ FK customer_id            │             │ FK customer_id            │
            │ order_date                │             │ subject, ticket_body      │
            │ sales_channel             │             │ ticket_status, priority   │
            │ order_status, order_total │             │ created_at, updated_at    │
            └─────────────┬─────────────┘             └───────────────────────────┘
                          │ 1
             ┌────────────┴─────────────┐
             │                          │
             │ 1                        │ 1
             ▼                          ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│ silver.order_items        │   │ silver.payments           │
│ PK order_item_id          │   │ PK payment_id             │
│ FK order_id               │   │ FK order_id               │
│ FK product_id             │   │ payment_method            │
│ quantity, unit_price      │   │ payment_status, amount    │
│ line_total                │   │ payment_date              │
└─────────────┬─────────────┘   └───────────────────────────┘
              │ N
              │
              │ 1
              ▼
┌───────────────────────────┐
│ silver.products           │
│ PK product_id             │
│ sku, product_name         │
│ category, unit_price      │
│ product_status            │
│ is_deleted                │
└───────────────────────────┘
```

## Tablas de historial (SCD2) y control

```text
silver.customers_history   customer_id, segment, email, city, country, is_deleted
silver.products_history    product_id, unit_price, product_status, is_deleted
silver.orders_history      order_id, order_status, order_total, is_deleted
silver.payments_history    payment_id, payment_status, amount, payment_date, is_deleted
                           + valid_from, valid_to, is_current, _row_hash, _silver_run_id

control.quarantine_records   entity, record_key, errors[], raw_record (JSON), quality_run_id
control.data_quality_results table_name, check_name, severity, failed_rows, total_rows, check_status
control.silver_progress      entity, last_bronze_ingested_at, rows_processed, rows_quarantined
```

Las tablas actuales (`silver.<tabla>`) son SCD1 con `MERGE`. Las tablas `_history` permiten consultas "as-of", por ejemplo el segmento que tenía el cliente el día del pedido o la secuencia de estados de un pago.

## Relaciones validadas

- `orders.customer_id → customers.customer_id`
- `support_tickets.customer_id → customers.customer_id`
- `order_items.order_id → orders.order_id`
- `order_items.product_id → products.product_id`
- `payments.order_id → orders.order_id`

## Decisiones de modelado

- La capa Silver conserva el modelo operacional relacional porque facilita validación, trazabilidad y consumo posterior para analítica y machine learning.
- Se conservan los IDs de negocio como llaves primarias; no se crearon surrogate keys en Silver.
- Las columnas `is_deleted` se conservan para representar eliminaciones lógicas sin perder historial.
- Las columnas `created_at` y `updated_at` permiten la carga incremental y ordenan las versiones para deduplicar y construir SCD2.
- `Payments` se procesa por `updated_at`, porque su estado puede cambiar después de la creación del pedido.
- Los productos con estado `discontinued` siguen siendo válidos para ventas históricas; esa condición no invalida la integridad referencial.

## Extensión analítica

Como extensión mínima, el proyecto materializa `workspace.gold.dim_customers`, `workspace.gold.dim_products`, `workspace.gold.dim_date`, `workspace.gold.fact_sales` y `workspace.gold.agg_daily_sales`. Estas tablas no sustituyen el modelo Silver: están optimizadas para consultas analíticas.