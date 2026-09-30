# Andina Market — AI Data Engineer Technical Challenge

Implementación de los niveles 1 y 2 del reto técnico de **Talento Para Ti**.

## Alcance

Este repositorio cubre:

- Nivel 1: ingesta desde Azure SQL Database a Databricks.
- Nivel 2: transformación, calidad y modelado de datos.
- Diseño documentado para streaming y SAP on-premise.

No se implementan en esta entrega los niveles opcionales de visualización, Feature Store, RAG ni agentes GenAI.

## Arquitectura objetivo

```text
Azure SQL Database
        |
        | JDBC + carga inicial / incremental por updated_at
        v
Databricks Bronze (Delta)
        |
        | tipado, deduplicación, calidad e integridad referencial
        v
Databricks Silver (Delta)
        |
        | modelo dimensional
        v
Databricks Gold (Delta)
```

## Decisiones iniciales

- **Fuente:** Azure SQL Database representa el sistema transaccional de Andina Market.
- **Formato:** Delta Lake para soportar transacciones, `MERGE`, versionado e idempotencia.
- **Incrementalidad:** la primera ejecución será completa; las siguientes utilizarán `updated_at` como watermark.
- **Eliminaciones:** se representan mediante la columna `is_deleted` para capturar bajas lógicas.
- **Trazabilidad:** las tablas Bronze incluirán metadatos de fuente, fecha de ingesta y ejecución.
- **Calidad:** los registros inválidos se separarán de los registros válidos y conservarán la razón del rechazo.
- **Histórico:** los cambios relevantes de clientes se manejarán con SCD Tipo 2 en la capa Gold.

## Estructura

```text
.
├── sql/          # Esquema fuente, generación de datos y validaciones SQL
├── notebooks/    # Ingesta, transformación y publicación de tablas
├── src/          # Código Python reutilizable y configuración sin secretos
├── tests/        # Pruebas y validaciones de calidad
├── docs/         # Decisiones, supuestos y diseños no implementados
└── diagrams/     # Diagramas de arquitectura y modelo de datos
```

## Modelo fuente

La fuente transaccional contiene:

- `dbo.Customers`
- `dbo.Products`
- `dbo.Orders`
- `dbo.OrderItems`
- `dbo.Payments`
- `dbo.SupportTickets`

El script `sql/01_create_source_schema.sql` define claves primarias, claves foráneas, restricciones de dominio, índices sobre `updated_at` y campos técnicos de auditoría.

## Ejecución

La guía de reproducción se completará cuando estén configurados Azure SQL Database y Databricks.

## Uso de IA

Se utilizó asistencia de IA generativa para acelerar la estructura inicial del repositorio, el diseño del esquema y la documentación. Las decisiones técnicas, la validación y la implementación final serán revisadas y entendidas por la autora.

## Estado

En construcción.