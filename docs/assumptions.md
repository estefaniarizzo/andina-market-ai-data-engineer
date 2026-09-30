# Supuestos

1. Andina Market opera inicialmente en Colombia, México, Chile, Perú y Argentina; por eso los países se representan mediante códigos ISO de dos letras.

2. La base fuente es Azure SQL Database y contiene las seis tablas solicitadas por el reto.

3. Todas las fechas y timestamps se almacenan en UTC para evitar ambigüedades entre países.

4. Las claves `customer_id`, `product_id`, `order_id`, `order_item_id`, `payment_id` y `ticket_id` son identificadores estables de negocio para su entidad correspondiente.

5. Un pedido puede contener una o varias líneas en `OrderItems`.

6. Un pedido puede tener más de un pago o intento de pago; por ejemplo, un pago rechazado seguido por uno aprobado.

7. El monto de `Orders.order_total` representa el total transaccional informado por el sistema fuente. La suma de `OrderItems.line_total` se validará como una regla de calidad con una tolerancia decimal definida.

8. Las eliminaciones se manejan como bajas lógicas mediante `is_deleted`; no se implementan eliminaciones físicas desde la fuente.

9. Cada tabla posee `updated_at`, actualizado por la aplicación fuente cuando hay inserciones, modificaciones o bajas lógicas. Esta columna permite la estrategia incremental basada en watermark.

10. El esquema fuente se considera propiedad del sistema transaccional. El pipeline de datos solo lo lee y no modifica datos de negocio, excepto durante la carga sintética inicial y las simulaciones controladas de cambios.

11. La primera ejecución del pipeline será una carga completa. Las posteriores serán incrementales por `updated_at`.

12. Para el alcance del reto, la frecuencia de actualización propuesta es cada hora. En producción se revisaría según el SLA de negocio, la carga de Azure SQL y el volumen de cambios.

13. Los datos son enteramente sintéticos y no representan personas reales.

14. El trabajo se limita a los niveles 1 y 2. Streaming y SAP on-premise se incluirán como propuesta de diseño, no como integración funcional.