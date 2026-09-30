/*
    Andina Market - Source validation queries
    These checks validate the relational source before ingestion into Databricks.
*/

-- 1. Record count by source table.
SELECT 'Customers' AS table_name, COUNT(*) AS total_rows FROM dbo.Customers
UNION ALL
SELECT 'Products', COUNT(*) FROM dbo.Products
UNION ALL
SELECT 'Orders', COUNT(*) FROM dbo.Orders
UNION ALL
SELECT 'OrderItems', COUNT(*) FROM dbo.OrderItems
UNION ALL
SELECT 'Payments', COUNT(*) FROM dbo.Payments
UNION ALL
SELECT 'SupportTickets', COUNT(*) FROM dbo.SupportTickets;

-- 2. Orders whose header total does not match the sum of active order lines.
SELECT
    COUNT(*) AS orders_with_inconsistent_total
FROM dbo.Orders AS o
LEFT JOIN (
    SELECT
        order_id,
        SUM(line_total) AS calculated_total
    FROM dbo.OrderItems
    WHERE is_deleted = 0
    GROUP BY order_id
) AS items
    ON items.order_id = o.order_id
WHERE ABS(o.order_total - ISNULL(items.calculated_total, 0)) > 0.01;

-- 3. Orders without detail rows.
SELECT
    COUNT(*) AS orders_without_items
FROM dbo.Orders AS o
LEFT JOIN dbo.OrderItems AS oi
    ON oi.order_id = o.order_id
   AND oi.is_deleted = 0
WHERE oi.order_item_id IS NULL;

-- 4. Payments whose amount is inconsistent with the order total.
SELECT
    COUNT(*) AS payments_with_inconsistent_amount
FROM dbo.Payments AS p
INNER JOIN dbo.Orders AS o
    ON o.order_id = p.order_id
WHERE ABS(p.amount - o.order_total) > 0.01;

-- 5. Controlled changes: latest changes that an incremental pipeline must capture.
SELECT TOP (10)
    payment_id,
    order_id,
    payment_status,
    amount,
    payment_date,
    created_at,
    updated_at
FROM dbo.Payments
ORDER BY updated_at DESC;

SELECT TOP (10)
    customer_id,
    first_name,
    last_name,
    segment,
    created_at,
    updated_at
FROM dbo.Customers
ORDER BY updated_at DESC;

SELECT
    product_id,
    sku,
    product_name,
    product_status,
    is_deleted,
    updated_at
FROM dbo.Products
WHERE is_deleted = 1
   OR product_status = 'discontinued';