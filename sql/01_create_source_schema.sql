/*
    Andina Market - Source Database Schema
    Purpose: transactional source schema hosted in Azure SQL Database.

    Design notes:
    - The source uses relational integrity through PKs and FKs.
    - updated_at supports incremental ingestion using a watermark.
    - is_deleted represents logical deletions so the data pipeline can capture removals.
    - All timestamps are stored in UTC.
*/

-- Create the schema only if it does not exist.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC('CREATE SCHEMA dbo');
END;
GO

-- Drop tables only during local/repeatable setup.
-- The order respects foreign-key dependencies.
DROP TABLE IF EXISTS dbo.SupportTickets;
DROP TABLE IF EXISTS dbo.Payments;
DROP TABLE IF EXISTS dbo.OrderItems;
DROP TABLE IF EXISTS dbo.Orders;
DROP TABLE IF EXISTS dbo.Products;
DROP TABLE IF EXISTS dbo.Customers;
GO

CREATE TABLE dbo.Customers (
    customer_id      INT IDENTITY(1,1) NOT NULL,
    first_name       NVARCHAR(80) NOT NULL,
    last_name        NVARCHAR(80) NOT NULL,
    email            NVARCHAR(255) NOT NULL,
    phone            NVARCHAR(30) NULL,
    city             NVARCHAR(100) NOT NULL,
    country          CHAR(2) NOT NULL,
    segment          VARCHAR(30) NOT NULL,
    signup_date      DATE NOT NULL,
    created_at       DATETIME2(3) NOT NULL
        CONSTRAINT DF_Customers_created_at DEFAULT SYSUTCDATETIME(),
    updated_at       DATETIME2(3) NOT NULL
        CONSTRAINT DF_Customers_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted       BIT NOT NULL
        CONSTRAINT DF_Customers_is_deleted DEFAULT 0,

    CONSTRAINT PK_Customers PRIMARY KEY (customer_id),
    CONSTRAINT UQ_Customers_email UNIQUE (email),
    CONSTRAINT CK_Customers_country
        CHECK (country IN ('CO', 'MX', 'CL', 'PE', 'AR')),
    CONSTRAINT CK_Customers_segment
        CHECK (segment IN ('Regular', 'Premium', 'Business'))
);
GO

CREATE TABLE dbo.Products (
    product_id        INT IDENTITY(1,1) NOT NULL,
    sku               VARCHAR(40) NOT NULL,
    product_name      NVARCHAR(200) NOT NULL,
    category          NVARCHAR(100) NOT NULL,
    unit_price        DECIMAL(12,2) NOT NULL,
    product_description NVARCHAR(MAX) NULL,
    product_status    VARCHAR(20) NOT NULL,
    created_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Products_created_at DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Products_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted        BIT NOT NULL
        CONSTRAINT DF_Products_is_deleted DEFAULT 0,

    CONSTRAINT PK_Products PRIMARY KEY (product_id),
    CONSTRAINT UQ_Products_sku UNIQUE (sku),
    CONSTRAINT CK_Products_unit_price CHECK (unit_price >= 0),
    CONSTRAINT CK_Products_status
        CHECK (product_status IN ('active', 'inactive', 'discontinued'))
);
GO

CREATE TABLE dbo.Orders (
    order_id          BIGINT IDENTITY(1,1) NOT NULL,
    customer_id       INT NOT NULL,
    order_date        DATETIME2(3) NOT NULL,
    sales_channel     VARCHAR(20) NOT NULL,
    order_status      VARCHAR(20) NOT NULL,
    order_total       DECIMAL(14,2) NOT NULL,
    created_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Orders_created_at DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Orders_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted        BIT NOT NULL
        CONSTRAINT DF_Orders_is_deleted DEFAULT 0,

    CONSTRAINT PK_Orders PRIMARY KEY (order_id),
    CONSTRAINT FK_Orders_Customers
        FOREIGN KEY (customer_id) REFERENCES dbo.Customers(customer_id),
    CONSTRAINT CK_Orders_channel
        CHECK (sales_channel IN ('web', 'app', 'store')),
    CONSTRAINT CK_Orders_status
        CHECK (order_status IN ('created', 'paid', 'shipped', 'delivered', 'cancelled')),
    CONSTRAINT CK_Orders_total CHECK (order_total >= 0)
);
GO

CREATE TABLE dbo.OrderItems (
    order_item_id     BIGINT IDENTITY(1,1) NOT NULL,
    order_id          BIGINT NOT NULL,
    product_id        INT NOT NULL,
    quantity          INT NOT NULL,
    unit_price        DECIMAL(12,2) NOT NULL,
    line_total        AS (CONVERT(DECIMAL(14,2), quantity * unit_price)) PERSISTED,
    created_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_OrderItems_created_at DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_OrderItems_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted        BIT NOT NULL
        CONSTRAINT DF_OrderItems_is_deleted DEFAULT 0,

    CONSTRAINT PK_OrderItems PRIMARY KEY (order_item_id),
    CONSTRAINT FK_OrderItems_Orders
        FOREIGN KEY (order_id) REFERENCES dbo.Orders(order_id),
    CONSTRAINT FK_OrderItems_Products
        FOREIGN KEY (product_id) REFERENCES dbo.Products(product_id),
    CONSTRAINT CK_OrderItems_quantity CHECK (quantity > 0),
    CONSTRAINT CK_OrderItems_unit_price CHECK (unit_price >= 0)
);
GO

CREATE TABLE dbo.Payments (
    payment_id        BIGINT IDENTITY(1,1) NOT NULL,
    order_id          BIGINT NOT NULL,
    payment_method    VARCHAR(30) NOT NULL,
    payment_status    VARCHAR(20) NOT NULL,
    amount            DECIMAL(14,2) NOT NULL,
    payment_date      DATETIME2(3) NULL,
    created_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Payments_created_at DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2(3) NOT NULL
        CONSTRAINT DF_Payments_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted        BIT NOT NULL
        CONSTRAINT DF_Payments_is_deleted DEFAULT 0,

    CONSTRAINT PK_Payments PRIMARY KEY (payment_id),
    CONSTRAINT FK_Payments_Orders
        FOREIGN KEY (order_id) REFERENCES dbo.Orders(order_id),
    CONSTRAINT CK_Payments_method
        CHECK (payment_method IN ('credit_card', 'debit_card', 'pse', 'cash', 'wallet')),
    CONSTRAINT CK_Payments_status
        CHECK (payment_status IN ('pending', 'approved', 'rejected', 'refunded')),
    CONSTRAINT CK_Payments_amount CHECK (amount >= 0)
);
GO

CREATE TABLE dbo.SupportTickets (
    ticket_id          BIGINT IDENTITY(1,1) NOT NULL,
    customer_id        INT NOT NULL,
    subject            NVARCHAR(250) NOT NULL,
    ticket_body        NVARCHAR(MAX) NOT NULL,
    ticket_status      VARCHAR(20) NOT NULL,
    priority           VARCHAR(20) NOT NULL,
    created_at         DATETIME2(3) NOT NULL
        CONSTRAINT DF_SupportTickets_created_at DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2(3) NOT NULL
        CONSTRAINT DF_SupportTickets_updated_at DEFAULT SYSUTCDATETIME(),
    is_deleted         BIT NOT NULL
        CONSTRAINT DF_SupportTickets_is_deleted DEFAULT 0,

    CONSTRAINT PK_SupportTickets PRIMARY KEY (ticket_id),
    CONSTRAINT FK_SupportTickets_Customers
        FOREIGN KEY (customer_id) REFERENCES dbo.Customers(customer_id),
    CONSTRAINT CK_SupportTickets_status
        CHECK (ticket_status IN ('open', 'in_progress', 'resolved', 'closed')),
    CONSTRAINT CK_SupportTickets_priority
        CHECK (priority IN ('low', 'medium', 'high', 'urgent'))
);
GO

-- These indexes support the incremental extraction query:
-- WHERE updated_at > last_watermark AND updated_at <= current_watermark.
CREATE INDEX IX_Customers_updated_at ON dbo.Customers(updated_at);
CREATE INDEX IX_Products_updated_at ON dbo.Products(updated_at);
CREATE INDEX IX_Orders_updated_at ON dbo.Orders(updated_at);
CREATE INDEX IX_OrderItems_updated_at ON dbo.OrderItems(updated_at);
CREATE INDEX IX_Payments_updated_at ON dbo.Payments(updated_at);
CREATE INDEX IX_SupportTickets_updated_at ON dbo.SupportTickets(updated_at);
GO