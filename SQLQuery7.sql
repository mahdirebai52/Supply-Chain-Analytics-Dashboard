-- Assumes WideWorldImporters data has been imported to SQLite tables
-- Create a SQLite table from existing data (modify as needed)
CREATE TABLE IF NOT EXISTS SalesInvoices AS
SELECT * FROM WideWorldImporters_Sales_Invoices;

-- Create primary key (SQLite syntax)
CREATE UNIQUE INDEX IF NOT EXISTS PK_SalesInvoices ON SalesInvoices(InvoiceID);