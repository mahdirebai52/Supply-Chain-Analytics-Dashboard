-- Create indexes for SQLite
CREATE INDEX IF NOT EXISTS IX_SIL_LastEditedWhen_StockItemID
ON SalesInvoiceLines(LastEditedWhen);

CREATE INDEX IF NOT EXISTS IX_SIT_ByType_Date
ON StockItemTransactions(TransactionTypeID, TransactionOccurredWhen);

CREATE UNIQUE INDEX IF NOT EXISTS IX_TT_Name
ON ApplicationTransactionTypes(TransactionTypeName);