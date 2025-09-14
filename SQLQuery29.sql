-- usp_KPI_SupplierPerformance
WITH Receipts AS (
  SELECT
    sit.SupplierID,
    sit.TransactionOccurredWhen AS ReceiptDate,
    sit.Quantity
  FROM StockItemTransactions sit
  JOIN ApplicationTransactionTypes tt
    ON tt.TransactionTypeID = sit.TransactionTypeID
  WHERE
    tt.TransactionTypeName = 'Stock Receipt'
    AND sit.SupplierID IS NOT NULL
),
Numbered AS (
  SELECT
    SupplierID,
    Quantity,
    ReceiptDate,
    lag(ReceiptDate) OVER(
      PARTITION BY SupplierID ORDER BY ReceiptDate
    ) AS PrevReceipt
  FROM Receipts
)
SELECT
  s.SupplierID,
  sp.SupplierName,
  COUNT(*) AS ReceiptEvents,
  SUM(s.Quantity) AS TotalQtyReceived,
  AVG(julianday(s.ReceiptDate) - julianday(s.PrevReceipt)) AS AvgDaysBetweenReceipts
FROM Numbered s
JOIN PurchasingSuppliers sp
  ON sp.SupplierID = s.SupplierID
GROUP BY
  s.SupplierID,
  sp.SupplierName
ORDER BY TotalQtyReceived DESC;

-- usp_KPI_PromoPerformance
SELECT
  grp.StockGroupID,
  grp.StockGroupName,
  COUNT(DISTINCT sd.SpecialDealID) AS ActiveDeals,
  AVG(sd.DiscountPercentage) AS AvgDiscountPct,
  MAX(sd.DiscountPercentage) AS MaxDiscountPct,
  SUM(COALESCE(il.ExtendedPrice, 0)) AS SalesDuringDeals,
  SUM(COALESCE(il.LineProfit, 0)) AS ProfitDuringDeals
FROM SalesSpecialDeals AS sd
LEFT JOIN StockItemsStockGroups AS sisg
  ON sisg.StockItemID = sd.StockItemID
JOIN WarehouseStockGroups AS grp
  ON grp.StockGroupID = COALESCE(sisg.StockGroupID, sd.StockGroupID)
LEFT JOIN StockItemsStockGroups AS sisg2
  ON sisg2.StockGroupID = grp.StockGroupID
LEFT JOIN SalesInvoiceLines AS il
  ON il.StockItemID = sisg2.StockItemID
GROUP BY
  grp.StockGroupID,
  grp.StockGroupName
ORDER BY
  ActiveDeals DESC;

-- usp_KPI_CustomerSegmentSales
SELECT
  cc.CustomerCategoryName,
  COUNT(DISTINCT sit.CustomerID) AS Customers,
  COUNT(*) AS ShipmentEvents,
  SUM(ABS(sit.Quantity)) AS TotalQtyShipped
FROM StockItemTransactions sit
JOIN SalesCustomers c
  ON c.CustomerID = sit.CustomerID
JOIN SalesCustomersCategories cc
  ON cc.CustomerCategoryID = c.CustomerCategoryID
WHERE sit.CustomerID IS NOT NULL
  AND sit.TransactionTypeID = 10    -- Stock Issue
GROUP BY cc.CustomerCategoryName
ORDER BY TotalQtyShipped DESC;

-- usp_KPI_TransactionDistribution
SELECT
  tt.TransactionTypeName,
  COUNT(*) AS TxnCount,
  COUNT(*)*100.0/SUM(COUNT(*)) OVER() AS PctShare
FROM StockItemTransactions sit
JOIN ApplicationTransactionTypes tt
  ON tt.TransactionTypeID = sit.TransactionTypeID
WHERE
  (? IS NULL OR sit.TransactionOccurredWhen >= ?)
  AND (? IS NULL OR sit.TransactionOccurredWhen <= ?)
GROUP BY tt.TransactionTypeName
ORDER BY TxnCount DESC;

-- Deal Coverage → % of stock groups with at least one deal
SELECT
    (SELECT COUNT(DISTINCT StockGroupID)
     FROM SalesSpecialDeals
     WHERE StockGroupID IS NOT NULL) AS GroupsWithDeals,
    (SELECT COUNT(*) FROM WarehouseStockGroups) AS TotalGroups,
    ROUND(
        CAST((SELECT COUNT(DISTINCT StockGroupID)
              FROM SalesSpecialDeals
              WHERE StockGroupID IS NOT NULL) AS REAL)
        / NULLIF((SELECT COUNT(*) FROM WarehouseStockGroups), 0) * 100.0,
        2
    ) AS DealCoveragePercent;