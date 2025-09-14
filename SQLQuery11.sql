-- usp_KPI_SalesVsPurchases
SELECT
  (SELECT SUM(ExtendedPrice)
   FROM SalesInvoiceLines
   WHERE (? IS NULL OR LastEditedWhen >= ?)
     AND (? IS NULL OR LastEditedWhen <= ?)
  ) AS TotalSales,
  (SELECT SUM(ExpectedUnitPricePerOuter * OrderedOuters)
   FROM PurchaseOrderLines
   WHERE (? IS NULL OR LastReceiptDate >= ?)
     AND (? IS NULL OR LastReceiptDate <= ?)
  ) AS TotalPurchases;

-- usp_KPI_AvgMarginPerProductWithGroup
SELECT
  si.StockItemID,
  si.StockItemName,
  sg.StockGroupID,
  sg.StockGroupName,
  AVG(il.LineProfit) AS AvgMargin,
  COUNT(DISTINCT il.InvoiceID) AS InvoiceCount,
  SUM(il.LineProfit) AS TotalProfit,
  SUM(il.ExtendedPrice) AS TotalRevenue,
  ROUND(
    SUM(il.LineProfit)*1.0
    / NULLIF(SUM(il.ExtendedPrice),0)
    * 100,2
  ) AS MarginPct
FROM SalesInvoiceLines AS il
JOIN WarehouseStockItem AS si
  ON si.StockItemID = il.StockItemID
LEFT JOIN StockItemsStockGroups AS sisg
  ON sisg.StockItemID = si.StockItemID
LEFT JOIN WarehouseStockGroups AS sg
  ON sg.StockGroupID = sisg.StockGroupID
WHERE (? IS NULL OR il.LastEditedWhen >= ?)
  AND (? IS NULL OR il.LastEditedWhen <= ?)
GROUP BY
  si.StockItemID,
  si.StockItemName,
  sg.StockGroupID,
  sg.StockGroupName
ORDER BY AvgMargin DESC;

-- usp_KPI_DealCoverage
SELECT
  (SELECT COUNT(DISTINCT StockGroupID)
   FROM SalesSpecialDeals
   WHERE StockGroupID IS NOT NULL) AS GroupsWithDeals,
  (SELECT COUNT(*) FROM WarehouseStockGroups) AS TotalGroups,
  CAST((SELECT COUNT(DISTINCT StockGroupID)
        FROM SalesSpecialDeals
        WHERE StockGroupID IS NOT NULL) AS REAL)
    / NULLIF((SELECT COUNT(*) FROM WarehouseStockGroups),0) * 100
    AS DealCoveragePercent;

-- usp_KPI_StockMovementVolume
SELECT
  SUM(Quantity) AS TotalMovementVolume
FROM StockItemTransactions
WHERE (? IS NULL OR TransactionOccurredWhen >= ?)
  AND (? IS NULL OR TransactionOccurredWhen <= ?);

-- usp_KPI_MostDiscountedClients
SELECT
  bg.BuyingGroupName AS ClientGroup,
  SUM(sd.DiscountPercentage) AS TotalDiscountPct,
  COUNT(*) AS DealCount
FROM SalesSpecialDeals AS sd
JOIN SalesBuyingGroups AS bg
  ON bg.BuyingGroupID = sd.BuyingGroupID
WHERE sd.DiscountPercentage IS NOT NULL
GROUP BY
  bg.BuyingGroupName
ORDER BY
  SUM(sd.DiscountPercentage) DESC
LIMIT ?;