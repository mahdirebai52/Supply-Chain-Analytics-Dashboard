-- Most-Discounted "Clients" → top N BuyingGroups by % discount
SELECT
    bg.BuyingGroupName AS ClientGroup,
    ROUND(SUM(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS TotalDiscountPct,
    COUNT(sd.SpecialDealID) AS DealCount
FROM SalesBuyingGroups AS bg
LEFT JOIN SalesSpecialDeals AS sd
    ON bg.BuyingGroupID = sd.BuyingGroupID
    AND sd.DiscountPercentage IS NOT NULL
GROUP BY bg.BuyingGroupName
HAVING COUNT(sd.SpecialDealID) > 0
ORDER BY SUM(COALESCE(sd.DiscountPercentage, 0.0)) DESC
LIMIT :TopN;

-- usp_KPI_GrossProfit
SELECT
  SUM(LineProfit) AS TotalProfit,
  SUM(ExtendedPrice) AS TotalRevenue,
  (SUM(LineProfit)*1.0)/NULLIF(SUM(ExtendedPrice),0) AS GrossMarginPct
FROM SalesInvoiceLines;

-- usp_KPI_COGSvsPurchases
SELECT
  SUM(ExtendedPrice - LineProfit) AS COGS,
  (SELECT SUM(ExpectedUnitPricePerOuter*OrderedOuters)
   FROM PurchaseOrderLines) AS TotalPurchases
FROM SalesInvoiceLines;

-- usp_KPI_PromoDealsByStockGroup
SELECT
  grp.StockGroupID,
  grp.StockGroupName,
  COUNT(DISTINCT sd.SpecialDealID) AS DealCount,
  COUNT(DISTINCT COALESCE(sd.StockItemID, sisg2.StockItemID)) AS AffectedItems,
  AVG(sd.DiscountPercentage) AS AvgDiscountPct
FROM SalesSpecialDeals AS sd
LEFT JOIN StockItemsStockGroups AS sisg
  ON sisg.StockItemID = sd.StockItemID
JOIN WarehouseStockGroups AS grp
  ON grp.StockGroupID = COALESCE(sisg.StockGroupID, sd.StockGroupID)
LEFT JOIN StockItemsStockGroups AS sisg2
  ON sisg2.StockGroupID = grp.StockGroupID
GROUP BY
  grp.StockGroupID,
  grp.StockGroupName
ORDER BY
  DealCount DESC;

-- usp_KPI_PromoPerformanceByBuyingGroup
SELECT
  bg.BuyingGroupID,
  bg.BuyingGroupName,
  grp.StockGroupID,
  grp.StockGroupName,
  COUNT(DISTINCT sd.SpecialDealID) AS DealCount,
  AVG(sd.DiscountPercentage) AS AvgDiscountPct,
  SUM(COALESCE(il.ExtendedPrice, 0)) AS SalesDuringDeals,
  SUM(COALESCE(il.LineProfit, 0)) AS ProfitDuringDeals
FROM SalesSpecialDeals AS sd
JOIN SalesBuyingGroups AS bg
  ON bg.BuyingGroupID = sd.BuyingGroupID
LEFT JOIN StockItemsStockGroups AS sisg
  ON sisg.StockItemID = sd.StockItemID
JOIN WarehouseStockGroups AS grp
  ON grp.StockGroupID = COALESCE(sisg.StockGroupID, sd.StockGroupID)
LEFT JOIN StockItemsStockGroups AS sisg2
  ON sisg2.StockGroupID = grp.StockGroupID
LEFT JOIN SalesInvoiceLines AS il
  ON il.StockItemID = sisg2.StockItemID
GROUP BY
  bg.BuyingGroupID,
  bg.BuyingGroupName,
  grp.StockGroupID,
  grp.StockGroupName
ORDER BY
  SalesDuringDeals DESC;

-- usp_KPI_SupposedTaxAmount
SELECT
  il.InvoiceLineID,
  il.InvoiceID,
  il.ExtendedPrice AS LineTotalWithTax,
  il.TaxRate,
  il.TaxAmount AS RecordedTaxAmount,
  ROUND(
    il.ExtendedPrice
    * (il.TaxRate / (100.0 + il.TaxRate))
  ,2) AS ExpectedTaxAmount,
  il.TaxAmount
    - ROUND(
        il.ExtendedPrice
        * (il.TaxRate / (100.0 + il.TaxRate))
      ,2)
    AS TaxVariance
FROM SalesInvoiceLines il
WHERE (? IS NULL OR il.LastEditedWhen >= ?)
  AND (? IS NULL OR il.LastEditedWhen <= ?);

-- usp_KPI_SalesByStockGroup
WITH SalesLines AS (
  SELECT
    il.StockItemID,
    il.Quantity,
    il.LineProfit,
    il.ExtendedPrice,
    si.CustomerID
  FROM SalesInvoiceLines AS il
  LEFT JOIN SalesInvoices AS si
    ON si.InvoiceID = il.InvoiceID
  WHERE
    (? IS NULL OR il.LastEditedWhen >= ?)
    AND (? IS NULL OR il.LastEditedWhen <= ?)
),
SalesWithGroups AS (
  SELECT
    COALESCE(sisg.StockGroupID, sg0.StockGroupID) AS StockGroupID,
    sl.Quantity,
    sl.LineProfit,
    sl.ExtendedPrice,
    sl.CustomerID
  FROM SalesLines AS sl
  LEFT JOIN StockItemsStockGroups AS sisg
    ON sisg.StockItemID = sl.StockItemID
  LEFT JOIN WarehouseStockGroups AS sg0
    ON sg0.StockGroupID = sisg.StockGroupID
)
SELECT
  sg.StockGroupID,
  sg.StockGroupName,
  cn.CountryName,
  SUM(swg.Quantity) AS TotalUnitsSold,
  SUM(swg.LineProfit) AS TotalProfit,
  SUM(swg.ExtendedPrice) AS TotalRevenue,
  ROUND(
    SUM(swg.LineProfit)*1.0
    / NULLIF(SUM(swg.ExtendedPrice),0)
    * 100,2
  ) AS GrossMarginPct
FROM SalesWithGroups AS swg
JOIN WarehouseStockGroups AS sg
  ON sg.StockGroupID = swg.StockGroupID
LEFT JOIN SalesCustomers AS sc
  ON sc.CustomerID = swg.CustomerID
LEFT JOIN ApplicationCities AS city
  ON city.CityID = sc.DeliveryCityID
LEFT JOIN ApplicationStatesProvinces AS sp
  ON sp.StateProvinceID = city.StateProvinceID
LEFT JOIN ApplicationCountries AS cn
  ON cn.CountryID = sp.CountryID
WHERE (? IS NULL OR cn.CountryID = ?)
GROUP BY
  sg.StockGroupID,
  sg.StockGroupName,
  cn.CountryName
ORDER BY
  TotalUnitsSold DESC;