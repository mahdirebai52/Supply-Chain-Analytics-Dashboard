-- usp_KPI_ProductImbalance_SingleRow
WITH
Sales AS (
  SELECT StockItemID, SUM(Quantity) AS QtySold
  FROM SalesInvoiceLines
  WHERE (? IS NULL OR LastEditedWhen >= ?)
    AND (? IS NULL OR LastEditedWhen <= ?)
  GROUP BY StockItemID
),
Purch AS (
  SELECT
    pol.StockItemID,
    po.SupplierID,
    SUM(pol.OrderedOuters) AS QtyPurchased
  FROM PurchaseOrderLines pol
  JOIN PurchaseOrders po
    ON po.PurchaseOrderID = pol.PurchaseOrderID
  WHERE (? IS NULL OR pol.LastReceiptDate >= ?)
    AND (? IS NULL OR pol.LastReceiptDate <= ?)
  GROUP BY pol.StockItemID, po.SupplierID
),
Imb AS (
  SELECT
    pur.StockItemID,
    pur.SupplierID,
    COALESCE(pur.QtyPurchased,0) AS QtyPurchased,
    COALESCE(sal.QtySold,0) AS QtySold,
    COALESCE(pur.QtyPurchased,0) - COALESCE(sal.QtySold,0) AS NetBuildUp,
    CASE
      WHEN COALESCE(sal.QtySold,0)=0 THEN NULL
      ELSE CAST(pur.QtyPurchased AS REAL)/sal.QtySold
    END AS PurchaseToSalesRatio
  FROM Purch pur
  LEFT JOIN Sales sal
    ON sal.StockItemID = pur.StockItemID
)
SELECT
  i.StockItemID,
  si.StockItemName,
  group_concat(sg.StockGroupName, ', ') AS StockGroupNames,
  i.SupplierID,
  sup.SupplierName,
  i.QtyPurchased,
  i.QtySold,
  i.NetBuildUp,
  i.PurchaseToSalesRatio
FROM Imb i
JOIN WarehouseStockItem si
  ON si.StockItemID = i.StockItemID
JOIN PurchasingSuppliers sup
  ON sup.SupplierID = i.SupplierID
LEFT JOIN StockItemsStockGroups sisg
  ON sisg.StockItemID = i.StockItemID
LEFT JOIN WarehouseStockGroups sg
  ON sg.StockGroupID = sisg.StockGroupID
GROUP BY
  i.StockItemID,
  si.StockItemName,
  i.SupplierID,
  sup.SupplierName,
  i.QtyPurchased,
  i.QtySold,
  i.NetBuildUp,
  i.PurchaseToSalesRatio
ORDER BY
  NetBuildUp DESC
LIMIT ?;