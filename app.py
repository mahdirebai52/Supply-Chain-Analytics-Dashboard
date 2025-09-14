import datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text
import sqlite3

# ── 1. Set Streamlit page config ───────────────────────────────────────────
st.set_page_config(page_title="Supply-Chain KPI Dashboard", layout="wide")

# ── 2. DB Engine and Query Functions ────────────────────────────────────────
engine = create_engine("sqlite:///mydb.db")


def get_query(query_name):
    queries = {
        "dbo.usp_KPI_SalesVsPurchases": """
            SELECT 
                (SELECT SUM(ExtendedPrice)
                 FROM SalesInvoiceLines
                 WHERE LastEditedWhen BETWEEN ? AND ?) AS TotalSales,
                (SELECT SUM(ExpectedUnitPricePerOuter * OrderedOuters) 
                 FROM PurchaseOrderLines 
                 WHERE LastReceiptDate BETWEEN ? AND ?) AS TotalPurchases
        """,

        "dbo.usp_KPI_AvgMarginPerProductWithGroup": """
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
                    SUM(il.LineProfit) * 1.0
                    / NULLIF(SUM(il.ExtendedPrice), 0)
                    * 100, 2
                ) AS MarginPct
            FROM SalesInvoiceLines AS il
            JOIN WarehouseStockItem AS si
                ON si.StockItemID = il.StockItemID
            LEFT JOIN StockItemsStockGroups AS sisg
                ON sisg.StockItemID = si.StockItemID
            LEFT JOIN WarehouseStockGroups AS sg
                ON sg.StockGroupID = sisg.StockGroupID
            WHERE il.LastEditedWhen BETWEEN ? AND ?
            GROUP BY
                si.StockItemID,
                si.StockItemName,
                sg.StockGroupID,
                sg.StockGroupName
            ORDER BY AvgMargin DESC
        """,

        "dbo.usp_KPI_DealCoverage": """
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
                ) AS DealCoveragePercent
        """,

        "dbo.usp_KPI_StockMovementVolume": """
            SELECT 
                SUM(Quantity) AS TotalMovementVolume
            FROM StockItemTransactions
            WHERE TransactionOccurredWhen BETWEEN ? AND ?
        """,

        "dbo.usp_KPI_MostDiscountedClients": """
            SELECT
                bg.BuyingGroupName AS ClientGroup,
                ROUND(SUM(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS TotalDiscountPct,
                COUNT(sd.SpecialDealID) AS DealCount,
                ROUND(AVG(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS AvgDiscount,
                ROUND(MAX(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS MaxDiscount
            FROM SalesBuyingGroups AS bg
            LEFT JOIN SalesSpecialDeals AS sd
                ON bg.BuyingGroupID = sd.BuyingGroupID
                AND sd.DiscountPercentage IS NOT NULL
            GROUP BY bg.BuyingGroupName
            HAVING COUNT(sd.SpecialDealID) > 0
            ORDER BY SUM(COALESCE(sd.DiscountPercentage, 0.0)) DESC
            LIMIT ?
        """,

        "dbo.usp_KPI_SupplierPerformance": """
            WITH Receipts AS (
                SELECT
                    sit.SupplierID,
                    sit.TransactionOccurredWhen AS ReceiptDate,
                    sit.Quantity
                FROM StockItemTransactions sit
                JOIN ApplicationTransactionTypes tt
                    ON tt.TransactionTypeID = sit.TransactionTypeID
                WHERE tt.TransactionTypeName = 'Stock Receipt'
                    AND sit.SupplierID IS NOT NULL
            ),
            Numbered AS (
                SELECT
                    SupplierID,
                    Quantity,
                    ReceiptDate,
                    LAG(ReceiptDate) OVER(
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
            WHERE s.PrevReceipt IS NOT NULL
            GROUP BY s.SupplierID, sp.SupplierName
            ORDER BY TotalQtyReceived DESC
        """,

        "dbo.usp_KPI_PromoPerformance": """
            SELECT
                COUNT(DISTINCT sd.SpecialDealID) AS ActiveDeals,
                ROUND(AVG(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS AvgDiscountPct,
                ROUND(MAX(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS MaxDiscountPct,
                COUNT(DISTINCT sd.StockGroupID) AS GroupsWithDeals,
                COUNT(DISTINCT sd.BuyingGroupID) AS BuyingGroupsWithDeals
            FROM SalesSpecialDeals sd
            WHERE sd.DiscountPercentage IS NOT NULL
        """,

        "dbo.usp_KPI_TransactionDistribution": """
            SELECT 
                tt.TransactionTypeName,
                COUNT(*) AS TxnCount,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS PctShare
            FROM StockItemTransactions sit
            JOIN ApplicationTransactionTypes tt 
                ON sit.TransactionTypeID = tt.TransactionTypeID
            WHERE sit.TransactionOccurredWhen BETWEEN ? AND ?
            GROUP BY tt.TransactionTypeName
            ORDER BY TxnCount DESC
        """,

        "dbo.usp_KPI_GrossProfit": """
            SELECT 
                SUM(LineProfit) AS TotalProfit,
                SUM(ExtendedPrice) AS TotalRevenue,
                (SUM(LineProfit) * 1.0) / NULLIF(SUM(ExtendedPrice), 0) AS GrossMarginPct
            FROM SalesInvoiceLines
        """,

        "dbo.usp_KPI_COGSvsPurchases": """
            SELECT 
                SUM(ExtendedPrice - LineProfit) AS COGS,
                (SELECT SUM(ExpectedUnitPricePerOuter * OrderedOuters) 
                 FROM PurchaseOrderLines) AS TotalPurchases
            FROM SalesInvoiceLines
        """,

        "dbo.usp_KPI_PromoDealsByStockGroup": """
            SELECT
                grp.StockGroupID,
                grp.StockGroupName,
                COUNT(DISTINCT sd.SpecialDealID) AS DealCount,
                COUNT(DISTINCT COALESCE(sd.StockItemID, sisg2.StockItemID)) AS AffectedItems,
                ROUND(AVG(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS AvgDiscountPct
            FROM WarehouseStockGroups AS grp
            LEFT JOIN SalesSpecialDeals AS sd
                ON grp.StockGroupID = sd.StockGroupID
                OR grp.StockGroupID IN (
                    SELECT sisg.StockGroupID 
                    FROM StockItemsStockGroups sisg 
                    WHERE sisg.StockItemID = sd.StockItemID
                )
            LEFT JOIN StockItemsStockGroups AS sisg2
                ON sisg2.StockGroupID = grp.StockGroupID
            GROUP BY grp.StockGroupID, grp.StockGroupName
            ORDER BY DealCount DESC
        """,

        "dbo.usp_KPI_PromoPerformanceByBuyingGroup": """
            SELECT
                bg.BuyingGroupID,
                bg.BuyingGroupName,
                COUNT(DISTINCT sd.SpecialDealID) AS DealCount,
                ROUND(AVG(COALESCE(sd.DiscountPercentage, 0.0)), 2) AS AvgDiscountPct,
                SUM(COALESCE(il.ExtendedPrice, 0)) AS SalesDuringDeals,
                SUM(COALESCE(il.LineProfit, 0)) AS ProfitDuringDeals
            FROM SalesBuyingGroups AS bg
            LEFT JOIN SalesSpecialDeals AS sd
                ON bg.BuyingGroupID = sd.BuyingGroupID
            LEFT JOIN StockItemsStockGroups AS sisg
                ON sisg.StockItemID = sd.StockItemID
            LEFT JOIN WarehouseStockGroups AS grp
                ON grp.StockGroupID = COALESCE(sisg.StockGroupID, sd.StockGroupID)
            LEFT JOIN StockItemsStockGroups AS sisg2
                ON sisg2.StockGroupID = grp.StockGroupID
            LEFT JOIN SalesInvoiceLines AS il
                ON il.StockItemID = sisg2.StockItemID
            GROUP BY
                bg.BuyingGroupID,
                bg.BuyingGroupName
            ORDER BY SalesDuringDeals DESC
        """,

        "dbo.usp_KPI_SupposedTaxAmount": """
            SELECT 
                il.TaxRate,
                SUM(ROUND(
                    il.ExtendedPrice
                    * (il.TaxRate / (100.0 + il.TaxRate)),
                    2
                )) AS ExpectedTaxAmount,
                SUM(il.TaxAmount) AS RecordedTaxAmount,
                SUM(il.TaxAmount) - SUM(ROUND(
                    il.ExtendedPrice
                    * (il.TaxRate / (100.0 + il.TaxRate)),
                    2
                )) AS TaxVariance
            FROM SalesInvoiceLines il
            WHERE il.LastEditedWhen BETWEEN ? AND ?
            GROUP BY il.TaxRate
        """,

        "dbo.usp_KPI_SalesByStockGroup": """
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
                WHERE il.LastEditedWhen BETWEEN ? AND ?
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
                SUM(swg.Quantity) AS TotalUnitsSold,
                SUM(swg.LineProfit) AS TotalProfit,
                SUM(swg.ExtendedPrice) AS TotalRevenue,
                ROUND(
                    SUM(swg.LineProfit) * 1.0
                    / NULLIF(SUM(swg.ExtendedPrice), 0)
                    * 100, 2
                ) AS GrossMarginPct
            FROM SalesWithGroups AS swg
            JOIN WarehouseStockGroups AS sg
                ON sg.StockGroupID = swg.StockGroupID
            GROUP BY
                sg.StockGroupID,
                sg.StockGroupName
            ORDER BY TotalUnitsSold DESC
        """,

        "dbo.usp_KPI_CustomerSegmentSales": """
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
                AND sit.TransactionTypeID = 10
            GROUP BY cc.CustomerCategoryName
            ORDER BY TotalQtyShipped DESC
        """,

        "dbo.usp_KPI_ProductImbalance_SingleRow": """
            WITH
            Sales AS (
                SELECT StockItemID, SUM(Quantity) AS QtySold
                FROM SalesInvoiceLines
                WHERE LastEditedWhen BETWEEN ? AND ?
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
                WHERE pol.LastReceiptDate BETWEEN ? AND ?
                GROUP BY pol.StockItemID, po.SupplierID
            ),
            Imb AS (
                SELECT
                    pur.StockItemID,
                    pur.SupplierID,
                    COALESCE(pur.QtyPurchased, 0) AS QtyPurchased,
                    COALESCE(sal.QtySold, 0) AS QtySold,
                    COALESCE(pur.QtyPurchased, 0) - COALESCE(sal.QtySold, 0) AS NetBuildUp,
                    CASE
                        WHEN COALESCE(sal.QtySold, 0) = 0 THEN NULL
                        ELSE CAST(pur.QtyPurchased AS REAL) / sal.QtySold
                    END AS PurchaseToSalesRatio
                FROM Purch pur
                LEFT JOIN Sales sal
                    ON sal.StockItemID = pur.StockItemID
            )
            SELECT
                i.StockItemID,
                si.StockItemName,
                GROUP_CONCAT(sg.StockGroupName, ', ') AS StockGroupNames,
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
            ORDER BY NetBuildUp DESC
            LIMIT ?
        """,

        # Data validation query
        "check_special_deals": """
            SELECT 
                COUNT(*) as TotalRecords,
                COUNT(StockGroupID) as RecordsWithStockGroupID,
                COUNT(BuyingGroupID) as RecordsWithBuyingGroupID,
                COUNT(DiscountPercentage) as RecordsWithDiscount,
                ROUND(AVG(COALESCE(DiscountPercentage, 0.0)), 2) as AvgDiscountPct,
                ROUND(MAX(COALESCE(DiscountPercentage, 0.0)), 2) as MaxDiscountPct,
                COUNT(DISTINCT StockGroupID) as UniqueStockGroups,
                COUNT(DISTINCT BuyingGroupID) as UniqueBuyingGroups
            FROM SalesSpecialDeals
        """
    }
    return queries.get(query_name, "")


def run_proc(proc_name: str, params=()):
    query = get_query(proc_name)
    if not query:
        return pd.DataFrame()  # Return empty DataFrame if query not found

    # For procedures with specific parameter handling
    if proc_name == "dbo.usp_KPI_SalesVsPurchases":
        # This proc needs date parameters twice (for sales and purchases)
        return pd.read_sql(query, engine, params=(params[0], params[1], params[0], params[1]))
    elif proc_name == "dbo.usp_KPI_ProductImbalance_SingleRow":
        # This proc needs start_date, end_date, start_date, end_date, limit
        if len(params) == 3:  # If called with (start_date, end_date, limit)
            return pd.read_sql(query, engine, params=(params[0], params[1], params[0], params[1], params[2]))

    # Default parameter handling
    try:
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        st.error(f"Error executing query {proc_name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def check_special_deals_data():
    """Check if SalesSpecialDeals has data and validate schema"""
    query = get_query("check_special_deals")
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error checking SalesSpecialDeals: {e}")
        return pd.DataFrame()


# ── 3. Sidebar controls with 2013–2016 defaults ───────────────────────────
MIN_DATE = dt.date(2013, 1, 1)
MAX_DATE = dt.date(2016, 12, 31)
st.sidebar.header("Date Window")
start_date = st.sidebar.date_input("Start date", MIN_DATE, MIN_DATE, MAX_DATE)
end_date = st.sidebar.date_input("End date", MAX_DATE, MIN_DATE, MAX_DATE)
sd = dt.datetime.combine(start_date, dt.time.min)
ed = dt.datetime.combine(end_date, dt.time.max)

# Add SalesSpecialDeals validation button in sidebar
st.sidebar.markdown("---")
st.sidebar.header("🔍 Data Validation")
if st.sidebar.button("Check SalesSpecialDeals"):
    deals_data = check_special_deals_data()
    if not deals_data.empty:
        total_records = deals_data.iloc[0]['TotalRecords']
        if total_records == 0:
            st.sidebar.error("❌ SalesSpecialDeals is empty!")
        else:
            st.sidebar.success(f"✅ {total_records} deals found")
            with_discounts = deals_data.iloc[0]['RecordsWithDiscount']
            st.sidebar.info(f"📊 {with_discounts} with discounts")
    else:
        st.sidebar.error("❌ Cannot access SalesSpecialDeals")


# ── 4. Load KPI DataFrames ─────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_kpis(s, e):
    try:
        return {
            "sales_vs_pur": run_proc("dbo.usp_KPI_SalesVsPurchases", (s, e)),
            "avg_margin_with_group": run_proc("dbo.usp_KPI_AvgMarginPerProductWithGroup", (s, e)),
            "deal_cov": run_proc("dbo.usp_KPI_DealCoverage", ()),
            "movement": run_proc("dbo.usp_KPI_StockMovementVolume", (s, e)),
            "top_clients": run_proc("dbo.usp_KPI_MostDiscountedClients", (10,)),
            "supplier_perf": run_proc("dbo.usp_KPI_SupplierPerformance", ()),
            "promo_perf": run_proc("dbo.usp_KPI_PromoPerformance", ()),
            "txn_dist": run_proc("dbo.usp_KPI_TransactionDistribution", (s, e)),
            "gross": run_proc("dbo.usp_KPI_GrossProfit", ()),
            "cogs_vs_po": run_proc("dbo.usp_KPI_COGSvsPurchases", ()),
            "promo_by_group": run_proc("dbo.usp_KPI_PromoDealsByStockGroup", ()),
            "promo_by_buy": run_proc("dbo.usp_KPI_PromoPerformanceByBuyingGroup", ()),
            "tax_variance": run_proc("dbo.usp_KPI_SupposedTaxAmount", (s, e)),
            "sales_by_group": run_proc("dbo.usp_KPI_SalesByStockGroup", (s, e)),
            "cust_seg": run_proc("dbo.usp_KPI_CustomerSegmentSales", ()),
            "imbalance": run_proc("dbo.usp_KPI_ProductImbalance_SingleRow", (sd, ed, 10)),
        }
    except Exception as e:
        st.error(f"Error loading KPI data: {e}")
        return {
            "sales_vs_pur": pd.DataFrame(),
            "avg_margin_with_group": pd.DataFrame(),
            "deal_cov": pd.DataFrame(),
            "movement": pd.DataFrame(),
            "top_clients": pd.DataFrame(),
            "supplier_perf": pd.DataFrame(),
            "promo_perf": pd.DataFrame(),
            "txn_dist": pd.DataFrame(),
            "gross": pd.DataFrame(),
            "cogs_vs_po": pd.DataFrame(),
            "promo_by_group": pd.DataFrame(),
            "promo_by_buy": pd.DataFrame(),
            "tax_variance": pd.DataFrame(),
            "sales_by_group": pd.DataFrame(),
            "cust_seg": pd.DataFrame(),
            "imbalance": pd.DataFrame(),
        }


@st.cache_data(ttl=600)
def load_trend(s, e):
    sql = text("""
        WITH Sales AS (
            SELECT 
                strftime('%Y-%m-01', LastEditedWhen) AS Period,
                SUM(ExtendedPrice) AS Sales
            FROM SalesInvoiceLines
            WHERE LastEditedWhen BETWEEN :start AND :end
            GROUP BY strftime('%Y-%m', LastEditedWhen)
        ), Purchases AS (
            SELECT 
                strftime('%Y-%m-01', LastReceiptDate) AS Period,
                SUM(ExpectedUnitPricePerOuter * OrderedOuters) AS Purchases
            FROM PurchaseOrderLines
            WHERE LastReceiptDate BETWEEN :start AND :end
            GROUP BY strftime('%Y-%m', LastReceiptDate)
        )
        SELECT 
            COALESCE(s.Period, p.Period) AS Period,
            COALESCE(s.Sales, 0) AS Sales,
            COALESCE(p.Purchases, 0) AS Purchases
        FROM Sales s
        LEFT JOIN Purchases p ON s.Period = p.Period
        UNION ALL
        SELECT 
            p.Period,
            0 AS Sales,
            p.Purchases
        FROM Purchases p
        WHERE p.Period NOT IN (SELECT Period FROM Sales)
        ORDER BY Period;
    """)
    try:
        return pd.read_sql(sql, engine, params={"start": s, "end": e})
    except Exception as e:
        st.error(f"Error loading trend data: {e}")
        return pd.DataFrame(columns=["Period", "Sales", "Purchases"])


try:
    kpis = load_kpis(sd, ed)
    trend = load_trend(sd, ed)

    # ── 5. Fix AvgMargin dtype so nlargest works ───────────────────────────────
    if not kpis["avg_margin_with_group"].empty and "AvgMargin" in kpis["avg_margin_with_group"].columns:
        kpis["avg_margin_with_group"]["AvgMargin"] = pd.to_numeric(
            kpis["avg_margin_with_group"]["AvgMargin"], errors="coerce"
        )


    # ── 6. Helper to safely extract a single value ─────────────────────────────
    def get_first(df, col, default=0):
        return df[col].iloc[0] if col in df.columns and not df.empty and pd.notna(df[col].iloc[0]) else default


    # ── 7. Extract headline metrics ────────────────────────────────────────────
    sales = get_first(kpis["sales_vs_pur"], "TotalSales")
    purch = get_first(kpis["sales_vs_pur"], "TotalPurchases")
    profit = get_first(kpis["gross"], "TotalProfit")
    margin = get_first(kpis["gross"], "GrossMarginPct")
    cogs = get_first(kpis["cogs_vs_po"], "COGS")
    total_txn = int(kpis["txn_dist"]["TxnCount"].sum() if "TxnCount" in kpis["txn_dist"].columns and not kpis[
        "txn_dist"].empty else 0)
    mov = get_first(kpis["movement"], "TotalMovementVolume")
    cov = get_first(kpis["deal_cov"], "DealCoveragePercent")
    deals = int(get_first(kpis["promo_perf"], "ActiveDeals"))
    avg_disc = get_first(kpis["promo_perf"], "AvgDiscountPct") / 100.0
    max_disc = get_first(kpis["promo_perf"], "MaxDiscountPct") / 100.0

    # ── 8. Headline metrics display ────────────────────────────────────────────
    st.title("📊 Optimisation de la chaîne d'approvisionnement")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Sales", f"${sales:,.2f}")
    c2.metric("Total Profit", f"${profit:,.2f}")
    c3.metric("Gross Margin", f"{margin:.1%}")
    c4.metric("Total Purchases", f"${purch:,.2f}")

    # ── 9. Cost & inventory metrics ───────────────────────────────────────────
    c5, c6, c7 = st.columns(3)
    c5.metric("COGS", f"${cogs:,.2f}")
    c6.metric("Total Transactions", f"{total_txn:,}")
    c7.metric("Stock Movement Vol.", f"{mov:,}")

    # ── 10. Performance & promotions metrics ──────────────────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Deal Coverage", f"{cov:.1f}%")
    p2.metric("Active Deals", f"{deals}")
    p3.metric("Avg Discount %", f"{avg_disc:.1%}")
    p4.metric("Max Discount %", f"{max_disc:.1%}")

    # ── 11. Top-discounted clients ─────────────────────────────────────────────
    st.subheader("🏷️ Top 10 Most-Discounted Clients")
    if not kpis["top_clients"].empty:
        # Display with better formatting
        display_df = kpis["top_clients"].copy()
        if "TotalDiscountPct" in display_df.columns:
            display_df["TotalDiscountPct"] = display_df["TotalDiscountPct"].apply(lambda x: f"{x:.2f}%")
        if "AvgDiscount" in display_df.columns:
            display_df["AvgDiscount"] = display_df["AvgDiscount"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(display_df)
    else:
        st.warning("⚠️ No client discount data available - check SalesSpecialDeals table")

    # ── 12. Section Tabs ────────────────────────────────────────────────────────
    tabs = st.tabs([
        "📊 Sales vs Purchases Trend",
        "📈 Margin by Product",
        "🚚 Supplier Performance",
        "🛒 Sales by Stock Group",
        "👥 Customer Segments",
        "🔄 Transaction Mix",
        "🎯 Promo by Stock Group",
        "👥 Promo by Buying Group",
        "💲 Tax Analysis",
        "📦 Imbalance",
    ])

    # Trend chart
    with tabs[0]:
        st.subheader("Monthly Sales vs Purchases")
        if not trend.empty and "Period" in trend.columns and "Sales" in trend.columns and "Purchases" in trend.columns:
            try:
                trend["Period"] = pd.to_datetime(trend["Period"])
                fig = px.line(trend, x="Period", y=["Sales", "Purchases"],
                              labels={"value": "Amount ($)", "Period": "Month"})
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating trend chart: {e}")
        else:
            st.warning("No trend data available for the selected date range")

        st.dataframe(trend.style.format({"Sales": "${:,.2f}", "Purchases": "${:,.2f}"}))

    # Margin by Product (with Group)
    with tabs[1]:
        st.subheader("Average Margin per Product (Top 10)")

        if not kpis["avg_margin_with_group"].empty and "AvgMargin" in kpis["avg_margin_with_group"].columns:
            try:
                df_mg = kpis["avg_margin_with_group"].nlargest(10, "AvgMargin")
                if not df_mg.empty and "StockItemName" in df_mg.columns and "AvgMargin" in df_mg.columns:
                    fig = px.bar(
                        df_mg,
                        x="StockItemName",
                        y="AvgMargin",
                        color="StockGroupName",
                        labels={
                            "StockItemName": "Product",
                            "AvgMargin": "Avg Margin",
                            "StockGroupName": "Product Group"
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_mg)
                else:
                    st.warning("Insufficient data to display the margin chart")
            except Exception as e:
                st.error(f"Error creating margin chart: {e}")
        else:
            st.warning("No margin data available for the selected date range")
            st.dataframe(kpis["avg_margin_with_group"])

    # Supplier Performance
    with tabs[2]:
        st.subheader("Top Suppliers by Quantity Received")

        if not kpis["supplier_perf"].empty and "TotalQtyReceived" in kpis["supplier_perf"].columns:
            try:
                df_sup = kpis["supplier_perf"].nlargest(20, "TotalQtyReceived")
                if not df_sup.empty and "SupplierName" in df_sup.columns:
                    fig = px.bar(df_sup, x="SupplierName", y="TotalQtyReceived")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_sup)
                else:
                    st.warning("Insufficient data to display the supplier performance chart")
            except Exception as e:
                st.error(f"Error creating supplier chart: {e}")
        else:
            st.warning("No supplier performance data available")
            st.dataframe(kpis["supplier_perf"])

    # Sales by Stock Group
    with tabs[3]:
        st.subheader("Units Sold & Profit by Stock Group")

        if not kpis["sales_by_group"].empty and "StockGroupName" in kpis["sales_by_group"].columns:
            try:
                df_sbg = kpis["sales_by_group"]
                if "TotalUnitsSold" in df_sbg.columns and "TotalProfit" in df_sbg.columns:
                    fig = px.bar(df_sbg, x="StockGroupName", y=["TotalUnitsSold", "TotalProfit"], barmode="group")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_sbg)
                else:
                    st.warning("Missing required columns for sales by group chart")
            except Exception as e:
                st.error(f"Error creating sales by group chart: {e}")
        else:
            st.warning("No sales by stock group data available for the selected date range")
            st.dataframe(kpis["sales_by_group"])

    # Customer Segments
    with tabs[4]:
        st.subheader("Quantity Shipped by Customer Category")

        if not kpis["cust_seg"].empty and "CustomerCategoryName" in kpis["cust_seg"].columns:
            try:
                df_cs = kpis["cust_seg"]
                if "TotalQtyShipped" in df_cs.columns:
                    fig = px.bar(df_cs, x="CustomerCategoryName", y="TotalQtyShipped")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_cs)
                else:
                    st.warning("Missing required columns for customer segments chart")
            except Exception as e:
                st.error(f"Error creating customer segments chart: {e}")
        else:
            st.warning("No customer segment data available")
            st.dataframe(kpis["cust_seg"])

    # Transaction Mix
    with tabs[5]:
        st.subheader("Transaction Type Distribution")

        if not kpis["txn_dist"].empty and "TransactionTypeName" in kpis["txn_dist"].columns:
            try:
                df_tx = kpis["txn_dist"]
                if "TxnCount" in df_tx.columns and not df_tx["TxnCount"].sum() == 0:
                    fig = px.pie(df_tx, names="TransactionTypeName", values="TxnCount")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_tx)
                else:
                    st.warning("No transaction count data available")
            except Exception as e:
                st.error(f"Error creating transaction distribution chart: {e}")
        else:
            st.warning("No transaction distribution data available for the selected date range")
            st.dataframe(kpis["txn_dist"])

    # Promo by Stock Group
    with tabs[6]:
        st.subheader("Deals by Stock Group")

        if not kpis["promo_by_group"].empty and "StockGroupName" in kpis["promo_by_group"].columns:
            try:
                df_ps = kpis["promo_by_group"]
                # Filter out groups with zero deals for cleaner visualization
                df_ps_filtered = df_ps[df_ps["DealCount"] > 0] if "DealCount" in df_ps.columns else df_ps

                if not df_ps_filtered.empty and "DealCount" in df_ps_filtered.columns:
                    fig = px.bar(df_ps_filtered, x="StockGroupName", y="DealCount",
                                 hover_data=["AvgDiscountPct", "AffectedItems"])
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_ps)
                else:
                    st.warning("No active deals found by stock group")
                    st.dataframe(df_ps)
            except Exception as e:
                st.error(f"Error creating promo by stock group chart: {e}")
        else:
            st.warning("⚠️ No deals by stock group—verify SalesSpecialDeals mapping.")
            st.dataframe(kpis["promo_by_group"])

    # Promo by Buying Group
    with tabs[7]:
        st.subheader("Deals by Buying Group")

        if not kpis["promo_by_buy"].empty and "BuyingGroupName" in kpis["promo_by_buy"].columns:
            try:
                df_pb = kpis["promo_by_buy"]
                # Filter out groups with zero deals
                df_pb_filtered = df_pb[df_pb["DealCount"] > 0] if "DealCount" in df_pb.columns else df_pb

                if not df_pb_filtered.empty and "DealCount" in df_pb_filtered.columns:
                    fig = px.bar(df_pb_filtered, x="BuyingGroupName", y="DealCount",
                                 hover_data=["AvgDiscountPct", "SalesDuringDeals"])
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_pb)
                else:
                    st.warning("No active deals found by buying group")
                    st.dataframe(df_pb)
            except Exception as e:
                st.error(f"Error creating promo by buying group chart: {e}")
        else:
            st.warning("⚠️ No deals by buying group—verify SalesSpecialDeals mapping.")
            st.dataframe(kpis["promo_by_buy"])

    # Tax Analysis
    with tabs[8]:
        st.subheader("Expected vs Recorded Tax by Rate")

        if not kpis["tax_variance"].empty and "TaxRate" in kpis["tax_variance"].columns:
            try:
                df_tv = kpis["tax_variance"]
                if "ExpectedTaxAmount" in df_tv.columns and "RecordedTaxAmount" in df_tv.columns:
                    fig2 = px.bar(df_tv, x="TaxRate", y=["ExpectedTaxAmount", "RecordedTaxAmount"], barmode="group")
                    st.plotly_chart(fig2, use_container_width=True)
                    st.dataframe(df_tv.style.format({
                        "ExpectedTaxAmount": "${:,.2f}",
                        "RecordedTaxAmount": "${:,.2f}",
                        "TaxVariance": "${:,.2f}"
                    }))
                else:
                    st.warning("Missing required columns for tax analysis chart")
            except Exception as e:
                st.error(f"Error creating tax analysis chart: {e}")
        else:
            st.warning("No tax variance data available for the selected date range")
            st.dataframe(kpis["tax_variance"])

    # Imbalance
    with tabs[9]:
        st.subheader("Top 10 Products by Purchase–Sales Buildup")

        if not kpis["imbalance"].empty and "StockItemName" in kpis["imbalance"].columns:
            try:
                df_im = kpis["imbalance"]
                if "NetBuildUp" in df_im.columns and "StockGroupNames" in df_im.columns:
                    fig = px.bar(df_im,
                                 x="StockItemName",
                                 y="NetBuildUp",
                                 color="StockGroupNames",
                                 hover_data=["SupplierName", "QtyPurchased", "QtySold", "PurchaseToSalesRatio"])
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_im.style.format({
                        "QtyPurchased": "{:,}",
                        "QtySold": "{:,}",
                        "NetBuildUp": "{:,}",
                        "PurchaseToSalesRatio": ".2f"
                    }))
                else:
                    st.warning("Missing required columns for product imbalance chart")
            except Exception as e:
                st.error(f"Error creating product imbalance chart: {e}")
        else:
            st.warning("No product imbalance data available for the selected date range")
            st.dataframe(kpis["imbalance"])

    st.caption("⟡ Powered by SQLite + Streamlit + Plotly (© 2025) | Developed by Ali Aydi & Mahdi Rebai")


except Exception as e:
    st.error(f"An unexpected error occurred while loading the dashboard: {e}")
    st.warning("Please make sure your database is properly initialized and contains all required tables and data.")

    # Show some debugging info
    with st.expander("🔧 Debug Information"):
        st.write("**Expected Tables:** SalesSpecialDeals, SalesBuyingGroups, WarehouseStockGroups, etc.")
        st.write("**Common Issues:**")
        st.write("- SalesSpecialDeals table is empty")
        st.write("- Table names don't match schema")
        st.write("- Missing foreign key relationships")
        st.write("- NULL values in DiscountPercentage column")