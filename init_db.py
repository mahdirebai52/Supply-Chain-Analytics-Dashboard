import sqlite3
import os
import pandas as pd
import random
from datetime import datetime, timedelta


def create_database(db_file="mydb.db"):
    """Create SQLite database with all required tables matching app.py schema"""
    # Remove existing database if it exists
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Removed existing database: {db_file}")

    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")

    # Create all tables
    print("Creating tables...")

    # Create base tables without foreign key constraints first
    create_base_tables(cursor)

    # Create tables with foreign key constraints
    create_relationship_tables(cursor)

    # Create indexes for better performance
    create_indexes(cursor)

    # Insert sample data
    print("Inserting sample data...")
    insert_sample_data(conn)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print(f"Database {db_file} created successfully!")
    return db_file


def create_base_tables(cursor):
    """Create tables without foreign key dependencies with correct naming"""

    # Create ApplicationCountries table
    cursor.execute('''
    CREATE TABLE ApplicationCountries (
        CountryID INTEGER PRIMARY KEY,
        CountryName TEXT NOT NULL
    )
    ''')

    # Create ApplicationStatesProvinces table
    cursor.execute('''
    CREATE TABLE ApplicationStatesProvinces (
        StateProvinceID INTEGER PRIMARY KEY,
        StateProvinceName TEXT NOT NULL,
        CountryID INTEGER NOT NULL,
        FOREIGN KEY (CountryID) REFERENCES ApplicationCountries(CountryID)
    )
    ''')

    # Create ApplicationCities table
    cursor.execute('''
    CREATE TABLE ApplicationCities (
        CityID INTEGER PRIMARY KEY,
        CityName TEXT NOT NULL,
        StateProvinceID INTEGER NOT NULL,
        FOREIGN KEY (StateProvinceID) REFERENCES ApplicationStatesProvinces(StateProvinceID)
    )
    ''')

    # Create ApplicationPeople table
    cursor.execute('''
    CREATE TABLE ApplicationPeople (
        PersonID INTEGER PRIMARY KEY,
        FullName TEXT NOT NULL,
        PreferredName TEXT
    )
    ''')

    # Create SalesCustomersCategories table
    cursor.execute('''
    CREATE TABLE SalesCustomersCategories (
        CustomerCategoryID INTEGER PRIMARY KEY,
        CustomerCategoryName TEXT NOT NULL
    )
    ''')

    # Create PurchasingSuppliers table
    cursor.execute('''
    CREATE TABLE PurchasingSuppliers (
        SupplierID INTEGER PRIMARY KEY,
        SupplierName TEXT NOT NULL,
        PhoneNumber TEXT,
        WebsiteURL TEXT
    )
    ''')

    # Create ApplicationDeliveryMethods table
    cursor.execute('''
    CREATE TABLE ApplicationDeliveryMethods (
        DeliveryMethodID INTEGER PRIMARY KEY,
        DeliveryMethodName TEXT NOT NULL
    )
    ''')

    # Create ApplicationTransactionTypes table
    cursor.execute('''
    CREATE TABLE ApplicationTransactionTypes (
        TransactionTypeID INTEGER PRIMARY KEY,
        TransactionTypeName TEXT NOT NULL
    )
    ''')

    # Create WarehouseStockGroups table
    cursor.execute('''
    CREATE TABLE WarehouseStockGroups (
        StockGroupID INTEGER PRIMARY KEY,
        StockGroupName TEXT NOT NULL
    )
    ''')

    # Create TaxRates table
    cursor.execute('''
    CREATE TABLE TaxRates (
        TaxRateID INTEGER PRIMARY KEY,
        TaxRate REAL NOT NULL,
        TaxRateName TEXT NOT NULL
    )
    ''')

    # Create SalesBuyingGroups table
    cursor.execute('''
    CREATE TABLE SalesBuyingGroups (
        BuyingGroupID INTEGER PRIMARY KEY,
        BuyingGroupName TEXT NOT NULL
    )
    ''')


def create_relationship_tables(cursor):
    """Create tables with foreign key relationships using correct naming"""

    # Create WarehouseStockItem table
    cursor.execute('''
    CREATE TABLE WarehouseStockItem (
        StockItemID INTEGER PRIMARY KEY,
        StockItemName TEXT NOT NULL,
        SupplierID INTEGER,
        UnitPrice REAL,
        RecommendedRetailPrice REAL,
        TypicalWeightPerUnit REAL,
        FOREIGN KEY (SupplierID) REFERENCES PurchasingSuppliers(SupplierID)
    )
    ''')

    # Create StockItemsStockGroups table - many-to-many relationship
    cursor.execute('''
    CREATE TABLE StockItemsStockGroups (
        StockItemID INTEGER,
        StockGroupID INTEGER,
        PRIMARY KEY (StockItemID, StockGroupID),
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID),
        FOREIGN KEY (StockGroupID) REFERENCES WarehouseStockGroups(StockGroupID)
    )
    ''')

    # Create SalesCustomers table
    cursor.execute('''
    CREATE TABLE SalesCustomers (
        CustomerID INTEGER PRIMARY KEY,
        CustomerName TEXT NOT NULL,
        CustomerCategoryID INTEGER,
        BuyingGroupID INTEGER,
        DeliveryCityID INTEGER,
        FOREIGN KEY (CustomerCategoryID) REFERENCES SalesCustomersCategories(CustomerCategoryID),
        FOREIGN KEY (BuyingGroupID) REFERENCES SalesBuyingGroups(BuyingGroupID),
        FOREIGN KEY (DeliveryCityID) REFERENCES ApplicationCities(CityID)
    )
    ''')

    # Create Orders table
    cursor.execute('''
    CREATE TABLE Orders (
        OrderID INTEGER PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        ExpectedDeliveryDate TEXT,
        OrderStatus INTEGER DEFAULT 0,
        Quantity INTEGER NOT NULL,
        ContactPersonID INTEGER,
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID),
        FOREIGN KEY (ContactPersonID) REFERENCES ApplicationPeople(PersonID)
    )
    ''')

    # Create Transactions table
    cursor.execute('''
    CREATE TABLE Transactions (
        TransactionID INTEGER PRIMARY KEY,
        TransactionDate TEXT NOT NULL,
        TransactionTypeID INTEGER NOT NULL,
        CustomerID INTEGER,
        SupplierID INTEGER,
        InvoiceID INTEGER,
        PurchaseOrderID INTEGER,
        PaymentMethodID INTEGER,
        Amount REAL,
        IsFinalized INTEGER DEFAULT 1,
        FOREIGN KEY (TransactionTypeID) REFERENCES ApplicationTransactionTypes(TransactionTypeID),
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID),
        FOREIGN KEY (SupplierID) REFERENCES PurchasingSuppliers(SupplierID)
    )
    ''')

    # Create SalesSpecialDeals table
    cursor.execute('''
    CREATE TABLE SalesSpecialDeals (
        SpecialDealID INTEGER PRIMARY KEY,
        StockItemID INTEGER,
        StockGroupID INTEGER,
        CustomerID INTEGER,
        BuyingGroupID INTEGER,
        DiscountPercentage REAL,
        StartDate TEXT,
        EndDate TEXT,
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID),
        FOREIGN KEY (StockGroupID) REFERENCES WarehouseStockGroups(StockGroupID),
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID),
        FOREIGN KEY (BuyingGroupID) REFERENCES SalesBuyingGroups(BuyingGroupID)
    )
    ''')

    # Create PurchaseOrders table
    cursor.execute('''
    CREATE TABLE PurchaseOrders (
        PurchaseOrderID INTEGER PRIMARY KEY,
        SupplierID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        DeliveryMethodID INTEGER,
        ContactPersonID INTEGER,
        AuthorisedPersonID INTEGER,
        ExpectedDeliveryDate TEXT,
        FOREIGN KEY (SupplierID) REFERENCES PurchasingSuppliers(SupplierID),
        FOREIGN KEY (DeliveryMethodID) REFERENCES ApplicationDeliveryMethods(DeliveryMethodID),
        FOREIGN KEY (ContactPersonID) REFERENCES ApplicationPeople(PersonID),
        FOREIGN KEY (AuthorisedPersonID) REFERENCES ApplicationPeople(PersonID)
    )
    ''')

    # Create PurchaseOrderLines table
    cursor.execute('''
    CREATE TABLE PurchaseOrderLines (
        PurchaseOrderLineID INTEGER PRIMARY KEY,
        PurchaseOrderID INTEGER NOT NULL,
        StockItemID INTEGER NOT NULL,
        OrderedOuters INTEGER NOT NULL,
        ReceivedOuters INTEGER,
        ExpectedUnitPricePerOuter REAL NOT NULL,
        LastReceiptDate TEXT,
        FOREIGN KEY (PurchaseOrderID) REFERENCES PurchaseOrders(PurchaseOrderID),
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID)
    )
    ''')

    # Create SalesInvoices table
    cursor.execute('''
    CREATE TABLE SalesInvoices (
        InvoiceID INTEGER PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        InvoiceDate TEXT NOT NULL,
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID)
    )
    ''')

    # Create SalesInvoiceLines table
    cursor.execute('''
    CREATE TABLE SalesInvoiceLines (
        InvoiceLineID INTEGER PRIMARY KEY,
        InvoiceID INTEGER NOT NULL,
        StockItemID INTEGER NOT NULL,
        Quantity INTEGER NOT NULL,
        UnitPrice REAL NOT NULL,
        ExtendedPrice REAL NOT NULL,
        TaxAmount REAL NOT NULL,
        TaxRate REAL NOT NULL,
        TaxRateID INTEGER,
        LineProfit REAL NOT NULL,
        LastEditedWhen TEXT NOT NULL,
        FOREIGN KEY (InvoiceID) REFERENCES SalesInvoices(InvoiceID),
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID),
        FOREIGN KEY (TaxRateID) REFERENCES TaxRates(TaxRateID)
    )
    ''')

    # Create StockItemTransactions table with TransactionOccurredWhen
    cursor.execute('''
    CREATE TABLE StockItemTransactions (
        StockItemTransactionID INTEGER PRIMARY KEY,
        StockItemID INTEGER NOT NULL,
        TransactionTypeID INTEGER NOT NULL,
        CustomerID INTEGER,
        SupplierID INTEGER,
        Quantity INTEGER NOT NULL,
        TransactionOccurredWhen TEXT NOT NULL,
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID),
        FOREIGN KEY (TransactionTypeID) REFERENCES ApplicationTransactionTypes(TransactionTypeID),
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID),
        FOREIGN KEY (SupplierID) REFERENCES PurchasingSuppliers(SupplierID)
    )
    ''')

    # Create StockMovements table
    cursor.execute('''
    CREATE TABLE StockMovements (
        StockMovementID INTEGER PRIMARY KEY,
        StockItemID INTEGER NOT NULL,
        MovementDate TEXT NOT NULL,
        Quantity INTEGER NOT NULL,
        MovementTypeID INTEGER,
        CustomerID INTEGER,
        SupplierID INTEGER,
        Notes TEXT,
        FOREIGN KEY (StockItemID) REFERENCES WarehouseStockItem(StockItemID),
        FOREIGN KEY (CustomerID) REFERENCES SalesCustomers(CustomerID),
        FOREIGN KEY (SupplierID) REFERENCES PurchasingSuppliers(SupplierID)
    )
    ''')


def create_indexes(cursor):
    """Create indexes for better performance"""

    cursor.execute('CREATE INDEX idx_SalesInvoiceLines_StockItemID ON SalesInvoiceLines(StockItemID)')
    cursor.execute('CREATE INDEX idx_SalesInvoiceLines_LastEditedWhen ON SalesInvoiceLines(LastEditedWhen)')
    cursor.execute('CREATE INDEX idx_StockItemTransactions_StockItemID ON StockItemTransactions(StockItemID)')
    cursor.execute('CREATE INDEX idx_StockItemTransactions_TransactionOccurredWhen ON StockItemTransactions(TransactionOccurredWhen)')
    cursor.execute('CREATE INDEX idx_StockItemTransactions_TransactionTypeID ON StockItemTransactions(TransactionTypeID)')
    cursor.execute('CREATE INDEX idx_PurchaseOrderLines_StockItemID ON PurchaseOrderLines(StockItemID)')
    cursor.execute('CREATE INDEX idx_PurchaseOrderLines_LastReceiptDate ON PurchaseOrderLines(LastReceiptDate)')
    cursor.execute('CREATE INDEX idx_SalesSpecialDeals_EndDate ON SalesSpecialDeals(EndDate)')
    cursor.execute('CREATE INDEX idx_SalesSpecialDeals_StockGroupID ON SalesSpecialDeals(StockGroupID)')
    cursor.execute('CREATE INDEX idx_SalesSpecialDeals_BuyingGroupID ON SalesSpecialDeals(BuyingGroupID)')
    cursor.execute('CREATE INDEX idx_StockMovements_MovementDate ON StockMovements(MovementDate)')
    cursor.execute('CREATE INDEX idx_StockMovements_StockItemID ON StockMovements(StockItemID)')
    cursor.execute('CREATE INDEX idx_Transactions_TransactionDate ON Transactions(TransactionDate)')
    cursor.execute('CREATE INDEX idx_Transactions_TransactionTypeID ON Transactions(TransactionTypeID)')
    cursor.execute('CREATE INDEX idx_Orders_CustomerID ON Orders(CustomerID)')
    cursor.execute('CREATE INDEX idx_Orders_OrderDate ON Orders(OrderDate)')


def insert_sample_data(conn):
    """Insert sample data into all tables with correct table names"""
    cursor = conn.cursor()

    # Insert ApplicationCountries
    countries = [
        (1, "United States"), (2, "United Kingdom"), (3, "Canada"),
        (4, "Germany"), (5, "France")
    ]
    cursor.executemany("INSERT INTO ApplicationCountries (CountryID, CountryName) VALUES (?, ?)", countries)

    # Insert ApplicationStatesProvinces
    states = [
        (1, "California", 1), (2, "New York", 1), (3, "Texas", 1),
        (4, "England", 2), (5, "Scotland", 2),
        (6, "Ontario", 3), (7, "Quebec", 3),
        (8, "Bavaria", 4), (9, "Ile-de-France", 5)
    ]
    cursor.executemany("INSERT INTO ApplicationStatesProvinces (StateProvinceID, StateProvinceName, CountryID) VALUES (?, ?, ?)", states)

    # Insert ApplicationCities
    cities = [
        (1, "Los Angeles", 1), (2, "San Francisco", 1), (3, "New York City", 2),
        (4, "London", 4), (5, "Manchester", 4), (6, "Edinburgh", 5),
        (7, "Toronto", 6), (8, "Montreal", 7),
        (9, "Munich", 8), (10, "Paris", 9)
    ]
    cursor.executemany("INSERT INTO ApplicationCities (CityID, CityName, StateProvinceID) VALUES (?, ?, ?)", cities)

    # Insert ApplicationPeople
    people = [
        (1, "John Smith", "John"), (2, "Jane Doe", "Jane"),
        (3, "Robert Johnson", "Rob"), (4, "Sarah Williams", "Sarah")
    ]
    cursor.executemany("INSERT INTO ApplicationPeople (PersonID, FullName, PreferredName) VALUES (?, ?, ?)", people)

    # Insert SalesCustomersCategories
    customer_categories = [
        (1, "Retail"), (2, "Wholesale"), (3, "Corporate"), (4, "Government")
    ]
    cursor.executemany("INSERT INTO SalesCustomersCategories (CustomerCategoryID, CustomerCategoryName) VALUES (?, ?)",
                       customer_categories)

    # Insert SalesBuyingGroups
    buying_groups = [
        (1, "Premium"), (2, "Standard"), (3, "Budget"), (4, "Luxury")
    ]
    cursor.executemany("INSERT INTO SalesBuyingGroups (BuyingGroupID, BuyingGroupName) VALUES (?, ?)", buying_groups)

    # Insert PurchasingSuppliers
    suppliers = [
        (1, "Acme Supplies", "555-123-4567", "www.acmesupplies.com"),
        (2, "Global Distributors", "555-234-5678", "www.globaldist.com"),
        (3, "Tech Parts Inc.", "555-345-6789", "www.techparts.com"),
        (4, "Wholesale Direct", "555-456-7890", "www.wholesaledirect.com"),
        (5, "Quality Products", "555-567-8901", "www.qualityproducts.com")
    ]
    cursor.executemany("INSERT INTO PurchasingSuppliers (SupplierID, SupplierName, PhoneNumber, WebsiteURL) VALUES (?, ?, ?, ?)",
                       suppliers)

    # Insert ApplicationDeliveryMethods
    delivery_methods = [
        (1, "Standard"), (2, "Express"), (3, "Overnight")
    ]
    cursor.executemany("INSERT INTO ApplicationDeliveryMethods (DeliveryMethodID, DeliveryMethodName) VALUES (?, ?)",
                       delivery_methods)

    # Insert ApplicationTransactionTypes
    transaction_types = [
        (1, "Sales"), (2, "Purchase"), (3, "Stock Receipt"), (4, "Stock Issue"),
        (5, "Customer Payment"), (6, "Supplier Payment"), (7, "Stock Transfer"),
        (8, "Customer Refund"), (9, "Supplier Refund"), (10, "Stock Issue"),
        (11, "Stock Receipt"), (12, "Customer Returns"), (13, "Supplier Returns")
    ]
    cursor.executemany("INSERT INTO ApplicationTransactionTypes (TransactionTypeID, TransactionTypeName) VALUES (?, ?)",
                       transaction_types)

    # Insert WarehouseStockGroups
    stock_groups = [
        (1, "Electronics"), (2, "Clothing"), (3, "Food"),
        (4, "Books"), (5, "Furniture")
    ]
    cursor.executemany("INSERT INTO WarehouseStockGroups (StockGroupID, StockGroupName) VALUES (?, ?)", stock_groups)

    # Insert TaxRates
    tax_rates = [
        (1, 0.0, "Tax Exempt"), (2, 8.0, "Standard Rate"),
        (3, 5.0, "Reduced Rate"), (4, 20.0, "Higher Rate")
    ]
    cursor.executemany("INSERT INTO TaxRates (TaxRateID, TaxRate, TaxRateName) VALUES (?, ?, ?)", tax_rates)

    # Insert WarehouseStockItem
    stock_items = [
        (1, "Laptop Computer", 1, 800.00, 1200.00, 2.5),
        (2, "Smartphone Device", 1, 400.00, 699.99, 0.3),
        (3, "Tablet Computer", 3, 200.00, 349.99, 0.5),
        (4, "Cotton T-shirt", 2, 8.00, 19.99, 0.2),
        (5, "Denim Jeans", 2, 20.00, 49.99, 0.8),
        (6, "Premium Coffee", 3, 6.00, 12.99, 0.5),
        (7, "Herbal Tea", 3, 3.00, 7.99, 0.1),
        (8, "Dark Chocolate", 4, 2.00, 4.99, 0.2),
        (9, "Fiction Novel", 4, 5.00, 14.99, 0.4),
        (10, "Technical Textbook", 4, 30.00, 79.99, 1.5),
        (11, "Office Chair", 5, 40.00, 99.99, 5.0),
        (12, "Study Desk", 5, 100.00, 249.99, 20.0),
        (13, "Wooden Bookshelf", 5, 70.00, 179.99, 15.0)
    ]
    cursor.executemany(
        "INSERT INTO WarehouseStockItem (StockItemID, StockItemName, SupplierID, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit) VALUES (?, ?, ?, ?, ?, ?)",
        stock_items)

    # Insert StockItemsStockGroups
    stock_item_groups = [
        (1, 1), (2, 1), (3, 1),  # Electronics
        (4, 2), (5, 2),  # Clothing
        (6, 3), (7, 3), (8, 3),  # Food
        (9, 4), (10, 4),  # Books
        (11, 5), (12, 5), (13, 5)  # Furniture
    ]
    cursor.executemany("INSERT INTO StockItemsStockGroups (StockItemID, StockGroupID) VALUES (?, ?)", stock_item_groups)

    # Insert SalesCustomers
    customers = [
        (1, "ABC Corporation", 3, 1, 1),
        (2, "Local Shop Ltd", 1, 2, 2),
        (3, "Big Retailer Inc", 2, 3, 4),
        (4, "Government Agency", 4, None, 3),
        (5, "Small Business Co", 1, 2, 5),
        (6, "Tech Startup LLC", 3, 1, 1),
        (7, "Fashion Boutique", 1, 4, 2),
        (8, "Grocery Chain Corp", 2, 2, 4)
    ]
    cursor.executemany(
        "INSERT INTO SalesCustomers (CustomerID, CustomerName, CustomerCategoryID, BuyingGroupID, DeliveryCityID) VALUES (?, ?, ?, ?, ?)",
        customers)

    # Insert SalesSpecialDeals with meaningful discounts
    deals = [
        (1, 1, None, 1, 1, 10.0, "2013-01-01", "2016-12-31"),
        (2, None, 1, 2, 2, 15.0, "2013-01-01", "2016-12-31"),
        (3, 4, None, 3, None, 5.0, "2013-01-01", "2016-12-31"),
        (4, None, 2, 4, 4, 12.5, "2013-01-01", "2016-12-31"),
        (5, 7, None, 5, 1, 7.5, "2013-01-01", "2016-12-31"),
        (6, 2, None, 6, 3, 20.0, "2013-01-01", "2016-12-31"),
        (7, 3, None, 7, 2, 18.0, "2013-01-01", "2016-12-31"),
        (8, 5, None, 8, 4, 25.0, "2013-01-01", "2016-12-31"),
        (9, None, 3, 1, 1, 8.0, "2013-01-01", "2016-12-31"),
        (10, None, 4, 2, 2, 10.0, "2013-01-01", "2016-12-31"),
        (11, 9, None, 3, 3, 6.0, "2013-01-01", "2016-12-31"),
        (12, None, 5, 4, 4, 14.0, "2013-01-01", "2016-12-31"),
        (13, 11, None, 5, 1, 9.0, "2013-01-01", "2016-12-31"),
        (14, 12, None, 6, 2, 22.0, "2013-01-01", "2016-12-31"),
        (15, None, 1, 7, 3, 11.0, "2013-01-01", "2016-12-31")
    ]
    cursor.executemany(
        "INSERT INTO SalesSpecialDeals (SpecialDealID, StockItemID, StockGroupID, CustomerID, BuyingGroupID, DiscountPercentage, StartDate, EndDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        deals)

    # Generate data for years 2013-2016 to match app's default date range
    years = [2013, 2014, 2015, 2016]

    # Insert PurchaseOrders
    purchase_orders = []
    po_id = 1
    for year in years:
        for month in range(1, 13):
            for supplier_id in range(1, 6):
                order_date = f"{year}-{month:02d}-15"
                delivery_date = f"{year}-{month:02d}-25"
                purchase_orders.append((
                    po_id,
                    supplier_id,
                    order_date,
                    random.randint(1, 3),  # DeliveryMethodID
                    random.randint(1, 4),  # ContactPersonID
                    random.randint(1, 4),  # AuthorisedPersonID
                    delivery_date
                ))
                po_id += 1

    cursor.executemany(
        "INSERT INTO PurchaseOrders (PurchaseOrderID, SupplierID, OrderDate, DeliveryMethodID, ContactPersonID, AuthorisedPersonID, ExpectedDeliveryDate) VALUES (?, ?, ?, ?, ?, ?, ?)",
        purchase_orders)

    # Insert PurchaseOrderLines
    po_lines = []
    pol_id = 1
    for po in purchase_orders:
        # Each PO has 2-4 line items
        for _ in range(random.randint(2, 4)):
            stock_id = random.randint(1, 13)
            ordered = random.randint(10, 100)
            received = ordered - random.randint(0, 5)  # Sometimes receive less

            # Get unit price for the stock item
            cursor.execute("SELECT UnitPrice FROM WarehouseStockItem WHERE StockItemID = ?", (stock_id,))
            unit_price = cursor.fetchone()[0]

            po_lines.append((
                pol_id,
                po[0],  # PurchaseOrderID
                stock_id,
                ordered,
                received,
                unit_price * 0.7,  # Wholesale price (70% of retail)
                po[6]  # ExpectedDeliveryDate as LastReceiptDate
            ))
            pol_id += 1

    cursor.executemany(
        "INSERT INTO PurchaseOrderLines (PurchaseOrderLineID, PurchaseOrderID, StockItemID, OrderedOuters, ReceivedOuters, ExpectedUnitPricePerOuter, LastReceiptDate) VALUES (?, ?, ?, ?, ?, ?, ?)",
        po_lines)

    # Insert Orders
    orders = []
    order_id = 1
    for year in years:
        for month in range(1, 13):
            for customer_id in range(1, 9):  # 8 customers
                # Each customer has 1-3 orders per month
                for _ in range(random.randint(1, 3)):
                    day = random.randint(1, 28)
                    order_date = f"{year}-{month:02d}-{day:02d}"

                    delivery_date = datetime.strptime(order_date, "%Y-%m-%d") + timedelta(days=random.randint(5, 10))
                    delivery_date = delivery_date.strftime("%Y-%m-%d")

                    quantity = random.randint(1, 50)
                    contact_id = random.randint(1, 4)
                    status = random.randint(0, 4)

                    orders.append((
                        order_id,
                        customer_id,
                        order_date,
                        delivery_date,
                        status,
                        quantity,
                        contact_id
                    ))
                    order_id += 1

    cursor.executemany(
        "INSERT INTO Orders (OrderID, CustomerID, OrderDate, ExpectedDeliveryDate, OrderStatus, Quantity, ContactPersonID) VALUES (?, ?, ?, ?, ?, ?, ?)",
        orders)

    # Insert SalesInvoices
    invoices = []
    invoice_id = 1
    for year in years:
        for month in range(1, 13):
            for customer_id in range(1, 9):
                # Each customer gets 1-2 invoices per month
                for _ in range(random.randint(1, 2)):
                    day = random.randint(1, 28)
                    invoice_date = f"{year}-{month:02d}-{day:02d}"
                    invoices.append((
                        invoice_id,
                        customer_id,
                        invoice_date
                    ))
                    invoice_id += 1

    cursor.executemany("INSERT INTO SalesInvoices (InvoiceID, CustomerID, InvoiceDate) VALUES (?, ?, ?)", invoices)

    # Insert SalesInvoiceLines
    invoice_lines = []
    line_id = 1

    for invoice in invoices:
        # Each invoice has 2-5 line items
        for _ in range(random.randint(2, 5)):
            stock_id = random.randint(1, 13)
            quantity = random.randint(1, 20)

            # Get the unit price from the WarehouseStockItem table
            cursor.execute("SELECT UnitPrice FROM WarehouseStockItem WHERE StockItemID = ?", (stock_id,))
            unit_price = cursor.fetchone()[0]

            extended_price = quantity * unit_price
            tax_rate = 8.0  # 8% tax rate
            tax_amount = extended_price * (tax_rate / 100.0)
            profit_margin = random.uniform(0.2, 0.4)  # 20-40% profit margin
            line_profit = extended_price * profit_margin

            # Use invoice date as LastEditedWhen
            edited_date = invoice[2]

            invoice_lines.append((
                line_id, invoice[0], stock_id, quantity,
                unit_price, extended_price, tax_amount, tax_rate, 2,
                line_profit, edited_date
            ))
            line_id += 1

    cursor.executemany(
        "INSERT INTO SalesInvoiceLines (InvoiceLineID, InvoiceID, StockItemID, Quantity, UnitPrice, ExtendedPrice, TaxAmount, TaxRate, TaxRateID, LineProfit, LastEditedWhen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        invoice_lines)

    # Insert StockItemTransactions with TransactionOccurredWhen
    stock_transactions = []
    transaction_id = 1

    # Stock receipts (matching purchase orders)
    for po_line in po_lines:
        stock_transactions.append((
            transaction_id,
            po_line[2],  # Stock item ID
            11,  # Receipt transaction type
            None,  # No customer
            po_line[1] % 5 + 1,  # Supplier ID
            po_line[4],  # Quantity from ReceivedOuters
            po_line[6]  # Last receipt date as TransactionOccurredWhen
        ))
        transaction_id += 1

    # Stock issues (matching invoices)
    for line in invoice_lines:
        stock_transactions.append((
            transaction_id,
            line[2],  # Stock item ID
            10,  # Issue transaction type
            line[1] % 8 + 1,  # Customer ID (based on invoice)
            None,  # No supplier
            -line[3],  # Negative quantity (issue)
            line[10]  # Date from LastEditedWhen as TransactionOccurredWhen
        ))
        transaction_id += 1

    cursor.executemany(
        "INSERT INTO StockItemTransactions (StockItemTransactionID, StockItemID, TransactionTypeID, CustomerID, SupplierID, Quantity, TransactionOccurredWhen) VALUES (?, ?, ?, ?, ?, ?, ?)",
        stock_transactions)

    # Insert StockMovements
    stock_movements = []
    movement_id = 1

    # Create stock movements (based on transactions)
    for transaction in stock_transactions:
        movement_type = 1 if transaction[5] > 0 else 2  # 1 for inbound, 2 for outbound
        stock_movements.append((
            movement_id,
            transaction[1],  # Stock item ID
            transaction[6],  # Transaction date
            abs(transaction[5]),  # Use absolute value for quantity
            movement_type,
            transaction[3],  # Customer ID
            transaction[4],  # Supplier ID
            f"Stock movement for item {transaction[1]}"
        ))
        movement_id += 1

    cursor.executemany(
        "INSERT INTO StockMovements (StockMovementID, StockItemID, MovementDate, Quantity, MovementTypeID, CustomerID, SupplierID, Notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        stock_movements)

    # Insert Transactions
    transactions = []
    trans_id = 1

    # Create transactions for invoices (sales transactions)
    for invoice in invoices:
        # Get the total amount from SalesInvoiceLines
        cursor.execute("""
            SELECT SUM(ExtendedPrice) 
            FROM SalesInvoiceLines 
            WHERE InvoiceID = ?
        """, (invoice[0],))
        result = cursor.fetchone()
        total_amount = result[0] if result and result[0] else 0

        transactions.append((
            trans_id,
            invoice[2],  # Invoice date
            1,  # Sales transaction type
            invoice[1],  # Customer ID
            None,  # No supplier
            invoice[0],  # Invoice ID
            None,  # No purchase order
            1,  # Payment method (1 = cash, for example)
            total_amount,
            1  # Finalized
        ))
        trans_id += 1

    # Create transactions for purchase orders (purchase transactions)
    for po in purchase_orders:
        # Get the total amount from PurchaseOrderLines
        cursor.execute("""
            SELECT SUM(OrderedOuters * ExpectedUnitPricePerOuter) 
            FROM PurchaseOrderLines 
            WHERE PurchaseOrderID = ?
        """, (po[0],))
        result = cursor.fetchone()
        total_amount = result[0] if result and result[0] else 0

        transactions.append((
            trans_id,
            po[2],  # Order date
            2,  # Purchase transaction type
            None,  # No customer
            po[1],  # Supplier ID
            None,  # No invoice
            po[0],  # Purchase order ID
            1,  # Payment method
            total_amount,
            1  # Finalized
        ))
        trans_id += 1

    # Add stock transactions to Transactions table
    for st in stock_transactions:
        # Calculate amount based on stock item price
        cursor.execute("SELECT UnitPrice FROM WarehouseStockItem WHERE StockItemID = ?", (st[1],))
        result = cursor.fetchone()
        unit_price = result[0] if result and result[0] else 0
        amount = abs(st[5]) * unit_price  # Use absolute value of quantity

        transactions.append((
            trans_id,
            st[6],  # Transaction date
            st[2],  # Transaction type ID (10 or 11)
            st[3],  # Customer ID
            st[4],  # Supplier ID
            None,  # No invoice
            None,  # No purchase order
            1,  # Payment method
            amount,
            1  # Finalized
        ))
        trans_id += 1

    cursor.executemany(
        "INSERT INTO Transactions (TransactionID, TransactionDate, TransactionTypeID, CustomerID, SupplierID, InvoiceID, PurchaseOrderID, PaymentMethodID, Amount, IsFinalized) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        transactions)

    # Commit the changes
    conn.commit()
    print(f"Inserted {len(invoices)} invoices with {len(invoice_lines)} invoice lines")
    print(f"Inserted {len(purchase_orders)} purchase orders with {len(po_lines)} purchase order lines")
    print(f"Inserted {len(orders)} orders")
    print(f"Inserted {len(stock_transactions)} stock transactions")
    print(f"Inserted {len(stock_movements)} stock movements")
    print(f"Inserted {len(transactions)} transactions")
    print(f"Inserted {len(deals)} special deals with meaningful discount percentages")


if __name__ == "__main__":
    create_database()
    print("Database created successfully with corrected table names matching app.py schema.")
    print("Generated comprehensive sample data for years 2013-2016.")
    print("✅ SalesSpecialDeals table populated with deals across all stock groups and buying groups.")
    print("✅ All table names match app.py expectations.")
    print("✅ TransactionOccurredWhen column added to StockItemTransactions.")
    print("Your Streamlit app should now work without errors and show meaningful deal data!")