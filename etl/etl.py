import pandas as pd
from sqlalchemy import create_engine, text, Integer, Float, DateTime, DECIMAL
from sqlalchemy.types import NVARCHAR
import urllib
import os

# --- CONFIGURATION ---
SERVER_NAME = 'localhost' 
DB_NAME = 'DWH_Northwind'
DRIVER = 'ODBC Driver 17 for SQL Server'

# Paths for dual data sources
NORTHWIND_PATH = 'data/northwind'  # Your existing 20-table database
SQLSERVER_PATH = r'C:\Users\ADMIN\Desktop\BI Project\data\SQLserver\csv'  # New SQL Server 13 tables

params = urllib.parse.quote_plus(f"DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DB_NAME};Trusted_Connection=yes;")
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- HELPER FUNCTIONS ---
def get_existing_dimension_keys(conn, table_name, key_column):
    """Fetch existing keys from a dimension table."""
    query = text(f"SELECT {key_column} FROM {table_name}")
    result = conn.execute(query)
    return set(row[0] for row in result.fetchall())

def validate_and_fix_foreign_keys(df, fact_key_column, dim_table, dim_key_column, placeholder_name="Unknown"):
    """
    Validate foreign keys and fix orphaned references by:
    1. Adding missing dimension entries
    2. OR removing invalid records (configurable)
    """
    with engine.connect() as conn:
        existing_keys = get_existing_dimension_keys(conn, dim_table, dim_key_column)
        
        # Find orphaned keys
        orphaned_keys = set(df[fact_key_column].dropna().unique()) - existing_keys
        
        if len(orphaned_keys) > 0:
            print(f"⚠️  Found {len(orphaned_keys)} orphaned {dim_table} keys")
            print(f"   Orphaned IDs: {sorted(list(orphaned_keys))[:10]}")  # Show first 10
            
            # Option 1: Add placeholder entries (recommended for employees)
            if dim_table == 'DWH_TFOUTD_Dim_Employee':
                print(f"   Adding placeholder employees to {dim_table}...")
                
                # Create placeholder rows
                placeholder_data = []
                for key in orphaned_keys:
                    placeholder_data.append({
                        'EmployeeID': int(key),
                        'Company': 'Unknown',
                        'FirstName': placeholder_name,
                        'LastName': f'ID_{key}',
                        'JobTitle': 'Unknown'
                    })
                
                if placeholder_data:
                    placeholder_df = pd.DataFrame(placeholder_data)
                    placeholder_df.to_sql(
                        dim_table, 
                        engine, 
                        if_exists='append', 
                        index=False,
                        dtype={
                            'EmployeeID': Integer,
                            'Company': NVARCHAR(50),
                            'FirstName': NVARCHAR(50),
                            'LastName': NVARCHAR(50),
                            'JobTitle': NVARCHAR(50)
                        }
                    )
                    print(f"✓ Added {len(placeholder_data)} placeholder records to {dim_table}")
            
            # Option 2: Remove orphaned records (for other dimensions if needed)
            else:
                print(f"   Removing {len(orphaned_keys)} orphaned records...")
                df = df[df[fact_key_column].isin(existing_keys) | df[fact_key_column].isna()]
        
        return df

def clear_all_tables():
    """Clear all data from tables before loading new data."""
    print("\n🧹 Clearing all existing data from tables...")
    
    with engine.connect() as conn:
        # Clear data in correct order (facts first due to foreign keys)
        clear_queries = [
            "DELETE FROM DWH_TFOUTD_Sales_Fact",
            "DELETE FROM DWH_TFOUTD_Purchases_Fact",
            "DELETE FROM DWH_TFOUTD_Dim_Product",
            "DELETE FROM DWH_TFOUTD_Dim_Customer",
            "DELETE FROM DWH_TFOUTD_Dim_Employee",
            "DELETE FROM DWH_TFOUTD_Dim_Supplier"
        ]
        
        for query in clear_queries:
            result = conn.execute(text(query))
            deleted_count = result.rowcount
            if deleted_count > 0:
                table_name = query.split()[-1]
                print(f"✓ Cleared {deleted_count} rows from {table_name}")
            conn.commit()
    
    print("✓ All tables cleared, ready for fresh ETL load")

def ensure_schema_exists():
    """Check if tables exist, create them if they don't."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('DWH_TFOUTD_Dim_Product', 'DWH_TFOUTD_Dim_Customer', 'DWH_TFOUTD_Dim_Employee', 'DWH_TFOUTD_Dim_Supplier', 'DWH_TFOUTD_Sales_Fact', 'DWH_TFOUTD_Purchases_Fact')
        """))
        count = result.fetchone()[0]
        
        if count < 6:
            print("\n⚠️  WARNING: Not all tables exist!")
            print(f"Found {count}/6 tables. Creating missing tables...\n")
            
            # Create the schema
            conn.execute(text("""
                -- Drop existing tables in correct order
                IF OBJECT_ID('DWH_TFOUTD_Sales_Fact', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Sales_Fact;
                IF OBJECT_ID('DWH_TFOUTD_Purchases_Fact', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Purchases_Fact;
                IF OBJECT_ID('DWH_TFOUTD_Dim_Product', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Dim_Product;
                IF OBJECT_ID('DWH_TFOUTD_Dim_Customer', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Dim_Customer;
                IF OBJECT_ID('DWH_TFOUTD_Dim_Supplier', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Dim_Supplier;
                IF OBJECT_ID('DWH_TFOUTD_Dim_Employee', 'U') IS NOT NULL DROP TABLE DWH_TFOUTD_Dim_Employee;
            """))
            conn.commit()
            
            conn.execute(text("""
                CREATE TABLE DWH_TFOUTD_Dim_Product (
                    ProductID INT PRIMARY KEY,
                    ProductCode NVARCHAR(50),
                    ProductName NVARCHAR(100),
                    Category NVARCHAR(50),
                    StandardCost DECIMAL(18,4),
                    ListPrice DECIMAL(18,4),
                    ReorderLevel INT
                );

                CREATE TABLE DWH_TFOUTD_Dim_Customer (
                    CustomerID INT PRIMARY KEY,
                    Company NVARCHAR(50),
                    FirstName NVARCHAR(50),
                    LastName NVARCHAR(50),
                    City NVARCHAR(50),
                    CountryRegion NVARCHAR(50)
                );

                CREATE TABLE DWH_TFOUTD_Dim_Supplier (
                    SupplierID INT PRIMARY KEY,
                    Company NVARCHAR(50),
                    FirstName NVARCHAR(50),
                    LastName NVARCHAR(50),
                    City NVARCHAR(50),
                    CountryRegion NVARCHAR(50)
                );

                CREATE TABLE DWH_TFOUTD_Dim_Employee (
                    EmployeeID INT PRIMARY KEY,
                    Company NVARCHAR(50),
                    FirstName NVARCHAR(50),
                    LastName NVARCHAR(50),
                    JobTitle NVARCHAR(50)
                );

                CREATE TABLE DWH_TFOUTD_Sales_Fact (
                    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
                    OrderDate DATETIME NOT NULL,
                    CustomerKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Customer(CustomerID),
                    EmployeeKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Employee(EmployeeID),
                    ProductKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Product(ProductID),
                    Quantity INT NOT NULL,
                    UnitPrice DECIMAL(18,4) NOT NULL,
                    Discount FLOAT NOT NULL,
                    TaxRate FLOAT NOT NULL,
                    TotalRevenue DECIMAL(18,4) NOT NULL,
                    FreightCost DECIMAL(18,4),
                    OrderStatus NVARCHAR(50)
                );

                CREATE TABLE DWH_TFOUTD_Purchases_Fact (
                    PurchaseKey INT IDENTITY(1,1) PRIMARY KEY,
                    CreationDate DATETIME NOT NULL,
                    SupplierKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Supplier(SupplierID),
                    EmployeeKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Employee(EmployeeID),
                    ProductKey INT FOREIGN KEY REFERENCES DWH_TFOUTD_Dim_Product(ProductID),
                    Quantity INT NOT NULL,
                    UnitCost DECIMAL(18,4) NOT NULL,
                    TotalPurchaseCost DECIMAL(18,4) NOT NULL
                );
            """))
            conn.commit()
            
            print("✓ All tables created with correct schema!\n")
        else:
            print(f"✓ All 6 tables already exist. Proceeding with ETL...\n")

def extract_csv(filename, source='northwind'):
    """
    Reads a CSV file from either data source.
    
    Args:
        filename: Name of the CSV file
        source: 'northwind' or 'sqlserver'
    """
    try:
        if source == 'northwind':
            path = os.path.join(NORTHWIND_PATH, filename)
        else:
            path = os.path.join(SQLSERVER_PATH, filename)
        
        df = pd.read_csv(path, encoding='latin1')
        
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        
        print(f"✓ [{source.upper()}] Extracted {len(df)} rows from {filename}")
        return df
    except Exception as e:
        print(f"✗ [{source.upper()}] Error reading {filename}: {e}")
        return None

def load_to_sql(df, table_name, dtype_dict=None):
    """Loads a pandas DataFrame into SQL Server with explicit data types."""
    if df is None or df.empty:
        print(f"✗ Skipping {table_name}: DataFrame is empty.")
        return False
        
    try:
        print(f"→ Loading {len(df)} rows into {table_name}...")
        
        # Show first row to verify data
        print(f"  Sample row: {df.iloc[0].to_dict()}")
        
        if dtype_dict:
            df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_dict, method='multi', chunksize=1000)
        else:
            df.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        # Verify the load
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table_name}"))
            count = result.fetchone()[0]
            print(f"✓ SUCCESS: {count} total rows now in {table_name}")
        
        return True
    except Exception as e:
        print(f"✗ FAILED to load {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== NEW: SQL SERVER DATA EXTRACTION ====================

def extract_sqlserver_data():
    """
    Extract and consolidate data from SQL Server CSV files (13 tables).
    This handles the heterogeneous data from the SQL Server database.
    """
    print("\n" + "="*60)
    print("EXTRACTING SQL SERVER DATA (13 TABLES)")
    print("="*60)
    
    sqlserver_data = {}
    
    # List of SQL Server tables
    sqlserver_tables = [
        'Categories',
        'CustomerCustomerDemo',
        'CustomerDemographics',
        'Customers',
        'Employees',
        'EmployeeTerritories',
        'Order Details',
        'Orders',
        'Products',
        'Region',
        'Shippers',
        'Suppliers',
        'Territories'
    ]
    
    for table_name in sqlserver_tables:
        df = extract_csv(f'{table_name}.csv', source='sqlserver')
        if df is not None:
            sqlserver_data[table_name] = df
    
    print(f"\n✓ Loaded {len(sqlserver_data)} SQL Server tables")
    return sqlserver_data

# ==================== DIMENSION TRANSFORMATION (MERGED FROM BOTH SOURCES) ====================

def transform_dimensions_merged(sqlserver_data):
    """
    Transform dimensions by merging data from BOTH sources:
    1. Northwind database (20 tables)
    2. SQL Server database (13 tables)
    
    This handles heterogeneity between the two data sources.
    """
    print("\n" + "="*60)
    print("STEP 1: TRANSFORMING DIMENSION TABLES (MERGED SOURCES)")
    print("="*60)
    
    # ===== 1. PRODUCTS - Merge from both sources =====
    print("\n--- Merging Products from both sources ---")
    
    # Source 1: Northwind
    products_nw = extract_csv('Products.csv', source='northwind')
    
    # Source 2: SQL Server
    products_sql = sqlserver_data.get('Products')
    
    # Transform Northwind products
    if products_nw is not None:
        dim_product_nw = products_nw[['ID', 'Product Code', 'Product Name', 'Category', 'Standard Cost', 'List Price', 'Reorder Level']].copy()
        dim_product_nw.columns = ['ProductID', 'ProductCode', 'ProductName', 'Category', 'StandardCost', 'ListPrice', 'ReorderLevel']
        dim_product_nw['ProductID'] = pd.to_numeric(dim_product_nw['ProductID'], errors='coerce').astype('Int64')
        dim_product_nw = dim_product_nw.dropna(subset=['ProductID'])
        print(f"  ✓ Northwind Products: {len(dim_product_nw)} records")
    else:
        dim_product_nw = pd.DataFrame()
    
    # Transform SQL Server products
    if products_sql is not None:
        # Handle different column names in SQL Server version
        sql_cols = {}
        if 'ProductID' in products_sql.columns:
            sql_cols['ProductID'] = 'ProductID'
        elif 'Product ID' in products_sql.columns:
            sql_cols['Product ID'] = 'ProductID'
        
        if 'ProductName' in products_sql.columns:
            sql_cols['ProductName'] = 'ProductName'
        elif 'Product Name' in products_sql.columns:
            sql_cols['Product Name'] = 'ProductName'
        
        # Add category from Categories table if available
        if 'Categories' in sqlserver_data and 'CategoryID' in products_sql.columns:
            categories = sqlserver_data['Categories']
            products_sql = products_sql.merge(
                categories[['CategoryID', 'CategoryName']], 
                on='CategoryID', 
                how='left'
            )
        
        dim_product_sql = pd.DataFrame()
        dim_product_sql['ProductID'] = pd.to_numeric(products_sql.get('ProductID', products_sql.get('Product ID')), errors='coerce').astype('Int64')
        dim_product_sql['ProductCode'] = products_sql.get('ProductCode', '')
        dim_product_sql['ProductName'] = products_sql.get('ProductName', products_sql.get('Product Name', ''))
        dim_product_sql['Category'] = products_sql.get('CategoryName', 'Unknown')
        dim_product_sql['StandardCost'] = pd.to_numeric(products_sql.get('StandardCost', 0), errors='coerce')
        dim_product_sql['ListPrice'] = pd.to_numeric(products_sql.get('UnitPrice', products_sql.get('List Price', 0)), errors='coerce')
        dim_product_sql['ReorderLevel'] = pd.to_numeric(products_sql.get('ReorderLevel', 0), errors='coerce').astype('Int64')
        
        dim_product_sql = dim_product_sql.dropna(subset=['ProductID'])
        print(f"  ✓ SQL Server Products: {len(dim_product_sql)} records")
    else:
        dim_product_sql = pd.DataFrame()
    
    # Merge products (remove duplicates, prioritize Northwind)
    if not dim_product_nw.empty and not dim_product_sql.empty:
        # Add offset to SQL Server IDs to avoid conflicts
        max_nw_id = dim_product_nw['ProductID'].max()
        dim_product_sql['ProductID'] = dim_product_sql['ProductID'] + max_nw_id + 1000
        dim_product = pd.concat([dim_product_nw, dim_product_sql], ignore_index=True)
        print(f"  ✓ MERGED Products: {len(dim_product)} total records")
    elif not dim_product_nw.empty:
        dim_product = dim_product_nw
    else:
        dim_product = dim_product_sql
    
    dtype_product = {
        'ProductID': Integer,
        'ProductCode': NVARCHAR(50),
        'ProductName': NVARCHAR(100),
        'Category': NVARCHAR(50),
        'StandardCost': DECIMAL(18, 4),
        'ListPrice': DECIMAL(18, 4),
        'ReorderLevel': Integer
    }
    
    # ===== 2. CUSTOMERS - Merge from both sources =====
    print("\n--- Merging Customers from both sources ---")
    
    customers_nw = extract_csv('Customers.csv', source='northwind')
    customers_sql = sqlserver_data.get('Customers')
    
    # Transform Northwind customers
    if customers_nw is not None:
        dim_customer_nw = customers_nw[['ID', 'Company', 'First Name', 'Last Name', 'City', 'Country/Region']].copy()
        dim_customer_nw.columns = ['CustomerID', 'Company', 'FirstName', 'LastName', 'City', 'CountryRegion']
        dim_customer_nw['CustomerID'] = pd.to_numeric(dim_customer_nw['CustomerID'], errors='coerce').astype('Int64')
        dim_customer_nw = dim_customer_nw.dropna(subset=['CustomerID'])
        print(f"  ✓ Northwind Customers: {len(dim_customer_nw)} records")
    else:
        dim_customer_nw = pd.DataFrame()
    
    # Transform SQL Server customers
    if customers_sql is not None:
        dim_customer_sql = pd.DataFrame()
        dim_customer_sql['CustomerID'] = pd.to_numeric(customers_sql.get('CustomerID', customers_sql.get('Customer ID')), errors='coerce').astype('Int64')
        dim_customer_sql['Company'] = customers_sql.get('CompanyName', customers_sql.get('Company', ''))
        dim_customer_sql['FirstName'] = customers_sql.get('ContactName', '').str.split().str[0] if 'ContactName' in customers_sql.columns else ''
        dim_customer_sql['LastName'] = customers_sql.get('ContactName', '').str.split().str[-1] if 'ContactName' in customers_sql.columns else ''
        dim_customer_sql['City'] = customers_sql.get('City', '')
        dim_customer_sql['CountryRegion'] = customers_sql.get('Country', customers_sql.get('CountryRegion', ''))
        
        dim_customer_sql = dim_customer_sql.dropna(subset=['CustomerID'])
        print(f"  ✓ SQL Server Customers: {len(dim_customer_sql)} records")
    else:
        dim_customer_sql = pd.DataFrame()
    
    # Merge customers
    if not dim_customer_nw.empty and not dim_customer_sql.empty:
        max_nw_id = dim_customer_nw['CustomerID'].max()
        dim_customer_sql['CustomerID'] = dim_customer_sql['CustomerID'] + max_nw_id + 1000
        dim_customer = pd.concat([dim_customer_nw, dim_customer_sql], ignore_index=True)
        print(f"  ✓ MERGED Customers: {len(dim_customer)} total records")
    elif not dim_customer_nw.empty:
        dim_customer = dim_customer_nw
    else:
        dim_customer = dim_customer_sql
    
    dtype_customer = {
        'CustomerID': Integer,
        'Company': NVARCHAR(50),
        'FirstName': NVARCHAR(50),
        'LastName': NVARCHAR(50),
        'City': NVARCHAR(50),
        'CountryRegion': NVARCHAR(50)
    }

    # ===== 3. EMPLOYEES - Merge from both sources =====
    print("\n--- Merging Employees from both sources ---")
    
    employees_nw = extract_csv('Employees.csv', source='northwind')
    employees_sql = sqlserver_data.get('Employees')
    
    # Transform Northwind employees
    if employees_nw is not None:
        dim_employee_nw = employees_nw[['ID', 'Company', 'First Name', 'Last Name', 'Job Title']].copy()
        dim_employee_nw.columns = ['EmployeeID', 'Company', 'FirstName', 'LastName', 'JobTitle']
        dim_employee_nw['EmployeeID'] = pd.to_numeric(dim_employee_nw['EmployeeID'], errors='coerce').astype('Int64')
        dim_employee_nw = dim_employee_nw.dropna(subset=['EmployeeID'])
        print(f"  ✓ Northwind Employees: {len(dim_employee_nw)} records")
    else:
        dim_employee_nw = pd.DataFrame()
    
    # Transform SQL Server employees
    if employees_sql is not None:
        dim_employee_sql = pd.DataFrame()
        dim_employee_sql['EmployeeID'] = pd.to_numeric(employees_sql.get('EmployeeID', employees_sql.get('Employee ID')), errors='coerce').astype('Int64')
        dim_employee_sql['Company'] = employees_sql.get('Company', 'Northwind Traders')
        dim_employee_sql['FirstName'] = employees_sql.get('FirstName', employees_sql.get('First Name', ''))
        dim_employee_sql['LastName'] = employees_sql.get('LastName', employees_sql.get('Last Name', ''))
        dim_employee_sql['JobTitle'] = employees_sql.get('Title', employees_sql.get('Job Title', ''))
        
        dim_employee_sql = dim_employee_sql.dropna(subset=['EmployeeID'])
        print(f"  ✓ SQL Server Employees: {len(dim_employee_sql)} records")
    else:
        dim_employee_sql = pd.DataFrame()
    
    # Merge employees
    if not dim_employee_nw.empty and not dim_employee_sql.empty:
        max_nw_id = dim_employee_nw['EmployeeID'].max()
        dim_employee_sql['EmployeeID'] = dim_employee_sql['EmployeeID'] + max_nw_id + 1000
        dim_employee = pd.concat([dim_employee_nw, dim_employee_sql], ignore_index=True)
        print(f"  ✓ MERGED Employees: {len(dim_employee)} total records")
    elif not dim_employee_nw.empty:
        dim_employee = dim_employee_nw
    else:
        dim_employee = dim_employee_sql
    
    dtype_employee = {
        'EmployeeID': Integer,
        'Company': NVARCHAR(50),
        'FirstName': NVARCHAR(50),
        'LastName': NVARCHAR(50),
        'JobTitle': NVARCHAR(50)
    }

    # ===== 4. SUPPLIERS - Merge from both sources =====
    print("\n--- Merging Suppliers from both sources ---")
    
    suppliers_nw = extract_csv('Suppliers.csv', source='northwind')
    suppliers_sql = sqlserver_data.get('Suppliers')
    
    # Transform Northwind suppliers
    if suppliers_nw is not None:
        dim_supplier_nw = suppliers_nw[['ID', 'Company', 'First Name', 'Last Name', 'City', 'Country/Region']].copy()
        dim_supplier_nw.columns = ['SupplierID', 'Company', 'FirstName', 'LastName', 'City', 'CountryRegion']
        dim_supplier_nw['SupplierID'] = pd.to_numeric(dim_supplier_nw['SupplierID'], errors='coerce').astype('Int64')
        dim_supplier_nw = dim_supplier_nw.dropna(subset=['SupplierID'])
        print(f"  ✓ Northwind Suppliers: {len(dim_supplier_nw)} records")
    else:
        dim_supplier_nw = pd.DataFrame()
    
    # Transform SQL Server suppliers
    if suppliers_sql is not None:
        dim_supplier_sql = pd.DataFrame()
        dim_supplier_sql['SupplierID'] = pd.to_numeric(suppliers_sql.get('SupplierID', suppliers_sql.get('Supplier ID')), errors='coerce').astype('Int64')
        dim_supplier_sql['Company'] = suppliers_sql.get('CompanyName', suppliers_sql.get('Company', ''))
        dim_supplier_sql['FirstName'] = suppliers_sql.get('ContactName', '').str.split().str[0] if 'ContactName' in suppliers_sql.columns else ''
        dim_supplier_sql['LastName'] = suppliers_sql.get('ContactName', '').str.split().str[-1] if 'ContactName' in suppliers_sql.columns else ''
        dim_supplier_sql['City'] = suppliers_sql.get('City', '')
        dim_supplier_sql['CountryRegion'] = suppliers_sql.get('Country', suppliers_sql.get('CountryRegion', ''))
        
        dim_supplier_sql = dim_supplier_sql.dropna(subset=['SupplierID'])
        print(f"  ✓ SQL Server Suppliers: {len(dim_supplier_sql)} records")
    else:
        dim_supplier_sql = pd.DataFrame()
    
    # Merge suppliers
    if not dim_supplier_nw.empty and not dim_supplier_sql.empty:
        max_nw_id = dim_supplier_nw['SupplierID'].max()
        dim_supplier_sql['SupplierID'] = dim_supplier_sql['SupplierID'] + max_nw_id + 1000
        dim_supplier = pd.concat([dim_supplier_nw, dim_supplier_sql], ignore_index=True)
        print(f"  ✓ MERGED Suppliers: {len(dim_supplier)} total records")
    elif not dim_supplier_nw.empty:
        dim_supplier = dim_supplier_nw
    else:
        dim_supplier = dim_supplier_sql
    
    dtype_supplier = {
        'SupplierID': Integer,
        'Company': NVARCHAR(50),
        'FirstName': NVARCHAR(50),
        'LastName': NVARCHAR(50),
        'City': NVARCHAR(50),
        'CountryRegion': NVARCHAR(50)
    }

    print(f"\n✓ Dimension merging complete!")
    return (dim_product, dtype_product), (dim_customer, dtype_customer), (dim_employee, dtype_employee), (dim_supplier, dtype_supplier)

# ===== FACT SALES TRANSFORMATION (MERGED FROM BOTH SOURCES) =====

def transform_fact_sales_merged(sqlserver_data):
    """
    Transform fact sales by merging data from BOTH sources.
    """
    print("\n" + "="*60)
    print("STEP 2: TRANSFORMING FACT SALES (MERGED SOURCES)")
    print("="*60)
    
    # Source 1: Northwind
    orders_nw = extract_csv('Orders.csv', source='northwind')
    details_nw = extract_csv('Order Details.csv', source='northwind')
    order_status_nw = extract_csv('Orders Status.csv', source='northwind')
    
    # Source 2: SQL Server
    orders_sql = sqlserver_data.get('Orders')
    details_sql = sqlserver_data.get('Order Details')
    
    all_fact_sales = []
    
    # Process Northwind data (your existing logic)
    if orders_nw is not None and details_nw is not None and order_status_nw is not None:
        print("\n--- Processing Northwind sales data ---")
        
        merged_nw = pd.merge(
            details_nw, 
            orders_nw, 
            left_on='Order ID', 
            right_on='Order ID', 
            how='inner',
            suffixes=('_detail', '_order')
        )

        if 'ID' in order_status_nw.columns:
            order_status_nw = order_status_nw.rename(columns={'ID': 'Status ID_key'})
        elif 'Status ID' in order_status_nw.columns:
            order_status_nw = order_status_nw.rename(columns={'Status ID': 'Status ID_key'})
        
        merged_nw = pd.merge(
            merged_nw, 
            order_status_nw, 
            left_on='Status ID_order', 
            right_on='Status ID_key', 
            how='left'
        )
        
        # Business Logic
        merged_nw['TotalRevenue'] = (
            pd.to_numeric(merged_nw['Unit Price'], errors='coerce') * 
            pd.to_numeric(merged_nw['Quantity'], errors='coerce') * 
            (1 - pd.to_numeric(merged_nw['Discount'], errors='coerce').fillna(0))
        )

        def calculate_tax_rate(freight_cost):
            if pd.isna(freight_cost): return 0.0
            return 0.10 if freight_cost >= 500 else 0.0

        merged_nw['TaxRate'] = pd.to_numeric(merged_nw['Shipping Fee'], errors='coerce').apply(calculate_tax_rate)
        
        # Final Fact Table
        fact_sales_nw = pd.DataFrame()
        fact_sales_nw['OrderDate'] = pd.to_datetime(merged_nw['Order Date'], errors='coerce')
        fact_sales_nw['CustomerKey'] = pd.to_numeric(merged_nw['Customer ID'], errors='coerce').astype('Int64')
        fact_sales_nw['EmployeeKey'] = pd.to_numeric(merged_nw['Employee ID'], errors='coerce').astype('Int64')
        fact_sales_nw['ProductKey'] = pd.to_numeric(merged_nw['Product ID'], errors='coerce').astype('Int64')
        fact_sales_nw['Quantity'] = pd.to_numeric(merged_nw['Quantity'], errors='coerce').astype('Int64')
        fact_sales_nw['UnitPrice'] = pd.to_numeric(merged_nw['Unit Price'], errors='coerce')
        fact_sales_nw['Discount'] = pd.to_numeric(merged_nw['Discount'], errors='coerce').fillna(0)
        fact_sales_nw['TaxRate'] = merged_nw['TaxRate']
        fact_sales_nw['TotalRevenue'] = merged_nw['TotalRevenue']
        fact_sales_nw['FreightCost'] = pd.to_numeric(merged_nw['Shipping Fee'], errors='coerce')
        fact_sales_nw['OrderStatus'] = merged_nw['Status Name'].astype(str)
        
        all_fact_sales.append(fact_sales_nw)
        print(f"  ✓ Northwind Sales: {len(fact_sales_nw)} records")
    
    # Process SQL Server data
    if orders_sql is not None and details_sql is not None:
        print("\n--- Processing SQL Server sales data ---")
        
        # Merge orders and order details
        merged_sql = pd.merge(
            details_sql,
            orders_sql,
            left_on='OrderID' if 'OrderID' in details_sql.columns else 'Order ID',
            right_on='OrderID' if 'OrderID' in orders_sql.columns else 'Order ID',
            how='inner',
            suffixes=('_detail', '_order')
        )
        
        # Calculate business metrics
        merged_sql['TotalRevenue'] = (
            pd.to_numeric(merged_sql.get('UnitPrice', merged_sql.get('Unit Price', 0)), errors='coerce') * 
            pd.to_numeric(merged_sql['Quantity'], errors='coerce') * 
            (1 - pd.to_numeric(merged_sql.get('Discount', 0), errors='coerce').fillna(0))
        )
        
        # Tax rate calculation (10% if freight >= 500)
        freight_col = 'Freight' if 'Freight' in merged_sql.columns else 'Shipping Fee'
        if freight_col in merged_sql.columns:
            merged_sql['TaxRate'] = pd.to_numeric(merged_sql[freight_col], errors='coerce').apply(
                lambda x: 0.10 if pd.notna(x) and x >= 500 else 0.0
            )
        else:
            merged_sql['TaxRate'] = 0.0
        
        # Build fact table for SQL Server data
        fact_sales_sql = pd.DataFrame()
        fact_sales_sql['OrderDate'] = pd.to_datetime(merged_sql.get('OrderDate', merged_sql.get('Order Date')), errors='coerce')
        fact_sales_sql['CustomerKey'] = pd.to_numeric(merged_sql.get('CustomerID', merged_sql.get('Customer ID')), errors='coerce').astype('Int64')
        fact_sales_sql['EmployeeKey'] = pd.to_numeric(merged_sql.get('EmployeeID', merged_sql.get('Employee ID')), errors='coerce').astype('Int64')
        fact_sales_sql['ProductKey'] = pd.to_numeric(merged_sql.get('ProductID', merged_sql.get('Product ID')), errors='coerce').astype('Int64')
        fact_sales_sql['Quantity'] = pd.to_numeric(merged_sql['Quantity'], errors='coerce').astype('Int64')
        fact_sales_sql['UnitPrice'] = pd.to_numeric(merged_sql.get('UnitPrice', merged_sql.get('Unit Price', 0)), errors='coerce')
        fact_sales_sql['Discount'] = pd.to_numeric(merged_sql.get('Discount', 0), errors='coerce').fillna(0)
        fact_sales_sql['TaxRate'] = merged_sql['TaxRate']
        fact_sales_sql['TotalRevenue'] = merged_sql['TotalRevenue']
        fact_sales_sql['FreightCost'] = pd.to_numeric(merged_sql.get('Freight', merged_sql.get('Shipping Fee', 0)), errors='coerce')
        # Handle OrderStatus - check multiple possible column names
        if 'Status' in merged_sql.columns:
            fact_sales_sql['OrderStatus'] = merged_sql['Status'].astype(str)
        elif 'OrderStatus' in merged_sql.columns:
            fact_sales_sql['OrderStatus'] = merged_sql['OrderStatus'].astype(str)
        elif 'Status Name' in merged_sql.columns:
            fact_sales_sql['OrderStatus'] = merged_sql['Status Name'].astype(str)
        else:
            fact_sales_sql['OrderStatus'] = 'Unknown'
        
        # Adjust keys to avoid conflicts (add offset)
        fact_sales_sql['CustomerKey'] = fact_sales_sql['CustomerKey'] + 1000
        fact_sales_sql['EmployeeKey'] = fact_sales_sql['EmployeeKey'] + 1000
        fact_sales_sql['ProductKey'] = fact_sales_sql['ProductKey'] + 1000
        
        all_fact_sales.append(fact_sales_sql)
        print(f"  ✓ SQL Server Sales: {len(fact_sales_sql)} records")
    
    # Combine all sales data
    if len(all_fact_sales) > 0:
        fact_sales = pd.concat(all_fact_sales, ignore_index=True)
        print(f"\n✓ MERGED Sales Fact: {len(fact_sales)} total records")
    else:
        print("✗ No sales data available")
        return pd.DataFrame(), None
    
    # --- VALIDATION SECTION ---
    print(f"\n🔍 Validating foreign keys for Fact_Sales...")
    
    fact_sales = validate_and_fix_foreign_keys(fact_sales, 'EmployeeKey', 'DWH_TFOUTD_Dim_Employee', 'EmployeeID')
    fact_sales = validate_and_fix_foreign_keys(fact_sales, 'CustomerKey', 'DWH_TFOUTD_Dim_Customer', 'CustomerID', placeholder_name="Unknown_Customer")
    fact_sales = validate_and_fix_foreign_keys(fact_sales, 'ProductKey', 'DWH_TFOUTD_Dim_Product', 'ProductID', placeholder_name="Unknown_Product")
    
    # Remove rows with critical nulls
    fact_sales = fact_sales.dropna(subset=['OrderDate', 'CustomerKey', 'ProductKey'])
    
    dtype_sales = {
        'OrderDate': DateTime,
        'CustomerKey': Integer,
        'EmployeeKey': Integer,
        'ProductKey': Integer,
        'Quantity': Integer,
        'UnitPrice': DECIMAL(18, 4),
        'Discount': Float,
        'TaxRate': Float,
        'TotalRevenue': DECIMAL(18, 4),
        'FreightCost': DECIMAL(18, 4),
        'OrderStatus': NVARCHAR(50)
    }
    
    print(f"✓ Prepared {len(fact_sales)} valid sales records for loading")
    return fact_sales, dtype_sales

# --- FACT PURCHASES TRANSFORMATION (EXISTING LOGIC) ---

def transform_fact_purchases():
    print("\n" + "="*60)
    print("STEP 3: TRANSFORMING FACT PURCHASES (IMPORTS)")
    print("="*60)
    
    purchase_orders = extract_csv('Purchase Orders.csv', source='northwind')
    purchase_details = extract_csv('Purchase Order Details.csv', source='northwind')
    
    if purchase_orders is None or purchase_details is None:
        print("✗ Skipping Fact Purchases: Source files missing.")
        return pd.DataFrame(), None
        
    if 'ID' in purchase_orders.columns:
        po_pk = 'ID'
    elif 'Purchase Order ID' in purchase_orders.columns:
        po_pk = 'Purchase Order ID'
    else:
        print(f"✗ Cannot find primary key in Purchase Orders.csv.")
        return pd.DataFrame(), None
        
    merged = pd.merge(
        purchase_details, 
        purchase_orders, 
        left_on='Purchase Order ID', 
        right_on=po_pk, 
        how='inner',
        suffixes=('_detail', '_order')
    )
    
    print(f"✓ Merged {len(merged)} purchase records")
    
    # Find cost column
    cost_col = 'Unit Cost'
    if cost_col not in merged.columns:
        print("✗ Cannot find Unit Cost column.")
        return pd.DataFrame(), None
        
    # Calculate Total Purchase Cost
    merged['TotalPurchaseCost'] = (
        pd.to_numeric(merged[cost_col], errors='coerce') * 
        pd.to_numeric(merged['Quantity'], errors='coerce')
    )
    
    # Final Fact Table
    fact_purchases = pd.DataFrame()
    
    fact_purchases['CreationDate'] = pd.to_datetime(merged['Creation Date'], errors='coerce')
    fact_purchases['SupplierKey'] = pd.to_numeric(merged['Supplier ID'], errors='coerce').astype('Int64')
    fact_purchases['EmployeeKey'] = pd.to_numeric(merged['Created By'], errors='coerce').astype('Int64').fillna(0)
    fact_purchases['ProductKey'] = pd.to_numeric(merged['Product ID'], errors='coerce').astype('Int64')
    fact_purchases['Quantity'] = pd.to_numeric(merged['Quantity'], errors='coerce').astype('Int64')
    fact_purchases['UnitCost'] = pd.to_numeric(merged[cost_col], errors='coerce')
    fact_purchases['TotalPurchaseCost'] = merged['TotalPurchaseCost']
    
    # --- VALIDATION SECTION ---
    print(f"\n🔍 Validating foreign keys for Fact_Purchases...")
    
    fact_purchases = validate_and_fix_foreign_keys(fact_purchases, 'EmployeeKey', 'DWH_TFOUTD_Dim_Employee', 'EmployeeID')
    fact_purchases = validate_and_fix_foreign_keys(fact_purchases, 'SupplierKey', 'DWH_TFOUTD_Dim_Supplier', 'SupplierID', placeholder_name="Unknown_Supplier")
    fact_purchases = validate_and_fix_foreign_keys(fact_purchases, 'ProductKey', 'DWH_TFOUTD_Dim_Product', 'ProductID', placeholder_name="Unknown_Product")
    
    # Remove rows with critical nulls
    fact_purchases = fact_purchases.dropna(subset=['CreationDate', 'SupplierKey', 'ProductKey'])
    
    dtype_purchases = {
        'CreationDate': DateTime,
        'SupplierKey': Integer,
        'EmployeeKey': Integer,
        'ProductKey': Integer,
        'Quantity': Integer,
        'UnitCost': DECIMAL(18, 4),
        'TotalPurchaseCost': DECIMAL(18, 4)
    }
    
    print(f"✓ Prepared {len(fact_purchases)} valid purchase records for loading")
    return fact_purchases, dtype_purchases


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    
    print("\n" + "="*70)
    print(" "*15 + "NORTHWIND ETL PROCESS - DUAL SOURCE")
    print(" "*10 + "Source 1: Northwind DB (20 tables)")
    print(" "*10 + "Source 2: SQL Server DB (13 tables)")
    print("="*70)
    
    # Check paths exist
    if not os.path.exists(NORTHWIND_PATH):
        print(f"\n⚠️  WARNING: Northwind path not found: {NORTHWIND_PATH}")
        print("   Creating directory...")
        os.makedirs(NORTHWIND_PATH, exist_ok=True)
    
    if not os.path.exists(SQLSERVER_PATH):
        print(f"\n⚠️  WARNING: SQL Server path not found: {SQLSERVER_PATH}")
        print("   Please ensure the path is correct!")
    else:
        print(f"✓ SQL Server data path found: {SQLSERVER_PATH}")
    
    # Check and create schema if needed
    ensure_schema_exists()
    
    # Clear all tables
    clear_all_tables()
    
    success_count = 0
    fail_count = 0
    
    # ========== EXTRACT SQL SERVER DATA ==========
    sqlserver_data = extract_sqlserver_data()
    
    # ========== TRANSFORM & LOAD DIMENSIONS (MERGED) ==========
    dims = transform_dimensions_merged(sqlserver_data)
    if dims[0] is None:
        print("\n✗ FAILED: Could not load dimension data")
        exit(1)
    
    (d_prod, dtype_prod), (d_cust, dtype_cust), (d_emp, dtype_emp), (d_supp, dtype_supp) = dims
    
    if load_to_sql(d_prod, 'DWH_TFOUTD_Dim_Product', dtype_prod): 
        success_count += 1
    else: 
        fail_count += 1
    
    if load_to_sql(d_cust, 'DWH_TFOUTD_Dim_Customer', dtype_cust): 
        success_count += 1
    else: 
        fail_count += 1
    
    if load_to_sql(d_emp, 'DWH_TFOUTD_Dim_Employee', dtype_emp): 
        success_count += 1
    else: 
        fail_count += 1
    
    if load_to_sql(d_supp, 'DWH_TFOUTD_Dim_Supplier', dtype_supp): 
        success_count += 1
    else: 
        fail_count += 1
    
    # ========== TRANSFORM & LOAD FACT SALES (MERGED) ==========
    f_sales, dtype_sales = transform_fact_sales_merged(sqlserver_data)
    if not f_sales.empty and dtype_sales:
        if load_to_sql(f_sales, 'DWH_TFOUTD_Sales_Fact', dtype_sales): 
            success_count += 1
        else: 
            fail_count += 1
    
    # ========== TRANSFORM & LOAD FACT PURCHASES ==========
    f_purchases, dtype_purchases = transform_fact_purchases()
    if not f_purchases.empty and dtype_purchases:
        if load_to_sql(f_purchases, 'DWH_TFOUTD_Purchases_Fact', dtype_purchases): 
            success_count += 1
        else: 
            fail_count += 1

    # ========== SUMMARY ==========
    print("\n" + "="*70)
    print(f"ETL COMPLETE: {success_count} tables loaded successfully, {fail_count} failed")
    print("="*70)
    
    # Display merged statistics
    print("\n" + "="*70)
    print("DATA INTEGRATION SUMMARY")
    print("="*70)
    
    with engine.connect() as conn:
        tables = ['DWH_TFOUTD_Dim_Product', 'DWH_TFOUTD_Dim_Customer', 'DWH_TFOUTD_Dim_Employee', 'DWH_TFOUTD_Dim_Supplier', 'DWH_TFOUTD_Sales_Fact', 'DWH_TFOUTD_Purchases_Fact']
        
        print("\n📊 Final Table Counts:")
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table}"))
            count = result.fetchone()[0]
            print(f"  • {table}: {count:,} records")
        
        # Revenue statistics
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_orders,
                SUM(TotalRevenue) as total_revenue,
                AVG(TotalRevenue) as avg_revenue,
                SUM(CASE WHEN TaxRate > 0 THEN 1 ELSE 0 END) as taxed_orders
            FROM DWH_TFOUTD_Sales_Fact
        """))
        
        stats = result.fetchone()
        print(f"\n💰 Sales Analytics:")
        print(f"  • Total Orders: {stats[0]:,}")
        print(f"  • Total Revenue: ${stats[1]:,.2f}" if stats[1] else "  • Total Revenue: $0.00")
        print(f"  • Average Revenue: ${stats[2]:,.2f}" if stats[2] else "  • Average Revenue: $0.00")
        print(f"  • Orders with Freight Tax (≥$500): {stats[3]:,}")
    
    print("\n✓ ETL Process completed successfully!")
    print("✓ Data from both sources integrated into Data Warehouse")
    print("\n" + "="*70 + "\n")