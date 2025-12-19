import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import urllib
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Northwind Traders BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS FIXES ---
st.markdown("""
<style>
    /* Headers */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #FFFFFF;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #FFFFFF;
        margin-bottom: 2rem;
    }
    
    /* Metric Cards */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        color: #FFFFFF;
    }
    
    /* FIX: Expander Background and Text Color */
    .streamlit-expanderContent {
        background-color: #ffffff !important;
        color: #1f2937 !important;
    }
    
    div[data-testid="stExpander"] details {
        background-color: #f9fafb;
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
        color: #1f2937; /* Force dark text */
    }

    div[data-testid="stExpander"] details:hover {
        background-color: #f3f4f6;
        color: #000000; /* Force black text on hover */
    }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
        font-weight: 600;
        font-size: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# --- CONNECTION SETUP ---
@st.cache_resource
def get_engine():
    SERVER_NAME = 'localhost' 
    DB_NAME = 'DWH_Northwind'
    DRIVER = 'ODBC Driver 17 for SQL Server'
    
    params = urllib.parse.quote_plus(f"DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DB_NAME};Trusted_Connection=yes;")
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine

engine = get_engine()

# --- DATA LOADING FUNCTIONS ---
@st.cache_data(ttl=300)
def load_sales_data():
    """Load and prepare sales data for analysis."""
    query = """
    SELECT 
        fs.SalesKey,
        fs.OrderDate,
        dc.Company as CustomerCompany,
        dc.FirstName + ' ' + dc.LastName as CustomerName,
        dc.City as CustomerCity,
        dc.CountryRegion as CustomerCountry,
        dp.ProductName,
        dp.Category,
        de.FirstName + ' ' + de.LastName as EmployeeName,
        fs.Quantity,
        fs.UnitPrice,
        fs.Discount,
        fs.TotalRevenue,
        fs.FreightCost,
        fs.TaxRate,
        fs.OrderStatus,
        YEAR(fs.OrderDate) as Year,
        MONTH(fs.OrderDate) as Month,
        DATENAME(month, fs.OrderDate) as MonthName,
        FORMAT(fs.OrderDate, 'MM/yyyy') as MonthYear
    FROM DWH_TFOUTD_Sales_Fact fs
    LEFT JOIN DWH_TFOUTD_Dim_Customer dc ON fs.CustomerKey = dc.CustomerID
    LEFT JOIN DWH_TFOUTD_Dim_Product dp ON fs.ProductKey = dp.ProductID
    LEFT JOIN DWH_TFOUTD_Dim_Employee de ON fs.EmployeeKey = de.EmployeeID
    WHERE fs.OrderDate IS NOT NULL
    ORDER BY fs.OrderDate DESC
    """
    df = pd.read_sql(query, engine)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    
    # Determine delivery status based on OrderStatus
    df['IsDelivered'] = df['OrderStatus'].str.lower().str.contains('shipped|complete|delivered', na=False).astype(int)
    df['DeliveryStatus'] = df['IsDelivered'].map({1: 'Delivered', 0: 'Not Delivered'})
    
    # Calculate freight with tax
    df['FreightTax'] = df['FreightCost'].apply(lambda x: x * 0.10 if pd.notna(x) and x >= 500 else 0)
    df['FreightWithTax'] = df['FreightCost'] + df['FreightTax']
    
    return df

@st.cache_data(ttl=300)
def load_purchase_data():
    """Load and prepare purchase data for analysis."""
    query = """
    SELECT 
        fp.PurchaseKey,
        fp.CreationDate,
        ds.Company as SupplierCompany,
        ds.CountryRegion as SupplierCountry,
        dp.ProductName,
        dp.Category,
        de.FirstName + ' ' + de.LastName as EmployeeName,
        fp.Quantity,
        fp.UnitCost,
        fp.TotalPurchaseCost,
        YEAR(fp.CreationDate) as Year,
        MONTH(fp.CreationDate) as Month,
        DATENAME(month, fp.CreationDate) as MonthName
    FROM DWH_TFOUTD_Purchases_Fact fp
    LEFT JOIN DWH_TFOUTD_Dim_Supplier ds ON fp.SupplierKey = ds.SupplierID
    LEFT JOIN DWH_TFOUTD_Dim_Product dp ON fp.ProductKey = dp.ProductID
    LEFT JOIN DWH_TFOUTD_Dim_Employee de ON fp.EmployeeKey = de.EmployeeID
    WHERE fp.CreationDate IS NOT NULL
    ORDER BY fp.CreationDate DESC
    """
    df = pd.read_sql(query, engine)
    df['CreationDate'] = pd.to_datetime(df['CreationDate'])
    return df

# --- DASHBOARD UI ---

# Header
st.markdown('<div class="main-header">Northwind Traders Business Intelligence</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Comprehensive analysis of import/export operations and sales performance</div>', unsafe_allow_html=True)

# Sidebar
st.sidebar.header("Filters & Options")

# Load data
with st.spinner("Loading data from Data Warehouse..."):
    try:
        sales_df = load_sales_data()
        purchases_df = load_purchase_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")
        st.stop()

# Sidebar filters
st.sidebar.subheader("Data Filters")

# Year filter
all_years = sorted(sales_df['Year'].dropna().unique().tolist())
selected_years = st.sidebar.multiselect(
    "Select Years",
    options=all_years,
    default=all_years[-3:] if len(all_years) > 3 else all_years
)

# Category filter
all_categories = ['All'] + sorted(sales_df['Category'].dropna().unique().tolist())
selected_category = st.sidebar.selectbox(
    "Select Product Category",
    options=all_categories
)

# Apply filters
filtered_sales = sales_df.copy()
if selected_years:
    filtered_sales = filtered_sales[filtered_sales['Year'].isin(selected_years)]
if selected_category != 'All':
    filtered_sales = filtered_sales[filtered_sales['Category'] == selected_category]

filtered_purchases = purchases_df.copy()
if selected_years:
    filtered_purchases = filtered_purchases[filtered_purchases['Year'].isin(selected_years)]

# --- KEY METRICS SECTION ---
st.markdown("---")
st.subheader("Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

total_revenue = filtered_sales['TotalRevenue'].sum()
total_orders = len(filtered_sales)
delivered_orders = filtered_sales['IsDelivered'].sum()
delivery_rate = (delivered_orders / total_orders * 100) if total_orders > 0 else 0
avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

with col1:
    st.metric(
        label="Total Revenue",
        value=f"${total_revenue:,.0f}",
        delta=f"{len(selected_years)} year(s)"
    )

with col2:
    st.metric(
        label="Total Orders",
        value=f"{total_orders:,}",
        delta=f"{delivered_orders:,} delivered"
    )

with col3:
    st.metric(
        label="Delivery Rate",
        value=f"{delivery_rate:.1f}%",
        delta="On-time performance"
    )

with col4:
    st.metric(
        label="Average Order Value",
        value=f"${avg_order_value:.2f}",
        delta="Per transaction"
    )

with col5:
    high_freight_orders = len(filtered_sales[filtered_sales['FreightCost'] >= 500])
    total_freight_tax = filtered_sales['FreightTax'].sum()
    st.metric(
        label="Freight Tax Collected",
        value=f"${total_freight_tax:,.0f}",
        delta=f"{high_freight_orders} orders taxed"
    )

st.markdown("---")

# --- MAIN TABS ---
tab1, tab2, tab3 = st.tabs([
    "Question 1: Revenue Analysis", 
    "Question 2: Freight Tax Analysis", 
    "Question 3: Delivery Statistics"
])

# --- TAB 1: REVENUE ANALYSIS (Q1) ---
with tab1:
    st.header("Q1: Chiffre d'affaires Commandé et Réalisé")
    st.markdown("Analysis by year, month, customer, product, and category")
    
    # Revenue by Year and Month
    col1, col2 = st.columns(2)
    
    with col1:
        yearly_revenue = filtered_sales.groupby('Year').agg({
            'TotalRevenue': 'sum',
            'SalesKey': 'count'
        }).reset_index()
        yearly_revenue.columns = ['Year', 'Revenue', 'Orders']
        
        # FIX: Ensure proper bar chart formatting even with single year
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(
            x=yearly_revenue['Year'],
            y=yearly_revenue['Revenue'],
            name='Revenue',
            marker_color='#3b82f6',
            text=yearly_revenue['Revenue'].apply(lambda x: f'${x:,.0f}'),
            textposition='outside',
            cliponaxis=False # FIX: Prevents numbers from being cut off
        ))
        fig1.update_layout(
            title="Annual Revenue",
            xaxis_title="Year",
            yaxis_title="Revenue (USD)",
            showlegend=False,
            height=400,
            template="plotly_white",
            # FIX: Force x-axis to behave as categories (prevents wide bars)
            xaxis=dict(type='category'),
            # FIX: Add top margin so numbers don't get hidden
            margin=dict(t=50, l=20, r=20, b=20) 
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        monthly_revenue = filtered_sales.groupby(['Year', 'Month', 'MonthName']).agg({
            'TotalRevenue': 'sum'
        }).reset_index().sort_values(['Year', 'Month'])
        
        fig2 = px.line(
            monthly_revenue,
            x='MonthName',
            y='TotalRevenue',
            color='Year',
            title="Monthly Revenue Trend",
            labels={'TotalRevenue': 'Revenue (USD)', 'MonthName': 'Month'},
            markers=True
        )
        fig2.update_layout(
            height=400,
            template="plotly_white",
            margin=dict(t=50)
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    # Revenue by Category and Top Customers
    col3, col4 = st.columns(2)
    
    with col3:
        category_revenue = filtered_sales.groupby('Category').agg({
            'TotalRevenue': 'sum',
            'SalesKey': 'count'
        }).reset_index().sort_values('TotalRevenue', ascending=False)
        category_revenue.columns = ['Category', 'Revenue', 'Orders']
        
        fig3 = px.bar(
            category_revenue,
            x='Category',
            y='Revenue',
            title="Revenue by Product Category",
            labels={'Revenue': 'Total Revenue (USD)', 'Category': 'Category'},
            color='Revenue',
            color_continuous_scale='Blues',
            text='Revenue'
        )
        # FIX: Adjusted formatting and cliponaxis
        fig3.update_traces(
            texttemplate='$%{text:,.0f}', 
            textposition='outside',
            cliponaxis=False
        )
        fig3.update_layout(
            height=400, 
            showlegend=False,
            template="plotly_white",
            margin=dict(t=50)
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    with col4:
        top_customers = filtered_sales.groupby('CustomerCompany').agg({
            'TotalRevenue': 'sum',
            'SalesKey': 'count'
        }).nlargest(10, 'TotalRevenue').reset_index()
        top_customers.columns = ['Customer', 'Revenue', 'Orders']
        
        fig4 = px.bar(
            top_customers,
            y='Customer',
            x='Revenue',
            orientation='h',
            title="Top 10 Customers by Revenue",
            labels={'Revenue': 'Total Revenue (USD)', 'Customer': 'Customer'},
            color='Revenue',
            color_continuous_scale='Greens',
            text='Revenue'
        )
        fig4.update_traces(
            texttemplate='$%{text:,.0f}', 
            textposition='outside',
            cliponaxis=False
        )
        fig4.update_layout(
            height=400, 
            showlegend=False,
            template="plotly_white",
            margin=dict(t=50)
        )
        st.plotly_chart(fig4, use_container_width=True)
    
    # Top Products
    st.subheader("Top 20 Products by Revenue")
    top_products = filtered_sales.groupby('ProductName').agg({
        'TotalRevenue': 'sum',
        'Quantity': 'sum',
        'SalesKey': 'count'
    }).nlargest(20, 'TotalRevenue').reset_index()
    top_products.columns = ['Product', 'Revenue', 'Quantity Sold', 'Number of Orders']
    top_products['Revenue'] = top_products['Revenue'].apply(lambda x: f'${x:,.2f}')
    
    st.dataframe(
        top_products,
        use_container_width=True,
        hide_index=True
    )
    
    # Detailed Revenue Table
    with st.expander("View Detailed Revenue Data"):
        detailed_data = filtered_sales[[
            'OrderDate', 'CustomerCompany', 'ProductName', 'Category', 
            'Quantity', 'UnitPrice', 'Discount', 'TotalRevenue', 'OrderStatus'
        ]].sort_values('OrderDate', ascending=False).head(100)
        
        detailed_data['OrderDate'] = detailed_data['OrderDate'].dt.strftime('%Y-%m-%d')
        detailed_data['TotalRevenue'] = detailed_data['TotalRevenue'].apply(lambda x: f'${x:,.2f}')
        detailed_data['UnitPrice'] = detailed_data['UnitPrice'].apply(lambda x: f'${x:,.2f}')
        detailed_data['Discount'] = detailed_data['Discount'].apply(lambda x: f'{x:.0%}')
        
        st.dataframe(detailed_data, use_container_width=True, hide_index=True)

# --- TAB 2: FREIGHT TAX ANALYSIS (Q2) ---
with tab2:
    st.header("Q2: Freight Tax Analysis")
    st.markdown("10% tax applied to orders with freight cost ≥ $500")
    
    # Filter orders with freight >= 500
    high_freight = filtered_sales[filtered_sales['FreightCost'] >= 500].copy()
    
    if not high_freight.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Orders with High Freight",
                f"{len(high_freight):,}",
                f"Out of {len(filtered_sales):,} total"
            )
        
        with col2:
            total_freight = high_freight['FreightCost'].sum()
            st.metric(
                "Total Freight Cost",
                f"${total_freight:,.2f}",
                "Before tax"
            )
        
        with col3:
            total_tax = high_freight['FreightTax'].sum()
            st.metric(
                "Total Tax Collected",
                f"${total_tax:,.2f}",
                "10% on eligible orders"
            )
        
        st.markdown("---")
        
        # Visualization
        col4, col5 = st.columns(2)
        
        with col4:
            # Freight distribution
            fig5 = px.histogram(
                filtered_sales,
                x='FreightCost',
                nbins=50,
                title="Distribution of Freight Costs",
                labels={'FreightCost': 'Freight Cost (USD)', 'count': 'Number of Orders'},
                color_discrete_sequence=['#3b82f6']
            )
            fig5.add_vline(
                x=500, 
                line_dash="dash", 
                line_color="red",
                annotation_text="Tax Threshold: $500",
                annotation_position="top right"
            )
            fig5.update_layout(height=400, template="plotly_white")
            st.plotly_chart(fig5, use_container_width=True)
        
        with col5:
            # Tax impact comparison
            tax_comparison = pd.DataFrame({
                'Type': ['Without Tax', 'With Tax'],
                'Amount': [
                    high_freight['FreightCost'].sum(),
                    high_freight['FreightWithTax'].sum()
                ]
            })
            
            fig6 = go.Figure()
            fig6.add_trace(go.Bar(
                x=tax_comparison['Type'],
                y=tax_comparison['Amount'],
                marker_color=['#3b82f6', '#ef4444'],
                text=tax_comparison['Amount'].apply(lambda x: f'${x:,.0f}'),
                textposition='outside',
                cliponaxis=False
            ))
            fig6.update_layout(
                title="Total Freight Cost: Impact of 10% Tax",
                yaxis_title="Amount (USD)",
                showlegend=False,
                height=400,
                template="plotly_white",
                margin=dict(t=50)
            )
            st.plotly_chart(fig6, use_container_width=True)
        
        # Detailed table
        st.subheader("Orders with Freight ≥ $500 (Detailed)")
        
        freight_table = high_freight[[
            'OrderDate', 'CustomerCompany', 'FreightCost', 'FreightTax', 'FreightWithTax'
        ]].sort_values('FreightCost', ascending=False).copy()
        
        freight_table.columns = [
            'Order Date', 'Customer', 'Freight (No Tax)', 'Tax Amount', 'Freight (With Tax)'
        ]
        freight_table['Order Date'] = freight_table['Order Date'].dt.strftime('%Y-%m-%d')
        freight_table['Freight (No Tax)'] = freight_table['Freight (No Tax)'].apply(lambda x: f'${x:,.2f}')
        freight_table['Tax Amount'] = freight_table['Tax Amount'].apply(lambda x: f'${x:,.2f}')
        freight_table['Freight (With Tax)'] = freight_table['Freight (With Tax)'].apply(lambda x: f'${x:,.2f}')
        
        st.dataframe(freight_table, use_container_width=True, hide_index=True)
        
    else:
        st.info("No orders found with freight cost ≥ $500 in the selected period.")

# --- TAB 3: DELIVERY STATISTICS (Q3) ---
with tab3:
    st.header("Q3: Delivery Status Analysis")
    st.markdown("Orders delivered vs not delivered by customer, employee, month, and year")
    
    # Overall delivery statistics
    col1, col2, col3 = st.columns(3)
    
    total_orders = len(filtered_sales)
    delivered = filtered_sales['IsDelivered'].sum()
    not_delivered = total_orders - delivered
    delivery_pct = (delivered / total_orders * 100) if total_orders > 0 else 0
    
    with col1:
        st.metric("Total Orders", f"{total_orders:,}")
    
    with col2:
        st.metric("Delivered Orders", f"{delivered:,}", f"{delivery_pct:.1f}%")
    
    with col3:
        st.metric("Not Yet Delivered", f"{not_delivered:,}", f"{100-delivery_pct:.1f}%")
    
    st.markdown("---")
    
    # Visualizations
    col4, col5 = st.columns(2)
    
    with col4:
        # Delivery status pie chart
        status_counts = filtered_sales['DeliveryStatus'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        
        fig7 = px.pie(
            status_counts,
            values='Count',
            names='Status',
            title="Overall Delivery Status",
            color='Status',
            color_discrete_map={'Delivered': '#10b981', 'Not Delivered': '#ef4444'},
            hole=0.4
        )
        fig7.update_traces(textposition='inside', textinfo='percent+label')
        fig7.update_layout(height=400, template="plotly_white")
        st.plotly_chart(fig7, use_container_width=True)
    
    with col5:
        # Monthly delivery trend
        monthly_delivery = filtered_sales.groupby(['MonthYear', 'DeliveryStatus']).size().reset_index(name='Count')
        
        fig8 = px.bar(
            monthly_delivery,
            x='MonthYear',
            y='Count',
            color='DeliveryStatus',
            title="Monthly Delivery Status (MM/YYYY)",
            labels={'Count': 'Number of Orders', 'MonthYear': 'Month/Year'},
            color_discrete_map={'Delivered': '#10b981', 'Not Delivered': '#ef4444'},
            barmode='group'
        )
        fig8.update_layout(height=400, template="plotly_white")
        st.plotly_chart(fig8, use_container_width=True)
    
    # Delivery by Employee
    st.subheader("Delivery Performance by Employee")
    
    employee_delivery = filtered_sales.groupby(['EmployeeName', 'DeliveryStatus']).size().unstack(fill_value=0)
    employee_delivery['Total'] = employee_delivery.sum(axis=1)
    employee_delivery['Delivery Rate'] = (employee_delivery.get('Delivered', 0) / employee_delivery['Total'] * 100).round(1)
    employee_delivery = employee_delivery.sort_values('Total', ascending=False).head(10)
    
    fig9 = go.Figure()
    fig9.add_trace(go.Bar(
        name='Delivered',
        x=employee_delivery.index,
        y=employee_delivery.get('Delivered', 0),
        marker_color='#10b981'
    ))
    fig9.add_trace(go.Bar(
        name='Not Delivered',
        x=employee_delivery.index,
        y=employee_delivery.get('Not Delivered', 0),
        marker_color='#ef4444'
    ))
    fig9.update_layout(
        title="Top 10 Employees: Delivery Performance",
        xaxis_title="Employee",
        yaxis_title="Number of Orders",
        barmode='stack',
        height=400,
        template="plotly_white"
    )
    st.plotly_chart(fig9, use_container_width=True)
    
    # Delivery by Customer
    st.subheader("Delivery Performance by Customer")
    
    customer_delivery = filtered_sales.groupby(['CustomerCompany', 'DeliveryStatus']).size().unstack(fill_value=0)
    customer_delivery['Total'] = customer_delivery.sum(axis=1)
    customer_delivery['Delivery Rate'] = (customer_delivery.get('Delivered', 0) / customer_delivery['Total'] * 100).round(1)
    customer_delivery = customer_delivery.sort_values('Total', ascending=False).head(10)
    
    fig10 = go.Figure()
    fig10.add_trace(go.Bar(
        name='Delivered',
        y=customer_delivery.index,
        x=customer_delivery.get('Delivered', 0),
        orientation='h',
        marker_color='#10b981'
    ))
    fig10.add_trace(go.Bar(
        name='Not Delivered',
        y=customer_delivery.index,
        x=customer_delivery.get('Not Delivered', 0),
        orientation='h',
        marker_color='#ef4444'
    ))
    fig10.update_layout(
        title="Top 10 Customers: Delivery Performance",
        xaxis_title="Number of Orders",
        yaxis_title="Customer",
        barmode='stack',
        height=400,
        template="plotly_white"
    )
    st.plotly_chart(fig10, use_container_width=True)
    
    # Detailed table with fixed colors
    with st.expander("View Detailed Delivery Data"):
        delivery_detail = employee_delivery.reset_index()
        delivery_detail.columns = ['Employee', 'Delivered', 'Not Delivered', 'Total Orders', 'Delivery Rate (%)']
        st.dataframe(delivery_detail, use_container_width=True, hide_index=True)

# --- SIDEBAR SUMMARY ---
st.sidebar.markdown("---")
st.sidebar.subheader("Data Summary")
st.sidebar.write(f"**Sales Records:** {len(sales_df):,}")
st.sidebar.write(f"**Purchase Records:** {len(purchases_df):,}")
st.sidebar.write(f"**Filtered Sales:** {len(filtered_sales):,}")
if not sales_df.empty:
    st.sidebar.write(f"**Date Range:** {sales_df['OrderDate'].min().strftime('%Y-%m-%d')} to {sales_df['OrderDate'].max().strftime('%Y-%m-%d')}")

st.sidebar.markdown("---")
st.sidebar.caption("Northwind Traders BI Dashboard v1.1")
st.sidebar.caption("Data Source: DWH_Northwind (SQL Server)")
st.sidebar.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if st.sidebar.button("Refresh Data", type="primary"):
    st.cache_data.clear()
    st.rerun()