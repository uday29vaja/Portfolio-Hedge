# web_app_local_dynamic.py - Portfolio Beta & Hedging with Custom Volatility & Risk-Free Rate
import json
import math
import os
import time
import requests
import streamlit as st
import pandas as pd
import numpy as np
import urllib3
import yfinance as yf
from datetime import datetime, timezone, timedelta
import io
from openpyxl import Workbook
from openpyxl.styles import Font
from scipy.stats import norm
from math import log, sqrt, exp
from zeep import Client, Transport
# -------------------------------
# Config
# -------------------------------
END_DATE = datetime.now(timezone.utc).date()
START_DATE = END_DATE - timedelta(days=365)
YAHOO_INDEX_TICKER = "^NSEI"

# -------------------------------
# Helper Functions
# -------------------------------
def download_yahoo_adjclose(ticker, start, end):
    try:
        data = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            progress=False,
            threads=False,
            auto_adjust=True
        )
        if data is None or data.empty:
            return 0
        return data.get("Adj Close") or data.get("Close")
    except Exception as e:
        st.warning(f"Failed to fetch {ticker}: {e}")
        return 0

def compute_beta(stock_series, index_series):
    df = pd.concat([stock_series, index_series], axis=1, join="inner").dropna()
    df = df.sort_index()
    if df.shape[0] < 30:
        return np.nan
    returns = df.pct_change().dropna()
    if returns.shape[0] < 20:
        return np.nan
    cov = returns.cov().iloc[0,1]
    var_index = returns.iloc[:,1].var()
    return cov/var_index if var_index != 0 else np.nan

def get_stock_beta(symbol, index_series):
    yf_ticker = f"{symbol}.NS"
    series = download_yahoo_adjclose(yf_ticker, START_DATE, END_DATE)
    if series is None or series.empty:
        return symbol, np.nan
    beta = compute_beta(series, index_series)
    return symbol, beta

def get_nav_data( scheme_code):
        """Get historical NAV data for a scheme"""
        try:
            url = f"https://api.mfapi.in/mf/{scheme_code}"
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    df = pd.DataFrame(data['data'])
                    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
                    df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
                    df.set_index('date', inplace=True)
                    df = df.sort_index().dropna()
                    
                    # Filter for last 2 years
                    two_years_ago = datetime.now() - timedelta(days=730)
                    df = df[df.index >= two_years_ago]
                    
                    return df['nav']
            return None
        except Exception as e:
            print(f"❌ Error fetching NAV for {scheme_code}: {e}")
            return None

# Excel Export
def create_excel_export(portfolio_data, hedging_data, portfolio_beta, total_amount, hedge_percentage):
    wb = Workbook()
    ws = wb.active
    ws.title = "Portfolio Summary"
    ws['A1'] = "Portfolio Beta & Hedging Results"
    ws['A1'].font = Font(bold=True, size=14)

    ws['A3'] = "Total Portfolio Value"
    ws['B3'] = f"₹{total_amount:,.2f}"
    ws['A4'] = "Hedge Percentage"
    ws['B4'] = f"{hedge_percentage}%"
    ws['A5'] = "Portfolio Beta"
    ws['B5'] = f"{portfolio_beta:.4f}"
    ws['A6'] = "Hedge Exposure"
    ws['B6'] = f"₹{total_amount * portfolio_beta * (hedge_percentage/100):,.2f}"

    # Portfolio Breakdown
    ws['A8'] = "Portfolio Breakdown"
    ws['A8'].font = Font(bold=True)
    for col, header in enumerate(portfolio_data.columns, 1):
        ws.cell(row=9, column=col, value=header).font = Font(bold=True)
    for row, (_, data) in enumerate(portfolio_data.iterrows(), 10):
        for col, value in enumerate(data, 1):
            ws.cell(row=row, column=col, value=value)

    # Hedging Costs
    ws2 = wb.create_sheet("Hedging Costs")
    ws2['A1'] = "Hedging Costs"
    ws2['A1'].font = Font(bold=True, size=14)
    headers = ["Option Type", "Put Strike", "Expiry", "Cost", "Annualized Cost %"]
    for col, h in enumerate(headers, 1):
        ws2.cell(row=3, column=col, value=h).font = Font(bold=True)
    for i, period in enumerate(["monthly", "quarterly", "annual"], 4):
        ws2.cell(row=i, column=1, value=period.capitalize())
        ws2.cell(row=i, column=2, value=f"₹{hedging_data[f'{period}_put_strike']}")
        ws2.cell(row=i, column=3, value=hedging_data[f'{period}_expiry'])
        ws2.cell(row=i, column=4, value=f"₹{hedging_data[f'{period}_cost']:,.2f}")
        ws2.cell(row=i, column=5, value=f"{hedging_data[f'{period}_annualized_cost']:.2f}%")

    return wb

# Disable SSL warnings for localhost only
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#fetch data from API
def Get_hedge_data(portfolio_beta, total_value, hedge_percentage):
    print("Fetching hedging data from local API...")

    # Disable SSL warnings (for self-signed localhost certs)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Prepare a session that ignores SSL certificate errors
    session = requests.Session()
    session.verify = False

    # Pass it to Zeep transport
    transport = Transport(session=session)

    # Create SOAP client
    wsdl = "https://localhost:44386/Main/ILTSALGO.asmx?WSDL"
    client = Client(wsdl=wsdl, transport=transport)

    # Call your web method
    result = client.service.hedge_calculation(portfolio_beta, total_value, hedge_percentage)

    # Usually result is a JSON string — convert it
    try:
        data = json.loads(result)
    except Exception as e:
        print("⚠️ Failed to parse JSON:", e)
        print("Raw result:", result)
        return None

    print("✅ Hedging data received successfully.")
    return data
# Mutual Fund Beta Mapping (Category-based fallback)

def Get_EQSymbol():
    print("Fetching EQ Symbol from local API...")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    transport = Transport(session=session)

    wsdl = "https://localhost:44386/Main/ILTSALGO.asmx?WSDL"
    client = Client(wsdl=wsdl, transport=transport)

    result = client.service.Get_EQSymbol()

    try:
        data = json.loads(result)
    except Exception as e:
        print("⚠️ Failed to parse JSON:", e)
        print("Raw result:", result)
        return []

    # Extract only SYMBOL values (if available)
    if isinstance(data, list):
        symbols = [item["SYMBOL"] for item in data if "SYMBOL" in item]
        return symbols
    else:
        return []
    
# -------------------------------
# Mutual Fund Beta Calculation Functions
schemes_cache = None
benchmark_data = None
    
def get_all_schemes():
    """Get all mutual fund schemes with caching"""
    global schemes_cache
    if schemes_cache is not None:
        return schemes_cache
        
    try:
        print("📡 Fetching mutual fund schemes...")
        url = "https://api.mfapi.in/mf"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            schemes_cache = response.json()
            print(f"✅ Loaded {len(schemes_cache)} mutual fund schemes")
            return schemes_cache
        return []
    except Exception as e:
        print(f"❌ Error fetching schemes: {e}")
        return []

def find_scheme( scheme_name):
    """Find scheme by name (flexible matching)"""
    schemes = get_all_schemes()
    if not schemes:
        return None, None
        
    # Try exact match first
    for scheme in schemes:
        if scheme_name.lower() in scheme['schemeName'].lower():
            return scheme['schemeCode'], scheme['schemeName']
            
    return None, None

def get_nav_data( scheme_code):
    """Get historical NAV data for a scheme"""
    try:
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
                df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
                df.set_index('date', inplace=True)
                df = df.sort_index().dropna()
                
                # Filter for last 2 years
                two_years_ago = datetime.now() - timedelta(days=730)
                df = df[df.index >= two_years_ago]
                
                return df['nav']
        return None
    except Exception as e:
        print(f"❌ Error fetching NAV for {scheme_code}: {e}")
        return None

def get_benchmark_data():
    """Get Nifty 50 benchmark data - FIXED VERSION"""
    global benchmark_data
    if benchmark_data is not None:
        return benchmark_data
        
    try:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=365)  # 1 years
        
        print("📈 Downloading Nifty 50 benchmark data...")
        nifty_data = yf.download(
            "^NSEI", 
            start=start_date.date(), 
            end=end_date.date(), 
            auto_adjust=True,
            progress=False
        )
        
        # FIX: Ensure we get 1D series
        if isinstance(nifty_data, pd.DataFrame):
            nifty_series = nifty_data['Close']
        else:
            nifty_series = nifty_data
            
        # Convert to 1D series if needed
        if hasattr(nifty_series, 'squeeze'):
            nifty_series = nifty_series.squeeze()
        
        benchmark_data = nifty_series
        print(f"✅ Nifty data: {len(nifty_series)} points")
        return nifty_series
    except Exception as e:
        print(f"❌ Error downloading benchmark: {e}")
        return None

def calculate_beta( nav_series, benchmark_series):
    """Calculate beta between MF and benchmark - FIXED VERSION"""
    try:

        # FIX: Ensure both are 1D series
        if hasattr(nav_series, 'squeeze'):
            nav_series = nav_series.squeeze()
        if hasattr(benchmark_series, 'squeeze'):
            benchmark_series = benchmark_series.squeeze()
        
        # Convert to DataFrames to align dates properly
        mf_df = pd.DataFrame({'nav': nav_series})
        bench_df = pd.DataFrame({'nifty': benchmark_series})
        
        # Merge on date index
        combined = mf_df.merge(bench_df, left_index=True, right_index=True, how='inner')
        
        if len(combined) < 60:
            print(f"   ⚠ Only {len(combined)} common data points")
            return np.nan
        
        # Calculate daily returns
        returns = combined.pct_change().dropna()
        
        if len(returns) < 40:
            return np.nan
        
        # Calculate beta
        covariance = returns['nav'].cov(returns['nifty'])
        variance = returns['nifty'].var()
        
        beta = covariance / variance if variance != 0 else np.nan
        return beta
        
    except Exception as e:
        print(f"❌ Beta calculation error: {e}")
        return np.nan

def calculate_scheme_beta( scheme_name):
    """
    Calculate beta for a single mutual fund
    """
    print(f"\n🔍 Analyzing: {scheme_name}")
    
    # Step 1: Find scheme code
    scheme_code, full_name = find_scheme(scheme_name)
    if not scheme_code:
        print(f"   ❌ Mutual fund not found: {scheme_name}")
        print(f"   💡 Try using the exact name from the fund house")
        return {
            'scheme_name': scheme_name,
            'full_name': 'N/A',
            'scheme_code': 'N/A',
            'beta': 0,
            'data_points': '0',
            'status': 'Failed'
        }
    
    print(f"   ✅ Found: {full_name}")
    
    # Step 2: Get NAV data
    nav_data = get_nav_data(scheme_code)
    if nav_data is None or len(nav_data) < 60:
        print(f"   ❌ Insufficient NAV data: {len(nav_data) if nav_data else 0} points")
        return {
            'scheme_name': scheme_name,
            'full_name': 'N/A',
            'scheme_code': 'N/A',
            'beta': 0,
            'data_points': '0',
            'status': 'Failed'
        }
    
    print(f"   📊 NAV data points: {len(nav_data)}")
    
    # Step 3: Get benchmark data
    benchmark_data = get_benchmark_data()
    if benchmark_data is None or len(benchmark_data) < 60:
        print("   ❌ Insufficient benchmark data")
        return {
            'scheme_name': scheme_name,
            'full_name': 'N/A',
            'scheme_code': 'N/A',
            'beta': 0,
            'data_points': '0',
            'status': 'Failed'
        }
    
    # Step 4: Calculate beta
    beta = calculate_beta(nav_data, benchmark_data)
    
    if not np.isnan(beta):
        print(f"   🎯 Beta: {beta:.4f}")
        return {
            'scheme_name': scheme_name,
            'full_name': full_name,
            'scheme_code': scheme_code,
            'beta': round(beta, 4),
            'data_points': len(nav_data),
            'status': 'Success'
        }
    else:
        print("   ❌ Could not calculate beta")
        return {
            'scheme_name': scheme_name,
            'full_name': full_name,
            'scheme_code': scheme_code,
            'beta': '0',
            'data_points': len(nav_data) if nav_data else 0,
            'status': 'Failed'
        }

def get_beta_for_symbol(symbol, ptype="Stocks"):
    
    # Download NIFTY Index data
    index_series = download_yahoo_adjclose(YAHOO_INDEX_TICKER, START_DATE, END_DATE)
    if index_series is None or index_series.empty:
        st.error("❌ Failed to download index data.")
    else:
        index_series = index_series.dropna().sort_index()

    if ptype == "Stocks":
        print(f"Calculating beta for stock: {symbol}")
        # Use stock beta function
        sym, beta = get_stock_beta(symbol, index_series)
        print(f"Beta for {symbol}: {beta}")
        return sym, beta
    else:
        print(f"💼 Calculating beta for mutual fund scheme code: {symbol}")
        scheme_name = symbol
        # Calculate beta for this scheme
        result = calculate_scheme_beta(scheme_name)
        if result and 'beta' in result:
            beta = result['beta']
            print(f"Beta for {symbol}: {beta}")
            return symbol, beta
            

# -------------------------------
# Streamlit App
# -------------------------------
st.set_page_config(page_title="Portfolio Beta Calculator", layout="wide")  # 'centered' works better for mobile

st.markdown("<h1>📊 Portfolio Beta & Hedging Calculator</h1>", unsafe_allow_html=True)
# Custom mobile-friendly CSS
st.markdown("""
<style>
h1, h2, h3, h4, h5, h6 {
    
    color: #1E1E1E;
    font-weight: 700;
    margin-top: 0.5rem;
    margin-bottom: 1rem;
    word-wrap: break-word;
}

/* Ensure emoji or icons are vertically aligned */
h1 {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.4rem;
}

/* Prevent title from shrinking on mobile */
@media (max-width: 600px) {
    h1 {
        font-size: 1.4rem !important;
        line-height: 1.6rem !important;
        text-align: center;
    }
}
</style>
""", unsafe_allow_html=True)


# -------------------------------
# Portfolio Input
# -------------------------------
# Load Stock & Mutual Fund Symbols
# -------------------------------
nse_symbols = Get_EQSymbol() #pd.read_csv("EQUITY_L.csv")["SYMBOL"].tolist()

# Fetch mutual fund scheme list from MFAPI
scheme_url = "https://api.mfapi.in/mf"
try:
    scheme_resp = requests.get(scheme_url)
    scheme_resp.raise_for_status()
    scheme_list = scheme_resp.json()
    df_schemes = pd.DataFrame(scheme_list)
    mf_symbols = df_schemes['schemeCode'].tolist()
    mf_name_map = dict(zip(df_schemes['schemeCode'], df_schemes['schemeName']))
except Exception as e:
    st.error(f"Failed to fetch mutual fund list: {e}")
    mf_symbols = []
    mf_name_map = {}

# -------------------------------
# Portfolio Selection
# -------------------------------
portfolio_type = st.selectbox("Select Portfolio Type:", ["Stocks", "Mutual Funds"])

# -------------------------------
# Input Method
# -------------------------------
st.header("1. Portfolio Input")
input_method = st.radio("Choose input method:", ["Manual Entry", "CSV/XLSX Upload"], horizontal=True)
portfolio_data = None

# -------------------------------
# Manual Entry
# -------------------------------
if input_method == "Manual Entry":
    st.subheader(f"Enter {portfolio_type} Manually")
    num_items = st.number_input(f"Number of {portfolio_type}:", min_value=1, max_value=20, value=3)
    items = []

    for i in range(num_items):
        col1, col2 = st.columns(2)
        with col1:
            if portfolio_type == "Stocks":
                symbol = st.selectbox(
                    f"Select Stock Symbol {i+1}",
                    options=nse_symbols,
                    index=nse_symbols.index("RELIANCE") if "RELIANCE" in nse_symbols else 0,
                    key=f"sym_{i}"
                )
                display_name = symbol  # For stocks, symbol is same as display name
            else:
                symbol = st.selectbox(
                    f"Select MF Scheme {i+1}",
                    options=mf_symbols,
                    format_func=lambda code: mf_name_map.get(code, code),
                    key=f"mf_sym_{i}"
                )
                display_name = mf_name_map.get(symbol, str(symbol))  # Use readable MF name

        with col2:
            amount = st.number_input(
                f"Investment Amount (₹) {i+1}",
                min_value=0,
                value=10000,
                step=1000,
                key=f"amt_{i}"
            )

        items.append({
            "SYMBOL": display_name,  
            "AMOUNT": amount
        })

    portfolio_data = pd.DataFrame(items)
    portfolio_data["TYPE"] = portfolio_type  # Add TYPE column

    st.write(f"Your {portfolio_type} Portfolio:")
    st.dataframe(portfolio_data, use_container_width=True)


# -------------------------------
# CSV/XLSX Upload
# -------------------------------
else:
    st.subheader(f"Upload {portfolio_type} Portfolio")
    
    # Sample CSV
    if portfolio_type == "Stocks":
        
        st.info("Your file should have columns: SYMBOL, AMOUNT")
        sample_data = pd.DataFrame({
            "SYMBOL": ["RELIANCE", "INFY"],
            "AMOUNT": [10000, 15000]
        })
        file_name = "sample_stock_portfolio.csv"
    else:
        
        st.info("Your file should have columns: SCHEME_NAME, AMOUNT")
        sample_data = pd.DataFrame({
            "SCHEME_NAME": [mf_symbols[0] if mf_symbols else ""],
            "AMOUNT": [15000]
        })
        file_name = "sample_mf_portfolio.csv"

    csv_buffer = io.StringIO()
    sample_data.to_csv(csv_buffer, index=False)
    st.download_button(
        label=f"📥 Download Sample {portfolio_type} CSV",
        data=csv_buffer.getvalue(),
        file_name=file_name,
        mime="text/csv"
    )

    uploaded_file = st.file_uploader("Choose Portfolio File", type=['csv', 'xlsx'])

    if uploaded_file is not None:
        if uploaded_file.name.endswith('.csv'):
            portfolio_data = pd.read_csv(uploaded_file)
        else:
            portfolio_data = pd.read_excel(uploaded_file)
            
        if "SCHEME_NAME" in portfolio_data.columns and "SYMBOL" not in portfolio_data.columns:
            portfolio_data.rename(columns={"SCHEME_NAME": "SYMBOL"}, inplace=True)

        portfolio_data["TYPE"] = portfolio_type  # Add TYPE column

        
        st.write(f"Uploaded {portfolio_type} Portfolio:")
        valid_rows = []
        not_found = []

        if portfolio_type == "Mutual Funds":
            # --- Validate Mutual Fund Schemes ---
            for _, row in portfolio_data.iterrows():
                try:
                    scheme_name = str(row["SYMBOL"]).strip()
                    if not scheme_name:
                        continue

                    code, name = find_scheme(scheme_name)
                    if code:
                        
                        valid_rows.append(row)
                    else:
                        not_found.append(scheme_name)

                except Exception as e:
                    not_found.append(f"{row['SYMBOL']} (Error: {e})")

        elif portfolio_type == "Stocks":
            # --- Validate Stock Symbols ---

            for _, row in portfolio_data.iterrows():
                try:
                    symbol_name = str(row["SYMBOL"]).strip().upper()
                    if not symbol_name:
                        continue

                    if symbol_name in nse_symbols:
                        valid_rows.append(row)
                    else:
                        not_found.append(symbol_name)

                except Exception as e:
                    not_found.append(f"{row['SYMBOL']} (Error: {e})")

        # --- Keep only valid rows ---
        if valid_rows:
            portfolio_data = pd.DataFrame(valid_rows)
        else:
            portfolio_data = pd.DataFrame(columns=["SYMBOL", "AMOUNT", "TYPE"])

        # --- Show warning for invalid records ---
        if not_found:
            st.warning(f"⚠️ The following {portfolio_type} were not found and removed: {', '.join(not_found)}")

        st.dataframe(portfolio_data ,use_container_width=True)

# Hedge Calculation
if portfolio_data is not None:
    st.header("2. Hedge Settings & Black-Scholes Parameters")
     # Add Hedge Percentage Selection
    st.subheader("🛡️ Hedge Protection Level")
    col1, col2 = st.columns(2)
    with col1:
        hedge_percentage = st.selectbox(
            "How much portfolio do you want to hedge?",
            [100, 75, 50, 25],
            index=0,
            help="Select the percentage of your portfolio exposure you want to protect"
        )
        st.write(f"**Selected: {hedge_percentage}% protection**")
    
    with col2:
        st.info(f"""
        **Hedge Percentage Guide:**
        - **100%**: Full protection (most expensive)
        - **75%**: Balanced protection
        - **50%**: Moderate protection  
        - **25%**: Basic protection (least expensive)
        """)

    if st.button("🚀 Calculate Beta & Hedging", type="primary"):
        if "AMOUNT" not in portfolio_data.columns:
            st.error("❌ Portfolio must have 'AMOUNT' column")
        else:
            try:
                # Convert AMOUNT to numbers
                portfolio_data["AMOUNT"] = pd.to_numeric(portfolio_data["AMOUNT"], errors='coerce')
                print("Portfolio Data after conversion:", portfolio_data)
                if portfolio_data["AMOUNT"].isna().any():
                    st.error("❌ Some AMOUNT values are not valid numbers.")
                else:
                    with st.spinner("📊 Calculating betas and advanced hedging costs..."):
                        # Calculate weights and beta
                        total_amount = portfolio_data["AMOUNT"].sum()
                        if total_amount == 0:
                            st.error("❌ Total portfolio amount cannot be zero")
                        else:
                            portfolio_data["WEIGHT"] = portfolio_data["AMOUNT"] / total_amount
                            print("Portfolio Data with Weights:", portfolio_data)   
                            # # Download index data and calculate beta
                            # index_series = download_yahoo_adjclose(YAHOO_INDEX_TICKER, START_DATE, END_DATE)
                            
                            # if index_series is None or index_series.empty:
                            #     st.error("❌ Failed to download index data.")
                            # else:
                            #     index_series = index_series.dropna().sort_index()

                            # Calculate betas
                            betas = []
                            for sym in portfolio_data["SYMBOL"]:
                                symbol, beta = get_beta_for_symbol(sym, ptype=portfolio_type)
                                betas.append((symbol, beta))
                            
                            # Merge results
                            beta_df = pd.DataFrame(betas, columns=["SYMBOL", "BETA"])
                            merged = pd.merge(portfolio_data, beta_df, on="SYMBOL", how="left")
                            merged["WEIGHTED_BETA"] = merged["WEIGHT"] * merged["BETA"]
                            portfolio_beta = merged["WEIGHTED_BETA"].sum()

                            # Local Hedging
                            hedging_data_tables = Get_hedge_data(portfolio_beta, total_amount, hedge_percentage)
                            #print("Hedging Data Tables:", hedging_data_tables)
                            table1 = hedging_data_tables["Table"]
                            row = table1[0]

                            hedging_data = {
                                "monthly_expiry": row["Curr_Expiry"].split("T")[0],
                                "quarterly_expiry": row["Qut_Expiry"].split("T")[0],
                                "annual_expiry": row["Annual_Expiry"].split("T")[0],

                                "monthly_put_strike": row["Monthly_Strike"],
                                "quarterly_put_strike": row["Quarterly_Strike"],
                                "annual_put_strike": row["Annual_Strike"],

                                "monthly_cost": row["M_totHedgeCost"],
                                "quarterly_cost": row["Q_totHedgeCost"],
                                "annual_cost": row["A_totHedgeCost"],

                                "monthly_annualized_cost": row["M_costPer"],
                                "quarterly_annualized_cost": row["Q_costPer"],
                                "annual_annualized_cost": row["A_costPer"],

                                "monthly_lots": row["MLot"],
                                "quarterly_lots": row["Qlot"],
                                "annual_lots": row["ALot"],

                                "monthly_premium": row["M_Premium"],
                                "quarterly_premium": row["Q_Premium"],
                                "annual_premium": row["A_Premium"]
                            }

                            # Display Results
                            st.header("3. Advanced Hedging Results")
                            st.success("✅ Calculation Complete!")
                            
                            # Protection Level Info
                            st.subheader("🛡️ Protection Details")
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Portfolio Value", f"₹{total_amount:,.2f}")
                            with col2:
                                st.metric("Hedge Percentage", f"{hedge_percentage}%")
                            with col3:
                                st.metric("Portfolio Beta", f"{portfolio_beta:.4f}")
                            with col4:
                                st.metric("Hedge Exposure", f"₹{total_amount * portfolio_beta * (hedge_percentage/100):,.2f}")
                            
                            # hedging cost metrics
                            st.subheader("💰 Hedging Costs & Details")

                            col1, col2, col3 = st.columns(3)

                            # 🗓️ Monthly
                            with col1:
                                st.markdown("#### 🗓️ Monthly")
                                st.metric("Put Strike (₹)", f"{hedging_data['monthly_put_strike']:,}")
                                st.metric("Expiry", hedging_data['monthly_expiry'])
                                st.metric("Cost", f"₹{hedging_data['monthly_cost']:,.2f}")
                                st.metric("Annualized Cost %", f"{hedging_data['monthly_annualized_cost']:.2f}%")
                                st.metric("Lots Required", hedging_data['monthly_lots'])
                                st.metric("Premium per Lot", f"₹{hedging_data['monthly_premium']}")
                            
                            # 📅 Quarterly
                            with col2:
                                st.markdown("#### 📅 Quarterly")
                                st.metric("Put Strike (₹)", f"{hedging_data['quarterly_put_strike']:,}")
                                st.metric("Expiry", hedging_data['quarterly_expiry'])
                                st.metric("Cost", f"₹{hedging_data['quarterly_cost']:,.2f}")
                                st.metric("Annualized Cost %", f"{hedging_data['quarterly_annualized_cost']:.2f}%")
                                st.metric("Lots Required", hedging_data['quarterly_lots'])
                                st.metric("Premium per Lot", f"₹{hedging_data['quarterly_premium']}")
                                                        
                            # 🗓️ Annual
                            with col3:
                                st.markdown("#### 🗓️ Annual")
                                st.metric("Put Strike (₹)", f"{hedging_data['annual_put_strike']:,}")
                                st.metric("Expiry", hedging_data['annual_expiry'])
                                st.metric("Cost", f"₹{hedging_data['annual_cost']:,.2f}")
                                st.metric("Annualized Cost %", f"{hedging_data['annual_annualized_cost']:.2f}%")
                                st.metric("Lots Required", hedging_data['annual_lots'])
                                st.metric("Premium per Lot", f"₹{hedging_data['annual_premium']}")


                            # Portfolio Breakdown
                            st.subheader("📈 Portfolio Breakdown")
                            st.dataframe(merged)
                            
                            table2 = hedging_data_tables["Table1"]
                            # Check if table2 exists and has data
                            if table2 is not None and len(table2) > 0:
                                st.subheader("🎯 Scenario Analysis")
                                
                                # Convert to DataFrame if not already
                                scenario_df = pd.DataFrame(table2)
                                
                                # Normalize column names to lowercase
                                scenario_df.columns = [col.lower() for col in scenario_df.columns]
                                
                                # Display by period (match exact case in data)
                                for period in ['Monthly', 'Quarterly', 'Annual']:  # <-- Capitalized
                                    period_data = scenario_df[scenario_df['period'] == period]
                                    if not period_data.empty:
                                        st.write(f"**{period} Hedging Scenarios:**")
                                        display_data = period_data.drop('period', axis=1)
                                        st.dataframe(display_data, width=800)
                            else:
                                st.info("📊 Scenario analysis data will be available when calculations are complete")

                            # Download Options
                            st.subheader("📥 Download Results")
                            csv = portfolio_data.to_csv(index=False)
                            st.download_button("Download Portfolio CSV", csv, "portfolio_results.csv", "text/csv")

                            excel_wb = create_excel_export(portfolio_data, hedging_data, portfolio_beta, total_amount, hedge_percentage)
                            buffer = io.BytesIO()
                            excel_wb.save(buffer)
                            buffer.seek(0)
                            st.download_button("Export Full Excel Report", buffer, "portfolio_hedging.xlsx",
                                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"❌ Error during calculation: {e}")   

