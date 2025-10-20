# web_app_local_dynamic.py - Portfolio Beta & Hedging with Custom Volatility & Risk-Free Rate
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, timedelta
import io
from openpyxl import Workbook
from openpyxl.styles import Font
from scipy.stats import norm

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
            threads=False
        )
        if data is None or data.empty:
            return None
        return data.get("Adj Close") or data.get("Close")
    except Exception as e:
        st.warning(f"Failed to fetch {ticker}: {e}")
        return None

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

def black_scholes_put_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(K - S, 0)
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    P = K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)
    return P

def calculate_hedging(portfolio_beta, total_value, hedge_percentage, r, sigma):
    hedge_exposure = total_value * portfolio_beta * (hedge_percentage / 100)

    monthly_strike = round(hedge_exposure * 0.95)
    quarterly_strike = round(hedge_exposure * 0.90)
    annual_strike = round(hedge_exposure * 0.85)

    monthly_T = 30/365
    quarterly_T = 90/365
    annual_T = 365/365

    monthly_cost = black_scholes_put_price(hedge_exposure, monthly_strike, monthly_T, r, sigma)
    quarterly_cost = black_scholes_put_price(hedge_exposure, quarterly_strike, quarterly_T, r, sigma)
    annual_cost = black_scholes_put_price(hedge_exposure, annual_strike, annual_T, r, sigma)

    monthly_annualized = (monthly_cost / total_value) * (365/30) * 100
    quarterly_annualized = (quarterly_cost / total_value) * (365/90) * 100
    annual_annualized = (annual_cost / total_value) * 100

    scenarios = [-0.2, -0.1, 0, 0.1, 0.2]
    scenario_analysis = []
    for pct in scenarios:
        new_portfolio = total_value * (1 + pct)
        monthly_payoff = max(0, monthly_strike - new_portfolio)
        quarterly_payoff = max(0, quarterly_strike - new_portfolio)
        annual_payoff = max(0, annual_strike - new_portfolio)
        scenario_analysis.append({
            "period": "Monthly",
            "scenario": f"{pct*100:+.0f}%",
            "end_portfolio": new_portfolio,
            "put_payoff": monthly_payoff,
            "net_value_hedge": new_portfolio + monthly_payoff - monthly_cost
        })
        scenario_analysis.append({
            "period": "Quarterly",
            "scenario": f"{pct*100:+.0f}%",
            "end_portfolio": new_portfolio,
            "put_payoff": quarterly_payoff,
            "net_value_hedge": new_portfolio + quarterly_payoff - quarterly_cost
        })
        scenario_analysis.append({
            "period": "Annual",
            "scenario": f"{pct*100:+.0f}%",
            "end_portfolio": new_portfolio,
            "put_payoff": annual_payoff,
            "net_value_hedge": new_portfolio + annual_payoff - annual_cost
        })

    return {
        "monthly_put_strike": monthly_strike,
        "quarterly_put_strike": quarterly_strike,
        "annual_put_strike": annual_strike,
        "monthly_expiry": (datetime.now() + timedelta(days=30)).strftime("%d-%b-%Y"),
        "quarterly_expiry": (datetime.now() + timedelta(days=90)).strftime("%d-%b-%Y"),
        "annual_expiry": (datetime.now() + timedelta(days=365)).strftime("%d-%b-%Y"),
        "monthly_cost": monthly_cost,
        "quarterly_cost": quarterly_cost,
        "annual_cost": annual_cost,
        "monthly_annualized_cost": monthly_annualized,
        "quarterly_annualized_cost": quarterly_annualized,
        "annual_annualized_cost": annual_annualized,
        "scenario_analysis": scenario_analysis
    }

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

# -------------------------------
# Streamlit App
# -------------------------------
st.set_page_config(page_title="Portfolio Beta Calculator", layout="wide")
st.title("📊 Portfolio Beta & Hedging Calculator (Local, Custom Volatility)")

# Portfolio Input
st.header("1. Portfolio Input")
input_method = st.radio("Choose input method:", ["Manual Entry", "CSV Upload"])
portfolio_data = None

if input_method == "Manual Entry":
    num_stocks = st.number_input("Number of stocks:", min_value=1, max_value=20, value=3)
    stocks = []
    for i in range(num_stocks):
        col1, col2 = st.columns(2)
        with col1:
            symbol = st.text_input(f"Stock Symbol {i+1}", value="RELIANCE", key=f"sym_{i}")
        with col2:
            amount = st.number_input(f"Investment Amount (₹) {i+1}", min_value=0, value=10000, key=f"amt_{i}")
        stocks.append({"SYMBOL": symbol.upper().replace('.NS', ''), "AMOUNT": amount})
    portfolio_data = pd.DataFrame(stocks)
    st.write("Your Portfolio:")
    st.dataframe(portfolio_data)

else:
    uploaded_file = st.file_uploader("Choose CSV file", type=['csv'])
    if uploaded_file is not None:
        portfolio_data = pd.read_csv(uploaded_file)
        st.write("Uploaded Portfolio:")
        st.dataframe(portfolio_data)

# Hedge Calculation
if portfolio_data is not None:
    st.header("2. Hedge Settings & Black-Scholes Parameters")
    hedge_percentage = st.selectbox("Hedge Percentage", [100, 75, 50, 25], index=0)
    col1, col2 = st.columns(2)
    with col1:
        r = st.number_input("Risk-Free Rate (%)", min_value=0.0, max_value=20.0, value=7.0) / 100
    with col2:
        sigma = st.number_input("Volatility (%)", min_value=0.0, max_value=200.0, value=25.0) / 100

    if st.button("🚀 Calculate Beta & Hedging"):
        total_value = portfolio_data["AMOUNT"].sum()
        portfolio_data["WEIGHT"] = portfolio_data["AMOUNT"] / total_value

        # Download index series
        index_series = download_yahoo_adjclose(YAHOO_INDEX_TICKER, START_DATE, END_DATE)
        betas = []
        for sym in portfolio_data["SYMBOL"]:
            _, beta = get_stock_beta(sym, index_series)
            betas.append(beta)
        portfolio_data["BETA"] = betas
        portfolio_data["WEIGHTED_BETA"] = portfolio_data["WEIGHT"] * portfolio_data["BETA"]
        portfolio_beta = portfolio_data["WEIGHTED_BETA"].sum()

        # Local Hedging
        hedging_data = calculate_hedging(portfolio_beta, total_value, hedge_percentage, r, sigma)

        # Display Metrics
        st.subheader("🛡️ Protection Details")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Value", f"₹{total_value:,.2f}")
        col2.metric("Hedge %", f"{hedge_percentage}%")
        col3.metric("Portfolio Beta", f"{portfolio_beta:.4f}")
        col4.metric("Hedge Exposure", f"₹{total_value*portfolio_beta*(hedge_percentage/100):,.2f}")

        # Hedging Costs
        st.subheader("💰 Hedging Costs")
        for period in ["monthly", "quarterly", "annual"]:
            st.write(f"**{period.capitalize()}**")
            st.write(f"Put Strike: ₹{hedging_data[f'{period}_put_strike']}")
            st.write(f"Expiry: {hedging_data[f'{period}_expiry']}")
            st.write(f"Cost: ₹{hedging_data[f'{period}_cost']:,.2f}")
            st.write(f"Annualized: {hedging_data[f'{period}_annualized_cost']:.2f}%")

        # Scenario Analysis
        st.subheader("🎯 Scenario Analysis")
        scenario_df = pd.DataFrame(hedging_data["scenario_analysis"])
        st.dataframe(scenario_df)

        # Download Options
        st.subheader("📥 Download Results")
        csv = portfolio_data.to_csv(index=False)
        st.download_button("Download Portfolio CSV", csv, "portfolio_results.csv", "text/csv")

        excel_wb = create_excel_export(portfolio_data, hedging_data, portfolio_beta, total_value, hedge_percentage)
        buffer = io.BytesIO()
        excel_wb.save(buffer)
        buffer.seek(0)
        st.download_button("Export Full Excel Report", buffer, "portfolio_hedging.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
