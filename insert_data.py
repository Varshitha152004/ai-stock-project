import math

import yfinance as yf
import json
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
def safe_number(v):
    if v is None:
        return 0
    if isinstance(v, float) and math.isnan(v):
        return 0
    return float(v)
# ======================================================
# DATABASE CONNECTION
# ======================================================

engine = create_engine(
    "postgresql://postgres:newpassword123@localhost:5432/stock_screener"
)
def normalize_symbol(symbol):

    indian_stocks = [
        "INFY","TCS","WIPRO","HCLTECH","RELIANCE","HDFCBANK",
        "ICICIBANK","KOTAKBANK","SBIN","AXISBANK","LT","ITC",
        "MARUTI","TATAMOTORS","HINDUNILVR","NESTLEIND",
        "ASIANPAINT","ULTRACEMCO","BAJAJ-AUTO","DIVISLAB",
        "ADANIGREEN","ADANIPORTS","ADANIENT","ADANIGAS","ADANITRANS"
    ]

    if symbol in indian_stocks:
        return symbol + ".NS"

    return symbol
# ======================================================
# STOCK LIST
# ======================================================

symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "INFY", "TCS", "WIPRO", "HCLTECH","RELIANCE","HDFCBANK", "ICICIBANK", "KOTAKBANK", "SBIN", "AXISBANK", "LT", "ITC", "MARUTI", "TATAMOTORS","HINDUNILVR","NESTLEIND","ASIANPAINT","ULTRACEMCO","BAJAJ-AUTO","DIVISLAB","ADANIGREEN","ADANIPORTS","ADANIENT","ADANIGAS","ADANITRANS","ADANIGREEN","ADANIPORTS","ADANIENT","ADANIGAS","ADANITRANS","ADANIGREEN","ADANIPORTS","ADANIENT","ADANIGAS"]

DATA_FOLDER = "yf_data"

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# ======================================================
# PROCESS EACH STOCK
# ======================================================

for ticker_symbol in symbols:

    print(f"\nFetching {ticker_symbol}...")

    try:
        yf_symbol = normalize_symbol(ticker_symbol)

        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        history = ticker.history(period="1y")

        if not info or history.empty:
            print(f" Skipping {ticker_symbol} (No valid data)")
            continue

        # ======================================================
        # COMPANY PROFILE
        # ======================================================

        company_profile = {
            "name": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "website": info.get("website"),
            "market_cap": info.get("marketCap") or 0,
            "description": info.get("longBusinessSummary"),
            "logo_url": info.get("logo_url"),
            "Category": info.get("category"),
            "Book Value": info.get("bookValue") or 0,
            "Show Profile": info.get("showProfile") or False,
            "Change From Previous Close": info.get("regularMarketChangePercent") or 0,
            "Profit after Tax": info.get("profitMargin") or 0,
            "change percent": info.get("changePercent") or 0,
            "Last Traded Price": info.get("regularMarketPrice") or 0,
        }

        # ======================================================
        # CALCULATED METRICS (Aligned With main.py)
        # ======================================================

        pe_ratio = info.get("trailingPE") or 0
        eps = info.get("trailingEps") or 0
        revenue = info.get("totalRevenue") or 0
        debt = info.get("totalDebt") or 0
        market_cap = info.get("marketCap") or 0

        revenue_growth = info.get("revenueGrowth")
        revenue_growth = (revenue_growth * 100) if revenue_growth else 0

        first_close = history.iloc[0]["Close"]
        last_close = history.iloc[-1]["Close"]
        price_change_1y = ((last_close - first_close) / first_close) * 100

        if math.isnan(price_change_1y):
            price_change_1y = 0

        # ======================================================
        # HISTORICAL PRICES
        # ======================================================

        historical_prices = []

        for date, row in history.iterrows():
            historical_prices.append({
                "date": str(date.date()),
                "open": safe_number(row["Open"]),
                "high": safe_number(row["High"]),
                "low": safe_number(row["Low"]),
                "close": safe_number(row["Close"]),
                "volume": int(row["Volume"] or 0)
            })

        # ======================================================
        # SAVE JSON SNAPSHOT
        # ======================================================

        stock_json = {
            "ticker": ticker_symbol,
            "snapshot_date": datetime.now().isoformat(),
            "company_profile": company_profile,
            "fundamentals": {
                "pe_ratio": pe_ratio,
                "eps": eps,
                "revenue": revenue,
                "debt": debt,
                "market_cap": market_cap,
                "revenue_growth": revenue_growth,
                "price_change_1y": price_change_1y
            },
            "historical_prices": historical_prices
        }

        file_path = os.path.join(DATA_FOLDER, f"{ticker_symbol}_structured.json")

        with open(file_path, "w") as f:
            json.dump(stock_json, f, indent=4)

        print(f" JSON saved for {ticker_symbol}")

        # ======================================================
        # DATABASE INSERTION
        # ======================================================

        # 1️. Insert into symbols
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO symbols (symbol, company_name, sector)
                VALUES (:symbol, :company_name, :sector)
                ON CONFLICT (symbol) DO NOTHING
            """), {
                "symbol": ticker_symbol,
                "company_name": company_profile["name"],
                "sector": company_profile["sector"]
            })

        # 2️. Get symbol_id
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id FROM symbols WHERE symbol = :symbol
            """), {"symbol": ticker_symbol})
            symbol_id = result.fetchone()[0]

        # 3️. Insert fundamentals (SAFE + UPDATE IF EXISTS)

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fundamentals
                (symbol_id, pe_ratio, eps, revenue,
                debt, market_cap, revenue_growth,
                price_change_1y, reported_date)
                VALUES
                (:symbol_id, :pe_ratio, :eps, :revenue,
                :debt, :market_cap, :revenue_growth,
                :price_change_1y, :reported_date)
                ON CONFLICT (symbol_id, reported_date)
                DO UPDATE SET
                    pe_ratio = EXCLUDED.pe_ratio,
                    eps = EXCLUDED.eps,
                    revenue = EXCLUDED.revenue,
                    debt = EXCLUDED.debt,
                    market_cap = EXCLUDED.market_cap,
                    revenue_growth = EXCLUDED.revenue_growth,
                    price_change_1y = EXCLUDED.price_change_1y
            """), {
                "symbol_id": symbol_id,
                "pe_ratio": safe_number(pe_ratio),
                "eps": safe_number(eps),
                "revenue": safe_number(revenue),
                "debt": safe_number(debt),
                "market_cap": safe_number(market_cap),
                "revenue_growth": safe_number(revenue_growth),
                "price_change_1y": safe_number(price_change_1y),
                "reported_date": datetime.now().date()
            })

        # 4️. Insert historical prices properly
        with engine.begin() as conn:
            for price in historical_prices:
                conn.execute(text("""
                    INSERT INTO historical_prices
                    (symbol_id, price_date, open, high, low, close, volume)
                    VALUES
                    (:symbol_id, :price_date, :open, :high, :low, :close, :volume)
                    ON CONFLICT (symbol_id, price_date) DO NOTHING
                """), {
                    "symbol_id": symbol_id,
                    "price_date": price["date"],
                    "open": price["open"],
                    "high": price["high"],
                    "low": price["low"],
                    "close": price["close"],
                    "volume": price["volume"]
                })

        print(f" Database updated for {ticker_symbol}")

        time.sleep(2)

    except Exception as e:
        print(f" Error processing {ticker_symbol}: {e}")

print("\n Snapshot + database ingestion completed successfully.")
