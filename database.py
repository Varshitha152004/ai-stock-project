from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:newpassword123@localhost:5432/stock_screener"
engine = create_engine(DATABASE_URL)