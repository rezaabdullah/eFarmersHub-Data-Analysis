# data manipulation and analysis
import pandas as pd
import numpy as np

# dash
import dash
from dash import dcc
from dash import html
import plotly.express as px

# database
from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import select

# env file
from dotenv import load_dotenv
import os

# path handling
from pathlib import Path

# logging
import logging

# load env variables
dotenv_path = Path("./.env")
load_dotenv(dotenv_path=dotenv_path)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOSTNAME")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")

def extract_sale(engine):
    """
    read sale table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_sale_transactions", conn, columns=["country_name", "parent_name", "user_region",
                "user_type", "user_name", "user_id", "customer_name", "customer_id", "customer_mobile", "market_type",
                "business_category", "product", "category", "product_id", "transaction_date", "transaction_id", "quantity",
                "unit_type", "unit_price", "currency_exchange_rate", "paid_amount", "product_amount", "cogs_amount",
                "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_sale(df):
    """
    transform sale dataframe and returns df
    :param df: actual sale dataframe
    :return df: transformed dataframe
    """
    
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "category" : "product_category",
        "customer_mobile" : "phone_number",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "cogs_amount" : "cogs",
        "transaction_date" : "date_of_transaction"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"]
    df["product_amount"] = df["product_amount"].astype(float)
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["cogs"] = df["cogs"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]

    # revenue
    df["revenue"] = df["paid_amount"]
    df["revenue_usd"] = df["paid_amount_usd"]
    df["profit"] = df["revenue"] - df["cogs"]
    df['profit_usd'] = df["profit"] / df["currency_rate"]

    # add transaction_type_level_2 column
    df["transaction_type_level_2"] = "Sale"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id", "product_id"], keep="last")

    return df

def extract_purchase(engine):
    """
    read purchase table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_purchase_transactions", conn, columns=["country_name", "parent_name",
                "user_region", "user_type", "user_name", "user_id", "supplier_name", "supplier_mobile", "supplier_id",
                "market_type", "business_category", "product", "category", "product_id", "transaction_date",
                "transaction_id", "quantity", "unit_type", "unit_price", "product_amount", "paid_amount",
                "currency_exchange_rate", "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_purchase(df):
    """
    transform purchase dataframe and returns df
    :param df: actual purchase dataframe
    :return df: transformed dataframe
    """
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "category" : "product_category",
        "supplier_id" : "customer_id",
        "supplier_name" : "customer_name",
        "supplier_mobile" : "phone_number",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "transaction_date" : "date_of_transaction"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["product_amount"] = df["product_amount"].astype(float)
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]

    # purchase
    df["purchase"] = df["paid_amount"]
    df["purchase_usd"] = df["paid_amount_usd"]

    # add transaction_type_level_2 column
    df["transaction_type_level_2"] = "Purchase"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id", "product_id"], keep="last")

    return df

def extract_machine_rent(engine):
    """
    read machine rent table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_machine_rent_transactions", conn, columns=["country_name", "parent_name",
            "user_region", "user_type", "user_name", "user_id", "customer_name", "customer_mobile", "customer_id",
            "business_category", "product", "category", "product_id", "transaction_date", "transaction_id",
            "quantity", "unit_type","unit_price", "amount", "paid_amount", "currency_exchange_rate", "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_machine_rent(df):
    """
    transform machine_rent dataframe and returns df
    :param df: actual machine_rent dataframe
    :return df: transformed dataframe
    """
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "category" : "product_category",
        "customer_mobile" : "phone_number",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "amount" : "product_amount",
        "transaction_date" : "date_of_transaction"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["product_amount"] = df["product_amount"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]

     # revenue
    df["revenue"] = df["paid_amount"]
    df["revenue_usd"] = df["paid_amount_usd"]
    df["profit"] = df["paid_amount"]
    df['profit_usd'] = df["paid_amount_usd"]

    # add transaction_type_level_2 column
    df["market_type'"] = "Farmer"
    df["transaction_type_level_2"] = "Machinery Rental"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id", "product_id"], keep="last")

    return df

def extract_processing(engine):
    """
    read processing table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_processing_transactions", conn, columns=["country_name", "parent_name",
                "user_region", "user_type", "user_name", "user_id", "business_category", "product", "category",
                "product_id", "transaction_date", "transaction_id", "quantity", "unit_type","unit_price", "amount",
                "production_cost", "currency_exchange_rate", "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_processing(df):
    """
    transform processing dataframe and returns df
    :param df: actual processing dataframe
    :return df: transformed dataframe
    """
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "category" : "product_category",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "production_cost" : "paid_amount",
        "amount" : "product_amount",
        "transaction_date" : "date_of_transaction"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)
    df["product_amount"] = df["product_amount"].astype(float)

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]

     # processing
    df["processing"] = df["paid_amount"]
    df["processing_usd"] = df["paid_amount_usd"]

    # add transaction_type_level_2 column
    df["market_type"] = "Farmers' Hub"
    df["transaction_type_level_2"] = "Processing"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id", "product_id"], keep="last")

    return df

def extract_expense(engine):
    """
    read expense table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_expense_transactions", conn, columns=["country_name", "parent_name", "user_region",
                "user_type", "user_name", "user_id", "business_category", "expense_type", "expense_category",
                "product_category", "transaction_date", "transaction_id", "currency_exchange_rate", "total_amount",
                "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_expense(df):
    """
    transform expense dataframe and returns df
    :param df: actual expense dataframe
    :return df: transformed dataframe
    """
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "expense_category" : "transaction_type_level_2",
        "expense_type" : "product",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "total_amount" : "paid_amount",
        "transaction_date" : "date_of_transaction"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)

    # product_amount and paid_amount is same for processing
    df["product_amount"] = df["paid_amount"]

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]

     # processing
    df["expenses"] = df["paid_amount"]
    df["expenses_usd"] = df["paid_amount_usd"]

    # add transaction_type_level_2 column
    df["market_type"] = "Farmers' Hub"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id"], keep="last")

    return df

def extract_machine_purchase(engine):
    """
    read machine_purchase table from sql database and returns df
    :param engine: SQLAlchemy engine object
    :return df: sale dataframe
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_table("gds_machine_purchase_transactions", conn, columns=["country_name", "parent_name",
            "user_region", "user_type", "user_name", "user_id", "supplier_name", "supplier_mobile", "supplier_id",
            "business_category", "product", "category", "product_id", "transaction_date", "transaction_id",
            "quantity", "unit_price", "total_amount", "paid_amount", "currency_exchange_rate", "version"])
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error
        
    return df

def transform_machine_purchase(df):
    """
    transform machine_purchase dataframe and returns df
    :param df: actual machine_purchase dataframe
    :return df: transformed dataframe
    """
    # rename columns
    df.rename(columns={"country_name" : "country",
        "business_category" : "transaction_type",
        "category" : "product_category",
        "user_name" : "user",
        "user_region" : "region",
        "parent_name" : "franchisee",
        "currency_exchange_rate" : "currency_rate",
        "total_amount" : "product_amount",
        "transaction_date" : "date_of_transaction",
        "supplier_id" : "customer_id",
        "supplier_name" : "customer_name",
        "supplier_mobile" : "phone_number"}, inplace=True)
    
    # convert date_of_transaction to datetime
    df["date_of_transaction"] = pd.to_datetime(df["date_of_transaction"], format="%Y/%m/%d")

    # convert user_id to string
    df["user_id"] = df["user_id"].astype(str)

    # convert and round numerical columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["paid_amount"] = df["paid_amount"].astype(float)
    df["currency_rate"] = df["currency_rate"].astype(float)
    df["product_amount"] = df["product_amount"].astype(float)

    # usd conversion
    df["paid_amount_usd"] = df["paid_amount"] / df["currency_rate"]
    df["machine_purchase_usd"] = df["paid_amount_usd"]
    df["machine_purchase"] = df["paid_amount"]

    # add transaction_type_level_2 column
    df["market_type"] = "Farmer"
    df["transaction_type_level_2"] = "Machinery"

    # sorting data based on version and keep the latest version only
    df = df.sort_values(["country", "franchisee", "user_id", "transaction_id", "version"]) \
            .drop_duplicates(subset=["transaction_id", "product_id"], keep="last")

    return df

if __name__ == "__main__":
    # connect to database
    connect_url = URL.create(
        "mysql+pymysql",
        username=USERNAME,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DATABASE
    )

    # engine = create_engine(connect_url, echo=True) # debug
    engine = create_engine(connect_url)

    # debug
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            print(table_names)
    except Exception as e:
        logging.basicConfig(filename="./log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", level=logging.ERROR)
        logging.error(e)

    # sale
    # sale = extract_sale(engine)
    # sale = transform_sale(sale)

    # # purchase
    # purchase = extract_purchase(engine)
    # purchase = transform_purchase(purchase)

    # # machine rent
    # machine_rent = extract_machine_rent(engine)
    # machine_rent = transform_machine_rent(machine_rent)

    # # processing
    # processing = extract_processing(engine)
    # processing = transform_processing(processing)

    # # expense
    # expense = extract_expense(engine)
    # expense = transform_expense(expense)

    # # machine purchase
    # machine_purchase = extract_machine_purchase(engine)
    # machine_purchase = transform_machine_purchase(machine_purchase)