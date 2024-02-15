import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
import requests
import numpy as np
import pandas as pd
import sqlite3
from sqlalchemy import create_engine


## CONFIGURATION
# -----

if os.path.isfile("./configs/.env"):
    load_dotenv("./configs/.env")
DATA_PTH = os.getenv("DATA_PTH")
IMG_PTH = os.getenv("IMG_PTH")
DB_NAME = os.getenv("DB_NAME")
DELTA = int(os.getenv("DELTA"))
VERBOSE = bool(os.getenv("VERBOSE"))


## HELPER FUNCTIONS
# -----

def execute_sql(sql:str) -> None:
    with sqlite3.connect(f"./{DATA_PTH}/{DB_NAME}.db") as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
    
    return None


def query_database(sql:str) -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{DATA_PTH}/{DB_NAME}.db", echo=False)
    
    df = pd.read_sql(sql, con=engine)
    
    engine.dispose()
    
    return df


def get_data_period() -> tuple:
    end = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=DELTA)
    
    end = end.strftime("%Y%m%d")
    start = start.strftime("%Y%m%d")
    
    db_date = query_database(
        """
        SELECT MAX(date) AS db_date
        FROM t_price;
        """
    )["db_date"][0]
    
    if db_date and db_date > start:
        start = datetime.strptime(db_date, "%Y%m%d") \
            + timedelta(days=1)
        start = start.strftime("%Y%m%d")
    
    return start, end


def fetch_file_version(start:str, end:str) -> list:
    url = (
        "https://api.data.gov.hk/v1/historical-archive/list-file-versions?"
        "url=https://online-price-watch.consumer.org.hk/opw/opendata/pricewatch.json&"
        f"start={start}&"
        f"end={end}&"
    )
    file_version = requests.get(url)
    file_version = file_version.json()
    file_version = file_version["timestamps"]
    
    return file_version


def fetch_opw_data(file_dt:str) -> tuple:
    url = (
        "https://api.data.gov.hk/v1/historical-archive/get-file?"
        "url=https://online-price-watch.consumer.org.hk/opw/opendata/pricewatch.json&"
        f"time={file_dt}&"
    )
    file = requests.get(url)
    file = file.json()
    
    price_list, item_list = [], []
    for item in file:
        code = item["code"] = item["code"].upper()
        
        prices = item.pop("prices")
        offers = item.pop("offers")
        
        price_dict = {
            price["supermarketCode"]: price
            for price in prices
        }
        offer_dict = {
            offer["supermarketCode"]: offer
            for offer in offers
        }
        
        price = [
            {
                "code": code, "date": file_dt,
                **price_dict.get(smkt, {}),
                **offer_dict.get(smkt, {}),
            }
            for smkt in set(price_dict) | set(offer_dict)
        ]
        
        price_list += price
        item_list.append(item)
    
    df_price = pd.DataFrame.from_records(price_list)
    df_item = pd.json_normalize(item_list)
    
    return df_price, df_item


def cleanse_opw_data(df_price:pd.DataFrame, df_item:pd.DataFrame) -> tuple:
    for df in (df_price, df_item):
        df.drop(columns=df.filter(regex="zh-Hans").columns, inplace=True)
    
    df_price["price"] = df_price["price"].str.extract(r"([\d\.]+)") \
        .astype(float).fillna(0)
    df_price["date"] = df_price["date"].str.extract("(\d{8})")
    df_price["en"] = df_price["en"].fillna("No Promotion")
    df_price["zh-Hant"] = df_price["zh-Hant"].fillna("No Promotion")
    
    price_metadata = {
        "code": "sku",
        "date": "date",
        "supermarketCode": "smkt",
        "price": "price_orig",
        "en": "promo_en",
        "zh-Hant": "promo_zh",
    }
    df_price.rename(columns=price_metadata, inplace=True)
    
    item_metadata = {
        "code": "sku",
        "brand.en": "brand_en",
        "brand.zh-Hant": "brand_zh",
        "name.en": "name_en",
        "name.zh-Hant": "name_zh",
        "cat1Name.en": "dept_en",
        "cat1Name.zh-Hant": "dept_zh",
        "cat2Name.en": "cat_en",
        "cat2Name.zh-Hant": "cat_zh",
        "cat3Name.en": "subcat_en",
        "cat3Name.zh-Hant": "subcat_zh",
    }
    df_item.rename(columns=item_metadata, inplace=True)
    df_item.drop_duplicates(subset="sku", ignore_index=True, inplace=True)
    
    return df_price, df_item


def calculate_discounted_price(price_data:pd.DataFrame) -> pd.DataFrame:
    df = price_data.copy()
    
    df["pat"] = df["promo_en"].apply(
        lambda x: str(x).lower().split("/")
    )
    df = df.explode("pat", ignore_index=True)
    
    pats = {
        r"\s": "",
        r"\$(\d+\.?\d*)": "{AMT}",
        r"\d+%": "{PCT}",
        r"\d+": "{NUM}",
    }
    for pat, tag in pats.items():
        df["pat"] = df["pat"].str.replace(pat, tag, regex=True)
    
    df["amt"] = df["pat"].str.count("{AMT}")
    df["pct"] = df["pat"].str.count("{PCT}")
    df["num"] = df["pat"].str.count("{NUM}")
    
    # AMT and PCT | get {PCT}% off upon buying ${AMT}
    data = df.query(
        "amt + pct == 2 and num != 2 and "
        "pat.str.contains('{AMT}.*{PCT}')"
    )
    amt = data["promo_en"].str.extract(r"\$(\d+\.?\d*)")[0] \
        .astype(float)
    qty = (amt / data["price_orig"]).map(np.ceil)
    pct = data["promo_en"].str.extract(r"(\d+)%")[0] \
        .astype(float)
    pct = 1 - pct / 100
    
    df.loc[data.index, "unit_price"] = data["price_orig"] * qty * pct / qty
    
    # AMT and NUM | buy {NUM} at ${AMT} or buy {NUM} save ${AMT}
    data = df.query(
        "amt + num == 2 and num != 2 and "
        "pat.str.contains('{NUM}[\w\s]*{AMT}\.?$')"
    )
    qty = data["promo_en"].str.extract(r"(\d+)")[0] \
        .astype(float)
    amt = data["promo_en"].str.extract(r"\$(\d+\.?\d*)")[0] \
        .astype(float)
    dis = data["pat"].str.contains("save") \
        .astype(int)
    
    df.loc[data.index, "unit_price"] = abs(
        (data["price_orig"] * qty * dis - amt) / qty
    )
    
    # PCT and NUM | buy {NUM} get {PCT}% off or get {PCT}% off on the second item
    data = df.query(
        "pct + num == 2 and num != 2 and "
        "pat.str.contains('{NUM}(?!nd)[\w\s]*{PCT}') or "
        "pat.str.contains('{NUM}nd[\w\s]*{PCT}')"
    )
    qty = data["promo_en"].str.extract(r"(\d+)")[0] \
        .astype(float)
    pct = data["promo_en"].str.extract(r"(\d+)%")[0] \
        .astype(float)
    pct = 1 - pct / 100
    dis = data["pat"].str.contains("{NUM}nd") \
        .astype(int)
    
    df.loc[data.index, "unit_price"] = data["price_orig"] / qty \
        * (dis * (pct+1) + (1-dis) * pct*qty)
    
    # 2 NUM | buy {NUM} get {NUM} FREE
    data = df.query(
        "num == 2 and "
        "pat.str.contains('buy{NUM}get{NUM}')"
    )
    qty = data["promo_en"].str.extract(r"(\d+)")[0] \
        .astype(float)
    num = data["promo_en"].str.extract(r".*(\d+)")[0] \
        .astype(float)
    
    df.loc[data.index, "unit_price"] = data["price_orig"] * qty / (qty + num)
    
    df["price"] = np.where(
        df["unit_price"] / df["price_orig"] > .3,
        df["unit_price"], df["price_orig"],
    )
    
    df.sort_values(by=["sku", "date", "smkt", "price"], inplace=True)
    df.drop_duplicates(subset=["sku", "date", "smkt"], inplace=True)
    
    col = [
        "sku", "date", "smkt",
        "price", "price_orig",
        "promo_en", "promo_zh",
    ]
    df = df[col]
    
    return df


def filter_out_cached_item(item_data:pd.DataFrame) -> pd.DataFrame:
    sku_list = query_database(
        """
        SELECT sku
        FROM t_item;
        """
    )["sku"].to_list()
    
    df = item_data.copy()
    df.query("sku not in @sku_list", inplace=True)
    
    return df


def cache_opw_data(*args:tuple) -> None:
    engine = create_engine(f"sqlite:///{DATA_PTH}/{DB_NAME}.db", echo=False)
    
    for arg in args:
        arg[0].to_sql(arg[1], con=engine, if_exists=arg[2], index=False)
    
    engine.dispose()
    
    return None


def get_smkt_order(smkt_lst:list) -> list:
    smkt_order = [
        "WELLCOME", "PARKNSHOP", "JASONS", "AEON",
        "MANNINGS", "WATSONS", "DCHFOOD",
    ]
    
    smkt_misc = [
        smkt for smkt in smkt_lst
        if smkt not in smkt_order
    ]
    
    smkt_order += smkt_misc
    
    return smkt_order


## SETUP FUNCTIONS
# -----
def create_data_path() -> None:
    for pth in [DATA_PTH, IMG_PTH]:
        os.makedirs(pth, exist_ok=True)
    
    return None


def initialise_database() -> None:
    sql_create_t_item = """
    CREATE TABLE IF NOT EXISTS t_item (
        sku       TEXT PRIMARY KEY,
        brand_en  TEXT,
        brand_zh  TEXT,
        name_en   TEXT,
        name_zh   TEXT,
        dept_en   TEXT,
        dept_zh   TEXT,
        cat_en    TEXT,
        cat_zh    TEXT,
        subcat_en TEXT,
        subcat_zh TEXT
    );
    """
    
    sql_create_t_price = """
    CREATE TABLE IF NOT EXISTS t_price (
        sku        TEXT,
        date       TEXT,
        smkt       TEXT,
        price      FLOAT,
        price_orig FLOAT,
        promo_en   TEXT,
        promo_zh   TEXT
    );
    """
    
    sql_create_t_tracker = """
    CREATE TABLE IF NOT EXISTS t_tracker (
        uid TEXT,
        sku TEXT
    );
    """
    
    sql_create_t_user = """
    CREATE TABLE IF NOT EXISTS t_user (
        uid  TEXT PRIMARY KEY,
        lang TEXT
    );
    """
    
    sql_create_t_watchdog = """
    CREATE TABLE IF NOT EXISTS t_watchdog (
        sku       TEXT PRIMARY KEY,
        smkt      TEXT,
        price     FLOAT,
        promo_en  TEXT,
        promo_zh  TEXT,
        freq      INT,
        price_avg FLOAT,
        price_std FLOAT,
        price_q0  FLOAT,
        price_q1  FLOAT,
        price_q4  FLOAT,
        alert     INT,
        date      TEXT
    );
    """
    
    for var, sql in dict(locals()).items():
        if var.startswith("sql_") and not callable(sql):
            execute_sql(sql)
    
    return None


## STEP FUNCTIONS
# -----

def refresh_database() -> bool:
    sql_delete_t_price = f"""
    DELETE FROM t_price
    WHERE date < strftime('%Y%m%d',date('now', '-{DELTA+1} days'));
    """
    
    execute_sql(sql_delete_t_price)
    print("\u21ba Deleted preceding OPW data.") if VERBOSE else None
    
    return True


def ingest_opw_data() -> bool:
    date_start, date_end = get_data_period()
    
    if date_start >= date_end:
        print("\u21ba OPW data is up-to-date.") if VERBOSE else None
        
        return True
    
    versions = fetch_file_version(date_start, date_end)
    print(f"\u21ba Impending {len(versions)} days of OPW data.") if VERBOSE else None
    
    df_price = pd.DataFrame()
    df_item = pd.DataFrame()
    
    for idx, version in enumerate(versions):
        price, item = fetch_opw_data(version)
        df_price = pd.concat([df_price, price], ignore_index=True)
        df_item = pd.concat([df_item, item], ignore_index=True)
        print(f"\t\u2605 Awaiting {len(versions)-idx-1} days of OPW data.") \
            if VERBOSE and (idx+1) % 10 == 0 and idx + 1 != len(versions) else None
    
    df_price, df_item = cleanse_opw_data(df_price, df_item)
    df_price = calculate_discounted_price(df_price)
    df_item = filter_out_cached_item(df_item)
    print(f"\u21ba Cleansed OPW data.") if VERBOSE else None
    
    cache_price = (df_price, "t_price", "append")
    cache_item = (df_item, "t_item", "append")
    cache_opw_data(cache_price, cache_item)
    
    return True


def analyse_opw_data() -> bool:
    ytd = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    update_date = query_database(
        """
        SELECT MAX(date) AS update_date
        FROM t_watchdog;
        """
    )["update_date"][0]
    
    if update_date and update_date == ytd:
        print("\u21ba OPW data is analysed.") if VERBOSE else None
        
        return True
    
    sku_ts = query_database(
        """
        SELECT sku, date
            , MIN(price) AS price
        FROM t_price
        GROUP BY sku, date;
        """
    )
    
    col = [
        "sku", "freq", "price_avg", "price_std",
        "price_q0", "price_q1", "price_q2", "price_q3", "price_q4",
    ]
    sku_stats = sku_ts.groupby("sku", as_index=False) \
        ["price"].describe()
    sku_stats.columns = col
    
    sku_ytd = query_database(
        """
        SELECT
            sku, smkt, price
            , promo_en, promo_zh
        FROM t_price
        WHERE date = (
            SELECT MAX(date)
            FROM t_price
        );
        """
    )
    
    smkt_lst = sku_ytd["smkt"].unique()
    smkt_order = get_smkt_order(smkt_lst)
    
    sku_ytd["smkt"] = sku_ytd["smkt"].astype("category")
    sku_ytd["smkt"] = sku_ytd["smkt"].cat \
        .set_categories(smkt_order, ordered=True)
    
    sku_ytd.sort_values(by=["price", "smkt"], inplace=True)
    sku_ytd.drop_duplicates(subset="sku", ignore_index=True, inplace=True)
    
    sku_ytd = sku_ytd.merge(sku_stats, how="left", on="sku")
    
    col = [
        "sku", "smkt", "price", "promo_en", "promo_zh",
        "freq", "price_avg", "price_std", "price_q0", "price_q1", "price_q4",
    ]
    sku_alert = sku_ytd[col].assign(
        alert=(sku_ytd["price"] < sku_ytd["price_q1"]).astype("int8"),
        date=(datetime.today() - timedelta(days=1)).strftime("%Y%m%d"),
    )
    
    cache_watchdog = (sku_alert, "t_watchdog", "replace")
    cache_opw_data(cache_watchdog)
    
    print(f"\u21ba Yesterday, {len(sku_alert):,} items reached bid level.") \
        if VERBOSE else None
    
    return True


## MAIN FUNCTION
# -----

def set_up_backend() -> None:
    create_data_path()
    initialise_database()
    
    return None


def refresh_opw_data() -> bool:
    status = refresh_database()
    print("[DONE] \u2713 Refreshed database.") if status \
        else print("[ERROR] \u2717 Failed to refresh database.")
    
    status = ingest_opw_data()
    print("[DONE] \u2713 Ingested OPW data.") if status \
        else print("[ERROR] \u2717 Failed to ingest OPW data.")
    
    status = analyse_opw_data()
    print("[DONE] \u2713 Analysed OPW data.") if status \
        else print("[ERROR] \u2717 Failed to analyse OPW data.")
    
    return True


if __name__ == "__main__":
    set_up_backend()
    refresh_opw_data()
