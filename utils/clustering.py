from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Dict
import json

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "life_db")
CHATS_COLLECTION = os.getenv("CHATS_COLLECTION", "chats")
MEDIA_COLLECTION = os.getenv("MEDIA_COLLECTION", "media")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "pipeline_outputs")


def mongo_connect(uri: str = MONGO_URI, db_name: str = DB_NAME):
    client = MongoClient(uri)
    db = client[db_name]
    return client, db


def load_collection_to_df(db, collection_name: str, projection: Dict[str, int] = None):
    projection = projection or {}
    coll = db[collection_name]
    docs = list(coll.find({}, projection))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    # normalize datetime field if present
    if "Datetime" in df.columns:
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    return df


def save_df_parquet(df: pd.DataFrame, name: str):
    path = os.path.join(OUTPUT_DIR, f"{name}.parquet")
    df.to_parquet(path, index=False)
    print(f"[saved] {path}")


def save_json(obj, name: str):
    path = os.path.join(OUTPUT_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, default=str, ensure_ascii=False, indent=2)
    print(f"[saved] {path}")


def ensure_dt(df):
    if "Datetime" not in df.columns:
        raise ValueError("No 'Datetime' column found.")
    df = df.copy()
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    return df
