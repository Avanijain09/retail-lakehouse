"""
Centralized project configuration.
Never hardcode credentials or paths in scripts.
"""

from dotenv import load_dotenv
import os

load_dotenv()

# ── PROJECT ROOT ─────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# ── DATA PATHS ───────────────────────────────────────

RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")

BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver")

GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "gold")

WATERMARK_PATH = os.path.join(SILVER_PATH, "meta", "watermarks")

QUARANTINE_PATH = os.path.join(SILVER_PATH, "_quarantine")

# ── POSTGRESQL ───────────────────────────────────────

DB_HOST = os.getenv("DB_HOST", "localhost")

DB_PORT = os.getenv("DB_PORT", "5432")

DB_NAME = os.getenv("DB_NAME", "retail_warehouse")

DB_USER = os.getenv("DB_USER", "postgres")

DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}" f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── PIPELINE TABLES ──────────────────────────────────

TABLES = ["orders", "order_items", "customers", "products", "stores"]

GOLD_TABLES = ["daily_sales_kpi", "store_performance", "category_margin"]
