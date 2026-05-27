"""
All configuration from .env file.
NEVER hardcode credentials or paths in job scripts.
"""

from dotenv import load_dotenv
import os

load_dotenv()

# ── Derive project root from this file's location ──────
# config.py is at: retail-lakehouse/pyspark_jobs/utils/config.py
# PROJECT_ROOT = retail-lakehouse/
_THIS_FILE = os.path.abspath(__file__)
_UTILS_DIR = os.path.dirname(_THIS_FILE)
_JOBS_DIR = os.path.dirname(_UTILS_DIR)
PROJECT_ROOT = os.path.dirname(_JOBS_DIR)

# ── Data layer paths ────────────────────────────────────
RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "gold")
QUARANTINE_PATH = os.path.join(SILVER_PATH, "_quarantine")
WATERMARK_PATH = os.path.join(SILVER_PATH, "meta", "watermarks")

# ── PostgreSQL ──────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}" f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Table lists ─────────────────────────────────────────
# Transactional tables — daily file expected
TRANSACTIONAL_TABLES = ["orders", "order_items"]

# Dimension tables — less frequent updates
DIMENSION_TABLES = ["customers", "products", "stores"]

# All tables
TABLES = TRANSACTIONAL_TABLES + DIMENSION_TABLES

# Gold tables
GOLD_TABLES = [
    "daily_sales_kpi",
    "store_performance",
    "category_margin",
    "promotion_effectiveness",
]
