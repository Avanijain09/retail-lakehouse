import sys

print("=" * 50)
print("RETAIL LAKEHOUSE — ENVIRONMENT VERIFICATION")
print("=" * 50)

checks = []

# Python version
py_ok = sys.version_info >= (3, 10)
checks.append(("Python 3.10+", py_ok, sys.version))

# PySpark
try:
    import pyspark

    checks.append(("PySpark", True, pyspark.__version__))
except ImportError as e:
    checks.append(("PySpark", False, str(e)))

# Pandas
try:
    import pandas as pd

    checks.append(("Pandas", True, pd.__version__))
except ImportError as e:
    checks.append(("Pandas", False, str(e)))

# SQLAlchemy
try:
    import sqlalchemy

    checks.append(("SQLAlchemy", True, sqlalchemy.__version__))
except ImportError as e:
    checks.append(("SQLAlchemy", False, str(e)))

# psycopg2
try:
    import psycopg2

    checks.append(("psycopg2", True, psycopg2.__version__))
except ImportError as e:
    checks.append(("psycopg2", False, str(e)))

# dotenv
try:
    from dotenv import load_dotenv

    checks.append(("python-dotenv", True, "installed"))
except ImportError as e:
    checks.append(("python-dotenv", False, str(e)))

# PostgreSQL connection
try:
    from dotenv import load_dotenv
    import os

    load_dotenv()

    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "retail_warehouse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )

    conn.close()

    checks.append(("PostgreSQL connection", True, "Connected!"))

except Exception as e:
    checks.append(("PostgreSQL connection", False, str(e)))

print()

for name, status, detail in checks:
    icon = "OK" if status else "FAIL"
    print(f"  [{icon}]  {name:<28} {detail}")

passed = sum(1 for _, s, _ in checks if s)
total = len(checks)

print()
print(f"Result: {passed}/{total} checks passed")

if passed == total:
    print("Environment is fully ready!")
else:
    print("Fix the FAIL items above before proceeding.")

print("=" * 50)
