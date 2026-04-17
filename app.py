import oracledb
import os
from dotenv import load_dotenv

# ENV
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
DB_DSN  = os.getenv("DB_DSN") or f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE')}"

if not DB_USER or not DB_PASS or not DB_DSN:
    raise SystemExit("Missing DB credentials in .env file")

conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

# Helpr
def run_query(sql, params=None):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or {})
        rows = cursor.fetchall()
        cols = [col[0] for col in cursor.description]

        print("\n--- RESULTS ---")
        print(" | ".join(cols))
        print("-" * 70)

        if not rows:
            print("No results found.\n")
            return

        for row in rows:
            print(" | ".join(str(x) for x in row))
        print()

    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()

# Feature 1
def feature1():
    print("\n[Fraud Rate by Date Range]")
    start = input("Start date (YYYY-MM-DD): ")
    end   = input("End date (YYYY-MM-DD): ")

    sql = """
    SELECT 
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        ROUND(
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4
        ) AS fraud_rate,
        ROUND(
            (SELECT SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) FROM transactions) /
            (SELECT COUNT(*) FROM transactions), 4
        ) AS overall_fraud_rate
    FROM transactions
    WHERE DATE '1970-01-01' + (unix_time / 86400)
          BETWEEN :start_date AND :end_date
    """

    run_query(sql, {"start_date": start, "end_date": end})

# Feature 2
def feature2():
    print("\n[Fraud Cases by Merchant]")

    sql = """
    SELECT 
        m.merchant_id,
        m.merchant_name,
        COUNT(*) AS fraud_count,
        SUM(t.amount) AS total_fraud_amount
    FROM transactions t
    JOIN merchant m ON t.merchant_id = m.merchant_id
    WHERE t.is_fraud = 1
    GROUP BY m.merchant_id, m.merchant_name
    HAVING COUNT(*) > 0
    ORDER BY fraud_count DESC
    """

    run_query(sql)

# Feature 3
def feature3():
    print("\n[Suspicious Location Transactions]")
    threshold = float(input("Enter distance threshold (e.g., 2): "))

    sql = """
    SELECT 
        c.cardholder_id,
        c.first_name,
        c.last_name,
        c.dob,
        t.amount,
        DATE '1970-01-01' + (t.unix_time / 86400) AS transaction_time,
        t.transaction_lat,
        t.transaction_long,
        cl.latitude AS home_lat,
        cl.longitude AS home_long,
        cat.category_name
    FROM transactions t
    JOIN cardholder c ON t.cardholder_id = c.cardholder_id
    JOIN cardholder_location cl ON c.cardholder_id = cl.cardholder_id
    JOIN category cat ON t.category_id = cat.category_id
    WHERE ABS(t.transaction_lat - cl.latitude) > :threshold
       OR ABS(t.transaction_long - cl.longitude) > :threshold
    """
    run_query(sql, {"threshold": threshold})

# feature 4
def feature4():
    print("\n[Fraud Rate by City or State]")
    state = input("Enter state code (or leave blank): ") or None
    city  = input("Enter city (or leave blank): ") or None

    sql = """
    SELECT 
        s.state_code,
        ci.city,
        ci.city_pop,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        ROUND(
            SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
            4
        ) AS fraud_rate
    FROM transactions t
    JOIN cardholder c ON t.cardholder_id = c.cardholder_id
    JOIN cardholder_location cl ON c.cardholder_id = cl.cardholder_id
    JOIN cities ci ON cl.city_id = ci.city_id
    JOIN states s ON ci.state_code = s.state_code
    WHERE (:state_code IS NULL OR s.state_code = :state_code)
      AND (:city IS NULL OR LOWER(ci.city) = LOWER(:city))
    GROUP BY s.state_code, ci.city, ci.city_pop
    ORDER BY fraud_rate DESC
    """

    run_query(sql, {"state_code": state, "city": city})

# Feature 5
def feature5():
    print("\n[Transactions by Cardholder]")
    first = input("First name: ")
    last  = input("Last name: ")

    sql = """
    SELECT 
        t.transaction_id,
        DATE '1970-01-01' + (t.unix_time / 86400) AS transaction_time,
        t.transaction_lat,
        t.transaction_long,
        t.amount,
        cat.category_name,
        t.is_fraud
    FROM transactions t
    JOIN cardholder c ON t.cardholder_id = c.cardholder_id
    JOIN category cat ON t.category_id = cat.category_id
    WHERE LOWER(c.first_name) = LOWER(:first_name)
      AND LOWER(c.last_name) = LOWER(:last_name)
    ORDER BY t.is_fraud DESC, transaction_time DESC
    """

    run_query(sql, {"first_name": first, "last_name": last})

# Menu
def main():
    while True:
        print("\n========== FRAUD ANALYSIS SYSTEM ==========")
        print("1. Fraud rate by date range")
        print("2. Fraud cases by merchant")
        print("3. Suspicious location transactions")
        print("4. Fraud rate by city/state")
        print("5. Transactions by cardholder")
        print("0. Exit")

        choice = input("Choose an option: ")

        if choice == "1":
            feature1()
        elif choice == "2":
            feature2()
        elif choice == "3":
            feature3()
        elif choice == "4":
            feature4()
        elif choice == "5":
            feature5()
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Try again.")

    conn.close()

if __name__ == "__main__":
    main()
