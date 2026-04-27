'''
If you are running this code first time, and you don't have streamlit installed, then follow this instruction:
1. open a terminal
2. enter this command
    pip install streamlit
'''
import os
import oracledb
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
DB_DSN  = os.getenv("DB_DSN") or f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE')}"

if not DB_USER or not DB_PASS or not DB_DSN:
    raise SystemExit("Missing DB credentials in .env file")

LIB_DIR    = r"C:\Users\zachd\OneDrive\Documents\oracle_database\instantclient_11_2" 

@st.cache_resource
def init_db():
    if LIB_DIR:
        try:
            oracledb.init_oracle_client(lib_dir=LIB_DIR)
        except Exception as e:
            st.error(f"Error initializing Oracle Client: {e}")

init_db()

def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

def run_query(sql, params=None):
    if params is None:
        params = []
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return cols, rows


def show_results(cols, rows):
    if rows:
        st.dataframe([dict(zip(cols, row)) for row in rows], use_container_width=True)
    else:
        st.write("No records found.")


#App stuff

st.title("Fraud Analysis System")

menu = [
    "Fraud rate by date range",
    "Fraud cases by merchant",
    "Suspicious location transactions",
    "Fraud rate by city/state",
    "Transactions by cardholder",
]
choice = st.sidebar.selectbox("Choose a feature", menu)

st.sidebar.divider()
if st.sidebar.button("Exit", type="primary"):
    st.success("Application stopped. You can now close this tab.")
    st.stop()

# Feature 1:
if choice == "Fraud rate by date range":
    st.write("### Fraud Rate by Date Range")

    start = st.date_input("Start date")
    end   = st.date_input("End date")

    if st.button("Run", key="run_feature1"):
        if start > end:
            st.error("Start date must be before or equal to end date.")
        else:
            sql = """
                SELECT
                    COUNT(*) AS total_transactions,
                    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
                    ROUND(
                        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4
                    ) AS fraud_rate,
                    overall.overall_fraud_rate
                FROM transactions,
                (
                    SELECT ROUND(
                        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4
                    ) AS overall_fraud_rate
                    FROM transactions
                ) overall
            WHERE DATE '1970-01-01' + (unix_time / 86400)
                BETWEEN TO_DATE(:1, 'YYYY-MM-DD') AND TO_DATE(:2, 'YYYY-MM-DD')
            """
            cols, rows = run_query(sql, [str(start), str(end)])
            show_results(cols, rows)


# Feature 2:
elif choice == "Fraud cases by merchant":
    st.write("### Fraud Cases by Merchant")
    st.caption("Shows all merchants with at least one fraudulent transaction, ordered by fraud count.")

    if st.button("Run", key="run_feature2"):
        sql = """
        SELECT
            m.merchant_id,
            m.merchant_name,
            cat.category_name,
            COUNT(*) AS fraud_count,
            ROUND(SUM(t.amount), 2) AS total_fraud_amount
        FROM transactions t
        JOIN merchant m   ON t.merchant_id = m.merchant_id
        JOIN category cat ON t.category_id = cat.category_id
        WHERE t.is_fraud = 1
        GROUP BY m.merchant_id, m.merchant_name, cat.category_name
        ORDER BY fraud_count DESC
        """
        cols, rows = run_query(sql)
        show_results(cols, rows)


# Feature 3:
elif choice == "Suspicious location transactions":
    st.write("### Suspicious Location Transactions")
    st.caption("Finds transactions where the transaction location is far from the cardholder's home address.")

    threshold = st.number_input(
        "Distance threshold in degrees (e.g. 2.0)",
        min_value=0.0,
        value=2.0,
        step=0.5
    )

    if st.button("Run", key="run_feature3"):
        sql = """
        SELECT
            c.cardholder_id,
            c.first_name,
            c.last_name,
            t.amount,
            DATE '1970-01-01' + (t.unix_time / 86400) AS transaction_date,
            t.transaction_lat,
            t.transaction_long,
            cl.latitude  AS home_lat,
            cl.longitude AS home_long,
            ABS(t.transaction_lat  - cl.latitude)  AS lat_diff,
            ABS(t.transaction_long - cl.longitude) AS long_diff,
            cat.category_name,
            t.is_fraud
        FROM transactions t
        JOIN cardholder c           ON t.cardholder_id = c.cardholder_id
        JOIN cardholder_location cl ON c.cardholder_id = cl.cardholder_id
        JOIN category cat           ON t.category_id   = cat.category_id
        WHERE ABS(t.transaction_lat  - cl.latitude)  > :1
           OR ABS(t.transaction_long - cl.longitude) > :1
        ORDER BY lat_diff DESC, long_diff DESC
        """
        cols, rows = run_query(sql, [threshold])
        show_results(cols, rows)


# Feature 4:
elif choice == "Fraud rate by city/state":
    st.write("### Fraud Rate by City or State")
    st.caption("Leave a field blank to search across all values for that field.")

    state = st.text_input("State code (e.g. FL)").strip().upper() or None
    city  = st.text_input("City name (e.g. Orlando)").strip() or None

    if st.button("Run", key="run_feature4"):
        sql = """
        SELECT
            s.state_code,
            s.state_name,
            ci.city,
            ci.city_pop,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(
                SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4
            ) AS fraud_rate
        FROM transactions t
        JOIN cardholder c           ON t.cardholder_id = c.cardholder_id
        JOIN cardholder_location cl ON c.cardholder_id = cl.cardholder_id
        JOIN cities ci              ON cl.city_id       = ci.city_id
        JOIN states s               ON ci.state_code    = s.state_code
        WHERE (:1 IS NULL OR s.state_code = :2)
        AND (:3 IS NULL OR LOWER(ci.city) = LOWER(:4))
        GROUP BY s.state_code, s.state_name, ci.city, ci.city_pop
        ORDER BY fraud_rate DESC
        """
        cols, rows = run_query(sql, [state, state, city, city])
        show_results(cols, rows)


#Feature 5:
else:
    st.write("### Transactions by Cardholder")

    first = st.text_input("First name")
    last  = st.text_input("Last name")

    if st.button("Run", key="run_feature5"):
        if not first or not last:
            st.error("Enter both a first and last name.")
        else:
            sql = """
            SELECT
                t.transaction_id,
                DATE '1970-01-01' + (t.unix_time / 86400) AS transaction_date,
                t.transaction_lat,
                t.transaction_long,
                t.amount,
                cat.category_name,
                t.is_fraud
            FROM transactions t
            JOIN cardholder c ON t.cardholder_id = c.cardholder_id
            JOIN category cat ON t.category_id   = cat.category_id
            WHERE LOWER(c.first_name) = LOWER(:1)
              AND LOWER(c.last_name)  = LOWER(:2)
            ORDER BY t.is_fraud DESC, transaction_date DESC
            """
            cols, rows = run_query(sql, [first, last])
            show_results(cols, rows)

# run using: streamlit run streamlit_app.py
