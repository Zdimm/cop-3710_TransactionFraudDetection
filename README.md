# cop-3710_TransactionFraudDetection
High-volume financial transaction  fraud detection analysis. Includes anomaly ranking via window functions, transaction audit logs via triggers, and query performance tuning. 

This projects falls within the banking application domain and uses the following data set :
[https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud/data ](https://www.kaggle.com/datasets/kartik2112/fraud-detection/data)

How to use:
Step 1: Use the “create_db.sql” to create the database
Step 2: Read dataload.py and app.py and add your database credentials where directed by commits within
Step 3: Use the dataload.py to populate the database with toy data
Step 4: run the app.py using the command below
    Python -m streamlit run app.py


