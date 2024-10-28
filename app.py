import streamlit as st
import psycopg2
import pandas as pd

# Function to create a database connection
def create_connection():
    try:
        connection = psycopg2.connect(
            dbname=st.secrets["database"]["dbname"],
            user=st.secrets["database"]["user"],
            password=st.secrets["database"]["password"],
            host=st.secrets["database"]["host"],
            port=st.secrets["database"]["port"]
        )
        return connection
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Function to fetch data from the PostgreSQL database
def fetch_data(query, connection):
    try:
        return pd.read_sql_query(query, connection)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

def main():
    # Streamlit app setup
    st.title("Getstream Assignment Dashboard")

    # Query to execute
    query = st.text_area("SQL Query", value="""  
    SELECT 
        TO_CHAR(dt, 'Month') AS "month", 
        ROUND(SUM(total)/COUNT(DISTINCT customer_id), 2) as avg_customer_transaction_amount
    FROM
        silver.fct_transactions 
    GROUP BY 1
    ORDER BY 
    TO_DATE(TO_CHAR(dt, 'Month'), 'Month') DESC LIMIT 6;  """)

    # Create a button to fetch data
    if st.button("Fetch Data"):
        # Create a connection
        connection = create_connection()

        if connection:
            # Fetch data from the database
            df = fetch_data(query, connection)
            if df is not None:
                st.write("Data from PostgreSQL:")
                st.dataframe(df)

                st.bar_chart(df.set_index('month')['avg_customer_transaction_amount'])

            # Close the connection
            connection.close()

# Entry point for the script
if __name__ == '__main__':
    main()