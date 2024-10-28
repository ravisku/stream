import streamlit as st
import psycopg2
import pandas as pd

# Function to create a database connection
def create_connection(db_name, user, password, host, port):
    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
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

    # Database connection details
    db_name = st.text_input("Database Name", value="postgres")
    user = st.text_input("User", value="postgres")
    password = st.text_input("Password", value="Streamdeassignment", type="password")
    host = st.text_input("Host", value="stream.cd2o2ocim4fu.us-west-2.rds.amazonaws.com")
    port = st.text_input("Port", value="5432")

    # Query to execute
    query = st.text_area("SQL Query", value=""" SELECT 
    EXTRACT(MONTH FROM dt) AS month_id, 
    TO_CHAR(dt, 'Month') AS "month", 
    SUM(total) AS monthly_total_transaction_amount,
    COUNT(DISTINCT customer_id) AS monthly_customers_with_transactions,
    ROUND(SUM(total)/COUNT(DISTINCT customer_id), 2) as avg_customer_transaction_amount
FROM
    silver.fct_transactions 
GROUP BY 1,2 """)

    # Create a button to fetch data
    if st.button("Fetch Data"):
        # Create a connection
        connection = create_connection(db_name, user, password, host, port)

        if connection:
            # Fetch data from the database
            df = fetch_data(query, connection)
            if df is not None:
                st.write("Data from PostgreSQL:")
                st.dataframe(df)

                # Example visualization (showing a bar chart if there's a numeric column)
                numeric_columns = df.select_dtypes(include=['float64', 'int']).columns.tolist()
                if numeric_columns:
                    column_to_plot = st.selectbox("Select column to plot", numeric_columns)
                    st.bar_chart(df[column_to_plot])

            # Close the connection
            connection.close()

# Entry point for the script
if __name__ == '__main__':
    main()