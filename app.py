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

    tab1, tab2, tab3 = st.tabs(["Average User Transaction Amount for the Last 6 Months", "Product Category with the Highest Total Sales", "Monthly Revenue Growth for the Last 6 Months"])

    with tab1:
        st.header("Average User Transaction Amount for the Last 6 Months")
        query_1 = """  
            SELECT 
                cast(date_trunc('month', dt) AS date) AS month_date,
                TO_CHAR(EXTRACT(MONTH FROM dt), 'FM00')||'_'||TO_CHAR(dt, 'Month') AS "month", 
                SUM(total) AS monthly_total_transaction_amount,
                COUNT(DISTINCT customer_id) AS monthly_customers_with_transactions,
                ROUND(SUM(total) / COUNT(DISTINCT customer_id), 2) AS avg_customer_transaction_amount
            FROM 
                silver.fct_transactions
            WHERE 
                cast(date_trunc('month', dt) AS date) >= date_trunc('month', current_date) - INTERVAL '5 months'
            GROUP BY 1, 2
            ORDER BY month_date;  """

        # Create a button to fetch data
        if st.button("Fetch Average User Transaction for the Last 6 Months"):
            # Create a connection
            connection = create_connection()

            if connection:
                # Fetch data from the database
                df_1 = fetch_data(query_1, connection)
                if df_1 is not None:
                    st.write("Data from PostgreSQL:")
                    st.dataframe(df_1)

                    st.bar_chart(df_1.set_index('month')['avg_customer_transaction_amount'])

                # Close the connection
                connection.close()

    with tab2:
        st.header("Product Category with the Highest Total Sales")
        query_2 = """  
            WITH cte_products AS 
            (
                SELECT
                    subscription_id,
                    split_part(product, ' ', 1) AS product_category 
                FROM
                    silver.dim_products 
                GROUP BY
                    1,
                    2 
                ORDER BY
                    2 
            )
            ,
            cte_product_category AS 
            (
                SELECT
                    subscription_id,
                    STRING_AGG(product_category, ' | ') AS product_category 
                FROM
                    cte_products 
                GROUP BY
                    1 
            )
            SELECT
                p.product_category,
                SUM(t.total) AS total_sales 
            FROM
                silver.fct_transactions t 
                LEFT JOIN
                    cte_product_category p 
                    ON t.subscription_id = p.subscription_id 
            WHERE p.product_category is NOT NULL
            GROUP BY
                1 
            ORDER BY
                2 DESC;  """

        # Create a button to fetch data
        if st.button("Fetch Product Category with the Highest Total Sales"):
            # Create a connection
            connection = create_connection()

            if connection:
                # Fetch data from the database
                df_2 = fetch_data(query_2, connection)
                if df_2 is not None:
                    st.write("Data from PostgreSQL:")
                    st.dataframe(df_2)

                    st.bar_chart(df_2.set_index('product_category')['total_sales'])

                # Close the connection
                connection.close()

    with tab3:
        st.header("Monthly Revenue Growth for the Last 6 Months")
        query_3 = """  
            WITH cte_revenue_growth AS (
            SELECT 
                cast(date_trunc('month', dt) AS date) AS month_date,
                TO_CHAR(EXTRACT(MONTH FROM dt), 'FM00')||'_'||TO_CHAR(dt, 'Month') AS "month",
                SUM(total) AS monthly_revenue,
                LAG(SUM(total)) OVER(ORDER BY cast(date_trunc('month', dt) AS date)) AS previous_monthly_revenue,
                (SUM(total)) - COALESCE(LAG(SUM(total)) OVER(ORDER BY cast(date_trunc('month', dt) AS date)), 0) AS revenue_growth
            FROM 
                silver.fct_transactions 
            GROUP BY 1,2 
            )
            SELECT 
                * 
            FROM 
                cte_revenue_growth 
            WHERE 
                month_date >= date_trunc('month', current_date) - INTERVAL '5 months';
            """

        # Create a button to fetch data
        if st.button("Fetch Monthly Revenue Growth for the Last 6 Months"):
            # Create a connection
            connection = create_connection()

            if connection:
                # Fetch data from the database
                df_3 = fetch_data(query_3, connection)
                if df_3 is not None:
                    st.write("Data from PostgreSQL:")
                    st.dataframe(df_3)

                    st.bar_chart(df_3.set_index('month')['revenue_growth'])

                # Close the connection
                connection.close()

# Entry point for the script
if __name__ == '__main__':
    main()