import streamlit as st
import snowflake.connector
import pandas as pd

# Streamlit application title
st.title("Snowflake Customers Table Viewer v0.0.1")

# Snowflake connection parameters
def create_snowflake_connection():
    conn = snowflake.connector.connect(
        user='YOUR_SNOWFLAKE_USER',  # Replace with your Snowflake user
        password='YOUR_SNOWFLAKE_PASSWORD',  # Replace with your Snowflake password
        account='YOUR_SNOWFLAKE_ACCOUNT',  # Replace with your Snowflake account identifier
        warehouse='YOUR_SNOWFLAKE_WAREHOUSE',  # Replace with your Snowflake warehouse
        database='YOUR_SNOWFLAKE_DATABASE',  # Replace with your Snowflake database
        schema='YOUR_SNOWFLAKE_SCHEMA'  # Replace with your Snowflake schema
    )
    return conn

# Fetch and display the Customers table
def fetch_customers_data(conn):
    query = "SELECT * FROM Customers;"  # SQL query to fetch all data from the Customers table
    try:
        # Execute the query and fetch the results into a pandas DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        cursor.close()
        
        # Display the DataFrame in the Streamlit app
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error fetching data from Snowflake: {str(e)}")

# Main app function
def main():
    # Display a header
    st.header("Snowflake Customers Table")

    # Create a button to fetch data
    if st.button("Fetch Customers Data"):
        try:
            # Establish connection to Snowflake
            conn = create_snowflake_connection()
            fetch_customers_data(conn)
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {str(e)}")
        finally:
            # Ensure the connection is closed
            conn.close()

# Run the app
if __name__ == "__main__":
    main()
