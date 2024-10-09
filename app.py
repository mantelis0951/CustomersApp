import streamlit as st
import snowflake.connector
import pandas as pd

# Title of the Streamlit app
st.title("Customers Purchasing Behaviour")


# Snowflake connection details
# You can replace the placeholders with your Snowflake credentials
@st.cache_resource
def create_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            role=st.secrets["snowflake"]["role"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]
        )
        return conn
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Programming error: {e}")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Database error: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}") 

# Function to fetch data from the Snowflake customers table
def fetch_customers_data():
    conn = create_snowflake_connection()
    query = "SELECT * FROM customers"
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    
    # Fetch column names
    columns = [desc[0] for desc in cur.description]
    
    # Close cursor and connection
    cur.close()
    conn.close()
    
    # Create a DataFrame
    return pd.DataFrame(data, columns=columns)

# Button to load the customers data
if st.button("Load Customers Data :)"):
    st.text("Fetching data from Snowflake...")
    
    # Fetch customers data from Snowflake
    customers_df = fetch_customers_data()
    
    if customers_df.empty:
        st.text("No data found in the customers table.")
    else:
        st.dataframe(customers_df)

