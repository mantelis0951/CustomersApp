import streamlit as st
import snowflake.connector
import pandas as pd

# Streamlit application title
st.title("Snowflake Customers Table Viewer v0.0.1")
@st.cache_resource
# Snowflake connection parameters
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
