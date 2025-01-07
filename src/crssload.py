import mysql.connector
from mysql.connector import Error
import crss_err_log as el

def db_connection(details):
    """
    Uses the connection details as an parameter to connect to the database
    """
    try:
        connection = mysql.connector.connect(details)
        return connection
    except Error as e:
        print("Database not connected!")
        el.error_log(3, "Database connection", e)
        return None


def get_columns(name, cursor):
    """
    Connect to MySQL and retrieve column names with provided query and input for table
    """ 

    #SQL query to retrieve column names
    col_names_query = f"""
        SELECT GROUP_CONCAT(COLUMN_NAME) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'crss' 
        AND TABLE_NAME = '{name}';
        """
    cursor.execute(col_names_query)
    col_results = cursor.fetchall()

    # Extracts column names from the results
    # extrtact the first item(the actual list of col names)  
    col_string = col_results[0][0]

    # Split the column names into a list
    col_split = col_string.split(',')
    return [col.strip() for col in col_split]



def db_insert(table_name, columns):
    """
    Insert values into specified table
    table_name- name of the database table to target
    columns- columns names for load statement
    """
    try:
        # ensure columns is a list
        if not isinstance(columns, list):
            el.error_log(3, "db_insert()", "Columns must be a list of column names.")
            return None
    
        # convert list to a comma-separated string and generate placeholders for parameterization
        column_names = ", ".join(columns)
        placeholders = ", ".join(['%s'] * len(columns))

        # Use parameterized query to prevent SQL injection
        insert_stmt = f"INSERT INTO {table_name}({column_names}) VALUES ({placeholders});"
        return insert_stmt
    
    except Exception as e:
        el.error_log(3, f"Load phase error generating INSERT statement for table {table_name}.", f"{e}\n{Error}")
        return None