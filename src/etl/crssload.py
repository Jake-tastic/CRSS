import mysql.connector



def db_connection():
    pword = str(input("Enter database password"))
    connection = mysql.connector.connect(user='root', password=pword, database='crss', port=3306)
    cursor = connection.cursor()
    return connection, cursor

def get_columns(cursor, table_name):
    col_names = f"SELECT GROUP_CONCAT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA= 'crss' AND TABLE_NAME='{table_name}';"
    cursor.execute(col_names)
    col_results = cursor.fetchall()
    return col_results

def db_insert(cursor, table_name, col_results,values):
    connection, cursor = db_connection()
    col_results = get_columns(cursor, table_name)
    insert_stmt = f"INSERT INTO '{table_name}'('{col_results}') VALUES ('{values}') "
    cursor.executemany()

def close_connection():
    connection, cursor = db_connection()
    cursor.close()
    print("Database cursor closed")
    connection.close()
    print("Database connection closed")



if __name__=="__main__":
    try:
        connection, cursor = db_connection()
        table_name = ("fact_person")
        col_results = get_columns(cursor, table_name)
        print(col_results)
        close_connection()
    except mysql.connector.Error as err:
         print(f"Database Error: {err}")
