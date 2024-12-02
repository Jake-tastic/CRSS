#import crsstransform as ctran
import dbconnect as dbc
import pandas as pd
import mysql.connector
#def main():
   # print(main)









if __name__ == "__main__":
    try:
#using dbconnect.py to connect to MySQL
        connection, cursor = dbc.db_connection() 
        print("connected to db")

#function in dbconnect.py to retrieve column names
        cols = dbc.get_columns(cursor, table_name='fact_person') 
        print("got column names")

#cols is returned as a tubple, extrtact the first item(the actual list of col names) from the tuple
        col_string = cols[0][0]
#col_string list col names as a single string, split into individual items
        col_split = col_string.split(',')

#converting to list with quotes around each col name to be used in dataframe using pandas

        cols_list = [f"'{col}'" for col in col_split]
        quoted_cols = ', '.join(cols_list)
        print(type(quoted_cols))


#col_list determines columns required from CSV
        values = pd.read_csv("~/Projects/crss/sourcedata/2022/person.csv", usecols = quoted_cols)

#insert the the dataframe into the corrisponding database tabe
        dbc.dbinsert(table_name = 'fact_person', values = values)

#close connection to MySQL
        dbc.close_connection()

#incase there are any errors with MySQL connection
    except mysql.connector.Error as err:
         print("no dice!")
         print(f"Database Error: {err}")