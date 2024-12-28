import crssextract as ext
import crsstransform as trf
import crssload as ld
import column_maps as cm
import pandas
from itertools import chain
import mysql.connector

# uses itertools to flatten the list so all file names are on the same level
crss_files = ["ACCIDENT.CSV", "VEHICLE.CSV", "PARKWORK.CSV", "PERSON.CSV"]

# current records avialable by NHTSA
available_years = [2016]
local_directory = "C:/Users/cojac/Projects/crss/data/sourcedata"


crss = {# start_file-files from NHTSA (using 4 out of 20 available files)
        # db_table-corrisponding table in database
        # end_file-end result ready for database (vehicle merges 2 files containing moving vehicles and parked/work vehicles)
        # columns will be retrieved from the database to ensure appropriate columns are loaded
    "accident" : {
        "start_file" : "ACCIDENT.CSV",
        "db_table" : "fact_accident",
        "end_file" : f"{local_directory}/ACCIDENT.CSV",
        "columns" : {}
        },
    "vehicle" : {
        "start_file" : ["VEHICLE.CSV", "PARKWORK.CSV"],
        "db_table" : "fact_vehicle",
        "end_file" : f"{local_directory}/VEHICLE.CSV",
        "columns" : {}
        },
    "person": {
        "start_file" : "PERSON.CSV",
        "db_table" : "fact_person",
        "end_file" : f"{local_directory}/PERSON.CSV",
        "columns" : {}
        }
    }
            
def col_test(crss_file):
    try:
        db_con = mysql.connector.connect(user="root", password="Corntruck85!", database="crss", port=3306)
        db_curs = db_con.cursor()

    except mysql.connector.Error as e:
        print("Failed to extablish connection with crss database")
        print(f"Error: {e}")
    
    try:
        # retrieving the column names for each database fact table
        crss[f"{crss_file}"]["columns"] = [col.strip().replace('"', '') for col in ld.get_columns(f"fact_{crss_file}", db_curs).split(",")]
        print("Columns obtained")

    except Exception as e:
        print("Failed to retrieve column names from the database")
        print(f"Error: {e}")
        db_curs.close()
        db_con.close()

    file_columns = crss[f"{crss_file}"]["columns"]
    try:
        crss_df = pandas.read_csv(f"{local_directory}/{crss_file}.csv")
        unmatched = crss_df.columns.difference(file_columns)
        print(unmatched)
        print(crss_df.columns)
    except Exception as e:
        print(e)
    
if __name__ == "__main__":
    try:
        df = pandas.read_csv(f"{local_directory}/PERSON.csv")
        print(df.columns)
    except Exception as e:
        print(e)
