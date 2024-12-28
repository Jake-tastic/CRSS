import crssextract as ext
import crsstransform as trf
import crssload as ld
import crss_err_log as el
import pandas
import os
import mysql.connector

# file names, available years, and path
crss_files = ["ACCIDENT.CSV", "VEHICLE.CSV", "PARKWORK.CSV", "PERSON.CSV"]
available_years = [2016, 2017, 2018, 2019, 2020, 2021, 2022]
local_directory = "~/Projects/crss/data/sourcedata"

#CRSS configuration
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
        "end_file" : f"{local_directory}/VEHICLES.CSV",
        "columns" : {}
        },
    "person": {
        "start_file" : "PERSON.CSV",
        "db_table" : "fact_person",
        "end_file" : f"{local_directory}/PERSON.CSV",
        "columns" : {}
        }
    }





def crss_etl (record_years, files, directory):
    """
    Perform the ETL process for CRSS data:
    - Extract files from the NHTSA website.
    - Transform files locally into database-ready format.
    - Load files into the database.
    """
    print("Initiating CRSS ETL...")

    # Database connection 
    try:
        db_pass = input("Enter database password")
        db_con = mysql.connector.connect(user="root", password=db_pass, database="crss", port=3306)
        db_curs = db_con.cursor()
    except mysql.connector.Error as e:
        el.error_log("Connecting to database:", e)
        print("DB CONNECTION FAILED!\nCheck ERROR_LOG.txt")
        return
        
    try:
        # retrieving the column names from database
        print("Retrieving column names from database...")
        for value in crss.values():
            value["columns"] = ld.get_columns(value["db_table"], db_curs)
    except Exception as e:
        el.error_log(f"Retrieving column names from database:", e)
        print("COLUMN NAMES FAILED!\nCheck ERROR_LOG.txt")
        db_curs.close()
        db_con.close()
        return

    for r in record_years:
        print(f"Beginning {r} ETL process...")
        try:
            # Step 1: extraction
            print(f"Beginning Extraction...")
            try:
                ext.extraction(record_year=r, files=files, directory=directory)
                print("......Complete")
            except Exception as e:
                el.error_log(f"Extraction phase for year {r}, :", e)
                print("EXTRACTION FAILED!\nCheck ERROR_LOG.txt")
                break
            
            # Step 2: transformation
            try:
                print("...Beginning transformation...")
                trf.crss_transform(
                    crss["accident"]["columns"],
                    crss["vehicle"]["columns"],
                    crss["person"]["columns"]
                )
                print("......Complete")
            except Exception as e:
                el.error_log(f"Transformation phase, year {r}:", e)
                print("TRANSORMATION FAILED!\nCheck ERROR_LOG.txt")
                break

            # Step 3. load
            try:
                print("...Beginning Load Phase...")
                for key in crss.keys():
                    # load files into a dataframe
                    print(f"......{crss[key]['db_table']}...")
                    crss_df = pandas.read_csv(crss[key]["end_file"])

                    # get columns names from the dataframe and check for missing columns in files
                    # some columns have been added to the reports since it's inception
                    df_cols = crss[key]["columns"]

                    # align columns and handle mismatches
                    missing_cols = set(df_cols) - set(crss_df.columns)
                    if missing_cols:
                        print(f"......Adding missing columns {missing_cols}\nto {key}")
                    crss_df = crss_df.reindex(columns=df_cols, fill_value=pandas.NA)

                    # generate parameterized SQL statement
                    load_ready = ld.db_insert(crss[key]["db_table"], df_cols)
                    if load_ready is None:
                        el.error_log(f"SQL statement for {crss[key]['db_table']} could not be generated.")
                        print("SQL FAILED!\nCheck ERROR_LOG.txt")
                        continue

                    try:
                        # execute the query and commitchanges
                        db_curs.executemany(load_ready, crss_df.values.tolist()) 
                        db_con.commit()
                        print("...Load Complete!")
                    except mysql.connector.Error as my_e:
                        el.error_log(f"Loading {crss[key]['db_table']} failed!", my_e)

            except Exception as e:
                el.error_log(f"Loading Phase for year {r}, table {crss[key]['db_table']} failed to load", e)
                print("FAILED!\nCheck ERROR_LOG.txt")
                break
            
        except Exception as e:
            el.error_log("ETL process:", e)
            print("ETL FAILED!\nCheck ERROR_LOG.txt")
            break

        print(f"......{r} Complete")
    db_curs.close()
    db_con.close()
    print("Database connection closed.")
           
if __name__ == "__main__":
    try:
        crss_etl(record_years=available_years, files=crss_files, directory=local_directory)

    except Exception as e:
        print("Didn't work")
        print(f"Error: {e}")
