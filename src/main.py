import crssextract as ext
import crsstransform as trf
import crssload as ld
import crss_logging as el
import pandas, os, shutil, sys, mysql.connector

# file names, available years
crss_files = ["ACCIDENT.CSV", 
              "VEHICLE.CSV", 
              "PARKWORK.CSV", 
              "PERSON.CSV"]
available_years = [#2016, 
                   #2017, 
                   #2018, 
                   2019, 
                   2020, 
                   2021, 
                   2022]

# define directories to be used
directory = "~/Projects/crss/data/"
source_directory = os.path.expanduser(f"{directory}sourcedata/") # for the raw files
ready_directory = os.path.expanduser(f"{directory}readydata/") # for cleaned files 
archive_directory = os.path.expanduser(f"{directory}archivedata/") # archived files for future use

#CRSS configuration
crss = {# start_file-files from NHTSA (using 4 out of 20 available files)
        # db_table-corrisponding table in database
        # end_file-end result ready for database (vehicle merges 2 files containing moving vehicles and parked/work vehicles)
        # columns will be retrieved from the database to ensure appropriate columns are loaded
    "accident" : {
        "start_file" : "ACCIDENT.CSV",
        "db_table" : "fact_accident",
        "end_file" : f"{source_directory}ACCIDENT.CSV",
        "columns" : {},
        "ready_dir" : f"{ready_directory}ACCIDENT.CSV"
        },
    "vehicle" : {
        "start_file" : ["VEHICLE.CSV", "PARKWORK.CSV"],
        "db_table" : "fact_vehicle",
        "end_file" : f"{source_directory}VEHICLE.CSV",
        "columns" : {},
        "ready_dir" : f"{ready_directory}VEHICLE.CSV"
        },
    "person" : {
        "start_file" : "PERSON.CSV",
        "db_table" : "fact_person",
        "end_file" : f"{source_directory}PERSON.CSV",
        "columns" : {},
        "ready_dir" : f"{ready_directory}PERSON.CSV"
        }
    }

def clear_directory(directory_path):
    """
    Clears the contents of a directory without deleting the directory itself.
    Args:
        directory_path (str): The path of the directory to clear.
    """
    try:
        if not os.path.exists(directory_path):
            print(f"Directory does not exist: {directory_path}")
            return
        # Iterate through directory contents
        for item_name in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item_name)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # Remove files or symbolic links
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # Remove directories recursively
        el.logging(1, "Contents of directory cleared", {directory_path})
    except:
        el.logging(3, f"Failed to clear contents of directory: {directory_path}", Exception)
        sys.exit(1)

def crss_etl (record_years, files, src, rdy, arch):
    """
    Perform the ETL process for CRSS data:
    - Extract files from the NHTSA website.
    - Transform files locally into database-ready format.
    - Load files into the database.
    """
    print("Initiating CRSS ETL...")

    # ensure directories are cleaned out and ready
    clear_directory(src)
    clear_directory(rdy)

    # Database connection 
    try:
        db_pass = input("Enter database password")
        db_con = mysql.connector.connect(user="root", password=db_pass, database="crss", port=3306)
        db_curs = db_con.cursor()
    except:
        el.logging(3, "Connecting to database:", mysql.connector.Error)
        print("DB CONNECTION FAILED!\nCheck ERROR_LOG.txt")
        sys.exit(1)
        
    try:
        # retrieving the column names from database
        print("Retrieving column names from database...")
        for value in crss.values():
            value["columns"] = ld.get_columns(value["db_table"], db_curs)
    except:
        el.logging(3, f"Retrieving column names from database:", Exception)
        print("COLUMN NAMES FAILED!\nCheck ERROR_LOG.txt")
        db_curs.close()
        db_con.close()
        sys.exit(1)

    for r in record_years:
        print(f"Beginning {r} ETL process...")
        try:
            # Step 1: extraction
            print(f"Beginning Extraction...")
            try:
                ext.extraction(record_year=r, files=files, directory=src)
                print(".........Complete")
            except:
                el.logging(3, f"Extraction phase for year {r}", Exception)
                print("EXTRACTION FAILED!\nCheck ERROR_LOG.txt")
                sys.exit(1)
            # Step 2: transformation
            try:
                print("...Beginning transformation...")
                trf.crss_transform(
                    crss["accident"]["columns"],
                    crss["vehicle"]["columns"],
                    crss["person"]["columns"],
                    record_year = r,
                    src_dir = src,
                    rdy_dir=rdy
                )
                print("......Transformations Complete")

            except:
                el.logging(3, f"Transformation phase, year {r}: ", Exception)
                print("TRANSFORMATION FAILED!\nCheck ERROR_LOG.txt")
                sys.exit(1)

            # Step 3. load
            try:
                print("...Beginning Load Phase...")
                for key in crss.keys():
                    # load files into a dataframe
                    print(f"......{crss[key]['db_table']}")
                    crss_df = pandas.read_csv(crss[key]["ready_dir"], low_memory=False)

                    # align columns with database schema
                    df_cols = crss[key]["columns"]
                    crss_df = crss_df.reindex(columns=df_cols, fill_value=pandas.NA)

                    # ensure column datatypes match database schema
                    for col in crss_df.columns:
                        crss_df[col] = pandas.to_numeric(crss_df[col], errors='coerce', downcast='integer').fillna(0).astype(int)

                    # generate parameterized SQL statement
                    load_ready = ld.db_insert(crss[key]["db_table"], df_cols)
                    if load_ready is None:
                        el.logging(3, f"SQL statement for {crss[key]['db_table']} could not be generated.")
                        print("SQL FAILED!\nCheck ERROR_LOG.txt")
                        db_curs.close()
                        db_con.close()
                        sys.exit(1)

                    # execute batch inserts
                    data = crss_df.values.tolist()
                    batch_size = 500
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        try:
                            db_curs.executemany(load_ready, batch) 
                            db_con.commit()
                            
                        except:
                            el.logging(3, f"Loading {crss[key]['db_table']} failed!", mysql.connector.Error)
                            db_con.rollback()

                            # retry logic
                            retries = 3
                            for attempt in range(retries):
                                try:
                                    el.logging(2, "Reattempting database insert", f"Attempt {attempt + 1} of {retries}")
                                    db_con.reconnect(attempt=3, delay=5)
                                    db_curs = db_con.cursor()
                                    db_curs.executemany(load_ready, batch)
                                    db_con.commit()
                                    el.logging(1, "Successsful insertion", "")
                                except:
                                    el.logging(3, f"Retry attempt {attempt + 1}", f"Failed!")
                                    db_con.rollback()
                            else:
                                el.logging(3, f"All retries failed for batch in {crss[key]['db_table']}", mysql.connector.Error)

                        print(".........Load Complete!")
            except:
                el.logging(3, f"Loading Phase for year {r}, table {crss[key]['db_table']} failed to load", Exception)
                print("FAILED!\nCheck ERROR_LOG.txt")
                db_curs.close()
                db_con.close()
                sys.exit(1)

            # move files from ready_directory to archives
            for item in os.listdir(rdy):
                target_file = os.path.join(rdy, item)
                archive_file = os.path.join(arch, f"{r}{item}")
                try:
                    if os.path.exists(archive_file):
                        os.remove(archive_file)
                    shutil.move(target_file, archive_file)
                except:
                    el.logging(3, "Could not move files", Exception)

            # clean out source_directory for next batch of transformations
            clear_directory(src)
            clear_directory(rdy)

        except:
            el.logging(3, "ETL process:", Exception)
            print("ETL FAILED!\nCheck ERROR_LOG.txt")
            db_curs.close()
            db_con.close()
            sys.exit(1)

        print(f"......{r} Complete")
    db_curs.close()
    db_con.close()

print("ETL COMPLETE!!!")
           
if __name__ == "__main__":
    try:
        crss_etl(record_years=available_years, files=crss_files, src=source_directory, rdy=ready_directory, arch=archive_directory)
    except:
        print("ETL Error")
