import column_maps, crss_logging, sys, os, polars
from charset_normalizer import detect

def load_file(file_path):
    """
    Load CSV file using Polars and ensure all columns are read as strings (Utf8)
    """
    try:
        # detect file encoding
        with open(file_path, "rb") as file:
            raw_data = file.read()
            encoding = detect(raw_data)["encoding"] or "utf-8"
            # Log the detected encoding
            crss_logging.logging(1, f"Detected encoding for {file_path}:", encoding)
        
        # detect problematic lines and clean
        temp_path = file_path + ".temp"
        with open(file_path, "r", encoding=encoding, errors="strict") as infile, open(temp_path, "w", encoding="utf-8") as outfile:
            for i, line in enumerate(infile, start=1):
                try:
                    outfile.write(line)
                except UnicodeDecodeError as e:
                    crss_logging.logging(2, f"Problem on line {i}", f"{e}\nProblem bytes {line[e.start:e.end]}")
                    cleaned_line = line.encode(encoding, error="replace").decode(encoding)
                    outfile.write(cleaned_line)

        # read file and retrieve column names
        preview = polars.read_csv(
            temp_path, n_rows=1, 
            encoding=encoding, 
            infer_schema_length=0)
        column_names = preview.columns

        # set all columns to Utf8
        schema = {col: polars.Utf8 for col in column_names}

        # load csv and force columns to Utf8 stings
        raw_df = polars.read_csv(
            temp_path,
            schema=schema,
            infer_schema=False
            )

        # Identify and retain only the "WEATHER" column, dropping others like "WEATHER1" or "WEATHER2"
        weather_columns = [col for col in raw_df.columns if "weather" in col.lower()]
        if len(weather_columns) > 1: # if multiple weather columns exist  (weather, weather1, weather2)
            if "WEATHER" in weather_columns:
                weather_columns.remove("WEATHER") # keep "WEATHER" and drop others
            raw_df = raw_df.drop(weather_columns)

        # convert column names to lowercase
        ready_df = raw_df.rename({col: col.lower() for col in raw_df.columns})
        
        crss_logging.logging(1, f"Loaded file: {file_path}", "")
        return ready_df
    except:
        crss_logging.logging(3, f"Failed to load file: {file_path}", Exception)
        sys.exit(1)

def rename_columns(dataframe, column_map):
    """
    Rename columns in a DataFrame using the column_map from column_maps.py.
    """
    mapping = column_map.get_map()
    dataframe = dataframe.rename({old: new for old, new in mapping.items() if old in dataframe.columns})
    return dataframe

def transform_acc(a_cols, record_year, src_dir, rdy_dir):

    primary_path = (f"{src_dir}ACCIDENT.CSV")
    secondary_path = (f"{src_dir}CRSS{record_year}CSV/ACCIDENT.CSV")
    
    if os.path.exists(primary_path):
        target_file = primary_path
    elif os.path.exists(secondary_path):
        target_file = secondary_path
    else:
        raise FileNotFoundError(f"ACCIDENT.CSV not found in expected locations for {record_year}")
    
    # initialize variables for row count logging
    raw_count = 0
    clean_count = 0

    try:
        # retrieve dataframe
        acc_df = load_file(target_file)

        # log row count before transformations
        raw_count += acc_df.height

        # rename columns using column_maps
        acc_df = rename_columns(acc_df, column_maps.accident)

        # transformations
        acc_df = acc_df.with_columns([
            (acc_df["case_num"].cast(polars.Utf8).str.slice(2)).alias("case_num"),
            (polars.lit(record_year).cast(polars.Utf8) + acc_df["month"].cast(polars.Utf8).str.zfill(2)).alias("acc_date"),
            (polars.lit("1") + acc_df["hour"].cast(polars.Utf8).str.zfill(2) + acc_df["minute"].cast(polars.Utf8).str.zfill(2)).alias("acc_time")
            ])
        
        # validate columns in renamed dataframe
        missing_columns = [col for col in a_cols if col not in acc_df.columns]
        if missing_columns:
            crss_logging.logging(
                                3, 
                                f"Missing columns in: {record_year} ACCIDENT: [{missing_columns}]", 
                                f"Columns in dataframe:\n{acc_df.columns}"
                                )
        
        acc_df = acc_df.select(a_cols)
        
        # log row count after transformations
        clean_count += acc_df.height
        crss_logging.row_counts(f"{record_year} Accident", raw_count, clean_count)
        
        # retain only required columns and write output
        acc_df = acc_df.select([col for col in a_cols])
        acc_df.write_csv(f"{rdy_dir}ACCIDENT.CSV", include_header=True)
        
        print("......Accident file transformation complete!")
    except:
        crss_logging.logging(3, "Transforming accident file ", Exception)
        sys.exit(1)

def transform_veh(v_cols, record_year, src_dir, rdy_dir):
    """
    Transform and merge the vehicle and parkwork files, renaming columns,
    and creating a unique vehicle ID.
    Args:
        v_cols (list): List of columns required in the database.
    """
    try:

        # files to merge
        vehicles = "VEHICLE", "PARKWORK"
        file_count = 0

        for v in vehicles:
            primary_path = (f"{src_dir}{v}.CSV")
            secondary_path = (f"{src_dir}CRSS{record_year}CSV/{v}.CSV")

            # initialize variables for row count logging
            raw_count = 0
            clean_count = 0


            if os.path.exists(primary_path):
                target_file = primary_path
            elif os.path.exists(secondary_path):
                target_file = secondary_path
            else:
                raise FileNotFoundError(f"{v}.CSV not found in expected locations for {record_year}")

            try:
                # load the vehicle or parkwork file
                veh_df = load_file(target_file)

                veh_df = rename_columns(veh_df, column_maps.vehicle)

                # log row count before transformations
                raw_count += veh_df.height

                # transformations
                veh_df = veh_df.with_columns([
                    (veh_df["case_num"].cast(polars.Utf8).str.slice(2)).alias("case_num")
                    ])
                veh_df = veh_df.with_columns([
                    (veh_df["case_num"] + veh_df["veh_no"].cast(polars.Utf8).str.zfill(2)).alias("veh_id")
                    ])
                
                # validate columns in renamed dataframe
                missing_columns = [col for col in v_cols if col not in veh_df.columns]
                if missing_columns:
                    crss_logging.logging(
                                        3, 
                                        f"Missing columns in {record_year} {v}: [{missing_columns}]", 
                                        f"Columns in dataframe:\n{veh_df.columns}"
                                        )
                for col in missing_columns:
                    veh_df = veh_df.with_columns(polars.lit("0").cast(polars.Utf8).alias(col))

                # retain only required columns
                veh_df = veh_df.select(v_cols)

                # row count after transformations and log for comparison 
                clean_count = veh_df.height
                crss_logging.row_counts(f"{record_year} Vehicle", raw_count, clean_count)

                # write to output
                output_file = f"{rdy_dir}VEHICLE.CSV"
                if os.path.exists(f"{rdy_dir}VEHICLE.CSV"):
                    existing_df = polars.read_csv(output_file)

                    # ensure both dataframes have matching datatypes
                    for col in veh_df.columns:
                        if col in existing_df.columns:
                            veh_df = veh_df.with_columns(
                                veh_df[col].cast(existing_df[col].dtype).alias(col)
                            )
                        else:
                            crss_logging.logging(2, f"Columns '{col}' is missing in existing_df", "Skipping type adjustment")
                        
                    # concatenate dataframes
                    veh_df = polars.concat([existing_df, veh_df])
                veh_df.write_csv(output_file, include_header=True)
                
            except:
                crss_logging.logging(3, f"{record_year} Vehicle transformation failed", Exception)
                sys.exit(1)
        
        
        print("......Vehicle file transfomration complete!")
    except:
        
        sys.exit(1)

def transform_per(p_cols, record_year, src_dir, rdy_dir):
    try:
        primary_path = (f"{src_dir}PERSON.CSV")
        secondary_path = (f"{src_dir}CRSS{record_year}CSV/PERSON.CSV")

        # initialize variables for row count logging
        raw_count = 0
        clean_count = 0
    
        if os.path.exists(primary_path):
            target_file = primary_path
        elif os.path.exists(secondary_path):
            target_file = secondary_path
        else:
            raise FileNotFoundError(f"PERSON.CSV not found in expected locations for {record_year}")

        # load the person file and retrieve Spark dataframe
        per_df = load_file(target_file)

        # log row count before transformations
        raw_count += per_df.height

        # rename columns using column_maps
        per_df = rename_columns(per_df, column_maps.person)

        # transformations
        per_df = per_df.with_columns([(per_df["case_num"].cast(polars.Utf8).str.slice(2)).alias("case_num")])
        per_df = per_df.with_columns([(per_df["case_num"] + per_df["veh_no"].cast(polars.Utf8).str.zfill(2)).alias("veh_id")])
        per_df = per_df.with_columns([(per_df["veh_id"] + per_df["per_no"].cast(polars.Utf8).str.zfill(2)).alias("per_id")])

        # validate columns in renamed dataframe
        missing_columns = [col for col in p_cols if col not in per_df.columns]
        if missing_columns:
            crss_logging.logging(
                                3, 
                                f"Missing columns in {record_year} PERSON: [{missing_columns}]", 
                                f"Columns in dataframe:\n{per_df.columns}"
                                )
        

        # retain only required columns and write output csv
        per_df = per_df.select(p_cols)

        # row count after transformations and log for comparison 
        clean_count += per_df.height
        crss_logging.row_counts(f"{rdy_dir} Person", raw_count, clean_count)

        # write dataframe to csv file
        per_df.write_csv(f"{rdy_dir}PERSON.CSV", include_header=True)
        print("......Person file transfomration complete!")

    except:
        crss_logging.logging(3, "Tranforming Person file", Exception)
        sys.exit(1)

def crss_transform(a_col, v_col, p_col, record_year, src_dir, rdy_dir):
    """
    Tranform files accident, vehicle, and person
    to conform to database structure

    """
    transform_acc(a_col, record_year = record_year, src_dir=src_dir, rdy_dir=rdy_dir)
    transform_veh(v_col, record_year = record_year, src_dir=src_dir, rdy_dir=rdy_dir)
    transform_per(p_col, record_year = record_year, src_dir=src_dir, rdy_dir=rdy_dir)