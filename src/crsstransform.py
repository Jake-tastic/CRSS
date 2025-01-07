import pandas as pd
import column_maps as cm
import crss_err_log as el
from charset_normalizer import detect
import sys
import os

def detect_encoding(file_path):
    """
    Detect the encoding of a file and load it as a pandas DataFrame.
    
    Args:
        file_path (str): Path to the file.
        
    Returns:
        pd.DataFrame: Loaded DataFrame with the detected encoding.
    """
    # determine encoding of file and convert if not utf-8
    with open(file_path, "rb")as enc_file:
        try:
            return pd.read_csv(file_path, encoding="utf-8", low_memory=False)
        except:
            el.error_log(2, f"loading in utf-8 failed for {file_path}.", UnicodeDecodeError)
            # read first 100 KB
            raw_data = enc_file.read(100000) 
            result = detect(raw_data)
            # default to utf-8 if detection fails
            detected_encoding = result.get("encoding", "ISO-8859-1")
            try:
                # attempt ti load file with the detected encoding or utf-8
                return pd.read_csv(file_path, encoding=detected_encoding, low_memory=False)
            except:
                # if detection fails attempt ISO-8859-1 encoding
                el.error_log(2, f"Encoding fallback to ISO-8859-1 failed for {file_path}, trying other encodings.", Exception)
                try:
                    return pd.read_csv(file_path, encoding='latin-1', low_memory=False)
                except:
                    return pd.read_csv(file_path, encoding='cp1252', low_memory=False)
                


def transform_acc(a_cols, record_year):
    try:
        primary_path = ("C:/Users/cojac/Projects/crss/data/sourcedata/ACCIDENT.CSV")
        secondary_path = (f"C:/Users/cojac/Projects/crss/data/sourcedata/CRSS{record_year}CSV/ACCIDENT.CSV")
    
        if os.path.exists(primary_path):
            target_file = primary_path
        elif os.path.exists(secondary_path):
            target_file = secondary_path
        else:
            raise FileNotFoundError(f"ACCIDENT.CSV not found in expected locations for {record_year}")
        # convert to pandas index
        a_cols = pd.Index(a_cols)

        # load file
        acc = detect_encoding(target_file)
        el.error_log(1, "Raw cols accident.csv: ", list(acc.columns))

        # check for and remove duplicate columns
        if acc.columns.duplicated().any():
            el.error_log(1, "Duplicate columns detected", acc.columns[acc.columns.duplicated()].tolist())
            acc = acc.loc[:, ~acc.columns.duplicated()]

        # make columns lowercase, remove white space, and rename
        acc.columns = acc.columns.str.lower().str.strip()
        el.error_log(1, "Normalized cols accident.csv: ", list(acc.columns))
        acc.rename(columns=cm.accident.get_map(), inplace=True)
        el.error_log(1, "Renamed cols accident.csv: ", list(acc.columns))

        # create and populate columns acc_date and acc_time
        acc['acc_date'] = (str(record_year) + 
                           acc['month'].astype(str).str.zfill(2)).astype(int)
        acc['acc_time'] = ('1' + acc['hour'].astype(str).str.zfill(2) + 
                           acc['minute'].astype(str).str.zfill(2)).astype(int)

        # remove duplicates again after renaming
        if acc.columns.duplicated().any():
            el.error_log(1, "Duplicate columns detected", acc.columns[acc.columns.duplicated()].tolist())
            acc = acc.loc[:, ~acc.columns.duplicated()]

        acc.drop(columns=acc.columns.difference(a_cols), inplace=True, errors="ignore")

        # align datafram with database schema (add missing columns as NaN)
        acc = acc.reindex(columns=a_cols, fill_value=pd.NA)

        # overwrite the accident file with the new dataframe
        acc.to_csv(f"C:/Users/cojac/Projects/crss/data/readydata/ACCIDENT.CSV", index=False, encoding="utf-8")
        print("......Accident file transformation complete!")
    except:
        el.error_log(3, "Transforming accident file ", Exception)
        sys.exit(1)

def transform_veh(v_cols, record_year):
    """
    Transform and merge the vehicle and parkwork files, renaming columns,
    and creating a unique vehicle ID.
    Args:
        v_cols (list): List of columns required in the database.
    """
    try:
        # convert to a pandas index
        v_cols = pd.Index(v_cols)

        # files to merge
        vehicles = "VEHICLE", "PARKWORK"
        combined_data = []

        for v in vehicles:
            primary_path = (f"C:/Users/cojac/Projects/crss/data/sourcedata/{v}.CSV")
            secondary_path = (f"C:/Users/cojac/Projects/crss/data/sourcedata/CRSS{record_year}CSV/{v}.CSV")

            if os.path.exists(primary_path):
                target_file = primary_path
            elif os.path.exists(secondary_path):
                target_file = secondary_path
            else:
                raise FileNotFoundError(f"ACCIDENT.CSV not found in expected locations for {record_year}")
            
            # load the vehicle or parkwork file
            veh = detect_encoding(target_file)

            # make columns all lower case and remove white space
            veh.columns = veh.columns.str.lower().str.strip()

            # rename columns to match database using column_maps
            veh.rename(columns=cm.vehicle.get_map(), inplace=True)

            # combine columns casenum and veh_no to create unique vehicle id
            # remove first two digits of casenum (first four are year: strip 2016 to 16)
            veh["veh_id"] = (veh["case_num"].astype(str).str[2:] + 
                             veh["veh_no"].astype(str).str.zfill(2))

            veh.drop(columns=veh.columns.difference(v_cols), inplace=True, errors="ignore")

            # align dataframe with database schema and append
            veh = veh.reindex(columns=v_cols, fill_value=pd.NA)
            combined_data.append(veh)

            # combine vehicle and parkwork dataframes and save
            veh_final = pd.concat(combined_data, ignore_index=True)
            veh_final.to_csv(f"C:/Users/cojac/Projects/crss/data/readydata/VEHICLES.CSV", index=False, encoding="utf-8")
            print("......Vehicle file transformation complete!")
    except:
        el.error_log(3, f"Transforming vehicle file: {v}", Exception)
        sys.exit(1)
    
def transform_per(p_cols, record_year):
    try:
        primary_path = ("C:/Users/cojac/Projects/crss/data/sourcedata/PERSON.CSV")
        secondary_path = (f"C:/Users/cojac/Projects/crss/data/sourcedata/CRSS{record_year}CSV/PERSON.CSV")
    
        if os.path.exists(primary_path):
            target_file = primary_path
        elif os.path.exists(secondary_path):
            target_file = secondary_path
        else:
            raise FileNotFoundError(f"ACCIDENT.CSV not found in expected locations for {record_year}")
        # convert to a pandas index
        p_cols = pd.Index(p_cols)

        # open the file
        per = detect_encoding(target_file)

        # make columns lower case and remove white space
        per.columns = per.columns.str.lower().str.strip()

        # use column_maps to rename columns to corrisponding names
        per.rename(columns=cm.person.get_map(), inplace=True)

        # ensure required columns exist
        required_cols = ["case_num", "veh_no"]
        for col in required_cols:
            if col not in per.columns:
                print(f"......Missing columns {col} in PERSON.CSV. Filling with default values")
                per[col] = ""

        # not every person is an occupant of a vehicle
        # in these cases, make veh_no 99
        per["veh_no"] = per["veh_no"].fillna(99)

         # combine columns casenum and veh_no to match the veh_id in the vehicle table
        # remove first two digits of casenum (first four are year: strip 2016 to 16)
        per["veh_id"] = (per["case_num"].astype(str).str[2:] + 
                         per["veh_no"].astype(str).str.zfill(2))

        per.drop(columns=per.columns.difference(p_cols), inplace=True, errors="ignore")

        # align dataframe with database schema (add missing columns as NaN)
        per = per.reindex(columns=p_cols, fill_value=pd.NA)

        # rewrite the person.csv file
        per.to_csv(f"C:/Users/cojac/Projects/crss/data/readydata/PERSON.CSV", index=False, encoding="utf-8")
        print("......Person file transfomration complete!")
    except:
        el.error_log(3, "Tranforming Person file", Exception)
        sys.exit(1)

def crss_transform(a_col, v_col, p_col, record_year):
    """
    Tranform files accident, vehicle, and person
    to conform to database structure

    """
    transform_acc(a_col, record_year = record_year)
    transform_veh(v_col, record_year = record_year)
    transform_per(p_col, record_year = record_year)

if __name__ == "__main__":
    acols = cm.accident.columns.split(",") if isinstance(cm.accident.columns, str) else cm.accident.columns
    vcols = cm.vehicle.columns.split(",") if isinstance(cm.vehicle.columns, str) else cm.vehicle.columns
    pcols = cm.person.columns.split(",") if isinstance(cm.person.columns, str) else cm.person.columns

    crss_transform(acols, vcols, pcols)