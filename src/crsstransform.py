import pandas as pd
import column_maps as cm
import crss_err_log as el

def transform_acc(a_cols):
    try:
        # convert to pandas index
        a_cols = pd.Index(a_cols)

        # load file
        acc = pd.read_csv("C:/Users/cojac/Projects/crss/data/sourcedata/ACCIDENT.CSV", low_memory=False)

        # check for and remove duplicate columns
        if acc.columns.duplicated().any():
            el.error_log("Duplicate columns detected", acc.columns[acc.columns.duplicated()].tolist())
            acc = acc.loc[:, ~acc.columns.duplicated()]

        # make columns lowercase and rename
        acc.columns = acc.columns.str.lower()
        acc.rename(columns=cm.accident.get_map(), inplace=True)

        # create and populate columns acc_date and acc_time
        acc['acc_date'] = (acc['year'].astype(str).str.zfill(4) + 
                           acc['month'].astype(str).str.zfill(2)).astype(int)
        acc['acc_time'] = ('1' + acc['hour'].astype(str).str.zfill(2) + 
                           acc['minute'].astype(str).str.zfill(2)).astype(int)

        # remove duplicates again after renaming
        if acc.columns.duplicated().any():
            el.error_log("Duplicate columns detected", acc.columns[acc.columns.duplicated()].tolist())
            acc = acc.loc[:, ~acc.columns.duplicated()]

        acc.drop(columns=acc.columns.difference(a_cols), inplace=True, errors="ignore")

        # align datafram with database schema (add missing columns as NaN)
        acc = acc.reindex(columns=a_cols, fill_value=pd.NA)

        # overwrite the accident file with the new dataframe
        acc.to_csv("C:/Users/cojac/Projects/crss/data/sourcedata/ACCIDENT.CSV", index=False)
        print("......Accident file transformation complete!")
    except Exception as e:
        el.error_log("Transforming accident file ", e)

def transform_veh(v_cols):
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
            # load the vehicle or parkwork file
            veh = pd.read_csv(f"C:/Users/cojac/Projects/crss/data/sourcedata/{v}.CSV", low_memory=False)

            # make columns all lower case
            veh.columns = veh.columns.str.lower()

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
        veh_final.to_csv("C:/Users/cojac/Projects/crss/data/sourcedata/VEHICLES.CSV", index=False)
        print("......Vehicle file transformation complete!")
    except Exception as e:
        el.error_log(f"Transforming vehicle file: {v}", e)
    
def transform_per(p_cols):
    try:
        # convert to a pandas index
        p_cols = pd.Index(p_cols)

        # open the file
        per = pd.read_csv("C:/Users/cojac/Projects/crss/data/sourcedata/PERSON.CSV", low_memory=False)

        # make columns lower case
        per.columns = per.columns.str.lower()

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
        per["veh_no"] = per["veh_no"].fillna(99).astype(int)

         # combine columns casenum and veh_no to match the veh_id in the vehicle table
        # remove first two digits of casenum (first four are year: strip 2016 to 16)
        per["veh_id"] = (per["case_num"].astype(str).str[2:] + 
                         per["veh_no"].astype(str).str.zfill(2))

        per.drop(columns=per.columns.difference(p_cols), inplace=True, errors="ignore")

        # align dataframe with database schema (add missing columns as NaN)
        per = per.reindex(columns=p_cols, fill_value=pd.NA)

        # rewrite the person.csv file
        per.to_csv("C:/Users/cojac/Projects/crss/data/sourcedata/PERSON.CSV", index=False)
        print("......Person file transfomration complete.")
    except Exception as e:
        el.error_log("Tranforming Person file", e)

def crss_transform(a_col, v_col, p_col):
    """
    Tranform files accident, vehicle, and person
    to conform to database structure

    """
    try:
        transform_acc(a_col)
    except Exception as e:
        el.error_log("Accident Transformation", e)

    try:
        transform_veh(v_col)
    except Exception as e:
        el.error_log("Vehicle Transformation", e)
    
    try:
        transform_per(p_col)
    except Exception as e:
        el.error_log("Person Transformation", e)

if __name__ == "__main__":
    acols = cm.accident.columns.split(",") if isinstance(cm.accident.columns, str) else cm.accident.columns
    vcols = cm.vehicle.columns.split(",") if isinstance(cm.vehicle.columns, str) else cm.vehicle.columns
    pcols = cm.person.columns.split(",") if isinstance(cm.person.columns, str) else cm.person.columns
    #transform_acc(acols)
    #transform_veh(vcols)
    #transform_per(pcols)
    crss_transform(acols, vcols, pcols)