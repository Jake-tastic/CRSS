import requests
from zipfile import ZipFile
import os
import sys
import crss_err_log as el



def extraction(record_year, files, directory):
    """Extract accident reports from NHTSA website and store locally
    Args:
        record_year(list): available years for accident reports 2016-2022 (as of 2024/12)
        files(list): out of 20 files per report year, only four are needed
        directory(str): location for files to be stored locally
    """
    print(f"...Moving to {directory}")
    if os.path.exists(directory):
        os.chdir(path=directory)
    else:
        raise FileNotFoundError(f"...Failed to change directory, {directory} does not exist. Check the path!")

    print(f"...Downloading data for {record_year}...")
    try:
        # obtain zip files from NHTSA website
        response = requests.get(f'https://static.nhtsa.gov/nhtsa/downloads/CRSS/{record_year}/CRSS{record_year}CSV.zip')
    # alternatively, some years include a "." after the year
    except:
        response = requests.get(f'https://static.nhtsa.gov/nhtsa/downloads/CRSS/{record_year}/CRSS{record_year}.CSV.zip')

    if response.status_code == 200:
        print("...Download successful, extracting files...")
        with open("temp.zip", "wb") as temp_file:
            temp_file.write(response.content)

        # unzip only required files into source directory
        with ZipFile("temp.zip", "r") as zip_ref:
            # normalize all zip contents to uppercase for matching
            zip_contents = [item.lower() for item in zip_ref.namelist()]
            el.error_log(1, f"Zipfile contents for {record_year}", zip_contents)
            for f in files:
                f_lower = f.lower()
                file_found = False
                # if file is in the root, extract it
                for item in zip_ref.namelist():
                    item_lower = item.lower()
                    if item_lower.endswith(f_lower):
                        print(f"......Extracting {f}...")
                        zip_ref.extract(item)
                        file_found = True
                        break
            if not file_found:
                print(f"......Error: {f} not found in ZIP archive")
                el.error_log(3, f"Extracting {f} for {record_year}", Exception)
                sys.exit(1)
        os.remove("temp.zip")
    else:
        print(f"...failed, error code: {response.status_code}")
        



if __name__ == "__main__":
    record_year = 2016
    files = ["ACCIDENT.CSV", "VEHICLE.CSV"]
    directory = "C:/Users/cojac/Projects/crss/data/sourcedata"

    extraction(record_year, files, directory)


  