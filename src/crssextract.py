import requests
from zipfile import ZipFile
import os



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

    #get the zip file for inputed year
    url = f'https://static.nhtsa.gov/nhtsa/downloads/CRSS/{record_year}/CRSS{record_year}CSV.zip'
    print(f"...Downloading data for {record_year}...")

    #obtain zip files from NHTSA website
    response = requests.get(url)

    if response.status_code == 200:
        print("...Download successful, extracting files...")
        with open("temp.zip", "wb") as temp_file:
            temp_file.write(response.content)

        #unzip only required files into source directory
        with ZipFile("temp.zip", "r") as zip_ref:
            zip_contents = zip_ref.namelist()
            for f in files:
                if f in zip_contents:
                    print(f"......Extracting {f}")
                    zip_ref.extract(f)
                else:
                    print(f"......Error: {f} not found in ZIP archive")
        os.remove("temp.zip")
    else:
        print(f"...failed, error code: {response.status_code}")
        



if __name__ == "__main__":
    record_year = 2016
    files = ["ACCIDENT.CSV", "VEHICLE.CSV"]
    directory = "C:/Users/cojac/Projects/crss/data/sourcedata"

    extraction(record_year, files, directory)


  