CRSS: Crash Reporting Sampling System

This project is designed to extract data from the NHTSA CRSS, 
process the data, then store in a MySQL database for analytics 
using SQL queries visualized with PowerBI.

The CRSS data is requested and utilized by national and international highway safety, state and
local governments, Congress, Federal agencies, research organizations, media, and the public.


What is the NHTSA's CRSS?

    As of December 2024 the availble yearly reports range from 2016 to 2022. The CRSS model was first 
available in 2016 as a replacement for the GES report model previously used, and is independent of the 
GES model. The CRSS model is a national probability based sample system that uses data from police reported 
accidents, filtering accidents that occured on trafficways and resulted in property dameage, injury, or death.

    Estimates are more than six million accidents occur annually nation wide. Not all are reported to police for
various or unknown reasons. Data for the CRSS report is selected from 60 selected sites accross the United States
reflecting geography, population, driven miles, and accidents. Each site receives tens of thousands of reports 
from hundreds of agencies. Police reports are selected from 60 sites (in 2022 only 59 responded with 
available reports) and sent to a central location to be coded into a common format with 120 data elements.

    Selecting data for CRSS directly would be an unrealistic task. The data gos through a three stage process
to be stratified. Provided below is a link to the 63 page document explaining the strata process.
https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812706

Steps:
    1.Extract only the required CSV files from the NHTSA website, "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CRSS/". 
        As of December 2024 the availble yearly reports range 2016 to 2022.


Overview of the ETL Pipeline:

1. Extraction Phase (crssextract.py)

Purpose: 
Downloads and extracts specific files (ACCIDENT.CSV, VEHICLE.CSV, etc.) for a given year from the NHTSA website.

Integration:
Called in main.py during the extraction step.
Extracted files are stored in local_directory and are expected to be processed in subsequent steps.

2. Transformation Phase (crsstransform.py)

Purpose: 
Transforms the extracted files to match the database schema by:
Renaming columns.
Creating new columns (e.g., acc_date, veh_id).
Dropping unnecessary columns.

Integration:
Called in main.py during the transformation step.
Uses column mappings from column_maps.py.
Saves transformed files back to local_directory.

3. Loading Phase (crssload.py)

Purpose: 
Loads the transformed files into the database by:
Generating SQL INSERT statements using the db_insert function.
Executing the queries using MySQL.

Integration:
Used in main.py during the loading step.
Relies on the get_columns function to retrieve the database schema.

4. Column Mappings (column_maps.py)

Purpose: 
Provides mappings between source file column names and database column names.

Integration:
Used in crsstransform.py for column renaming.
Provides flexibility to handle multiple naming conventions in source files.
