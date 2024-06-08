#!/bin/bash
echo "Beginning download and unzipping of the CRSS files"

#downlad the manual with detials of the CRSS and FARS data
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/Links%20for%20CRSS%20Manuals.pdf --output ~/crss/CRSSManual.pdf

# downlaod and unzip files for years 2016-2022

# creating directory for project
mkdir ~/crss

# directory for the data to be stored
mkdir ~/crss/sourcedata

# directory to store the zip files after unzipping
mkdir ~/crss/archive_zips

# 2022
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2022/CRSS2022CSV.zip --output ~/crss/sourcedata/2022CSV.zip
cd ~/crss/sourcedata
unzip -o ~/crss/sourcedata/2022CSV.zip
mv ~/crss/sourcedata/2022CSV.zip ~/crss/archive_zips
mv ~/crss/sourcedata/CRSS2022CSV ~/crss/sourcedata/2022

# 2021
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2021/CRSS2021CSV.zip --output ~/crss/sourcedata/2021.CSV.zip
cd ~/crss/sourcedata
unzip -o ~/crss/sourcedata/2021.CSV.zip
mv ~/crss/sourcedata/2021.CSV.zip ~/crss/archive_zips
mv ~/crss/sourcedata/CRSS2021CSV ~/crss/sourcedata/2021

# 2020
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2020/CRSS2020CSV.zip --output ~/crss/sourcedata/2020CSV.zip
cd ~/Projects/crss/sourcedata
unzip -o ~/crss/sourcedata/2020CSV.zip
mv ~/crss/sourcedata/2020CSV.zip ~/crss/archive_zips
mv ~/crss/sourcedata/CRSS2020CSV ~/crss/sourcedata/2020

# 2019
mkdir ~/Projects/crss/sourcedata/2019
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2019/CRSS2019CSV.zip --output ~/crss/sourcedata/2019/2019CSV.zip
cd ~/crss/sourcedata/2019
unzip -o ~/crss/sourcedata/2019/2019CSV.zip
mv ~/crss/sourcedata/2019/2019CSV.zip ~/crss/archive_zips

# 2018
mkdir ~/crss/sourcedata/2018
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2018/CRSS2018CSV.zip --output ~/crss/sourcedata/2018/2018CSV.zip
cd ~/crss/sourcedata/2018
unzip -o ~/crss/sourcedata/2018/2018CSV.zip
mv ~/crss/sourcedata/2018/2018CSV.zip ~/crss/archive_zips

# 2017
mkdir ~/crss/sourcedata/2017
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2017/CRSS2017CSV.zip --output ~/crss/sourcedata/2017/2017CSV.zip
cd ~/crss/sourcedata/2017
unzip -o ~/crss/sourcedata/2017/2017CSV.zip
mv ~/crss/sourcedata/2017/2017CSV.zip ~/crss/archive_zips

# 2016
mkdir ~/crss/sourcedata/2016
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2016/CRSS2016CSV.zip --output ~/Projects/crss/sourcedata/2016/2016CSV.zip
cd ~/Projects/crss/sourcedata/2016
unzip -o ~/crss/sourcedata/2016/2016CSV.zip
mv ~/crss/sourcedata/2016/2016CSV.zip ~/crss/archive_zips

# creating files for all data to be merged into
mkdir ~/crss/sourcedata/all_years



echo "All needed files loaded and unzipped"
echo "All zip files are archived in it's own directory in the main crss directory"