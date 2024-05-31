#!/bin/bash
echo "Beginning download and unzipping of the CRSS files"

# downlad the manual with detials of the CRSS and FARS data
#curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/Links%20for%20CRSS%20Manuals.pdf --output ~/Projects/crss/CRSSManual.pdf

# downlaod and unzip files for years 2016-2022

# directory for the data to be stored
mkdir ~/Projects/crss/sourcedata

# directory to store the zip files after unzipping
mkdir ~/Projects/crss/archive_zips

# 2022
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2022/CRSS2022CSV.zip --output ~/Projects/crss/sourcedata/2022CSV.zip
cd ~/Projects/crss/sourcedata
unzip -o ~/Projects/crss/sourcedata/2022CSV.zip
mv ~/Projects/crss/sourcedata/2022CSV.zip ~/Projects/crss/archive_zips

# 2021
mkdir ~/Projects/crss/sourcedata/2021
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2021/CRSS2021CSV.zip --output ~/Projects/crss/sourcedata/2021/2021.CSV.zip
cd ~/Projects/crss/sourcedata/2021
unzip -o ~/Projects/crss/sourcedata/2021/2021.CSV.zip
mv ~/Projects/crss/sourcedata/2021/2021.CSV.zip ~/Projects/crss/archive_zips

# 2020
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2020/CRSS2020CSV.zip --output ~/Projects/crss/sourcedata/2020CSV.zip
cd ~/Projects/crss/sourcedata
unzip -o ~/Projects/crss/sourcedata/2020CSV.zip
mv ~/Projects/crss/sourcedata/2020CSV.zip ~/Projects/crss/archive_zips

# 2019
mkdir ~/Projects/crss/sourcedata/2019
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2019/CRSS2019CSV.zip --output ~/Projects/crss/sourcedata/2019/2019CSV.zip
cd ~/Projects/crss/sourcedata/2019
unzip -o ~/Projects/crss/sourcedata/2019/2019CSV.zip
mv ~/Projects/crss/sourcedata/2019/2019CSV.zip ~/Projects/crss/archive_zips

# 2018
mkdir ~/Projects/crss/sourcedata/2018
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2018/CRSS2018CSV.zip --output ~/Projects/crss/sourcedata/2018/2018CSV.zip
cd ~/Projects/crss/sourcedata/2018
unzip -o ~/Projects/crss/sourcedata/2018/2018CSV.zip
mv ~/Projects/crss/sourcedata/2018/2018CSV.zip ~/Projects/crss/archive_zips

# 2017
mkdir ~/Projects/crss/sourcedata/2017
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2017/CRSS2017CSV.zip --output ~/Projects/crss/sourcedata/2017/2017CSV.zip
cd ~/Projects/crss/sourcedata/2017
unzip -o ~/Projects/crss/sourcedata/2017/2017CSV.zip
mv ~/Projects/crss/sourcedata/2017/2017CSV.zip ~/Projects/crss/archive_zips

# 2016
mkdir ~/Projects/CRSS/sourcedata/2016
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2016/CRSS2016CSV.zip --output ~/Projects/crss/sourcedata/2016/2016CSV.zip
cd ~/Projects/crss/sourcedata/2016
unzip -o ~/Projects/crss/sourcedata/2016/2016CSV.zip
mv ~/Projects/crss/sourcedata/2016/2016CSV.zip ~/Projects/crss/archive_zips

# creating files for all data to be merged into
mkdir ~/Projects/crss/sourcedata/all_years



echo "All needed files loaded and unzipped"
echo "All zip files are archived in it's own directory in the main crss directory"