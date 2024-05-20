#!/bin/bash
cd ~/Projects/CRSS/sourcedata
echo "Beginning download and unzipping of the CRSS files"

# downlad the manual with detials of the CRSS and FARS data
cd ~/Projects/CRSS
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/Links%20for%20CRSS%20Manuals.pdf --output CRSSManual.pdf

# downlaod and unzip files for years 2016-2022

# 2022
cd ~/Projects/CRSS/sourcedata
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2022/CRSS2022CSV.zip --output 2022CSV.zip
unzip -o 2022CSV.zip

# 2021
mkdir ~/Projects/CRSS/sourcedata/2021
cd ~/Projects/CRSS/sourcedata/2021
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2021/CRSS2021CSV.zip --output 2021.CSV.zip
unzip -o 2021CSV.zip

# 2020
cd ~/Projects/CRSS/sourcedata
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2020/CRSS2020CSV.zip --output 2020CSV.zip
unzip -o 2020CSV.zip

# 2019
mkdir ~/Projects/CRSS/2019
cd ~/Projects/CRSS/2019
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2019/CRSS2019CSV.zip --output 2019CSV.zip
unzip -o 2019CSV.zip

# 2018
mkdir ~/Projects/CRSS/sourcedata/2018
cd ~/Projects/CRSS/sourcedata/2018
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2018/CRSS2018CSV.zip --output 2018CSV.zip
unzip -o 2018CSV.zip

# 2017
mkdir ~/Projects/CRSS/2017
cd ~/Projects/CRSS/2017
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2017/CRSS2017CSV.zip --output 2017CSV.zip
unzip -o 2017CSV.zip

# 2016
mkdir ~/Projects/CRSS/2016
cd ~/Projects/CRSS/2016
curl -s https://static.nhtsa.gov/nhtsa/downloads/CRSS/2016/CRSS2016CSV.zip --output 2016CSV.zip
unzip -o 2016CSV.zip


# creating master file to combine necessary data years 2016-2022 for each category

echo "All needed files loaded into: "
$pwd