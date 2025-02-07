-- changes for handling large ID's
ALTER TABLE fact_accident MODIFY COLUMN case_num BIGINT UNSIGNED;
ALTER TABLE fact_vehicle MODIFY COLUMN veh_id BIGINT UNSIGNED;
ALTER TABLE fact_person MODIFY COLUMN case_num BIGINT UNSIGNED;
ALTER TABLE fact_person MODIFY COLUMN veh_id BIGINT UNSIGNED;
ALTER TABLE fact_person MODIFY COLUMN per_id BIGINT UNSIGNED;

-- count rows by year to compare to ETL logs
SELECT dd.date_year, COUNT(fa.case_num)
FROM fact_accident AS fa
JOIN dimacc_dates AS dd
ON fa.acc_date = dd.date_id
GROUP BY dd.date_year;

-- accident totals by month/year
SELECT
	ddat.date_year AS year_record
	,ddat.month_num AS month_record
	,COUNT(acc.case_num) AS total_accidents
FROM fact_accident AS acc
JOIN dimacc_dates AS ddat
ON acc.acc_date = ddat.date_id
GROUP BY month_record, year_record
;

-- count of weather conditions per month/year
SELECT
	ddat.acc_date AS Record_Date,
    dweth.weather_cond AS Weather,
	COUNT(acc.case_num) AS Total_Accidents
FROM fact_accident AS acc
JOIN dimacc_dates AS ddat
ON acc.acc_date = ddat.date_id
JOIN dimacc_weather AS dweth
ON acc.weather = dweth.weather_id
GROUP BY Record_Date, Weather
;

-- query for month w/ most accidents per year
WITH month_most_accidents AS(
SELECT
	*
    ,row_number() OVER(PARTITION BY year_record ORDER BY total_accidents DESC) AS rownum
FROM (
	SELECT
		ddates.date_year AS year_record
		,ddates.month_number AS month_record
		,COUNT(acc.case_num) AS total_accidents
	FROM fact_accident AS acc
	JOIN dim_dates AS ddates
	ON acc.acc_date = ddates.date_id
	GROUP BY month_record, year_record
    ) AS a)
SELECT
	year_record
    ,month_record
    ,total_accidents
FROM month_most_accidents
WHERE rownum = 1
;

-- Top 10 vehicle year by Year
WITH vyear_by_year AS(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Accident_Year ORDER BY Total DESC) AS rownum
FROM (
	SELECT
		dts.date_year AS Accident_Year,
		fv.veh_year AS Vehicle_Year,
		COUNT(fv.veh_year) AS Total
	FROM fact_accident AS fa
    JOIN dimacc_dates AS dts
		ON fa.acc_date = dts.date_id
	JOIN fact_person AS fp
		ON fa.case_num = fp.case_num
	JOIN fact_vehicle AS fv
		ON fv.veh_id = fp.veh_id
	GROUP BY Accident_Year, Vehicle_Year
	) AS a)
SELECT *
FROM vyear_by_year
WHERE rownum BETWEEN 1 AND 10
;

-- top 10 vehicle makes per year
WITH vmake_by_year AS(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Accident_Year ORDER BY Total DESC) AS rownum
FROM (
	SELECT
		COUNT(fv.veh_id) AS Total,
		dts.date_year AS Accident_Year,
        dmod.make_id AS Make_ID,
        dmake.make_name AS Vehicle_Make
	FROM fact_person AS fp
    JOIN fact_vehicle AS fv
		ON fv.veh_id = fp.veh_id
	JOIN fact_accident AS fa
		ON fa.case_num = fp.case_num
    JOIN dimacc_dates AS dts
		ON fa.acc_date = dts.date_id
	JOIN dimveh_model AS dmod
		ON dmod.model_id = fv.make_model
	JOIN dimveh_make as dmake
		ON dmod.make_id = dmake.make_id
	GROUP BY Accident_Year, Make_ID, Vehicle_Make
	) AS a)
SELECT Accident_Year, Make_ID, Vehicle_Make, Total, rownum
FROM vmake_by_year
WHERE rownum BETWEEN 1 AND 10
;

-- Top 10 vehicle models by year
WITH vmodel_by_year AS(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Accident_Year ORDER BY Total DESC) AS rownum
FROM (
	SELECT
		COUNT(fv.veh_id) AS Total,
		dts.date_year AS Accident_Year,
        dmod.model_id AS Model_ID,
        dmake.make_name AS Vehicle_Make,
        dmod.model_name AS Vehicle_Model
	FROM fact_person AS fp
    JOIN fact_vehicle AS fv
		ON fv.veh_id = fp.veh_id
	JOIN fact_accident AS fa
		ON fa.case_num = fp.case_num
    JOIN dimacc_dates AS dts
		ON fa.acc_date = dts.date_id
	JOIN dimveh_model AS dmod
		ON dmod.model_id = fv.make_model
	JOIN dimveh_make as dmake
		ON dmod.make_id = dmake.make_id
	GROUP BY Accident_Year, Model_ID, Model_Year, Vehicle_Make
	) AS a)
SELECT Accident_Year, Model_ID, Vehicle_Make, Vehicle_Model, Total, rownum
FROM vmodel_by_year
WHERE rownum BETWEEN 1 AND 10
;

-- Top 10 Year Make Model by Year
WITH yearmakmod_by_year AS(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Accident_Year ORDER BY Total DESC) AS rownum
FROM (
	SELECT
		COUNT(fv.veh_id) AS Total,
		dts.date_year AS Accident_Year,
        dmod.model_id AS Model_ID,
        fv.veh_year AS Model_Year,
        dmake.make_name AS Vehicle_Make,
        dmod.model_name AS Vehicle_Model
	FROM fact_person AS fp
    JOIN fact_vehicle AS fv
		ON fv.veh_id = fp.veh_id
	JOIN fact_accident AS fa
		ON fa.case_num = fp.case_num
    JOIN dimacc_dates AS dts
		ON fa.acc_date = dts.date_id
	JOIN dimveh_model AS dmod
		ON dmod.model_id = fv.make_model
	JOIN dimveh_make as dmake
		ON dmod.make_id = dmake.make_id
	WHERE Model_ID NOT LIKE '99%'
	GROUP BY Accident_Year, Model_ID, Model_Year, Vehicle_Make
	) AS a)
SELECT Accident_Year, Model_ID, Model_Year, Vehicle_Make, Vehicle_Model, Total, rownum
FROM vmake_by_year
WHERE rownum BETWEEN 1 AND 10
;