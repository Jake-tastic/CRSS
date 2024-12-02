CREATE TABLE light_cond(light_id INT PRIMARY KEY, lgt_cond VARCHAR(25));
INSERT INTO light_cond 
VALUES
(1, "Daylight"),
(2, "Dark-No Light"),
(3, "Dark-Lighted"),
(4, "Dawn"),
(5, "Dusk"),
(6, "Dark-Unknown Lighting"),
(7, "other"),
(8, "Not Reported"),
(9, "Reported As Unknown");

CREATE TABLE weather(weather_id INT PRIMARY KEY, weather_cond VARCHAR(25));
INSERT INTO weather 
VALUES 
(1, "Clear"),
(2, "Rain"),
(3, "Sleet or Hail"),
(4, "Snow"),
(5, "Fog, Smog, Smoke"),
(6, "Severe Crosswinds"),
(7, "Blowing Sand, Soil Dirt"),
(8, "Other"),
(10, "Cloudy"),
(11, "Blowing Snow"),
(12, "Freezing Rain or Drizzle"),
(98, "Not Reported"),
(99, "Reported as Unknown");

CREATE TABLE schoolbus(bus_id INT PRIMARY KEY, bus_involved VARCHAR(3));
INSERT INTO schoolbus
VALUES
(0, "No"),
(1, "Yes");

CREATE TABLE interstate_hwy(hwy_id INT PRIMARY KEY, hwy_status VARCHAR(8));
INSERT INTO interstate_hwy
VALUES
(0, "No"),
(1, "Yes"),
(9, "Unknown");

CREATE TABLE injury_severity(injury_id INT PRIMARY KEY, injury_status VARCHAR(30));
INSERT INTO injury_severity
VALUES
(0, "No Apparent Injury"),
(1, "Possible Injury"),
(2, "Suspected Minor Injury"),
(3, "Suspected Serious Injury"),
(4, "Fatal"),
(5, "Injured, Severity Unknown"),
(6, "Died Prior to Crash"),
(8, "No Person Involved in Crash"),
(9, "Unknown/Not Reported ");


CREATE TABLE dim_dates(
	date_id INT PRIMARY KEY -- year + month
    ,month_number INT -- 1-12
    ,month_name VARCHAR(9) -- January-December
    ,date_year INT -- year value of date
    ,quarter_number INT -- 1-4
    ,quarter_name CHAR(2) -- Q1, Q2, Q3, Q4

    )
;

-- fill dim_dates table

DROP PROCEDURE IF EXISTS fill_dim_dates;
DELIMITER //
CREATE PROCEDURE fill_dim_dates(IN startdate DATE, IN stopdate DATE)
BEGIN
	DECLARE dates DATE;
    SET dates = startdate;
	WHILE dates <= stopdate 
		DO
		INSERT INTO dim_dates
		VALUES(date_format(dates, "%Y%m") -- date_id
			,dates -- record_date
			,MONTH(dates) -- month_number
			,MONTHNAME(dates) -- month_name
			,YEAR(dates) -- date_year
			,QUARTER(dates) -- quarter_number
			,CASE QUARTER(dates) WHEN 1 THEN 'Q1' WHEN 2 THEN 'Q2' WHEN 3 THEN 'Q3' ELSE 'Q4' END -- quarter_name
			);
		SET dates = ADDDATE(dates, INTERVAL 1 MONTH);
	END WHILE;
END
//
DELIMITER ;
CALL fill_dim_dates('2016-01-01', '2022-12-31')
;



-- query for month w/ most accidents per year
WITH month_most_accidents AS(
SELECT
	*
    ,row_number() OVER(PARTITION BY year_record ORDER BY TotalAccidents DESC) AS rownum
FROM (
	SELECT
		ddates.date_year AS year_record
		,ddates.month_number AS month_record
		,COUNT(acc.case_num) AS TotalAccidents
	FROM fact_accident AS acc
	JOIN dim_dates AS ddates
	ON acc.acc_date = ddates.date_id
	GROUP BY month_record, year_record
    ) AS a)
SELECT
	year_record
    ,month_record
    ,TotalAccidents
FROM month_most_accidents
WHERE rownum = 1
;