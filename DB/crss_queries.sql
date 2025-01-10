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


WITH 
veh_nothave AS (SELECT COUNT(veh_id) AS "Not_Have" FROM fact_vehicle WHERE v_make IS NULL),
veh_have AS (SELECT COUNT(veh_id) AS "Have" FROM fact_vehicle WHERE v_make IS NOT NULL),
veh_all AS (SELECT COUNT(veh_id) AS "Total" FROM fact_vehicle)
SELECT Have, 
	ROUND((Have/Total), 2)*100 AS "Percent_Have", 
    Not_Have, 
    ROUND((Not_Have/Total), 2)*100 AS "Percent_NotHave", 
    (Have+Not_Have) AS "Added",
    ROUND(((Have/Total)+(Not_have/Total)), 2) * 100 AS "Percent_Added",
    Total 
FROM veh_nothave JOIN veh_have JOIN veh_all
;