// month with most accidents for each year

WITH month_most_accidents AS(
SELECT
	*
    ,row_number() OVER(PARTITION BY year_record ORDER BY TotalAccidents DESC) AS rownum
FROM (
	SELECT
		year_record
		,month_record
		,COUNT(*) AS TotalAccidents
	FROM accident
	GROUP BY month_record, year_record
    ) AS a)
SELECT
	year_record
    ,month_record
    ,TotalAccidents
FROM month_most_accidents
WHERE rownum = 1
;