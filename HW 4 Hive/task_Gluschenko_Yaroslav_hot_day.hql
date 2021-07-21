SELECT 
	`date`,
	COUNT(*) as num
FROM logs
GROUP BY `date`
ORDER BY num DESC
LIMIT 10;
