set hive.auto.convert.join=false;
set mapreduce.job.reduces=2;

SELECT 
	browser,
SUM(CASE WHEN sex = "male" THEN 1 ELSE 0 END),
SUM(CASE WHEN sex = "female" THEN 1 ELSE 0 END)	
FROM 
	logs 
	INNER JOIN users AS users
		ON logs.ip = users.ip
GROUP BY 
	browser
LIMIT 10;	
