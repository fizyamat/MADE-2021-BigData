SELECT 
	count(*) 
FROM 
	${table_name} 
WHERE 
	user_agent = 'Chrome/5.0' 
	AND page_size > 500 
	AND page_size < 1500
