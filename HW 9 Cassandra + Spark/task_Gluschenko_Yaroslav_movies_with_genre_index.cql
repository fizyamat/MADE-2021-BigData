SELECT 
    count(*) 
FROM 
    movies_with_genre_index 
WHERE 
    genres CONTAINS 'Horror'
    AND year >= 1980 
    AND year <= 1990
ALLOW FILTERING
;