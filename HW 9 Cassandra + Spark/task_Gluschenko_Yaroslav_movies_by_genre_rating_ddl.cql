CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
    movieid int,
    title text,
    year int,
    genre text,
    timestamp text,
    rating float,
    userid int,
    PRIMARY KEY((genre), year, rating, movieid, userid, timestamp)
);