CREATE TABLE IF NOT EXISTS movies_with_genre_index (
    movieid int,
    title text,
    year int,
    genres text,
    PRIMARY KEY((genres), year, movieid)
);
