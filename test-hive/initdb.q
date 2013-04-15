DROP TABLE names;
DROP TABLE artists;
DROP TABLE albums;

CREATE TABLE names (name STRING);

CREATE TABLE artists
  (id INT, name STRING, year int)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ':';

CREATE TABLE albums
  (artistid INT, name STRING, year int)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ':';

LOAD DATA LOCAL INPATH './input/names'
  OVERWRITE INTO TABLE names;

LOAD DATA LOCAL INPATH './input/artists'
  OVERWRITE INTO TABLE artists;

LOAD DATA LOCAL INPATH './input/albums'
  OVERWRITE INTO TABLE albums;

LOAD DATA LOCAL INPATH './input/albums2'
  INTO TABLE albums;

-- Loaded data are stored in hdfs - unmodified!
dfs -lsr /user;