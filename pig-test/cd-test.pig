--execute this script in local mode via pig -x local

albums = LOAD 'albums' USING PigStorage(':') AS (artistId:int, title:chararray, date:int);
artists = LOAD 'artists' USING PigStorage(':') AS (artistId:int, name:chararray, birth:int);

-- Count albums per artist
artistAlbums = GROUP albums BY artistId;
albumCount = FOREACH artistAlbums GENERATE group, COUNT(albums);

-- Join with artist to see artist name, etc.
albumCount2 = JOIN albumCount BY group, artists by artistId;

-- Remove some columns (projection)
albumCount3 = FOREACH albumCount2 GENERATE group AS artistId, artists::name AS name, $1 AS albumCount;

-- dump
describe albumCount3;
dump albumCount3;