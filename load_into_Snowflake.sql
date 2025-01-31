--Loading Transformed Data into Snowflake 

--Creating Database and Schema
create or replace database spotify;
create or replace schema data_types;

--Creating Tables
create or replace table spotify.data_types.albums(
album_id varchar,
album_name varchar(255),
album_rel_date date,
album_tracks int,
album_ext_urls varchar
);

create or replace table spotify.data_types.artists(
artist_id varchar,
artist_name varchar,
external_url varchar
);

create or replace table spotify.data_types.songs(
song_id varchar,
song_name varchar,
song_duartion int,
song_url varchar,
song_popularity int,
song_added date,
album_id varchar,
artist_id varchar
);


--Initiating connection between AWS S3 and Snowflake
create or replace storage integration spotify_S3
type = external_stage
storage_provider = S3
enabled = true
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::600627350141:role/s3_snow_flake_connection_spotify'
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-tharun/transformed_data/');


--Defining a CSV File Format 
create or replace file format csv_format
type = csv
field_delimiter = ','
skip_header = 1
--field_optionally_enclosed_by = ','
null_if = ('NULL','null')
empty_field_as_null = true;


--Using AWS S3 Bucket as Stage 
create or replace stage stage_spotify
url = 's3://spotify-etl-tharun/transformed_data/'
file_format = csv_format
storage_integration = spotify_S3;

--Initiating a Snow Pipe for Auto Ingestion of Transofrmed Data 
create or replace pipe pipe_spotify 
auto_ingest = true
as
copy into albums 
from @stage_spotify/albums_data/;
copy into artists
from @stage_spotify/artists_data/;
copy into songs
from @stage_spotify/songs_data/;
