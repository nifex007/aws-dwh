# Project 3 - Data Warehouse in AWS

## Project Goal

To build an ETL pipeline for a database hosted on Redshift

### Description 
Load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from in staging tables.

### Datasets

Song data: `s3://udacity-dend/song_data` <br>
Log data: `s3://udacity-dend/log_data`


### Schema
#### Fact Table
#### songplays
 `songplay_id`
 `start_time`,
 `user_id, level`,
 `song_id`, 
`artist_id`,
 `session_id`,
 `location`,
 `user_agent`

#### Dimension Tables 
##### users
`user_id`, `first_name`, `last_name`, `gender`, `level`

##### songs
`song_id`, `title`, `artist_id`, `year`, `duration`

##### artists
`artist_id`, `name`, `location`, `lattitude`, `longitude`

##### time
`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

### Setup
Setup a config file dwh.cfg file (<span style="color:red">DO NOT PUSH YOUR AWS CREDENTIALS</span>). 

```
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=<IAM Role arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

```

#### Run project

    $ python create_tables.py
    $ python etl.py




