sqoop import \
  --connect jdbc:mysql://localhost:3306/mal \
  --username root \
  --password Hao@23133020 \
  --table mal_anime \
  --target-dir /user/hive/warehouse/mal_anime \
  --fields-terminated-by '\t' \
  --enclosed-by '"' \
  --escaped-by '\\' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --delete-target-dir \
  --num-mappers 1
  
CREATE EXTERNAL TABLE mal_anime (
  mal_id INT,
  title STRING,
  approved BOOLEAN,
  type STRING,
  episodes INT,
  year INT,
  season STRING,
  status STRING,
  score DOUBLE,
  scored_by INT,
  rank INT,
  popularity INT,
  members INT,
  favorites INT,
  genres STRING,
  studios STRING,
  demographics STRING,
  url STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/mal_anime';

sqoop import \
  --connect jdbc:mysql://localhost:3306/anilist \
  --username root \
  --password Hao@23133020 \
  --table anime \
  --delete-target-dir \
  --target-dir /user/hive/warehouse/anl_anime \
  --fields-terminated-by '\t' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --num-mappers 1

CREATE EXTERNAL TABLE anl_anime (
  id INT,
  title_romaji STRING,
  title_english STRING,
  title_native STRING,
  format STRING,
  episodes INT,
  duration INT,
  source STRING,
  country STRING,
  isAdult BOOLEAN,
  score DOUBLE,
  meanScore DOUBLE,
  popularity INT,
  favourites INT,
  trending INT,
  genres STRING,
  tags STRING,
  status STRING,
  studio STRING,
  season STRING,
  start_year INT,
  url STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/anl_anime';