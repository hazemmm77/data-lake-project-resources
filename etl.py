import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')



    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id")
        .parquet(output_data)

    # extract columns to create artists table
    artists_table =  df.selectExpr('artist_id','artist_name as name' ,\
                                  'artist_location as location' ,'artist_latitude as latitude ',\
                                  'artist_longitude as longitude ')

    # write artists table to parquet files
    artists_table.write.parquet(output_data)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data,'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page =='NextSong'")

    # extract columns for users table
    users_table=df.selectExpr('userId as user_id','firstName as first_name','lastName as last_name','gender','level')

    # write users table to parquet files
    users_table..write.parquet(output_data)

    # create timestamp column from original timestamp column
     def format_datetime(ts):
         return datetime.fromtimestamp(ts/1000.0)

    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df =df.withColumn("start_time", get_timestamp(dflogs_temp.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df =df.withColumn('date', get_datetime(df.ts))

    # extract columns to create time table
    time_table = 

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
