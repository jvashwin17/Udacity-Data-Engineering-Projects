import configparser
from datetime import datetime
from pyspark.sql.types import TimestampType
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format , dayofweek



config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input + "song_data/A/A/A/*.json"
    
    # read song data file
    print('Input song data json file read started')
    df = spark.read.json(song_data,mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')
    print('Input song data json file read completed')
    
    # extract columns to create songs table
    print('songs_table data extraction started \n')
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    print('songs_table data extraction completed \n')
    
    # write songs table to parquet files partitioned by year and artist
    print('songs_table data write started \n')
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])
    print('Songs_table data write Completed \n')

    # extract columns to create artists table
    print('artist_table data extraction started \n')
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    print('artist_table data extraction completed \n')
    
    # write artists table to parquet files
    print('artist_table data write started \n')
    artists_table.write.parquet(output_data + "artists_table/", mode="overwrite")
    print('artist_table data write Completed')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input + "log-data/*/*/*.json"

    # read log data file
    print('Input log data json file read started')
    df = spark.read.json(log_data,mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')
    print('Input log data json file read completed')
    
    # filter by actions for song plays
    df=df.filter(df.page == 'NextSong')

    # extract columns for users table
    print('users_table data exteaction started \n')
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    print('users_table data exteaction completed \n')
    
    # write users table to parquet files
    print('users_table data write started \n')
    users_table.write.parquet(output_data + "users_table/", mode="overwrite")
    print('users_table write Completed')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    print('time_table data extraction started \n')
    time_table=df.select('start_time').drop_duplicates() \
        .withColumn('hour', hour(col('start_time'))) \
        .withColumn('day', dayofmonth(col('start_time'))) \
        .withColumn('week', weekofyear(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('weekday', dayofweek(col('start_time')))
    #time_table.show()
    print('time_table data extraction completed \n')
    
    # write time table to parquet files partitioned by year and month
    print('time_table data write started \n')
    time_table.write.parquet(output_data + "time_table/", mode="overwrite" )
    print('time_table data write Completed \n')

    # read in song data to use for songplays table
    song_df = spark.read.format("parquet").option("basePath", os.path.join(output_data, "songs/")).load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    print('songplays_table data extraction started \n')
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),
                         col("start_time"),
                         col("userId").alias("user_id"),
                         col("level"),
                         col("song_id"),
                         col("artist_id"), 
                         col("sessionId").alias("session_id"), 
                         col("location"), 
                         col("userAgent").alias("user_agent")
                        )
    print('songplays_table data extraction completed \n')
    # write songplays table to parquet files partitioned by year and month 
    print('songplays_table data write started \n')
    songplays_table = songplays_table.write.parquet(output_data + "songplays/", mode="overwrite")
    print('songsplay_table data write completed \n')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-as/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
