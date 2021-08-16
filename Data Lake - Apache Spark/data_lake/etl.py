import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
        Extract, transform and load the song and artist tables
        Inputs:
            spark = Spark Session
            input_data = input file path
            output_data = output file path
            
    '''
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    df =  spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='broken').drop_duplicates()

    # songs_table(song_id, title, artist_id, year, duration)
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    songs_table.write.parquet(output_data + 'songs_table.parquet', mode='overwrite', partitionBy=['year', 'artist_id'])

    # artists_table(artist_id, name, location, lattitude, longitude)
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_lattitude', 'artist_longitude').drop_duplicates()
    artists_table.write.parquet(output_data + 'artists_table.parquet', mode='overwrite')

    
def process_log_data(spark, input_data, output_data):
    '''
        Extract, transform and load the users, time and songplays tables
        Inputs:
            spark = Spark Session
            input_data = input file path
            output_data = output file path
            
    '''
    
    log_data = input_data + 'log_data/*/*/*/*.json'

    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='broken').drop_duplicates()
    df = df.filter(df.page == 'NextSong')
    
    # users_table(user_id, first_name, last_name, gender, level)
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates()
    users_table.write.parquet(output_data + 'users_table.parquet', mode='overwrite')

    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # time_table(tart_time, hour, day, week, month, year, weekday)
    time_table = df.withColumn('hour', hour('start_time'))\
                   .withColumn('day', dayofmonth('start_time'))\
                   .withColumn('week', weekofyear('start_time'))\
                   .withColumn('month', month('start_time'))\
                   .withColumn('year', year('start_time'))\
                   .withColumn('weekday', dayofweek('start_time'))\
                   .select('ts', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').drop_duplicates()
    time_table.write.parquet(output_data + 'time_table.parquet', mode='overwrite', partitionBy=['year', 'month'])

    song_df = spark.read.parquet(output_data + 'songs_table.parquet')
    # songplays_table(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    songplays_table = df.selectColumn('songplay_id', monotonically_increasing_id())\
                        .join('song_df', df.song == song_df.title, how='inner')\
                        .select('songplay_id', 
                                'start_time', 
                                col('userId').alias('user_id'),
                                'level', 
                                'song_id', 
                                'artist_id', 
                                col('sessionId').alias('session_id'), 
                                'location', 
                                col('userAgent').alias('user_agent'))
    songplays_table.write.parquet(output_data + 'songplays_table.parquet', mode='overwrite', partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://udacity-dend/output/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
