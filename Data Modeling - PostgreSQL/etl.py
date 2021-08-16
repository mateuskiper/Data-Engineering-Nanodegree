import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process song file
    
    cur: cusor of psycopg2
    filepath: song file path (json)
    """
    df = pd.read_json(filepath, typ='series')

    song_data = [df.loc['song_id'], df.loc['title'], df.loc['artist_id'], df.loc['year'], df.loc['duration']]
    cur.execute(song_table_insert, song_data)
    
    artist_data =[df.loc['artist_id'],df.loc['artist_name'],df.loc['artist_location'],
                  df.loc['artist_latitude'],df.loc['artist_longitude']]
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process log file
    
    cur: cusor of psycopg2
    filepath: log file path (json)
    """
    df = pd.read_json(filepath, lines=True) 

    df = df[df['page'] == 'NextSong']

    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = [df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.week,
                 df['ts'].dt.month, df['ts'].dt.year, df['ts'].dt.weekday]
    
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.userId, artistid, row.ts, row.level, songid, row.sessionId, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process data
    
    cur: cursor of psycopg2
    conn: connection of postgresql
    filepath: directory path of data
    func: function of processing
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()