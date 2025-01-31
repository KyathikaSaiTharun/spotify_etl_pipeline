import json
from datetime import datetime
import os
import boto3
import pandas as pd 
from io import StringIO

def albums_list(data):
    albums = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_rel_date = row['track']['album']['release_date']
        album_tracks = row['track']['album']['total_tracks']
        album_ext_urls = row['track']['album']['external_urls']['spotify']
        album_dict = {"album_id":album_id,"album_name": album_name,"album_rel_date": album_rel_date,"album_tracks":album_tracks,"album_ext_urls":album_ext_urls}
        albums.append(album_dict)
    return albums

def artists_list(data):
    artist_list = []
    for row in data['items']:
        for key,value in row.items():
            if key == 'track':
                for val in value['artists']:
                    artist_dict = {"artist_id": val['id'],"name": val["name"],"external_url": val['external_urls']['spotify']}
                    artist_list.append(artist_dict)
    return artist_list
def songs_list(data):
    songs_list = []
    for song in data['items']:
        song_id = song['track']['id']
        song_name = song['track']['name']
        song_duration = song['track']['duration_ms']
        song_url = song['track']['external_urls']['spotify']
        song_popularity = song['track']['popularity']
        song_added = song['added_at']
        song_id = song['track']['id']
        album_id = song['track']['album']['id']
        artist_id = song['track']['album']['artists'][0]['id']
        song_dict = {"song_id":song_id,"song_name":song_name,"song_duration":song_duration,"song_url":song_url,"song_popularity":song_popularity,"song_added":song_added,"song_id":song_id,"album_id":album_id,"artist_id":artist_id}
        songs_list.append(song_dict)
    return songs_list
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-tharun'
    Key = 'raw_data/to_be_processed/'
    list = s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']
    spotify_data =[]
    spotify_keys = []
    for i in list:
        if i['Key'].split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket,Key=i['Key'])
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(i['Key'])
    for data in spotify_data:
            albums = albums_list(data)
            artists = artists_list(data)
            songs = songs_list(data) 
            albums_df = pd.DataFrame.from_dict(albums)
            albums_df = albums_df.drop_duplicates(subset=['album_id'])
            artists_df = pd.DataFrame.from_dict(artists)
            artists_df = artists_df.drop_duplicates(subset = ['artist_id'])
            songs_df = pd.DataFrame.from_dict(songs)   
            song_file = 'transformed_data/songs_data/songs_transformed_'+str(datetime.now())+'.csv'
            song_buffer = StringIO()
            songs_df.to_csv(song_buffer,index=False)
            songs_content = song_buffer.getvalue()
            s3.put_object(Bucket=Bucket,Key=song_file,Body=songs_content)
    
            album_file = 'transformed_data/albums_data/albums_transformed_'+str(datetime.now())+'.csv'
            album_buffer = StringIO()
            albums_df.to_csv(album_buffer,index=False)
            albums_content = album_buffer.getvalue()
            s3.put_object(Bucket=Bucket,Key=album_file,Body=albums_content)

            artists_file = 'transformed_data/artists_data/artists_transformed_'+str(datetime.now())+'.csv'
            artists_buffer = StringIO()
            artists_df.to_csv(artists_buffer,index=False)
            artists_content = artists_buffer.getvalue()
            s3.put_object(Bucket=Bucket,Key=artists_file,Body=artists_content)
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(source,Bucket,'raw_data/processed/'+key.split('/')[-1])
        s3_resource.Object(Bucket,key).delete()
        
