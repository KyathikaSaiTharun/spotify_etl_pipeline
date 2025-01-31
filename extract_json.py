import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime
def lambda_handler(event, context):
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlist = "https://open.spotify.com/playlist/6VOedaf3eNWDOVpa9Qdlvg"
    playlist_uri = playlist.split('/')[-1]
    data = sp.playlist_tracks(playlist_uri)

    file = 'spotify_raw'+str(datetime.now())+'.json'

    client = boto3.client('s3')
    client.put_object(Bucket='spotify-etl-tharun',Key='raw_data/to_be_processed/'+file,Body=json.dumps(data))
