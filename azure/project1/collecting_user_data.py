import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

def get_playlist_tracks(username,playlist_id):
    results = sp.user_playlist_tracks(username,playlist_id)
    tracks = results['items']
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])
    return tracks

all_tracks = get_playlist_tracks('31vcahs2gz73srtl6su7ivnj2jau?si=aec85a3b991c4c30', '6a6PeS5kG2AuEE2lM7JLaE')
all_track_ids = []
for i in all_tracks:
    all_track_ids.append(i['track']['id'])

def get_track_features(id):
    metadata = sp.track(id)
    features = sp.audio_features(id)
    # metadata
    name = metadata['name']
    album = metadata['album']['name']
    artist = metadata['album']['artists'][0]['name']
    release_date = metadata['album']['release_date']
    length = metadata['duration_ms']
    popularity = metadata['popularity']

    # audio features
    acousticness = features[0]['acousticness']
    danceability = features[0]['danceability']
    energy = features[0]['energy']
    instrumentalness = features[0]['instrumentalness']
    liveness = features[0]['liveness']
    loudness = features[0]['loudness']
    speechiness = features[0]['speechiness']
    tempo = features[0]['tempo']
    time_signature = features[0]['time_signature']

    track = [id, name , album, artist, release_date, length, popularity, danceability, acousticness, energy, instrumentalness, liveness, loudness, speechiness, tempo, time_signature]
    return track

tracks = []
for i in range(len(all_track_ids)):
  track = get_track_features(all_track_ids[i])
  tracks.append(track)
df = pd.DataFrame(tracks, columns = ['id', 'name', 'album', 'artist', 'release_date', 'length', 'popularity', 'danceability', 'acousticness', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'tempo', 'time_signature'])

file_path = '/home/system/big_data_project/spotify_data.csv'

df.to_csv(file_path, index=False)
