import pandas as pd
from sklearn.preprocessing import LabelEncoder
## Reading the CSV
file_path = "/home/system/big_data_project/spotify_data.csv"
df = pd.read_csv(file_path)## Removing Duplicates
df.drop_duplicates(inplace=True)

### Encoding Artists as Categorical Variables
# Initialize LabelEncoder
label_encoder = LabelEncoder()
df['artist_id'] = label_encoder.fit_transform(df['artist'])

## Creating a dataframe to store track data
track_columns = ['id', 'name', 'album', 'release_date', 'artist_id']
track_data = df[track_columns]### Storing Track Data to a csv
track_data.to_csv('/home/system/big_data_project/spotify_track_data.csv')

### Creating the dataframe to store track features data
track_features_columns =['id', 'length', 'popularity', 'danceability', 'acousticness', 'energy', 'liveness', 'speechiness', 'tempo']
track_features_data = df[track_features_columns]

### Storing Track Features Data to a csv
track_features_data.to_csv('/home/system/big_data_project/spotify_track_features_data.csv')

## Creating the Artist Tables
artist_data = df[['artist_id', 'artist']]
artist_data.drop_duplicates(inplace=True)

### Storing Artist Data to a csv
artist_data.to_csv('/home/system/big_data_project/spotify_artist_data.csv'). can you summarise this code, do not use any code in the output.
