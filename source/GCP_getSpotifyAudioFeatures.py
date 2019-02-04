"""
Created on Fri Nov  2 10:41:30 2018
This program will retrieve Spotify ID/URI given an ArtistID and Song Title
And will retrieve the Audio Track features, and Audio Analysis features using the Spotify URI
@author: ayeshamendoza
"""
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import config as cfg

client_id = cfg.client_id
client_secret = cfg.client_secret
password = cfg.password


##Query artist to get track IDs

def get_Spotify_trackID(title, artist):
    ''' this function send query request to Spotify by title and artist name
        and will return corresponding the track ID'''
    
    urilist = []
    artistid = ''
    error = 0
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    sp.trace=False
    search_query = title + ' ' + artist
    
    try:
        result = sp.search(search_query)
        #find a song that matches the title and artist from the query result
        for i in result['tracks']['items']:
            if (i['artists'][0]['name'] == artist) and (i['name'] == title):
                urilist.append(i['uri'])
                artistid = i['artists'][0]['id']
                return (urilist, artistid, error)
                break

    except:
        error = 999
        print('get_Spotify_trackID error processing : ', title, artist)
    
    
                
    return (urilist, artistid, error)
    
### Getting audio data:

def getFeatures(uri):
    '''this function will send query request to Spotify for audio features for the specified uri'''
    
    features = []
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    sp.trace = False
    error = 0
    try:
        features = sp.audio_features(uri)
    except:
        error = 888
        print('getFeatures error processing : ' ,uri)
    
    return (features, error)

def getAudioAnalysis(uri):
    '''this function will send query request to Spotify for audio analysis for the specified uri'''
    
    features = []
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    sp.trace = False
    error = 0
    try:
        features = sp.audio_analysis(uri)
    except:
        error = 888
        print('getFeatures error processing : ' ,uri)
    
    return (features, error)

def processAudioDict(audioAnalysis):
    '''This function will process the Audio Analysis Features and save into a dictionary format for
       pitch_mean, pitch_med, timbre_mean, timbre_med, bar length, beat length and segment length'''

    audioDict = {}
    

    sum_pitch_mean = 0
    sum_pitch_median = []
    song_pitch_mean = 0
    song_pitch_med = 0

    sum_timbre_mean = 0
    sum_timbre_median = []
    song_timbre_mean = 0
    song_timbre_med = 0

    seg_len = len(audioAnalysis['segments']) 
    
    audioDict['bar_len'] = len(audioAnalysis['bars'])
    audioDict['beat_len'] = len(audioAnalysis['beats'])
    audioDict['seg_len'] = seg_len
    
    for i in range(seg_len):
 
        pitches = np.array(audioAnalysis['segments'][i]['pitches'])
        timbre = np.array(audioAnalysis['segments'][i]['timbre'])
        mean_pitch = np.mean(pitches)
        med_pitch = np.median(pitches)
        sum_pitch_mean += mean_pitch
        sum_pitch_median.append(med_pitch)
    
        mean_timbre = np.mean(timbre)
        med_timbre = np.median(timbre)
        sum_timbre_mean += mean_timbre
        sum_timbre_median.append(med_timbre)
    
    song_pitch_mean = sum_pitch_mean / seg_len
    song_timbre_mean = sum_timbre_mean / seg_len
    song_pitch_med = np.median(np.array(sum_pitch_median))
    song_timbre_med = np.median(np.array(sum_timbre_median))

    audioDict['pitch_mean'] = song_pitch_mean
    audioDict['pitch_med'] = song_pitch_med
    audioDict['timbre_mean'] = song_timbre_mean
    audioDict['timbre_med'] = song_timbre_med
    
    return audioDict


def getArtist(artistid):
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    sp.trace=False
    err = 0
    genres = []

    try:
        result = sp.artist(artistid)
        genres = result['genres']
        #print(genres)
    except:
        print('error')
        err = 555
        
    return (genres, err)


#Read input file
songsDF = pd.read_csv('../data/gcp_songs_input.csv').head(500)
songsDF.drop('Unnamed: 0', axis=1, inplace=True)
songsDF.drop_duplicates(subset=['artist_id', 'title'], inplace=True)

songsDF.reset_index(inplace=True)

x = 0
dflen = len(songsDF)

for i, row in enumerate(songsDF.itertuples(), start=0):
    
    # get track id:
    urilist, artistid, err = get_Spotify_trackID(row.title, row.artist_name)
    if err > 0:
        print('...breaking')
        songsDF.to_pickle('../data/spotify_bkp.pkl')
        #break
    else: 
        if len(urilist) == 1:
            #print('getting Features....')
            uri = urilist[0].split(':')[2]
            songFeatures, err = getFeatures(uri)
            if err > 0:
                print('breaking....')
                songsDF.to_pickle('spotify_bkp.pkl')
                #break
            else:
                songsDF.set_value(i,'spotifyURI',uri)
                songsDF.set_value(i,'songFeatures',songFeatures)
                
                audioAnalysis, err = getAudioAnalysis(uri)
                if err > 0:
                    print('breaking....')
                    songsDF.to_pickle('spotify_1118_bkp.p')
                else:
                    audioDict = processAudioDict(audioAnalysis)
                    songsDF.set_value(i,'bar_len',audioDict['bar_len'])
                    songsDF.set_value(i,'beat_len',audioDict['beat_len'])
                    songsDF.set_value(i,'seg_len',audioDict['seg_len'])
                    songsDF.set_value(i,'pitch_mean',audioDict['pitch_mean'])
                    songsDF.set_value(i,'pitch_med',audioDict['pitch_med'])
                    songsDF.set_value(i,'timbre_mean',audioDict['timbre_mean'])
                    songsDF.set_value(i,'timbre_med',audioDict['timbre_med'])
                    
                    #get song Genre
                    genre, err = getArtist(artistid)
                    if err > 0:
                        print('breaking....')
                        artistDF.to_pickle('spotifyGenre_1122_bkp.p')
                    #break
                    else:
                        genre_count = {'country': 0, 'pop': 0, 'other':0}
                
                    for tag in genre:
                        str1 = tag.strip().lower()
                    
                        if (str1.find('pop')) != -1:  #string contains pop
                            genre_count['pop'] += 1
                        elif (str1.find('country')) != -1:  #string contains country
                            genre_count['country'] += 1
                        else:
                            genre_count['other'] += 1                    
                    
                    songsDF.set_value(i,'SpotifyGenre', max(genre_count, key=lambda key: genre_count[key]))
                    songsDF.set_value(i,'country_count', genre_count['country'])
                    songsDF.set_value(i,'pop_count', genre_count['pop'])
                    songsDF.set_value(i,'other_count', genre_count['other'])                  
                    songsDF.set_value(i,'spotifyArtistID',artistid)
                    
    if (x < dflen) & (x % 200 == 0):
        print('processing row {}'.format(i), row.title, row.artist_name)
        songsDF.to_pickle('spotify_1118_bkp.p')
    x += 1
    
#format song features
null_Features_idx = songsDF.index[songsDF['songFeatures'].isnull()]
songsDF.drop(null_Features_idx, inplace=True)

for i, row in enumerate(songsDF.itertuples(),start=0):
    
    #print(i, type(row.songFeatures[0]), row.songFeatures[0])
   # print(row.artist_name_x, row.title_y, row.songFeatures)
    songfeatures = row.songFeatures[0]
    
    #save features as individual column names
    try:
        for key, value in songfeatures.items():
            songsDF.set_value(i, key, value)
    except:
        print('error processing features for row {}'.format(i))
        #print(row.artist_name_x, row.title_y)
        #print(key, values)


none_Features_idx = songsDF.index[songsDF['valence'].isnull()]
songsDF.drop(none_Features_idx, inplace=True)

songsDF = songsDF[['artist_id', 'tags', 'track_id', 'title', 'song_id', 'release',
       'artist_mbid', 'artist_name', 'duration', 'artist_familiarity',
       'artist_hotttnesss', 'year', 'track_7digitalid', 'shs_perf', 'shs_work',
       'spotifyURI', 'bar_len', 'beat_len', 'seg_len',
       'pitch_mean', 'pitch_med', 'timbre_mean', 'timbre_med', 'danceability',
       'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
       'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'id', 'uri',
       'track_href', 'analysis_url', 'duration_ms', 'time_signature']]

from sqlalchemy import create_engine

def insert_db(df, name, engine):
    df.to_sql(name, engine, if_exists="replace")

    return None

import pickle

def archive_data(df, name):
    "Stores the data locally as a pickle file"

    pickle.dump(df, open('../data/{}.pkl'.format(name), 'wb'))

    return None


eng_str = 'postgresql://postgres:{}@35.239.82.136:5432/postgres'.format(password)

try:
    print('trying')
    engine = create_engine(eng_str)
    print('Database found')
except:
    print('No database available.')
    
insert_db(songsDF, 'sp_audio_features', engine)
    
query = """
SELECT *
   FROM sp_audio_features
"""

#df = pd.read_sql(query, engine)
#print(df.head())

songsDF.to_csv('../data/sp_audio_features.csv', index=False)

print('process completed.')
