#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import libraries
import pandas as pd
import json
import requests
from sqlalchemy import create_engine
import psycopg2


# # Extraction

# In[2]:


url = 'https://itunes.apple.com/search?term=game+of+thrones&media=tvShow'  


# In[3]:


source = requests.get(url)


# In[4]:


#store result as json 
response = source.json()


# In[5]:


response


# In[6]:


#create pandas dataframe to store the data
df = pd.DataFrame(columns = ['season', 'track_name', 'track_time_in_millis', 'release_date', 'track_view_url', 'track_price', 'short_decription'])


# In[7]:


movie = response['results']
for results in movie:
    season = results['collectionName']
    track_name = results['trackName']
    track_time_in_millis = results['trackTimeMillis']
    release_date = results['releaseDate']
    track_view_url = results['trackViewUrl']
    track_price = results['trackPrice']
    short_decription = results['shortDescription']
    print(season)
    print(track_name)
    print(track_time_in_millis)
    print(release_date)
    print(track_view_url)
    print(track_price)
    print(short_decription)
    #sava as pandas dataframe
    df = df.append({'season':season,
                     'track_name': track_name,
                     'track_time_in_millis': track_time_in_millis,
                     'release_date': release_date,
                     'track_view_url': track_view_url,
                     'track_price': track_price,
                     'short_decription': short_decription}, ignore_index=True)


# In[8]:


df.head()


# # Transforming

# In[9]:


#spliting the season column to get only the seasons
df[['title', 'season']] = df['season'].str.split(',', 2, expand=True)
df.head()


# In[10]:


#delete title column
del df['title']
df.head()


# In[11]:


#covert track_time_in_millis column to minutes
# 1 millisecond = 0.00001667
df['track_time_in_millis'] = df['track_time_in_millis'] * 0.00001667
df.head()


# In[12]:


#rename track_time_in_millis to track_time_in_min
df.rename(columns = {'track_time_in_millis': 'track_time_in_min'}, inplace = True)
df.head()


# In[13]:


#sort release_date in ascending order to get tracks first released
df = df.sort_values(by = 'release_date')
df.head()


# # Loading into postgresql database

# In[14]:


# connecting to postgresql
engine = create_engine('postgresql://username:password@localhost:5432/Challenge')


# In[15]:


#saving to postgresql database(Challenge)
df.to_sql('game_of_thrones', engine, if_exists= 'replace', index=False)


# In[16]:


#load data from postgresql using pandas
pd.read_sql('game_of_thrones', engine, index_col= 'season')


# In[17]:


#save dataset to csv
df.to_csv('game_of_thrones.csv', index=False)


# In[ ]:




