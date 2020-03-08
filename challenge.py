import json
import pandas as pd
import numpy as np
# import regular expression
import re 
# allow Pandas to communicate wz SQL server
from sqlalchemy import create_engine
import psycopg2
# import my SQL pwd
from config import db_password
# To record runtime 
import time

def moviesETL(jason, csv1, csv2):
    # Read files
    file_dir = 'C:/Users/pingy/Desktop/Analysis Projects/Crowdfunding Analysis/Class/8. ETL Extract Transform Load/Movies-ETL'
    with open(f'{file_dir}/json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    kaggle_metadata = pd.read_csv('csv1')
    ratings = pd.read_csv('csv2')
    
    # Convert jason to dataframe
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
   
    # List comprehension
    wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]
    
    # a function to clean the data
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key] # store value fm movie[key] to alt_titles[key]
                movie.pop(key) # remove key-value pair fm movie
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles # append alt_titles as new key-value in movie

        # Merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name) # replace old key wz new key
    
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
    
        return movie
    
    # make a list of cleaned movies
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    # convert to dataframe
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    # remove columns that have too many nulls
    wiki_columns_to_keep =[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum()< len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    ## box_office column
    # Drop missing data, using dropna()
    box_office = wiki_movies_df['Box office'].dropna() 
    # define box_office not a string: lambda method
    box_office[box_office.map(lambda x: type(x) != str)]
    
    # Convert list to string for regex only works on str
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Some values are given as a range
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # regex for million or billion
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    # regex for “$123,456,789”
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    # Extract part of strings that match
    box_office.str.extract(f'({form_one}|{form_two})')
    
    # A func turn the extracted values into a numeric value
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            return value

        # otherwise, return NaN
        else:
            return np.nan    
        
    # Extract values fm box_office using str.extract; then apply parse_dollars to 1st column in DataFrame retned by str.extract
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # No longer need the Box Office column, so we’ll just drop it
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
        
    ## budget column
    # Create a budget variable
    budget = wiki_movies_df['Budget'].dropna() # drop missing data

    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # Remove values btw a dollar sign n a hyphen
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Parse budge values, changing "Budget" to "budget"
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # remove old date Budget
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    ## release_date column
    # Store non-null value, and also convert list to str
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ''.join(x) if type(x) == list else x)
    
    # Full month name, one- to two-digit day, four-digit yr (i.e., January 1, 2000) [123] because only 30, 31 days a month
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    # Four-digit yr, two-digit mo, two-digit day, with any separator (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d' # . is wild card to match anything
    # Full mo name, four-digit yr (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    # Four digit yr
    date_form_four = r'\d{4}'
    
    # new cloumn 'release_date' to store those dates 
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # remove old data column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
    ## running_time column
    # add hour form
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # convert to number wz to_numeric() method n fillna() to chg NaNs to 0
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    # Convert hr capture grps n min capture grps to mins if pure mins capture grp is 0
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    # Drop old Running time data
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    ## Convert kaggle_metadata (movies_metadata.csv) to the right data type
    # we’ll only keep rows where adult is False, and then drop the “adult” column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # Conver 'video' as boolen, and assign it back to video column
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # Conver other data types
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    ## Convert ratings (ratings.csv) to the right data type
    # Convert timestamp to datetime, unit is second
    pd.to_datetime(ratings['timestamp'], unit='s')
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    
    # Merge Wikipedia and Kaggle Metadata
    # use suffixes parameter to identify redundant cols
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # drop the outliner of release_date
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki']> '1996-01-01') & (movies_df['release_date_kaggle']< '1965-01-01')].index)
    
    # drop some wiki cols since no null in kaggle cols
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    # a func to fill kaggle missing data wz wiki data
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # drop video col, since only 1 value
    movies_df.drop('video', axis=1, inplace=True)
    
    # reorder columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on']]
    
    # rename columns
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'}, axis='columns', inplace=True)
    
    ## Transform and Merge Rating Data
    # pivot data so that movieId is index, cols will be the rating values, n rows is counts fr each rating value
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    
    # left merge to movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # fill 0 if cols fm rating_counts values are missing
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    ## Import movies_df to SQL as movies table
    # Connecting String
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    # Create database engine
    engine = create_engine(db_string)
    # Save movies_df to a SQL table
    movies_df.to_sql(name='movies', con=engine)
    
    ## Import the Ratings Data
    for data in pd.read_csv(f'csv2', chunksize=1000000):
    data.to_sql(name='ratings', con=engine, if_exists='append')
    
    
# Run the function 
moviesETL(wikipedia.movies.json, movies_metadata.csv, ratings.csv)