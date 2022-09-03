import json
import datetime
import logging
from functools import reduce
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import re
import string
import nltk
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from wordcloud import WordCloud,STOPWORDS
from nltk.util import ngrams
from nltk import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer


# Get or create a logger
log_format = '%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
logger = logging.getLogger(__name__) 

# set log level
logger.setLevel(logging.DEBUG)

# define file handler and set formatter
file_handler = logging.StreamHandler()
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        logger.info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


def format_reddit_json_file(file_name=None, output_file='formatted.json'):
    if file_name is None:
        print('Provide a input file to be formatted')
    
    data = []

    with open(file_name) as f:
        for line in f:
            data.append(json.loads(line))

    df = pd.DataFrame.from_records(data)
    
    df.to_json(output_file)

# Text preprocessing functions
def replace_none_values(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x : x or '')
    
def remove_handlers(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x:re.sub('@[^\s]+','',x))

def remove_urls(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x:re.sub(r"http\S+", "", x))

def remove_special_characters(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x:' '.join(re.findall(r'\w+', x)))

def remove_single_characters(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x:re.sub(r'\s+[a-zA-Z]\s+', '', x))

def remove_multiple_spaces(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x:re.sub(r'\s+', ' ', x, flags=re.I))

def to_date(dataframe, column):
    dataframe[column] = pd.to_datetime(dataframe[column]).dt.date


def load_reddit_columns(file_name: str) -> pd.DataFrame:
    dataframe = pd.read_json(file_name)
    # remove irrelevant columns
    dataframe.drop(columns=['_type', 'author', 'url', 'link', 'selftext', 'id', 'parentId', 'subreddit'], inplace=True)
    replace_none_values(dataframe, 'body')
    replace_none_values(dataframe, 'title')
    dataframe["content"] = dataframe['body'].astype(str) + dataframe['title'].astype(str)
    dataframe.drop(columns=['body', 'title'], inplace=True)
    return dataframe

def pre_process_text(dataframe: pd.DataFrame, column_name: str) -> None:
    remove_handlers(dataframe, column_name)
    remove_urls(dataframe, column_name)
    remove_special_characters(dataframe, column_name)
    remove_single_characters(dataframe, column_name)
    remove_multiple_spaces(dataframe, column_name)


def add_sentiment_scores(dataframe, column, 
                         pos_column_name='pos_score', 
                         neu_column_name='neu_score',
                         neg_column_name='neg_score'):
    sid = SIA()
    dataframe['sentiments'] = dataframe[column].apply(lambda x: sid.polarity_scores(' '.join(re.findall(r'\w+',x.lower()))))
    dataframe[pos_column_name]   = dataframe['sentiments'].apply(lambda x: x['pos']+1*(10**-6)) 
    dataframe[neu_column_name]    = dataframe['sentiments'].apply(lambda x: x['neu']+1*(10**-6))
    dataframe[neg_column_name]   = dataframe['sentiments'].apply(lambda x: x['neg']+1*(10**-6))
    dataframe.drop(columns=['sentiments'],inplace=True)

def create_reddit_sentiment_file(input_file: str, file_name: str) -> None:
    # json_files = [file for file in listdir(mypath) if isfile(join(mypath, file))]
    # for json_file in json_files:
    dataframe = load_reddit_columns(input_file)
    pre_process_text(dataframe, 'content')
    to_date(dataframe, 'created')
    add_sentiment_scores(dataframe, 'content',
                    pos_column_name='r_pos_score', 
                    neu_column_name='r_neu_score',
                    neg_column_name='r_neg_score')
    dataframe = dataframe.groupby('created').mean()
    dataframe.to_csv(file_name, mode='wb', header=True)

def load_twitter_columns(file_name: str) -> pd.DataFrame:
    dataframe = pd.read_json(file_name)
    # select relevant columns
    dataframe = dataframe[['date', 'content']]
    return dataframe

def create_twitter_sentiment_file(input_file: str, file_name: str) -> None:
    # json_files = [file for file in listdir(mypath) if isfile(join(mypath, file))]
    # for json_file in json_files:
    dataframe = load_twitter_columns(input_file)
    pre_process_text(dataframe, 'content')
    to_date(dataframe, 'date')
    add_sentiment_scores(dataframe, 'content',
                    pos_column_name='t_pos_score', 
                    neu_column_name='t_neu_score',
                    neg_column_name='t_neg_score')
    dataframe = dataframe.groupby('date').mean()
    dataframe.to_csv(file_name, mode='wb', header=True)

def create_aggregate_file(name: str) -> None:
    """This method is used to aggregate columns from 
    historical price data csv, reddit sentiment scores and twitter sentiment scores
    for analysis"""
    price_data = pd.read_csv(f'data/{name}/{name.upper()}-USD.csv')
    reddit_sent_data = pd.read_csv(f'data/{name}/reddit/{name}-reddit-sentiment.csv')
    twitter_sent_data = pd.read_csv(f'data/{name}/twitter/json/{name}-twitter-sentiment.csv')
    price_data.rename(columns={'Date': 'date', 'Open': 'open', 'High':'high', 
                              'Low': 'low', 'Close':'close', 'Adj Close':'adj_close'}, inplace=True)
    reddit_sent_data.rename(columns={'created': 'date'}, inplace=True)

    price_data.date = pd.to_datetime(price_data.date)
    reddit_sent_data.date = pd.to_datetime(reddit_sent_data.date)
    reddit_sent_data.date = reddit_sent_data.date.apply(lambda x: x + datetime.timedelta(days=1))

    twitter_sent_data.date = pd.to_datetime(twitter_sent_data.date)
    twitter_sent_data.date = twitter_sent_data.date.apply(lambda x: x + datetime.timedelta(days=1))

    data_frames = [price_data, reddit_sent_data, twitter_sent_data]


    merged_dataframes = reduce(lambda  left,right: pd.merge(left,right,on=['date'],
                                          how='outer'), data_frames).fillna('')

    merged_dataframes.to_csv(f'data/{name}/{name}-aggregate.csv', mode='wb', header=True)

if __name__ == '__main__':
    # format_reddit_json_file('../avax-tweets.json', '../avax-tweets-formatted.json')
    # print('finished conversion')
    # df = pd.read_json('../avax-tweets-formatted.json')
    # print(df.columns)
    # format_reddit_json_file('data/ada/reddit/ada-reddit-raw.json', 'data/ada/reddit/ada-reddit.json')
   pass
