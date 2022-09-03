import logging
import os
from twitter import Twitter
from utils import format_reddit_json_file, create_reddit_sentiment_file, create_twitter_sentiment_file, create_aggregate_file

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


coin_configuration = {
  'ada' : {
    'start_date' : '2017-11-09',
    'end_date' : '2022-07-05',
    'hash_tag' : 'ADA',
    'output_file' : 'data/ada/twitter/pickled/ada-tweets.pickle',
    'json_dir' : 'data/ada/twitter/json/',
    'reddit_dir' : 'data/ada/reddit/',
  },
  'avax' : {
    'start_date' : '2020-07-13',
    'end_date' : '2022-07-05',
    'hash_tag' : 'AVAX',
    'output_file' : 'data/avax/twitter/pickled/avax-tweets.pickle',
    'json_dir' : 'data/avax/twitter/json/',
    'reddit_dir' : 'data/avax/reddit/',
  },
  'dot' : {
    'start_date' : '2020-08-20',
    'end_date' : '2022-07-05',
    'hash_tag' : 'DOT',
    'output_file' : 'data/dot/twitter/pickled/dot-tweets.pickle',
    'json_dir' : 'data/dot/twitter/json/',
    'reddit_dir' : 'data/dot/reddit/',
  },
  'matic' : {
    'start_date' : '2019-04-28',
    'end_date' : '2022-07-05',
    'hash_tag' : 'matic',
    'output_file' : 'data/matic/twitter/pickled/matic-tweets.pickle',
    'json_dir' : 'data/matic/twitter/json/',
    'reddit_dir' : 'data/matic/reddit/',
  },
  'sol' : {
    'start_date' : '2020-04-10',
    'end_date' : '2022-07-05',
    'hash_tag' : 'solana',
    'output_file' : 'data/sol/twitter/pickled/sol-tweets.pickle',
    'json_dir' : 'data/sol/twitter/json/',
    'reddit_dir' : 'data/sol/reddit/',
  },
}


coin_test_configuration = {
  'ada' : {
    'start_date' : '2017-11-09',
    'end_date' : '2017-11-10',
    'hash_tag' : 'ADA',
    'output_file' : 'data/ada/twitter/pickled/ada-tweets.pickle',
    'json_dir' : 'data/ada/twitter/json/',
    'reddit_dir' : 'data/ada/reddit/',
  },
  'avax' : {
    'start_date' : '2020-07-13',
    'end_date' : '2020-07-14',
    'hash_tag' : 'AVAX',
    'output_file' : 'data/avax/twitter/pickled/avax-tweets.pickle',
    'json_dir' : 'data/avax/twitter/json/',
    'reddit_dir' : 'data/avax/reddit/',
  },
  'dot' : {
    'start_date' : '2020-08-20',
    'end_date' : '2020-08-21',
    'hash_tag' : 'DOT',
    'output_file' : 'data/dot/twitter/pickled/dot-tweets.pickle',
    'json_dir' : 'data/dot/twitter/json/',
    'reddit_dir' : 'data/dot/reddit/',
  },
  'matic' : {
    'start_date' : '2019-04-28',
    'end_date' : '2019-04-28',
    'hash_tag' : 'matic',
    'output_file' : 'data/matic/twitter/pickled/matic-tweets.pickle',
    'json_dir' : 'data/matic/twitter/json/',
    'reddit_dir' : 'data/matic/reddit/',
  },
  'sol' : {
    'start_date' : '2020-04-10',
    'end_date' : '2020-04-11',
    'hash_tag' : 'solana',
    'output_file' : 'data/sol/twitter/pickled/sol-tweets.pickle',
    'json_dir' : 'data/sol/twitter/json/',
    'reddit_dir' : 'data/sol/reddit/',
  },
}

def download_twitter_data():
    for name, configuration in coin_test_configuration.items():
      twitter = Twitter(name, configuration)
      # logger.info(f"Starting data download for the coin with hashtage-{configuration['hash_tag']} \
      #   for the period from {configuration['start_date']} till {configuration['end_date']}")
      
      # twitter.download_tweets()
      
      # logger.info(f"Completed data download for the coin with hashtage-{configuration['hash_tag']} \
      #   for the period from {configuration['start_date']} till {configuration['end_date']}")
      
      logger.info(f"Creating json file.......")
      
      twitter.pickle_to_json()
      
      logger.info(f"Completed json file creation")

def create_reddit_sentiment_data():
    for name,configuration in coin_configuration.items():
        logger.info(f'creating reddit sentiment data file for {name}')
        output_file = f'data/{name}/reddit/{name}-reddit-sentiment.csv'
        input_file = f'data/{name}/reddit/{name}-reddit.json'
        create_reddit_sentiment_file(input_file, output_file)

def create_twitter_sentiment_data():
    for name,configuration in coin_configuration.items():
      if name != 'dot':
        logger.info(f'creating twitter sentiment data file for {name}')
        output_file = f'data/{name}/twitter/json/{name}-twitter-sentiment.csv'
        input_file = f'data/{name}/twitter/json/{name}-tweets.json'
        create_twitter_sentiment_file(input_file, output_file)

def create_merged_file():
  """This method is used to aggregate columns from 
  historical price data csv, reddit sentiment scores and twitter sentiment scores
  for analysis"""
  for name, configuration in coin_configuration.items():
    if name != 'dot' and name != 'sol':
      create_aggregate_file(name)


def format_reddit_data():
    for name, configuration in coin_configuration.items():
      input_file = f'{configuration["reddit_dir"]}{name}-reddit-raw.json'
      output_file = f'{configuration["reddit_dir"]}{name}-reddit.json'
      format_reddit_json_file(input_file, output_file)


if __name__ == '__main__':
    # download_twitter_data()
    # format_reddit_data()
    # create_reddit_sentiment_data()
    # create_twitter_sentiment_data()
    create_merged_file()
    