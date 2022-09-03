from time import time
import snscrape.modules.twitter as sntwitter
import pickle
import pandas as pd
from snscrape.modules.twitter import TwitterSearchScraper
import json
import logging
import os
from dataclasses import dataclass


# Get or create a logger
logger = logging.getLogger(__name__)  

# set log level
logger.setLevel(logging.DEBUG)


def configure_log(log_filename: str='logs/app.log') -> None:
    # create log_file if it doesn't exists
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    # define file handler and set formatter
    file_handler = logging.FileHandler(log_filename, mode="w", encoding=None, delay=False)
    log_format = '%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    formatter    = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)

    # add file handler to logger
    logger.addHandler(file_handler)


@dataclass
class Twitter():
    name: str
    configuration: dict

    BATCH_SIZE = 3_00_000

    def download_tweets(self) -> None:
        configure_log('logs/twitter.log')
        os.makedirs(os.path.dirname(self.configuration['output_file']), exist_ok=True)
        start_date = self.configuration['start_date']
        end_date = self.configuration['end_date']
        hash_tag = self.configuration['hash_tag']

        with open(self.configuration['output_file'], 'wb+') as file:
            search_query = f'#{hash_tag} since:{start_date} until:{end_date}'

            for i,tweet in enumerate(TwitterSearchScraper(search_query).get_items()):
              try:
                pickle.dump(tweet, file)
                logger.info(f'Fetched {hash_tag}:tweet for time: {tweet.date}')
              except Exception:
                logger.error(f'Data fetching failed. Change the dates and start over from last fetched date')


    def pickle_to_json(self) -> None:
        json_objects = []
        batch = 1
        os.makedirs(os.path.dirname(self.configuration['json_dir']), exist_ok=True)

        with open(self.configuration['output_file'], 'rb') as pickled_file:
            while True:
                try:
                  json_object = pickle.load(pickled_file)
                  json_objects.append(json_object)
                  print(json_object)

                  if len(json_objects) == self.BATCH_SIZE:
                      self.write_to_json_file(json_objects, batch)
                      logger.info(f'Finished processing batch {batch}')
                      batch += 1
                      json_objects.clear()
                except EOFError:
                    break
                finally:
                  if len(json_objects) != 0:
                    self.write_to_json_file(json_objects, batch)
    
    
    def write_to_json_file(self, json_objects: list, batch: int) -> None:
      df = pd.DataFrame(json_objects)
      file_name = f"{self.configuration['json_dir']}/{batch}_{self.name+'.json'}"
      df.to_json(file_name )