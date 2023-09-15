import re
import os
import logging
import threading
from datetime import datetime
from typing import Dict, Tuple, Union
from queue import Queue, PriorityQueue, Empty
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm

from src.utils import get, write, multi_queues, each_task

PBAR_LOCK = threading.Lock()


class US10YRCrawler:

    def __init__(self, req_info: Dict[str, str], save_fp: str,
                 dates: Tuple[datetime, datetime]) -> None:
        """Constructor

        :param req_info: a dict of urls and params
        :param save_fp: the save path
        :param dates: [start, end)
        """

        self.req_info = req_info
        self.save_fp = save_fp
        self.dates = dates

        self.finished = pd.Series()
        self.queues = multi_queues(PriorityQueue(), Queue(), Queue())
        self.pbar = tqdm(total=1, unit='code', desc='Saving')

        self.load_resume()
        self.queues.crawler.put(
            [0, each_task(self.get_data, ['US10YR.OTC'], 0)])

    def load_resume(self):
        """Load finished tasks"""

        if not os.path.exists(self.save_fp):
            return

        data_df = pd.read_csv(self.save_fp,
                              encoding='utf-8-sig',
                              usecols=['date'])
        data_df['date'] = pd.to_datetime(data_df['date'])
        data_df[(data_df['date'] >= self.dates[0])
                & (data_df['date'] < self.dates[1])]

        if not data_df.empty:
            self.finished = data_df['date']

    def assign_tasks(self, num_crawler: int, max_retry: int = 3) -> None:
        """Start the thread pool and submit tasks

        :param num_crawler: the number of crawler threads
        :param max_retry: the limit of retries
        """

        logging.getLogger(__name__).info('Thread Pool starts %d threads...',
                                         num_crawler + 1)

        with ThreadPoolExecutor(max_workers=num_crawler + 1) as pool:
            for _ in range(num_crawler):
                pool.submit(self.do_task, max_retry=max_retry)
            pool.submit(self.write_result)

        logging.getLogger(__name__).info('%d/%d stocks\' data are retrieved.',
                                         self.pbar.n, self.pbar.total)
        self.pbar.close()

    def do_task(self, max_retry: int = 3) -> None:
        """Do tasks in the crawler queue

        :param max_retry: the limit of retries
        """

        thread_id = threading.get_ident()
        self.queues.other.put(thread_id)
        logging.getLogger(__name__).info('Crawler Thread %d starts...',
                                         thread_id)

        while True:
            try:
                prior, task = self.queues.crawler.get(timeout=10)
                result = task.func(*task.args)
                task = task._replace(tries=task.tries + 1)

                if result is None:

                    if task.tries >= max_retry:  # exceeds max retries
                        logging.getLogger(__name__).error(
                            '%s(*%s) fails %d times. Stop retrying!',
                            task.func.__name__, str(task.args), max_retry)
                        continue

                    logging.getLogger(
                        __name__).warning(  # continue with lower prior
                            '%s(*%s) fails %d times. Continue retrying...',
                            task.func.__name__, str(task.args), task.tries)

                    prior += 2
                    self.queues.crawler.put([prior, task])
                    continue

                result = result[~result['date'].isin(self.finished)]
                self.queues.writer.put(result)  # add to write

            except Empty:
                break

            except Exception as exc:
                logging.getLogger(__name__).exception(exc)
                logging.getLogger(__name__).error('%s(*%s)',
                                                  task.func.__name__,
                                                  str(task.args))
                break

        self.queues.other.get()
        logging.getLogger(__name__).info('Crawler Thread %d stopped.',
                                         thread_id)

    def write_result(self) -> None:
        """Write results"""

        thread_id = threading.get_ident()
        logging.getLogger(__name__).info('Writer Thread %d starts...',
                                         thread_id)

        while True:
            try:
                result = self.queues.writer.get(timeout=30)

                if result.empty:  # no data
                    with PBAR_LOCK:
                        self.pbar.total -= 1
                        self.pbar.refresh()
                else:
                    if write(result, self.save_fp):  # write success
                        with PBAR_LOCK:
                            self.pbar.update(1)

            except Empty:
                if self.queues.other.empty():  # all crawlers stopped
                    break

            except Exception as exc:
                logging.getLogger(__name__).exception(exc)
                break

        logging.getLogger(__name__).info('Writer Thread %d stopped', thread_id)

    def get_data(self, code: str) -> Union[pd.DataFrame, None]:
        """Get the specified code's history

        :param code: a specified code
        :return: a dataframe of its history
        """

        params = self.req_info['params']
        params['prod_code'] = code
        params['tick_count'] = min(4497, (self.dates[1] - self.dates[0]).days)

        content = get(url=self.req_info['url'], params=params)

        if content is None:
            return None

        try:
            cols = map(lambda each: re.sub(r'_?px_?', '', each),
                       content['data']['fields'])
            data = pd.DataFrame(
                content['data']['candle']['US10YR.OTC']['lines'])
            data.columns = cols

            data.rename(columns={'tick_at': 'date'}, inplace=True)
            data = data[[
                'date', 'open', 'close', 'high', 'low', 'change', 'change_rate'
            ]]

            data['date'] = data['date'].apply(datetime.fromtimestamp)
            data = data[(data['date'] >= self.dates[0])
                        & (data['date'] < self.dates[1])]

            return data

        except KeyError as key_err:
            logging.getLogger(__name__).exception(key_err, exc_info=False)

        return None
