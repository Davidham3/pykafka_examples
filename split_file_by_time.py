# -*- coding:utf-8 -*-
import os
from datetime import datetime
from datetime import timedelta
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def readFile(filename):
    '''read files by line

    Parameters
    ----------
    filename: str
        the path of the file

    Returns
    ----------
    data: str
        one line of the file
    '''
    with open(filename, 'r') as f:
        data = f.readline()
        while data:
            yield data
            data = f.readline()

def split_line(line):
    '''split the line by '\x01' and clean the data

    Parameters
    ----------
    line: str
        one line of a file

    Returns
    ----------
    t: list
        a list contains the elements of the line, the last element is a datetime type variable
    '''
    t = line.strip().split('\x01')
    if t[-1].endswith('.0'):
        t[-1] = t[-1][: t[-1].find('.')]
    t[-1] = datetime.strptime(t[-1], '%Y-%m-%d %H:%M:%S')
    return t

def generate_text(results):
    '''generate a piece of text to describe results

    Parameters
    ----------
    results: list
        a list contains lots of list, each one have 4 elements, the last one is a datetime object

    Returns
    ----------
    variable: str
        return a str which was interpolated by '\n' and '\t'
    '''
    for i in results:
        i[-1] = datetime.strftime(i[-1], '%Y-%m-%d %H:%M:%S')
    return '\n'.join('\t'.join(i) for i in results)

def output(filename, freq):
    '''split file into multiple chunks and return a generator

    Parameters
    ----------
    filename: str

    freq: str
        'second' means second, 'hour' means hour

    Returns
    ----------
    variable: generator(str)
        each element is a chunk of records which have same timestamps depends on "freq"
    '''
    freq_strformat = {'second': '%Y-%m-%d %H:%M:%S',
                      'hour': '%Y-%m-%d %H'}
    last_time = None
    results = []
    for line in readFile(filename):
        data = split_line(line)
        if last_time is None:
            last_time = data[-1].strftime(freq_strformat[freq])
            logger.info('capture data in %s'%(last_time))
            results.append(data)
        else:
            current_time = data[-1].strftime(freq_strformat[freq])
            if current_time == last_time:
                results.append(data)
            else:
                yield generate_text(results)
                results.clear()
                last_time = data[-1].strftime(freq_strformat[freq])
                logger.info('capture data in %s'%(last_time))
                results.append(data)

def time_continuous_output(filename, freq, start_time):
    '''output records by the second or by the hour

    Parameters
    ----------
    filename: str

    freq: str
        'second' means second, 'hour' means hour

    start_time: str
        %Y-%m-%d %H:%M:%S

    Returns
    ----------
    variable: generator(str)
        each element is a chunk of records which have same timestamps depends on "freq",
        but the output chunks' timestamps are continuous
    '''
    freq_strformat = {'second': '%Y-%m-%d %H:%M:%S',
                      'hour': '%Y-%m-%d %H'}
    if freq == 'second':
        delta, sleep_time = timedelta(seconds = 1), 1
    elif freq == 'hour':
        delta, sleep_time = timedelta(hours = 1), 3
    else:
        logger.error('freq is neither second nor hour, freq is %s'%(freq))
        return 

    last_time = datetime.strptime(start_time, freq_strformat['second']).strftime(freq_strformat[freq])
    for data in output(filename, freq):
        current_time = datetime.strptime(data[data.rfind('\t')+1:], freq_strformat['second'])\
                        .strftime(freq_strformat[freq])
        while current_time != last_time:
            yield ''
            time.sleep(sleep_time)
            last_time = (datetime.strptime(last_time, freq_strformat[freq]) + delta)\
                        .strftime(freq_strformat[freq])
        yield data
        time.sleep(sleep_time)
        last_time = (datetime.strptime(last_time, freq_strformat[freq]) + delta)\
                        .strftime(freq_strformat[freq])

if __name__ == '__main__':
    for i in time_continuous_output('smallUserBehaviorSortedByTime_part1.txt', 'hour', '2015-01-01 00:00:00'):
        print(i)
