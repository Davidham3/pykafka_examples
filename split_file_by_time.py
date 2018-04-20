# -*- coding:utf-8 -*-
import os
from datetime import datetime
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

def output(filename, freq, continuous = False):
    '''
    Parameters
    ----------
    filename: str
    freq: str
        'second' means second, 'hour' means hour
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

if __name__ == '__main__':
    for i in output('smallUserBehaviorSortedByTime_part1.txt', 'hour'):
        print(i)
