# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import pymysql
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('consumer')

MYSQL_CONFIGURATION = {
    'host': '192.168.56.121',
    'user': 'root',
    'password': 'cluster',
    'port': 3306
}

def create_table():
    '''
    check if the database experiment2 exists, check if the table USER_BEHAVIOR exists

    Returns
    ----------
    conn: pymysql.connections.Connection

    cur: pymysql.cursors.Cursor
    '''
    conn = pymysql.connect(host = MYSQL_CONFIGURATION['host'],
                           user = MYSQL_CONFIGURATION['user'],
                           password = MYSQL_CONFIGURATION['password'],
                           port = MYSQL_CONFIGURATION['port'])
    cur = conn.cursor()
    cur.execute('show databases;')
    if 'experiment2' not in (i[0] for i in cur._rows):
        cur.execute('create database experiment2')
        conn.commit()
    cur.execute('show databases;')
    if 'experiment2' not in (i[0] for i in cur._rows):
        logger.info('create database experiment2 failed!')
        return None

    cur.execute('use experiment2;')
    logger.info('found database experiment2!')
    cur.execute('show tables;')
    if 'USER_BEHAVIOR' not in (i[0] for i in cur._rows):
        cur.execute('''create table USER_BEHAVIOR(
                       Uid char(8) not null,
                       Behavior int not null,
                       Aid char(8) not null,
                       BehaviorTime datetime not null);''')
        conn.commit()
    cur.execute('show tables;')
    if 'USER_BEHAVIOR' not in (i[0] for i in cur._rows):
        logger.info('create table USER_BEHAVIOR failed')
        return None
    logger.info('found table USER_BEHAVIOR!')
    logger.info('checking table finished!')
    return conn, cur

def insert_record(balanced_consumer, conn, cur):
    '''
    use the consumer to consume the records from kafka cluster and then insert them 
    into the mysql table USER_BEHAVIOR

    Parameters
    ----------
    balanced_consumer: pykafka.balancedconsumer.BalancedConsumer
        a consumer object

    conn: pymysql.connections.Connection

    cur: pymysql.cursors.Cursor 
    '''
    for mes in balanced_consumer:
        if mes is not None:
            record = mes.value.decode('utf-8').split('\t')
            if len(record) != 4:
                logger.warning('%s does not have enough elements!'%(record))
                logger.debug(str(record))
                continue
            try:
                cur.execute('insert into USER_BEHAVIOR values("%s", %s, "%s", "%s");'%tuple(record))
                conn.commit()
                balanced_consumer.commit_offsets()
                logger.info('insert record successful! %s'%(record))
            except Exception as e:
                logger.error(e)
                logger.error('failed! insert into USER_BEHAVIOR values("%s", %s, "%s", "%s");'%tuple(record))
                continue

def parse_topic(topic_name, num_consumer = 1):
    '''
    build connections to the kafka cluster and create multiple consumers
    to consume date from the specific topic

    Parameters
    ----------
    topic_name: str
        the topic name which we want to consume

    num_consumer: int, default 1
        the number of consumers we want to create
    '''
    client = KafkaClient(hosts="192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092")
    topic = client.topics[topic_name.encode('utf-8')]
    with ThreadPoolExecutor(max_workers=num_consumer) as executor:
        for _ in range(num_consumer):
            balanced_consumer = topic.get_balanced_consumer(
                consumer_group = b'testgroup',
                zookeeper_connect = "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181",
                auto_commit_enable = True,
                reset_offset_on_start = False,
            )
            conn, cur = create_table()
            executor.submit(insert_record, balanced_consumer, conn, cur)

if __name__ == '__main__':
    parse_topic('userbehavior10', 5)