# -*- coding:utf-8 -*-
from pykafka import KafkaClient
from split_file_by_time import output

def send_records(topic_name, filename):
    '''send the records of the file to the specific topic

    Parameters
    ----------
    topic_name: str
        the name of the topic which you will sent records to

    filename: str
        the path of the file you want to read
    '''
    client = KafkaClient(hosts="192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092")
    topic = client.topics[topic_name.encode('utf-8')]
    count = 0
    all_count = 0
    with topic.get_sync_producer() as producer:
        for record in output(filename, 'second'):
            for t in record.strip().split('\n'):
                producer.produce(t.encode('utf-8'))
                print("send message:%s"%(t))
                all_count += 1
            count += 1
            if count > 15000:
                break
    print('send %d batchs, %d records'%(count, all_count))

if __name__ == "__main__":
    send_records('userbehavior10', 'smallUserBehaviorSortedByTime_part1.txt')