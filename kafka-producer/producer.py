from kafka import KafkaProducer
import time


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='IP_ADDRESS_HERE:9092')  # use your VM's external IP Here!
    with open("C:/Users/Downloads/archive/medals.csv") as f: # Save file locally and update file path
        lines = f.readlines()
    
    i = 0
    for line in lines:
        if i > 0:
            list = line.split(',')
            if i == 1:
                date = list[2]
                discipline, event = list[-2:]
            else: 
                new_date = list[2]
                if date != new_date:
                    time.sleep(60)
                new_discipline, new_event = list[-2:]
                if (new_discipline != discipline) or (new_event != event):
                    time.sleep(10)
                date, discipline, event = new_date, new_discipline, new_event
            kafka_python_producer_sync(producer, line, 'medals')
        i = i + 1

    f.close()
