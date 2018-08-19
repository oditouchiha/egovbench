from pykafka import KafkaClient

from pykafka.common import OffsetType
import json
import logging


class Broker:

    def __init__(self, topic, brokeradress='localhost:9092'):
        self.conf = KafkaClient(
            hosts=brokeradress
        )
        self.topic = self.conf.topics[str.encode(topic)]


class Producer:

    def __init__(self, topic, prompter):
        self.broker = Broker(topic)
        self.topic = self.broker.topic
        self.prompter = prompter

    def promptProducer(self, texts):
        return logging.info('[{}]>'.format(self.prompter) + ' ' + texts)

    # Method serialisasi yang dipanggil sebagai argumen untuk producer

    def serializer_method(self, v, pk):
        self.promptProducer('Serializing data . . .')

        value = json.dumps(v).encode('utf-8')
        pk = None

        return value, pk

    # Memulai Producer

    def send_message(self, dict_or_json):
        self.promptProducer('(_id: {}) Sending data . . .'.format(dict_or_json['_id']))

        producer = self.topic.get_producer(
            serializer=self.serializer_method,
            sync=True,
            max_request_size=1024 * 1024 * 10,
            use_rdkafka=False,
            delivery_reports=False,
            # pending_timeout_ms=100,
        )

        with producer as producer:
            try:
                producer.produce(dict_or_json)
                # msg, exc = self.producer.get_delivery_report(block=True, timeout=0.1)
                # logging.debug(msg + exc)
            except Exception as e:
                logging.warning(e)

        self.promptProducer('(_id: {}) Data sent!'.format(dict_or_json['_id']))

        latest_available_offsets = json.dumps(self.topic.latest_available_offsets())

        logging.debug('Latest available offsets: ' + latest_available_offsets)

    # Latest Offset Checker


class Consumer:

    def __init__(self, topic, prompter):
        self.broker = Broker(topic)
        self.topic = self.broker.topic
        self.consumer = self.topic.get_simple_consumer(
            # consumer_group='mygroup',
            # consumer_timeout_ms=100,
            deserializer=self.deserializer_method,
            auto_offset_reset=OffsetType.LATEST,
            fetch_message_max_bytes=1024 * 1024 * 10,
            reset_offset_on_start=True,
            # auto_commit_enable=True,
            # use_rdkafka=True
        )
        self.prompter = prompter

    def promptConsumer(self, texts):
        return logging.info('[{}]>'.format(self.prompter) + ' ' + texts)

    # Method deserialisasi untuk konsumerx`

    def deserializer_method(self, v, pk):

        value = json.loads(v.decode('utf-8'))
        pk = None

        return value, pk

    # Membaca message terakhir yang di kirim producer
    # Method tidak akan berjalan (menunggu) sebelum adanya pesan yang dikirim oleh producer

    def get_latest_message(self):
        self.promptConsumer('Waiting for message . . .')

        try:
            for message in self.consumer:

                if message is not None:

                    logging.debug(message.offset + message.value)
                    self.consumer.stop()

        except Exception as e:
            logging.warning(e)

        latest_message_value = message.value

        self.promptConsumer('Message Received!')

        return latest_message_value


class FacebookKafkaPost(Producer, Consumer):

    def __init__(self, topic='facebook-kafka-post'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())


class FacebookKafkaComment(Producer, Consumer):

    def __init__(self, topic='facebook-kafka-comment'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())


class TwitterKafkaPost(Producer, Consumer):

    def __init__(self, topic='twitter-kafka-post'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())


class TwitterKafkaComment(Producer, Consumer):

    def __init__(self, topic='twitter-kafka-comment'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())


class YoutubeKafkaPost(Producer, Consumer):

    def __init__(self, topic='youtube-kafka-post'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())


class YoutubeKafkaComment(Producer, Consumer):

    def __init__(self, topic='youtube-kafka-comment'):
        logging.info('[EGOVBENCH_KAFKA]>' + 'topic connected: ' + topic)
        Producer.__init__(self, topic, prompter=topic.upper())
        Consumer.__init__(self, topic, prompter=topic.upper())
