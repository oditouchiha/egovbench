from egovbench_kafka import YoutubeKafkaInput
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if not logger.handlers:

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

yki = YoutubeKafkaInput()

while True:

    try:
        data = yki.get_latest_message()
        logging.info(data)

    except Exception as e:
        logging.error(e)
