import concurrent.futures
import logging
import threading
import random


# ANSI colors
CYAN = '\033[36m'
MAGENTA = '\033[35m'
RESET = '\033[0m'


logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO,
)
# logging.getLogger().setLevel(logging.DEBUG)


SENTINEL = object()


class Pipeline:
    def __init__(self):
        self.message = 0
        self.producer_lock = threading.Lock()
        self.consumer_lock = threading.Lock()
        self.consumer_lock.acquire()

    def set_message(self, message):
        logging.debug(CYAN + 'Producer: About to acquire set_lock' + RESET)
        self.producer_lock.acquire()
        logging.debug(CYAN + f'Producer: Acquired set_lock; about to push {message} to pipeline' + RESET)
        self.message = message
        logging.debug(CYAN + f'Producer: Finished pushing; pipeline is now {self.message}; about to release get_lock' + RESET)
        self.consumer_lock.release()
        logging.debug(CYAN + 'Producer: Released get_lock; consumer is allowed to pull' + RESET)

    def get_message(self):
        logging.debug(MAGENTA + 'Consumer: About to acquire get_lock' + RESET)
        self.consumer_lock.acquire()
        logging.debug(MAGENTA + f'Consumer: Acquired get_lock; about to pull from pipeline, which is currently {self.message}' + RESET)
        message = self.message
        logging.debug(MAGENTA +  f'Consumer: Pulled {message} from pipeline; about to release set_lock' + RESET)
        self.producer_lock.release()
        logging.debug(MAGENTA + 'Consumer: Released set_lock; producer is allowed to push' + RESET)
        return message


def producer(pipeline):
    """Push message to the pipeline."""
    for _ in range(5):
        message = random.randint(1, 101)
        logging.info(CYAN + f'Producer: Attempting to push data {message} to pipeline ...' + RESET)
        pipeline.set_message(message)
        logging.info(CYAN + f'Producer: ... pushed {message} to pipeline' + RESET)

    # Send a sentinel message to tell consumer we're done
    pipeline.set_message(SENTINEL)


def consumer(pipeline):
    """Pull message from the pipeline."""
    while True:
        logging.info(MAGENTA + 'Consumer: Attempting to pull data from pipeline ...' + RESET)
        message = pipeline.get_message()
        if message is SENTINEL:
            logging.info(MAGENTA + f'Consumer: ... bye!' + RESET)
            return
        logging.info(MAGENTA + f'Consumer: ... pulled data {message} from pipeline' + RESET)


if __name__ == '__main__':
    pipeline = Pipeline()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(producer, pipeline)
        executor.submit(consumer, pipeline)
