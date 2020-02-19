import random

import redis

from utils.utils import Utils


def testing(_):
    client = redis.Redis()
    while True:
        member = '{}'.format(random.randint(1, 100)) * 200
        client.zincrby('test_queue', 1, member)


if __name__ == '__main__':
    Utils.run_tasks_parallelly(testing, list(range(10)), 10)
