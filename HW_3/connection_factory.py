import redis

from constants.constants import Constants
from utils.singleton import SingletonMeta


class RedisConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host=Constants.REDIS_HOST, port=Constants.REDIS_PORT,
                 socket_timeout=Constants.REDIS_SOCKET_TIMEOUT) -> None:
        self.pool = redis.ConnectionPool(host=host, port=port, socket_timeout=socket_timeout, decode_responses=True)

    def get_connection(self) -> redis.Redis:
        return redis.Redis(connection_pool=self.pool)


class ConnectionFactory:

    @classmethod
    def create_redis_connection(cls) -> redis.Redis:
        return RedisConnectionPool().get_connection()
