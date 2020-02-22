import redis
from redisbloom.client import Client

from constants.constants import Constants
from utils.singleton import SingletonMeta


class RedisConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host=Constants.REDIS_HOST, port=Constants.REDIS_PORT,
                 socket_timeout=Constants.REDIS_SOCKET_TIMEOUT) -> None:
        self.redis_configs = {
            "host": host,
            "port": port,
            "socket_timeout": socket_timeout,
            "decode_responses": True
        }
        self.pool = redis.ConnectionPool(**self.redis_configs)

    def get_connection(self) -> Client:
        # return redis.Redis(connection_pool=self.pool)
        return Client(connection_pool=self.pool)

    # def get_redis_bloom_filter_client(self) -> Client:
    #     return Client(**self.redis_configs)


class ConnectionFactory:

    @classmethod
    def create_redis_connection(cls) -> Client:
        return RedisConnectionPool().get_connection()

    # @classmethod
    # def create_redis_bloom_filter_client(cls) -> Client:
    #     return RedisConnectionPool().get_redis_bloom_filter_client()
