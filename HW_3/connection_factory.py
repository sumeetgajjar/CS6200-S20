from contextlib import closing
from typing import Union

import redis
from mysql.connector import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

from constants.constants import Constants
from utils.singleton import SingletonMeta


class RedisConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host=Constants.REDIS_HOST, port=Constants.REDIS_PORT,
                 socket_timeout=Constants.REDIS_SOCKET_TIMEOUT) -> None:
        self.pool = redis.ConnectionPool(host=host, port=port, socket_timeout=socket_timeout, decode_responses=True)

    def get_connection(self) -> redis.Redis:
        return redis.Redis(connection_pool=self.pool)


class MysqlConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host=Constants.MYSQL_HOST, port=Constants.MYSQL_PORT) -> None:
        self.pool = MySQLConnectionPool(pool_name=Constants.MYSQL_POOL_NAME, database=Constants.MYSQL_DATABASE,
                                        user=Constants.MYSQL_USERNAME, password=Constants.MYSQL_PASSWORD, host=host,
                                        port=port)

    def get_connection(self) -> Union[PooledMySQLConnection, MySQLConnection]:
        return self.pool.get_connection()


class ConnectionFactory:

    @classmethod
    def create_redis_connection(cls) -> redis.Redis:
        return RedisConnectionPool().get_connection()

    @classmethod
    def create_mysql_connection(cls):
        return closing(MysqlConnectionPool().get_connection())
