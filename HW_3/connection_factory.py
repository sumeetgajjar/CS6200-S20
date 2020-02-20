from contextlib import closing
from typing import Union

import redis
from mysql.connector import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

from utils.singleton import SingletonMeta


class RedisConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host='127.0.0.1', port=6379, socket_timeout=10) -> None:
        self.pool = redis.ConnectionPool(host=host, port=port, socket_timeout=socket_timeout, decode_responses=True)

    def get_connection(self) -> redis.Redis:
        return redis.Redis(connection_pool=self.pool)


class MysqlConnectionPool(metaclass=SingletonMeta):

    def __init__(self, host='127.0.0.1', port=3306) -> None:
        self.pool = MySQLConnectionPool(pool_name='cs6200', database='cs6200', user='cs6200', password='cs6200',
                                        host=host, port=port)

    def get_connection(self) -> Union[PooledMySQLConnection, MySQLConnection]:
        return self.pool.get_connection()


class ConnectionFactory:

    @classmethod
    def create_redis_connection(cls) -> redis.Redis:
        return RedisConnectionPool().get_connection()

    @classmethod
    def create_mysql_connection(cls):
        return closing(MysqlConnectionPool().get_connection())
