import mysql.connector
import redis

from utils.singleton import SingletonMeta


class RedisConnection(metaclass=SingletonMeta):

    def __init__(self, host='127.0.0.1', port=6379, socket_timeout=10) -> None:
        self.host = host
        self.port = port
        self.conn = redis.Redis(host, port, socket_timeout=socket_timeout, decode_responses=True)


class MysqlConnection:

    def __init__(self, host='127.0.0.1', port=3306) -> None:
        self.host = host
        self.port = port
        self.conn = mysql.connector.connect(pool_size='cs6200', database='cs6200', user='cs6200', password='cs6200',
                                            host=host, port=port)


class ConnectionFactory:

    @classmethod
    def create_redis_connection(cls) -> RedisConnection:
        return RedisConnection()

    @classmethod
    def create_mysql_connection(cls) -> MysqlConnection:
        return MysqlConnection()
