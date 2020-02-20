import redis

from utils.singleton import SingletonMeta


class RedisConnection(metaclass=SingletonMeta):

    def __init__(self, host='127.0.0.1', port=6379, socket_timeout=10) -> None:
        self.host = host
        self.port = port
        self.client = redis.Redis(host, port, socket_timeout=socket_timeout, decode_responses=True)


class MysqlConnection(metaclass=SingletonMeta):

    def __init__(self, host='127.0.0.1', port=3306) -> None:
        pass

    def get_client(self):
        pass


class ConnectionFactory:

    @classmethod
    def get_redis_connection(cls) -> RedisConnection:
        return RedisConnection()

    @classmethod
    def get_mysql_connection(cls) -> MysqlConnection:
        return MysqlConnection()
