import os
import ssl
from datetime import timedelta
from typing import Any, List
import redis

from proteus.storage.cache import KeyValueCache


class RedisCache(KeyValueCache):

    def __init__(self, host: str, database_number: int, port=6380):
        """
          Initialises a secure Redis connection.

        :param host: Redis hostname.
        :param database_number: Redis database number (0..15)
        :param port: Connection port.
        """
        self._redis = redis.StrictRedis(
            host=host,
            port=port,
            db=database_number,
            password=os.environ['PROTEUS__CACHE_REDIS_PASSWORD'],
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl=True,
            decode_responses=False
        )

    def exists(self, key: str) -> bool:
        return self._redis.exists(key) == 1

    def get(self, key: str) -> Any:
        return self._redis.get(key)

    def multi_get(self, keys: List[str]) -> List[Any]:
        return self._redis.mget(keys)

    def set(self, key: str, value: Any, expires_after=timedelta(seconds=60), return_old_value=False) -> Any:
        return_value = self._redis.get(key) if return_old_value else value
        self._redis.set(key, value, ex=expires_after)

        return return_value
