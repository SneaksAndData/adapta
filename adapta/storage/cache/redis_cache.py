"""
  Key-value cache based on Redis.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os
import ssl
from datetime import timedelta
from typing import Any, List, Optional

import redis
from redis import default_backoff
from redis.cluster import RedisCluster
from redis.retry import Retry

from adapta.storage.cache import KeyValueCache


class RedisCache(KeyValueCache):
    """
    Redis cache.
    """

    def __init__(self, host: str, database_number: int, port=6380, cluster_mode=False):
        """
          Initialises a secure Redis connection.

        :param host: Redis hostname.
        :param database_number: Redis database number (0..15)
        :param port: Connection port.
        :param cluster_mode: Whether this cache is a standalone or a clustered Redis.
        """
        if not cluster_mode:
            self._redis = redis.StrictRedis(
                host=host,
                port=port,
                db=database_number,
                password=os.environ["PROTEUS__CACHE_REDIS_PASSWORD"],
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                ssl=True,
                decode_responses=False,
                retry_on_timeout=True,
                retry_on_error=[redis.exceptions.ConnectionError],
            )
        else:
            self._redis = RedisCluster(
                host=host,
                port=port,
                password=os.environ["PROTEUS__CACHE_REDIS_PASSWORD"],
                retry=Retry(default_backoff(), 3),
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                ssl=True,
            )

    def multi_exists(self, keys: List[str]) -> bool:
        return self._redis.exists(*keys) == len(keys)

    def evict(self, key: str) -> None:
        self._redis.delete(key)

    def exists(self, key: str, attribute: Optional[str] = None) -> bool:
        if not attribute:
            return self._redis.exists(key) == 1

        return self._redis.hexists(name=key, key=attribute)

    def get(self, key: str, is_map=False) -> Any:
        if not is_map:
            return self._redis.get(key)

        return self._redis.hgetall(key)

    def multi_get(self, keys: List[str]) -> List[Any]:
        return self._redis.mget(keys)

    def set(
        self,
        key: str,
        value: Any,
        expires_after=timedelta(seconds=60),
        return_old_value=False,
    ) -> Any:
        return_value = self._redis.get(key) if return_old_value else value
        self._redis.set(key, value, ex=expires_after)

        return return_value

    def include(self, key: str, attribute: str, value: Any) -> None:
        self._redis.hsetnx(key, attribute, value)

        return value

    def set_expiration(self, key: str, expires_after: timedelta) -> None:
        self._redis.expire(name=key, time=expires_after)
