import pickle

import pandas as pd
import redis


# from distutils.version import StrictVersion  # Uncomment if needed
# class RedisWrapper(object):
#     """docstring for RedisWrapper"""

#     def __init__(self, mode: str = None):
#         """
#         Init for Redis
#         Require mode can be standalone or cluster
#         """
#         """assert StrictVersion(redis.__version__) >= StrictVersion(
#             "4.4.0"
#         ), "Install redis>=4.4.0" """
#         super(RedisWrapper, self).__init__()
#         self.client = None
#         self.mode = mode or os.getenv("REDIS_MODE")
#         self._init_client()

#     def _init_client(self):
#         redis_config = self._parse_redis_config()
#         if self.mode == "standalone":
#             self.client = redis.Redis(**redis_config)
#             return
#         if self.mode == "cluster":
#             self.client = redis.RedisCluster(**redis_config)
#             return
#         else:
#             raise ValueError("mode can be either standalone or cluster")

#     def _parse_redis_config(self):
#         # Assuming REDIS_CONFIG is a comma-separated string of key-value pairs
#         # Example: "host=localhost,port=6379,db=0"
#         config_str = os.getenv("REDIS_CONFIG")
#         if not config_str:
#             raise ValueError("REDIS_CONFIG environment variable is not set")

#         config_pairs = config_str.split(",")
#         config_dict = {}
#         for pair in config_pairs:
#             key, value = pair.split("=")
#             config_dict[key] = value

#         return config_dict


class RedisOnlineStore:
    """_summary_

    This class implements redis storage functionalities
    """

    conn = None

    def __init__(self):
        self.client = self._get_client()

    def _get_client(self):
        """
        Creates the Redis client RedisCluster or Redis depending on configuration
        """
        self.client = redis.Redis(
            host="localhost", port=6379, db=0
        )  # RedisWrapper().client

        return self.client

    def online_write_batch(
        self,
        entity_feature_group_key: str,
        hash_field: str,
        df: pd.DataFrame,
        key_ttl_seconds: int = 60 * 60,
    ) -> None:
        """_summary_

        Args:
            entity_feature_group_key (str): entity and feature_group i.e. userid and cea
            hash_field (str): key for redis fields. i.e. userid for cea group features
            df (pd.DataFrame): df to be written into redis
        """
        self.df = df

        df.groupby(hash_field).apply(
            lambda grp: self.client.hset(
                entity_feature_group_key,
                str(grp[hash_field].values[0]),
                pickle.dumps(grp),
            )
        )
        # self.client.expire(
        #         entity_feature_group_key, key_ttl_seconds
        #     )

    def online_read(
        self, entity_feature_group_key: str, hash_field: str
    ) -> pd.DataFrame:
        """_summary_

        Args:
            entity_feature_group_key (str):entity and feature_group i.e. userid and cea
            hash_field (str): _description_

        Returns:
            Dict: _description_
        """
        # self.client = self._get_client()
        pkl_obj = self.client.hget(entity_feature_group_key, hash_field)
        return pickle.loads(pkl_obj)

    def set(self, key, value):
        self.client = self._get_client()
        self.client.set(key, value)

    def get(self, key):
        self.client = self._get_client()
        return self.client.get(key)

    def __repr__(self):
        return "Redis Online store debugging".format()

    def __str__(self):
        pass


if __name__ == "__main__":
    # client = redis.Redis(host="localhost", port=6379, db=0)

    # print(client.set("foo", "bar"))

    redis_client = RedisOnlineStore()
    df = pd.DataFrame({"userid": 2, "B": 3}, index=[0])
    print(df)
    ENTITY_KEY = "test_id"
    FEATURE_GROUP = "test_group"
    redis_client.online_write_batch(
        "{}:{}".format(ENTITY_KEY, FEATURE_GROUP), "userid", df
    )

    d = redis_client.online_read("{}:{}".format(ENTITY_KEY, FEATURE_GROUP), "2")
    print(d)
