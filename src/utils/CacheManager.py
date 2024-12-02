import time
from threading import Lock

class SimpleCache:
    def __init__(self, ttl: int = 300):
        """
        初始化缓存工具类
        :param ttl: 缓存过期时间（秒）
        """
        self.cache = {}
        self.ttl = ttl
        self.lock = Lock()

    def set(self, key: str, value: any):
        """
        设置缓存
        :param key: 缓存键
        :param value: 缓存值
        """
        with self.lock:
            self.cache[key] = (time.time(), value)

    def get(self, key: str):
        """
        获取缓存
        :param key: 缓存键
        :return: 缓存值,如果缓存不存在或过期则返回None
        """
        with self.lock:
            if key in self.cache:
                timestamp, value = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return value
                else:
                    # 缓存过期，删除缓存
                    del self.cache[key]
            return None

    def delete(self, key: str):
        """
        删除缓存
        :param key: 缓存键
        """
        with self.lock:
            if key in self.cache:
                del self.cache[key]

    def clear(self):
        """
        清空缓存
        """
        with self.lock:
            self.cache.clear()

class CacheManager:
    def __init__(self, cache: SimpleCache):
        self.cache = cache

    def getCacheKey(self, func_name: str, *args, **kwargs):
        """生成缓存键"""
        # 使用函数名和参数生成缓存键
        key_parts = [func_name] + list(args) + [f"{k}={v}" for k, v in kwargs.items()]
        return "-".join(map(str, key_parts))

    def getData(self, func_name: str, *args, **kwargs):
        """从缓存中获取数据,如果缓存不存在或过期则返回None"""
        cache_key = self.getCacheKey(func_name, *args, **kwargs)
        return self.cache.get(cache_key)

    def setData(self, func_name: str, data, *args, **kwargs):
        """设置缓存数据"""
        cache_key = self.getCacheKey(func_name, *args, **kwargs)
        self.cache.set(cache_key, data)
