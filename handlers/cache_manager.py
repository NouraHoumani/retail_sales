import redis
import json
import pickle
import logging
from typing import Any, Optional
from datetime import timedelta
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class CacheManager:
   
    
    def __init__(
        self, 
        redis_host: str = None,
        redis_port: int = None,
        redis_password: str = None,
        use_redis: bool = True,
        default_ttl: int = 3600 
    ):
        """
        Initialize cache manager
        
        Args:
            redis_host: Redis server host (default: localhost)
            redis_port: Redis server port (default: 6379)
            redis_password: Redis password (optional)
            use_redis: Attempt to use Redis if True
            default_ttl: Default time-to-live in seconds
        """
        self.default_ttl = default_ttl
        self.redis_client = None
        self.memory_cache = {}  # Fallback in-memory cache
        
        
        redis_host = redis_host or os.getenv('REDIS_HOST', 'localhost')
        redis_port = redis_port or int(os.getenv('REDIS_PORT', 6379))
        redis_password = redis_password or os.getenv('REDIS_PASSWORD')
        
        if use_redis:
            try:
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    decode_responses=False,  
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                
                self.redis_client.ping()
                log.info(f" Connected to Redis at {redis_host}:{redis_port}")
                self.cache_type = "redis"
            except (redis.ConnectionError, redis.TimeoutError) as e:
                log.warning(f" Redis connection failed: {e}")
                log.warning(" Falling back to in-memory cache")
                self.redis_client = None
                self.cache_type = "memory"
        else:
            log.info("Using in-memory cache (Redis disabled)")
            self.cache_type = "memory"
    
    def get(self, key: str) -> Optional[Any]:
        
        try:
            if self.redis_client:
                value = self.redis_client.get(key)
                if value:
                    log.debug(f" Cache HIT: {key} (Redis)")
                    return pickle.loads(value)
                log.debug(f" Cache MISS: {key} (Redis)")
                return None
            else:
                if key in self.memory_cache:
                    log.debug(f" Cache HIT: {key} (Memory)")
                    return self.memory_cache[key]
                log.debug(f" Cache MISS: {key} (Memory)")
                return None
        except Exception as e:
            log.error(f"Cache get error for key '{key}': {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in cache
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (default: self.default_ttl)
            
        Returns:
            True if successful, False otherwise
        """
        ttl = ttl or self.default_ttl
        
        try:
            if self.redis_client:
                serialized = pickle.dumps(value)
                self.redis_client.setex(key, ttl, serialized)
                log.debug(f" Cached: {key} (Redis, TTL: {ttl}s)")
                return True
            else:
                self.memory_cache[key] = value
                log.debug(f" Cached: {key} (Memory, no expiration)")
                return True
        except Exception as e:
            log.error(f"Cache set error for key '{key}': {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache"""
        try:
            if self.redis_client:
                result = self.redis_client.delete(key)
                log.debug(f"Deleted key: {key} (Redis)")
                return result > 0
            else:
                if key in self.memory_cache:
                    del self.memory_cache[key]
                    log.debug(f"Deleted key: {key} (Memory)")
                    return True
                return False
        except Exception as e:
            log.error(f"Cache delete error for key '{key}': {e}")
            return False
    
    def clear(self, pattern: Optional[str] = None) -> int:
        """
        Clear cache entries
        
        Args:
            pattern: Redis key pattern (e.g., "sales:*") or None for all
            
        Returns:
            Number of keys deleted
        """
        try:
            if self.redis_client:
                if pattern:
                    keys = self.redis_client.keys(pattern)
                    if keys:
                        count = self.redis_client.delete(*keys)
                        log.info(f"Cleared {count} keys matching '{pattern}' (Redis)")
                        return count
                    return 0
                else:
                    self.redis_client.flushdb()
                    log.info("Cleared entire Redis cache")
                    return -1  
            else:
                if pattern:
                    
                    keys_to_delete = [k for k in self.memory_cache.keys() 
                                     if pattern.replace('*', '') in k]
                    for key in keys_to_delete:
                        del self.memory_cache[key]
                    log.info(f"Cleared {len(keys_to_delete)} keys matching '{pattern}' (Memory)")
                    return len(keys_to_delete)
                else:
                    count = len(self.memory_cache)
                    self.memory_cache.clear()
                    log.info(f"Cleared entire memory cache ({count} keys)")
                    return count
        except Exception as e:
            log.error(f"Cache clear error: {e}")
            return 0
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        try:
            if self.redis_client:
                return self.redis_client.exists(key) > 0
            else:
                return key in self.memory_cache
        except Exception as e:
            log.error(f"Cache exists error for key '{key}': {e}")
            return False
    
    def get_stats(self) -> dict:
        
        try:
            if self.redis_client:
                info = self.redis_client.info('stats')
                return {
                    'cache_type': 'redis',
                    'total_keys': self.redis_client.dbsize(),
                    'hits': info.get('keyspace_hits', 0),
                    'misses': info.get('keyspace_misses', 0),
                    'hit_rate': (info.get('keyspace_hits', 0) / 
                                (info.get('keyspace_hits', 0) + info.get('keyspace_misses', 1)) * 100)
                }
            else:
                return {
                    'cache_type': 'memory',
                    'total_keys': len(self.memory_cache),
                    'hits': 'N/A',
                    'misses': 'N/A',
                    'hit_rate': 'N/A'
                }
        except Exception as e:
            log.error(f"Cache stats error: {e}")
            return {'error': str(e)}

def cache_query_result(cache: CacheManager, query_name: str, query_func, 
                       ttl: int = 3600, force_refresh: bool = False) -> Any:
    """
    Cache wrapper for database queries
    
    Args:
        cache: CacheManager instance
        query_name: Unique name for this query (cache key)
        query_func: Function that executes the query (no args)
        ttl: Cache duration in seconds
        force_refresh: Skip cache and force query execution
        
    Returns:
        Query result (from cache or fresh)
    """
    cache_key = f"query:{query_name}"
    
    if not force_refresh:
        result = cache.get(cache_key)
        if result is not None:
            log.info(f" Using cached result for: {query_name}")
            return result
    
    log.info(f" Executing query: {query_name}")
    result = query_func()
    cache.set(cache_key, result, ttl=ttl)
    
    return result

def invalidate_cache_pattern(cache: CacheManager, pattern: str):
    """
    Invalidate all cache keys matching a pattern
    Useful after data updates/ETL runs
    
    Args:
        cache: CacheManager instance
        pattern: Pattern to match (e.g., "sales:*")
    """
    count = cache.clear(pattern)
    log.info(f" Invalidated {count} cache entries matching '{pattern}'")

if __name__ == "__main__":
    
   
    cache = CacheManager(use_redis=True)
    
    print(f"\n{'='*70}")
    print(f"CACHE TYPE: {cache.cache_type.upper()}")
    print(f"{'='*70}\n")
    
    
    print("Example 1: Basic caching")
    cache.set("test_key", {"data": "Hello World"}, ttl=60)
    result = cache.get("test_key")
    print(f"Cached value: {result}")
    
    
    print("\nExample 2: Query result caching")
    
    def expensive_query():
        import time
        print("  Running expensive query...")
        time.sleep(1)  # Simulate slow query
        return {"total_sales": 1000000, "orders": 5000}
    
    
    result1 = cache_query_result(cache, "monthly_sales", expensive_query, ttl=300)
    print(f"Result 1: {result1}")
    
    
    result2 = cache_query_result(cache, "monthly_sales", expensive_query, ttl=300)
    print(f"Result 2: {result2}")
    
    
    print(f"\nExample 3: Cache stats")
    stats = cache.get_stats()
    print(f"Stats: {stats}")
    
    
    print(f"\nExample 4: Clear cache")
    cache.clear()
    print("Cache cleared")
    
    print(f"\n{'='*70}")
    print("DONE")
    print(f"{'='*70}\n")
