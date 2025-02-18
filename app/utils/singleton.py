import asyncio 
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync 
from influxdb_client.client.write_api_async import WriteApiAsync 
from influxdb_client.client.query_api_async import QueryApiAsync
from redis.asyncio import Redis # Needs to be async version
from utils.logging_setup import local_logger as logger
from utils.config import settings

class InfluxClient:
    _instance = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        raise RuntimeError("Use 'InfluxClient.get_instance()' to get the instance.")

    @classmethod
    async def get_instance(cls):
        async with cls._lock:
            if cls._instance is None:
                token = settings.TOKEN
                org = settings.ORG
                url = settings.INFLUXDB_URL
                try:
                    client = InfluxDBClientAsync(url=url, token=token, org=org)
                    await client.__aenter__()  # Properly initialize the client
                    cls._instance = client
                    logger.debug("InfluxDB Client Created and Initialized")
                except Exception as e:
                    logger.critical(f"Failed to create InfluxDB Client: {e}")
                    raise e
            return cls._instance

    @classmethod
    async def close_instance(cls):
        async with cls._lock:
            if cls._instance:
                try:
                    await cls._instance.__aexit__(None, None, None)
                    logger.debug("InfluxDB Client Closed")
                except Exception as e:
                    logger.error(f"Error closing InfluxDB Client: {e}")
                finally:
                    cls._instance = None


class InfluxWriter:
    _instance = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        raise RuntimeError("Use 'InfluxWriter.get_instance()' to get the instance.")
        
    @classmethod
    async def get_instance(cls):
        async with cls._lock:
            if cls._instance is None:
                try:
                    client = await InfluxClient.get_instance()
                    writer = WriteApiAsync(client)
                    cls._instance = writer
                    logger.debug("InfluxDB Write API Created")
                except Exception as e:
                    logger.critical(f"Failed to create InfluxDB Write API: {e}")
                    raise e
            return cls._instance
        
    @classmethod
    async def close_instance(cls):
        async with cls._lock:
            if cls._instance:
                try:
                    await cls._instance.__aexit__(None, None, None)
                    logger.debug("InfluxDB Write API Closed")
                except Exception as e:
                    logger.error(f"Error closing InfluxDB Write API: {e}")
                finally:
                    cls._instance = None
                

class InfluxQuery:
    _instance = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        raise RuntimeError("Use 'InfluxQuery.get_instance()' to get the instance.")
        
    @classmethod
    async def get_instance(cls):
        async with cls._lock:
            if cls._instance is None:
                try:
                    client = await InfluxClient.get_instance()
                    query_api = QueryApiAsync(client)
                    cls._instance = query_api
                    logger.debug("InfluxDB Query API Created")
                except Exception as e:
                    logger.critical(f"Failed to create InfluxDB Query API: {e}")
                    raise e
            return cls._instance
        
    @classmethod
    async def close_instance(cls):
        async with cls._lock:
            if cls._instance:
                try:
                    await cls._instance.__aexit__(None, None, None)
                    logger.debug("InfluxDB Query API Closed")
                except Exception as e:
                    logger.error(f"Error closing InfluxDB Query API: {e}")
                finally:
                    cls._instance = None


class RedisClient:
    _instance = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        raise RuntimeError("Use 'RedisClient.get_instance()' to get the instance.")
    
    @classmethod
    async def get_instance(cls):
        async with cls._lock:
            if cls._instance is None:
                redis_url = settings.REDIS_URL
                try:
                    cls._instance = Redis.from_url(redis_url)
                    await cls._instance.ping()  # Test the connection
                    logger.debug("Redis Client Created and Connected")
                except Exception as e:
                    logger.critical(f"Failed to create Redis Client: {e}")
                    raise e
            return cls._instance

    @classmethod
    async def close_instance(cls):
        async with cls._lock:
            if cls._instance:
                try:
                    await cls._instance.close()
                    logger.debug("Redis Client Closed")
                except Exception as e:
                    logger.error(f"Error closing Redis Client: {e}")
                finally:
                    cls._instance = None
