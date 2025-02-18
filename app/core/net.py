from datetime import datetime, timezone
import asyncio
import aioping 
from utils.logging_setup import local_logger as logger
from utils.config import settings
from utils.singleton import RedisClient

# This script also needs better error handling

class NetworkData:
    def __init__(self):
        self.target_ip = settings.PING_TARGET
        self.collection_interval = settings.COLLECTION_INTERVAL
        self.ping_count = 5  # Number of pings per test
        self.null = settings.NULL
        
    async def async_init(self):
        self.redis = await RedisClient.get_instance()
        
    async def run_ping_test(self):
        try:
            response_list = await asyncio.gather(*[self.ping_host() for _ in range(self.ping_count)])
            packets_lost = response_list.count(None)
            packet_loss_percent = packets_lost / self.ping_count * 100
            valid_responses = list(filter(None, response_list))
            if valid_responses:
                avg_rtt = round(sum(valid_responses)/len(valid_responses),2)
                min_rtt = round(min(valid_responses),2)
                max_rtt = round(max(valid_responses),2)
            else:
                avg_rtt = self.null
                min_rtt = self.null
                max_rtt = self.null
            await self.stream_data(avg_rtt, min_rtt, max_rtt, packet_loss_percent)
        except Exception as e:
            logger.error(f"Failed to perform ping test: {e}")
            
    async def ping_host(self):
        try:
            delay = await aioping.ping(self.target_ip)
            return delay * 1000
        except TimeoutError:
            return None
    
    async def stream_data(self, avg_rtt, min_rtt, max_rtt, packet_loss_percent):
        """Saves the data to Redis."""
        try:
            timestamp = datetime.now(timezone.utc).astimezone().isoformat()
            data = {
                "timestamp": timestamp,
                "avg_rtt": avg_rtt,
                "min_rtt": min_rtt,
                "max_rtt": max_rtt,
                "packet_loss_percent": packet_loss_percent
            }
            await self.redis.xadd('network', data)
        except Exception as e:
            logger.error(f"Failed to stream data to Redis: {e}", exc_info=True)
        
    async def run(self):
        await self.async_init()
        while True:
            await self.run_ping_test()
            await asyncio.sleep(self.collection_interval)