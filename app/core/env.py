import asyncio
import smbus2
from datetime import datetime, timezone
import time
from utils.logging_setup import local_logger as logger
from utils.config import settings
from utils.singleton import RedisClient

class EnvironmentalData:
    def __init__(self, i2c_bus=1, address=0x38):
        self.null = settings.NULL
        self.collection_interval = settings.COLLECTION_INTERVAL
        try:
            self.bus = smbus2.SMBus(i2c_bus)
        except Exception as e:
            self.bus = None
        self.address = address
        if self.bus:
            self.init_sensor()

    async def async_init(self):
        self.redis = await RedisClient.get_instance()
    
    def init_sensor(self):
        if self.bus:
            self.bus.write_i2c_block_data(self.address, 0xE1, [0x08, 0x00])
            time.sleep(0.01)
    
    async def read_humidity(self):
        data = await self._read_raw_data()
        if data:
            return round(((data[1] << 12) | (data[2] << 4) | (data[3] >> 4)) * 100 / 1048576, 1)
        return None

    async def read_temperature(self):
        data = await self._read_raw_data()
        if data:
            temperature_c = (((data[3] & 0x0F) << 16) | (data[4] << 8) | data[5]) * 200 / 1048576 - 50
            return round(temperature_c * 9 / 5 + 32, 1)
        return None

    async def _read_raw_data(self):
        if self.bus:
            await asyncio.to_thread(self.bus.write_i2c_block_data, self.address, 0xAC, [0x33, 0x00])
            await asyncio.sleep(0.05)
            return await asyncio.to_thread(self.bus.read_i2c_block_data, self.address, 0x00, 6)
        return None

    async def process_data(self):
        try:
            temperature = await self.read_temperature()
            humidity = await self.read_humidity()
            timestamp = datetime.now(timezone.utc).astimezone().isoformat()
            await self.stream_data(temperature=temperature, humidity=humidity, timestamp=timestamp)
        except BaseException as e:
            await logger.error(f"Error processing data: {e}")

    async def stream_data(self, temperature, humidity, timestamp):
        data = {
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity
        }
        await self.redis.xadd('environmental', data)

    async def run(self):
        await self.async_init()
        while True:
            await self.process_data()
            await asyncio.sleep(self.collection_interval)
