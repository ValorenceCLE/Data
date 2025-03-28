import asyncio
import smbus2
from datetime import datetime, timezone
import time
from utils.logging_setup import local_logger as logger
from utils.config import settings
from utils.singleton import RedisClient

class EnvironmentalData:
    def __init__(self, i2c_bus=1, address=0x45):
        self.null = settings.NULL
        self.collection_interval = settings.COLLECTION_INTERVAL
        try:
            self.bus = smbus2.SMBus(i2c_bus)
        except Exception as e:
            logger.error(f"Failed to open I2C bus: {e}")
            self.bus = None
        self.address = address
        if self.bus:
            self.init_sensor()

    async def async_init(self):
        self.redis = await RedisClient.get_instance()
    
    def init_sensor(self):
        if self.bus:
            try:
                # Optional soft reset command for SHT30-DIS:
                # According to the datasheet, a soft reset is 0x30A2 (MSB=0x30, LSB=0xA2)
                self.bus.write_i2c_block_data(self.address, 0x30, [0xA2])
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Error initializing sensor: {e}")

    async def read_data(self):
        """
        Triggers a single-shot high repeatability measurement without clock stretching
        (command 0x2400), waits for the measurement to complete, and then reads 6 bytes of data.
        The returned data contains the temperature and humidity raw values, which are converted
        using the SHT3x formulas:
          Temperature (°C) = -45 + 175 * (temp_raw / 65535)
          Relative Humidity (%) = 100 * (humidity_raw / 65535)
        Temperature is then converted to Fahrenheit.
        """
        if self.bus:
            # Send measurement command 0x2400 (High repeatability, no clock stretching)
            await asyncio.to_thread(self.bus.write_i2c_block_data, self.address, 0x24, [0x00])
            # Wait for the sensor to complete the measurement (~15 ms typical for high repeatability)
            await asyncio.sleep(0.015)
            # Read 6 bytes: two bytes temperature, one CRC, two bytes humidity, one CRC
            data = await asyncio.to_thread(self.bus.read_i2c_block_data, self.address, 0x00, 6)
            if data and len(data) == 6:
                # Combine bytes to get raw temperature and humidity values
                temp_raw = (data[0] << 8) | data[1]
                humidity_raw = (data[3] << 8) | data[4]
                
                # Convert raw temperature to Celsius then to Fahrenheit
                temperature_c = -45 + (175 * (temp_raw / 65535.0))
                temperature_f = round(temperature_c * 9 / 5 + 32, 1)
                
                # Convert raw humidity to percentage
                humidity = round(100 * (humidity_raw / 65535.0), 1)
                return temperature_f, humidity
        return None, None

    async def process_data(self):
        try:
            temperature, humidity = await self.read_data()
            timestamp = datetime.now(timezone.utc).astimezone().isoformat()
            await self.stream_data(temperature=temperature, humidity=humidity, timestamp=timestamp)
        except Exception as e:
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
