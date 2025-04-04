import os
import asyncio
import aiofiles
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

class Settings:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
        return cls._instance

    @lru_cache(maxsize=1)
    async def rpi_serial(self):
        """
        Get the Raspberry Pi serial number from /proc/cpuinfo.
        """
        try: 
            async with aiofiles.open('/proc/cpuinfo', 'r') as f:
                async for line in f:
                    if line.startswith('Serial'):
                        serial = line.split(':')[-1].strip().lower()
                        return serial
            logger.error("Failed to read serial number from /proc/cpuinfo.")
            return None
        except IOError as e:
            logger.error(f"Error reading serial number: {e}")
            return None

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                task = loop.create_task(self.rpi_serial())
                self.serial_number = None
                task.add_done_callback(lambda t: setattr(self, 'serial_number', t.result()))
            else:
                self.serial_number = asyncio.run(self.rpi_serial())
        except RuntimeError as e:
            logger.error(f"Error initializing serial number: {e}")
            self.serial_number = None

        # Database settings - added default values and type checking
        self.TOKEN = os.getenv('DOCKER_INFLUXDB_INIT_ADMIN_TOKEN', '')
        self.ORG = os.getenv('DOCKER_INFLUXDB_INIT_ORG', 'valorence')
        self.BUCKET = os.getenv('DOCKER_INFLUXDB_INIT_BUCKET', 'dpm_data')
        
        # Improved InfluxDB URL handling
        influxdb_url = os.getenv('INFLUXDB_URL')
        self.INFLUXDB_URL = influxdb_url if isinstance(influxdb_url, str) else 'http://influxdb:8086'
        
        # Validate InfluxDB settings
        if not self.INFLUXDB_URL:
            logger.warning("InfluxDB URL not set. Using default.")
            self.INFLUXDB_URL = 'http://influxdb:8086'
        
        # Added safety checks for other database variables
        if not self.TOKEN:
            logger.warning("InfluxDB token not set. This may cause connection issues.")
        if not self.ORG:
            logger.warning("InfluxDB organization not set. Using default.")
        if not self.BUCKET:
            logger.warning("InfluxDB bucket not set. Using default.")

        # Other existing settings remain the same
        self.REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')

        # AWS settings (remaining unchanged)
        self.AWS_CLIENT_ID = self.serial_number
        # ... (rest of the existing AWS settings)

        # System and other settings (remaining unchanged)
        self.COLLECTION_INTERVAL = 30
        self.NULL = -9999

settings = Settings()