import os
import asyncio
import aiofiles
from functools import lru_cache
from utils.logging_setup import local_logger as logger

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
                self.serial_number=asyncio.run(self.rpi_serial())
        except RuntimeError as e:
            logger.error(f"Error initializing serial number: {e}")
            self.serial_number = None



        # Database settings
        self.TOKEN = os.getenv('DOCKER_INFLUXDB_INIT_ADMIN_TOKEN')
        self.ORG = os.getenv('DOCKER_INFLUXDB_INIT_ORG')
        self.BUCKET = os.getenv('DOCKER_INFLUXDB_INIT_BUCKET')
        self.INFLUXDB_URL = os.getenv('INFLUXDB_URL')

        # Redis settings
        self.REDIS_URL = os.getenv('REDIS_URL')

        # AWS settings
        self.AWS_CLIENT_ID = self.serial_number  # Uses the cached result
        self.AWS_REGION = os.getenv('AWS_REGION')  # AWS Region (us-east-1)
        self.AWS_ENDPOINT = os.getenv('AWS_ENDPOINT')  # AWS IoT endpoint
        self.CERT_DIR = os.getenv('CERT_DIR')  # Directory to store certs
        self.AWS_ROOT_CA = os.getenv('AWS_ROOT_CA')  # Required
        self.DEVICE_ROOT_KEY = os.getenv('DEVICE_ROOT_KEY')  # Required for Generating Device Certs
        self.DEVICE_ROOT_PEM = os.getenv('DEVICE_ROOT_PEM')  # Required for Generating Device Certs
        self.DEVICE_KEY = os.getenv('DEVICE_KEY')  # Generated private key
        self.DEVICE_CSR = os.getenv('DEVICE_CSR')  # Generated CSR
        self.DEVICE_CRT = os.getenv('DEVICE_CRT')  # Generated CRT
        self.DEVICE_COMBINED_CRT = os.getenv('DEVICE_COMBINED_CRT')  # Generated combined CRT
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


        # Certificate subject attributes
        self.COUNTRY_NAME = "US"
        self.STATE_NAME = "Utah"
        self.LOCALITY_NAME = "Logan"
        self.ORGANIZATION_NAME = "Valorence"
        self.ORGANIZATIONAL_UNIT_NAME = "RPi"

        # AWS IoT MQTT topics
        self.RELAY_TOPIC = f"{self.AWS_CLIENT_ID}/relay/data"
        self.NET_TOPIC = f"{self.AWS_CLIENT_ID}/network/data"
        self.CELL_TOPIC = f"{self.AWS_CLIENT_ID}/cellular/data"
        self.ENV_TOPIC = f"{self.AWS_CLIENT_ID}/environmental/data"
        self.ALERTS_TOPIC = f"{self.AWS_CLIENT_ID}/alerts/data"

        # AWS IoT Management topics
        self.GET_SHADOW_TOPIC = f"$aws/things/{self.AWS_CLIENT_ID}/shadow/get"
        self.UPDATE_SHADOW_TOPIC = f"$aws/things/{self.AWS_CLIENT_ID}/shadow/update"

        # System settings
        self.SERIAL_NUMBER = self.serial_number # Uses the cached result

        # SNMP settings
        self.COMMUNITY = 'public'
        self.SNMP_TARGET = '192.168.1.1'
        self.OIDS = {
            'sinr': '.1.3.6.1.4.1.23695.200.1.12.1.1.1.5.0',
            'rsrp': '.1.3.6.1.4.1.23695.200.1.12.1.1.1.7.0',
            'rsrq': '.1.3.6.1.4.1.23695.200.1.12.1.1.1.8.0',
        }
        
        # Network/Ping settings
        self.PING_TARGET = '8.8.8.8'

        # Data collection settings
        self.COLLECTION_INTERVAL = 30
        self.NULL = -9999  # Value to use for missing data, may need to be adjusted based on data type

settings = Settings()
