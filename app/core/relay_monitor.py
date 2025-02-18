import asyncio
from typing import Dict
import board
import adafruit_ina260
from utils.validator import RelayConfig
from utils.logging_setup import local_logger as logger
from utils.singleton import RedisClient
from core.rules_engine import RulesEngine
from core.schedule_engine import ScheduleEngine
from core.relay_manager import RelayManager

class RelayMonitor:
    def __init__(self, relay_id: str, relay_config: RelayConfig, relay_manager: RelayManager):
        """
        Initializes the RelayMonitor with the given relay ID, configuration, and a shared RelayManager.

        Args:
            relay_id (str): The identifier for the relay.
            relay_config (RelayConfig): The configuration for the relay.
            relay_manager (RelayManager): The RelayManager instance for controlling relay states.
        """
        self.relay_id = relay_id
        self.config = relay_config
        self.name = relay_config.name
        self.pin = relay_config.pin
        self.address = int(relay_config.address, 16)
        self.boot_power = relay_config.boot_power
        self.monitor = relay_config.monitor
        self.schedule = relay_config.schedule
        self.rules = relay_config.rules if relay_config.rules else {}
        self.collection_interval = 1
        self.relay_manager = relay_manager

        # Initialize RulesEngine with Rule objects
        self.rules_engine = RulesEngine(self.relay_id, self.rules, relay_manager=self.relay_manager)
        self.schedule_engine = ScheduleEngine(self.relay_id, self.schedule)
        self.state = self.boot_power
        self.i2c = None
        self.sensor = None
        self.redis = None
    
    async def start(self):
        await self.init_redis()
        tasks = []
        if self.schedule_engine.is_enabled():
            tasks.append(self.manage_schedule())
        else:
            logger.debug(f"Schedule disabled for {self.relay_id}")

        if self.monitor:
            try:
                self.i2c = board.I2C()
                self.sensor = adafruit_ina260.INA260(self.i2c, address=self.address)
                tasks.append(self.collect_data_loop())
                logger.debug(f"Sensor initialized for relay {self.relay_id}")
            except ValueError as e:
                logger.error(f"Error initializing sensor for relay {self.relay_id}: {e}")
                self.monitor = False
        else:
            logger.debug(f"Monitoring disabled for {self.relay_id}")

        if tasks:
            await asyncio.gather(*tasks)
        else:
            logger.warning(f"No tasks running for relay {self.relay_id}")
    
    async def init_redis(self):
        try:
            self.redis = await RedisClient.get_instance()
            logger.debug("Redis client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")

    async def manage_schedule(self):
        """
        Manages the relay schedule by checking the desired state from the schedule engine
        and updating the relay state if necessary.
        """
        while True:
            try:
                new_state = self.schedule_engine.get_desired_state()
                if new_state != self.state:
                    await self.set_relay_state(new_state)
            except Exception as e:
                logger.error(f"Error managing schedule for relay {self.relay_id}: {e}")
            await asyncio.sleep(60)  # Check every minute
    
    async def set_relay_state(self, state: bool):
        self.state = state
        logger.info(f"Relay {self.relay_id} state set to {'ON' if state else 'OFF'}")
        if state:
            await self.relay_manager.set_relay_on(self.relay_id)
        else:
            await self.relay_manager.set_relay_off(self.relay_id)
    
    async def collect_data_loop(self):
        """
        Collects data from the sensor in a loop and evaluates rules based on the collected data.
        """
        while True:
            try:
                data = await self.collect_data()
                await self.stream_data(data)
                await self.rules_engine.evaluate_rules(data)
                logger.debug(f"Collected and evaluated data for relay {self.relay_id}: {data}")
            except Exception as e:
                logger.error(f"Error collecting data for relay {self.relay_id}: {e}")
            await asyncio.sleep(self.collection_interval)
    
    async def collect_data(self) -> Dict[str, float]:
        """
        Collects voltage, power, and current data from the sensor.

        Returns:
            Dict[str, float]: A dictionary containing the collected data.
        """
        volts = await asyncio.to_thread(lambda: round(self.sensor.voltage, 2))
        watts = await asyncio.to_thread(lambda: round(self.sensor.power / 1000, 2))
        amps = await asyncio.to_thread(lambda: round(self.sensor.current / 1000, 2))
        return {"relay": self.relay_id, "volts": volts, "amps": amps, "watts": watts}
    
    async def stream_data(self, data: Dict[str, float]):
        """
        Streams the collected data to Redis for storage and further processing.

        Args:
            data (Dict[str, float]): The data to be streamed.
        """
        if not self.redis:
            logger.error("Redis client not initialized")
            return
        try:
            await self.redis.xadd(self.relay_id, data)
            logger.debug(f"Data streamed for relay {self.relay_id}: {data}")
        except Exception as e:
            logger.error(f"Error streaming data for relay {self.relay_id}: {e}")
