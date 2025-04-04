"""
Data collection manager for sensors and metrics.

This module handles collecting data from various sensors and streams the data
to the task manager for evaluation.
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
import logging
from app.utils.validator import Config, load_config
from app.core.tasks import TaskManager
from services.smbus import INA260Sensor, SHT30Sensor


# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("DataCollectionManager")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# If the config file is in the same directory as main.py
import os
config_path = os.path.join(os.path.dirname(__file__), "config.json")
config = load_config(config_path)

class DataCollectionManager:
    """
    Manages data collection from various sensors and metrics.
    """
    def __init__(self, config: Config, task_manager: Optional[TaskManager] = None):
        """
        Initialize the DataCollectionManager.
        
        Args:
            config (Config): The system configuration.
            task_manager (Optional[TaskManager]): The task manager for evaluating data against tasks.
        """
        self.config = config
        self.task_manager = task_manager
        self._running = False
        self._collection_interval = 5  # Collect data every 5 seconds
        
        # Track what sensors are available
        self.ina260_sensors: Dict[str, INA260Sensor] = {}
        self.sht30_sensor: Optional[SHT30Sensor] = None
        
        # Track collection tasks
        self.collection_tasks = []
    
    async def initialize(self) -> bool:
        """
        Initialize data collection components.
        
        Returns:
            bool: True if initialization was successful, False otherwise.
        """
        try:
            # Initialize sensors
            await self._initialize_sensors()
            return True
        except Exception as e:
            logger.error(f"Error initializing data collection: {e}")
            return False
    
    async def _initialize_sensors(self):
        """
        Initialize all available sensors.
        """
        # Configuration for INA260 sensors
        INA260_SENSORS = [
            {"id": "relay_1", "sensor": "ina260_1", "address": "0x44"},
            {"id": "relay_2", "sensor": "ina260_2", "address": "0x45"},
            {"id": "relay_3", "sensor": "ina260_3", "address": "0x46"},
            {"id": "relay_4", "sensor": "ina260_4", "address": "0x47"},
            {"id": "relay_5", "sensor": "ina260_5", "address": "0x48"},
            {"id": "relay_6", "sensor": "ina260_6", "address": "0x49"},
            {"id": "main", "sensor": "ina260_7", "address": "0x4B"},
        ]
        
        for sensor_config in INA260_SENSORS:
            try:
                relay_id = sensor_config["id"]
                address = int(sensor_config["address"], 16)
                
                # Skip if relay is not in our config and not 'main'
                if relay_id != "main" and not any(r.id == relay_id for r in self.config.relays):
                    continue
                
                # Create sensor
                sensor = INA260Sensor(address=address)
                self.ina260_sensors[relay_id] = sensor
                logger.debug(f"Initialized INA260 sensor for {relay_id} at address 0x{address:02X}")
            except Exception as e:
                logger.error(f"Failed to initialize INA260 sensor for {sensor_config['id']}: {e}")
        
        # Initialize SHT30 sensor (if available)
        try:
            self.sht30_sensor = SHT30Sensor(address=0x45)
            await self.sht30_sensor.reset()
            logger.debug("Initialized SHT30 environmental sensor")
        except Exception as e:
            logger.error(f"Failed to initialize SHT30 sensor: {e}")
            self.sht30_sensor = None
    
    async def _collect_relay_data(self, relay_id: str, sensor: INA260Sensor):
        """
        Collect data from a relay's INA260 sensor.
        
        Args:
            relay_id (str): The relay identifier.
            sensor (INA260Sensor): The relay's INA260 sensor.
        """
        while self._running:
            try:
                # Read voltage, current, and power
                voltage = await sensor.read_voltage()
                current = await sensor.read_current()
                power = await sensor.read_power()
                
                # Skip if any reading failed
                if voltage is None or current is None or power is None:
                    logger.warning(f"Incomplete sensor readings for {relay_id}")
                    await asyncio.sleep(self._collection_interval)
                    continue
                
                # Send to task manager for evaluation
                if self.task_manager:
                    # Create a copy with only numeric data for task evaluation
                    eval_data = {
                        "volts": voltage,
                        "amps": current,
                        "watts": power
                    }
                    await self.task_manager.evaluate_data(relay_id, eval_data)
                
                # Log periodically
                logger.debug(f"Sensor readings for {relay_id}: {voltage:.2f}V, {current:.3f}A, {power:.2f}W")
                
            except Exception as e:
                logger.error(f"Error collecting data for {relay_id}: {e}")
            
            # Wait until next collection
            await asyncio.sleep(self._collection_interval)
    
    async def _collect_environmental_data(self):
        """
        Collect data from the SHT30 environmental sensor.
        """
        if not self.sht30_sensor:
            logger.warning("No SHT30 sensor available, skipping environmental data collection")
            return
        
        while self._running:
            try:
                # Read temperature and humidity
                temperature = await self.sht30_sensor.read_temperature()
                humidity = await self.sht30_sensor.read_humidity()
                
                # Skip if any reading failed
                if temperature is None or humidity is None:
                    logger.warning("Incomplete environmental sensor readings")
                    await asyncio.sleep(self._collection_interval)
                    continue
                
                # Send to task manager for evaluation
                if self.task_manager:
                    # Create a copy with only numeric data for task evaluation
                    eval_data = {
                        "temperature": temperature,
                        "humidity": humidity
                    }
                    await self.task_manager.evaluate_data("environmental", eval_data)
                
                # Log periodically
                logger.debug(f"Environmental readings: {temperature:.1f}°F, {humidity:.1f}%")
                
            except Exception as e:
                logger.error(f"Error collecting environmental data: {e}")
            
            # Wait until next collection
            await asyncio.sleep(self._collection_interval * 2)  # Collect less frequently
    
    async def run(self):
        """
        Start the data collection.
        """
        if self._running:
            logger.warning("Data collection already running")
            return
        
        self._running = True
        
        # Initialize if not already done
        if not self.ina260_sensors and not self.sht30_sensor:
            await self.initialize()
        
        logger.info("Starting data collection")
        
        # Start a collection task for each sensor
        for relay_id, sensor in self.ina260_sensors.items():
            task = asyncio.create_task(self._collect_relay_data(relay_id, sensor))
            self.collection_tasks.append(task)
            logger.debug(f"Started data collection for {relay_id}")
        
        # Start environmental data collection if sensor is available
        if self.sht30_sensor:
            task = asyncio.create_task(self._collect_environmental_data())
            self.collection_tasks.append(task)
            logger.debug("Started environmental data collection")
        
        # Wait for all tasks to complete (they should run indefinitely)
        try:
            await asyncio.gather(*self.collection_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Data collection tasks cancelled")
        finally:
            self._running = False
    
    async def shutdown(self):
        """
        Stop all data collection.
        """
        if not self._running:
            return
        
        logger.info("Shutting down data collection")
        self._running = False
        
        # Cancel all collection tasks
        for task in self.collection_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.collection_tasks:
            await asyncio.wait(
                self.collection_tasks,
                timeout=5,  # Wait up to 5 seconds
                return_when=asyncio.ALL_COMPLETED
            )
        
        logger.info("Data collection shut down")


if __name__ == "__main__":
    from app.utils.validator import load_config
    import os
    import asyncio
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    config = load_config(config_path)
    
    
    async def main():
        # Load configuration
        
        # Initialize data collection manager
        data_manager = DataCollectionManager(config)
        
        # Start data collection
        try:
            await data_manager.run()
        except KeyboardInterrupt:
            await data_manager.shutdown()
    
    # Run the main function
    asyncio.run(main())