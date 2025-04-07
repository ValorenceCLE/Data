"""
Data collection manager for sensors and metrics.

This module handles collecting data from various sensors and streams the data
to the task manager for evaluation.
"""
import os
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
import logging
from app.utils.validator import Config, load_config
from app.core.tasks import TaskManager
from app.data.collectors import DataCollectionManager


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# If the config file is in the same directory as main.py

config_path = os.path.join(os.path.dirname(__file__), "config.json")
config = load_config(config_path)


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