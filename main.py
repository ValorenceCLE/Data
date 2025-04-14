#!/usr/bin/env python3
"""
Main entry point for the DPM system.

This module initializes and runs all system components, including
applying network and time settings.
"""
import os
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging
from app.utils.validator import load_config
from app.system.network import NetworkManager
from app.system.time import TimeManager
from app.system.relay import RelayManager
from app.core.tasks import TaskManager
from app.core.schedule import ScheduleManager
from app.data.collectors import DataCollectionManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Main")

async def main():
    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        logger.info(f"Loading configuration from {config_path}")
        config = load_config(config_path)
        logger.info("Configuration loaded successfully")
        
        # Apply system settings
        logger.info("Applying system settings...")
        
        # Apply network settings
        logger.info("Initializing network manager...")
        network_manager = NetworkManager(config.network)
        logger.info("Applying network configuration...")
        network_success = await network_manager.apply_config()
        if network_success:
            logger.info("Network settings applied successfully")
        else:
            logger.warning("Failed to apply network settings, continuing with current network configuration")
        
        # Apply time settings
        logger.info("Initializing time manager...")
        time_manager = TimeManager(config.date_time)
        logger.info("Applying time configuration...")
        time_success = await time_manager.apply_config()
        if time_success:
            logger.info("Time settings applied successfully")
        else:
            logger.warning("Failed to apply time settings, continuing with current time configuration")
        
        # Initialize relay manager
        logger.info("Initializing relay manager...")
        relay_manager = RelayManager(config.relays)
        init_success = await relay_manager.init()
        if init_success:
            logger.info("Relay manager initialized successfully")
        else:
            logger.error("Failed to initialize relay manager")
            return
        
        # Initialize task manager
        logger.info("Initializing task manager...")
        task_manager = TaskManager(config.tasks, relay_manager)
        logger.info("Task manager initialized")
        
        # Initialize schedule manager
        logger.info("Initializing schedule manager...")
        schedule_manager = ScheduleManager(config.relays, relay_manager)
        logger.info("Schedule manager initialized")
        
        # Initialize data collection manager
        logger.info("Initializing data collection manager...")
        data_manager = DataCollectionManager(config, task_manager)
        data_init_success = await data_manager.initialize()
        if data_init_success:
            logger.info("Data collection manager initialized successfully")
        else:
            logger.warning("Failed to fully initialize data collection manager, some features may be limited")
        
        # Start all services concurrently
        try:
            logger.info("Starting all services...")
            # Create tasks for each service
            task_manager_task = asyncio.create_task(task_manager.run())
            schedule_manager_task = asyncio.create_task(schedule_manager.run())
            data_manager_task = asyncio.create_task(data_manager.run())
            
            # Print status message
            logger.info("All services are now running. Press Ctrl+C to stop.")
            
            # Wait for all tasks concurrently
            await asyncio.gather(
                task_manager_task,
                schedule_manager_task,
                data_manager_task
            )
        except KeyboardInterrupt:
            logger.info("Shutdown requested via keyboard interrupt")
        except asyncio.CancelledError:
            logger.info("Tasks cancelled")
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        finally:
            # Graceful shutdown
            logger.info("Shutting down all services...")
            try:
                # Use gather with return_exceptions to ensure all services get shutdown attempts
                await asyncio.gather(
                    data_manager.shutdown(),
                    task_manager.shutdown(),
                    schedule_manager.shutdown(),
                    relay_manager.shutdown(),
                    return_exceptions=True
                )
                logger.info("All services shut down")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")
    except Exception as e:
        logger.error(f"Fatal error in main function: {e}")

if __name__ == "__main__":
    # Handle KeyboardInterrupt gracefully
    try:
        # Run the async function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        exit(1)