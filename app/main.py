import asyncio
import signal
import json
from typing import Optional
from utils.validator import validate_config
from utils.logging_setup import local_logger as logger
from core.relay_manager import RelayManager
from core.relay_monitor import RelayMonitor
from core.processor import GeneralProcessor, RelayProcessor
from core.cell import CellularData
from core.net import NetworkData
from core.env import EnvironmentalData
from aws.manager import AWSManager

class ApplicationManager:
    def __init__(self):
        self.tasks = []
        self.config = None
        self.relay_manager = None
        self.aws_manager = AWSManager()
        self.shutdown_event: Optional[asyncio.Event] = None
        self.shutdown_signal_received = False

    def setup_signal_handlers(self):
        self.shutdown_event = asyncio.Event()
        
        def handle_shutdown_signal(signum, frame):
            signame = signal.Signals(signum).name
            logger.info(f"Received shutdon signal {signame}")
            if not self.shutdown_signal_received:
                self.shutdown_signal_received = True
                # Use call_soon_threadsafe since signals can come from different threads
                asyncio.get_event_loop().call_soon_threadsafe(self.shutdown_event.set)
        # SIGTERM is the signal sent by Docker to stop the container
        signal.signal(signal.SIGTERM, handle_shutdown_signal)
        # SIGINT is the signal sent by the user to stop the container
        signal.signal(signal.SIGINT, handle_shutdown_signal)

    async def initialize_relay_tasks(self):
        """Initialize tasks for relay monitoring and processing."""
        for relay_id, relay_config in self.config.relays.items():
            should_monitor = relay_config.monitor
            has_schedule = (hasattr(relay_config, 'schedule') and 
                          relay_config.schedule and 
                          relay_config.schedule.enabled)
            if should_monitor or has_schedule:
                monitor = RelayMonitor(relay_id, relay_config, relay_manager=self.relay_manager)
                monitor_task = asyncio.create_task(monitor.start())
                self.tasks.append(monitor_task)

                processor=RelayProcessor(relay_id)
                processor_task = asyncio.create_task(processor.run())
                self.tasks.append(processor_task)
                logger.debug(f"Relay {relay_id}: monitoring and processing tasks created.")
            else:
                logger.debug(f"No monitoring or scheduling configured for relay {relay_id}.")

    async def initialize_general_tasks(self):
        """Initialize tasks for general data collection and processing."""
        collectors = [
            ('network', NetworkData()),
            ('cellular', CellularData()),
            ('environmental', EnvironmentalData())
        ]
        for name, collector in collectors:
            collector_task = asyncio.create_task(collector.run())
            self.tasks.append(collector_task)

        streams = [name for name, _ in collectors]
        general_processor = GeneralProcessor(streams=streams)
        processor_task = asyncio.create_task(general_processor.run())
        self.tasks.append(processor_task)

    async def setup(self):
        """Initialize all application components"""
        try:
            # Setup shutdown signal handlers
            self.setup_signal_handlers()

            # Validate Configuration
            logger.info("Validating configuration...")
            self.config = validate_config()

            # Initialize Relay Manager
            logger.info("Initializing Relay Manager...")
            self.relay_manager = RelayManager(self.config.relays)
            await self.relay_manager.init()

            # Initialize AWS Manager and wait for connection
            logger.info("Setting up AWS components...")
            await self.aws_manager.setup()
            
            # Ensure AWS is connected before proceeding
            if self.aws_manager.is_connected:
                logger.info("AWS connection established")
                # Initialize shadow state if available
                if self.aws_manager.shadow_manager:
                    logger.info("Reading shadow file...")
                    with open('/utils/json/shadow.json', 'r') as f:
                        initial_state = json.load(f)
                    logger.info("Shadow file read successfully.")
                    await self.aws_manager.shadow_manager.update_shadow(initial_state)
            else:
                logger.warning("AWS connection not established")

            # Initialize application tasks
            logger.info("Initializing tasks...")
            await self.initialize_relay_tasks()
            await self.initialize_general_tasks()

            if not self.tasks:
                logger.warning("No tasks have been initialized")
            else:
                logger.info(f"All tasks initialized: {len(self.tasks)} tasks created")
                
        except Exception as e:
            logger.error(f"Setup failed: {e}", exc_info=True)
            raise  # Re-raise to ensure proper shutdown
            
    async def run(self):
        """Main application run loop."""
        try:
            logger.info("Starting application...")
            await self.setup()
            
            if self.tasks:
                logger.info(f"Running {len(self.tasks)} tasks...")
                # Wait for either tasks to complete or shutdown signal
                await asyncio.gather(
                    self.shutdown_event.wait(),
                    *self.tasks,
                    )
            
        except asyncio.CancelledError:
            logger.info("Application shutdown initiated...")
        except Exception as e:
            logger.error(f"Application error: {e}", exc_info=True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Clean shutdown of all components."""
        logger.info("Initiating graceful shutdown...")
        
        try:
            # Cancel all tasks
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            if self.tasks:
                # Wait for tasks to complete with timeout
                try:
                    async with asyncio.timeout(10):  # 10 second timeout
                        await asyncio.gather(*self.tasks, return_exceptions=True)
                except asyncio.TimeoutError:
                    logger.warning("Some tasks did not complete within shutdown timeout")
            
            # Shutdown AWS components
            if self.aws_manager:
                await self.aws_manager.shutdown()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            logger.info("Application shutdown complete.")

async def main():
    app = ApplicationManager()
    await app.run()

if __name__=="__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
