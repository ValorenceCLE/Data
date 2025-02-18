from aws.certificates import CertificateManager
from aws.client import AWSIoTClient
from aws.jobs import JobManager
from aws.shadow import ShadowManager
from utils.logging_setup import local_logger as logger
import asyncio

class AWSManager:
    def __init__(self):
        self.aws_client = None
        self.job_manager = None
        self.shadow_manager = None
        self.is_connected = False

    async def setup(self):
        """Initialize AWS components - continue if AWS unreachable"""
        try:
            # Check certificates
            cert_manager = CertificateManager()
            if not cert_manager.certificate_exists():
                logger.info("Certificates do not exist - generating...")
                cert_manager.create_certificates()

            # Initialize AWS IoT client
            logger.info("Initializing AWS IoT client...")
            self.aws_client = AWSIoTClient()
            await self.aws_client.start()
            # Brief wait to ensure connection
            await asyncio.sleep(1)
            if self.aws_client.is_connected:
                logger.info("AWS IoT client connected")
                self.is_connected = True
                await self._setup_managers()
            else:
                logger.warning("AWS IoT client not connected")
        except Exception as e:
            logger.error(f"AWS setup failed: {e}")
            logger.warning("Continuing without AWS connectivity...")
            self.is_connected = False

    async def _setup_managers(self):
        """Setup job and shadow managers if client is connected"""
        try:
            # Setup Shadow Manager
            self.shadow_manager = ShadowManager(self.aws_client)
            
            # Setup Job Manager
            logger.debug("Starting job processing in its own task...")
            self.job_manager = JobManager(self.aws_client)
            asyncio.create_task(self.job_manager.start_job_processing())
            
        except Exception as e:
            logger.warning(f"Manager setup failed: {e}")

    async def shutdown(self):
        """Shutdown AWS components"""
        try:
            if self.job_manager:
                await self.job_manager.stop_job_processing()
            if self.aws_client:
                await self.aws_client.stop()
        except Exception as e:
            logger.error(f"AWS shutdown error: {e}")