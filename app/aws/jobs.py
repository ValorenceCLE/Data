import time
import json
import asyncio
import subprocess
from abc import ABC, abstractmethod
from awscrt import mqtt5, io
from awsiot import iotjobs
from utils.logging_setup import local_logger as logger
from utils.config import settings
from aws.client import AWSIoTClient

class JobHandler(ABC):
    # Base class for Job Handlers
    @abstractmethod
    async def execute(self, job_id: str, job_document: dict, version_number: int)-> tuple[bool, dict]:
        """
        Execute the job
        Returns: (success: bool, status_details: dict)
        """
        pass

class RebootJobHandler(JobHandler):
    # Handle device reboot commands from AWS IoT Jobs
    async def execute(self, job_id:str, job_document:dict, version_number:int)-> tuple[bool, dict]:
        message=job_document.get('message', 'Rebooting device...')
        logger.info(f"Rebooting device: {message}")

        status_details = {
            'status': 'Reboot Initiated',
            'message': message,
            'completedAt': int(time.time())
        }
        # Schedule the reboot to happen after the job is marked as COMPLETED
        asyncio.create_task(self._delayed_reboot())
        return True, status_details
    
    async def _delayed_reboot(self):
        # Reboot the device after a short delay to ensure the job is marked as COMPLETED
        logger.info("Initiating system reboot...")
        subprocess.run(['sudo', 'reboot'])

"""
Add more classed for job handlers here, follow the same pattern as RebootJobHandler
"""


class JobManager:
    def __init__(self, mqtt_client: AWSIoTClient):
        self.mqtt_client = mqtt_client
        self.mqtt_connection = mqtt_client.get_mqtt_connection()  # Get the underlying connection
        self.thing_name = settings.AWS_CLIENT_ID
        
        # State Tracking
        self._running = False
        self._check_interval = 60
        self.is_connected = False
        self.jobs_client = None
        self.current_job = None
        self.processing_job = False
        self.loop = asyncio.get_event_loop()

        # Register the job handlers, add more as needed
        self.job_handlers = {
            'reboot': RebootJobHandler()
        }

        # If connected, create the JobsClient immediately
        if self.mqtt_connection:
            self.jobs_client = iotjobs.IotJobsClient(self.mqtt_connection)
            self.is_connected = True
    
    async def connect(self):
        # Establish the JobsClient connection
        if self.mqtt_connection:
            await self.setup_jobs()
            return
    
    async def setup_jobs(self):
        # Subscribe to the Job topics
        logger.debug("Subscribing to Job topics...")
        try:
            notify_requests = iotjobs.JobExecutionsChangedSubscriptionRequest(
                thing_name=self.thing_name
            )
            self.jobs_client.subscribe_to_job_executions_changed_events(
                request=notify_requests,
                qos=mqtt5.QoS.AT_LEAST_ONCE,
                callback=self.on_job_notification
            )
            logger.debug("Subscribed to Job topics successfully.")

        except Exception as e:
            logger.error(f"Failed to subscribe to Job topics: {e}")
    
    def on_job_notification(self, event):
        # Handle job notifications
        logger.info(f"Received Job Notification: {event}")
        try:
            if hasattr(event,'jobs'):
                for status, job_list in event.jobs.items():
                    for job in job_list:
                        logger.info(f"Job ID: {job['jobId']}")
                        logger.info(f"Status: {status}")
                        logger.info(f"Details: {json.dumps(job)}")
        except Exception as e:
            logger.error(f"Error processing Job Notification: {e}")

    async def start_next_job(self):
        if not self.processing_job and self.current_job is None:
            logger.debug("Checking for new Jobs...")
            try:
                request = iotjobs.StartNextPendingJobExecutionRequest(
                    thing_name=self.thing_name
                )
                self.jobs_client.publish_start_next_pending_job_execution(
                    request=request,
                    qos=mqtt5.QoS.AT_LEAST_ONCE
                )
            except Exception as e:
                logger.error(f"Error starting next job: {e}")
    
    def on_publish_received(self, publish_packet_data):
        # Handle received MQTT messages
        try:
            topuc = publish_packet_data.publish_packet.topic
            if isinstance(topic, bytes):
                topic = topic.decode('utf-8')
            payload = publish_packet_data.publish_packet.payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
                try:
                    payload_json = json.loads(payload)
                    logger.info(f"Received message from topic '{topic}': {payload_json}")
                    if 'start-next/accepted'in topic:
                        if 'execution' in payload_json:
                            if not self.processing_job:
                                self.processing_job = True
                                execution = payload_json['execution']
                                asyncio.run_coroutine_threadsafe(
                                    self.handle_job(
                                        execution['jobId'],
                                        execution['jobDocument'],
                                        execution['versionNumber']
                                    ),
                                    self.loop
                                )
                        else:
                            logger.debug("No pending jobs.")
                except json.JSONDecodeError:
                    logger.error(f"Received message on topic '{topic}': {payload}")
        except Exception as e:
            print(f"Error processing received message: {e}")

    async def handle_job(self, job_id, job_document, version_number):
        # Handle Job Execution
        try:
            logger.info(f"Handling Job: {job_id} (Version: {version_number})")

            # Update job execution status
            await self.update_job_execution(
                job_id,
                'IN_PROGRESS',
                {'status': 'Starting job execution'},
                version_number
            )

            # Get the appropriate job handler
            operation = job_document.get('operation', '').lower()
            handler = self.job_handlers.get(operation)

            if handler:
                # Execute the job
                success, status_details = await handler.execute(job_id, job_document, version_number)
                final_status = 'SUCCEEDED' if success else 'FAILED'
                await self.update_job_execution(
                    job_id,
                    final_status,
                    status_details,
                    version_number + 1
                )
            else:
                # Handle Unknown operation
                await self.update_job_execution(
                    job_id,
                    'FAILED',
                    {
                        'status': 'Failed',
                        'error': f'Unknown operation: {operation}'
                    },
                    version_number + 1
                )
        
        except Exception as e:
            logger.error(f"Error handling job: {e}")
            await self.update_job_execution(
                job_id,
                'FAILED',
                {
                    'error': str(e),
                    'status': 'Failed'
                },
                version_number + 1
            )
        finally:
            self.current_job = None
            self.processing_job = False
            
    async def update_job_execution(self, job_id, status, status_details, version_number):
        # Update the Job Execution status
        logger.info(f"Updating Job Execution: {job_id} to {status}")
        try:
            status_details['timestamp'] = int(time.time())
            request = iotjobs.UpdateJobExecutionRequest(
                thing_name=self.thing_name,
                job_id=job_id,
                status=status,
                status_details=status_details,
                expected_version=version_number,
                execution_number=1,
                include_job_execution_state=True
            )
            self.jobs_client.publish_update_job_execution(
                request=request,
                qos=mqtt5.QoS.AT_LEAST_ONCE
            )
        except Exception as e:
            logger.error(f"Error updating Job Execution: {e}")

    def on_lifecycle_stopped(self, lifecycle_stopped_data):
        logger.info("MQTT connection stopped")
        self.is_connected = False
        self.processing_job = False

    def on_lifecycle_connection_success(self, lifecycle_connect_success_data):
        self.is_connected = True

    def on_lifecycle_connection_failure(self, lifecycle_connection_failure):
        logger.warning(f"MQTT connection failed: {lifecycle_connection_failure.exception}")
        self.is_connected = False
        self.processing_job = False

    async def start_job_processing(self):
        """
        Main entrypoint to start the job manager.
        This will connect, setup jobs, and continuously monitor for new jobs.
        """
        if self._running:
            logger.info("Job Manager already running.")
            return
        try:
            self._running=True
            logger.info("Starting Job Manager...")

            # Ensure the JobsClient is connected
            await self.connect()

            # Setup initial jobs subscriptions
            await self.setup_jobs()

            # Start the main loop
            while self._running:
                try:
                    await self.start_next_job()

                    # Wait before checking for new jobs
                    await asyncio.sleep(self._check_interval)
                except Exception as e:
                    logger.error(f"Error in Job Manager loop: {e}")
                    # Wait before retrying
                    await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Fatal Error Job Manager: {e}")
            self._running = False
    
    async def stop_job_processing(self):
        """
        Gracefully stop the job manager.
        """
        logger.info("Stopping job manager...")
        self._running = False
        
        # Wait for current job to complete if one is running
        while self.processing_job:
            logger.info("Waiting for current job to complete...")
            await asyncio.sleep(1)

        logger.info("Job manager stopped")

    @property
    def is_running(self):
        """
        Check if the job manager is currently running.
        """
        return self._running

    def set_check_interval(self, seconds: int):
        """
        Set the interval between job checks.
        """
        self._check_interval = max(30, seconds)