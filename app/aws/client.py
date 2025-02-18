import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from awsiot import mqtt5_client_builder
from awscrt import mqtt5
from utils.logging_setup import local_logger as logger
from utils.config import settings

class AWSIoTClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True
        self.client = None
        self.device_id = settings.AWS_CLIENT_ID
        self.TIMEOUT = 100
        self.executor = ThreadPoolExecutor()
        self.is_connected = False
        self._initialize_client()

    def _initialize_client(self):
        try:
            self.client = mqtt5_client_builder.mtls_from_path(
                endpoint=settings.AWS_ENDPOINT,
                port=8883,
                cert_filepath=settings.DEVICE_COMBINED_CRT,
                pri_key_filepath=settings.DEVICE_KEY,
                ca_filepath=settings.AWS_ROOT_CA,
                http_proxy_options=None,
                on_publish_received=self.on_publish_received,
                on_lifecycle_stopped=self.on_lifecycle_stopped,
                on_lifecycle_connection_success=self.on_lifecycle_connection_success,
                on_lifecycle_connection_failure=self.on_lifecycle_connection_failure,
                client_id=settings.AWS_CLIENT_ID,
                keep_alive_seconds=60,
                clean_session=False
            )
        except Exception as e:
            logger.error(f"Failed to initialize AWS IoT client: {e}")
            self.client = None

    def get_mqtt_connection(self):
        return self.client if self.is_connected else None

    async def start(self):
        try:
            if not self.client:
                logger.warning("No MQTT client available - skipping start")
                return
            await asyncio.get_event_loop().run_in_executor(self.executor, self._start_sync)
        except Exception as e:
            logger.error(f"Error in start: {e}")
            self.is_connected = False

    def _start_sync(self):
        try:
            logger.debug("Starting AWS IoT client...")
            self.client.start()
            # Connection success will be set by the lifecycle callback
            logger.debug("AWS IoT client start initiated.")
        except Exception as e:
            logger.error(f"Error starting AWS IoT client: {e}")
            self.is_connected = False

    async def stop(self):
        try:
            if not self.client:
                return
            await asyncio.get_event_loop().run_in_executor(self.executor, self._stop_sync)
        except Exception as e:
            logger.error(f"Error in stop: {e}")
        finally:
            self.is_connected = False

    def _stop_sync(self):
        try:
            logger.info("Stopping AWS IoT client...")
            self.client.stop()
            logger.info("AWS IoT client stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping AWS IoT client: {e}")

    async def publish(self, topic, payload, source=None):
        # Always handle publishes gracefully, even if not connected
        try:
            await asyncio.get_event_loop().run_in_executor(
                self.executor, self._publish_sync, topic, payload, source
            )
        except Exception as e:
            # Log and move on without raising - no crash for the caller
            logger.debug(f"Failed to publish to {topic}: {e}")

    def _publish_sync(self, topic, payload, source=None):
        # If not connected or no client, just log and return - no crash
        if not self.client or not self.is_connected:
            logger.debug(f"Skipping publish to '{topic}' - client not connected")
            return

        if not isinstance(payload, dict):
            logger.debug("Skipping publish - payload must be a dictionary")
            return

        payload["device_id"] = self.device_id
        if source:
            payload["source"] = source

        json_payload = json.dumps(payload)
        prefixed_topic = f"{self.device_id}/{topic}"
        logger.debug(f"Attempting publish to '{prefixed_topic}'")

        try:
            self.client.publish(
                mqtt5.PublishPacket(
                    topic=prefixed_topic,
                    payload=json_payload.encode("utf-8"),
                    qos=mqtt5.QoS.AT_LEAST_ONCE,
                )
            )
            logger.debug("Publish operation successful.")
        except Exception as e:
            logger.debug(f"Error in publish_sync: {e}")

    async def subscribe(self, topic, callback=None):
        # Also handle subscribe gracefully
        try:
            await asyncio.get_event_loop().run_in_executor(
                self.executor, self._subscribe_sync, topic, callback
            )
        except Exception as e:
            logger.debug(f"Failed to subscribe to {topic}: {e}")

    def _subscribe_sync(self, topic, callback=None):
        if not self.client or not self.is_connected:
            logger.debug(f"Skipping subscribe to '{topic}' - client not connected")
            return

        logger.debug(f"Attempting subscribe to '{topic}'")
        try:
            self.client.subscribe(
                subscribe_packet=mqtt5.SubscribePacket(
                    subscriptions=[
                        mqtt5.Subscription(
                            topic_filter=f"{self.device_id}/{topic}",
                            qos=mqtt5.QoS.AT_LEAST_ONCE,
                        )
                    ]
                )
            )
            if callback:
                self.client.on_publish_received = callback
            logger.debug(f"Subscribed to topic '{topic}' successfully.")
        except Exception as e:
            logger.debug(f"Error in subscribe_sync: {e}")

    def on_publish_received(self, publish_packet_data):
        try:
            publish_packet = publish_packet_data.publish_packet
            logger.debug(
                f"Received message from topic '{publish_packet.topic}'"
            )
        except Exception as e:
            logger.debug(f"Error in publish callback: {e}")

    def on_lifecycle_stopped(self, lifecycle_stopped_data):
        try:
            logger.debug("MQTT connection stopped")
            self.is_connected = False
        except Exception as e:
            logger.debug(f"Error in lifecycle stopped callback: {e}")

    def on_lifecycle_connection_success(self, lifecycle_connect_success_data):
        try:
            logger.debug("MQTT connection successful")
            self.is_connected = True
        except Exception as e:
            logger.debug(f"Error in connection success callback: {e}")

    def on_lifecycle_connection_failure(self, lifecycle_connection_failure):
        try:
            logger.warning(f"MQTT connection failed: {lifecycle_connection_failure.exception}")
            self.is_connected = False
        except Exception as e:
            logger.debug(f"Error in connection failure callback: {e}")

_client_instance = None

def _get_client_instance():
    global _client_instance
    if _client_instance is None:
        _client_instance = AWSIoTClient()
    return _client_instance

async def start():
    await _get_client_instance().start()

async def stop():
    await _get_client_instance().stop()

async def publish(topic, payload, source=None):
    await _get_client_instance().publish(topic, payload, source)

async def subscribe(topic, callback=None):
    await _get_client_instance().subscribe(topic, callback)
