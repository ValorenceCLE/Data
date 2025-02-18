import asyncio
from awsiot import iotshadow, mqtt5
from utils.logging_setup import local_logger as logger
from utils.config import settings
from aws.client import AWSIoTClient

class ShadowManager:
    def __init__(self, mqtt_client: AWSIoTClient):
        self.mqtt_client = mqtt_client
        self.mqtt_connection = mqtt_client.get_mqtt_connection()
        self.shadow_client = None
        self.thing_name = settings.AWS_CLIENT_ID
        self._setup_shadow_client()

    def _setup_shadow_client(self):
        """Safely setup shadow client"""
        try:
            if self.mqtt_connection:
                self.shadow_client = iotshadow.IotShadowClient(self.mqtt_connection)
                self._setup_subscriptions()
            else:
                logger.debug("No MQTT connection available for shadow client")
        except Exception as e:
            logger.debug(f"Failed to setup shadow client: {e}")

    def _setup_subscriptions(self):
        """Subscribe to necessary shadow topics with error handling"""
        if not self.shadow_client:
            logger.debug("No shadow client available for subscriptions")
            return

        subscriptions = [
            (self.shadow_client.subscribe_to_get_shadow_accepted,
             iotshadow.GetShadowSubscriptionRequest,
             self._on_get_shadow_accepted),
            (self.shadow_client.subscribe_to_get_shadow_rejected,
             iotshadow.GetShadowSubscriptionRequest,
             self._on_get_shadow_rejected),
            (self.shadow_client.subscribe_to_update_shadow_accepted,
             iotshadow.UpdateShadowSubscriptionRequest,
             self._on_update_shadow_accepted),
            (self.shadow_client.subscribe_to_update_shadow_rejected,
             iotshadow.UpdateShadowSubscriptionRequest,
             self._on_update_shadow_rejected),
            (self.shadow_client.subscribe_to_delete_shadow_accepted,
             iotshadow.DeleteShadowSubscriptionRequest,
             self._on_delete_shadow_accepted),
            (self.shadow_client.subscribe_to_delete_shadow_rejected,
             iotshadow.DeleteShadowSubscriptionRequest,
             self._on_delete_shadow_rejected)
        ]

        for subscribe_method, request_class, callback in subscriptions:
            try:
                subscribe_method(
                    request=request_class(thing_name=self.thing_name),
                    qos=mqtt5.QoS.AT_LEAST_ONCE,
                    callback=callback
                )
            except Exception as e:
                logger.debug(f"Failed to setup subscription: {e}")

    async def get_shadow(self):
        """Retrieve the shadow asynchronously with timeout"""
        if not self.shadow_client:
            logger.debug("No shadow client available for get operation")
            return None

        request = iotshadow.GetShadowRequest(thing_name=self.thing_name)
        try:
            # Add timeout to prevent blocking
            return await asyncio.wait_for(
                self._publish_async(
                    self.shadow_client.publish_get_shadow,
                    request,
                    mqtt5.QoS.AT_LEAST_ONCE
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.debug("Get shadow operation timed out")
            return None
        except Exception as e:
            logger.debug(f"Failed to get shadow: {e}")
            return None

    async def update_shadow(self, desired_state):
        """Update the shadow asynchronously with timeout"""
        if not self.shadow_client:
            logger.debug("No shadow client available for update operation")
            return None

        try:
            state = iotshadow.ShadowState(desired=desired_state)
            request = iotshadow.UpdateShadowRequest(
                thing_name=self.thing_name,
                state=state
            )
            
            # Add timeout to prevent blocking
            return await asyncio.wait_for(
                self._publish_async(
                    self.shadow_client.publish_update_shadow,
                    request,
                    mqtt5.QoS.AT_LEAST_ONCE
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.debug("Update shadow operation timed out")
            return None
        except Exception as e:
            logger.debug(f"Failed to update shadow: {e}")
            return None

    async def delete_shadow(self):
        """Delete the shadow asynchronously with timeout"""
        if not self.shadow_client:
            logger.debug("No shadow client available for delete operation")
            return None

        request = iotshadow.DeleteShadowRequest(thing_name=self.thing_name)
        try:
            # Add timeout to prevent blocking
            return await asyncio.wait_for(
                self._publish_async(
                    self.shadow_client.publish_delete_shadow,
                    request,
                    mqtt5.QoS.AT_LEAST_ONCE
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.debug("Delete shadow operation timed out")
            return None
        except Exception as e:
            logger.debug(f"Failed to delete shadow: {e}")
            return None

    async def _publish_async(self, publish_method, request, qos):
        """Non-blocking wrapper for publishing to shadow topics"""
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: publish_method(request, qos).result(timeout=2.0)
            )
        except Exception as e:
            logger.debug(f"Shadow publish operation failed: {e}")
            return None

    # Callback methods with error handling
    def _on_get_shadow_accepted(self, response):
        """Handle get shadow success without blocking"""
        try:
            logger.debug(f"GetShadow accepted: {response}")
        except Exception as e:
            logger.debug(f"Error in get shadow callback: {e}")

    def _on_get_shadow_rejected(self, error):
        """Handle get shadow rejection without blocking"""
        try:
            logger.debug(f"GetShadow rejected: {error}")
        except Exception as e:
            logger.debug(f"Error in get shadow rejection callback: {e}")

    def _on_update_shadow_accepted(self, response):
        """Handle update shadow success without blocking"""
        try:
            logger.debug(f"UpdateShadow accepted: {response}")
        except Exception as e:
            logger.debug(f"Error in update shadow callback: {e}")

    def _on_update_shadow_rejected(self, error):
        """Handle update shadow rejection without blocking"""
        try:
            logger.debug(f"UpdateShadow rejected: {error}")
        except Exception as e:
            logger.debug(f"Error in update shadow rejection callback: {e}")

    def _on_delete_shadow_accepted(self, response):
        """Handle delete shadow success without blocking"""
        try:
            logger.debug(f"DeleteShadow accepted: {response}")
        except Exception as e:
            logger.debug(f"Error in delete shadow callback: {e}")

    def _on_delete_shadow_rejected(self, error):
        """Handle delete shadow rejection without blocking"""
        try:
            logger.debug(f"DeleteShadow rejected: {error}")
        except Exception as e:
            logger.debug(f"Error in delete shadow rejection callback: {e}")
