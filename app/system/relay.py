"""
Relay Manager for controlling relay hardware via the local REST API.

This module provides a unified interface for controlling relays, but no longer
accesses the GPIO hardware directly. Instead, it sends HTTP requests to the REST API
endpoints (which use our combined authentication dependency) so that only one part
of the system is directly touching the hardware.
"""
import asyncio
from typing import Dict, List, Optional
import logging
import aiohttp
from app.utils.validator import RelayConfig
from app.utils.config import settings
import os
logger = logging.getLogger("RelayManager")
logger.setLevel(logging.DEBUG)

class RelayManager:
    def __init__(self, relays: List[RelayConfig]):
        """
        Initialize the RelayManager.

        Args:
            relays (List[RelayConfig]): List of relay configurations.
        """
        self.relays = relays
        
        # Instead of using the container's IP, use the Docker host's network alias.
        # Optionally allow overriding via an environment variable.
        host_ip = os.getenv("HOST_IP", "host.docker.internal")
        self.api_url = f"https://{host_ip}/api"
        
        self.internal_key = settings.SECRET_KEY
        self.initialized = False
        self.session = None

    async def init(self) -> bool:
        """
        Initialize the relay manager.

        Since this implementation uses HTTP requests to an already-running REST API,
        there is no actual hardware to initialize. This method exists only for
        compatibility with the previous interface.
        
        Returns:
            bool: Always True.
        """
        # Create a persistent session for all API requests
        self.session = aiohttp.ClientSession()
        self.initialized = True
        logger.info("RelayManager initialized (using REST API)")
        return True

    def _get_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "X-Internal-API-Key": self.internal_key
        }

    async def set_relay_on(self, relay_id: str) -> bool:
        """
        Turn a relay ON by posting to the REST API endpoint.
        """
        url = f"{self.api_url}/io/{relay_id}/state/on"
        try:
            async with self.session.post(url, headers=self._get_headers(), ssl=False) as response:
                response.raise_for_status()
                data = await response.json()
                success = data.get("status") == "success"
                if success:
                    logger.info(f"Relay {relay_id} turned ON via API.")
                else:
                    logger.error(f"Failed to turn relay {relay_id} ON via API: {data.get('message', 'Unknown error')}")
                return success
        except Exception as e:
            logger.exception(f"Error turning relay {relay_id} ON via API: {e}")
            return False

    async def set_relay_off(self, relay_id: str) -> bool:
        """
        Turn a relay OFF by posting to the REST API endpoint.
        """
        url = f"{self.api_url}/io/{relay_id}/state/off"
        try:
            async with self.session.post(url, headers=self._get_headers(), ssl=False) as response:
                response.raise_for_status()
                data = await response.json()
                success = data.get("status") == "success"
                if success:
                    logger.info(f"Relay {relay_id} turned OFF via API.")
                else:
                    logger.error(f"Failed to turn relay {relay_id} OFF via API: {data.get('message', 'Unknown error')}")
                return success
        except Exception as e:
            logger.exception(f"Error turning relay {relay_id} OFF via API: {e}")
            return False

    async def pulse_relay(self, relay_id: str, duration: float = None) -> bool:
        """
        Pulse the relay (toggle its state briefly) via the REST API.
        If duration is not provided, uses the relay configuration's pulse_time.
        """
        if duration is None:
            relay_config = next((r for r in self.relays if r.id == relay_id), None)
            duration = relay_config.pulse_time if relay_config else 5

        url = f"{self.api_url}/io/{relay_id}/state/pulse"
        try:
            async with self.session.post(url, headers=self._get_headers(), ssl=False) as response:
                response.raise_for_status()
                data = await response.json()
                success = data.get("status") == "success"
                if success:
                    logger.info(f"Relay {relay_id} pulsed via API for {duration} seconds.")
                else:
                    logger.error(f"Failed to pulse relay {relay_id} via API: {data.get('message', 'Unknown error')}")
                return success
        except Exception as e:
            logger.exception(f"Error pulsing relay {relay_id} via API: {e}")
            return False

    async def get_relay_state(self, relay_id: str) -> Optional[int]:
        """
        Get the current state of a relay via the REST API.
        """
        url = f"{self.api_url}/io/relays/state"
        try:
            async with self.session.get(url, headers=self._get_headers(), ssl=False) as response:
                response.raise_for_status()
                states = await response.json()  # Should be a dict mapping relay IDs to states
                return states.get(relay_id)
        except Exception as e:
            logger.exception(f"Error retrieving state for relay {relay_id}: {e}")
            return None

    async def get_all_relay_states(self) -> Dict[str, int]:
        """
        Retrieve the current states of all relays via the REST API.
        """
        url = f"{self.api_url}/io/relays/state"
        try:
            async with self.session.get(url, headers=self._get_headers(), ssl=False) as response:
                response.raise_for_status()
                states = await response.json()
                return states
        except Exception as e:
            logger.exception(f"Error retrieving all relay states: {e}")
            return {}

    def get_relay_by_id(self, relay_id: str) -> Optional[RelayConfig]:
        """
        Get a relay configuration by its ID.
        """
        for relay in self.relays:
            if relay.id == relay_id:
                return relay
        return None

    def get_relay_names(self) -> Dict[str, str]:
        """
        Get a mapping of relay IDs to relay names.
        """
        return {relay.id: relay.name for relay in self.relays}

    async def shutdown(self):
        """
        Perform any shutdown tasks if necessary.
        """
        logger.info("Shutting down relay manager")
        # Close the aiohttp session when shutting down
        if self.session and not self.session.closed:
            await self.session.close()
