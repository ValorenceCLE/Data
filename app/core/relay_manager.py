# core/relay_manager.py

import asyncio
from typing import Dict
from utils.logging_setup import local_logger as logger
from utils.validator import RelayConfig
import RPi.GPIO as GPIO

class RelayManager:
    """
    The RelayManager is responsible for controlling and maintaining the state of all relays.
    It serves as a single point of truth for relay states, ensuring consistent handling of 
    relay ON/OFF actions requested by rules, schedules, or other components.
    """

    def __init__(self, relay_configs: Dict[str, RelayConfig]):
        """
        Initialize the RelayManager with relay configurations.

        Args:
            relay_configs (Dict[str, RelayConfig]): A dictionary of relay configurations.
        """
        self.relays = {}
        for relay_id, cfg in relay_configs.items():
            self.relays[relay_id] = {
                "pin": cfg.pin,
                "address": cfg.address,
                "current_state": cfg.boot_power  # Initialize with boot_power state
            }
        self.initialized = False
        logger.debug("RelayManager created.")

    async def init(self):
        """
        Initialize the GPIO pins and set them to their boot states.
        This method must be awaited before using other RelayManager methods.
        """
        await asyncio.to_thread(GPIO.setmode, GPIO.BCM)

        # Setup each relay pin as output and set to boot state
        for relay_id, info in self.relays.items():
            pin = info["pin"]
            await asyncio.to_thread(GPIO.setup, pin, GPIO.OUT)
            desired_state = info["current_state"]  # Use current_state as initial state
            await self._set_pin_state(pin, desired_state)
            logger.debug(f"Relay {relay_id} initialized to {'ON' if desired_state else 'OFF'} at boot.")

        self.initialized = True

    async def set_relay_on(self, relay_id: str) -> bool:
        """
        Turn the specified relay ON.

        Args:
            relay_id (str): The identifier of the relay to turn ON.

        Returns:
            bool: True if the relay was turned on, False otherwise.
        """
        if relay_id not in self.relays:
            logger.error(f"Relay {relay_id} not found")
            return False
        pin = self.relays[relay_id]["pin"]
        current_state = await self._read_pin_state(pin)
        if current_state:
            logger.info(f"Relay {relay_id} is already ON.")
            return False
        await self._set_pin_state(pin, True)
        self.relays[relay_id]["current_state"] = True
        logger.info(f"Relay {relay_id} turned ON.")
        return True

    async def set_relay_off(self, relay_id: str) -> bool:
        """
        Turn the specified relay OFF.

        Args:
            relay_id (str): The identifier of the relay to turn OFF.

        Returns:
            bool: True if the relay was turned off, False otherwise.
        """
        if relay_id not in self.relays:
            logger.error(f"Relay {relay_id} not found")
            return False
        pin = self.relays[relay_id]["pin"]
        current_state = await self._read_pin_state(pin)
        if not current_state:
            logger.info(f"Relay {relay_id} is already OFF.")
            return False
        await self._set_pin_state(pin, False)
        self.relays[relay_id]["current_state"] = False
        logger.info(f"Relay {relay_id} turned OFF.")
        return True

    async def pulse_relay(self, relay_id: str, duration: float = 1.0) -> bool:
        """
        Pulse the specified relay: turn it ON for a duration, then turn it OFF.

        Args:
            relay_id (str): The identifier of the relay to pulse.
            duration (float): Duration in seconds to keep the relay ON.

        Returns:
            bool: True if the relay was pulsed successfully, False otherwise.
        """
        if relay_id not in self.relays:
            logger.error(f"Relay {relay_id} not found.")
            return False
        pin = self.relays[relay_id]["pin"]
        current_state = await self._read_pin_state(pin)
        new_state = not current_state
        await self._set_pin_state(pin, new_state)
        logger.info(f"Pulsing relay {relay_id}: set to {'ON' if new_state else 'OFF'} for {duration}s.")
        await asyncio.sleep(duration)
        await self._set_pin_state(pin, current_state)
        logger.info(f"Relay {relay_id} returned to {'ON' if current_state else 'OFF'}.")
        self.relays[relay_id]["current_state"] = current_state
        return True

    async def get_relay_state(self, relay_id: str) -> bool:
        """
        Get the current state of the specified relay.

        Args:
            relay_id (str): The identifier of the relay.

        Returns:
            bool: True if the relay is ON, False otherwise.
        """
        if relay_id not in self.relays:
            logger.error(f"Relay {relay_id} not found")
            return False
        pin = self.relays[relay_id]["pin"]
        return await self._read_pin_state(pin)

    async def _set_pin_state(self, pin: int, state: bool):
        """
        Set the GPIO pin state.

        Args:
            pin (int): GPIO pin number.
            state (bool): Desired state, True for HIGH, False for LOW.
        """
        await asyncio.to_thread(GPIO.output, pin, GPIO.HIGH if state else GPIO.LOW)
        logger.debug(f"Set pin {pin} to {'HIGH' if state else 'LOW'}.")

    async def _read_pin_state(self, pin: int) -> bool:
        """
        Read the current state of the GPIO pin.

        Args:
            pin (int): GPIO pin number.

        Returns:
            bool: True if HIGH, False if LOW.
        """
        value = await asyncio.to_thread(GPIO.input, pin)
        logger.debug(f"Read pin {pin}: {'HIGH' if value == GPIO.HIGH else 'LOW'}.")
        return value == GPIO.HIGH
