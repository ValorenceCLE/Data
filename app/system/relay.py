"""
Relay Manager for controlling relay hardware.

This module provides a unified interface for controlling relays, handling hardware
interactions through the RelayControl class from services.controller.
"""
import asyncio
from typing import Dict, List, Optional, Any
from app.utils.logging_setup import local_logger as logger
from app.utils.validator import RelayConfig
from services.controller import RelayControl

class RelayManager:
    """
    Manages all relays in the system, providing a unified interface for control.
    """
    def __init__(self, relays: List[RelayConfig]):
        """
        Initialize the RelayManager.
        
        Args:
            relays (List[RelayConfig]): List of relay configurations.
        """
        self.relays = relays
        self.relay_controllers: Dict[str, RelayControl] = {}
        self.initialized = False
    
    async def init(self) -> bool:
        """
        Initialize relay controllers based on configuration.
        This method must be awaited before using other RelayManager methods.
        
        Returns:
            bool: True if initialization was successful, False otherwise.
        """
        try:
            # Create a controller for each relay in the configuration
            for relay in self.relays:
                relay_id = relay.id
                
                try:
                    # Create the controller
                    controller = RelayControl(relay_id)
                    self.relay_controllers[relay_id] = controller
                    
                    # Set the initial state based on the 'enabled' setting
                    if relay.enabled:
                        await controller.turn_on()
                    else:
                        await controller.turn_off()
                    
                    logger.debug(f"Relay {relay_id} initialized to {'ON' if relay.enabled else 'OFF'}")
                except Exception as e:
                    logger.error(f"Error initializing relay {relay_id}: {e}")
                    # Continue with other relays even if one fails
            
            self.initialized = True
            logger.info(f"Initialized {len(self.relay_controllers)} relays")
            return True
        except Exception as e:
            logger.error(f"Error initializing relay controllers: {e}")
            return False
    
    async def set_relay_on(self, relay_id: str) -> bool:
        """
        Turn a relay ON.
        
        Args:
            relay_id (str): The identifier of the relay to turn ON.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.initialized:
            logger.error("RelayManager not initialized")
            return False
        
        controller = self.relay_controllers.get(relay_id)
        if not controller:
            logger.error(f"Relay {relay_id} not found")
            return False
        
        try:
            result = await controller.turn_on()
            success = result.get("status") == "success"
            if success:
                logger.info(f"Relay {relay_id} turned ON")
            else:
                logger.error(f"Failed to turn relay {relay_id} ON: {result.get('message', 'Unknown error')}")
            return success
        except Exception as e:
            logger.error(f"Error turning relay {relay_id} ON: {e}")
            return False
    
    async def set_relay_off(self, relay_id: str) -> bool:
        """
        Turn a relay OFF.
        
        Args:
            relay_id (str): The identifier of the relay to turn OFF.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.initialized:
            logger.error("RelayManager not initialized")
            return False
        
        controller = self.relay_controllers.get(relay_id)
        if not controller:
            logger.error(f"Relay {relay_id} not found")
            return False
        
        try:
            result = await controller.turn_off()
            success = result.get("status") == "success"
            if success:
                logger.info(f"Relay {relay_id} turned OFF")
            else:
                logger.error(f"Failed to turn relay {relay_id} OFF: {result.get('message', 'Unknown error')}")
            return success
        except Exception as e:
            logger.error(f"Error turning relay {relay_id} OFF: {e}")
            return False
    
    async def pulse_relay(self, relay_id: str, duration: float = None) -> bool:
        """
        Pulse a relay (toggle its state briefly).
        
        Args:
            relay_id (str): The identifier of the relay to pulse.
            duration (float, optional): Duration in seconds to pulse for.
                If None, uses the configured pulse_time from the relay config.
                
        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.initialized:
            logger.error("RelayManager not initialized")
            return False
        
        controller = self.relay_controllers.get(relay_id)
        if not controller:
            logger.error(f"Relay {relay_id} not found")
            return False
        
        # If no duration provided, get it from the configuration
        if duration is None:
            relay_config = next((r for r in self.relays if r.id == relay_id), None)
            if relay_config:
                duration = relay_config.pulse_time
            else:
                duration = 5  # Default pulse time
        
        try:
            # First toggle the relay
            result = await controller.toggle()
            if result.get("status") != "success":
                logger.error(f"Failed to toggle relay {relay_id}: {result.get('message', 'Unknown error')}")
                return False
            
            # Wait for the specified duration
            logger.info(f"Relay {relay_id} pulsed for {duration} seconds")
            await asyncio.sleep(duration)
            
            # Toggle it back
            result = await controller.toggle()
            success = result.get("status") == "success"
            if not success:
                logger.error(f"Failed to toggle relay {relay_id} back: {result.get('message', 'Unknown error')}")
            return success
        except Exception as e:
            logger.error(f"Error pulsing relay {relay_id}: {e}")
            return False
    
    async def get_relay_state(self, relay_id: str) -> Optional[int]:
        """
        Get the current state of a relay.
        
        Args:
            relay_id (str): The identifier of the relay.
            
        Returns:
            Optional[int]: 1 if ON, 0 if OFF, None if error or not found.
        """
        if not self.initialized:
            logger.error("RelayManager not initialized")
            return None
        
        controller = self.relay_controllers.get(relay_id)
        if not controller:
            logger.error(f"Relay {relay_id} not found")
            return None
        
        try:
            state = controller.state
            return state
        except Exception as e:
            logger.error(f"Error getting state for relay {relay_id}: {e}")
            return None
    
    async def get_all_relay_states(self) -> Dict[str, int]:
        """
        Get the current states of all relays.
        
        Returns:
            Dict[str, int]: Dictionary mapping relay IDs to states (1=ON, 0=OFF, -1=error).
        """
        if not self.initialized:
            logger.error("RelayManager not initialized")
            return {}
        
        states = {}
        for relay_id, controller in self.relay_controllers.items():
            try:
                states[relay_id] = controller.state
            except Exception as e:
                logger.error(f"Error getting state for relay {relay_id}: {e}")
                states[relay_id] = -1  # Error state
        
        return states
    
    def get_relay_by_id(self, relay_id: str) -> Optional[RelayConfig]:
        """
        Get a relay configuration by its ID.
        
        Args:
            relay_id (str): The relay identifier.
            
        Returns:
            Optional[RelayConfig]: The relay configuration or None if not found.
        """
        for relay in self.relays:
            if relay.id == relay_id:
                return relay
        return None
    
    def get_relay_names(self) -> Dict[str, str]:
        """
        Get a mapping of relay IDs to names.
        
        Returns:
            Dict[str, str]: Dictionary mapping relay IDs to names.
        """
        return {relay.id: relay.name for relay in self.relays}
    
    async def shutdown(self):
        """
        Perform a clean shutdown of all relays.
        """
        if not self.initialized:
            return
        
        logger.info("Shutting down relay manager")
        # No specific cleanup needed for RelayControl instances
        self.initialized = False