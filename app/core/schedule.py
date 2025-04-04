"""
Schedule Manager for managing relay schedules.

This module handles the scheduling of relay state changes based on time and days of the week.
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from app.utils.validator import RelayConfig, RelaySchedule


logger = logging.getLogger("ScheduleManager")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class ScheduleManager:
    """
    Manages scheduling for all relays based on their configured schedules.
    """
    def __init__(self, relays: List[RelayConfig], relay_manager):
        """
        Initialize the ScheduleManager.
        
        Args:
            relays (List[RelayConfig]): List of relay configurations.
            relay_manager: The relay manager for controlling relays.
        """
        self.relays = relays
        self.relay_manager = relay_manager
        self._running = False
        self._check_interval = 60  # Check schedules every minute
        
        # Keep track of the last state we set for each relay
        self._relay_states: Dict[str, bool] = {}
    
    def _should_be_on(self, relay_id: str, schedule: RelaySchedule) -> bool:
        """
        Determine if a relay should be ON based on its schedule and the current time.
        
        Args:
            relay_id (str): The relay identifier.
            schedule (RelaySchedule): The relay's schedule configuration.
            
        Returns:
            bool: True if the relay should be ON, False otherwise.
        """
        # Check if scheduling is enabled
        if not schedule.enabled:
            logger.debug(f"Relay {relay_id} schedule is disabled")
            return False
        
        # Get current time and day
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_day_index = now.weekday() + 1  # Monday=1, Sunday=7
        if current_day_index == 7:  # Convert Sunday from 7 to 0 for bit position
            current_day_index = 0
        
        # Check if the schedule is active for the current day
        day_bit = 1 << current_day_index
        is_scheduled_today = (schedule.days_mask & day_bit) != 0
        
        if not is_scheduled_today:
            logger.debug(f"Relay {relay_id} not scheduled for today (day bit {current_day_index})")
            return False
        
        # Check if current time is within the scheduled time range
        on_time = schedule.on_time or "00:00"
        off_time = schedule.off_time or "23:59"
        
        # Handle schedules that span midnight
        if on_time > off_time:
            # Schedule spans midnight, e.g., 22:00 to 06:00
            return current_time >= on_time or current_time < off_time
        else:
            # Regular schedule, e.g., 08:00 to 18:00
            return on_time <= current_time < off_time
    
    async def _check_schedules(self):
        """
        Check all relay schedules and update relay states as needed.
        """
        for relay in self.relays:
            relay_id = relay.id
            schedule = relay.schedule
            
            # Skip if the relay is disabled
            if not relay.enabled:
                continue
            
            try:
                should_be_on = self._should_be_on(relay_id, schedule)
                
                # Get the current state of the relay
                current_state = await self.relay_manager.get_relay_state(relay_id)
                is_on = current_state == 1 if current_state is not None else None
                
                # Skip if we couldn't determine the current state
                if is_on is None:
                    logger.warning(f"Couldn't determine current state of relay {relay_id}")
                    continue
                
                # Check if we already know the last state we set
                last_state = self._relay_states.get(relay_id)
                
                # Only update the relay if:
                # 1. The current state doesn't match what it should be, AND
                # 2. We haven't already tried to set it to this state
                if should_be_on != is_on and should_be_on != last_state:
                    if should_be_on:
                        logger.info(f"Schedule: Turning relay {relay_id} ON")
                        success = await self.relay_manager.set_relay_on(relay_id)
                    else:
                        logger.info(f"Schedule: Turning relay {relay_id} OFF")
                        success = await self.relay_manager.set_relay_off(relay_id)
                    
                    # Remember the state we just tried to set
                    if success:
                        self._relay_states[relay_id] = should_be_on
            except Exception as e:
                logger.error(f"Error checking schedule for relay {relay_id}: {e}")
    
    async def _schedule_loop(self):
        """
        Main scheduling loop. Checks schedules periodically.
        """
        while self._running:
            try:
                # Check all schedules
                await self._check_schedules()
                
                # Wait until the next check
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                logger.info("Schedule loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in schedule loop: {e}")
                await asyncio.sleep(10)  # Wait a bit before retrying
    
    async def run(self):
        """
        Start the schedule manager.
        """
        if self._running:
            logger.warning("Schedule manager already running")
            return
        
        self._running = True
        logger.info("Schedule manager started")
        
        try:
            # Run the main scheduling loop
            await self._schedule_loop()
        except Exception as e:
            logger.error(f"Error in schedule manager: {e}")
        finally:
            self._running = False
            logger.info("Schedule manager stopped")
    
    async def shutdown(self):
        """
        Shut down the schedule manager.
        """
        if not self._running:
            return
        
        self._running = False
        logger.info("Shutting down schedule manager")


# Helper function to convert days_mask to a list of day names
def days_mask_to_names(days_mask: int) -> List[str]:
    """
    Convert a days bitmask to a list of day names.
    
    Args:
        days_mask (int): The days bitmask (bit 0 = Sunday, bit 1 = Monday, etc.)
        
    Returns:
        List[str]: List of day names.
    """
    days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    result = []
    
    for i, day in enumerate(days):
        if (days_mask & (1 << i)) != 0:
            result.append(day)
            
    return result


# Helper function to convert a list of day names to a days_mask
def day_names_to_mask(day_names: List[str]) -> int:
    """
    Convert a list of day names to a days bitmask.
    
    Args:
        day_names (List[str]): List of day names.
        
    Returns:
        int: The days bitmask (bit 0 = Sunday, bit 1 = Monday, etc.)
    """
    days_map = {
        "sunday": 0,
        "monday": 1,
        "tuesday": 2,
        "wednesday": 3,
        "thursday": 4,
        "friday": 5,
        "saturday": 6
    }
    
    days_mask = 0
    for day in day_names:
        day_lower = day.lower()
        if day_lower in days_map:
            days_mask |= (1 << days_map[day_lower])
    
    return days_mask


# Helper function to calculate the next schedule change
def next_schedule_change(schedule: RelaySchedule) -> Optional[Dict[str, Any]]:
    """
    Calculate the next time this schedule will cause a state change.
    
    Args:
        schedule (RelaySchedule): The schedule to analyze.
        
    Returns:
        Optional[Dict[str, Any]]: Information about the next change, or None if no schedule.
    """
    if not schedule.enabled or schedule.days_mask == 0:
        return None
    
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    current_day_index = now.weekday() + 1  # Monday=1, Sunday=7
    if current_day_index == 7:  # Convert Sunday from 7 to 0
        current_day_index = 0
    
    on_time = schedule.on_time or "00:00"
    off_time = schedule.off_time or "23:59"
    
    # Convert times to datetime objects for comparison
    on_time_parts = list(map(int, on_time.split(":")))
    off_time_parts = list(map(int, off_time.split(":")))
    
    on_datetime = now.replace(hour=on_time_parts[0], minute=on_time_parts[1], second=0, microsecond=0)
    off_datetime = now.replace(hour=off_time_parts[0], minute=off_time_parts[1], second=0, microsecond=0)
    
    # Handle schedules that span midnight
    if on_time > off_time:
        # If current time is after off_time but before midnight, the next change is on_time tomorrow
        if current_time < on_time and current_time >= off_time:
            # Find the next day that has a schedule
            days_checked = 0
            next_day_index = current_day_index
            while days_checked < 7:
                next_day_index = (next_day_index + 1) % 7
                if (schedule.days_mask & (1 << next_day_index)) != 0:
                    # Found the next day with a schedule
                    days_to_add = (next_day_index - current_day_index) % 7
                    if days_to_add == 0:
                        days_to_add = 7  # Next week
                    
                    next_change = on_datetime + timedelta(days=days_to_add)
                    return {
                        "time": next_change,
                        "state": True,  # ON
                        "days_away": days_to_add
                    }
                days_checked += 1
                
        # If current time is before off_time, the next change is off_time today
        elif current_time < off_time:
            return {
                "time": off_datetime,
                "state": False,  # OFF
                "days_away": 0
            }
        # If current time is after on_time, the next change is off_time tomorrow
        elif current_time >= on_time:
            return {
                "time": off_datetime + timedelta(days=1),
                "state": False,  # OFF
                "days_away": 1
            }
    else:
        # Regular schedule (doesn't span midnight)
        
        # If current time is before on_time, the next change is on_time today
        if current_time < on_time:
            # Check if today has a schedule
            if (schedule.days_mask & (1 << current_day_index)) != 0:
                return {
                    "time": on_datetime,
                    "state": True,  # ON
                    "days_away": 0
                }
                
        # If current time is before off_time but after on_time, the next change is off_time today
        elif current_time < off_time and current_time >= on_time:
            # Check if today has a schedule
            if (schedule.days_mask & (1 << current_day_index)) != 0:
                return {
                    "time": off_datetime,
                    "state": False,  # OFF
                    "days_away": 0
                }
    
    # If we reach here, we need to find the next scheduled day
    days_checked = 0
    next_day_index = current_day_index
    while days_checked < 7:
        next_day_index = (next_day_index + 1) % 7
        if (schedule.days_mask & (1 << next_day_index)) != 0:
            # Found the next day with a schedule
            days_to_add = (next_day_index - current_day_index) % 7
            if days_to_add == 0:
                days_to_add = 7  # Next week
            
            next_change = on_datetime + timedelta(days=days_to_add)
            return {
                "time": next_change,
                "state": True,  # ON
                "days_away": days_to_add
            }
        days_checked += 1
    
    # If no schedule found, return None
    return None