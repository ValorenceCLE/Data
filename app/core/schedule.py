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
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Day bit values - using the provided values
DAY_VALUES = {
    "Sunday": 2,
    "Monday": 4,
    "Tuesday": 8,
    "Wednesday": 16,
    "Thursday": 32,
    "Friday": 64,
    "Saturday": 128
}

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
        
        # Get current day name
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        current_day_name = day_names[now.weekday()]
        
        # Get day bit value for current day
        current_day_bit = DAY_VALUES.get(current_day_name, 0)
        
        # Check if the schedule is active for the current day
        is_scheduled_today = (schedule.days_mask & current_day_bit) != 0
        
        if not is_scheduled_today:
            logger.debug(f"Relay {relay_id} not scheduled for today ({current_day_name})")
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
            
            # Skip if there's no schedule or it's disabled
            if not isinstance(schedule, RelaySchedule) or not schedule.enabled:
                continue
            
            try:
                should_be_on = self._should_be_on(relay_id, schedule)
                
                # Get the current state of the relay
                current_state = await self.relay_manager.get_relay_state(relay_id)
                
                # Skip if we couldn't determine the current state
                if current_state is None:
                    logger.warning(f"Couldn't determine current state of relay {relay_id}")
                    continue
                
                is_on = current_state == 1
                
                # Check if we already know the last state we set
                last_state = self._relay_states.get(relay_id)
                
                # Only update the relay if:
                # 1. The current state doesn't match what it should be, AND
                # 2. We haven't already tried to set it to this state
                if should_be_on != is_on and should_be_on != last_state:
                    if should_be_on:
                        logger.info(f"Schedule: Turning relay {relay_id} ON (current state: {'ON' if is_on else 'OFF'})")
                        success = await self.relay_manager.set_relay_on(relay_id)
                    else:
                        logger.info(f"Schedule: Turning relay {relay_id} OFF (current state: {'ON' if is_on else 'OFF'})")
                        success = await self.relay_manager.set_relay_off(relay_id)
                    
                    # Remember the state we just tried to set
                    if success:
                        self._relay_states[relay_id] = should_be_on
                        logger.info(f"Successfully set relay {relay_id} to {'ON' if should_be_on else 'OFF'}")
                    else:
                        logger.error(f"Failed to set relay {relay_id} to {'ON' if should_be_on else 'OFF'}")
                else:
                    logger.debug(f"Relay {relay_id} already in correct state: {'ON' if is_on else 'OFF'}")
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
    
    async def verify_schedules(self) -> bool:
        """
        Verify that all relay schedules are correctly configured.
        
        Returns:
            bool: True if all schedules are valid, False otherwise.
        """
        logger.info("Verifying relay schedules...")
        
        all_valid = True
        for relay in self.relays:
            if not relay.enabled:
                logger.debug(f"Relay '{relay.id}' is disabled, skipping schedule verification")
                continue
                
            schedule = relay.schedule
            if not isinstance(schedule, RelaySchedule):
                logger.debug(f"Relay '{relay.id}' has no schedule configuration")
                continue
                
            if not schedule.enabled:
                logger.debug(f"Schedule for relay '{relay.id}' is disabled")
                continue
                
            try:
                # Verify time formats
                on_time_valid = True
                off_time_valid = True
                
                if schedule.on_time:
                    try:
                        datetime.strptime(schedule.on_time, "%H:%M")
                    except ValueError:
                        on_time_valid = False
                        logger.error(f"Relay '{relay.id}' has invalid on_time format: {schedule.on_time}")
                        
                if schedule.off_time:
                    try:
                        datetime.strptime(schedule.off_time, "%H:%M")
                    except ValueError:
                        off_time_valid = False
                        logger.error(f"Relay '{relay.id}' has invalid off_time format: {schedule.off_time}")
                        
                # Verify days mask
                days_mask_valid = 0 <= schedule.days_mask <= 255  # Updated max value to include all possible days
                if not days_mask_valid:
                    logger.error(f"Relay '{relay.id}' has invalid days_mask: {schedule.days_mask}")
                    
                schedule_valid = on_time_valid and off_time_valid and days_mask_valid
                if schedule_valid:
                    days = days_mask_to_names(schedule.days_mask)
                    logger.info(f"Schedule for relay '{relay.id}' is valid - "
                              f"On: {schedule.on_time}, Off: {schedule.off_time}, "
                              f"Days: {', '.join(days)}")
                    
                    # Calculate next schedule change to verify logic
                    next_change = next_schedule_change(schedule)
                    if next_change:
                        days_away = next_change.get("days_away", 0)
                        change_time = next_change.get("time")
                        state = "ON" if next_change.get("state") else "OFF"
                        when = "today" if days_away == 0 else (
                            "tomorrow" if days_away == 1 else f"in {days_away} days"
                        )
                        logger.info(f"Relay '{relay.id}' next scheduled change: {state} at "
                                  f"{change_time.strftime('%H:%M')} {when}")
                    else:
                        logger.warning(f"No upcoming schedule changes for relay '{relay.id}'")
                else:
                    all_valid = False
                    
            except Exception as e:
                logger.error(f"Error verifying schedule for relay '{relay.id}': {e}")
                all_valid = False
                
        return all_valid
    
    async def run(self):
        """
        Start the schedule manager.
        """
        if self._running:
            logger.warning("Schedule manager already running")
            return
        
        self._running = True
        logger.info("Schedule manager started")
        
        # Verify schedules on startup
        await self.verify_schedules()
        
        try:
            # Run the main scheduling loop
            await self._schedule_loop()
        except asyncio.CancelledError:
            logger.info("Schedule manager cancelled")
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
        days_mask (int): The days bitmask using the custom bit values.
        
    Returns:
        List[str]: List of day names.
    """
    result = []
    
    for day, bit_value in DAY_VALUES.items():
        if (days_mask & bit_value) != 0:
            result.append(day)
            
    return result


# Helper function to convert a list of day names to a days_mask
def day_names_to_mask(day_names: List[str]) -> int:
    """
    Convert a list of day names to a days bitmask.
    
    Args:
        day_names (List[str]): List of day names.
        
    Returns:
        int: The days bitmask using the custom bit values.
    """
    days_mask = 0
    for day in day_names:
        day_title = day.title()  # Convert to title case for matching
        if day_title in DAY_VALUES:
            days_mask |= DAY_VALUES[day_title]
    
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
    
    # Get current day name
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    current_day_index = now.weekday()
    current_day_name = day_names[current_day_index]
    
    on_time = schedule.on_time or "00:00"
    off_time = schedule.off_time or "23:59"
    
    # Convert times to datetime objects for comparison
    on_time_parts = list(map(int, on_time.split(":")))
    off_time_parts = list(map(int, off_time.split(":")))
    
    on_datetime = now.replace(hour=on_time_parts[0], minute=on_time_parts[1], second=0, microsecond=0)
    off_datetime = now.replace(hour=off_time_parts[0], minute=off_time_parts[1], second=0, microsecond=0)
    
    # Check if today's schedule is active
    current_day_bit = DAY_VALUES.get(current_day_name, 0)
    is_scheduled_today = (schedule.days_mask & current_day_bit) != 0
    
    # Handle schedules that span midnight
    if on_time > off_time:
        # If it's currently after off_time but before on_time, and today is scheduled
        if current_time < on_time and current_time >= off_time and is_scheduled_today:
            return {
                "time": on_datetime,
                "state": True,  # ON
                "days_away": 0
            }
        
        # If it's before off_time and today is scheduled
        if current_time < off_time and is_scheduled_today:
            return {
                "time": off_datetime,
                "state": False,  # OFF
                "days_away": 0
            }
        
        # If it's after on_time and today is scheduled
        if current_time >= on_time and is_scheduled_today:
            # The next change would be OFF time tomorrow
            return {
                "time": off_datetime + timedelta(days=1),
                "state": False,  # OFF
                "days_away": 1
            }
    else:
        # Regular schedule (doesn't span midnight)
        
        # If it's before on_time and today is scheduled
        if current_time < on_time and is_scheduled_today:
            return {
                "time": on_datetime,
                "state": True,  # ON
                "days_away": 0
            }
            
        # If it's before off_time but after on_time and today is scheduled
        if current_time < off_time and current_time >= on_time and is_scheduled_today:
            return {
                "time": off_datetime,
                "state": False,  # OFF
                "days_away": 0
            }
    
    # If we reach here, we need to find the next scheduled day
    for days_ahead in range(1, 8):  # Check up to 7 days ahead
        next_day_index = (current_day_index + days_ahead) % 7
        next_day_name = day_names[next_day_index]
        next_day_bit = DAY_VALUES.get(next_day_name, 0)
        
        # Check if this day is scheduled
        if (schedule.days_mask & next_day_bit) != 0:
            # This day is scheduled, so the next change will be the ON time
            next_on_datetime = on_datetime + timedelta(days=days_ahead)
            return {
                "time": next_on_datetime,
                "state": True,  # ON
                "days_away": days_ahead
            }
    
    # If no schedule found, return None
    return None