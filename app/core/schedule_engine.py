# This may need to be gutted and started from fresh.
# A system where if a relay has a schedule (user defines ON and or OFF times in 24 hour format)
# When the system starts it will check to see if the relay should be ON or OFF based on the schedule
# If it has a schedule it will calculate the time between now and when the relay should be ON or OFF
# Use a timer to turn the relay ON/OFF. As opposed to constantly polling to see if the current time is the time specified in config

import datetime
from typing import Optional, Union
from utils.validator import Schedule
from utils.logging_setup import local_logger as logger

class ScheduleEngine:
    def __init__(self, relay_id: str, schedule: Optional[Union[Schedule, bool]]):
        self.relay_id = relay_id
        self.schedule = schedule

    def is_enabled(self) -> bool:
        return isinstance(self.schedule, Schedule) and self.schedule.enabled
    
    def get_desired_state(self) -> bool:
        if not self.is_enabled():
            return False

        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M")
        current_day = now.strftime("%A").lower()
        
        # Check if the relay should be on
        if self.schedule.every_day or current_day in self.schedule.days:
            # Check if the current time is within the on_time and off_time
            if self.schedule.on_time <= current_time < self.schedule.off_time:
                logger.debug(f"Relay {self.relay_id} schedule: should be ON.")
                return True
            else:
                logger.debug(f"Relay {self.relay_id} schedule: should be OFF.")
                return False
        else:
            logger.debug(f"Relay {self.relay_id} schedule: Not Scheduled for today.")
            return False