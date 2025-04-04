"""
Time and date configuration manager for the DPM system.

This module handles all time-related configurations, including:
- Setting the system timezone
- Configuring NTP servers
- Synchronizing the system clock
"""
import asyncio
import subprocess
import os
import tempfile
from typing import Optional, List, Dict
from datetime import datetime, timezone
import logging
from app.utils.validator import DateTimeConfig

logger = logging.getLogger("TimeManager")
logger.setLevel(logging.DEBUG)  # Set logging level to DEBUG for detailed logs
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class TimeManager:
    """
    Manages time and date configuration for the system.
    """
    def __init__(self, config: DateTimeConfig):
        """
        Initialize the TimeManager.
        
        Args:
            config (DateTimeConfig): The time configuration.
        """
        logger.debug(f"Initializing TimeManager with config: {config}")
        self.config = config
        self._current_config = None
    
    async def get_current_config(self) -> Dict:
        """
        Get the current time configuration from the system.
        
        Returns:
            Dict: The current time configuration.
        """
        logger.debug("Fetching current time configuration")
        try:
            result = {}
            
            # Get current timezone
            logger.debug("Getting current timezone")
            timezone_proc = await asyncio.create_subprocess_exec(
                "timedatectl", "show", "--property=Timezone",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await timezone_proc.communicate()
            
            if timezone_proc.returncode == 0:
                timezone_output = stdout.decode().strip()
                logger.debug(f"Timezone output: {timezone_output}")
                if timezone_output:
                    parts = timezone_output.split("=")
                    if len(parts) >= 2:
                        result["timezone"] = parts[1]
            else:
                logger.error(f"Failed to get timezone: {stderr.decode().strip()}")
            
            # Get NTP status
            logger.debug("Getting NTP status")
            ntp_proc = await asyncio.create_subprocess_exec(
                "timedatectl", "show", "--property=NTP",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ntp_proc.communicate()
            
            if ntp_proc.returncode == 0:
                ntp_output = stdout.decode().strip()
                logger.debug(f"NTP output: {ntp_output}")
                if ntp_output:
                    parts = ntp_output.split("=")
                    if len(parts) >= 2:
                        result["synchronize"] = parts[1].lower() == "yes"
            else:
                logger.error(f"Failed to get NTP status: {stderr.decode().strip()}")
            
            # Get NTP servers from configuration file
            logger.debug("Getting NTP servers")
            result["ntp_servers"] = await self._get_ntp_servers()
            
            # Calculate UTC offset based on the current timezone
            logger.debug("Calculating UTC offset")
            now = datetime.now()
            offset = datetime.now(timezone.utc).astimezone().utcoffset()
            if offset:
                result["utc_offset"] = int(offset.total_seconds() / 3600)  # Convert to hours
            else:
                result["utc_offset"] = 0
            
            self._current_config = result
            logger.debug(f"Current configuration: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting current time configuration: {e}")
            return {}
    
    async def _get_ntp_servers(self) -> List[str]:
        """
        Get the current NTP servers from configuration.
        
        Returns:
            List[str]: List of NTP server hostnames.
        """
        logger.debug("Fetching NTP servers from configuration files")
        ntp_servers = []
        try:
            config_files = [
                "/etc/systemd/timesyncd.conf",
                "/etc/ntp.conf",
                "/etc/chrony/chrony.conf"
            ]
            
            for file_path in config_files:
                logger.debug(f"Checking file: {file_path}")
                if os.path.exists(file_path):
                    with open(file_path, "r") as f:
                        content = f.read()
                        logger.debug(f"Content of {file_path}: {content}")
                        
                        if "timesyncd" in file_path:
                            for line in content.splitlines():
                                if line.startswith("NTP="):
                                    servers = line.split("=")[1].strip()
                                    ntp_servers.extend(servers.split())
                        else:
                            for line in content.splitlines():
                                if line.startswith("server "):
                                    parts = line.split()
                                    if len(parts) >= 2:
                                        ntp_servers.append(parts[1])
                    
                    if ntp_servers:
                        logger.debug(f"NTP servers found: {ntp_servers}")
                        break
            
            return ntp_servers
        except Exception as e:
            logger.error(f"Error reading NTP servers: {e}")
            return []
    
    async def apply_config(self) -> bool:
        """
        Apply the time configuration to the system.
        
        Returns:
            bool: True if the configuration was applied successfully, False otherwise.
        """
        logger.debug("Applying time configuration")
        try:
            current_config = await self.get_current_config()
            logger.debug(f"Current configuration: {current_config}")
            
            if self._config_matches_current():
                logger.info("Current time configuration already matches desired configuration")
                return True
            
            logger.debug("Applying timezone configuration")
            timezone_success = await self._set_timezone(self.config.timezone)
            
            logger.debug("Applying NTP configuration")
            ntp_success = await self._configure_ntp(
                self.config.synchronize,
                self.config.primary_ntp,
                self.config.secondary_ntp
            )
            
            if self.config.synchronize:
                logger.debug("Synchronizing time")
                sync_success = await self._sync_time()
            else:
                sync_success = True
            
            result = timezone_success and ntp_success and sync_success
            logger.debug(f"Configuration applied successfully: {result}")
            return result
        except Exception as e:
            logger.error(f"Error applying time configuration: {e}")
            return False
    
    # Add similar detailed logging to all other methods in the class
