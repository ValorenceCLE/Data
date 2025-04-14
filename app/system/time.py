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
logger.setLevel(logging.INFO)  # Set logging level to DEBUG for detailed logs
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
            
    async def _sync_time(self) -> bool:
        """
        Force time synchronization if NTP is enabled.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Forcing time synchronization")
        if not self.config.synchronize:
            logger.debug("NTP synchronization is disabled in config, skipping")
            return True
            
        try:
            # Try systemd-timesyncd first
            sync_proc = await asyncio.create_subprocess_exec(
                "sudo", "systemctl", "restart", "systemd-timesyncd",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await sync_proc.communicate()
            
            if sync_proc.returncode == 0:
                logger.info("Time synchronization initiated via systemd-timesyncd")
                # Give it a moment to synchronize
                await asyncio.sleep(2)
                return True
            else:
                # Try direct ntpdate as fallback
                ntp_proc = await asyncio.create_subprocess_exec(
                    "sudo", "ntpdate", self.config.primary_ntp,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await ntp_proc.communicate()
                
                if ntp_proc.returncode == 0:
                    logger.info(f"Time synchronized directly with {self.config.primary_ntp}")
                    return True
                else:
                    logger.error(f"Failed to synchronize time: {stderr.decode().strip()}")
                    return False
        except Exception as e:
            logger.error(f"Error synchronizing time: {e}")
            return False
            
    def _config_matches_current(self) -> bool:
        """
        Check if the desired configuration matches the current configuration.
        
        Returns:
            bool: True if configurations match, False otherwise.
        """
        if not self._current_config:
            return False
        
        # Compare timezone
        if self._current_config.get('timezone') != self.config.timezone:
            logger.debug(f"Timezone mismatch: current={self._current_config.get('timezone')}, desired={self.config.timezone}")
            return False
        
        # Compare NTP status
        if self._current_config.get('synchronize') != self.config.synchronize:
            logger.debug(f"NTP status mismatch: current={self._current_config.get('synchronize')}, desired={self.config.synchronize}")
            return False
        
        # Compare NTP servers (this is approximate as server list formats may differ)
        current_servers = self._current_config.get('ntp_servers', [])
        if self.config.primary_ntp not in current_servers:
            logger.debug(f"Primary NTP server mismatch: {self.config.primary_ntp} not in {current_servers}")
            return False
        
        if self.config.secondary_ntp and self.config.secondary_ntp not in current_servers:
            logger.debug(f"Secondary NTP server mismatch: {self.config.secondary_ntp} not in {current_servers}")
            return False
        
        logger.debug("Current time configuration matches desired configuration")
        return True
            
    async def _set_timezone(self, timezone: str) -> bool:
        """
        Set the system timezone.
        
        Args:
            timezone (str): The timezone to set (e.g., 'America/Denver').
            
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Setting timezone to {timezone}")
        try:
            # Check if timedatectl is available (Raspberry Pi OS uses systemd)
            which_proc = await asyncio.create_subprocess_exec(
                "which", "timedatectl",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await which_proc.communicate()
            
            if which_proc.returncode == 0:
                # Set the timezone using timedatectl
                proc = await asyncio.create_subprocess_exec(
                    "sudo", "timedatectl", "set-timezone", timezone,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if proc.returncode == 0:
                    logger.info(f"Timezone set to {timezone} successfully")
                    return True
                else:
                    logger.error(f"Failed to set timezone: {stderr.decode().strip()}")
                    return False
            else:
                # Alternative method for systems without timedatectl
                # Create a symlink to the timezone file
                tz_file = f"/usr/share/zoneinfo/{timezone}"
                localtime_file = "/etc/localtime"
                
                # Check if timezone file exists
                exists_proc = await asyncio.create_subprocess_exec(
                    "test", "-f", tz_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await exists_proc.communicate()
                
                if exists_proc.returncode != 0:
                    logger.error(f"Timezone file {tz_file} does not exist")
                    return False
                
                # Remove existing symlink
                rm_proc = await asyncio.create_subprocess_exec(
                    "sudo", "rm", "-f", localtime_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await rm_proc.communicate()
                
                # Create new symlink
                ln_proc = await asyncio.create_subprocess_exec(
                    "sudo", "ln", "-sf", tz_file, localtime_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await ln_proc.communicate()
                
                if ln_proc.returncode == 0:
                    logger.info(f"Timezone set to {timezone} successfully (fallback method)")
                    return True
                else:
                    logger.error(f"Failed to set timezone (fallback method): {stderr.decode().strip()}")
                    return False
        except Exception as e:
            logger.error(f"Error setting timezone: {e}")
            return False

    async def _configure_ntp(self, enable: bool, primary_ntp: str, secondary_ntp: Optional[str] = None) -> bool:
        """
        Configure NTP synchronization.
        
        Args:
            enable (bool): Whether to enable NTP.
            primary_ntp (str): Primary NTP server.
            secondary_ntp (Optional[str]): Secondary NTP server.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Configuring NTP: enable={enable}, primary={primary_ntp}, secondary={secondary_ntp}")
        try:
            # Check if we're using systemd-timesyncd (Raspberry Pi OS default)
            timesyncd_config_file = "/etc/systemd/timesyncd.conf"
            
            # Check if file exists
            exists_proc = await asyncio.create_subprocess_exec(
                "test", "-f", timesyncd_config_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await exists_proc.communicate()
            
            if exists_proc.returncode == 0:
                # Create a temporary file with the NTP configuration
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    temp_file.write("[Time]\n")
                    ntp_servers = primary_ntp
                    if secondary_ntp:
                        ntp_servers += f" {secondary_ntp}"
                    temp_file.write(f"NTP={ntp_servers}\n")
                    temp_file_path = temp_file.name
                
                # Copy the config file to /etc/systemd/timesyncd.conf
                copy_proc = await asyncio.create_subprocess_exec(
                    "sudo", "cp", temp_file_path, timesyncd_config_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await copy_proc.communicate()
                
                # Remove the temporary file
                os.unlink(temp_file_path)
                
                if copy_proc.returncode != 0:
                    logger.error(f"Failed to configure NTP servers: {stderr.decode().strip()}")
                    return False
                
                # Enable/disable NTP
                if enable:
                    # Enable NTP synchronization
                    ntp_proc = await asyncio.create_subprocess_exec(
                        "sudo", "timedatectl", "set-ntp", "true",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                else:
                    # Disable NTP synchronization
                    ntp_proc = await asyncio.create_subprocess_exec(
                        "sudo", "timedatectl", "set-ntp", "false",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                stdout, stderr = await ntp_proc.communicate()
                
                if ntp_proc.returncode != 0:
                    logger.error(f"Failed to {'enable' if enable else 'disable'} NTP: {stderr.decode().strip()}")
                    return False
                
                # Restart the timesyncd service
                restart_proc = await asyncio.create_subprocess_exec(
                    "sudo", "systemctl", "restart", "systemd-timesyncd",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await restart_proc.communicate()
                
                if restart_proc.returncode != 0:
                    logger.error(f"Failed to restart timesyncd: {stderr.decode().strip()}")
                    return False
                    
                logger.info(f"NTP {'enabled' if enable else 'disabled'} successfully with servers: {ntp_servers}")
                return True
            else:
                # Alternative: If no systemd-timesyncd, try using direct ntpdate
                if enable:
                    # Try to sync time directly
                    ntp_proc = await asyncio.create_subprocess_exec(
                        "sudo", "ntpdate", primary_ntp,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await ntp_proc.communicate()
                    
                    if ntp_proc.returncode != 0:
                        logger.error(f"Failed to sync with NTP server: {stderr.decode().strip()}")
                        return False
                    
                    logger.info(f"NTP time synchronized successfully with {primary_ntp}")
                    return True
                else:
                    # NTP is already disabled if timesyncd is not present
                    logger.info("NTP disabled successfully (timesyncd not present)")
                    return True
        except Exception as e:
            logger.error(f"Error configuring NTP: {e}")
            return False