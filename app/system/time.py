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
from app.utils.logging_setup import local_logger as logger
from app.utils.validator import DateTimeConfig

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
        self.config = config
        self._current_config = None
    
    async def get_current_config(self) -> Dict:
        """
        Get the current time configuration from the system.
        
        Returns:
            Dict: The current time configuration.
        """
        try:
            result = {}
            
            # Get current timezone
            timezone_proc = await asyncio.create_subprocess_exec(
                "timedatectl", "show", "--property=Timezone",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await timezone_proc.communicate()
            
            if timezone_proc.returncode == 0:
                # Parse output like: "Timezone=America/Denver"
                timezone_output = stdout.decode().strip()
                if timezone_output:
                    parts = timezone_output.split("=")
                    if len(parts) >= 2:
                        result["timezone"] = parts[1]
            
            # Get NTP status
            ntp_proc = await asyncio.create_subprocess_exec(
                "timedatectl", "show", "--property=NTP",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ntp_proc.communicate()
            
            if ntp_proc.returncode == 0:
                # Parse output like: "NTP=yes"
                ntp_output = stdout.decode().strip()
                if ntp_output:
                    parts = ntp_output.split("=")
                    if len(parts) >= 2:
                        result["synchronize"] = parts[1].lower() == "yes"
            
            # Get NTP servers from configuration file
            result["ntp_servers"] = await self._get_ntp_servers()
            
            # Calculate UTC offset based on the current timezone
            now = datetime.now()
            offset = datetime.now(timezone.utc).astimezone().utcoffset()
            if offset:
                result["utc_offset"] = int(offset.total_seconds() / 3600)  # Convert to hours
            else:
                result["utc_offset"] = 0
            
            self._current_config = result
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
        ntp_servers = []
        try:
            # Check different possible file locations
            config_files = [
                "/etc/systemd/timesyncd.conf",
                "/etc/ntp.conf",
                "/etc/chrony/chrony.conf"
            ]
            
            for file_path in config_files:
                if os.path.exists(file_path):
                    with open(file_path, "r") as f:
                        content = f.read()
                        
                        # Parse for systemd-timesyncd
                        if "timesyncd" in file_path:
                            for line in content.splitlines():
                                if line.startswith("NTP="):
                                    servers = line.split("=")[1].strip()
                                    ntp_servers.extend(servers.split())
                        
                        # Parse for ntp or chrony
                        else:
                            for line in content.splitlines():
                                if line.startswith("server "):
                                    parts = line.split()
                                    if len(parts) >= 2:
                                        ntp_servers.append(parts[1])
                    
                    if ntp_servers:
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
        try:
            # Get current configuration to see if changes are needed
            current_config = await self.get_current_config()
            
            # Check if any changes are needed
            if self._config_matches_current():
                logger.info("Current time configuration already matches desired configuration")
                return True
            
            # Apply timezone
            timezone_success = await self._set_timezone(self.config.timezone)
            
            # Apply NTP settings
            ntp_success = await self._configure_ntp(
                self.config.synchronize,
                self.config.primary_ntp,
                self.config.secondary_ntp
            )
            
            # Synchronize time if enabled
            if self.config.synchronize:
                sync_success = await self._sync_time()
            else:
                sync_success = True
            
            return timezone_success and ntp_success and sync_success
        except Exception as e:
            logger.error(f"Error applying time configuration: {e}")
            return False
    
    def _config_matches_current(self) -> bool:
        """
        Check if the current configuration matches the desired configuration.
        
        Returns:
            bool: True if the configurations match, False otherwise.
        """
        if not self._current_config:
            return False
            
        # Check timezone
        if self.config.timezone != self._current_config.get("timezone", ""):
            return False
            
        # Check NTP synchronization
        if self.config.synchronize != self._current_config.get("synchronize", False):
            return False
            
        # Check NTP servers
        current_ntp = self._current_config.get("ntp_servers", [])
        if self.config.primary_ntp not in current_ntp:
            return False
            
        if self.config.secondary_ntp and self.config.secondary_ntp not in current_ntp:
            return False
            
        # Check UTC offset
        if self.config.utc_offset != self._current_config.get("utc_offset", 0):
            return False
            
        return True
    
    async def _set_timezone(self, timezone: str) -> bool:
        """
        Set the system timezone.
        
        Args:
            timezone (str): The timezone to set (e.g., "America/Denver").
            
        Returns:
            bool: True if the timezone was set successfully, False otherwise.
        """
        try:
            # Set the timezone using timedatectl
            proc = await self._run_command("timedatectl", "set-timezone", timezone)
            
            if proc:
                logger.info(f"Timezone set to {timezone}")
                return True
            else:
                logger.error(f"Failed to set timezone to {timezone}")
                return False
        except Exception as e:
            logger.error(f"Error setting timezone: {e}")
            return False
    
    async def _configure_ntp(self, enable: bool, primary_server: str, secondary_server: Optional[str] = None) -> bool:
        """
        Configure NTP servers and enable/disable synchronization.
        
        Args:
            enable (bool): Whether to enable NTP synchronization.
            primary_server (str): The primary NTP server.
            secondary_server (Optional[str]): The secondary NTP server, if any.
            
        Returns:
            bool: True if NTP was configured successfully, False otherwise.
        """
        try:
            # First, enable or disable NTP synchronization
            ntp_cmd = "set-ntp"
            ntp_val = "yes" if enable else "no"
            
            proc = await self._run_command("timedatectl", ntp_cmd, ntp_val)
            
            if not proc:
                logger.error(f"Failed to {'enable' if enable else 'disable'} NTP synchronization")
                return False
            
            # If NTP is enabled, configure the servers
            if enable:
                # Determine which NTP service is in use
                if await self._file_exists("/etc/systemd/timesyncd.conf"):
                    return await self._configure_systemd_timesyncd(primary_server, secondary_server)
                elif await self._file_exists("/etc/ntp.conf"):
                    return await self._configure_ntp_conf(primary_server, secondary_server)
                elif await self._file_exists("/etc/chrony/chrony.conf"):
                    return await self._configure_chrony(primary_server, secondary_server)
                else:
                    # Default to systemd-timesyncd
                    return await self._configure_systemd_timesyncd(primary_server, secondary_server)
            
            return True
        except Exception as e:
            logger.error(f"Error configuring NTP: {e}")
            return False
    
    async def _configure_systemd_timesyncd(self, primary_server: str, secondary_server: Optional[str] = None) -> bool:
        """
        Configure systemd-timesyncd NTP service.
        
        Args:
            primary_server (str): The primary NTP server.
            secondary_server (Optional[str]): The secondary NTP server, if any.
            
        Returns:
            bool: True if configured successfully, False otherwise.
        """
        try:
            config_file = "/etc/systemd/timesyncd.conf"
            servers = primary_server
            if secondary_server:
                servers += f" {secondary_server}"
            
            # Create config content
            config_content = "[Time]\n"
            config_content += f"NTP={servers}\n"
            config_content += "FallbackNTP=time.google.com ntp.ubuntu.com\n"
            
            # Write to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
                temp_path = temp_file.name
                temp_file.write(config_content)
            
            # Move the file into place
            proc = await self._run_command("sudo", "mv", temp_path, config_file)
            
            if not proc:
                logger.error("Failed to write timesyncd configuration")
                return False
            
            # Restart the service
            restart_proc = await self._run_command("sudo", "systemctl", "restart", "systemd-timesyncd")
            
            if restart_proc:
                logger.info(f"NTP servers configured: {servers}")
                return True
            else:
                logger.error("Failed to restart timesyncd service")
                return False
        except Exception as e:
            logger.error(f"Error configuring systemd-timesyncd: {e}")
            return False
    
    async def _configure_ntp_conf(self, primary_server: str, secondary_server: Optional[str] = None) -> bool:
        """
        Configure traditional NTP service.
        
        Args:
            primary_server (str): The primary NTP server.
            secondary_server (Optional[str]): The secondary NTP server, if any.
            
        Returns:
            bool: True if configured successfully, False otherwise.
        """
        try:
            config_file = "/etc/ntp.conf"
            
            # Read existing config
            with open(config_file, "r") as f:
                lines = f.readlines()
            
            # Remove existing server lines
            new_lines = []
            for line in lines:
                if not line.startswith("server "):
                    new_lines.append(line)
            
            # Add new server lines
            new_lines.append(f"server {primary_server} iburst\n")
            if secondary_server:
                new_lines.append(f"server {secondary_server} iburst\n")
            
            # Write to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
                temp_path = temp_file.name
                for line in new_lines:
                    temp_file.write(line)
            
            # Move the file into place
            proc = await self._run_command("sudo", "mv", temp_path, config_file)
            
            if not proc:
                logger.error("Failed to write NTP configuration")
                return False
            
            # Restart the service
            restart_proc = await self._run_command("sudo", "systemctl", "restart", "ntp")
            
            if restart_proc:
                logger.info(f"NTP servers configured in ntp.conf")
                return True
            else:
                logger.error("Failed to restart NTP service")
                return False
        except Exception as e:
            logger.error(f"Error configuring ntp.conf: {e}")
            return False
    
    async def _configure_chrony(self, primary_server: str, secondary_server: Optional[str] = None) -> bool:
        """
        Configure chrony NTP service.
        
        Args:
            primary_server (str): The primary NTP server.
            secondary_server (Optional[str]): The secondary NTP server, if any.
            
        Returns:
            bool: True if configured successfully, False otherwise.
        """
        try:
            config_file = "/etc/chrony/chrony.conf"
            
            # Read existing config
            with open(config_file, "r") as f:
                lines = f.readlines()
            
            # Remove existing server lines
            new_lines = []
            for line in lines:
                if not line.startswith("server "):
                    new_lines.append(line)
            
            # Add new server lines
            new_lines.append(f"server {primary_server} iburst\n")
            if secondary_server:
                new_lines.append(f"server {secondary_server} iburst\n")
            
            # Write to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
                temp_path = temp_file.name
                for line in new_lines:
                    temp_file.write(line)
            
            # Move the file into place
            proc = await self._run_command("sudo", "mv", temp_path, config_file)
            
            if not proc:
                logger.error("Failed to write chrony configuration")
                return False
            
            # Restart the service
            restart_proc = await self._run_command("sudo", "systemctl", "restart", "chrony")
            
            if restart_proc:
                logger.info(f"NTP servers configured in chrony.conf")
                return True
            else:
                logger.error("Failed to restart chrony service")
                return False
        except Exception as e:
            logger.error(f"Error configuring chrony.conf: {e}")
            return False
    
    async def _sync_time(self) -> bool:
        """
        Force a time synchronization with NTP servers.
        
        Returns:
            bool: True if time was synchronized successfully, False otherwise.
        """
        try:
            # Check which NTP service is in use
            if await self._service_active("systemd-timesyncd"):
                proc = await self._run_command("sudo", "systemctl", "restart", "systemd-timesyncd")
                if proc:
                    logger.info("Time synchronized with systemd-timesyncd")
                    return True
            elif await self._service_active("ntp"):
                proc = await self._run_command("sudo", "systemctl", "restart", "ntp")
                if proc:
                    logger.info("Time synchronized with NTP")
                    return True
            elif await self._service_active("chrony"):
                proc = await self._run_command("sudo", "chronyd", "-q")
                if proc:
                    logger.info("Time synchronized with Chrony")
                    return True
            else:
                # Fall back to ntpdate
                proc = await self._run_command("sudo", "ntpdate", self.config.primary_ntp)
                if proc:
                    logger.info(f"Time synchronized with ntpdate to {self.config.primary_ntp}")
                    return True
            
            logger.error("Failed to synchronize time with NTP servers")
            return False
        except Exception as e:
            logger.error(f"Error synchronizing time: {e}")
            return False
    
    async def _service_active(self, service: str) -> bool:
        """
        Check if a systemd service is active.
        
        Args:
            service (str): The service name.
            
        Returns:
            bool: True if the service is active, False otherwise.
        """
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", service,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            return proc.returncode == 0 and stdout.decode().strip() == "active"
        except Exception:
            return False
    
    async def _file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            file_path (str): The path to check.
            
        Returns:
            bool: True if the file exists, False otherwise.
        """
        return os.path.exists(file_path)
    
    async def _run_command(self, *args) -> Optional[asyncio.subprocess.Process]:
        """
        Run a system command asynchronously.
        
        Args:
            *args: Command and arguments to run.
            
        Returns:
            Optional[asyncio.subprocess.Process]: The process, or None if an error occurred.
        """
        try:
            logger.debug(f"Running command: {' '.join(args)}")
            
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                return proc
            else:
                logger.error(f"Command failed with return code {proc.returncode}")
                logger.error(f"STDERR: {stderr.decode()}")
                return None
        except Exception as e:
            logger.error(f"Error running command: {e}")
            return None
    
    async def shutdown(self):
        """
        Perform any cleanup needed when shutting down.
        """
        logger.debug("Time manager shutdown")
        # No specific cleanup needed for time manager