"""
Network configuration manager for the DPM system.

This module handles all network-related configurations, including:
- Setting static IP or DHCP
- Configuring DNS servers
- Applying network settings to the system
"""
import asyncio
import subprocess
import os
import tempfile
from typing import Optional, List, Dict
import ipaddress
import logging
from app.utils.validator import NetworkConfig

logger = logging.getLogger("NetworkManager")
logger.setLevel(logging.DEBUG)

class NetworkManager:
    """
    Manages network configuration for the system.
    """
    def __init__(self, config: NetworkConfig):
        """
        Initialize the NetworkManager.
        
        Args:
            config (NetworkConfig): The network configuration.
        """
        self.config = config
        self.interface = "eth0"  # Default network interface
        self._current_config = None
    
    async def get_current_config(self) -> Dict:
        """
        Get the current network configuration from the system.
        
        Returns:
            Dict: The current network configuration.
        """
        try:
            result = {}
            
            # Get IP address and subnet mask
            ip_proc = await asyncio.create_subprocess_exec(
                "ip", "-o", "-4", "addr", "show", self.interface,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ip_proc.communicate()
            
            if ip_proc.returncode == 0:
                # Parse output like: "2: eth0    inet 192.168.1.2/24 brd 192.168.1.255 scope global eth0"
                ip_output = stdout.decode().strip()
                if ip_output:
                    ip_parts = ip_output.split()
                    for i, part in enumerate(ip_parts):
                        if part == "inet" and i + 1 < len(ip_parts):
                            ip_addr = ip_parts[i + 1].split("/")[0]
                            prefix = ip_parts[i + 1].split("/")[1]
                            subnet_mask = self._prefix_to_subnet_mask(int(prefix))
                            result["ip_address"] = ip_addr
                            result["subnet_mask"] = subnet_mask
            
            # Get gateway
            gateway_proc = await asyncio.create_subprocess_exec(
                "ip", "route", "show", "default",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await gateway_proc.communicate()
            
            if gateway_proc.returncode == 0:
                # Parse output like: "default via 192.168.1.1 dev eth0"
                gateway_output = stdout.decode().strip()
                if gateway_output:
                    parts = gateway_output.split()
                    if len(parts) >= 3 and parts[0] == "default" and parts[1] == "via":
                        result["gateway"] = parts[2]
            
            # Get DNS servers from resolv.conf
            result["dns_servers"] = await self._get_dns_servers()
            
            # Check if DHCP is enabled
            result["dhcp"] = await self._is_dhcp_enabled()
            
            self._current_config = result
            return result
            
        except Exception as e:
            logger.error(f"Error getting current network configuration: {e}")
            return {}
    
    async def _get_dns_servers(self) -> List[str]:
        """
        Get the current DNS servers from resolv.conf.
        
        Returns:
            List[str]: List of DNS server IP addresses.
        """
        dns_servers = []
        try:
            with open("/etc/resolv.conf", "r") as f:
                for line in f:
                    if line.startswith("nameserver"):
                        parts = line.split()
                        if len(parts) >= 2:
                            dns_servers.append(parts[1])
            return dns_servers
        except Exception as e:
            logger.error(f"Error reading DNS servers: {e}")
            return []
    
    async def _is_dhcp_enabled(self) -> bool:
        """
        Check if DHCP is enabled for the network interface.
        
        Returns:
            bool: True if DHCP is enabled, False otherwise.
        """
        try:
            # Check if dhclient is running for the interface
            ps_proc = await asyncio.create_subprocess_exec(
                "ps", "-aux",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ps_proc.communicate()
            
            if ps_proc.returncode == 0:
                output = stdout.decode()
                return f"dhclient {self.interface}" in output
            
            return False
        except Exception as e:
            logger.error(f"Error checking DHCP status: {e}")
            return False
    
    def _prefix_to_subnet_mask(self, prefix: int) -> str:
        """
        Convert a prefix length to a subnet mask.
        
        Args:
            prefix (int): The prefix length (e.g., 24 for 255.255.255.0).
            
        Returns:
            str: The subnet mask in dot notation.
        """
        try:
            return str(ipaddress.IPv4Network(f"0.0.0.0/{prefix}").netmask)
        except Exception:
            return "255.255.255.0"  # Default subnet mask
    
    async def apply_config(self) -> bool:
        """
        Apply the network configuration to the system.
        
        Returns:
            bool: True if the configuration was applied successfully, False otherwise.
        """
        try:
            # Get current configuration to see if changes are needed
            current_config = await self.get_current_config()
            
            # Check if any changes are needed
            if self._config_matches_current():
                logger.info("Current network configuration already matches desired configuration")
                return True
            
            if self.config.dhcp:
                # Configure DHCP
                return await self._configure_dhcp()
            else:
                # Configure static IP
                return await self._configure_static_ip()
        except Exception as e:
            logger.error(f"Error applying network configuration: {e}")
            return False
    
    def _config_matches_current(self) -> bool:
        """
        Check if the current configuration matches the desired configuration.
        
        Returns:
            bool: True if the configurations match, False otherwise.
        """
        if not self._current_config:
            return False
            
        # Check if DHCP setting matches
        if self.config.dhcp != self._current_config.get("dhcp", False):
            return False
            
        # If DHCP is enabled, we don't need to check the other settings
        if self.config.dhcp:
            return True
            
        # Check static IP settings
        if self.config.ip_address != self._current_config.get("ip_address", ""):
            return False
            
        if self.config.subnet_mask != self._current_config.get("subnet_mask", ""):
            return False
            
        if self.config.gateway != self._current_config.get("gateway", ""):
            return False
            
        # Check DNS servers
        current_dns = self._current_config.get("dns_servers", [])
        if self.config.primary_dns not in current_dns:
            return False
            
        if self.config.secondary_dns and self.config.secondary_dns not in current_dns:
            return False
            
        return True
    
    async def _configure_dhcp(self) -> bool:
        """
        Configure the network interface to use DHCP.
        
        Returns:
            bool: True if DHCP was configured successfully, False otherwise.
        """
        try:
            # Stop any running dhclient
            await self._run_command("killall", "dhclient")
            
            # Clear existing IP configuration
            await self._run_command("ip", "addr", "flush", "dev", self.interface)
            
            # Start dhclient to get IP from DHCP
            dhcp_proc = await self._run_command("dhclient", self.interface)
            
            if dhcp_proc:
                logger.info(f"DHCP enabled for {self.interface}")
                
                # Configure DNS servers
                await self._configure_dns()
                
                return True
            else:
                logger.error(f"Failed to enable DHCP for {self.interface}")
                return False
                
        except Exception as e:
            logger.error(f"Error configuring DHCP: {e}")
            return False
    
    async def _configure_static_ip(self) -> bool:
        """
        Configure the network interface with a static IP.
        
        Returns:
            bool: True if the static IP was configured successfully, False otherwise.
        """
        try:
            # Stop any running dhclient
            await self._run_command("killall", "dhclient")
            
            # Clear existing IP configuration
            await self._run_command("ip", "addr", "flush", "dev", self.interface)
            
            # Set the static IP
            prefix = self._subnet_mask_to_prefix(self.config.subnet_mask)
            await self._run_command("ip", "addr", "add", f"{self.config.ip_address}/{prefix}", "dev", self.interface)
            
            # Set the gateway
            await self._run_command("ip", "route", "add", "default", "via", self.config.gateway)
            
            # Configure DNS servers
            await self._configure_dns()
            
            logger.info(f"Static IP configured: {self.config.ip_address}")
            return True
        except Exception as e:
            logger.error(f"Error configuring static IP: {e}")
            return False
    
    def _subnet_mask_to_prefix(self, subnet_mask: str) -> int:
        """
        Convert a subnet mask to a prefix length.
        
        Args:
            subnet_mask (str): The subnet mask in dot notation.
            
        Returns:
            int: The prefix length.
        """
        try:
            return ipaddress.IPv4Network(f"0.0.0.0/{subnet_mask}").prefixlen
        except Exception:
            return 24  # Default prefix length
    
    async def _configure_dns(self) -> bool:
        """
        Configure DNS servers in resolv.conf.
        
        Returns:
            bool: True if DNS was configured successfully, False otherwise.
        """
        try:
            # Create resolv.conf content
            resolv_content = "# Generated by DPM NetworkManager\n"
            resolv_content += f"nameserver {self.config.primary_dns}\n"
            
            if self.config.secondary_dns:
                resolv_content += f"nameserver {self.config.secondary_dns}\n"
            
            # Write to a temporary file first
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
                temp_path = temp_file.name
                temp_file.write(resolv_content)
            
            # Move the temporary file to /etc/resolv.conf
            move_proc = await self._run_command("sudo", "mv", temp_path, "/etc/resolv.conf")
            
            if move_proc:
                logger.info("DNS configuration updated")
                return True
            else:
                logger.error("Failed to update DNS configuration")
                return False
        except Exception as e:
            logger.error(f"Error configuring DNS: {e}")
            return False
    
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
        logger.debug("Network manager shutdown")
        # No specific cleanup needed for network manager