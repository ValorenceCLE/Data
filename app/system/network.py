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
        logger.debug(f"Initialized NetworkManager with config: {self.config}")

    async def get_current_config(self) -> Dict:
        """
        Get the current network configuration from the system.
        
        Returns:
            Dict: The current network configuration.
        """
        logger.debug("Fetching current network configuration...")
        try:
            result = {}
            
            # Get IP address and subnet mask
            logger.debug(f"Getting IP address and subnet mask for interface {self.interface}")
            ip_proc = await asyncio.create_subprocess_exec(
                "ip", "-o", "-4", "addr", "show", self.interface,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ip_proc.communicate()
            
            if ip_proc.returncode == 0:
                ip_output = stdout.decode().strip()
                logger.debug(f"IP command output: {ip_output}")
                if ip_output:
                    ip_parts = ip_output.split()
                    for i, part in enumerate(ip_parts):
                        if part == "inet" and i + 1 < len(ip_parts):
                            ip_addr = ip_parts[i + 1].split("/")[0]
                            prefix = ip_parts[i + 1].split("/")[1]
                            subnet_mask = self._prefix_to_subnet_mask(int(prefix))
                            result["ip_address"] = ip_addr
                            result["subnet_mask"] = subnet_mask
                            logger.debug(f"Parsed IP address: {ip_addr}, Subnet mask: {subnet_mask}")
            else:
                logger.error(f"Failed to get IP address: {stderr.decode()}")

            # Get gateway
            logger.debug("Getting default gateway")
            gateway_proc = await asyncio.create_subprocess_exec(
                "ip", "route", "show", "default",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await gateway_proc.communicate()
            
            if gateway_proc.returncode == 0:
                gateway_output = stdout.decode().strip()
                logger.debug(f"Gateway command output: {gateway_output}")
                if gateway_output:
                    parts = gateway_output.split()
                    if len(parts) >= 3 and parts[0] == "default" and parts[1] == "via":
                        result["gateway"] = parts[2]
                        logger.debug(f"Parsed gateway: {result['gateway']}")
            else:
                logger.error(f"Failed to get gateway: {stderr.decode()}")

            # Get DNS servers from resolv.conf
            logger.debug("Getting DNS servers")
            result["dns_servers"] = await self._get_dns_servers()
            
            # Check if DHCP is enabled
            logger.debug("Checking if DHCP is enabled")
            result["dhcp"] = await self._is_dhcp_enabled()
            
            self._current_config = result
            logger.debug(f"Current network configuration: {result}")
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
        logger.debug("Reading DNS servers from /etc/resolv.conf")
        dns_servers = []
        try:
            with open("/etc/resolv.conf", "r") as f:
                for line in f:
                    if line.startswith("nameserver"):
                        parts = line.split()
                        if len(parts) >= 2:
                            dns_servers.append(parts[1])
                            logger.debug(f"Found DNS server: {parts[1]}")
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
        logger.debug(f"Checking if DHCP is enabled for interface {self.interface}")
        try:
            ps_proc = await asyncio.create_subprocess_exec(
                "ps", "-aux",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ps_proc.communicate()
            
            if ps_proc.returncode == 0:
                output = stdout.decode()
                is_dhcp = f"dhclient {self.interface}" in output
                logger.debug(f"DHCP status for {self.interface}: {is_dhcp}")
                return is_dhcp
            else:
                logger.error(f"Failed to check DHCP status: {stderr.decode()}")
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
        logger.debug(f"Converting prefix {prefix} to subnet mask")
        try:
            subnet_mask = str(ipaddress.IPv4Network(f"0.0.0.0/{prefix}").netmask)
            logger.debug(f"Converted prefix {prefix} to subnet mask {subnet_mask}")
            return subnet_mask
        except Exception as e:
            logger.error(f"Error converting prefix to subnet mask: {e}")
            return "255.255.255.0"  # Default subnet mask
    
    # Additional methods would also include similar detailed logging.
