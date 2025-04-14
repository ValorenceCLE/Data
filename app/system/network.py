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
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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
            # Check if dhclient is running for the interface
            ps_proc = await asyncio.create_subprocess_exec(
                "ps", "-aux",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await ps_proc.communicate()
            
            if ps_proc.returncode == 0:
                output = stdout.decode()
                dhcpcd_running = f"dhcpcd {self.interface}" in output or f"dhcpcd -i {self.interface}" in output
                dhclient_running = f"dhclient {self.interface}" in output
                is_dhcp = dhcpcd_running or dhclient_running
                
                # If no dhcp client running, check dhcpcd.conf
                if not is_dhcp and os.path.exists("/etc/dhcpcd.conf"):
                    # Check if interface is statically configured in dhcpcd.conf
                    with open("/etc/dhcpcd.conf", "r") as f:
                        content = f.read()
                        if f"interface {self.interface}" in content and "static ip_address" in content:
                            is_dhcp = False
                        else:
                            # Default is DHCP in dhcpcd.conf
                            is_dhcp = True
                
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
    
    def _subnet_mask_to_prefix(self, subnet_mask: str) -> int:
        """
        Convert a subnet mask to a prefix length.
        
        Args:
            subnet_mask (str): The subnet mask (e.g., 255.255.255.0).
            
        Returns:
            int: The prefix length (e.g., 24).
        """
        try:
            return sum(bin(int(x)).count('1') for x in subnet_mask.split('.'))
        except Exception as e:
            logger.error(f"Error converting subnet mask to prefix: {e}")
            return 24  # Default prefix length
    
    def _in_interface_block(self, line: str, file) -> bool:
        """
        Check if a line is within an interface block for the specified interface.
        Used for parsing dhcpcd.conf.
        
        Args:
            line (str): The line to check.
            file: The file being read.
            
        Returns:
            bool: True if in interface block, False otherwise.
        """
        # Simple implementation - could be improved with more context
        return line.strip().startswith("static ") or line.strip().startswith("nohook ")
    
    async def _check_command_exists(self, command: str) -> bool:
        """
        Check if a command exists.
        
        Args:
            command (str): Command to check.
            
        Returns:
            bool: True if exists, False otherwise.
        """
        try:
            which_proc = await asyncio.create_subprocess_exec(
                "which", command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await which_proc.communicate()
            
            return which_proc.returncode == 0
        except Exception:
            return False

    def _config_matches_current(self, current_config) -> bool:
        """
        Check if the desired configuration matches the current configuration.
        
        Args:
            current_config (Dict): The current network configuration.
            
        Returns:
            bool: True if configurations match, False otherwise.
        """
        # Skip if current config is empty
        if not current_config:
            return False
        
        # Check DHCP status
        if current_config.get('dhcp') != self.config.dhcp:
            logger.debug(f"DHCP status mismatch: current={current_config.get('dhcp')}, desired={self.config.dhcp}")
            return False
        
        # If using static IP, check all IP settings
        if not self.config.dhcp:
            # Check IP address
            if current_config.get('ip_address') != self.config.ip_address:
                logger.debug(f"IP address mismatch: current={current_config.get('ip_address')}, desired={self.config.ip_address}")
                return False
            
            # Check subnet mask
            if current_config.get('subnet_mask') != self.config.subnet_mask:
                logger.debug(f"Subnet mask mismatch: current={current_config.get('subnet_mask')}, desired={self.config.subnet_mask}")
                return False
            
            # Check gateway
            if current_config.get('gateway') != self.config.gateway:
                logger.debug(f"Gateway mismatch: current={current_config.get('gateway')}, desired={self.config.gateway}")
                return False
        
        # Check DNS servers
        current_dns = current_config.get('dns_servers', [])
        if self.config.primary_dns not in current_dns:
            logger.debug(f"Primary DNS mismatch: {self.config.primary_dns} not in {current_dns}")
            return False
        
        if self.config.secondary_dns and self.config.secondary_dns not in current_dns:
            logger.debug(f"Secondary DNS mismatch: {self.config.secondary_dns} not in {current_dns}")
            return False
        
        logger.debug("Current network configuration matches desired configuration")
        return True

    async def apply_config(self) -> bool:
        """
        Apply the network configuration to the system.
        
        Returns:
            bool: True if the configuration was applied successfully, False otherwise.
        """
        logger.debug("Applying network configuration")
        try:
            # Get current configuration to see if changes are needed
            current_config = await self.get_current_config()
            logger.debug(f"Current network configuration: {current_config}")
            
            if self._config_matches_current(current_config):
                logger.info("Current network configuration already matches desired configuration")
                return True
            
            # Try to detect what's available
            dhcpcd_exists = os.path.exists("/etc/dhcpcd.conf")
            netplan_exists = os.path.exists("/etc/netplan")
            has_nmcli = await self._check_command_exists("nmcli")
            
            if dhcpcd_exists:
                # Use dhcpcd approach
                success = await self._apply_dhcpcd_config()
            elif netplan_exists:
                # Use netplan approach
                success = await self._apply_netplan_config()
            elif has_nmcli:
                # Use NetworkManager approach
                success = await self._apply_network_manager_config()
            else:
                # Fallback to direct interfaces config
                success = await self._apply_interfaces_config()
                
            if success:
                logger.info("Network configuration applied successfully")
            else:
                logger.error("Failed to apply network configuration")
                
            return success
        except Exception as e:
            logger.error(f"Error applying network configuration: {e}")
            return False

    async def _apply_dhcpcd_config(self) -> bool:
        """
        Apply network configuration using dhcpcd.conf.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Applying network configuration via dhcpcd.conf")
        
        try:
            # Create a temporary file for the new configuration
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                # Copy existing configuration first
                if os.path.exists("/etc/dhcpcd.conf"):
                    in_our_interface_block = False
                    with open("/etc/dhcpcd.conf", 'r') as current_file:
                        for line in current_file:
                            if line.strip().startswith(f"interface {self.interface}"):
                                in_our_interface_block = True
                                continue
                            elif line.strip().startswith("interface "):
                                in_our_interface_block = False
                            
                            if not in_our_interface_block:
                                temp_file.write(line)
                
                # Add our interface configuration
                temp_file.write(f"\n# Configuration for {self.interface}\n")
                temp_file.write(f"interface {self.interface}\n")
                
                if self.config.dhcp:
                    # DHCP configuration
                    temp_file.write("# Use DHCP\n")
                else:
                    # Static IP configuration
                    temp_file.write("# Static IP configuration\n")
                    temp_file.write("static ip_address={}/{}\n".format(
                        self.config.ip_address,
                        self._subnet_mask_to_prefix(self.config.subnet_mask)
                    ))
                    temp_file.write(f"static routers={self.config.gateway}\n")
                    
                    # DNS configuration
                    dns_servers = self.config.primary_dns
                    if self.config.secondary_dns:
                        dns_servers += f" {self.config.secondary_dns}"
                    temp_file.write(f"static domain_name_servers={dns_servers}\n")
                
                temp_file_path = temp_file.name
            
            # Copy the new configuration to dhcpcd.conf
            copy_proc = await asyncio.create_subprocess_exec(
                "sudo", "cp", temp_file_path, "/etc/dhcpcd.conf",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await copy_proc.communicate()
            
            # Remove the temporary file
            os.unlink(temp_file_path)
            
            if copy_proc.returncode != 0:
                logger.error(f"Failed to update dhcpcd.conf: {stderr.decode().strip()}")
                return False
            
            # Restart dhcpcd service
            restart_proc = await asyncio.create_subprocess_exec(
                "sudo", "systemctl", "restart", "dhcpcd",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await restart_proc.communicate()
            
            if restart_proc.returncode != 0:
                logger.error(f"Failed to restart dhcpcd: {stderr.decode().strip()}")
                # Try to restart networking
                return await self._restart_networking()
            
            logger.info("Applied network configuration via dhcpcd.conf successfully")
            return True
        except Exception as e:
            logger.error(f"Error applying dhcpcd configuration: {e}")
            return False

    async def _apply_netplan_config(self) -> bool:
        """
        Apply network configuration using netplan.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Applying network configuration via netplan")
        
        try:
            # Create netplan YAML configuration
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write("# Generated by DPM NetworkManager\n")
                temp_file.write("network:\n")
                temp_file.write("  version: 2\n")
                temp_file.write("  renderer: networkd\n")
                temp_file.write("  ethernets:\n")
                temp_file.write(f"    {self.interface}:\n")
                
                if self.config.dhcp:
                    # DHCP configuration
                    temp_file.write("      dhcp4: true\n")
                else:
                    # Static IP configuration
                    temp_file.write("      dhcp4: false\n")
                    temp_file.write(f"      addresses: [{self.config.ip_address}/{self._subnet_mask_to_prefix(self.config.subnet_mask)}]\n")
                    temp_file.write(f"      gateway4: {self.config.gateway}\n")
                
                # DNS configuration
                temp_file.write("      nameservers:\n")
                temp_file.write("        addresses: [")
                if self.config.secondary_dns:
                    temp_file.write(f"{self.config.primary_dns}, {self.config.secondary_dns}")
                else:
                    temp_file.write(f"{self.config.primary_dns}")
                temp_file.write("]\n")
                
                temp_file_path = temp_file.name
            
            # Copy the configuration to netplan
            netplan_file = "/etc/netplan/01-netcfg.yaml"
            copy_proc = await asyncio.create_subprocess_exec(
                "sudo", "cp", temp_file_path, netplan_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await copy_proc.communicate()
            
            # Remove the temporary file
            os.unlink(temp_file_path)
            
            if copy_proc.returncode != 0:
                logger.error(f"Failed to update netplan configuration: {stderr.decode().strip()}")
                return False
            
            # Apply the netplan configuration
            apply_proc = await asyncio.create_subprocess_exec(
                "sudo", "netplan", "apply",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await apply_proc.communicate()
            
            if apply_proc.returncode != 0:
                logger.error(f"Failed to apply netplan configuration: {stderr.decode().strip()}")
                return False
            
            logger.info("Applied network configuration via netplan successfully")
            return True
        except Exception as e:
            logger.error(f"Error applying netplan configuration: {e}")
            return False

    async def _apply_network_manager_config(self) -> bool:
        """
        Apply network configuration using NetworkManager (nmcli).
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Applying network configuration via NetworkManager")
        
        try:
            # Get the current connection name for the interface
            conn_proc = await asyncio.create_subprocess_exec(
                "nmcli", "-t", "-f", "NAME,DEVICE", "connection", "show", "--active",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await conn_proc.communicate()
            
            connection_name = None
            if conn_proc.returncode == 0:
                output = stdout.decode().strip()
                for line in output.split('\n'):
                    parts = line.split(':')
                    if len(parts) >= 2 and parts[1] == self.interface:
                        connection_name = parts[0]
                        break
            
            if not connection_name:
                # Create a new connection if none exists
                connection_name = f"{self.interface}-connection"
                create_proc = await asyncio.create_subprocess_exec(
                    "sudo", "nmcli", "connection", "add", 
                    "type", "ethernet", 
                    "con-name", connection_name, 
                    "ifname", self.interface,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await create_proc.communicate()
                
                if create_proc.returncode != 0:
                    logger.error(f"Failed to create NetworkManager connection: {stderr.decode().strip()}")
                    return False
            
            # Modify the connection
            if self.config.dhcp:
                # DHCP configuration
                modify_proc = await asyncio.create_subprocess_exec(
                    "sudo", "nmcli", "connection", "modify", 
                    connection_name, 
                    "ipv4.method", "auto",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
            else:
                # Static IP configuration
                cmd = [
                    "sudo", "nmcli", "connection", "modify", 
                    connection_name, 
                    "ipv4.method", "manual",
                    "ipv4.addresses", f"{self.config.ip_address}/{self._subnet_mask_to_prefix(self.config.subnet_mask)}",
                    "ipv4.gateway", self.config.gateway
                ]
                
                # DNS configuration
                dns_servers = self.config.primary_dns
                if self.config.secondary_dns:
                    dns_servers += f",{self.config.secondary_dns}"
                cmd.extend(["ipv4.dns", dns_servers])
                
                modify_proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
            
            stdout, stderr = await modify_proc.communicate()
            
            if modify_proc.returncode != 0:
                logger.error(f"Failed to modify NetworkManager connection: {stderr.decode().strip()}")
                return False
            
            # Apply the connection
            up_proc = await asyncio.create_subprocess_exec(
                "sudo", "nmcli", "connection", "up", connection_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await up_proc.communicate()
            
            if up_proc.returncode != 0:
                logger.error(f"Failed to apply NetworkManager connection: {stderr.decode().strip()}")
                return False
            
            logger.info("Applied network configuration via NetworkManager successfully")
            return True
        except Exception as e:
            logger.error(f"Error applying NetworkManager configuration: {e}")
            return False

    async def _apply_interfaces_config(self) -> bool:
        """
        Apply network configuration using /etc/network/interfaces.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Applying network configuration via /etc/network/interfaces")
        
        try:
            # Create interfaces file
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write("# Generated by DPM NetworkManager\n")
                temp_file.write("# The loopback network interface\n")
                temp_file.write("auto lo\n")
                temp_file.write("iface lo inet loopback\n\n")
                
                temp_file.write(f"# The {self.interface} network interface\n")
                temp_file.write(f"auto {self.interface}\n")
                
                if self.config.dhcp:
                    # DHCP configuration
                    temp_file.write(f"iface {self.interface} inet dhcp\n")
                else:
                    # Static IP configuration
                    temp_file.write(f"iface {self.interface} inet static\n")
                    temp_file.write(f"    address {self.config.ip_address}\n")
                    temp_file.write(f"    netmask {self.config.subnet_mask}\n")
                    temp_file.write(f"    gateway {self.config.gateway}\n")
                    
                    # DNS configuration
                    dns_servers = self.config.primary_dns
                    if self.config.secondary_dns:
                        dns_servers += f" {self.config.secondary_dns}"
                    temp_file.write(f"    dns-nameservers {dns_servers}\n")
                
                temp_file_path = temp_file.name
            
            # Copy the configuration to interfaces file
            copy_proc = await asyncio.create_subprocess_exec(
                "sudo", "cp", temp_file_path, "/etc/network/interfaces",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await copy_proc.communicate()
            
            # Remove the temporary file
            os.unlink(temp_file_path)
            
            if copy_proc.returncode != 0:
                logger.error(f"Failed to update interfaces file: {stderr.decode().strip()}")
                return False
            
            # Restart networking
            return await self._restart_networking()
        except Exception as e:
            logger.error(f"Error applying interfaces configuration: {e}")
            return False

    async def _restart_networking(self) -> bool:
        """
        Restart networking services to apply configuration.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug("Restarting networking services")
        
        try:
            # Try systemd networking restart
            restart_proc = await asyncio.create_subprocess_exec(
                "sudo", "systemctl", "restart", "networking",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await restart_proc.communicate()
            
            if restart_proc.returncode == 0:
                logger.info("Networking restarted successfully via systemd")
                return True
            else:
                logger.warning(f"Failed to restart networking via systemd: {stderr.decode().strip()}")
                
                # Try ifdown/ifup as fallback
                logger.debug(f"Trying ifdown/ifup for interface {self.interface}")
                down_proc = await asyncio.create_subprocess_exec(
                    "sudo", "ifdown", self.interface,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await down_proc.communicate()
                
                # Wait a moment before bringing the interface back up
                await asyncio.sleep(2)
                
                up_proc = await asyncio.create_subprocess_exec(
                    "sudo", "ifup", self.interface,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await up_proc.communicate()
                
                if up_proc.returncode == 0:
                    logger.info(f"Interface {self.interface} restarted successfully via ifdown/ifup")
                    return True
                else:
                    logger.error(f"Failed to restart interface via ifdown/ifup: {stderr.decode().strip()}")
                    return False
        except Exception as e:
            logger.error(f"Error restarting networking: {e}")
            return False