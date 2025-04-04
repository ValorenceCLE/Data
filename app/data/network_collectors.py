"""
Network Data Collection Module

This module provides asynchronous network data collection capabilities,
focusing on ping metrics to Google.
"""
import asyncio
import re
import json
import logging
from typing import Dict, Any

# Set up logging
logger = logging.getLogger("NetworkDataCollector")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class NetworkDataCollector:
    """
    Manages asynchronous collection of network performance metrics.
    """
    def __init__(self, collection_interval: int = 60):
        """
        Initialize the NetworkDataCollector.
        
        Args:
            collection_interval (int): Time between data collection cycles.
        """
        self.target = "8.8.8.8"  # Google DNS
        self.collection_interval = collection_interval
        self._running = False
        
        # Store the latest metrics
        self.ping_metrics: Dict[str, Any] = {
            "min_rtt": None,
            "avg_rtt": None,
            "max_rtt": None,
            "packet_loss": None
        }
    
    async def _ping_target(self, target: str, count: int = 5) -> Dict[str, Any]:
        """
        Perform a ping to a target and collect metrics.
        
        Args:
            target (str): Hostname or IP to ping
            count (int): Number of ping packets to send
        
        Returns:
            Dict[str, Any]: Ping metrics
        """
        try:
            # Use the more modern Linux ping 
            ping_cmd = [
                "ping", "-c", str(count), 
                "-W", "2",  # 2-second timeout 
                target
            ]
            
            # Run ping command
            proc = await asyncio.create_subprocess_exec(
                *ping_cmd, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait for the process to complete
            stdout, stderr = await proc.communicate()
            
            if proc.returncode is None or proc.returncode > 0:
                # Ping failed
                return {
                    "success": False,
                    "error": stderr.decode('utf-8', errors='ignore').strip()
                }
            
            # Parse ping output
            output = stdout.decode('utf-8', errors='ignore')
            
            # Extract metrics using regex
            ping_stats = {}
            
            # Parse packet loss
            loss_match = re.search(r'(\d+)%\s+packet\s+loss', output)
            if loss_match:
                ping_stats['packet_loss'] = float(loss_match.group(1))
            
            # Parse round-trip times
            rtt_match = re.search(r'rtt\s+min/avg/max/mdev\s*=\s*([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)\s*ms', output)
            if rtt_match:
                ping_stats.update({
                    'min_rtt': float(rtt_match.group(1)),
                    'avg_rtt': float(rtt_match.group(2)),
                    'max_rtt': float(rtt_match.group(3))
                })
            
            # Build full result
            return {
                "success": True,
                **ping_stats
            }
        
        except Exception as e:
            logger.error(f"Ping error for {target}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _network_data_collection_cycle(self):
        """
        Perform a complete network data collection cycle.
        """
        # Collect ping metrics
        metrics = await self._ping_target(self.target)
        
        # Update ping metrics
        if metrics['success']:
            self.ping_metrics = {
                "min_rtt": metrics.get('min_rtt'),
                "avg_rtt": metrics.get('avg_rtt'),
                "max_rtt": metrics.get('max_rtt'),
                "packet_loss": metrics.get('packet_loss')
            }
        else:
            # Reset metrics if ping fails
            self.ping_metrics = {
                "min_rtt": None,
                "avg_rtt": None,
                "max_rtt": None,
                "packet_loss": None
            }
        
        # Log collected metrics
        logger.debug("Network Metrics:")
        logger.debug(json.dumps(self.ping_metrics, indent=2))
    
    async def run(self):
        """
        Start continuous network data collection.
        """
        if self._running:
            logger.warning("Network data collection already running")
            return
        
        self._running = True
        logger.info("Starting network data collection")
        
        try:
            while self._running:
                # Collect network data
                await self._network_data_collection_cycle()
                
                # Wait until next collection interval
                await asyncio.sleep(self.collection_interval)
        
        except asyncio.CancelledError:
            logger.info("Network data collection cancelled")
        except Exception as e:
            logger.error(f"Error in network data collection: {e}")
        finally:
            self._running = False
    
    async def shutdown(self):
        """
        Stop network data collection.
        """
        if not self._running:
            return
        
        logger.info("Shutting down network data collection")
        self._running = False
        
        # Wait a moment to ensure cleanup
        await asyncio.sleep(1)
    
    def get_network_metrics(self) -> Dict[str, Any]:
        """
        Get the latest network metrics.
        
        Returns:
            Dict[str, Any]: Current network metrics
        """
        return self.ping_metrics.copy()