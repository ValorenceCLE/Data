"""
Asynchronous InfluxDB Data Uploader

Provides an efficient method to upload sensor data to InfluxDB.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime, timezone
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api_async import WriteApiAsync

from app.utils.config import settings

logger = logging.getLogger(__name__)

class InfluxUploader:
    """
    Manages asynchronous uploads to InfluxDB with batching.
    """
    def __init__(
        self, 
        batch_size: int = 100,
        flush_interval: int = 5
    ):
        """
        Initialize the InfluxDB uploader.
        
        Args:
            batch_size (int): Number of points to batch before sending
            flush_interval (int): Seconds to wait before sending partial batches
        """
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        # Batch storage
        self.batch_queue: List[Point] = []
        
        # Client and write API
        self._client: Optional[InfluxDBClientAsync] = None
        self._write_api: Optional[WriteApiAsync] = None
        
        # State management
        self._running = False
    
    async def _initialize_client(self):
        """
        Initialize the InfluxDB async client.
        """
        if self._client is None:
            try:
                self._client = InfluxDBClientAsync(
                    url=settings.INFLUXDB_URL, 
                    token=settings.TOKEN, 
                    org=settings.ORG
                )
                # Create write API
                self._write_api = self._client.write_api_async()
                logger.info("InfluxDB client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize InfluxDB client: {e}")
                raise
    
    async def upload_sensor_data(
        self, 
        measurement: str, 
        tags: Dict[str, str], 
        fields: Dict[str, Union[float, int]], 
        timestamp: Optional[datetime] = None
    ):
        """
        Add a sensor data point to the upload batch.
        
        Args:
            measurement (str): Name of the measurement
            tags (Dict[str, str]): Tags to identify the data point
            fields (Dict[str, Union[float, int]]): Numeric fields to store
            timestamp (Optional[datetime]): Timestamp for the point
        """
        # Ensure client is initialized
        if self._client is None:
            await self._initialize_client()
        
        # Create point
        point = Point(measurement)
        
        # Add tags
        for tag_name, tag_value in tags.items():
            point = point.tag(tag_name, str(tag_value))
        
        # Add fields
        for field_name, field_value in fields.items():
            if isinstance(field_value, (int, float)):
                point = point.field(field_name, field_value)
            else:
                logger.warning(f"Skipping non-numeric field: {field_name}")
        
        # Set timestamp (default to now if not provided)
        point = point.time(timestamp or datetime.now(timezone.utc))
        
        # Add to batch queue
        self.batch_queue.append(point)
        
        # Check if batch is ready to send
        if len(self.batch_queue) >= self.batch_size:
            await self._send_batch()
    
    async def _send_batch(self):
        """
        Send the current batch of points to InfluxDB.
        """
        if not self.batch_queue:
            return
        
        try:
            # Ensure client is initialized
            if self._client is None:
                await self._initialize_client()
            
            # Write points
            await self._write_api.write(
                bucket=settings.BUCKET, 
                record=self.batch_queue
            )
            
            logger.info(f"Successfully uploaded {len(self.batch_queue)} points")
            
            # Clear the batch queue
            self.batch_queue.clear()
        
        except Exception as e:
            logger.error(f"Error uploading batch to InfluxDB: {e}")
    
    async def run(self):
        """
        Background task to periodically flush batched points.
        """
        self._running = True
        
        try:
            while self._running:
                # Wait for flush interval
                await asyncio.sleep(self.flush_interval)
                
                # Flush any pending points
                await self._send_batch()
        
        except asyncio.CancelledError:
            logger.info("InfluxDB uploader stopped")
        except Exception as e:
            logger.error(f"Error in InfluxDB uploader background task: {e}")
        finally:
            # Final flush on shutdown
            await self._send_batch()
    
    async def shutdown(self):
        """
        Gracefully shut down the uploader.
        """
        self._running = False
        
        # Final flush of any remaining points
        await self._send_batch()
        
        # Close the client
        if self._client:
            try:
                await self._client.close()
                logger.info("InfluxDB client closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB client: {e}")
            finally:
                self._client = None
                self._write_api = None