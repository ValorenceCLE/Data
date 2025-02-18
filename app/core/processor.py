import asyncio
from datetime import datetime, timezone
from influxdb_client import Point
from utils.logging_setup import local_logger as logger
from utils.config import settings
from utils.singleton import InfluxWriter, RedisClient
from aws.client import publish

class BaseProcessor:
    def __init__(self):
        self.redis=None
        self.write_api=None
        self.bucket=settings.BUCKET
        self.org=settings.ORG

    async def async_init(self):
        self.redis=await RedisClient.get_instance()
        self.write_api=await InfluxWriter.get_instance()
    
    async def write_to_influxdb(self, points):
        """Write one or many InfluxDB points to the database."""
        try:
            await self.write_api.write(bucket=self.bucket, org=self.org, record=points)
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
    
    async def publish_to_aws(self, topic: str, data: dict):
        """Publish data to AWS IoT Core."""
        try:
            await publish(topic, data)
            logger.debug(f"Published to AWS: {topic} - {data}")
        except Exception as e:
            logger.error(f"Failed to publish to AWS: {e}")

class RelayProcessor(BaseProcessor):
    """
    Processor for relay data streams. Relay data is collected at a high rate (Every Second) and must be averaged
    over a certain period (e.g., 60 data points over 60 seconds) before uploading to InfluxDB and AWS.
    """
    def __init__(self, relay_id, collection_interval=60, batch_size=60):
        super().__init__()
        self.relay_id=relay_id
        self.collection_interval=collection_interval
        self.batch_size=batch_size
        self.group_name=f'relay_group_{self.relay_id}'
        self.consumer_name=f'processor_{self.relay_id}'
    
    async def async_init(self):
        await super().async_init()
        await self.setup_groups()
    
    async def setup_groups(self):
        """
        Setup the Redis consumer groups for the relay stream. If the group already exists, proceed and log.
        """
        try:
            await self.redis.xgroup_create(self.relay_id, self.group_name, id='0', mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.critical(f"Error creating group {self.group_name} for {self.relay_id}: {e}")
            else:
                logger.debug(f"Group {self.group_name} already exists for {self.relay_id}")

    async def process_relay_stream(self):
        """Read a batch of messages from the relay stream and aggregate results."""
        try:
            message = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={self.relay_id: '>'},
                count=self.batch_size,
                block=1000
            )
            if message:
                await self.process_data(message)
        except Exception as e:
            logger.error(f"Error reading stream {self.relay_id}: {e}")

    async def process_data(self, message):
        """
        Average the relay data points (volts, watts, amps) and then:
        - Write the averaged data to InfluxDB
        - Publish the same averaged data to AWS IoT under topic `relay/data`
        """
        _volts=0.0
        _watts=0.0
        _amps=0.0
        msgs=message[0][1]
        count=0

        for message_id, msg in msgs:
            try:
                volts=float(msg[b'volts'])
                watts=float(msg[b'watts'])
                amps=float(msg[b'amps'])
                _volts+=volts
                _watts+=watts
                _amps+=amps
                count+=1
            except Exception as e:
                logger.error(f"Error processing message {message_id}: {e}")
        if count>0:
            avg_volts=round(_volts/count, 2)
            avg_watts=round(_watts/count, 2)
            avg_amps=round(_amps/count, 2)
            timestamp=datetime.now(timezone.utc).astimezone().isoformat()
            # Generic Data dictionary to be used for InfluxDB and AWS
            data={
                "source": self.relay_id,
                "timestamp": timestamp,
                "volts": avg_volts,
                "watts": avg_watts,
                "amps": avg_amps,
            }

            # InfluxDB Point
            point = Point(self.relay_id)\
                .tag("source", data['source'])\
                .field("volts", data['volts'])\
                .field("watts", data['watts'])\
                .field("amps", data['amps'])\
                .time(datetime.fromisoformat(data['timestamp']))
            
        # Concurrently write to InfluxDB and publish to AWS
            await asyncio.gather(
                self.write_to_influxdb(point),
                self.publish_to_aws("relay/data", data)
            )

    async def run(self):
        """Main loop for the Relay Processor."""
        await self.async_init()
        while True:
            await self.process_relay_stream()
            await asyncio.sleep(self.collection_interval)

class GeneralProcessor(BaseProcessor):
    """
    Processor for general data streams (cellular, network, environmental).
    These data points are collected less frequently and can be uploaded as-is.
    After uploading to InfluxDB, also publish to AWS IoT.
    """

    def __init__(self, streams, collection_interval=300):
        super().__init__()
        self.collection_interval = collection_interval
        self.group_name = 'general_group'
        self.consumer_name = 'general_processor'
        self.streams = streams

    async def async_init(self):
        await super().async_init()
        await self.setup_groups()

    async def setup_groups(self):
        """
        Create a consumer group for each stream. If already exists, proceed without error.
        """
        for stream in self.streams:
            try:
                await self.redis.xgroup_create(stream, self.group_name, id='0', mkstream=True)
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    logger.debug(f"Group {self.group_name} already exists for {stream}")
                else:
                    logger.critical(f"Error creating group {self.group_name} for {stream}: {e}")

    async def read_single_stream(self, stream_name):
        """
        Read up to 10 messages from a single stream. Blocks up to 1000ms if no messages are available.
        """
        try:
            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={stream_name: '>'},
                count=10,
                block=1000
            )
            return response
        except Exception as e:
            logger.error(f"Error reading stream {stream_name}: {e}")
            return []

    async def create_points_and_dicts(self, stream_name, messages):
        """
        From the raw Redis messages, create both:
        - A list of InfluxDB Point objects for database insertion.
        - A list of data dictionaries for AWS IoT publishing.

        Args:
            stream_name (bytes): The name of the stream as bytes.
            messages: The list of messages (message_id, message_dict) from Redis.

        Returns:
            (points, data_dicts) tuple
        """
        points = []
        data_dicts = []
        stream_str = stream_name.decode()

        for message_id, msg in messages:
            try:
                # Extract timestamp (if missing, use current time)
                timestamp = msg[b'timestamp'].decode() if b'timestamp' in msg else datetime.utcnow().isoformat()

                # Build a dict of fields from the message
                data = {}
                for key, value in msg.items():
                    key_str = key.decode()
                    if key_str == 'timestamp':
                        data[key_str] = value.decode()
                    else:
                        # Convert numerical fields to float
                        data[key_str] = float(value.decode())

                # Prepare Influx Point
                point = Point(stream_str).tag("source", stream_str).time(datetime.fromisoformat(timestamp))
                for k, v in data.items():
                    if k != 'timestamp':
                        point = point.field(k, v)
                
                points.append(point)
                data_dicts.append(data)
            except Exception as e:
                logger.error(f"Error processing message {message_id} in {stream_str}: {e}")

        return points, data_dicts

    def determine_aws_topic(self, stream_name: str):
        """
        Determine the AWS IoT topic based on the stream name.

        For example:
        - "cellular" -> "cellular/data"
        - "network" -> "network/data"
        - "environmental" -> "environmental/data"
        """
        return f"{stream_name}/data"

    async def process_single_stream(self, stream: str):
        """
        Process one stream: read messages, convert them, write to Influx, publish to AWS.
        """
        response = await self.read_single_stream(stream)
        if not response:
            return
        # response format: [(stream_name_bytes, [(message_id, {fields}), ...])]
        for s_name, messages in response:
            points, data_dicts = await self.create_points_and_dicts(s_name, messages)
            if points:
                # Write all points to InfluxDB
                await self.write_to_influxdb(points)
                
                # Publish each data dictionary to AWS
                stream_str = s_name.decode()
                topic = self.determine_aws_topic(stream_str)
                for data in data_dicts:
                    # Ensure timestamp is present; if not, add current time
                    if 'timestamp' not in data:
                        data['timestamp'] = datetime.now(timezone.utc).astimezone().isoformat()
                    await self.publish_to_aws(topic, data)

    async def run(self):
        await self.async_init()
        while True:
            # Every collection_interval seconds, process all streams in parallel
            await asyncio.sleep(self.collection_interval)
            tasks = [asyncio.create_task(self.process_single_stream(stream)) for stream in self.streams]
            await asyncio.gather(*tasks)