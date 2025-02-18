from datetime import datetime, timezone
import asyncio
import aiosnmp 
from utils.logging_setup import local_logger as logger
from utils.config import settings
from utils.singleton import RedisClient

# This script needs very strong error handling. It shouldnt cause a failure if the router is down/bad
# It also shouldnt fail if there is no SIM card or No ACTIVE SIM card.
# It should just pass and try again.
# Maybe using -9999 as a null value is not a good idea. 
# We do not want to store bad/null values as null or None or it will cause errors, it also should be an int or float
# Maybe we use 0 as a null value. 

class CellularData:
    def __init__(self):
        self.host = settings.SNMP_TARGET
        self.null = settings.NULL
        self.collection_interval = settings.COLLECTION_INTERVAL
        self.oid_mappings = settings.OIDS
        
        
    async def async_init(self):
        self.redis = await RedisClient.get_instance()
    
    # This function needs better error handling. Basically if it fails here at any point instead of breaking or shutting down it should just pass and try again.
    # This may cause infinate failures but it will ensure that no router related issues will cause failures and that as soon as data is avaliable it will start to run 
    async def fetch_snmp_data(self, host, community, oid_mappings):
        """Fetch SNMP data asynchronously using aiosnmp"""
        oids = [oid for oid in oid_mappings.values()]
        results = {}
        async with aiosnmp.Snmp(host=host, community=community, port=161, timeout=5, retries=3, max_repetitions=10) as snmp:
            response = await snmp.get(oids)
            for varbind in response:
                for key, oid in oid_mappings.items():
                    if varbind.oid == oid:
                        results[key] = varbind.value
        return results
    
    async def process_data(self):
        try:
            data = await self.fetch_snmp_data(self.host, 'public', self.oid_mappings)
            if data:
                sinr = await self.ensure_float(data.get('sinr_oid'))
                rsrp = await self.ensure_float(data.get('rsrp_oid'))
                rsrq = await self.ensure_float(data.get('rsrq_oid'))
                await self.stream_data(sinr, rsrp, rsrq)
            else:
                logger.warning("No data returned from SNMP request.")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
        
    async def stream_data(self, sinr, rsrp, rsrq):
        timestamp = datetime.now(timezone.utc).astimezone().isoformat()
        data = {
            "timestamp": timestamp,
            "sinr": sinr,
            "rsrp": rsrp,
            "rsrq": rsrq
        }
        await self.redis.xadd('cellular', data)
        
        
    async def ensure_float(self, value):
        if value is None:
            logger.debug("Warning: Received None value, using null placeholder.")
            return self.null
        try:
            return float(value)
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting value to float: {e}")
            return self.null
        
    async def run(self):
        await self.async_init()
        while True:
            await self.process_data()
            await asyncio.sleep(self.collection_interval)
        
    
