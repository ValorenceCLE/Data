import asyncio
import logging
from typing import Optional
import smbus2

class INA260Sensor:
    _instances = {}

    def __new__(cls, address: int, bus_num: int = 1, *args, **kwargs):
        if address in cls._instances:
            return cls._instances[address]
        instance = super().__new__(cls)
        cls._instances[address] = instance
        return instance

    def __init__(self, address: int, bus_num: int = 1):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.address = address
        self.bus = smbus2.SMBus(bus_num)  # Bus number is parameterized.
        self._bus_lock = asyncio.Lock()    # Ensure asynchronous bus access.
        self._initialized = True

    async def read_word(self, reg: int) -> int:
        """
        Asynchronously read a word from a given register,
        swapping bytes to account for little-endian format.
        """
        try:
            async with self._bus_lock:
                raw = await asyncio.to_thread(self.bus.read_word_data, self.address, reg)
            return ((raw & 0xFF) << 8) | ((raw >> 8) & 0xFF)
        except Exception as e:
            logging.error(f"Error reading register {hex(reg)} from INA260 sensor at address {hex(self.address)}: {e}")
            raise

    async def read_voltage(self) -> Optional[float]:
        """
        Asynchronously reads and converts the bus voltage in volts.
        """
        try:
            raw_voltage = await self.read_word(0x02)  # Voltage register.
            return round(raw_voltage * 0.00125, 3)  # LSB = 1.25 mV.
        except Exception as e:
            logging.error(f"Error reading voltage from INA260 sensor at address {hex(self.address)}: {e}")
            return None

    async def read_current(self) -> Optional[float]:
        """
        Asynchronously reads and converts the current in amps.
        """
        try:
            raw_current = await self.read_word(0x01)  # Current register.
            if raw_current >= 0x8000:  # Handle two's complement.
                raw_current -= 0x10000
            return round(raw_current * 0.00125, 3)  # LSB = 1.25 mA.
        except Exception as e:
            logging.error(f"Error reading current from INA260 sensor at address {hex(self.address)}: {e}")
            return None

    async def read_power(self) -> Optional[float]:
        """
        Asynchronously reads and converts the power in watts.
        """
        try:
            raw_power = await self.read_word(0x03)  # Power register.
            return round(raw_power * 0.01, 3)  # LSB = 10 mW.
        except Exception as e:
            logging.error(f"Error reading power from INA260 sensor at address {hex(self.address)}: {e}")
            return None
    
    async def read_all(self) -> Optional[dict]:
        """
        Asynchronously reads all sensor values (voltage, current, power).
        Returns a dictionary with the values or None if an error occurs.
        """
        try:
            voltage = await self.read_voltage()
            current = await self.read_current()
            power = await self.read_power()
            return {
                "voltage": voltage,
                "current": current,
                "power": power
            }
        except Exception as e:
            logging.error(f"Error reading all data from INA260 sensor at address {hex(self.address)}: {e}")
            return None


class SHT30Sensor:
    """
    A singleton class for the SHT30 sensor. It handles I²C initialization,
    resetting the sensor, and reading temperature and humidity.
    """
    _instance = None

    def __new__(cls, bus_num: int = 1, address: int = 0x45):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, bus_num: int = 1, address: int = 0x45):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.address = address
        self.bus = smbus2.SMBus(bus_num)  # Bus number is parameterized.
        self._bus_lock = asyncio.Lock()    # Ensure asynchronous bus access.
        self._initialized = True
        self._cached_data = None  # Cache sensor data.
        self._cache_timestamp = 0

    async def reset(self):
        """
        Asynchronously resets the SHT30 sensor.
        """
        try:
            async with self._bus_lock:
                await asyncio.to_thread(self.bus.write_i2c_block_data, self.address, 0x30, [0xA2])
            await asyncio.sleep(0.01)  # Allow sensor to reset.
        except Exception as e:
            logging.error(f"Error resetting SHT30 sensor: {e}")
            raise

    async def _get_data(self):
        """
        Asynchronously retrieves and caches sensor data for a short interval
        to avoid redundant measurements.
        """
        current_time = asyncio.get_event_loop().time()
        if self._cached_data and (current_time - self._cache_timestamp) < 0.1:
            return self._cached_data

        try:
            async with self._bus_lock:
                await asyncio.to_thread(self.bus.write_i2c_block_data, self.address, 0x24, [0x00])
            await asyncio.sleep(0.05)  # Wait for measurement to complete.
            async with self._bus_lock:
                data = await asyncio.to_thread(self.bus.read_i2c_block_data, self.address, 0x00, 6)
            if len(data) != 6:
                raise ValueError("Invalid data length received from SHT30 sensor.")
            self._cached_data = data
            self._cache_timestamp = current_time
            return data
        except Exception as e:
            logging.error(f"Error reading data from SHT30 sensor: {e}")
            raise

    async def read_temperature(self) -> Optional[float]:
        """
        Asynchronously reads the temperature in Fahrenheit.
        """
        try:
            data = await self._get_data()
            temp_raw = (data[0] << 8) | data[1]
            temperature_f = round((-45 + 175 * (temp_raw / 65535.0)) * 9 / 5 + 32, 1)
            return temperature_f
        except Exception as e:
            logging.error(f"Error reading temperature from SHT30 sensor: {e}")
            return None

    async def read_humidity(self) -> Optional[float]:
        """
        Asynchronously reads the humidity percentage.
        """
        try:
            data = await self._get_data()
            humidity_raw = (data[3] << 8) | data[4]
            humidity = round(100 * (humidity_raw / 65535.0), 1)
            return humidity
        except Exception as e:
            logging.error(f"Error reading humidity from SHT30 sensor: {e}")
            return None
    
    async def read_all(self) -> Optional[dict]:
        """
        Asynchronously reads all sensor values (temperature, humidity).
        Returns a dictionary with the values or None if an error occurs.
        """
        try:
            temperature = await self.read_temperature()
            humidity = await self.read_humidity()
            return {
                "temperature": temperature,
                "humidity": humidity
            }
        except Exception as e:
            logging.error(f"Error reading all data from SHT30 sensor: {e}")
            return None
