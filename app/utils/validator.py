"""
Configuration validator for the DPM system.

This module defines Pydantic models for validating the configuration file
and provides utility functions for working with the configuration.
"""
import json
import os
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, field_validator, model_validator, Field
from datetime import datetime
import ipaddress
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ConfigValidator")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Day bit values for schedule configuration
DAY_VALUES = {
    "Sunday": 2,
    "Monday": 4,
    "Tuesday": 8,
    "Wednesday": 16,
    "Thursday": 32,
    "Friday": 64,
    "Saturday": 128
}
# Max value for days_mask (all days combined)
MAX_DAYS_MASK = sum(DAY_VALUES.values())

class NetworkConfig(BaseModel):
    """
    Network configuration model.
    """
    ip_address: str
    subnet_mask: str
    gateway: str
    dhcp: bool
    primary_dns: str
    secondary_dns: Optional[str] = None

    @field_validator('ip_address', 'gateway', 'primary_dns', 'secondary_dns')
    def validate_ip(cls, v, info):
        if v is None and info.field_name == 'secondary_dns':
            return v
        try:
            ipaddress.ip_address(v)
            return v
        except ValueError:
            raise ValueError(f"Invalid IP address format: {v}")

    @field_validator('subnet_mask')
    def validate_subnet(cls, v):
        try:
            ipaddress.IPv4Network(f"0.0.0.0/{v}", strict=False)
            return v
        except ValueError:
            raise ValueError(f"Invalid subnet mask: {v}")

class DateTimeConfig(BaseModel):
    """
    Date and time configuration model.
    """
    primary_ntp: str
    secondary_ntp: Optional[str] = None
    synchronize: bool = True
    timezone: str
    utc_offset: int

    @field_validator('utc_offset')
    def validate_utc_offset(cls, v):
        if not -12 <= v <= 14:
            raise ValueError(f"UTC offset must be between -12 and 14, got {v}")
        return v

class RelaySchedule(BaseModel):
    """
    Relay schedule configuration model.
    """
    enabled: bool = False
    on_time: Optional[str] = None
    off_time: Optional[str] = None
    days_mask: int = 0  # Bitmask for days using custom bit values

    @field_validator('on_time', 'off_time')
    def validate_time(cls, v, info):
        if v is None:
            if info.field_name in ('on_time', 'off_time'):
                return v
        try:
            if v is not None:
                datetime.strptime(v, "%H:%M")
            return v
        except (ValueError, TypeError):
            raise ValueError(f"Time format must be HH:MM, got {v}")

    @field_validator('days_mask')
    def validate_days_mask(cls, v):
        if not 0 <= v <= MAX_DAYS_MASK:  # Updated max value for custom bit values
            raise ValueError(f"days_mask must be between 0 and {MAX_DAYS_MASK}, got {v}")
        return v

class ButtonConfig(BaseModel):
    """
    Button configuration model for the dashboard.
    """
    show: bool = True
    status_text: str
    status_color: str
    button_label: str

    @field_validator('status_color')
    def validate_color(cls, v):
        valid_colors = ["red", "green", "yellow", "blue", "gray"]
        if v.lower() not in valid_colors:
            logger.warning(f"Non-standard color {v} used for button status")
        return v

class DashboardConfig(BaseModel):
    """
    Dashboard configuration model.
    """
    on_button: ButtonConfig
    off_button: ButtonConfig
    pulse_button: ButtonConfig

class RelayConfig(BaseModel):
    """
    Relay configuration model.
    """
    id: str
    name: str
    enabled: bool = True
    pulse_time: int = 5  # Default 5 seconds
    schedule: Union[RelaySchedule, bool] = Field(default_factory=lambda: RelaySchedule())  # Allow bool or RelaySchedule
    dashboard: DashboardConfig

    @field_validator('schedule')
    def validate_schedule(cls, v):
        # If schedule is set to False, return a default disabled schedule
        if v is False:
            return RelaySchedule(enabled=False)
        return v

    @field_validator('pulse_time')
    def validate_pulse_time(cls, v):
        if v <= 0:
            raise ValueError(f"Pulse time must be positive, got {v}")
        return v

class TaskAction(BaseModel):
    """
    Task action configuration model.
    """
    type: str
    target: Optional[str] = None
    state: Optional[str] = None
    message: Optional[str] = None

    @field_validator('type')
    def validate_action_type(cls, v):
        valid_types = ["io", "log", "reboot"]
        if v not in valid_types:
            raise ValueError(f"Invalid action type: {v}. Valid types: {valid_types}")
        return v

    @model_validator(mode='after')
    def validate_action_fields(self):
        if self.type == "io" and (self.target is None or self.state is None):
            raise ValueError("IO actions require 'target' and 'state' fields")
        if self.type == "log" and self.message is None:
            raise ValueError("Log actions require a 'message' field")
        return self

class Task(BaseModel):
    """
    Task configuration model.
    """
    name: str
    source: str
    field: str
    operator: str
    value: Union[int, float]
    actions: List[TaskAction]

    @field_validator('field')
    def validate_field(cls, v):
        valid_fields = ["volts", "amps", "watts", "temperature", "humidity", "sinr", "rsrp", "rsrq"]
        if v not in valid_fields:
            raise ValueError(f"Invalid field: {v}. Valid fields: {valid_fields}")
        return v

    @field_validator('operator')
    def validate_operator(cls, v):
        valid_operators = [">", "<", ">=", "<=", "==", "!="]
        if v not in valid_operators:
            raise ValueError(f"Invalid operator: {v}. Valid operators: {valid_operators}")
        return v

class GeneralConfig(BaseModel):
    """
    General system configuration model.
    """
    system_name: str
    system_id: str
    version: str
    agency: str
    product: str
    reboot_time: str

    @field_validator('reboot_time')
    def validate_reboot_time(cls, v):
        try:
            datetime.strptime(v, "%H:%M")
            return v
        except ValueError:
            raise ValueError(f"Reboot time format must be HH:MM, got {v}")

class Config(BaseModel):
    """
    Main configuration model.
    """
    general: GeneralConfig
    network: NetworkConfig
    date_time: DateTimeConfig
    relays: List[RelayConfig]
    tasks: Dict[str, Task]

def load_config(config_path: str = "config.json") -> Config:
    """
    Load and validate the configuration from the specified path.
    
    Args:
        config_path (str): Path to the configuration file.
        
    Returns:
        Config: The validated configuration.
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist.
        ValueError: If the configuration is invalid.
    """
    try:
        # Check if config file exists
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load config file
        with open(config_path, 'r') as file:
            config_data = json.load(file)

        # Create Config instance, which will validate the configuration
        config = Config(**config_data)
        logger.info("Configuration loaded and validated successfully")
        return config

    except FileNotFoundError as e:
        logger.error(f"Configuration file error: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Configuration validation error: {str(e)}")
        raise

def validate_config(config_path: str = "config.json") -> Config:
    """
    Validate the configuration.
    
    Args:
        config_path (str): Path to the configuration file.
        
    Returns:
        Config: The validated configuration.
    """
    return load_config(config_path)

def save_config(config: Config, config_path: str = "config.json") -> bool:
    """
    Save the configuration to a file.
    
    Args:
        config (Config): The configuration to save.
        config_path (str): Path to save the configuration file.
        
    Returns:
        bool: True if the configuration was saved successfully, False otherwise.
    """
    try:
        # Convert to dictionary
        config_dict = config.model_dump(mode='json')
        
        # Save to file
        with open(config_path, 'w') as file:
            json.dump(config_dict, file, indent=2)
        
        logger.info(f"Configuration saved to {config_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration: {e}")
        return False

def days_mask_to_names(days_mask: int) -> List[str]:
    """
    Convert a days bitmask to a list of day names.
    
    Args:
        days_mask (int): The days bitmask using the custom bit values.
        
    Returns:
        List[str]: List of day names.
    """
    result = []
    
    for day, bit_value in DAY_VALUES.items():
        if (days_mask & bit_value) != 0:
            result.append(day)
            
    return result

def day_names_to_mask(day_names: List[str]) -> int:
    """
    Convert a list of day names to a days bitmask.
    
    Args:
        day_names (List[str]): List of day names.
        
    Returns:
        int: The days bitmask using the custom bit values.
    """
    days_mask = 0
    for day in day_names:
        day_title = day.title()  # Convert to title case for matching
        if day_title in DAY_VALUES:
            days_mask |= DAY_VALUES[day_title]
    
    return days_mask