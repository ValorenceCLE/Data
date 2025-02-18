from typing import Dict, Any
from utils.logging_setup import local_logger as logger
from utils.config import settings
from core.relay_manager import RelayManager
from aws.client import publish as aws_publish

class RulesEngine:
    """
    The RulesEngine class evaluates user-defined rules against incoming data points
    and triggers actions if conditions are met. Actions may include logging, 
    publishing events to AWS IoT, or controlling relays via the RelayManager.

    The engine tracks the state of each rule (triggered or not) to avoid spamming 
    repeated actions. When a rule first becomes triggered (alert_start) or returns 
    to normal (alert_clear), corresponding actions and notifications are performed.
    """

    def __init__(self, relay_id: str, rules: Dict[str, Any], relay_manager: RelayManager):
        """
        Initialize the RulesEngine with a given relay ID and a dictionary of rules.

        Args:
            relay_id (str): The identifier for the relay these rules apply to.
            rules (Dict[str, Any]): A dictionary of rules, keyed by rule_id.
                                    Each rule should be a Rule object containing 'field', 'condition', 'value', and 'actions'.
            relay_manager (RelayManager): The RelayManager instance for controlling relay states.
        """
        self.relay_id = relay_id
        self.rules = rules
        self.relay_manager = relay_manager
        self.publish = aws_publish

        # Initialize rule states to track if they've been triggered
        self.rule_states = {rule_id: False for rule_id in self.rules.keys()}

    async def evaluate_rules(self, data: Dict[str, float]):
        """
        Evaluate all rules against the provided data point.

        For each rule, if the condition changes from not triggered to triggered,
        handle the alert_start event. If it changes from triggered to not triggered,
        handle the alert_clear event.

        Args:
            data (Dict[str, float]): A dictionary with sensor readings (e.g., {"relay": "relay1", "volts": 2.43, "watts": 0.29, "amps": 0.12})
        """
        logger.debug(f"Evaluating rules for relay {self.relay_id} with data: {data}")
        for rule_id, rule in self.rules.items():
            condition_met = self._evaluate_condition(data, rule.field, rule.condition, rule.value)
            previously_triggered = self.rule_states[rule_id]

            logger.debug(f"Rule {rule_id}: condition_met={condition_met}, previously_triggered={previously_triggered}")

            if condition_met and not previously_triggered:
                # NOT TRIGGERED -> TRIGGERED (alert_start)
                self.rule_states[rule_id] = True
                await self._handle_alert_start(rule_id, rule, data)
            elif not condition_met and previously_triggered:
                # TRIGGERED -> NOT TRIGGERED (alert_clear)
                self.rule_states[rule_id] = False
                await self._handle_alert_clear(rule_id, rule, data)

    def _evaluate_condition(self, data: Dict[str, float], field: str, condition: str, value: float) -> bool:
        """
        Check if a rule's condition is met given the current data.

        Args:
            data (Dict[str, float]): Current sensor data.
            field (str): The field to check (e.g., 'volts').
            condition (str): The comparison operator (e.g., '<', '<=', '>', '>=', '==', '!=').
            value (float): The threshold to compare against.

        Returns:
            bool: True if the condition is met, False otherwise.
        """
        if field not in data:
            logger.error(f"Field '{field}' not found in data: {data}")
            return False

        data_value = data[field]

        logger.debug(f"Evaluating condition: {data_value} {condition} {value}")

        if condition == '<':
            return data_value < value
        elif condition == '<=':
            return data_value <= value
        elif condition == '>':
            return data_value > value
        elif condition == '>=':
            return data_value >= value
        elif condition == '==':
            return data_value == value
        elif condition == '!=':
            return data_value != value
        else:
            logger.error(f"Unknown condition '{condition}'.")
            return False

    async def _handle_alert_start(self, rule_id: str, rule: Any, data: Dict[str, float]):
        """
        Handle the transition from not triggered to triggered (alert_start).
        Run all actions and notify AWS of the alert start.

        Args:
            rule_id (str): Identifier of the triggered rule.
            rule (Rule): The rule configuration.
            data (Dict[str, float]): Current sensor data.
        """
        logger.debug(f"Alert START for rule {rule_id} on relay {self.relay_id}. Condition met.")
        for action in rule.actions:
            await self._execute_action(action, data, alert_state='start')
        await self._send_aws_alert(rule_id, data, alert_type='start')

    async def _handle_alert_clear(self, rule_id: str, rule: Any, data: Dict[str, float]):
        """
        Handle the transition from triggered to not triggered (alert_clear).
        Run actions if defined (if you want symmetric actions) and notify AWS of the alert clear.

        Args:
            rule_id (str): Identifier of the cleared rule.
            rule (Rule): The rule configuration.
            data (Dict[str, float]): Current sensor data.
        """
        logger.debug(f"Alert CLEAR for rule {rule_id} on relay {self.relay_id}. Condition not met.")
        # If you want symmetrical actions on clear, you can iterate and execute them here.
        # Currently, only AWS alert_clear is sent.
        await self._send_aws_alert(rule_id, data, alert_type='clear')

    async def _execute_action(self, action: Any, data: Dict[str, float], alert_state: str):
        """
        Execute a single action from the rule's action list.

        Args:
            action (Action): The action configuration.
            data (Dict[str, float]): Current sensor data.
            alert_state (str): 'start' or 'clear', indicating the alert phase.
        """
        action_type = action.type
        logger.debug(f"Executing action: {action_type} with data: {data} and alert_state: {alert_state}")

        if action_type == 'log':
            message = action.message or 'No message provided'
            logger.info(f"Rule action (log): {message}")
        elif action_type == 'relay_on':
            await self.relay_manager.set_relay_on(self.relay_id)
        elif action_type == 'relay_off':
            await self.relay_manager.set_relay_off(self.relay_id)
        elif action_type == 'pulse_relay':
            duration = action.duration or 1.0
            await self.relay_manager.pulse_relay(self.relay_id, duration)
        elif action_type == 'aws':
            message = action.message or 'Alert triggered'
            payload = {
                "relay_id": self.relay_id,
                "alert_state": alert_state,
                "message": message,
                "data": data
            }
            await self.publish("alerts/data", payload)
            logger.debug(f"Published AWS alert: {payload}")
        else:
            logger.error(f"Unknown action type: {action_type}")

    async def _send_aws_alert(self, rule_id: str, data: Dict[str, float], alert_type: str):
        """
        Send a standardized alert message to AWS IoT, indicating the start or clear of an alert.

        Args:
            rule_id (str): The rule identifier.
            data (Dict[str, float]): Current sensor data.
            alert_type (str): 'start' or 'clear'.
        """
        payload = {
            "relay_id": self.relay_id,
            "rule_id": rule_id,
            "alert_type": alert_type,
            "data": data
        }
        await self.publish('alerts/data', payload)
        logger.debug(f"Sent {alert_type.upper()} alert event to AWS for rule {rule_id} on relay {self.relay_id}.")
