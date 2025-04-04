"""
Task Manager for handling automated tasks and rules.

This module processes data from sensors and executes actions based on configured rules.
"""
import asyncio
import os
import subprocess
from typing import Dict, List, Any, Optional
import logging
from app.utils.validator import Task, TaskAction
from app.system.relay import RelayManager

logger = logging.getLogger("TaskManager")
logger.setLevel(logging.DEBUG)

class TaskManager:
    """
    Manages all automated tasks and rules in the system.
    """
    def __init__(self, tasks: Dict[str, Task], relay_manager: RelayManager):
        """
        Initialize the TaskManager.
        
        Args:
            tasks (Dict[str, Task]): Dictionary of task configurations.
            relay_manager (RelayManager): The relay manager for controlling relays.
        """
        self.tasks = tasks
        self.relay_manager = relay_manager
        
        # Track task/rule states (triggered or not)
        self.task_states: Dict[str, bool] = {task_id: False for task_id in tasks}
        
        # Create a mapping from sources to task IDs for quicker lookup
        self.source_to_tasks: Dict[str, List[str]] = {}
        for task_id, task in tasks.items():
            source = task.source
            if source not in self.source_to_tasks:
                self.source_to_tasks[source] = []
            self.source_to_tasks[source].append(task_id)
        
        self._running = False
    
    async def evaluate_data(self, source: str, data: Dict[str, float]):
        """
        Evaluate a data point against all tasks that use this source.
        
        Args:
            source (str): The source of the data (e.g., "relay_1").
            data (Dict[str, float]): The data point (e.g., {"volts": 12.3, "amps": 0.5}).
        """
        # Skip if no tasks for this source
        if source not in self.source_to_tasks:
            return
        
        task_ids = self.source_to_tasks[source]
        logger.debug(f"Evaluating {len(task_ids)} tasks for source {source}")
        
        for task_id in task_ids:
            task = self.tasks.get(task_id)
            if not task:
                continue
            
            # Skip if the field doesn't exist in the data
            if task.field not in data:
                logger.debug(f"Field '{task.field}' not in data for task '{task.name}' ({task_id})")
                continue
            
            # Evaluate the condition
            condition_met = self._evaluate_condition(data[task.field], task.operator, task.value)
            previously_triggered = self.task_states.get(task_id, False)
            
            logger.debug(f"Task '{task.name}' ({task_id}): condition_met={condition_met}, previously_triggered={previously_triggered}")
            
            # Handle state changes
            if condition_met and not previously_triggered:
                # NOT TRIGGERED -> TRIGGERED (alert_start)
                self.task_states[task_id] = True
                await self._handle_task_triggered(task_id, task, data)
            elif not condition_met and previously_triggered:
                # TRIGGERED -> NOT TRIGGERED (alert_clear)
                self.task_states[task_id] = False
                await self._handle_task_cleared(task_id, task, data)
    
    def _evaluate_condition(self, value: float, operator: str, threshold: float) -> bool:
        """
        Evaluate a condition based on the operator and threshold.
        
        Args:
            value (float): The measured value.
            operator (str): The comparison operator ('>', '<', '>=', '<=', '==', '!=').
            threshold (float): The threshold value to compare against.
            
        Returns:
            bool: True if the condition is met, False otherwise.
        """
        logger.debug(f"Evaluating condition: {value} {operator} {threshold}")
        
        if operator == '>':
            return value > threshold
        elif operator == '<':
            return value < threshold
        elif operator == '>=':
            return value >= threshold
        elif operator == '<=':
            return value <= threshold
        elif operator == '==':
            return value == threshold
        elif operator == '!=':
            return value != threshold
        else:
            logger.error(f"Unknown operator: {operator}")
            return False
    
    async def _handle_task_triggered(self, task_id: str, task: Task, data: Dict[str, float]):
        """
        Handle a task being triggered (transition from not triggered to triggered).
        
        Args:
            task_id (str): The task identifier.
            task (Task): The task configuration.
            data (Dict[str, float]): The data that triggered the task.
        """
        logger.info(f"Task '{task.name}' ({task_id}) triggered")
        
        # Execute all actions for this task
        for action in task.actions:
            await self._execute_action(action, task, data)
    
    async def _handle_task_cleared(self, task_id: str, task: Task, data: Dict[str, float]):
        """
        Handle a task being cleared (transition from triggered to not triggered).
        
        Args:
            task_id (str): The task identifier.
            task (Task): The task configuration.
            data (Dict[str, float]): The data that cleared the task.
        """
        logger.info(f"Task '{task.name}' ({task_id}) cleared")
        # No specific actions for clearing in this implementation
    
    async def _execute_action(self, action: TaskAction, task: Task, data: Dict[str, float]):
        """
        Execute a single action from a task.
        
        Args:
            action (TaskAction): The action to execute.
            task (Task): The parent task.
            data (Dict[str, float]): The data that triggered the task.
        """
        try:
            if action.type == "io":
                await self._execute_io_action(action)
            elif action.type == "log":
                await self._execute_log_action(action, task, data)
            elif action.type == "reboot":
                await self._execute_reboot_action()
            else:
                logger.error(f"Unknown action type: {action.type}")
        except Exception as e:
            logger.error(f"Error executing action: {e}")
    
    async def _execute_io_action(self, action: TaskAction):
        """
        Execute an IO action (relay control).
        
        Args:
            action (TaskAction): The IO action to execute.
        """
        if not action.target or not action.state:
            logger.error("IO action missing target or state")
            return
        
        target = action.target
        state = action.state.lower()
        
        if state == "on":
            await self.relay_manager.set_relay_on(target)
        elif state == "off":
            await self.relay_manager.set_relay_off(target)
        elif state == "pulse":
            relay_config = self.relay_manager.get_relay_by_id(target)
            pulse_time = relay_config.pulse_time if relay_config else 5
            await self.relay_manager.pulse_relay(target, pulse_time)
        else:
            logger.error(f"Unknown IO state: {state}")
    
    async def _execute_log_action(self, action: TaskAction, task: Task, data: Dict[str, float]):
        """
        Execute a log action.
        
        Args:
            action (TaskAction): The log action to execute.
            task (Task): The parent task.
            data (Dict[str, float]): The data that triggered the task.
        """
        message = action.message or f"Alert from task '{task.name}'"
        logger.info(f"Task '{task.name}' triggered log action: {message}")
        logger.info(f"Task data: {data}")
    
    async def _execute_reboot_action(self):
        """
        Execute a reboot action.
        """
        logger.warning("System reboot requested by task action")
        try:
            # Schedule the reboot to happen after a short delay
            asyncio.create_task(self._delayed_reboot())
        except Exception as e:
            logger.error(f"Error scheduling reboot: {e}")
    
    async def _delayed_reboot(self, delay: int = 5):
        """
        Reboot the system after a delay.
        
        Args:
            delay (int): Delay in seconds before rebooting.
        """
        logger.warning(f"System will reboot in {delay} seconds")
        await asyncio.sleep(delay)
        try:
            subprocess.run(["sudo", "reboot"])
        except Exception as e:
            logger.error(f"Failed to reboot system: {e}")
    
    async def run(self):
        """
        Start the task manager.
        
        Note: This method doesn't do anything in this implementation since tasks
        are evaluated when data is received via the evaluate_data method.
        """
        if self._running:
            logger.warning("Task manager already running")
            return
        
        self._running = True
        logger.info(f"Task manager started with {len(self.tasks)} tasks")
        
        # This implementation doesn't have a main loop since tasks are evaluated on-demand
        # when data is received. We just need to keep the run() method alive.
        while self._running:
            await asyncio.sleep(3600)  # Sleep for a long time (1 hour)
    
    async def shutdown(self):
        """
        Shut down the task manager.
        """
        if not self._running:
            return
        
        self._running = False
        logger.info("Shutting down task manager")