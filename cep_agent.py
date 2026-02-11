"""
Complex Event Processing Agent
Handles event stream processing, pattern detection, and real-time analytics
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import deque
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of events the system can process"""
    SENSOR_READING = "sensor_reading"
    ALERT = "alert"
    STATUS_CHANGE = "status_change"
    THRESHOLD_BREACH = "threshold_breach"
    SYSTEM_EVENT = "system_event"


class EventPriority(Enum):
    """Priority levels for events"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class Event:
    """Represents a single event in the system"""
    event_id: str
    event_type: EventType
    timestamp: datetime
    source: str
    data: Dict[str, Any]
    priority: EventPriority = EventPriority.MEDIUM
    processed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "data": self.data,
            "priority": self.priority.value,
            "processed": self.processed
        }


@dataclass
class Pattern:
    """Defines a pattern to detect in event streams"""
    pattern_id: str
    name: str
    event_types: List[EventType]
    time_window: timedelta
    condition: callable
    action: callable
    description: str = ""
    
    def matches(self, events: List[Event]) -> bool:
        """Check if a sequence of events matches this pattern"""
        if len(events) < len(self.event_types):
            return False
        
        # Check if events are within time window
        if events[-1].timestamp - events[0].timestamp > self.time_window:
            return False
        
        # Check if event types match
        event_type_sequence = [e.event_type for e in events]
        if event_type_sequence[-len(self.event_types):] != self.event_types:
            return False
        
        # Check custom condition
        return self.condition(events)


class CEPAgent:
    """
    Complex Event Processing Agent
    Manages event streams, pattern detection, and real-time processing
    """
    
    def __init__(self, buffer_size: int = 1000):
        self.event_buffer: deque = deque(maxlen=buffer_size)
        self.patterns: Dict[str, Pattern] = {}
        self.detected_patterns: List[Dict[str, Any]] = []
        self.statistics: Dict[str, Any] = {
            "total_events": 0,
            "events_by_type": {},
            "events_by_priority": {},
            "patterns_detected": 0
        }
        self.is_running = False
        
    def register_pattern(self, pattern: Pattern):
        """Register a new pattern for detection"""
        self.patterns[pattern.pattern_id] = pattern
        logger.info(f"Registered pattern: {pattern.name}")
    
    def add_event(self, event: Event):
        """Add a new event to the processing buffer"""
        self.event_buffer.append(event)
        self.statistics["total_events"] += 1
        
        # Update statistics
        event_type_key = event.event_type.value
        self.statistics["events_by_type"][event_type_key] = \
            self.statistics["events_by_type"].get(event_type_key, 0) + 1
        
        priority_key = event.priority.name
        self.statistics["events_by_priority"][priority_key] = \
            self.statistics["events_by_priority"].get(priority_key, 0) + 1
        
        logger.info(f"Event added: {event.event_id} - {event.event_type.value}")
        
        # Check for pattern matches
        self._check_patterns()
    
    def _check_patterns(self):
        """Check if any registered patterns match recent events"""
        for pattern in self.patterns.values():
            # Get events within the pattern's time window
            recent_events = self._get_recent_events(pattern.time_window)
            
            if pattern.matches(recent_events):
                self._handle_pattern_match(pattern, recent_events)
    
    def _get_recent_events(self, time_window: timedelta) -> List[Event]:
        """Get events within a specific time window"""
        if not self.event_buffer:
            return []
        
        cutoff_time = datetime.now() - time_window
        return [e for e in self.event_buffer if e.timestamp >= cutoff_time]
    
    def _handle_pattern_match(self, pattern: Pattern, matching_events: List[Event]):
        """Handle a detected pattern match"""
        pattern_detection = {
            "pattern_id": pattern.pattern_id,
            "pattern_name": pattern.name,
            "timestamp": datetime.now().isoformat(),
            "matching_events": [e.event_id for e in matching_events],
            "description": pattern.description
        }
        
        self.detected_patterns.append(pattern_detection)
        self.statistics["patterns_detected"] += 1
        
        logger.info(f"Pattern detected: {pattern.name}")
        
        # Execute pattern action
        try:
            pattern.action(matching_events)
        except Exception as e:
            logger.error(f"Error executing pattern action: {str(e)}")
    
    def get_events(self, 
                   event_type: Optional[EventType] = None,
                   priority: Optional[EventPriority] = None,
                   limit: int = 100) -> List[Event]:
        """Retrieve events with optional filtering"""
        events = list(self.event_buffer)
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if priority:
            events = [e for e in events if e.priority == priority]
        
        return events[-limit:]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current system statistics"""
        return self.statistics.copy()
    
    def get_detected_patterns(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recently detected patterns"""
        return self.detected_patterns[-limit:]
    
    def clear_buffer(self):
        """Clear the event buffer"""
        self.event_buffer.clear()
        logger.info("Event buffer cleared")
    
    async def start_monitoring(self):
        """Start continuous monitoring (can be used for real-time systems)"""
        self.is_running = True
        logger.info("CEP Agent monitoring started")
        
        while self.is_running:
            await asyncio.sleep(1)
            # Periodic checks can be added here
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.is_running = False
        logger.info("CEP Agent monitoring stopped")


# Example pattern definitions
def create_example_patterns() -> List[Pattern]:
    """Create example patterns for demonstration"""
    
    def temperature_spike_condition(events: List[Event]) -> bool:
        """Check if temperature has spiked"""
        if len(events) < 2:
            return False
        values = [e.data.get("temperature", 0) for e in events]
        return max(values) - min(values) > 10
    
    def temperature_spike_action(events: List[Event]):
        """Action when temperature spike is detected"""
        logger.warning(f"Temperature spike detected! Events: {len(events)}")
    
    pattern1 = Pattern(
        pattern_id="temp_spike_01",
        name="Temperature Spike",
        event_types=[EventType.SENSOR_READING, EventType.SENSOR_READING],
        time_window=timedelta(minutes=5),
        condition=temperature_spike_condition,
        action=temperature_spike_action,
        description="Detects rapid temperature changes"
    )
    
    def critical_sequence_condition(events: List[Event]) -> bool:
        """Check for critical event sequence"""
        return any(e.priority == EventPriority.CRITICAL for e in events)
    
    def critical_sequence_action(events: List[Event]):
        """Action for critical sequence"""
        logger.critical(f"Critical sequence detected! Immediate attention required.")
    
    pattern2 = Pattern(
        pattern_id="critical_seq_01",
        name="Critical Event Sequence",
        event_types=[EventType.ALERT, EventType.THRESHOLD_BREACH],
        time_window=timedelta(minutes=2),
        condition=critical_sequence_condition,
        action=critical_sequence_action,
        description="Detects critical alert sequences"
    )
    
    return [pattern1, pattern2]


# Example usage functions
def generate_sample_event(event_num: int) -> Event:
    """Generate a sample event for testing"""
    import random
    
    event_types = list(EventType)
    priorities = list(EventPriority)
    
    return Event(
        event_id=f"EVT_{event_num:04d}",
        event_type=random.choice(event_types),
        timestamp=datetime.now(),
        source=f"sensor_{random.randint(1, 10)}",
        data={
            "temperature": random.uniform(15, 35),
            "humidity": random.uniform(30, 70),
            "status": random.choice(["normal", "warning", "critical"])
        },
        priority=random.choice(priorities)
    )


if __name__ == "__main__":
    # Example usage
    agent = CEPAgent()
    
    # Register example patterns
    for pattern in create_example_patterns():
        agent.register_pattern(pattern)
    
    # Generate and process sample events
    for i in range(10):
        event = generate_sample_event(i)
        agent.add_event(event)
    
    # Display statistics
    print("\nSystem Statistics:")
    print(json.dumps(agent.get_statistics(), indent=2))
    
    print("\nDetected Patterns:")
    print(json.dumps(agent.get_detected_patterns(), indent=2))
