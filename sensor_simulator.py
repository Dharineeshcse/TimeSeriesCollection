import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SensorSimulator:
    """Simulates IoT sensor data generation for temperature and humidity"""
    
    def __init__(self):
        # Default sensor configuration
        self.location = "MCW Porur SEZ"
        self.building = "B9"
        self.room = "ServerRoom"
        self.sensor_id = "SR001"
        self.sensor_type = "environmental"
        
        # Realistic temperature and humidity ranges
        self.temp_safe_min = 65
        self.temp_safe_max = 75
        self.humidity_safe_min = 45
        self.humidity_safe_max = 55
        
        # Out-of-range probabilities for testing alerts
        self.out_of_range_probability = 0.1  # 10% chance
        
    def set_location(self, location, building, room):
        """Set the location details for the sensor"""
        self.location = location
        self.building = building
        self.room = room
    
    def set_sensor_id(self, sensor_id):
        """Set the sensor ID"""
        self.sensor_id = sensor_id
    
    def set_safe_ranges(self, temp_min, temp_max, humidity_min, humidity_max):
        """Set the safe ranges for temperature and humidity"""
        self.temp_safe_min = temp_min
        self.temp_safe_max = temp_max
        self.humidity_safe_min = humidity_min
        self.humidity_safe_max = humidity_max
    
    def set_out_of_range_probability(self, probability):
        """Set the probability of generating out-of-range values for testing"""
        self.out_of_range_probability = max(0.0, min(1.0, probability))
    
    def generate_temperature(self):
        """Generate realistic temperature value"""
        # Most of the time, generate values in safe range
        if random.random() > self.out_of_range_probability:
            temperature = random.uniform(self.temp_safe_min, self.temp_safe_max)
        else:
            # Occasionally generate out-of-range values for testing alerts
            if random.random() < 0.5:
                # Below safe range
                temperature = random.uniform(self.temp_safe_min - 10, self.temp_safe_min - 3)
            else:
                # Above safe range
                temperature = random.uniform(self.temp_safe_max + 3, self.temp_safe_max + 10)
        
        return round(temperature, 2)
    
    def generate_humidity(self):
        """Generate realistic humidity value"""
        # Most of the time, generate values in safe range
        if random.random() > self.out_of_range_probability:
            humidity = random.uniform(self.humidity_safe_min, self.humidity_safe_max)
        else:
            # Occasionally generate out-of-range values for testing alerts
            if random.random() < 0.5:
                # Below safe range
                humidity = random.uniform(self.humidity_safe_min - 10, self.humidity_safe_min - 3)
            else:
                # Above safe range
                humidity = random.uniform(self.humidity_safe_max + 3, self.humidity_safe_max + 10)
        
        return round(humidity, 2)
    
    def generate_sensor_data(self):
        """Generate complete sensor data document"""
        temperature = self.generate_temperature()
        humidity = self.generate_humidity()
        
        return {
            "timestamp": datetime.utcnow(),
            "metadata": {
                "location": self.location,
                "building": self.building,
                "room": self.room,
                "sensor_id": self.sensor_id,
                "sensor_type": self.sensor_type
            },
            "metrics": {
                "temperature": temperature,
                "humidity": humidity
            },
            "alert_type": None,
            "alert_message": None,
            "severity": None
        }
    
    def generate_health_status(self):
        """Generate daily health status document"""
        return {
            "timestamp": datetime.utcnow(),
            "metadata": {
                "location": self.location,
                "building": self.building,
                "room": self.room,
                "type": "health_status"
            },
            "status": "OPTIMAL",
            "message": "Server room environment is running within optimal parameters",
            "alert_type": "HEALTH_STATUS",
            "severity": "INFO"
        }
    
    def get_sensor_info(self):
        """Get current sensor configuration information"""
        return {
            "location": self.location,
            "building": self.building,
            "room": self.room,
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "safe_temperature_range": (self.temp_safe_min, self.temp_safe_max),
            "safe_humidity_range": (self.humidity_safe_min, self.humidity_safe_max),
            "out_of_range_probability": self.out_of_range_probability
        }
