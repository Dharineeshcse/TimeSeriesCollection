import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class AlertManager:
    """Manages threshold monitoring and alert generation for sensor data"""
    
    def __init__(self):
        # Default thresholds for server room environment
        self.temp_min = 63  # °F
        self.temp_max = 80  # °F
        self.humidity_min = 40  # %
        self.humidity_max = 60  # %
        
        # Alert severity levels
        self.severity_levels = {
            "WARNING": "WARNING",
            "CRITICAL": "CRITICAL",
            "INFO": "INFO"
        }
        
        # Alert type definitions
        self.alert_types = {
            "TEMPERATURE_LOW": "Temperature below minimum threshold",
            "TEMPERATURE_HIGH": "Temperature above maximum threshold",
            "HUMIDITY_LOW": "Humidity below minimum threshold",
            "HUMIDITY_HIGH": "Humidity above maximum threshold",
            "HEALTH_STATUS": "System health status"
        }
    
    def set_thresholds(self, temp_min, temp_max, humidity_min, humidity_max):
        """Set custom thresholds for monitoring"""
        self.temp_min = temp_min
        self.temp_max = temp_max
        self.humidity_min = humidity_min
        self.humidity_max = humidity_max
        
        logger.info(f"✅ Thresholds updated - Temp: {temp_min}°F to {temp_max}°F, Humidity: {humidity_min}% to {humidity_max}%")
    
    def get_thresholds(self):
        """Get current threshold values"""
        return {
            "temperature": (self.temp_min, self.temp_max),
            "humidity": (self.humidity_min, self.humidity_max)
        }
    
    def check_temperature_thresholds(self, temperature):
        """Check if temperature exceeds thresholds and generate alerts"""
        alerts = []
        
        if temperature < self.temp_min:
            alerts.append({
                "type": "TEMPERATURE_LOW",
                "message": f"Temperature {temperature}°F is below minimum threshold {self.temp_min}°F",
                "severity": self._determine_severity(temperature, self.temp_min, self.temp_min - 5)
            })
        elif temperature > self.temp_max:
            alerts.append({
                "type": "TEMPERATURE_HIGH",
                "message": f"Temperature {temperature}°F is above maximum threshold {self.temp_max}°F",
                "severity": self._determine_severity(temperature, self.temp_max, self.temp_max + 5)
            })
        
        return alerts
    
    def check_humidity_thresholds(self, humidity):
        """Check if humidity exceeds thresholds and generate alerts"""
        alerts = []
        
        if humidity < self.humidity_min:
            alerts.append({
                "type": "HUMIDITY_LOW",
                "message": f"Humidity {humidity}% is below minimum threshold {self.humidity_min}%",
                "severity": self._determine_severity(humidity, self.humidity_min, self.humidity_min - 5)
            })
            
        elif humidity > self.humidity_max:
            alerts.append({
                "type": "HUMIDITY_HIGH",
                "message": f"Humidity {humidity}% is above maximum threshold {self.humidity_max}%",
                "severity": self._determine_severity(humidity, self.humidity_max, self.humidity_max + 5)
            })
        
        return alerts
    
    def _determine_severity(self, value, threshold, critical_threshold):
        """Determine alert severity based on how far the value is from threshold"""
        if abs(value - threshold) <= 2:
            return self.severity_levels["WARNING"]
        else:
            return self.severity_levels["CRITICAL"]
    
    def check_all_thresholds(self, sensor_data):
        """Check all thresholds for a given sensor data document"""
        alerts = []
        
        if "metrics" in sensor_data:
            temperature = sensor_data["metrics"].get("temperature")
            humidity = sensor_data["metrics"].get("humidity")
            
            if temperature is not None:
                temp_alerts = self.check_temperature_thresholds(temperature)
                alerts.extend(temp_alerts)
            
            if humidity is not None:
                humidity_alerts = self.check_humidity_thresholds(humidity)
                alerts.extend(humidity_alerts)
        
        return alerts
    
    def apply_alerts_to_data(self, sensor_data, alerts):
        """Apply generated alerts to sensor data document"""
        if not alerts:
            return sensor_data
        
        # Add alert information to the document
        sensor_data["alert_type"] = [alert["type"] for alert in alerts]
        sensor_data["alert_message"] = [alert["message"] for alert in alerts]
        sensor_data["severity"] = [alert["severity"] for alert in alerts]
        
        return sensor_data
    
    def log_alerts(self, alerts):
        """Log generated alerts with appropriate severity levels"""
        for alert in alerts:
            if alert["severity"] == self.severity_levels["CRITICAL"]:
                logger.critical(f"🚨 CRITICAL ALERT: {alert['message']}")
            elif alert["severity"] == self.severity_levels["WARNING"]:
                logger.warning(f"⚠️ WARNING: {alert['message']}")
            else:
                logger.info(f"ℹ️ INFO: {alert['message']}")
    
    def get_alert_summary(self, alerts):
        """Get a summary of generated alerts"""
        if not alerts:
            return "No alerts generated"
        
        summary = f"Generated {len(alerts)} alerts:\n"
        for alert in alerts:
            summary += f"  - {alert['severity']}: {alert['type']} - {alert['message']}\n"
        
        return summary
    
    def validate_thresholds(self):
        """Validate that thresholds are set to reasonable values"""
        warnings = []
        
        if self.temp_min < 50 or self.temp_max > 100:
            warnings.append("Temperature thresholds are outside recommended range (50°F - 100°F)")
        
        if self.humidity_min < 20 or self.humidity_max > 80:
            warnings.append("Humidity thresholds are outside recommended range (20% - 80%)")
        
        if self.temp_min >= self.temp_max:
            warnings.append("Temperature minimum must be less than maximum")
        
        if self.humidity_min >= self.humidity_max:
            warnings.append("Humidity minimum must be less than maximum")
        
        return warnings
