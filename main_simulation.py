import logging
import time
from datetime import datetime

# Import our modular components
from database_manager import DatabaseManager
from time_series_collection import TimeSeriesCollection
from sensor_simulator import SensorSimulator
from alert_manager import AlertManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iot_simulation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IoTSimulationOrchestrator:
    """Orchestrates the IoT time-series simulation using modular components"""
    
    def __init__(self, connection_string, db_name, collection_name):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        
        # Initialize components
        self.db_manager = DatabaseManager(connection_string, db_name)
        self.ts_collection = TimeSeriesCollection(self.db_manager, collection_name)
        self.sensor_simulator = SensorSimulator()
        self.alert_manager = AlertManager()
        
        # Health check intervals
        self.health_check_interval = 24 * 60 * 60  # 24 hours in seconds
        self.last_health_check = None
        
        # Data collection interval
        self.data_collection_interval = 60  # 60 seconds = 1 minute
        
    def initialize_system(self):
        """Initialize all system components"""
        try:
            logger.info("Initializing IoT Time-Series Simulation System...")
            
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to MongoDB. Exiting...")
                return False
            
            # Setup time-series collection
            self.ts_collection.setup()
            
            # Perform initial health check using standard collection
            if not self.db_manager.check_health():
                logger.error("Database health check failed. Exiting...")
                return False
            
            # Configure sensor simulator with server room settings
            self.sensor_simulator.set_location("MCW", "CBE", "ServerRoom")
            self.sensor_simulator.set_sensor_id("SR001")
            
            # Configure alert manager with server room thresholds
            self.alert_manager.set_thresholds(63, 80, 40, 60)
            
            # Validate thresholds
            warnings = self.alert_manager.validate_thresholds()
            if warnings:
                for warning in warnings:
                    logger.warning(f"Threshold Warning: {warning}")
            
            logger.info("System initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            return False
    
    def run_simulation_cycle(self):
        """Run one complete simulation cycle"""
        try:
            # Generate sensor data
            sensor_data = self.sensor_simulator.generate_sensor_data()
            
            # Check thresholds and generate alerts
            alerts = self.alert_manager.check_all_thresholds(sensor_data)
            
            # Apply alerts to data
            if alerts:
                sensor_data = self.alert_manager.apply_alerts_to_data(sensor_data, alerts)
                self.alert_manager.log_alerts(alerts)
            
            # Insert data into time-series collection
            doc_id, success = self.ts_collection.insert_document(sensor_data)
            
            if success:
                # Verify insertion
                if self.ts_collection.verify_insertion(doc_id):
                    # Log the data
                    temp = sensor_data["metrics"]["temperature"]
                    humidity = sensor_data["metrics"]["humidity"]
                    
                    if alerts:
                        logger.info(f"[{sensor_data['timestamp']}] Temp: {temp}°F, Humidity: {humidity}% - ALERTS: {len(alerts)}")
                    else:
                        logger.info(f"[{sensor_data['timestamp']}] Temp: {temp}°F, Humidity: {humidity}%")
                else:
                    logger.error("Data insertion verification failed")
            else:
                logger.error("Failed to insert sensor data")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in simulation cycle: {e}")
            return False
    
    def run_health_maintenance(self):
        """Run health maintenance tasks"""
        try:
            current_time = datetime.utcnow()
            
            # Check if it's time for health maintenance
            if (self.last_health_check is None or 
                (current_time - self.last_health_check).total_seconds() >= self.health_check_interval):
                
                logger.info("Running health maintenance tasks...")
                
                # Log health status
                health_status = self.sensor_simulator.generate_health_status()
                self.ts_collection.insert_document(health_status)
                
                # Clean up old data
                cleaned_count = self.ts_collection.cleanup_old_data()
                logger.info(f"Data cleanup completed: {cleaned_count} old documents removed")
                
                # Update last health check time
                self.last_health_check = current_time
                
                logger.info("Health maintenance completed successfully")
                
        except Exception as e:
            logger.error(f"Error in health maintenance: {e}")
    
    def run_simulation(self):
        """Run the main simulation loop"""
        logger.info("Starting IoT Time-Series Simulation...")
        logger.info("Press Ctrl+C to stop.")
        
        try:
            while True:
                # Run simulation cycle
                self.run_simulation_cycle()
                
                # Run health maintenance if needed
                self.run_health_maintenance()
                
                # Wait for next cycle
                time.sleep(self.data_collection_interval)
                
        except KeyboardInterrupt:
            logger.info("IoT Simulation stopped by user.")
        except Exception as e:
            logger.error(f"Unexpected error in simulation: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        try:
            logger.info("Cleaning up resources...")
            self.db_manager.close()
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_system_status(self):
        """Get current system status"""
        return {
            "database_connected": self.db_manager.is_connected(),
            "collection_ready": self.ts_collection.get_collection() is not None,
            "sensor_config": self.sensor_simulator.get_sensor_info(),
            "alert_thresholds": self.alert_manager.get_thresholds(),
            "last_health_check": self.last_health_check,
            "data_collection_interval": self.data_collection_interval
        }

def main():
    """Main function to run the IoT simulation"""
    # Configuration
    connection_string = "mongodb://test:testDev%4012C@localhost:27017/"
    db_name = "monitoringDB"
    collection_name = "serverRoomLogs"
    
    # Create and run simulation
    orchestrator = IoTSimulationOrchestrator(connection_string, db_name, collection_name)
    
    # Initialize system
    if not orchestrator.initialize_system():
        logger.error("System initialization failed. Exiting...")
        return
    
    # Display system status
    status = orchestrator.get_system_status()
    logger.info("System Status:")
    logger.info(f"   Database Connected: {status['database_connected']}")
    logger.info(f"   Collection Ready: {status['collection_ready']}")
    logger.info(f"   Sensor Location: {status['sensor_config']['location']}, {status['sensor_config']['building']}, {status['sensor_config']['room']}")
    logger.info(f"   Temperature Thresholds: {status['alert_thresholds']['temperature']}°F")
    logger.info(f"   Humidity Thresholds: {status['alert_thresholds']['humidity']}%")
    logger.info(f"   Data Collection Interval: {status['data_collection_interval']} seconds")
    
    # Run simulation
    orchestrator.run_simulation()

if __name__ == "__main__":
    main()
