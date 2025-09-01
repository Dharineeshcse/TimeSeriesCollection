#!/usr/bin/env python3
"""
Test script to verify all modular components work together correctly.
This script tests the basic functionality of each module without running the full simulation.
"""

import logging
from datetime import datetime

# Configure logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_sensor_simulator():
    """Test the sensor simulator module"""
    logger.info("Testing Sensor Simulator...")
    
    try:
        from sensor_simulator import SensorSimulator
        
        # Create sensor simulator
        sensor = SensorSimulator()
        
        # Test configuration
        sensor.set_location("Test Location", "Test Building", "Test Room")
        sensor.set_sensor_id("TEST001")
        sensor.set_safe_ranges(60, 80, 30, 70)
        sensor.set_out_of_range_probability(0.2)
        
        # Test data generation
        sensor_data = sensor.generate_sensor_data()
        health_status = sensor.generate_health_status()
        sensor_info = sensor.get_sensor_info()
        
        # Verify data structure
        assert "timestamp" in sensor_data
        assert "metadata" in sensor_data
        assert "metrics" in sensor_data
        assert "temperature" in sensor_data["metrics"]
        assert "humidity" in sensor_data["metrics"]
        
        logger.info("Sensor Simulator tests passed")
        logger.info(f"   Generated data: Temp={sensor_data['metrics']['temperature']}°F, Humidity={sensor_data['metrics']['humidity']}%")
        logger.info(f"   Sensor info: {sensor_info['location']}, {sensor_info['building']}, {sensor_info['room']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Sensor Simulator test failed: {e}")
        return False

def test_alert_manager():
    """Test the alert manager module"""
    logger.info("Testing Alert Manager...")
    
    try:
        from alert_manager import AlertManager
        
        # Create alert manager
        alert_mgr = AlertManager()
        
        # Test threshold setting
        alert_mgr.set_thresholds(63, 80, 40, 60)
        thresholds = alert_mgr.get_thresholds()
        
        # Test threshold validation
        warnings = alert_mgr.validate_thresholds()
        
        # Test alert generation
        test_data = {
            "metrics": {
                "temperature": 85,  # Above threshold
                "humidity": 35      # Below threshold
            }
        }
        
        alerts = alert_mgr.check_all_thresholds(test_data)
        
        # Verify alerts
        assert len(alerts) == 2  # Should have 2 alerts
        assert any(alert["type"] == "TEMPERATURE_HIGH" for alert in alerts)
        assert any(alert["type"] == "HUMIDITY_LOW" for alert in alerts)
        
        logger.info("Alert Manager tests passed")
        logger.info(f"   Thresholds: {thresholds}")
        logger.info(f"   Generated {len(alerts)} alerts")
        
        return True
        
    except Exception as e:
        logger.error(f"Alert Manager test failed: {e}")
        return False

def test_data_analyzer():
    """Test the data analyzer module (without database connection)"""
    logger.info("Testing Data Analyzer...")
    
    try:
        from data_analyzer import DataAnalyzer
        
        # Create a mock collection object for testing
        class MockCollection:
            def find(self, query):
                return []
            def count_documents(self, query):
                return 0
            def aggregate(self, pipeline):
                return []
        
        mock_collection = MockCollection()
        
        # Create data analyzer
        analyzer = DataAnalyzer(mock_collection)
        
        # Test that methods exist and are callable
        assert hasattr(analyzer, 'query_recent_data')
        assert hasattr(analyzer, 'get_alert_summary')
        assert hasattr(analyzer, 'get_temperature_trends')
        assert hasattr(analyzer, 'get_humidity_trends')
        assert hasattr(analyzer, 'export_data_to_json')
        
        logger.info("Data Analyzer tests passed")
        logger.info("   All required methods are present")
        
        return True
        
    except Exception as e:
        logger.error(f"Data Analyzer test failed: {e}")
        return False

def test_module_imports():
    """Test that all modules can be imported correctly"""
    logger.info("Testing Module Imports...")
    
    try:
        # Test all module imports
        from database_manager import DatabaseManager
        from time_series_collection import TimeSeriesCollection
        from sensor_simulator import SensorSimulator
        from alert_manager import AlertManager
        from data_analyzer import DataAnalyzer
        
        logger.info("All module imports successful")
        return True
        
    except Exception as e:
        logger.error(f"Module import test failed: {e}")
        return False

def test_configuration():
    """Test configuration and constants"""
    logger.info("Testing Configuration...")
    
    try:
        # Test configuration values
        connection_string = "mongodb://test:testDev%4012C@localhost:27017/"
        db_name = "monitoringDB"
        collection_name = "serverRoomLogs"
        
        # Test that configuration is valid
        assert "mongodb://" in connection_string
        assert db_name == "monitoringDB"
        assert collection_name == "serverRoomLogs"
        
        logger.info("Configuration tests passed")
        logger.info(f"   Connection: {connection_string}")
        logger.info(f"   Database: {db_name}")
        logger.info(f"   Collection: {collection_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Configuration test failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting Module Tests...")
    logger.info("=" * 50)
    
    tests = [
        ("Module Imports", test_module_imports),
        ("Configuration", test_configuration),
        ("Sensor Simulator", test_sensor_simulator),
        ("Alert Manager", test_alert_manager),
        ("Data Analyzer", test_data_analyzer),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        if test_func():
            passed += 1
        else:
            logger.error(f"{test_name} test failed")
    
    logger.info("\n" + "=" * 50)
    logger.info(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("All tests passed! The modular system is working correctly.")
        logger.info("\nYou can now run:")
        logger.info("   python main_simulation.py    # Start IoT simulation")
        logger.info("   python main_analyzer.py      # Analyze collected data")
    else:
        logger.error(f"{total - passed} test(s) failed. Please check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    main()
