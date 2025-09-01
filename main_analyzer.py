import logging
from datetime import datetime

# Import our modular components
from database_manager import DatabaseManager
from time_series_collection import TimeSeriesCollection
from data_analyzer import DataAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TimeSeriesAnalyzerOrchestrator:
    """Orchestrates time-series data analysis using modular components"""
    
    def __init__(self, connection_string, db_name, collection_name):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        
        # Initialize components
        self.db_manager = DatabaseManager(connection_string, db_name)
        self.ts_collection = TimeSeriesCollection(self.db_manager, collection_name)
        self.data_analyzer = None
        
    def initialize_system(self):
        """Initialize all system components"""
        try:
            logger.info("Initializing Time-Series Data Analyzer...")
            
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to MongoDB. Exiting...")
                return False
            
            # Setup time-series collection
            self.ts_collection.setup()
            
            # Initialize data analyzer with collection
            collection = self.ts_collection.get_collection()
            self.data_analyzer = DataAnalyzer(collection)
            
            logger.info("System initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            return False
    
    def run_comprehensive_analysis(self):
        """Run comprehensive analysis of the time-series data"""
        try:
            logger.info("Time-Series Data Analyzer")
            logger.info("=" * 50)
            
            # Get collection statistics
            stats = self.ts_collection.get_collection_stats()
            if stats:
                logger.info(f"Collection Statistics:")
                logger.info(f"   Total Documents: {stats['total_documents']}")
                logger.info(f"   First Record: {stats['first_record']}")
                logger.info(f"   Last Record: {stats['last_record']}")
            
            logger.info("\n" + "=" * 50)
            
            # Get recent data (last 24 hours)
            recent_data = self.data_analyzer.query_recent_data(hours=24)
            if recent_data:
                logger.info(f"Recent Data (Last 24 hours): {len(recent_data)} records")
                
                # Show latest readings
                for i, doc in enumerate(recent_data[:5]):  # Show first 5
                    if "metrics" in doc:
                        temp = doc["metrics"].get("temperature", "N/A")
                        humidity = doc["metrics"].get("humidity", "N/A")
                        timestamp = doc["timestamp"]
                        logger.info(f"   {timestamp}: Temp={temp}°F, Humidity={humidity}%")
            
            logger.info("\n" + "=" * 50)
            
            # Get alert summary (last 7 days)
            alert_summary = self.data_analyzer.get_alert_summary(days=7)
            if alert_summary:
                logger.info(f"Alert Summary (Last 7 days):")
                for alert in alert_summary:
                    logger.info(f"   {alert['alert_type']}: {alert['count']} alerts")
                    logger.info(f"     Critical: {alert['critical_count']}, Warning: {alert['warning_count']}")
            
            logger.info("\n" + "=" * 50)
            
            # Get temperature trends (last 7 days)
            temp_trends = self.data_analyzer.get_temperature_trends(days=7)
            if temp_trends:
                logger.info(f"Temperature Trends (Last 7 days):")
                for trend in temp_trends[:5]:  # Show first 5
                    date_info = trend["_id"]
                    avg_temp = round(trend["avg_temperature"], 2)
                    min_temp = round(trend["min_temperature"], 2)
                    max_temp = round(trend["max_temperature"], 2)
                    logger.info(f"   {date_info['month']}/{date_info['day']} {date_info['hour']}:00 - "
                              f"Avg: {avg_temp}°F, Min: {min_temp}°F, Max: {max_temp}°F")
            
            logger.info("\n" + "=" * 50)
            
            # Get humidity trends (last 7 days)
            humidity_trends = self.data_analyzer.get_humidity_trends(days=7)
            if humidity_trends:
                logger.info(f"Humidity Trends (Last 7 days):")
                for trend in humidity_trends[:5]:  # Show first 5
                    date_info = trend["_id"]
                    avg_humidity = round(trend["avg_humidity"], 2)
                    min_humidity = round(trend["min_humidity"], 2)
                    max_humidity = round(trend["max_humidity"], 2)
                    logger.info(f"   {date_info['month']}/{date_info['day']} {date_info['hour']}:00 - "
                              f"Avg: {avg_humidity}%, Min: {min_humidity}%, Max: {max_humidity}%")
            
            logger.info("\n" + "=" * 50)
            
            # Get optimal periods
            optimal_periods = self.data_analyzer.get_optimal_periods(days=7)
            if optimal_periods:
                logger.info(f"Optimal Periods (Last 7 days): {len(optimal_periods)} records")
                logger.info("   Showing first 3 optimal periods:")
                for i, period in enumerate(optimal_periods[:3]):
                    temp = period["metrics"].get("temperature", "N/A")
                    humidity = period["metrics"].get("humidity", "N/A")
                    timestamp = period["timestamp"]
                    logger.info(f"   {timestamp}: Temp={temp}°F, Humidity={humidity}%")
            
            logger.info("\n" + "=" * 50)
            
            # Export data to JSON
            export_success = self.data_analyzer.export_data_to_json("time_series_export.json", days=1)
            if export_success:
                logger.info("Data exported to time_series_export.json")
            
            logger.info("\n" + "=" * 50)
            
            # Get aggregated metrics for last 24 hours
            from datetime import timedelta
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            aggregated_metrics = self.data_analyzer.get_aggregated_metrics(
                start_time, end_time, 
                location="MCW Porur SEZ", 
                building="B9", 
                room="ServerRoom"
            )
            
            if aggregated_metrics:
                logger.info(f"Aggregated Metrics (Last 24 hours):")
                logger.info(f"   Temperature: Avg={round(aggregated_metrics['avg_temperature'], 2)}°F, "
                          f"Min={round(aggregated_metrics['min_temperature'], 2)}°F, "
                          f"Max={round(aggregated_metrics['max_temperature'], 2)}°F")
                logger.info(f"   Humidity: Avg={round(aggregated_metrics['avg_humidity'], 2)}%, "
                          f"Min={round(aggregated_metrics['min_humidity'], 2)}%, "
                          f"Max={round(aggregated_metrics['max_humidity'], 2)}%")
                logger.info(f"   Total Readings: {aggregated_metrics['total_readings']}")
                logger.info(f"   Alert Count: {aggregated_metrics['alert_count']}")
            
            logger.info("\n" + "=" * 50)
            
            return True
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            return False
    
    def run_custom_analysis(self, hours=24, days=7):
        """Run custom analysis with specified time ranges"""
        try:
            logger.info(f"Custom Analysis - Hours: {hours}, Days: {days}")
            logger.info("=" * 50)
            
            # Recent data analysis
            recent_data = self.data_analyzer.query_recent_data(hours=hours)
            if recent_data:
                logger.info(f"Recent Data ({hours} hours): {len(recent_data)} records")
                
                # Calculate basic statistics
                temps = [doc["metrics"].get("temperature", 0) for doc in recent_data if "metrics" in doc]
                humidities = [doc["metrics"].get("humidity", 0) for doc in recent_data if "metrics" in doc]
                
                if temps:
                    avg_temp = sum(temps) / len(temps)
                    min_temp = min(temps)
                    max_temp = max(temps)
                    logger.info(f"   Temperature Stats: Avg={round(avg_temp, 2)}°F, Min={round(min_temp, 2)}°F, Max={round(max_temp, 2)}°F")
                
                if humidities:
                    avg_humidity = sum(humidities) / len(humidities)
                    min_humidity = min(humidities)
                    max_humidity = max(humidities)
                    logger.info(f"   Humidity Stats: Avg={round(avg_humidity, 2)}%, Min={round(min_humidity, 2)}%, Max={round(max_humidity, 2)}%")
            
            # Alert analysis
            alert_summary = self.data_analyzer.get_alert_summary(days=days)
            if alert_summary:
                logger.info(f"Alert Analysis ({days} days):")
                total_alerts = sum(alert['count'] for alert in alert_summary)
                total_critical = sum(alert['critical_count'] for alert in alert_summary)
                total_warnings = sum(alert['warning_count'] for alert in alert_summary)
                
                logger.info(f"   Total Alerts: {total_alerts}")
                logger.info(f"   Critical Alerts: {total_critical}")
                logger.info(f"   Warning Alerts: {total_warnings}")
                
                for alert in alert_summary:
                    logger.info(f"   {alert['alert_type']}: {alert['count']} alerts")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during custom analysis: {e}")
            return False
    
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
            "analyzer_ready": self.data_analyzer is not None
        }

def main():
    """Main function to run the time-series analyzer"""
    # Configuration
    connection_string = "mongodb://test:testDev%4012C@localhost:27017/"
    db_name = "monitoringDB"
    collection_name = "serverRoomLogs"
    
    # Create and run analyzer
    orchestrator = TimeSeriesAnalyzerOrchestrator(connection_string, db_name, collection_name)
    
    # Initialize system
    if not orchestrator.initialize_system():
        logger.error("System initialization failed. Exiting...")
        return
    
    # Display system status
    status = orchestrator.get_system_status()
    logger.info("System Status:")
    logger.info(f"   Database Connected: {status['database_connected']}")
    logger.info(f"   Collection Ready: {status['collection_ready']}")
    logger.info(f"   Analyzer Ready: {status['analyzer_ready']}")
    
    try:
        # Run comprehensive analysis
        logger.info("\nStarting comprehensive analysis...")
        orchestrator.run_comprehensive_analysis()
        
        # Run custom analysis
        logger.info("\nStarting custom analysis...")
        orchestrator.run_custom_analysis(hours=12, days=3)
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
    finally:
        orchestrator.cleanup()

if __name__ == "__main__":
    main()
