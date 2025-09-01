import logging
from datetime import datetime, timedelta
from pymongo import DESCENDING

logger = logging.getLogger(__name__)

class DataAnalyzer:
    """Handles data querying, aggregation, and analysis for time-series collections"""
    
    def __init__(self, collection):
        self.collection = collection
    
    def query_recent_data(self, hours=24, location=None, building=None, room=None):
        """Query recent data within specified hours with optional filters"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            query = {
                "timestamp": {
                    "$gte": start_time,
                    "$lte": end_time
                }
            }
            
            if location:
                query["metadata.location"] = location
            if building:
                query["metadata.building"] = building
            if room:
                query["metadata.room"] = room
            
            cursor = self.collection.find(query).sort("timestamp", DESCENDING)
            results = list(cursor)
            
            logger.info(f"✅ Retrieved {len(results)} documents from last {hours} hours")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error querying recent data: {e}")
            return []
    
    def get_alert_summary(self, days=7):
        """Get summary of alerts over specified days"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            pipeline = [
                {
                    "$match": {
                        "timestamp": {"$gte": start_time, "$lte": end_time},
                        "alert_type": {"$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": "$alert_type",
                        "count": {"$sum": 1},
                        "severity_counts": {
                            "$push": "$severity"
                        }
                    }
                },
                {
                    "$project": {
                        "alert_type": "$_id",
                        "count": 1,
                        "severity_counts": 1,
                        "critical_count": {
                            "$size": {
                                "$filter": {
                                    "input": "$severity_counts",
                                    "cond": {"$eq": ["$$this", "CRITICAL"]}
                                }
                            }
                        },
                        "warning_count": {
                            "$size": {
                                "$filter": {
                                    "input": "$severity_counts",
                                    "cond": {"$eq": ["$$this", "WARNING"]}
                                }
                            }
                        }
                    }
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            
            logger.info(f"✅ Alert summary retrieved for last {days} days")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting alert summary: {e}")
            return []
    
    def get_temperature_trends(self, days=7):
        """Get temperature trends over specified days"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            pipeline = [
                {
                    "$match": {
                        "timestamp": {"$gte": start_time, "$lte": end_time},
                        "metrics.temperature": {"$exists": True}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "day": {"$dayOfMonth": "$timestamp"},
                            "hour": {"$hour": "$timestamp"}
                        },
                        "avg_temperature": {"$avg": "$metrics.temperature"},
                        "min_temperature": {"$min": "$metrics.temperature"},
                        "max_temperature": {"$max": "$metrics.temperature"},
                        "readings_count": {"$sum": 1}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            
            logger.info(f"✅ Temperature trends retrieved for last {days} days")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting temperature trends: {e}")
            return []
    
    def get_humidity_trends(self, days=7):
        """Get humidity trends over specified days"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            pipeline = [
                {
                    "$match": {
                        "timestamp": {"$gte": start_time, "$lte": end_time},
                        "metrics.humidity": {"$exists": True}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "day": {"$dayOfMonth": "$timestamp"},
                            "hour": {"$hour": "$timestamp"}
                        },
                        "avg_humidity": {"$avg": "$metrics.humidity"},
                        "min_humidity": {"$min": "$metrics.humidity"},
                        "max_humidity": {"$max": "$metrics.humidity"},
                        "readings_count": {"$sum": 1}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            
            logger.info(f"✅ Humidity trends retrieved for last {days} days")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting humidity trends: {e}")
            return []
    
    def get_optimal_periods(self, days=7):
        """Get periods when conditions were optimal"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            query = {
                "timestamp": {"$gte": start_time, "$lte": end_time},
                "alert_type": None,  # No alerts means optimal conditions
                "metrics.temperature": {"$gte": 63, "$lte": 80},
                "metrics.humidity": {"$gte": 40, "$lte": 60}
            }
            
            cursor = self.collection.find(query).sort("timestamp", DESCENDING)
            results = list(cursor)
            
            logger.info(f"✅ Retrieved {len(results)} optimal condition records from last {days} days")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting optimal periods: {e}")
            return []
    
    def get_aggregated_metrics(self, start_time, end_time, location=None, building=None, room=None):
        """Get aggregated metrics (average, min, max) for the specified time range"""
        try:
            match_stage = {
                "timestamp": {
                    "$gte": start_time,
                    "$lte": end_time
                }
            }
            
            if location:
                match_stage["metadata.location"] = location
            if building:
                match_stage["metadata.building"] = building
            if room:
                match_stage["metadata.room"] = room
            
            pipeline = [
                {"$match": match_stage},
                {"$group": {
                    "_id": None,
                    "avg_temperature": {"$avg": "$metrics.temperature"},
                    "min_temperature": {"$min": "$metrics.temperature"},
                    "max_temperature": {"$max": "$metrics.temperature"},
                    "avg_humidity": {"$avg": "$metrics.humidity"},
                    "min_humidity": {"$min": "$metrics.humidity"},
                    "max_humidity": {"$max": "$metrics.humidity"},
                    "total_readings": {"$sum": 1},
                    "alert_count": {
                        "$sum": {
                            "$cond": [
                                {"$ne": ["$alert_type", None]},
                                1,
                                0
                            ]
                        }
                    }
                }}
            ]
            
            result = list(self.collection.aggregate(pipeline))
            
            if result:
                logger.info("✅ Aggregated metrics calculated successfully!")
                return result[0]
            else:
                logger.warning("⚠️ No data found for aggregation")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error calculating aggregated metrics: {e}")
            return None
    
    def get_data_quality_metrics(self, days=7):
        """Get data quality metrics including missing data and anomalies"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            # Total expected readings (assuming 1 reading per minute)
            total_minutes = days * 24 * 60
            expected_readings = total_minutes
            
            # Actual readings
            actual_readings = self.collection.count_documents({
                "timestamp": {"$gte": start_time, "$lte": end_time}
            })
            
            # Missing data percentage
            missing_percentage = ((expected_readings - actual_readings) / expected_readings) * 100
            
            # Anomaly detection (readings outside normal ranges)
            anomalies = self.collection.count_documents({
                "timestamp": {"$gte": start_time, "$lte": end_time},
                "$or": [
                    {"metrics.temperature": {"$lt": 60, "$gt": 85}},
                    {"metrics.humidity": {"$lt": 35, "$gt": 70}}
                ]
            })
            
            quality_metrics = {
                "period_days": days,
                "expected_readings": expected_readings,
                "actual_readings": actual_readings,
                "missing_readings": expected_readings - actual_readings,
                "missing_percentage": round(missing_percentage, 2),
                "anomalies": anomalies,
                "data_completeness": round((actual_readings / expected_readings) * 100, 2)
            }
            
            logger.info("✅ Data quality metrics calculated successfully!")
            return quality_metrics
            
        except Exception as e:
            logger.error(f"❌ Error calculating data quality metrics: {e}")
            return None
    
    def export_data_to_json(self, filename, days=7):
        """Export time-series data to JSON file"""
        try:
            import json
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            query = {
                "timestamp": {"$gte": start_time, "$lte": end_time}
            }
            
            cursor = self.collection.find(query).sort("timestamp", 1)
            results = list(cursor)
            
            # Convert datetime objects to string for JSON serialization
            for doc in results:
                if "timestamp" in doc:
                    doc["timestamp"] = doc["timestamp"].isoformat()
            
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"✅ Exported {len(results)} documents to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error exporting data: {e}")
            return False
    
    def search_by_metadata(self, location=None, building=None, room=None, sensor_id=None, limit=100):
        """Search for documents by metadata criteria"""
        try:
            query = {}
            
            if location:
                query["metadata.location"] = location
            if building:
                query["metadata.building"] = building
            if room:
                query["metadata.room"] = room
            if sensor_id:
                query["metadata.sensor_id"] = sensor_id
            
            cursor = self.collection.find(query).sort("timestamp", DESCENDING).limit(limit)
            results = list(cursor)
            
            logger.info(f"✅ Metadata search returned {len(results)} documents")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error searching by metadata: {e}")
            return []
    
    def get_time_range_data(self, start_time, end_time, limit=1000):
        """Get all data within a specific time range"""
        try:
            query = {
                "timestamp": {
                    "$gte": start_time,
                    "$lte": end_time
                }
            }
            
            cursor = self.collection.find(query).sort("timestamp", 1).limit(limit)
            results = list(cursor)
            
            logger.info(f"✅ Retrieved {len(results)} documents from {start_time} to {end_time}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting time range data: {e}")
            return []
