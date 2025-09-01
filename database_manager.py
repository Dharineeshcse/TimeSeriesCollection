import logging
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles MongoDB connection, health checks, and basic database operations"""
    
    def __init__(self, connection_string, db_name):
        self.connection_string = connection_string
        self.db_name = db_name
        self.client = None
        self.db = None
        
    def connect(self):
        """Establish connection to MongoDB with proper error handling"""
        try:
            logger.info("Attempting to connect to MongoDB...")
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test the connection
            self.client.admin.command('ping')
            logger.info("MongoDB connection successful")
            
            self.db = self.client[self.db_name]
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            return False
    
    def check_health(self):
        """Check database health using a standard (non time-series) collection."""
        try:
            # Basic server ping
            self.client.admin.command('ping')
            
            # Use a small standard collection for health checks
            health_col = self.db["__health"]
            test_doc = {"test": "health_check", "timestamp": datetime.utcnow()}
            result = health_col.insert_one(test_doc)
            inserted_id = result.inserted_id
            # Read back
            _ = health_col.find_one({"_id": inserted_id})
            # Clean up
            health_col.delete_one({"_id": inserted_id})
            
            logger.info("Database health check passed")
            return True
        
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def get_collection(self, collection_name):
        """Get a collection from the database"""
        if self.db is None:
            logger.error("Database not connected")
            return None
        return self.db[collection_name]
    
    def list_collections(self):
        """List all collections in the database"""
        try:
            if self.db is None:
                logger.error("Database not connected")
                return []
            return self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []
    
    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def is_connected(self):
        """Check if database is connected"""
        try:
            if self.client:
                self.client.admin.command('ping')
                return True
            return False
        except:
            return False
