import logging
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

logger = logging.getLogger(__name__)

class TimeSeriesCollection:
    """Manages time-series collection setup, indexing, and configuration"""
    
    def __init__(self, db_manager, collection_name):
        self.db_manager = db_manager
        self.collection_name = collection_name
        self.collection = None
        
    def setup(self):
        """Setup time-series collection with proper indexes and validation"""
        try:
            # Check if collection exists and is a time-series collection
            collections = self.db_manager.list_collections()
            
            if self.collection_name not in collections:
                logger.info("Creating time-series collection...")
                try:
                    self.db_manager.db.create_collection(
                        self.collection_name,
                        timeseries={
                            "timeField": "timestamp",
                            "metaField": "metadata",
                            "granularity": "minutes"
                        }
                    )
                    logger.info("Time-series collection created successfully")
                except CollectionInvalid:
                    # Collection already exists; continue gracefully
                    logger.info("Time-series collection already exists (race condition handled)")
            else:
                logger.info("Time-series collection already exists")
            
            # Get the collection reference
            self.collection = self.db_manager.get_collection(self.collection_name)
            
            # Create indexes for optimal query performance and cleanup incompatible indexes
            self._create_indexes()
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting up time-series collection: {e}")
            raise
    
    def _create_indexes(self):
        """Create necessary indexes for optimal performance"""
        try:
            # Drop incompatible indexes if they exist (array values not allowed on indexed measurement fields)
            existing = self.collection.index_information()
            for name, info in existing.items():
                keys = info.get('key', [])
                # keys is a list of tuples like [(field, direction)]
                if keys == [("alert_type", 1)] or keys == [("severity", 1)]:
                    try:
                        self.collection.drop_index(name)
                        logger.info(f"Dropped incompatible index: {name}")
                    except Exception as drop_err:
                        logger.warning(f"Could not drop index {name}: {drop_err}")
            
            # Time-based index (for sorting)
            self.collection.create_index([("timestamp", DESCENDING)])
            
            # Metadata indexes for filtering
            self.collection.create_index([("metadata.location", ASCENDING)])
            self.collection.create_index([("metadata.building", ASCENDING)])
            self.collection.create_index([("metadata.room", ASCENDING)])
            
            # Do NOT index alert fields because they may be arrays per document
            
            # Compound index for common queries
            self.collection.create_index([
                ("timestamp", DESCENDING),
                ("metadata.location", ASCENDING),
                ("metadata.building", ASCENDING)
            ])
            
            logger.info("Indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise
    
    def get_collection(self):
        """Get the time-series collection reference"""
        return self.collection
    
    def insert_document(self, document):
        """Insert a document into the time-series collection"""
        try:
            if self.collection is None:
                logger.error("Collection not initialized")
                return None, False
            
            result = self.collection.insert_one(document)
            return result.inserted_id, True
            
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return None, False
    
    def verify_insertion(self, document_id):
        """Verify that a document was successfully inserted"""
        try:
            if self.collection is None:
                return False
            
            saved = self.collection.find_one({"_id": document_id})
            return saved is not None
            
        except Exception as e:
            logger.error(f"Error verifying insertion: {e}")
            return False
    
    def cleanup_old_data(self, days_to_keep=30):
        """Clean up old data based on retention policy"""
        try:
            from datetime import datetime, timedelta
            
            if self.collection is None:
                logger.error("Collection not initialized")
                return 0
            
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            result = self.collection.delete_many({
                "timestamp": {"$lt": cutoff_date}
            })
            
            logger.info(f"Cleaned up {result.deleted_count} old documents (older than {days_to_keep} days)")
            return result.deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0
    
    def get_collection_stats(self):
        """Get basic statistics about the time-series collection"""
        try:
            if self.collection is None:
                return None
            
            total_documents = self.collection.count_documents({})
            
            # Get date range
            first_doc = self.collection.find_one({}, sort=[("timestamp", 1)])
            last_doc = self.collection.find_one({}, sort=[("timestamp", -1)])
            
            if first_doc and last_doc:
                stats = {
                    "first_record": first_doc["timestamp"],
                    "last_record": last_doc["timestamp"],
                    "total_documents": total_documents
                }
                
                logger.info("Collection statistics retrieved successfully")
                return stats
            else:
                logger.warning("No documents found in collection")
                return None
                
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return None
