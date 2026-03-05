/**
 * MongoDB Queue Wrapper
 * Wraps all MongoDB operations to queue them through RabbitMQ
 * Provides transparent queuing for insertOne, updateOne, deleteMany, etc.
 */

import { v4 as uuidv4 } from "uuid";
import rabbitMQ from "./rabbitmq.js";
import logger from "./logger.js";
import { getDatabase } from "./database.js";

class MongoDBQueueWrapper {
  constructor(collection, collectionName) {
    this.collectionName = collectionName;
  }

  /**
   * Dynamically fetch the collection on every query.
   * THIS IS COMPULSORY for auto-reconnection!
   * If we store `this.collection` statically in the constructor, 
   * this wrapper will permanently hold onto a dead connection object 
   * if the database ever restarts, requiring a PM2 restart to fix.
   * Fetching it dynamically via `getDatabase()` guarantees we always 
   * get the fresh, working database object after a reconnection.
   */
  get _collection() {
    return getDatabase().collection(this.collectionName);
  }

  /**
   * Queue MongoDB operation
   */
  async queueOperation(operation, args, options = {}) {
    try {
      const messageId = uuidv4();
      const message = {
        messageId,
        collection: this.collectionName,
        operation,
        args,
        options,
        timestamp: new Date().toISOString(),
        retryCount: 0,
      };

      await rabbitMQ.publish(message);

      logger.info("[MONGODB_QUEUE] Operation queued for RabbitMQ", {
        messageId,
        collection: this.collectionName,
        operation,
        data: JSON.stringify(args[0]) // Logging the "saved mongo string" (queued string)
      });

      return { queued: true, messageId };
    } catch (error) {
      logger.error("[MONGODB_QUEUE] Failed to queue operation", {
        error: error.message,
        collection: this.collectionName,
        operation,
      });
      throw error;
    }
  }

  /**
   * Wrapped MongoDB methods
   */
  async insertOne(document, options = {}) {
    return this.queueOperation("insertOne", [document], options);
  }

  async insertMany(documents, options = {}) {
    return this.queueOperation("insertMany", [documents], options);
  }

  async updateOne(filter, update, options = {}) {
    return this.queueOperation("updateOne", [filter, update], options);
  }

  async updateMany(filter, update, options = {}) {
    return this.queueOperation("updateMany", [filter, update], options);
  }

  async deleteOne(filter, options = {}) {
    return this.queueOperation("deleteOne", [filter], options);
  }

  async deleteMany(filter, options = {}) {
    return this.queueOperation("deleteMany", [filter], options);
  }

  async replaceOne(filter, replacement, options = {}) {
    return this.queueOperation("replaceOne", [filter, replacement], options);
  }

  async findOneAndUpdate(filter, update, options = {}) {
    return this.queueOperation("findOneAndUpdate", [filter, update], options);
  }

  async findOneAndReplace(filter, replacement, options = {}) {
    return this.queueOperation(
      "findOneAndReplace",
      [filter, replacement],
      options
    );
  }

  async findOneAndDelete(filter, options = {}) {
    return this.queueOperation("findOneAndDelete", [filter], options);
  }

  /**
   * Read operations - direct passthrough (no queuing needed)
   */
  async findOne(filter, options = {}) {
    return this._collection.findOne(filter, options);
  }

  find(filter, options = {}) {
    // Return the cursor directly so toArray(), forEach(), etc. work
    return this._collection.find(filter, options);
  }

  async countDocuments(filter, options = {}) {
    return this._collection.countDocuments(filter, options);
  }

  async estimatedDocumentCount(options = {}) {
    return this._collection.estimatedDocumentCount(options);
  }

  async distinct(key, filter, options = {}) {
    return this._collection.distinct(key, filter, options);
  }

  aggregate(pipeline, options = {}) {
    // Return the aggregation cursor directly
    return this._collection.aggregate(pipeline, options);
  }

  /**
   * Get the original collection (for advanced operations)
   */
  getOriginalCollection() {
    return this._collection;
  }
}

/**
 * Wrap all collections with queue functionality
 */

export function wrapCollectionsWithQueue(collections) {
  const wrappedCollections = {};

  for (const [collectionName, collection] of Object.entries(collections)) {
    // Use the actual MongoDB collection name from the collection object
    const actualCollectionName = collection.collectionName;

    wrappedCollections[collectionName] = new MongoDBQueueWrapper(
      collection,
      actualCollectionName // Use actual MongoDB collection name, not JS key
    );
  }

  return wrappedCollections;
}

export default MongoDBQueueWrapper;
