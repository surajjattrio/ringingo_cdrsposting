/**
 * RabbitMQ Consumer
 * Processes queued MongoDB operations
 * Handles retries, failures, and dead letter queue
 */

import rabbitMQ from './rabbitmq.js';
import config from './config.js';
import logger from './logger.js';
import { getDatabase } from './database.js';

class MongoDBConsumer {
    constructor() {
        this.isConsuming = false;
        this.processedCount = 0;
        this.failedCount = 0;
    }

    /**
     * Start consuming messages
     */
    async start() {
        try {
            if (this.isConsuming) {
                logger.warn('[CONSUMER] Already consuming messages');
                return;
            }

            const channel = rabbitMQ.getChannel();
            
            logger.info('[CONSUMER] Starting message consumption', {
                queue: config.rabbitmq.queue,
                prefetch: channel.prefetch,
            });

            // Start consuming
            await channel.consume(
                config.rabbitmq.queue,
                async (msg) => {
                    if (msg !== null) {
                        await this.processMessage(msg, channel);
                    }
                },
                {
                    noAck: false, // Manual acknowledgment
                }
            );

            this.isConsuming = true;
            logger.success('[CONSUMER] Consumer started successfully');

            // Log stats every 10 minutes
            this.startStatsLogger();

        } catch (error) {
            logger.error('[CONSUMER] Failed to start consumer', {
                error: error.message,
                stack: error.stack,
            });
            throw error;
        }
    }

    /**
     * Process individual message
     */
    async processMessage(msg, channel) {
        const startTime = Date.now();
        let message;

        try {
            // Parse message
            message = JSON.parse(msg.content.toString());
            
            logger.debug('[CONSUMER] Processing message', {
                messageId: message.messageId,
                collection: message.collection,
                operation: message.operation,
                retryCount: message.retryCount,
            });

            // Get database and collection
            const db = getDatabase();
            const collection = db.collection(message.collection);

            // Execute MongoDB operation
            await this.executeOperation(collection, message);

            // Acknowledge message
            channel.ack(msg);

            this.processedCount++;
            const duration = Date.now() - startTime;

            logger.debug('[CONSUMER] Message processed successfully', {
                messageId: message.messageId,
                collection: message.collection,
                operation: message.operation,
                durationMs: duration,
            });

        } catch (error) {
            await this.handleFailure(msg, message, error, channel);
        }
    }

    /**
     * Execute MongoDB operation based on message
     */
    async executeOperation(collection, message) {
        const { operation, args, options } = message;

        switch (operation) {
            case 'insertOne':
                return await collection.insertOne(args[0], options);
            
            case 'insertMany':
                return await collection.insertMany(args[0], options);
            
            case 'updateOne':
                return await collection.updateOne(args[0], args[1], options);
            
            case 'updateMany':
                return await collection.updateMany(args[0], args[1], options);
            
            case 'deleteOne':
                return await collection.deleteOne(args[0], options);
            
            case 'deleteMany':
                return await collection.deleteMany(args[0], options);
            
            case 'replaceOne':
                return await collection.replaceOne(args[0], args[1], options);
            
            case 'findOneAndUpdate':
                return await collection.findOneAndUpdate(args[0], args[1], options);
            
            case 'findOneAndReplace':
                return await collection.findOneAndReplace(args[0], args[1], options);
            
            case 'findOneAndDelete':
                return await collection.findOneAndDelete(args[0], options);
            
            default:
                throw new Error(`Unsupported operation: ${operation}`);
        }
    }

    /**
     * Handle message processing failure
     */
    async handleFailure(msg, message, error, channel) {
        this.failedCount++;

        logger.error('[CONSUMER] Message processing failed', {
            messageId: message?.messageId,
            collection: message?.collection,
            operation: message?.operation,
            retryCount: message?.retryCount || 0,
            error: error.message,
            stack: error.stack,
        });

        try {
            // Get retry count
            const retryCount = message?.retryCount || 0;

            // Check if we should retry
            if (retryCount < config.rabbitmq.maxRetries) {
                // Increment retry count
                message.retryCount = retryCount + 1;
                message.lastError = error.message;
                message.lastErrorTime = new Date().toISOString();

                // Requeue with delay
                await this.requeueWithDelay(message, channel);

                // Acknowledge original message
                channel.ack(msg);

                logger.info('[CONSUMER] Message requeued for retry', {
                    messageId: message.messageId,
                    retryCount: message.retryCount,
                    maxRetries: config.rabbitmq.maxRetries,
                });

            } else {
                // Max retries reached - reject to dead letter queue
                channel.reject(msg, false); // false = don't requeue

                logger.warn('[CONSUMER] Message sent to dead letter queue', {
                    messageId: message.messageId,
                    collection: message.collection,
                    operation: message.operation,
                    finalRetryCount: retryCount,
                    error: error.message,
                });

                // Log to failed operations collection (optional)
                await this.logFailedOperation(message, error);
            }

        } catch (handlerError) {
            logger.error('[CONSUMER] Error in failure handler', {
                error: handlerError.message,
                originalError: error.message,
            });
            // Reject message without requeue
            channel.reject(msg, false);
        }
    }

    /**
     * Requeue message with delay
     */
    async requeueWithDelay(message, channel) {
        try {
            // Calculate delay based on retry count (exponential backoff)
            const delay = Math.min(
                config.rabbitmq.retryDelay * Math.pow(2, message.retryCount - 1),
                config.rabbitmq.maxRetryDelay
            );

            // Publish with delay using message TTL
            const messageBuffer = Buffer.from(JSON.stringify(message));
            
            channel.sendToQueue(
                config.rabbitmq.queue,
                messageBuffer,
                {
                    persistent: true,
                    contentType: 'application/json',
                    timestamp: Date.now(),
                    expiration: delay.toString(), // Delay in milliseconds
                }
            );

            logger.debug('[CONSUMER] Message requeued with delay', {
                messageId: message.messageId,
                delayMs: delay,
            });

        } catch (error) {
            logger.error('[CONSUMER] Failed to requeue message', {
                error: error.message,
                messageId: message.messageId,
            });
            throw error;
        }
    }

    /**
     * Log failed operation to database (optional)
     */
    async logFailedOperation(message, error) {
        try {
            const db = getDatabase();
            const failedOpsCollection = db.collection('failed_mongodb_operations');

            await failedOpsCollection.insertOne({
                messageId: message.messageId,
                collection: message.collection,
                operation: message.operation,
                args: message.args,
                options: message.options,
                retryCount: message.retryCount,
                error: error.message,
                errorStack: error.stack,
                originalTimestamp: message.timestamp,
                failedAt: new Date().toISOString(),
            });

            logger.debug('[CONSUMER] Failed operation logged', {
                messageId: message.messageId,
            });

        } catch (logError) {
            logger.error('[CONSUMER] Failed to log failed operation', {
                error: logError.message,
                messageId: message.messageId,
            });
        }
    }

    /**
     * Start stats logger
     */
    startStatsLogger() {
        setInterval(() => {
            logger.info('[CONSUMER] Stats', {
                processed: this.processedCount,
                failed: this.failedCount,
                successRate: this.processedCount > 0 
                    ? ((this.processedCount / (this.processedCount + this.failedCount)) * 100).toFixed(2) + '%'
                    : 'N/A',
            });
        }, 600000); // Every 10 minutes 
    }

    /**
     * Stop consuming
     */
    async stop() {
        try {
            this.isConsuming = false;
            logger.info('[CONSUMER] Consumer stopped');
        } catch (error) {
            logger.error('[CONSUMER] Error stopping consumer', {
                error: error.message,
            });
        }
    }

    /**
     * Get stats
     */
    getStats() {
        return {
            processed: this.processedCount,
            failed: this.failedCount,
            isConsuming: this.isConsuming,
        };
    }
}

// Export singleton instance
const consumer = new MongoDBConsumer();
export default consumer;