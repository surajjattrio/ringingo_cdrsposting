import express from 'express';
import amqp from 'amqplib';
import { getDatabase } from './database.js';
import rabbitMQ from './rabbitmq.js';
import config from './config.js';
import logger from './logger.js';

const router = express.Router();

async function checkMongoDB() {
    try {
        const db = getDatabase();
        await db.command({ ping: 1 });
        const collections = await db.listCollections().toArray();
        return {
            status: 'connected',
            database: config.mongodb.database,
            collections: collections.length,
        };
    } catch (error) {
        return { status: 'disconnected', error: error.message };
    }
}

async function checkRabbitMQ() {
    try {
        if (!rabbitMQ.isConnected) {
            return { status: 'disconnected', error: 'Not connected' };
        }

        // Open a temporary channel to check queue stats
        const tempChannel = await rabbitMQ.connection.createChannel();
        const mainQueue = await tempChannel.checkQueue(config.rabbitmq.queue);
        const dlq = await tempChannel.checkQueue(config.rabbitmq.deadLetterQueue);
        await tempChannel.close();

        return {
            status: 'connected',
            host: `${config.rabbitmq.host}:${config.rabbitmq.port}`,
            mainQueue: {
                name: config.rabbitmq.queue,
                messages: mainQueue.messageCount,
                consumers: mainQueue.consumerCount,
            },
            deadLetterQueue: {
                name: config.rabbitmq.deadLetterQueue,
                messages: dlq.messageCount,
                warning: dlq.messageCount > 0,
            },
            warnings: [
                ...(mainQueue.messageCount > 1000 ? ['High queue size'] : []),
                ...(dlq.messageCount > 0 ? ['Messages in dead letter queue'] : []),
            ],
        };
    } catch (error) {
        return { status: 'disconnected', error: error.message };
    }
}

async function checkFailedOperations() {
    try {
        const db = getDatabase();
        const count = await db.collection('failed_mongodb_operations').countDocuments();
        const recent = count > 0
            ? await db.collection('failed_mongodb_operations')
                .find()
                .sort({ failedAt: -1 })
                .limit(5)
                .toArray()
            : [];

        return {
            status: count === 0 ? 'ok' : count > 100 ? 'critical' : 'warning',
            count,
            recent: recent.map(op => ({
                collection: op.collection,
                operation: op.operation,
                error: op.error,
                failedAt: op.failedAt,
            })),
        };
    } catch (error) {
        return { status: 'unknown', error: error.message };
    }
}

/**
 * GET /api/health
 * Full system health check
 */
router.get('/health', async (req, res) => {
    try {
        const [mongodb, rabbitmq, failedOps] = await Promise.all([
            checkMongoDB(),
            checkRabbitMQ(),
            checkFailedOperations(),
        ]);

        const allHealthy =
            mongodb.status === 'connected' &&
            rabbitmq.status === 'connected' &&
            failedOps.count === 0;

        const result = {
            status: allHealthy ? 'healthy' : 'degraded',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            services: { mongodb, rabbitmq, failedOperations: failedOps },
        };

        logger.info('[HEALTH_CHECK] Full health check requested', { status: result.status });
        res.status(allHealthy ? 200 : 207).json(result);
    } catch (error) {
        logger.error('[HEALTH_CHECK] Health check error', { error: error.message });
        res.status(500).json({ status: 'error', error: error.message });
    }
});

/**
 * GET /api/health/mongodb
 * MongoDB connectivity check
 */
router.get('/health/mongodb', async (req, res) => {
    const result = await checkMongoDB();
    res.status(result.status === 'connected' ? 200 : 503).json({
        timestamp: new Date().toISOString(),
        ...result,
    });
});

/**
 * GET /api/health/rabbitmq
 * RabbitMQ connectivity and queue stats check
 */
router.get('/health/rabbitmq', async (req, res) => {
    const result = await checkRabbitMQ();
    res.status(result.status === 'connected' ? 200 : 503).json({
        timestamp: new Date().toISOString(),
        ...result,
    });
});

/**
 * GET /api/health/failed-operations
 * Check failed MongoDB operations backlog
 */
router.get('/health/failed-operations', async (req, res) => {
    const result = await checkFailedOperations();
    const httpStatus = result.status === 'ok' ? 200 : result.status === 'critical' ? 503 : 207;
    res.status(httpStatus).json({
        timestamp: new Date().toISOString(),
        ...result,
    });
});

export default router;
