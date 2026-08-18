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

/**
 * Passively check one queue's stats using its own short-lived temporary
 * channel. A queue that doesn't exist yet (early boot) or any other
 * checkQueue failure closes the channel it happened on (AMQP channel-level
 * exception) - giving each queue its own channel means one missing/broken
 * queue can't take the others down with it.
 */
async function checkQueueStats(queueName) {
    let channel;
    try {
        channel = await rabbitMQ.connection.createChannel();
        const info = await channel.checkQueue(queueName);
        return { name: queueName, messages: info.messageCount, consumers: info.consumerCount };
    } catch (error) {
        return { name: queueName, error: error.message };
    } finally {
        if (channel) {
            await channel.close().catch(() => {});
        }
    }
}

async function checkRabbitMQ() {
    try {
        if (!rabbitMQ.isConnected) {
            return { status: 'disconnected', error: 'Not connected' };
        }

        // Must match how webhook-uae-consumer.js builds its queue name exactly.
        const uaeWebhookQueue = "_uae_" + config.webhooks.queue;

        const [mainQueue, dlq, webhookQueue, uaeQueue] = await Promise.all([
            checkQueueStats(config.rabbitmq.queue),
            checkQueueStats(config.rabbitmq.deadLetterQueue),
            checkQueueStats(config.webhooks.queue),
            checkQueueStats(uaeWebhookQueue),
        ]);

        // Top-level status stays keyed on connection health, not consumer
        // count - a brief zero-consumer window during reconnect is expected
        // and must not flip the whole health check to unhealthy. Consumer
        // liveness is reported separately below for a monitor to key off.
        const consumingQueues = { main: mainQueue, webhook: webhookQueue, uae: uaeQueue };
        const consumersHealthy = Object.values(consumingQueues).every(
            (q) => !q.error && q.consumers >= 1
        );
        const consumerWarnings = Object.values(consumingQueues).flatMap((q) => {
            if (q.error) return [`Queue ${q.name} could not be checked: ${q.error}`];
            if (q.consumers === 0) return [`Queue ${q.name} has zero consumers`];
            return [];
        });

        return {
            status: 'connected',
            host: `${config.rabbitmq.host}:${config.rabbitmq.port}`,
            mainQueue: {
                name: mainQueue.name,
                messages: mainQueue.messages,
                consumers: mainQueue.consumers,
                ...(mainQueue.error ? { error: mainQueue.error } : {}),
            },
            webhookQueue: {
                name: webhookQueue.name,
                messages: webhookQueue.messages,
                consumers: webhookQueue.consumers,
                ...(webhookQueue.error ? { error: webhookQueue.error } : {}),
            },
            uaeWebhookQueue: {
                name: uaeQueue.name,
                messages: uaeQueue.messages,
                consumers: uaeQueue.consumers,
                ...(uaeQueue.error ? { error: uaeQueue.error } : {}),
            },
            deadLetterQueue: {
                name: dlq.name,
                messages: dlq.messages,
                warning: !dlq.error && dlq.messages > 0,
                ...(dlq.error ? { error: dlq.error } : {}),
            },
            consumers: {
                main: mainQueue.error ? null : mainQueue.consumers,
                webhook: webhookQueue.error ? null : webhookQueue.consumers,
                uae: uaeQueue.error ? null : uaeQueue.consumers,
                allActive: consumersHealthy,
            },
            warnings: [
                ...(!mainQueue.error && mainQueue.messages > 1000 ? ['High queue size'] : []),
                ...(!dlq.error && dlq.messages > 0 ? ['Messages in dead letter queue'] : []),
                ...consumerWarnings,
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
