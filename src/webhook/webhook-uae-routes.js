/**
 * UAE Webhook Routes
 * REST API endpoints for managing UAE specialized webhooks
 */

import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { getUAEDatabase } from '../database.js';
import uaeWebhookManager from './webhook-uae-manager.js';
import logger from '../logger.js';
import { ObjectId } from 'mongodb';
import rabbitMQ from '../rabbitmq.js';
import config from '../config.js';

const router = express.Router();

/**
 * POST /api/uae/external/webhook-only
 * Trigger UAE webhook delivery directly WITHOUT saving to database
 */
router.post('/uae/external/webhook-only', async (req, res) => {
    try {
        const data = req.body;

        if (!data || Object.keys(data).length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Body data is required and cannot be empty',
            });
        }

        // Use uuid from data
        const cdrUuid = data.uuid

        // Prepare webhook task with full payload to bypass DB lookup in the consumer
        // Adding customMongoUri for consumer visibility
        const webhookTask = {
            cdrUuid: cdrUuid,
            collectionName: 'cdrs',
            tenantUuid: data.tenant_uuid || null,
            fullCdr: data, // Include the full payload
            timestamp: new Date().toISOString(),
            messageId: `uae_webhook_only_${cdrUuid}_${Date.now()}`,
            customMongoUri: "mongodb://db_pbxcc:dbYDHPyrRSnd@4.240.102.209:27017/admin"
        };

        // Publish directly to the UAE webhook delivery queue
        const uaeQueue = "_uae_" + config.webhooks.queue;

        await rabbitMQ.publish(
            webhookTask,
            {
                expiration: config.webhooks.delay?.toString() || "60000",
                type: 'uae_webhook_delivery_task'
            },
            uaeQueue
        );

        logger.info('[UAE_EXTERNAL_WEBHOOK] UAE Webhook-only task queued', {
            cdrUuid,
            messageId: webhookTask.messageId,
            remoteIp: req.ip,
            queue: uaeQueue
        });

        res.json({
            success: true,
            message: 'UAE Webhook-only delivery task queued successfully (Bypassed Database)',
            messageId: webhookTask.messageId,
            cdrUuid: cdrUuid,
            queue: uaeQueue
        });

    } catch (error) {
        logger.error('[UAE_EXTERNAL_WEBHOOK] Failed to process UAE webhook-only push', {
            error: error.message,
            stack: error.stack
        });
        res.status(500).json({
            success: false,
            error: 'Internal system error while queuing UAE webhook task',
            message: error.message,
        });
    }
});

/**
 * GET /api/uae/webhooks
 * Get all UAE webhooks
 */
router.get('/uae/webhooks', async (req, res) => {
    try {
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        const { tenant_uuid, tenant_id, is_active, webhook_type } = req.query;
        
        const filter = {};
        if (tenant_uuid) filter.tenant_uuid = tenant_uuid;
        else if (tenant_id) filter.tenant_uuid = tenant_id;
        
        if (is_active !== undefined) filter.is_active = is_active === 'true';
        if (webhook_type) filter.webhook_type = webhook_type;
        
        const webhooks = await webhooksCollection.find(filter).toArray();
        
        res.json({
            success: true,
            count: webhooks.length,
            data: webhooks,
        });
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to fetch UAE webhooks', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch UAE webhooks',
            message: error.message,
        });
    }
});

/**
 * GET /api/uae/webhooks/:id
 * Get a specific UAE webhook by uuid or _id
 */
router.get('/uae/webhooks/:id', async (req, res) => {
    try {
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        let webhook = await webhooksCollection.findOne({ uuid: req.params.id });
        
        if (!webhook) {
            try {
                webhook = await webhooksCollection.findOne({ _id: new ObjectId(req.params.id) });
            } catch (e) {}
        }
        
        if (!webhook) {
            return res.status(404).json({
                success: false,
                error: 'UAE Webhook not found',
            });
        }
        
        res.json({
            success: true,
            data: webhook,
        });
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to fetch UAE webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch UAE webhook',
            message: error.message,
        });
    }
});

/**
 * POST /api/uae/webhooks
 * Create a new UAE webhook
 */
router.post('/uae/webhooks', async (req, res) => {
    try {
        const {
            name,
            webhook_url,
            webhook_type = 'cdr',
            method = 'POST',
            headers = {},
            timeout = 30000,
            max_retries = 3,
            retry_delay = 2000,
            is_active = true,
            tenant_uuid,
            tenant_id,
            bearer_token = null,
        } = req.body;
        
        if (!name || !webhook_url) {
            return res.status(400).json({
                success: false,
                error: 'Name and webhook_url are required',
            });
        }
        
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        const webhookUuid = uuidv4();
        
        const newWebhook = {
            uuid: webhookUuid,
            name,
            webhook_url,
            webhook_type,
            method: method.toUpperCase(),
            headers,
            timeout,
            max_retries,
            retry_delay,
            is_active,
            tenant_uuid: tenant_uuid || tenant_id || null,
            bearer_token,
            created_at: new Date(),
            updated_at: new Date(),
        };
        
        const result = await webhooksCollection.insertOne(newWebhook);
        
        logger.info('[UAE_WEBHOOK_API] UAE Webhook created', {
            webhookUuid,
            name,
            webhook_url,
        });
        
        res.status(201).json({
            success: true,
            message: 'UAE Webhook created successfully',
            data: {
                _id: result.insertedId,
                uuid: webhookUuid,
                ...newWebhook,
            },
        });
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to create UAE webhook', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to create UAE webhook',
            message: error.message,
        });
    }
});

/**
 * PUT /api/uae/webhooks/:id
 * Update an existing UAE webhook
 */
router.put('/uae/webhooks/:id', async (req, res) => {
    try {
        const updates = { ...req.body, updated_at: new Date() };
        if (updates.method) updates.method = updates.method.toUpperCase();
        
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        let filter = { uuid: req.params.id };
        let result = await webhooksCollection.updateOne(filter, { $set: updates });
        
        if (result.matchedCount === 0) {
            try {
                filter = { _id: new ObjectId(req.params.id) };
                result = await webhooksCollection.updateOne(filter, { $set: updates });
            } catch (e) {}
        }
        
        if (result.matchedCount === 0) {
            return res.status(404).json({
                success: false,
                error: 'UAE Webhook not found',
            });
        }
        
        logger.info('[UAE_WEBHOOK_API] UAE Webhook updated', {
            webhookId: req.params.id,
        });
        
        res.json({
            success: true,
            message: 'UAE Webhook updated successfully',
        });
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to update UAE webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to update UAE webhook',
            message: error.message,
        });
    }
});

/**
 * DELETE /api/uae/webhooks/:id
 * Delete a UAE webhook
 */
router.delete('/uae/webhooks/:id', async (req, res) => {
    try {
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        let result = await webhooksCollection.deleteOne({ uuid: req.params.id });
        
        if (result.deletedCount === 0) {
            try {
                result = await webhooksCollection.deleteOne({ _id: new ObjectId(req.params.id) });
            } catch (e) {}
        }
        
        if (result.deletedCount === 0) {
            return res.status(404).json({
                success: false,
                error: 'UAE Webhook not found',
            });
        }
        
        logger.info('[UAE_WEBHOOK_API] UAE Webhook deleted', {
            webhookId: req.params.id,
        });
        
        res.json({
            success: true,
            message: 'UAE Webhook deleted successfully',
        });
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to delete UAE webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to delete UAE webhook',
            message: error.message,
        });
    }
});

/**
 * GET /api/uae/webhooks-stats
 * Get UAE webhook delivery statistics
 */
router.get('/uae/webhooks-stats', async (req, res) => {
    try {
        const stats = uaeWebhookManager.getStats();
        res.json({
            success: true,
            data: stats,
        });
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to fetch UAE stats', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch UAE stats',
        });
    }
});

/**
 * GET /api/uae/webhook-logs
 * Get UAE webhook delivery logs
 */
router.get('/uae/webhook-logs', async (req, res) => {
    try {
        const db = await getUAEDatabase();
        const logsCollection = db.collection('webhook_delivery_logs');
        
        const { webhook_uuid, status, limit = 100, skip = 0 } = req.query;
        const filter = {};
        if (webhook_uuid) filter.webhookUuid = webhook_uuid;
        if (status) filter.status = status;
        
        const logs = await logsCollection
            .find(filter)
            .sort({ timestamp: -1 })
            .skip(parseInt(skip))
            .limit(parseInt(limit))
            .toArray();
        
        const total = await logsCollection.countDocuments(filter);
        
        res.json({
            success: true,
            count: logs.length,
            total,
            data: logs,
        });
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] Failed to fetch UAE logs', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch UAE logs',
        });
    }
});

/**
 * POST /api/uae/webhooks/:id/test
 * Manually test a UAE webhook
 */
router.post('/uae/webhooks/:id/test', async (req, res) => {
    try {
        const db = await getUAEDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        let queryId = null;
        try { queryId = new ObjectId(req.params.id); } catch (e) {}
        
        let webhook = await webhooksCollection.findOne({ 
            $or: [{ uuid: req.params.id }, { _id: queryId }] 
        });
        
        if (!webhook) {
            return res.status(404).json({
                success: false,
                error: 'UAE Webhook not found',
            });
        }
        
        const sampleCdr = (req.body && Object.keys(req.body).length > 0) 
            ? (req.body.cdr || req.body) 
            : {
                uuid: "uae-test-uuid-" + Date.now(),
                caller_id_number: "97112345678",
                destination_number: "97187654321",
                duration: "60",
                tenant_uuid: webhook.tenant_uuid || "uae-test-tenant",
                is_test: true
            };
        
        const config = {
          uuid: webhook.uuid,
          url: webhook.webhook_url,
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: 1,
          retryDelay: 1000,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "UAE Test Webhook",
          bearerToken: webhook.bearer_token || null,
        };
        
        const result = await uaeWebhookManager.deliverToWebhook(config, sampleCdr);
        res.json(result);
        
    } catch (error) {
        logger.error('[UAE_WEBHOOK_API] UAE Manual test failed', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to execute UAE manual test',
        });
    }
});

export default router;
