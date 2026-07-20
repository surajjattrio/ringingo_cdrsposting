/**
 * Webhook Routes
 * REST API endpoints for managing webhooks
 * Add these routes to your Express server
 */

import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { getDatabase } from '../database.js';
import webhookManager from './webhook-manager.js';
import logger from '../logger.js';
import { ObjectId } from 'mongodb';
import rabbitMQ from '../rabbitmq.js';
import config from '../config.js';

const router = express.Router();
/**
 * POST /api/external/webhook-only
 * Trigger webhook delivery directly WITHOUT saving to database
 */
router.post('/external/webhook-only', async (req, res) => {
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
        const webhookTask = {
            cdrUuid: cdrUuid,
            collectionName: 'cdrs',
            tenantUuid: data.tenant_uuid || null,
            fullCdr: data, // Include the full payload
            timestamp: new Date().toISOString(),
            messageId: `webhook_only_${cdrUuid}_${Date.now()}`
        };

        // Publish directly to the webhook delivery queue using the system's RabbitMQ manager
        await rabbitMQ.publish(
            webhookTask,
            {
                expiration: config.webhooks.delay?.toString() || "60000",
                type: 'webhook_delivery_task'
            },
            config.webhooks.queue
        );

        logger.info('[EXTERNAL_WEBHOOK] Webhook-only task queued', {
            cdrUuid,
            messageId: webhookTask.messageId,
            remoteIp: req.ip
        });

        res.json({
            success: true,
            message: 'Webhook-only delivery task queued successfully (Bypassed Database)',
            messageId: webhookTask.messageId,
            cdrUuid: cdrUuid
        });

    } catch (error) {
        logger.error('[EXTERNAL_WEBHOOK] Failed to process webhook-only push', {
            error: error.message,
            stack: error.stack
        });
        res.status(500).json({
            success: false,
            error: 'Internal system error while queuing webhook task',
            message: error.message,
        });
    }
});

/**
 * POST /api/external/vb-webhook
 * Called by cdrs_posting.py after writing voicebroadcast_details to MongoDB.
 * Delivers voice broadcast call data to all active webhooks for the tenant
 * with webhook_type = "voice_broadcast".
 */
router.post('/external/vb-webhook', async (req, res) => {
    try {
        const data = req.body;

        if (!data || Object.keys(data).length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Body data is required and cannot be empty',
            });
        }

        const vbUuid = data.uuid;
        const tenantUuid = data.tenant_uuid;

        if (!tenantUuid) {
            return res.status(400).json({
                success: false,
                error: 'tenant_uuid is required',
            });
        }

        logger.info('[VB_WEBHOOK] Received voice broadcast webhook request', {
            vbUuid,
            tenantUuid,
            type: data.type,
        });

        const webhookTask = {
            type: 'vb_webhook_delivery',
            vbUuid,
            tenantUuid,
            timestamp: new Date().toISOString(),
            messageId: `vb_webhook_${vbUuid}_${Date.now()}`,
        };

        await rabbitMQ.publish(
            webhookTask,
            {
                expiration: config.webhooks.delay?.toString() || "60000",
                type: 'vb_webhook_delivery_task',
            },
            config.webhooks.queue
        );

        logger.info('[VB_WEBHOOK] Voice broadcast webhook task queued', {
            vbUuid,
            messageId: webhookTask.messageId,
        });

        return res.status(200).json({
            success: true,
            message: 'Voice broadcast webhook delivery queued',
            messageId: webhookTask.messageId,
            vbUuid,
        });

    } catch (error) {
        logger.error('[VB_WEBHOOK] Failed to process voice broadcast webhook', {
            error: error.message,
            stack: error.stack,
        });
        return res.status(500).json({
            success: false,
            error: 'Internal error while delivering voice broadcast webhook',
            message: error.message,
        });
    }
});

/**
 * GET /api/webhooks
 * Get all webhooks — ALWAYS scoped to a tenant, same reasoning as /webhook-logs:
 * the tenant boundary is enforced here, not just at the proxying API layer, so this
 * endpoint can never be called unfiltered and leak another tenant's webhook configs.
 */
router.get('/webhooks', async (req, res) => {
    try {
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');

        const {
            tenant_uuid,
            tenant_id,
            is_active,
            webhook_type,
            name,
            transform_strategy,
            method,
        } = req.query;

        // Support both tenant_uuid (preferred) and tenant_id (fallback) — but one of
        // them is required. filter.tenant_uuid is set first and never reassigned
        // below, so every optional filter after it can only narrow within this
        // tenant's own webhooks, never widen across tenants.
        const resolvedTenantUuid = tenant_uuid || tenant_id;
        if (!resolvedTenantUuid) {
            return res.status(400).json({
                success: false,
                error: 'tenant_uuid is required',
            });
        }

        const filter = { tenant_uuid: resolvedTenantUuid };

        if (is_active !== undefined) filter.is_active = is_active === 'true';
        if (webhook_type) filter.webhook_type = webhook_type;
        if (transform_strategy) filter.transform_strategy = transform_strategy;
        if (method) filter.method = method;

        if (name) {
            // Case-insensitive contains match. Escape regex metacharacters in the raw
            // user input so it's treated as a literal substring, not a regex pattern.
            const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            filter.name = { $regex: escapedName, $options: 'i' };
        }

        const webhooks = await webhooksCollection.find(filter).toArray();

        res.json({
            success: true,
            count: webhooks.length,
            data: webhooks,
        });

    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to fetch webhooks', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch webhooks',
            message: error.message,
        });
    }
});

/**
 * GET /api/webhooks/:id
 * Get a specific webhook by uuid or _id
 */
router.get('/webhooks/:id', async (req, res) => {
    try {
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        // Try to find by uuid first, then by _id
        let webhook = await webhooksCollection.findOne({ uuid: req.params.id });
        
        if (!webhook) {
            try {
                webhook = await webhooksCollection.findOne({ _id: new ObjectId(req.params.id) });
            } catch (e) {
                // Invalid ObjectId format
            }
        }
        
        if (!webhook) {
            return res.status(404).json({
                success: false,
                error: 'Webhook not found',
            });
        }
        
        res.json({
            success: true,
            data: webhook,
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to fetch webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch webhook',
            message: error.message,
        });
    }
});

/**
 * POST /api/webhooks
 * Create a new webhook
 */
router.post('/webhooks', async (req, res) => {
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
            tenant_id, // Fallback support
            bearer_token = null,
            transform_strategy = "legacy",
            transform_template = null,
            wrapInJson = false,
            wrapperKey = null,
            wrapperType = "object",
            // Delivery post-delay (ms) — how long to wait after the CDR is written
            // before posting THIS webhook, so late-set fields (disposition, comment)
            // set by the agent after hangup are captured. Applied per-webhook inside
            // WebhookManager#deliverCDR (each active webhook waits its own delay and
            // re-fetches the CDR fresh afterward), not shared across a tenant's other
            // webhooks.
            delay = 120000,
            stripEmpty = false,
        } = req.body;

        // Validation
        if (!name || !webhook_url) {
            return res.status(400).json({
                success: false,
                error: 'Name and webhook_url are required',
            });
        }
        
        // Validate URL format
        try {
            new URL(webhook_url);
        } catch {
            return res.status(400).json({
                success: false,
                error: 'Invalid webhook URL format',
            });
        }
        
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        // Generate UUID
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
            tenant_uuid: tenant_uuid || tenant_id || null, // Prefer tenant_uuid
            bearer_token,
            transform_strategy,
            transform_template,
            wrapInJson: !!wrapInJson,
            wrapperKey: wrapperKey || null,
            wrapperType: wrapperType || "object",
            // Stored as a Number regardless of what's sent — some existing documents
            // have this as a string (e.g. "120000"), which still works with setTimeout
            // (implicit coercion) but is worth normalizing going forward.
            delay: Number(delay) || 120000,
            stripEmpty: !!stripEmpty,
            created_at: new Date(),
            updated_at: new Date(),
        };
        
        const result = await webhooksCollection.insertOne(newWebhook);
        
        logger.info('[WEBHOOK_API] Webhook created', {
            webhookUuid,
            webhookId: result.insertedId,
            name,
            webhook_url,
        });
        
        res.status(201).json({
            success: true,
            message: 'Webhook created successfully',
            data: {
                _id: result.insertedId,
                uuid: webhookUuid,
                ...newWebhook,
            },
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to create webhook', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to create webhook',
            message: error.message,
        });
    }
});

/**
 * PUT /api/webhooks/:id
 * Update an existing webhook (by uuid or _id)
 */
router.put('/webhooks/:id', async (req, res) => {
    try {
        const {
            name,
            webhook_url,
            webhook_type,
            method,
            headers,
            timeout,
            max_retries,
            retry_delay,
            is_active,
            tenant_uuid,
            tenant_id, // Fallback support
            bearer_token,
            transform_strategy,
            transform_template,
            wrapInJson,
            wrapperKey,
            wrapperType,
            delay,
            stripEmpty,
        } = req.body;

        const updates = {};
        if (name !== undefined) updates.name = name;
        if (webhook_url !== undefined) {
            // Validate URL format
            try {
                new URL(webhook_url);
                updates.webhook_url = webhook_url;
            } catch {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid webhook URL format',
                });
            }
        }
        if (webhook_type !== undefined) updates.webhook_type = webhook_type;
        if (method !== undefined) updates.method = method.toUpperCase();
        if (headers !== undefined) updates.headers = headers;
        if (timeout !== undefined) updates.timeout = timeout;
        if (max_retries !== undefined) updates.max_retries = max_retries;
        if (retry_delay !== undefined) updates.retry_delay = retry_delay;
        if (is_active !== undefined) updates.is_active = is_active;
        if (tenant_uuid !== undefined) updates.tenant_uuid = tenant_uuid;
        else if (tenant_id !== undefined) updates.tenant_uuid = tenant_id;
        if (bearer_token !== undefined) updates.bearer_token = bearer_token;
        if (transform_strategy !== undefined) updates.transform_strategy = transform_strategy;
        if (transform_template !== undefined) updates.transform_template = transform_template;
        if (wrapInJson !== undefined) updates.wrapInJson = !!wrapInJson;
        if (wrapperKey !== undefined) updates.wrapperKey = wrapperKey || null;
        if (wrapperType !== undefined) updates.wrapperType = wrapperType || "object";
        if (delay !== undefined) updates.delay = Number(delay) || 120000;
        if (stripEmpty !== undefined) updates.stripEmpty = !!stripEmpty;

        updates.updated_at = new Date();
        
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        // Try to find by uuid first, then by _id
        let filter = { uuid: req.params.id };
        let result = await webhooksCollection.updateOne(filter, { $set: updates });
        
        if (result.matchedCount === 0) {
            try {
                filter = { _id: new ObjectId(req.params.id) };
                result = await webhooksCollection.updateOne(filter, { $set: updates });
            } catch (e) {
                // Invalid ObjectId format
            }
        }
        
        if (result.matchedCount === 0) {
            return res.status(404).json({
                success: false,
                error: 'Webhook not found',
            });
        }
        
        logger.info('[WEBHOOK_API] Webhook updated', {
            webhookId: req.params.id,
            updates,
        });
        
        res.json({
            success: true,
            message: 'Webhook updated successfully',
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to update webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to update webhook',
            message: error.message,
        });
    }
});

/**
 * DELETE /api/webhooks/:id
 * Delete a webhook (by uuid or _id)
 */
router.delete('/webhooks/:id', async (req, res) => {
    try {
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        // Try to delete by uuid first, then by _id
        let result = await webhooksCollection.deleteOne({ uuid: req.params.id });
        
        if (result.deletedCount === 0) {
            try {
                result = await webhooksCollection.deleteOne({ _id: new ObjectId(req.params.id) });
            } catch (e) {
                // Invalid ObjectId format
            }
        }
        
        if (result.deletedCount === 0) {
            return res.status(404).json({
                success: false,
                error: 'Webhook not found',
            });
        }
        
        logger.info('[WEBHOOK_API] Webhook deleted', {
            webhookId: req.params.id,
        });
        
        res.json({
            success: true,
            message: 'Webhook deleted successfully',
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to delete webhook', {
            error: error.message,
            webhookId: req.params.id,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to delete webhook',
            message: error.message,
        });
    }
});


/**
 * GET /api/webhooks-stats
 * Get webhook delivery statistics
 */
router.get('/webhooks-stats', async (req, res) => {
    try {
        const stats = webhookManager.getStats();
        
        res.json({
            success: true,
            data: {
                ...stats,
            },
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to fetch webhook stats', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch webhook stats',
            message: error.message,
        });
    }
});

/**
 * GET /api/webhook-logs
 * Get webhook delivery logs — ALWAYS scoped to a tenant. The tenant boundary is
 * enforced here, not just at the proxying API layer, so this endpoint can never
 * be called unfiltered and leak another tenant's delivery logs.
 */
router.get('/webhook-logs', async (req, res) => {
    try {
        const db = getDatabase();
        const logsCollection = db.collection('webhook_delivery_logs');

        const {
            webhook_id,
            webhook_uuid,
            status,
            webhook_type,
            status_code,
            from_date,
            to_date,
            tenant_uuid,
            limit = 100,
            skip = 0,
        } = req.query;

        if (!tenant_uuid) {
            return res.status(400).json({
                success: false,
                error: 'tenant_uuid is required',
            });
        }

        // tenant_uuid is set first and never reassigned below — every optional filter
        // below is ANDed on top of it, narrowing within the tenant's own logs only.
        // It can never be widened or overridden by any other query param.
        const filter = { tenant_uuid };
        if (webhook_uuid) filter.webhookUuid = webhook_uuid;
        else if (webhook_id) filter.webhookId = webhook_id;

        if (status) filter.status = status;
        if (webhook_type) filter.webhook_type = webhook_type;

        if (status_code) {
            const parsedStatusCode = parseInt(status_code, 10);
            if (!isNaN(parsedStatusCode)) filter.statusCode = parsedStatusCode;
        }

        if (from_date || to_date) {
            filter.createdAt = {};
            if (from_date) filter.createdAt.$gte = new Date(from_date);
            if (to_date) filter.createdAt.$lte = new Date(to_date);
        }

        const logs = await logsCollection
            .find(filter)
            .sort({ createdAt: -1 })
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
        logger.error('[WEBHOOK_API] Failed to fetch webhook logs', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to fetch webhook logs',
            message: error.message,
        });
    }
});

/**
 * POST /api/webhooks/:id/test
 * Manually test a webhook with a sample CDR
 */
router.post('/webhooks/:id/test', async (req, res) => {
    try {
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        let queryId = null;
        try {
            queryId = new ObjectId(req.params.id);
        } catch (e) {}
        
        let webhook = await webhooksCollection.findOne({ 
            $or: [{ uuid: req.params.id }, { _id: queryId }] 
        });
        
        if (!webhook) {
            return res.status(404).json({
                success: false,
                error: 'Webhook not found',
            });
        }
        
        // Use the payload from request body (nested or flat) if provided, otherwise fallback to a sample CDR
        const sampleCdr = (req.body && Object.keys(req.body).length > 0) 
            ? (req.body.cdr || req.body) 
            : {
                uuid: "test-uuid-" + Date.now(),
                caller_id_number: "1234567890",
                destination_number: "9876543210",
                duration: "60",
                tenant_uuid: webhook.tenant_uuid || "test-tenant-uuid",
                is_test: true,
                companyId: "TEST_COMP_001" // Include common required fields
            };
        
        const config = {
          uuid: webhook.uuid,
          url: webhook.webhook_url,
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: 1, // Only try once for manual tests
          retryDelay: 1000,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "Test Webhook",
          bearerToken: webhook.bearer_token || null,
        };
        
        logger.info('[WEBHOOK_API] Triggering manual test delivery', {
            webhookUuid: webhook.uuid,
            url: webhook.webhook_url
        });
        
        // Use the result logic from deliverToWebhook
        const result = await webhookManager.deliverToWebhook(config, sampleCdr);
        
        if (result.success) {
            res.json({
                success: true,
                message: 'Test delivery successful.',
                targetUrl: webhook.webhook_url,
                cdr: sampleCdr,
                serverResponse: {
                    status: result.statusCode,
                    statusText: result.statusText,
                    data: result.responseData,
                    durationMs: result.durationMs
                }
            });
        } else {
             logger.warn('[WEBHOOK_API] Manual test delivery failed', {
                webhookUuid: webhook.uuid,
                error: result.error
             });
             
             res.status(200).json({ // Return 200 with success: false
                success: false,
                message: 'Test delivery failed. Server responded with an error.',
                error: result.error,
                details: {
                    statusCode: result.statusCode,
                    statusText: result.statusText,
                    responseData: result.responseData,
                    durationMs: result.durationMs
                }
            });
        }
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Manual test failed', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to execute manual test',
            message: error.message,
        });
    }
});

export default router;