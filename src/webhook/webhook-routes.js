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

const router = express.Router();

/**
 * GET /api/webhooks
 * Get all webhooks
 */
router.get('/webhooks', async (req, res) => {
    try {
        const db = getDatabase();
        const webhooksCollection = db.collection('web_hook');
        
        const { tenant_uuid, tenant_id, is_active, webhook_type } = req.query;
        
        const filter = {};
        // Support both tenant_uuid (preferred) and tenant_id (fallback)
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
            created_at: new Date(),
            updated_at: new Date(),
        };
        
        const result = await webhooksCollection.insertOne(newWebhook);
        
        // Reload webhooks in memory
        await webhookManager.reloadWebhooks();
        
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
        
        // Reload webhooks in memory
        await webhookManager.reloadWebhooks();
        
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
        
        // Reload webhooks in memory
        await webhookManager.reloadWebhooks();
        
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
        const activeWebhooks = webhookManager.getActiveWebhooks();
        
        res.json({
            success: true,
            data: {
                ...stats,
                webhooks: activeWebhooks,
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
 * Get webhook delivery logs
 */
router.get('/webhook-logs', async (req, res) => {
    try {
        const db = getDatabase();
        const logsCollection = db.collection('webhook_delivery_logs');
        
        const { webhook_id, webhook_uuid, status, limit = 100, skip = 0 } = req.query;
        
        const filter = {};
        if (webhook_uuid) filter.webhookUuid = webhook_uuid;
        else if (webhook_id) filter.webhookId = webhook_id;
        
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
 * POST /api/webhooks-reload
 * Reload webhooks from database
 */
router.post('/webhooks-reload', async (req, res) => {
    try {
        await webhookManager.reloadWebhooks();
        
        logger.info('[WEBHOOK_API] Webhooks reloaded');
        
        res.json({
            success: true,
            message: 'Webhooks reloaded successfully',
            activeWebhooks: webhookManager.getActiveWebhooks().length,
        });
        
    } catch (error) {
        logger.error('[WEBHOOK_API] Failed to reload webhooks', {
            error: error.message,
        });
        res.status(500).json({
            success: false,
            error: 'Failed to reload webhooks',
            message: error.message,
        });
    }
});

export default router;