 
/**
 * Webhook Manager
 * Handles CDR webhook delivery with retry logic and error handling
 * Supports multiple webhook endpoints with individual configurations
 */

import axios from "axios";
import logger from "../logger.js";
import { getDatabase } from "../database.js";

class WebhookManager {
  constructor() {
    this.activeWebhooks = new Map();
    this.deliveryStats = {
      total: 0,
      success: 0,
      failed: 0,
    };
  }

  /**
   * Initialize webhooks from database
   */
  async initialize() {
    try {
      logger.info("[WEBHOOK] Initializing webhook manager...");

      const db = getDatabase();
      const webhooksCollection = db.collection("web_hook");

      // Fetch active webhooks
      const webhooks = await webhooksCollection
        .find({
          is_active: true,
          webhook_type: "cdr", // Filter for CDR webhooks only
        })
        .toArray();

      // Store active webhooks in memory
      webhooks.forEach((webhook) => {
        this.activeWebhooks.set(webhook._id.toString(), {
          uuid: webhook.uuid,
          url: webhook.webhook_url,
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: webhook.max_retries || 3,
          retryDelay: webhook.retry_delay || 2000,
          tenantId: webhook.tenant_id,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "Unnamed Webhook",
        });
      });

      logger.success(
        `[WEBHOOK] Loaded ${this.activeWebhooks.size} active webhooks`
      );
    } catch (error) {
      logger.error("[WEBHOOK] Failed to initialize webhooks", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Reload webhooks from database (call this periodically or on webhook changes)
   */
  async reloadWebhooks() {
    try {
      logger.info("[WEBHOOK] Reloading webhooks...");
      this.activeWebhooks.clear();
      await this.initialize();
    } catch (error) {
      logger.error("[WEBHOOK] Failed to reload webhooks", {
        error: error.message,
      });
    }
  }

  /**
   * Deliver CDR to all active webhooks
   * @param {Object} cdr - CDR data to send
   * @param {string} tenantId - Optional tenant filter
   */
  async deliverCDR(cdr, tenantUuid = null) {
    if (this.activeWebhooks.size === 0) {
      logger.debug("[WEBHOOK] No active webhooks configured");
      return;
    }

    const webhooksToDeliver = Array.from(this.activeWebhooks.values()).filter(
      (webhook) => !tenantUuid || webhook.tenantUuid === tenantUuid
    );

    if (webhooksToDeliver.length === 0) {
      logger.debug("[WEBHOOK] No webhooks match tenant filter", { tenantUuid });
      return;
    }

    logger.debug("[WEBHOOK] Delivering CDR to webhooks", {
      cdrUuid: cdr.cdrUuid,
      webhookCount: webhooksToDeliver.length,
    });

    // Deliver to all webhooks in parallel
    const deliveryPromises = webhooksToDeliver.map((webhook) =>
      this.deliverToWebhook(webhook, cdr)
    );

    await Promise.allSettled(deliveryPromises);
  }

  /**
   * Schedule delayed webhook delivery with fresh data fetch
   * @param {string} cdrUuid - CDR UUID to fetch
   * @param {string} collectionName - Collection to fetch from
   * @param {string} tenantUuid - Optional tenant filter
   * @param {number} delayMs - Delay in milliseconds (default 1 minutes)
   */
  async scheduleDelayedDelivery(cdrUuid, collectionName, tenantUuid = null, delayMs = 300000) {
    try {
      logger.debug('⏰[WEBHOOK] Scheduling delayed delivery', {
        cdrUuid,
        collectionName,
        tenantUuid,
        delayMinutes: delayMs / 60000,
      });

      console.log(`⏰ [WEBHOOK] Scheduled delivery for CDR ${cdrUuid} in ${delayMs / 60000} minutes`);

      // Wait for the specified delay
      await new Promise(resolve => setTimeout(resolve, delayMs));

      logger.info('[WEBHOOK] Fetching fresh CDR data for delivery', {
        cdrUuid,
        collectionName,
      });

      console.log(`🔄 [WEBHOOK] Fetching fresh data for CDR ${cdrUuid}`);

      // Fetch fresh CDR data from database
      const db = getDatabase();
      const collection = db.collection(collectionName);
      
      const freshCdr = await collection.findOne({ uuid: cdrUuid });

      if (!freshCdr) {
        logger.warn('[WEBHOOK] CDR not found, skipping delivery', {
          cdrUuid,
          collectionName,
        });
        console.log(`⚠️  [WEBHOOK] CDR ${cdrUuid} not found in database`);
        return;
      }

      logger.info('[WEBHOOK] Fresh CDR data fetched, delivering to webhooks', {
        cdrUuid,
        collectionName,
      });

      console.log(`📤 [WEBHOOK] Delivering fresh CDR ${cdrUuid} to webhooks`);

      // Deliver fresh CDR to webhooks
      await this.deliverCDR(freshCdr, tenantUuid);

    } catch (error) {
      logger.error('[WEBHOOK] Delayed delivery failed', {
        error: error.message,
        stack: error.stack,
        cdrUuid,
        collectionName,
      });
      console.error(`❌ [WEBHOOK] Failed to deliver CDR ${cdrUuid}: ${error.message}`);
    }
  }

  /**
   * Deliver CDR to a specific webhook with retry logic
   * @param {Object} webhook - Webhook configuration
   * @param {Object} cdr - CDR data
   */
  async deliverToWebhook(webhook, cdr) {
    const startTime = Date.now();
    let lastError = null;
    let lastErrorDetails = null;

    for (let attempt = 1; attempt <= webhook.retries; attempt++) {
      try {
        logger.debug("[WEBHOOK] Attempting delivery", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          attempt,
          maxRetries: webhook.retries,
          cdrUuid: cdr.uuid,
        });
        console.log(webhook, "webhook");
        const response = await axios({
          method: webhook.method,
          url: webhook.url,
          data: cdr,
          headers: {
            "Content-Type": "application/json",
            "User-Agent": "CDR-Webhook-Service/1.0",
            ...webhook.headers,
          },
          timeout: webhook.timeout,
          validateStatus: (status) => {
            return status === 200;
          },
        });

        const duration = Date.now() - startTime;

        logger.info("[WEBHOOK] Delivery successful", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          cdrUuid: cdr.uuid,
          statusCode: response.status,
          durationMs: duration,
          attempt,
        });

        this.deliveryStats.total++;
        this.deliveryStats.success++;

        // Log successful delivery to database
        await this.logWebhookDelivery({
          webhookUuid: webhook.uuid,
          webhookName: webhook.name,
          cdrUuid: cdr.uuid,
          status: "success",
          statusCode: response.status,
          attempt,
          duration,
        });

        return { success: true, webhook: webhook.name };
      } catch (error) {
        lastError = error;

         lastErrorDetails  = {
          message: error.message,
          code: error.code,
          statusCode: error.response?.status,
          statusText: error.response?.statusText,
          responseData: error.response?.data,
          isTimeout: error.code === "ECONNABORTED",
          isNetworkError:
            error.code === "ENOTFOUND" || error.code === "ECONNREFUSED",
          url: webhook.url,
        };

        logger.warn("[WEBHOOK] Delivery attempt failed", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          cdrUuid: cdr.uuid,
          attempt,
          maxRetries: webhook.retries,
          error: lastErrorDetails,
        });

        // Wait before retry (except on last attempt)
        if (attempt < webhook.retries) {
          const delay = webhook.retryDelay * attempt; // Exponential backoff
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All retries failed
    const duration = Date.now() - startTime;

    logger.error("[WEBHOOK] Delivery failed after all retries", lastErrorDetails);

    this.deliveryStats.total++;
    this.deliveryStats.failed++;

// Log failed delivery to database
    console.log("💾 [WEBHOOK] Saving failure to database...");
    
    try {
      await this.logWebhookDelivery({
        webhookUuid: webhook.uuid,
        webhookName: webhook.name,
        cdrUuid: cdr.uuid,
        status: "failed",
        statusCode: lastError.response?.status || 0,
        attempt: webhook.retries,
        duration,
        error: lastError.message,
        errorCode: lastError.code,
        errorType: lastError.name,
        statusText: lastError.response?.statusText,
        responseData: lastError.response?.data,
      });
      
      console.log("✅ [WEBHOOK] Failure logged to database");
    } catch (dbError) {
      console.error("❌ [WEBHOOK] Database save failed:", dbError.message);
    }
    return { success: false, webhook: webhook.name, error: lastError.message };
  }

  /**
   * Log webhook delivery to database
   * @param {Object} deliveryInfo - Delivery information
   */
  async logWebhookDelivery(deliveryInfo) {
    try {
      const db = getDatabase();
      const logsCollection = db.collection("webhook_delivery_logs");

      await logsCollection.insertOne({
        ...deliveryInfo,
        timestamp: new Date().toISOString(),
        createdAt: new Date(),
      });
    } catch (error) {
      logger.error("[WEBHOOK] Failed to log delivery", {
        error: error.message,
        deliveryInfo,
      });
    }
  }

  /**
   * Get delivery statistics
   */
  getStats() {
    const successRate =
      this.deliveryStats.total > 0
        ? (
            (this.deliveryStats.success / this.deliveryStats.total) *
            100
          ).toFixed(2)
        : "0.00";

    return {
      ...this.deliveryStats,
      successRate: `${successRate}%`,
      activeWebhooks: this.activeWebhooks.size,
    };
  }

  /**
   * Get list of active webhooks
   */
  getActiveWebhooks() {
    return Array.from(this.activeWebhooks.values()).map((webhook) => ({
      uuid: webhook.uuid,
      name: webhook.name,
      url: webhook.url,
      method: webhook.method,
      tenantUuid: webhook.tenantUuid,
    }));
  }
}

// Export singleton instance
const webhookManager = new WebhookManager();
export default webhookManager;