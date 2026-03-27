 
/**
 * Webhook Manager
 * Handles CDR webhook delivery with retry logic and error handling
 * Supports multiple webhook endpoints with individual configurations
 */

import axios from "axios";
import https from "https";
import logger from "../logger.js";
import { getDatabase } from "../database.js";

class WebhookManager {
  constructor() {
    this.deliveryStats = {
      total: 0,
      success: 0,
      failed: 0,
    };
  }

  /**
   * Deliver CDR to all active webhooks for a tenant
   * @param {Object} cdr - CDR data to send
   * @param {string} tenantUuid - Tenant filter
   */
  async deliverCDR(cdr, tenantUuid = null) {
    try {
      const db = getDatabase();
      const webhooksCollection = db.collection("web_hook");

      // Build filter
      const filter = {
        is_active: true,
        webhook_type: "cdr",
      };

      if (tenantUuid) {
        filter.tenant_uuid = tenantUuid;
      }

      // Fetch matching webhooks directly from DB
      const webhooks = await webhooksCollection.find(filter).toArray();

      if (webhooks.length === 0) {
        logger.debug("[WEBHOOK] No matching active webhooks found", { tenantUuid });
        return;
      }

      logger.debug("[WEBHOOK] Delivering CDR to webhooks", {
        cdrUuid: cdr.uuid,
        webhookCount: webhooks.length,
      });

      // Prepare webhook configs and deliver in parallel
      const deliveryPromises = webhooks.map((webhook) => {
        const config = {
          uuid: webhook.uuid,
          url: (webhook.webhook_url || "").trim(),
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: webhook.max_retries || 3,
          retryDelay: webhook.retry_delay || 2000,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "Unnamed Webhook",
          bearerToken: webhook.bearer_token || null,
        };
        return this.deliverToWebhook(config, cdr);
      });

      await Promise.allSettled(deliveryPromises);
    } catch (error) {
      logger.error("[WEBHOOK] Delivery process failed", {
        error: error.message,
        tenantUuid,
      });
    }
  }

  /**
   * Process a webhook delivery task from the queue
   * @param {Object} task - { cdrUuid, collectionName, tenantUuid }
   */
  async processWebhookTask(task) {
    const { cdrUuid, collectionName, tenantUuid } = task;

    try {
      logger.info('[WEBHOOK] Processing task from queue', {
        cdrUuid,
        collectionName,
        tenantUuid,
      });

      // Fetch fresh CDR data from database
      const db = getDatabase();
      const collection = db.collection(collectionName);
      
      const freshCdr = await collection.findOne({ uuid: cdrUuid });

      if (!freshCdr) {
        logger.warn('[WEBHOOK] CDR not found in database, skipping delivery', {
          cdrUuid,
          collectionName,
        });
        return;
      }

      // Deliver fresh CDR to webhooks
      await this.deliverCDR(freshCdr, tenantUuid);

    } catch (error) {
      logger.error('[WEBHOOK] Queue task processing failed', {
        error: error.message,
        stack: error.stack,
        cdrUuid,
        collectionName,
      });
      throw error; // Rethrow to allow RabbitMQ retry
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
          webhookUrl: webhook.url,
          attempt,
          maxRetries: webhook.retries,
          cdrUuid: cdr.uuid,
        });

        // Prepare headers
        const headers = {
          "Content-Type": "application/json",
          ...webhook.headers,
        };

        // Add Bearer Token if present
        if (webhook.bearerToken) {
          headers["Authorization"] = `Bearer ${webhook.bearerToken}`;
        }

        // Debug log for headers (masking sensitive data)
        const loggedHeaders = { ...headers };
        if (loggedHeaders["Authorization"]) {
          loggedHeaders["Authorization"] = "Bearer [MASKED]";
        }
        
        logger.debug("[WEBHOOK] Request details", {
          url: webhook.url,
          method: webhook.method,
          headers: loggedHeaders,
          data: cdr, // Add full CDR payload for inspection
        });

        const response = await axios({
          method: webhook.method,
          url: webhook.url,
          data: cdr,
          headers: headers,
          timeout: webhook.timeout,
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          validateStatus: (status) => {
            return status === 200;
          },
        });

        const duration = Date.now() - startTime;

        logger.info("[WEBHOOK] Delivery successful", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          webhookUrl: webhook.url,
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

        return {
          success: true,
          webhook: webhook.name,
          responseData: response.data,
          statusCode: response.status,
          statusText: response.statusText,
          durationMs: duration
        };
      } catch (error) {
        lastError = error;
        
        // Log basic error message safely without circular references
        logger.warn(`[WEBHOOK] Delivery attempt ${attempt} failed: ${error.message}`);

         lastErrorDetails  = {
          message: error.message,
          code: error.code,
          statusCode: error.response?.status,
          statusText: error.response?.statusText,
          responseData: error.response?.data,
          headers: error.response?.headers,
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
    return {
      success: false,
      webhook: webhook.name,
      error: lastError.message,
      responseData: lastError.response?.data,
      statusCode: lastError.response?.status,
      statusText: lastError.response?.statusText,
      durationMs: duration
    };
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
    };
  }

}

// Export singleton instance
const webhookManager = new WebhookManager();
export default webhookManager;