/**
 * Webhook Delivery Consumer
 * Consumes webhook tasks from RabbitMQ and delivers CDRs via WebhookManager
 */

import rabbitMQ from "../rabbitmq.js";
import config from "../config.js";
import logger from "../logger.js";
import webhookManager from "./webhook-manager.js";

class WebhookConsumer {
  constructor() {
    this.isConsuming = false;
    this.processedCount = 0;
    this.failedCount = 0;
  }

  /**
   * Start consuming webhook tasks
   */
  async start() {
    try {
      if (this.isConsuming) {
        logger.warn("[WEBHOOK_CONSUMER] Already consuming messages");
        return;
      }

      const channel = rabbitMQ.getChannel();

      logger.info("[WEBHOOK_CONSUMER] Starting webhook task consumption", {
        queue: config.webhooks.queue,
      });

      // Start consuming
      await channel.consume(
        config.webhooks.queue,
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
      logger.success("[WEBHOOK_CONSUMER] Webhook consumer started successfully");
    } catch (error) {
      logger.error("[WEBHOOK_CONSUMER] Failed to start webhook consumer", {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Process individual webhook task
   */
  async processMessage(msg, channel) {
    let task;
    try {
      // Parse task
      task = JSON.parse(msg.content.toString());

      logger.debug("[WEBHOOK_CONSUMER] Processing task", {
        cdrUuid: task.cdrUuid,
        messageId: task.messageId,
      });

      // Execute webhook delivery via manager
      await webhookManager.processWebhookTask(task);

      // Acknowledge message
      channel.ack(msg);
      this.processedCount++;

    } catch (error) {
      this.failedCount++;
      logger.error("[WEBHOOK_CONSUMER] Task processing failed", {
        error: error.message,
        cdrUuid: task?.cdrUuid,
      });

      // Reject to DLQ if max retries reached or on fatal error
      // RabbitMQ will handle retries if we nack with requeue=true
      // But since we use TTL for initial delay, we should handle custom retries here if needed
      // For now, let's reject to DLQ
      channel.reject(msg, false); 
    }
  }

  /**
   * Stop consuming
   */
  async stop() {
    this.isConsuming = false;
    logger.info("[WEBHOOK_CONSUMER] Webhook consumer stopped");
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
const webhookConsumer = new WebhookConsumer();
export default webhookConsumer;
