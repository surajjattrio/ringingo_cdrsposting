/**
 * UAE Webhook Delivery Consumer
 * Consumes UAE-specific webhook tasks from RabbitMQ and delivers CDRs via UAEWebhookManager
 */

import rabbitMQ from "../rabbitmq.js";
import config from "../config.js";
import logger from "../logger.js";
import uaeWebhookManager from "./webhook-uae-manager.js";

class UAEWebhookConsumer {
  constructor() {
    this.isConsuming = false;
    this.processedCount = 0;
    this.failedCount = 0;
    this.channel = null;
    this.consumerTag = null;
  }

  /**
   * Start consuming UAE webhook tasks
   */
  async start() {
    try {
      if (this.isConsuming) {
        logger.warn("[UAE_WEBHOOK_CONSUMER] Already consuming messages");
        return;
      }

      const channel = rabbitMQ.getChannel();
      const uaeQueue = "_uae_" + config.webhooks.queue;

      if (channel === this.channel && this.consumerTag) {
        logger.warn("[UAE_WEBHOOK_CONSUMER] start() called again for the same channel - skipping");
        return;
      }

      this.channel = channel;

      logger.info("[UAE_WEBHOOK_CONSUMER] Starting UAE webhook task consumption", {
        queue: uaeQueue,
      });

      // Start consuming
      const { consumerTag } = await channel.consume(
        uaeQueue,
        async (msg) => {
          if (msg !== null) {
            await this.processMessage(msg, channel);
          }
        },
        {
          noAck: false, // Manual acknowledgment
        }
      );
      this.consumerTag = consumerTag;

      this.isConsuming = true;
      logger.success("[UAE_WEBHOOK_CONSUMER] UAE Webhook consumer started successfully");
    } catch (error) {
      logger.error("[UAE_WEBHOOK_CONSUMER] Failed to start UAE webhook consumer", {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Process individual UAE webhook task
   */
  async processMessage(msg, channel) {
    let task;
    try {
      // Parse task
      task = JSON.parse(msg.content.toString());

      logger.debug("[UAE_WEBHOOK_CONSUMER] Processing task", {
        cdrUuid: task.cdrUuid,
        messageId: task.messageId,
      });

      // Execute webhook delivery via UAE manager
      await uaeWebhookManager.processWebhookTask(task);

      // Acknowledge message
      channel.ack(msg);
      this.processedCount++;

    } catch (error) {
      this.failedCount++;
      logger.error("[UAE_WEBHOOK_CONSUMER] UAE Task processing failed", {
        error: error.message,
        cdrUuid: task?.cdrUuid,
      });

      // Reject to DLQ if max retries reached or on fatal error
      channel.reject(msg, false); 
    }
  }

  /**
   * Stop consuming
   */
  async stop() {
    try {
      if (this.channel && this.consumerTag) {
        await this.channel.cancel(this.consumerTag);
      }
    } catch (error) {
      logger.warn("[UAE_WEBHOOK_CONSUMER] Error cancelling consumer (channel likely already closed)", {
        error: error.message,
      });
    } finally {
      this.isConsuming = false;
      this.consumerTag = null;
      this.channel = null;
      logger.info("[UAE_WEBHOOK_CONSUMER] UAE Webhook consumer stopped");
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
const uaeWebhookConsumer = new UAEWebhookConsumer();
export default uaeWebhookConsumer;
