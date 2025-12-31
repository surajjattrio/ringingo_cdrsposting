/**
 * Consumer Service with Webhook Support
 * Standalone service to consume and process MongoDB operations from RabbitMQ
 * Delivers CDRs to configured webhooks
 * Run this as a separate process: node consumer-service.js
 */

import logger from "./logger.js";
import { connectMongoDB, closeMongoDB } from "./database.js";
import rabbitMQ from "./rabbitmq.js";
import consumer from "./consumer.js";
import webhookManager from "./webhook/webhook-manager.js";

/**
 * Start consumer service with webhook support
 */
async function startConsumerService() {
  try {
    logger.info("==============================");
    logger.info("[CONSUMER_SERVICE] Starting Consumer Service");
    logger.info("==============================");

    // Connect to MongoDB
    await connectMongoDB();
    logger.success("✅ MongoDB connection established");

    // Connect to RabbitMQ
    await rabbitMQ.connect();
    logger.success("✅ RabbitMQ connection established");

    // Initialize webhook manager
    await webhookManager.initialize();
    logger.success("✅ Webhook manager initialized");

    // Start consuming messages
    await consumer.start();
    logger.success("✅ Consumer started successfully");

    logger.info("==============================");
    logger.info("[CONSUMER_SERVICE] Consumer Service Running");
    logger.info("  - Processing MongoDB operations");
    logger.info("  - Delivering CDRs to webhooks");
    logger.info(`  - Active webhooks: ${webhookManager.getActiveWebhooks().length}`);
    logger.info("==============================\n");

    // Reload webhooks periodically (every 30 minutes)
    const webhookReloadInterval = setInterval(async () => {
      logger.info('[CONSUMER_SERVICE] Reloading webhooks...');
      await webhookManager.reloadWebhooks();
    }, 1800000); // 30 min

    // Graceful shutdown
    const gracefulShutdown = async (signal) => {
      logger.info("==============================");
      logger.info(
        `[CONSUMER_SERVICE] ${signal} received, shutting down gracefully...`
      );
      logger.info("==============================");

      // Clear intervals
      clearInterval(webhookReloadInterval);

      await consumer.stop();
      logger.info("Consumer stopped");

      await rabbitMQ.close();
      logger.info("RabbitMQ connection closed");

      await closeMongoDB();
      logger.info("MongoDB connection closed");

      const stats = consumer.getStats();
      logger.info("[CONSUMER_SERVICE] Final Stats", stats);

      logger.info("==============================");
      logger.info("[CONSUMER_SERVICE] Shutdown complete");
      logger.info("==============================");

      process.exit(0);
    };


    process.on("SIGTERM", () => gracefulShutdown("SIGTERM")); //**SIGTERM** = Signal sent by `pm2 stop` or `pm2 restart`
    process.on("SIGINT", () => gracefulShutdown("SIGINT")); //**SIGINT** = Signal sent by pressing `Ctrl+C`
  } catch (error) {
    logger.error("[CONSUMER_SERVICE] Failed to start", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the consumer service
startConsumerService();
