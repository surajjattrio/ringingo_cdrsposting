/**
 * Consumer Service with Webhook Support
 * Standalone service to consume and process MongoDB operations from RabbitMQ
 * Delivers CDRs to configured webhooks
 * Run this as a separate process: node consumer-service.js
 */

import logger from "./logger.js";
import { connectMongoDB, closeMongoDB, checkConnection, reconnectMongoDB } from "./database.js";
import rabbitMQ from "./rabbitmq.js";
import consumer from "./consumer.js";
import webhookConsumer from "./webhook/webhook-consumer.js";

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


    // Start consuming messages
    await consumer.start();
    logger.success("✅ Main consumer started successfully");

    // Start consuming messages (Webhook Delivery Tasks)
    await webhookConsumer.start();
    logger.success("✅ Webhook consumer started successfully");

    logger.info("==============================");
    logger.info("[CONSUMER_SERVICE] Consumer Service Running");
    logger.info("  - Processing MongoDB operations");
    logger.info("  - Delivering CDRs to webhooks");
    logger.info("==============================\n");

    // MongoDB connection health check (every 60 seconds)
    const mongoHealthCheckInterval = setInterval(async () => {
      if (!(await checkConnection())) {
        logger.warn('[CONSUMER_SERVICE] Background health check detected dead MongoDB connection. Triggering reconnect...');
        await reconnectMongoDB();
      }
    }, 60000); // 60 sec

    // Graceful shutdown
    const gracefulShutdown = async (signal) => {
      logger.info("==============================");
      logger.info(
        `[CONSUMER_SERVICE] ${signal} received, shutting down gracefully...`
      );
      logger.info("==============================");

      // Clear intervals
      clearInterval(mongoHealthCheckInterval);

      await consumer.stop();
      await webhookConsumer.stop();
      logger.info("Consumers stopped");

      await rabbitMQ.close();
      logger.info("RabbitMQ connection closed");

      await closeMongoDB();
      logger.info("MongoDB connection closed");

      const stats = consumer.getStats();
      const webhookStats = webhookConsumer.getStats();
      logger.info("[CONSUMER_SERVICE] Final Stats", { ...stats, webhookStats });

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
