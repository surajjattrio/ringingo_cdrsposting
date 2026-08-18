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
import uaeWebhookConsumer from "./webhook/webhook-uae-consumer.js";

/**
 * (Re)start all three consumers in order, retrying the whole sequence on failure.
 * Rethrows if every attempt fails so the caller can decide how to escalate.
 */
async function startConsumersWithRetry(generation, { retries = 3, delayMs = 3000 } = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      consumer.isConsuming = false;
      webhookConsumer.isConsuming = false;
      uaeWebhookConsumer.isConsuming = false;

      await consumer.start();
      logger.success("✅ Main consumer started successfully");

      await webhookConsumer.start();
      logger.success("✅ Webhook consumer started successfully");

      await uaeWebhookConsumer.start();
      logger.success("✅ UAE Webhook consumer started successfully");

      return;
    } catch (error) {
      logger.error("[CONSUMER_SERVICE] Consumer start attempt failed", {
        generation,
        attempt,
        retries,
        error: error.message,
      });

      if (attempt === retries) {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

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

    // Re-subscribe all consumers every time RabbitMQ (re)connects. This
    // fires on the initial connect AND after every reconnect, since a
    // reconnect creates a brand new channel and any consumer registered on
    // the old (now dead) channel stops receiving messages otherwise -
    // messages would keep publishing successfully but never get consumed,
    // eventually expiring via TTL into the dead letter queue.
    //
    // EventEmitter.emit does not await async listeners, so if "connected"
    // fires again while a previous invocation is still mid-restart, both
    // would independently see isConsuming === false and both would call
    // channel.consume(), registering duplicate consumers. restartChain
    // serializes every invocation through a single promise chain so they
    // never run concurrently, and the generation check skips a stale link
    // if a newer reconnect has already superseded it by the time its turn
    // comes up - the outcome always ends up bound to the newest channel.
    let restartChain = Promise.resolve();
    let latestGeneration = 0;

    rabbitMQ.on("connected", ({ generation }) => {
      latestGeneration = generation;

      restartChain = restartChain
        .catch(() => {}) // a prior failure must not break the chain for later links
        .then(async () => {
          if (generation !== latestGeneration) {
            logger.info("[CONSUMER_SERVICE] Skipping superseded reconnect generation", {
              generation,
              latestGeneration,
            });
            return;
          }

          logger.info("[CONSUMER_SERVICE] RabbitMQ (re)connected, (re)starting consumers", {
            generation,
          });

          await startConsumersWithRetry(generation);
        })
        .catch((error) => {
          logger.error(
            "[CONSUMER_SERVICE] STUCK: consumers failed to start after retries",
            {
              error: error.message,
              stack: error.stack,
              generation,
            }
          );

          // generation === 1 is the very first channel this process ever opened
          // (channelGeneration starts at 0, becomes 1 on first success) - i.e. first
          // boot. A service that boots with zero working consumers has no reason to
          // stay alive - exit so PM2 restarts it fresh. A later reconnect (generation
          // > 1) must NOT kill an otherwise-healthy, already-running process; just log
          // it and let the next "connected" event retry naturally.
          if (generation === 1) {
            process.exit(1);
          }
        });
    });

    // Connect to RabbitMQ (retries indefinitely on failure - see rabbitmq.js)
    await rabbitMQ.connect();
    logger.success("✅ RabbitMQ connection established");

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
      await uaeWebhookConsumer.stop();
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
