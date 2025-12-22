/**
 * RabbitMQ Connection Manager
 * Handles connection, channel management, reconnection logic
 * Follows best practices for production deployment
 */

import amqp from "amqplib";
import config from "./config.js";
import logger from "./logger.js";

class RabbitMQManager {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.reconnectDelay = 5000; // 5 seconds
    this.prefetchCount = 10; // Process 10 messages at a time
  }

  /**
   * Connect to RabbitMQ with reconnection logic
   */
  async connect() {
    try {
      if (this.isConnected && this.connection) {
        logger.info("[RABBITMQ] Already connected");
        return;
      }

      const rabbitUrl = `amqp://${config.rabbitmq.user}:${config.rabbitmq.password}@${config.rabbitmq.host}:${config.rabbitmq.port}`;

      logger.info("[RABBITMQ] Connecting to RabbitMQ...", {
        host: config.rabbitmq.host,
        port: config.rabbitmq.port,
      });

      // Create connection
      this.connection = await amqp.connect(rabbitUrl, {
        heartbeat: 60,
      });

      this.isConnected = true;
      this.reconnectAttempts = 0;

      logger.success("[RABBITMQ] Connection established successfully");

      // Handle connection close
      this.connection.on("close", async (err) => {
        this.isConnected = false;
        logger.warn("[RABBITMQ] Connection closed", { error: err?.message });
        await this.reconnect();
      });

      // Handle connection errors
      this.connection.on("error", (err) => {
        this.isConnected = false;
        logger.error("[RABBITMQ] Connection error", {
          error: err.message,
          stack: err.stack,
        });
      });

      // Create channel
      await this.createChannel();
    } catch (error) {
      this.isConnected = false;
      logger.error("[RABBITMQ] Connection failed", {
        error: error.message,
        stack: error.stack,
        attempt: this.reconnectAttempts + 1,
      });
      await this.reconnect();
    }
  }

  /**
   * Create channel with prefetch
   */
  async createChannel() {
    try {
      this.channel = await this.connection.createChannel();

      // Set prefetch count (QoS)
      await this.channel.prefetch(this.prefetchCount);

      logger.info("[RABBITMQ] Channel created with prefetch", {
        prefetch: this.prefetchCount,
      });

      // Handle channel close
      this.channel.on("close", () => {
        logger.warn("[RABBITMQ] Channel closed");
      });

      // Handle channel errors
      this.channel.on("error", (err) => {
        logger.error("[RABBITMQ] Channel error", {
          error: err.message,
        });
      });

      // Setup queues and exchanges
      await this.setupQueues();
    } catch (error) {
      logger.error("[RABBITMQ] Channel creation failed", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Setup queues with dead letter exchange
   */
  async setupQueues() {
    try {
      // Main queue options
      const mainQueueOptions = {
        durable: true, // Persist queue on RabbitMQ restart
        deadLetterExchange: config.rabbitmq.deadLetterExchange,
        deadLetterRoutingKey: config.rabbitmq.deadLetterQueue,
        messageTtl: config.rabbitmq.messageTtl, // Message TTL: 24 hours
      };

      // Dead letter queue options
      const dlqOptions = {
        durable: true,
      };

      // Assert dead letter exchange
      await this.channel.assertExchange(
        config.rabbitmq.deadLetterExchange,
        "direct",
        { durable: true }
      );

      // Assert main queue
      await this.channel.assertQueue(config.rabbitmq.queue, mainQueueOptions);

      // Assert dead letter queue
      await this.channel.assertQueue(
        config.rabbitmq.deadLetterQueue,
        dlqOptions
      );

      // Bind dead letter queue to exchange
      await this.channel.bindQueue(
        config.rabbitmq.deadLetterQueue,
        config.rabbitmq.deadLetterExchange,
        config.rabbitmq.deadLetterQueue
      );

      logger.success("[RABBITMQ] Queues and exchanges configured", {
        mainQueue: config.rabbitmq.queue,
        deadLetterQueue: config.rabbitmq.deadLetterQueue,
        deadLetterExchange: config.rabbitmq.deadLetterExchange,
      });
    } catch (error) {
      logger.error("[RABBITMQ] Queue setup failed", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Reconnect with exponential backoff
   */
  async reconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      logger.error("[RABBITMQ] Max reconnection attempts reached", {
        attempts: this.reconnectAttempts,
      });
      process.exit(1); // Exit process - let process manager restart
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * this.reconnectAttempts;

    logger.info("[RABBITMQ] Attempting to reconnect", {
      attempt: this.reconnectAttempts,
      maxAttempts: this.maxReconnectAttempts,
      delayMs: delay,
    });

    await new Promise((resolve) => setTimeout(resolve, delay));
    await this.connect();
  }

  /**
   * Publish message to queue
   */
  async publish(message, options = {}) {
    try {
      if (!this.isConnected || !this.channel) {
        throw new Error("RabbitMQ not connected");
      }

      const messageBuffer = Buffer.from(JSON.stringify(message));

      const publishOptions = {
        persistent: true, // Persist message to disk
        contentType: "application/json",
        timestamp: Date.now(),
        ...options,
      };

      const result = this.channel.sendToQueue(
        config.rabbitmq.queue,
        messageBuffer,
        publishOptions
      );

      if (!result) {
        logger.warn("[RABBITMQ] Message buffer full, waiting...", {
          operation: message.operation,
          collection: message.collection,
        });
        // Wait for drain event
        await new Promise((resolve) => this.channel.once("drain", resolve));
      }

      logger.debug("[RABBITMQ] Message published", {
        operation: message.operation,
        collection: message.collection,
        messageId: message.messageId,
      });

      return true;
    } catch (error) {
      logger.error("[RABBITMQ] Publish failed", {
        error: error.message,
        stack: error.stack,
        message: message,
      });
      throw error;
    }
  }

  /**
   * Close connection gracefully
   */
  async close() {
    try {
      if (this.channel) {
        await this.channel.close();
        logger.info("[RABBITMQ] Channel closed");
      }
      if (this.connection) {
        await this.connection.close();
        this.isConnected = false;
        logger.info("[RABBITMQ] Connection closed");
      }
    } catch (error) {
      logger.error("[RABBITMQ] Error closing connection", {
        error: error.message,
      });
    }
  }

  /**
   * Get channel (for consumer)
   */
  getChannel() {
    if (!this.channel) {
      throw new Error("Channel not available");
    }
    return this.channel;
  }

  /**
   * Check if connected
   */
  isConnectionActive() {
    return this.isConnected;
  }
}

// Export singleton instance
const rabbitMQ = new RabbitMQManager();
export default rabbitMQ;
