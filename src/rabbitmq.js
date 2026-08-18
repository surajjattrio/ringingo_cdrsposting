/**
 * RabbitMQ Connection Manager
 * Handles connection, channel management, reconnection logic
 * Follows best practices for production deployment
 */

import { EventEmitter } from "events";
import amqp from "amqplib";
import config from "./config.js";
import logger from "./logger.js";

class RabbitMQManager extends EventEmitter {
  constructor() {
    super();
    this.connection = null;
    this.channel = null;
    this.isConnected = false;
    this.isConnecting = false;
    this.isReconnecting = false;
    this.isShuttingDown = false;
    this.reconnectAttempts = 0;
    this.reconnectDelay = 5000; // base delay: 5 seconds
    this.maxReconnectDelay = 60000; // cap backoff at 60 seconds, retries forever
    this.prefetchCount = 10; // Process 10 messages at a time

    // Channel generation + fast-failure tracking. Lets us tell a genuine
    // transient network drop apart from a channel that dies immediately
    // after opening every single time (a deterministic config problem -
    // e.g. a queue re-declared with different arguments than what's on the
    // broker - that no amount of retrying will ever fix).
    this.channelGeneration = 0;
    this.channelOpenedAt = null;
    this.failureRegisteredForCurrentChannel = false;
    this.consecutiveFastChannelFailures = 0;
    this.fastFailureThresholdMs = 10000; // dying within 10s of opening looks deterministic
    this.fastFailureEscalationCount = 3; // escalate loudly after 3 such deaths in a row
  }

  /**
   * Connect to RabbitMQ. On failure, hands off to reconnect() which retries
   * indefinitely (with capped backoff) until a connection succeeds.
   */
  async connect() {
    if (this.isConnected && this.connection) {
      logger.info("[RABBITMQ] Already connected");
      return;
    }

    if (this.isConnecting) {
      logger.info("[RABBITMQ] Connection attempt already in progress");
      return;
    }

    this.isConnecting = true;

    try {
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

      logger.success("[RABBITMQ] Connection established successfully");

      // Handle connection close
      this.connection.on("close", (err) => {
        this.isConnected = false;
        logger.warn("[RABBITMQ] Connection closed", { error: err?.message });
        if (!this.isShuttingDown) {
          this.registerChannelFailure();
          this.reconnect();
        }
      });

      // Handle connection errors (close will follow, which triggers reconnect)
      this.connection.on("error", (err) => {
        logger.error("[RABBITMQ] Connection error", {
          error: err.message,
          stack: err.stack,
        });
      });

      // Create channel
      await this.createChannel();

      // Only reset the attempt counter once the WHOLE sequence (TCP connect +
      // channel + queues) actually succeeds. Resetting it right after the
      // raw TCP connect (as before) hid repeated channel/queue setup
      // failures behind a permanently misleading "attempt: 1" and kept
      // retrying at the 5s floor forever instead of backing off.
      this.reconnectAttempts = 0;
      this.isConnecting = false;
    } catch (error) {
      this.isConnected = false;
      this.isConnecting = false;
      logger.error("[RABBITMQ] Connection failed", {
        error: error.message,
        stack: error.stack,
        attempt: this.reconnectAttempts + 1,
      });
      if (!this.isShuttingDown) {
        this.registerChannelFailure();
        await this.reconnect();
      }
    }
  }

  /**
   * Create channel with prefetch
   */
  async createChannel() {
    try {
      // A confirm channel lets publish() find out whether the broker
      // actually received/routed a message instead of just whether the
      // local write buffer accepted it - see publish() below.
      this.channel = await this.connection.createConfirmChannel();

      // Set prefetch count (QoS)
      await this.channel.prefetch(this.prefetchCount);

      logger.info("[RABBITMQ] Channel created with prefetch", {
        prefetch: this.prefetchCount,
      });

      // Handle channel close - a channel can die (e.g. precondition failure)
      // while the underlying connection stays open, so this must also
      // trigger reconnection or consumers are left attached to a dead channel.
      this.channel.on("close", () => {
        logger.warn("[RABBITMQ] Channel closed");
        if (!this.isShuttingDown && this.isConnected) {
          this.isConnected = false;
          this.registerChannelFailure();
          this.reconnect();
        }
      });

      // Handle channel errors (close will follow, which triggers reconnect)
      this.channel.on("error", (err) => {
        logger.error("[RABBITMQ] Channel error", {
          error: err.message,
        });
      });

      // Setup queues and exchanges
      await this.setupQueues();

      // Mark this channel's "birth" so registerChannelFailure() can tell a
      // channel that dies within seconds of opening (deterministic config
      // problem) apart from one that ran fine for a while and then hit a
      // genuine transient issue.
      this.channelGeneration++;
      this.channelOpenedAt = Date.now();
      this.failureRegisteredForCurrentChannel = false;

      // Notify listeners (consumers) that a channel is ready to consume from.
      // Fires on the initial connect AND every reconnect, since the previous
      // channel object (if any) is now dead and consumers must re-subscribe.
      this.emit("connected", { channel: this.channel, generation: this.channelGeneration });
    } catch (error) {
      logger.error("[RABBITMQ] Channel creation failed", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Record that the current channel/connection died, and whether it died
   * suspiciously fast after opening (< fastFailureThresholdMs). Guarded so a
   * single underlying failure that surfaces through both a rejected RPC call
   * (e.g. assertQueue) and the channel's own "close" event only counts once.
   */
  registerChannelFailure() {
    if (this.failureRegisteredForCurrentChannel) {
      return;
    }
    this.failureRegisteredForCurrentChannel = true;

    const openDurationMs = this.channelOpenedAt ? Date.now() - this.channelOpenedAt : null;
    this.channelOpenedAt = null;

    if (openDurationMs !== null && openDurationMs < this.fastFailureThresholdMs) {
      this.consecutiveFastChannelFailures++;
      logger.warn("[RABBITMQ] Channel died shortly after opening", {
        openDurationMs,
        consecutiveFastChannelFailures: this.consecutiveFastChannelFailures,
      });
    } else {
      this.consecutiveFastChannelFailures = 0;
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

      // Webhook queue options
      const webhookQueueOptions = {
        durable: true,
        deadLetterExchange: config.rabbitmq.deadLetterExchange,
        deadLetterRoutingKey: config.rabbitmq.deadLetterQueue,
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

      // Assert main exchange
      await this.channel.assertExchange(
        config.rabbitmq.mainExchange,
        "direct",
        { durable: true }
      );

      // Assert main queue
      await this.channel.assertQueue(config.rabbitmq.queue, mainQueueOptions);

      // Bind main queue to exchange
      await this.channel.bindQueue(
        config.rabbitmq.queue,
        config.rabbitmq.mainExchange,
        config.rabbitmq.queue
      );

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

      // Assert webhook delivery queue
      await this.channel.assertQueue(config.webhooks.queue, webhookQueueOptions);

      // Bind webhook delivery queue to exchange
      await this.channel.bindQueue(
        config.webhooks.queue,
        config.rabbitmq.mainExchange,
        config.webhooks.queue
      );

      // Assert UAE-specific webhook delivery queue
      const uaeWebhookQueue = "_uae_" + config.webhooks.queue;
      await this.channel.assertQueue(uaeWebhookQueue, webhookQueueOptions);

      // Bind UAE-specific webhook delivery queue to exchange
      await this.channel.bindQueue(
        uaeWebhookQueue,
        config.rabbitmq.mainExchange,
        uaeWebhookQueue
      );

      logger.success("[RABBITMQ] Queues and exchanges configured", {
        mainQueue: config.rabbitmq.queue,
        webhookQueue: config.webhooks.queue,
        uaeWebhookQueue: uaeWebhookQueue,
        mainExchange: config.rabbitmq.mainExchange,
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
   * Reconnect with capped backoff. Retries indefinitely - never gives up and
   * never exits the process. Guarded so overlapping triggers (connection
   * close + channel close firing around the same time) only run one loop.
   */
  async reconnect() {
    if (this.isReconnecting || this.isShuttingDown) {
      return;
    }
    this.isReconnecting = true;

    try {
      while (!this.isConnected && !this.isShuttingDown) {
        this.reconnectAttempts++;
        const delay = Math.min(
          this.reconnectDelay * this.reconnectAttempts,
          this.maxReconnectDelay
        );

        if (this.consecutiveFastChannelFailures >= this.fastFailureEscalationCount) {
          // The connection itself keeps succeeding but the channel dies
          // within seconds every single time - that pattern cannot be a
          // network flap, since the network is clearly fine. It's almost
          // certainly a static config mismatch (e.g. a queue/exchange
          // already declared on the broker with different arguments than
          // config.rabbitmq currently specifies). Retrying will never fix
          // this on its own, so log loudly instead of blending in with
          // normal reconnect attempts.
          logger.error(
            "[RABBITMQ] STUCK: channel keeps dying within seconds of (re)connecting - this looks like a deterministic setup problem, NOT a transient network issue. Check that the queue/exchange arguments (messageTtl, deadLetterExchange, deadLetterRoutingKey) match what's already declared on the broker.",
            {
              consecutiveFastChannelFailures: this.consecutiveFastChannelFailures,
              attempt: this.reconnectAttempts,
              delayMs: delay,
            }
          );
        } else {
          logger.info("[RABBITMQ] Attempting to reconnect", {
            attempt: this.reconnectAttempts,
            delayMs: delay,
          });
        }

        await new Promise((resolve) => setTimeout(resolve, delay));

        if (this.isShuttingDown) {
          break;
        }

        await this.connect();
      }
    } finally {
      this.isReconnecting = false;
    }
  }

  /**
   * Publish message to queue and wait for the broker to confirm it actually
   * received it (channel is opened via createConfirmChannel() in
   * createChannel()). channel.publish()'s own return value only reflects
   * local write-buffer backpressure, not delivery - relying on it alone
   * reports success even when the broker never got the message (crash /
   * network drop right after the local write). Concurrent callers each get
   * their own confirm callback, so independent publishes are NOT serialized
   * against each other - only each caller's own await is blocked on its own
   * message's ack, which is what keeps bulk/reprocessing throughput from
   * collapsing into one-at-a-time round trips.
   */
  async publish(message, options = {}, queueName = config.rabbitmq.queue) {
    if (!this.isConnected || !this.channel) {
      const error = new Error("RabbitMQ not connected");
      logger.error("[RABBITMQ] Publish failed", { error: error.message, queueName });
      throw error;
    }

    const messageId = message.messageId || message.cdrUuid;
    const messageBuffer = Buffer.from(JSON.stringify(message));

    const publishOptions = {
      persistent: true, // Persist message to disk
      contentType: "application/json",
      timestamp: Date.now(),
      ...options,
    };

    try {
      await new Promise((resolve, reject) => {
        const wroteToBuffer = this.channel.publish(
          config.rabbitmq.mainExchange,
          queueName,
          messageBuffer,
          publishOptions,
          (err) => {
            if (err) {
              reject(err instanceof Error ? err : new Error(String(err)));
            } else {
              resolve();
            }
          }
        );

        if (!wroteToBuffer) {
          logger.warn("[RABBITMQ] Local write buffer full, applying backpressure", {
            queueName,
            messageId,
          });
        }
      });

      logger.debug("[RABBITMQ] Message published and confirmed by broker", {
        queueName,
        messageId,
      });

      return true;
    } catch (error) {
      logger.error("[RABBITMQ] Publish failed or was not confirmed by broker", {
        error: error.message,
        queueName,
        messageId,
      });
      throw error;
    }
  }

  /**
   * Close connection gracefully
   */
  async close() {
    this.isShuttingDown = true;
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
