/**
 * Configuration Module
 * Loads and validates all environment variables
 * Follows best practices for production deployment
 */

import dotenv from 'dotenv';
dotenv.config();

// Validate required environment variables
const requiredEnvVars = [
    'ESL_HOST',
    'ESL_PORT',
    'ESL_PASSWORD',
    'MONGO_URL',
    'APP_PORT',
    'INSTANCE_ID',
    'RABBITMQ_HOST',
    'RABBITMQ_PORT',
    'RABBITMQ_USER',
    'RABBITMQ_PASSWORD'
];

const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingVars.length > 0) {
    console.error(`❌ Missing required environment variables: ${missingVars.join(', ')}`);
    console.error('Please check your .env file');
    process.exit(1);
}

const config = {
    // Application
    app: {
        env: process.env.NODE_ENV || 'production',
        port: parseInt(process.env.APP_PORT, 10) || 8735,
        maxRequestSize: process.env.MAX_REQUEST_SIZE || '50mb',
        requestTimeout: parseInt(process.env.REQUEST_TIMEOUT, 10) || 30000,
    },

    // FreeSWITCH ESL
    esl: {
        host: process.env.ESL_HOST || '127.0.0.1',
        port: parseInt(process.env.ESL_PORT, 10) || 8021,
        password: process.env.ESL_PASSWORD || 'ClueCon',
    },

    // MongoDB
    mongodb: {
        url: process.env.MONGO_URL,
        database: process.env.MONGO_DATABASE || 'db_pbxcc',
        options: {
            maxPoolSize: 10,
            minPoolSize: 2,
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
        }
    },

    // Logging
    logging: {
        level: process.env.LOG_LEVEL || 'info',
        dir: process.env.LOG_DIR || './logs',
        maxFiles: '10d', // Keep logs for 10 days
        maxSize: '200m',  // Max 200MB per file
    },

    // Email
    email: {
        host: process.env.SMTP_HOST || '',
        port: parseInt(process.env.SMTP_PORT, 10) || 587,
        secure: process.env.SMTP_SECURE === 'true',
        auth: {
            user: process.env.SMTP_USER || '',
            pass: process.env.SMTP_PASS || '',
        },
        from: process.env.SMTP_FROM || 'noreply@example.com',
    },

    // Security
    security: {
        corsOrigin: process.env.CORS_ORIGIN || '*',
    },

    // RabbitMQ
    rabbitmq: {
        host: process.env.RABBITMQ_HOST,
        port: parseInt(process.env.RABBITMQ_PORT, 10) || 5672,
        user: process.env.RABBITMQ_USER,
        password: process.env.RABBITMQ_PASSWORD,
        queue: `freeswitch_cdr_operations_${process.env.INSTANCE_ID}`,
        mainExchange: `freeswitch_cdr_operations_ex_${process.env.INSTANCE_ID}`,
        deadLetterQueue: `freeswitch_cdr_operations_dlq_${process.env.INSTANCE_ID}`,
        deadLetterExchange: `freeswitch_cdr_operations_dlx_${process.env.INSTANCE_ID}`,
        maxRetries: parseInt(process.env.RABBITMQ_MAX_RETRIES, 10) || 3,
        retryDelay: parseInt(process.env.RABBITMQ_RETRY_DELAY, 10) || 5000, // 5 seconds
        maxRetryDelay: parseInt(process.env.RABBITMQ_MAX_RETRY_DELAY, 10) || 60000, // 60 seconds
        messageTtl: parseInt(process.env.RABBITMQ_MESSAGE_TTL, 10) || 86400000, // 24 hours
    },

    webhooks: {
        queue: `freeswitch_webhook_delivery_${process.env.INSTANCE_ID}`,
        delay: 60000, // Default 1 minute
    }
};

// Validate MongoDB URL format
if (!config.mongodb.url.startsWith('mongodb://') && !config.mongodb.url.startsWith('mongodb+srv://')) {
    console.error('❌ Invalid MongoDB URL format');
    process.exit(1);
}

export default config;