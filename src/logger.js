/**
 * Logger Utility
 * Uses Winston with daily rotate file for date-wise error logging
 * Follows best practices for production logging
 */

import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import config from './config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Ensure log directory exists
const logDir = config.logging.dir;
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

// Custom format for log messages
const logFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, stack, ...meta }) => {
        let log = `[${timestamp}] ${level.toUpperCase()}: ${message}`;
        
        // Add stack trace for errors
        if (stack) {
            log += `\n${stack}`;
        }
        
        // Add metadata if present
        if (Object.keys(meta).length > 0) {
            log += `\n${JSON.stringify(meta, null, 2)}`;
        }
        
        return log;
    })
);

// Daily rotate file transport for error logs
const errorFileTransport = new DailyRotateFile({
    filename: path.join(logDir, 'error-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    level: 'error',
    maxFiles: config.logging.maxFiles,
    maxSize: config.logging.maxSize,
    format: logFormat,
});

// Daily rotate file transport for all logs (optional, can be removed if only errors needed)
const combinedFileTransport = new DailyRotateFile({
    filename: path.join(logDir, 'combined-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    maxFiles: config.logging.maxFiles,
    maxSize: config.logging.maxSize,
    format: logFormat,
});

// Console transport for non-error logs
const consoleTransport = new winston.transports.Console({
    format: winston.format.combine(
        winston.format.colorize(),
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ timestamp, level, message }) => {
            return `[${timestamp}] ${level}: ${message}`;
        })
    ),
});

// Create logger instance
const logger = winston.createLogger({
    level: config.logging.level,
    format: logFormat,
    transports: [
        errorFileTransport,      // Date-wise error logs
        combinedFileTransport,   // All logs combined
        consoleTransport,        // Console output
    ],
    exitOnError: false,
});

// Handle uncaught exceptions
logger.exceptions.handle(
    new DailyRotateFile({
        filename: path.join(logDir, 'exceptions-%DATE%.log'),
        datePattern: 'YYYY-MM-DD',
        maxFiles: config.logging.maxFiles,
        maxSize: config.logging.maxSize,
    })
);

// Handle unhandled promise rejections
logger.rejections.handle(
    new DailyRotateFile({
        filename: path.join(logDir, 'rejections-%DATE%.log'),
        datePattern: 'YYYY-MM-DD',
        maxFiles: config.logging.maxFiles,
        maxSize: config.logging.maxSize,
    })
);

// Export logger methods
export default {
    error: (message, meta = {}) => logger.error(message, meta),
    warn: (message, meta = {}) => logger.warn(message, meta),
    info: (message, meta = {}) => logger.info(message, meta),
    debug: (message, meta = {}) => logger.debug(message, meta),
    
    // Helper method for logging with context
    logError: (context, error, additionalInfo = {}) => {
        logger.error(`[${context}] ${error.message}`, {
            stack: error.stack,
            ...additionalInfo
        });
    },
    
    // Helper method for success operations (console only, no file logging)
    success: (message) => {
        console.log(`✅ ${message}`);
    },
};
