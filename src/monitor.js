#!/usr/bin/env node

/**
 * System Health Monitor
 * Checks status of API server, Consumer, RabbitMQ, and MongoDB
 * Usage: node monitor.js
 */

import axios from 'axios';
import { MongoClient } from 'mongodb';
import amqp from 'amqplib';
import config from './config.js';

const colors = {
    reset: '\x1b[0m',
    green: '\x1b[32m',
    red: '\x1b[31m',
    yellow: '\x1b[33m',
    blue: '\x1b[34m',
    cyan: '\x1b[36m',
};

function log(message, color = 'reset') {
    console.log(`${colors[color]}${message}${colors.reset}`);
}

async function checkAPIServer() {
    try {
        log('\n📡 Checking API Server...', 'cyan');
        const response = await axios.get(`http://localhost:${config.app.port}/health`, {
            timeout: 5000
        });
        
        if (response.status === 200) {
            log('✅ API Server: RUNNING', 'green');
            log(`   Port: ${config.app.port}`, 'blue');
            log(`   Uptime: ${Math.floor(response.data.uptime)}s`, 'blue');
            return true;
        }
    } catch (error) {
        log('❌ API Server: DOWN', 'red');
        log(`   Error: ${error.message}`, 'red');
        return false;
    }
}

async function checkMongoDB() {
    try {
        log('\n🍃 Checking MongoDB...', 'cyan');
        const client = new MongoClient(config.mongodb.url, {
            serverSelectionTimeoutMS: 5000,
        });
        
        await client.connect();
        await client.db('admin').command({ ping: 1 });
        
        const db = client.db(config.mongodb.database);
        const collections = await db.listCollections().toArray();
        
        log('✅ MongoDB: CONNECTED', 'green');
        log(`   Database: ${config.mongodb.database}`, 'blue');
        log(`   Collections: ${collections.length}`, 'blue');
        
        await client.close();
        return true;
    } catch (error) {
        log('❌ MongoDB: DISCONNECTED', 'red');
        log(`   Error: ${error.message}`, 'red');
        return false;
    }
}

async function checkRabbitMQ() {
    try {
        log('\n🐰 Checking RabbitMQ...', 'cyan');
        const rabbitUrl = `amqp://${config.rabbitmq.user}:${config.rabbitmq.password}@${config.rabbitmq.host}:${config.rabbitmq.port}`;
        
        const connection = await amqp.connect(rabbitUrl, {
            timeout: 5000,
        });
        
        const channel = await connection.createChannel();
        
        // Check main queue
        const mainQueue = await channel.checkQueue(config.rabbitmq.queue);
        const dlq = await channel.checkQueue(config.rabbitmq.deadLetterQueue);
        
        log('✅ RabbitMQ: CONNECTED', 'green');
        log(`   Host: ${config.rabbitmq.host}:${config.rabbitmq.port}`, 'blue');
        log(`   Main Queue Messages: ${mainQueue.messageCount}`, 'blue');
        log(`   Dead Letter Queue: ${dlq.messageCount}`, dlq.messageCount > 0 ? 'yellow' : 'blue');
        
        if (mainQueue.messageCount > 1000) {
            log('   ⚠️  Warning: High queue size!', 'yellow');
        }
        
        if (dlq.messageCount > 0) {
            log('   ⚠️  Warning: Messages in DLQ!', 'yellow');
        }
        
        await channel.close();
        await connection.close();
        return true;
    } catch (error) {
        log('❌ RabbitMQ: DISCONNECTED', 'red');
        log(`   Error: ${error.message}`, 'red');
        return false;
    }
}

async function checkFailedOperations() {
    try {
        log('\n💾 Checking Failed Operations...', 'cyan');
        const client = new MongoClient(config.mongodb.url, {
            serverSelectionTimeoutMS: 5000,
        });
        
        await client.connect();
        const db = client.db(config.mongodb.database);
        
        const count = await db.collection('failed_mongodb_operations').countDocuments();
        
        if (count === 0) {
            log('✅ Failed Operations: NONE', 'green');
        } else {
            log(`⚠️  Failed Operations: ${count}`, count > 100 ? 'red' : 'yellow');
            
            // Get recent failures
            const recent = await db.collection('failed_mongodb_operations')
                .find()
                .sort({ failedAt: -1 })
                .limit(5)
                .toArray();
            
            log('\n   Recent Failures:', 'blue');
            recent.forEach((op, idx) => {
                log(`   ${idx + 1}. ${op.collection}.${op.operation} - ${op.error}`, 'yellow');
            });
        }
        
        await client.close();
        return count;
    } catch (error) {
        log('⚠️  Could not check failed operations', 'yellow');
        log(`   Error: ${error.message}`, 'yellow');
        return -1;
    }
}

async function getSystemSummary() {
    log('\n' + '='.repeat(60), 'cyan');
    log('           SYSTEM HEALTH MONITOR', 'cyan');
    log('='.repeat(60), 'cyan');
    log(`Timestamp: ${new Date().toISOString()}`, 'blue');
    
    const results = {
        api: await checkAPIServer(),
        mongodb: await checkMongoDB(),
        rabbitmq: await checkRabbitMQ(),
    };
    
    const failedOps = await checkFailedOperations();
    
    log('\n' + '='.repeat(60), 'cyan');
    log('                   SUMMARY', 'cyan');
    log('='.repeat(60), 'cyan');
    
    const allHealthy = results.api && results.mongodb && results.rabbitmq && failedOps === 0;
    
    if (allHealthy) {
        log('✅ ALL SYSTEMS OPERATIONAL', 'green');
    } else {
        log('⚠️  SOME ISSUES DETECTED', 'yellow');
        
        if (!results.api) log('   - API Server is down', 'red');
        if (!results.mongodb) log('   - MongoDB is disconnected', 'red');
        if (!results.rabbitmq) log('   - RabbitMQ is disconnected', 'red');
        if (failedOps > 0) log(`   - ${failedOps} failed operations`, 'yellow');
    }
    
    log('\n' + '='.repeat(60), 'cyan');
    log('💡 Tips:', 'blue');
    log('   - View logs: tail -f logs/combined-$(date +%Y-%m-%d).log', 'blue');
    log('   - Check PM2: pm2 status', 'blue');
    log('   - RabbitMQ UI: http://10.180.67.48:15672', 'blue');
    log('='.repeat(60) + '\n', 'cyan');
    
    process.exit(allHealthy ? 0 : 1);
}

// Run monitor
getSystemSummary().catch(error => {
    log(`\n❌ Monitor Error: ${error.message}`, 'red');
    process.exit(1);
});