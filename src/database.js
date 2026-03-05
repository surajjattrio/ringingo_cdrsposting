/**
 * MongoDB Database Module
 * Handles MongoDB connection and provides access to all collections
 */

import { MongoClient } from 'mongodb';
import config from './config.js';
import logger from './logger.js';

let client = null;
let db = null;
let isConnected = false;

/**
 * Initialize MongoDB connection
 */
async function connectMongoDB() {
    try {
        if (isConnected && client) {
            logger.info('[MONGODB] Already connected');
            return db;
        }

        logger.info('[MONGODB] Connecting to MongoDB...', {
            url: config.mongodb.url.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@') // Mask credentials
        });

        client = new MongoClient(config.mongodb.url, config.mongodb.options);
        await client.connect();
        
        // Verify connection
        await client.db('admin').command({ ping: 1 });
        
        db = client.db(config.mongodb.database);
        isConnected = true;

        logger.info('[MONGODB] Connection successful', {
            database: config.mongodb.database,
            url: config.mongodb.url.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@')
        });
        return db;

    } catch (error) {
        logger.error('[MONGODB] Connection failed', {
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Get MongoDB database instance
 */
function getDatabase() {
    if (!db) {
        throw new Error('MongoDB not connected. Call connectMongoDB() first.');
    }
    return db;
}

/**
 * Get all collections used in the application
 */
function getCollections() {
    const database = getDatabase();
    
    return {
        // Main CDR collections
        cdrs: database.collection('cdrs'),
        missedCalls: database.collection('missed_calls'),
        inboundCdrs: database.collection('inbound_cdrs'),
        recordings: database.collection('recording'),
        cdrsSummary: database.collection('cdrs_summary'),
        
        // Campaign related
        outboundCampaign: database.collection('outbound_campaign'),
        blendedCampaign: database.collection('blended_campaign'),
        campaignStatistics: database.collection('campaign_statistics'),
        autoCampaignOriginate: database.collection('auto_campaign_originate'),
        predictiveCalculation: database.collection('predictive_calculation'),
        
        // Lead management
        leadManagement: database.collection('lead_management'),
        leadReport: database.collection('lead_report'),
        leadOriginate: database.collection('lead_originate'),
        leadsInQueue: database.collection('leads_in_queue'),
        leadBroadcastQueue: database.collection('lead_broadcast_queue'),
        
        // User and extension
        user: database.collection('user'),
        userRole: database.collection('user_role'),
        extensions: database.collection('extensions'),
        tenant: database.collection('tenant'),
        
        // Call routing
        ringGroup: database.collection('ring_group'),
        tmpRinggroup: database.collection('tmp_ringgroup_details'),
        
        // Other
        webHook: database.collection('web_hook'),
        customerCallerid: database.collection('customer_callerid_management'),
        disposition: database.collection('disposition'),
        stickyAgent: database.collection('sticky_agent'),
        telecomCircle: database.collection('telecom_circle'),
        voiceBroadcast: database.collection('voice_broadcast'),
        agentLiveReport: database.collection('agent_live_report'),
        ipMap: database.collection('ip_map'),
        voiceBroadcastDetails: database.collection('voice_broadcast_details'),
    };
}

/**
 * Close MongoDB connection
 */
async function closeMongoDB() {
    try {
        if (client) {
            await client.close();
            isConnected = false;
            db = null;
            client = null;
            logger.info('[MONGODB] Connection closed');
        }
    } catch (error) {
        logger.error('[MONGODB] Error closing connection', {
            error: error.message
        });
    }
}

/**
 * Check if MongoDB is connected
 */
function isMongoDBConnected() {
    return isConnected;
}

export {
    connectMongoDB,
    getDatabase,
    getCollections,
    closeMongoDB,
    isMongoDBConnected
};
