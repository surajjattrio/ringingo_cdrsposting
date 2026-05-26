 
/**
 * Webhook Manager
 * Handles CDR webhook delivery with retry logic and error handling
 * Supports multiple webhook endpoints with individual configurations
 */

import axios from "axios";
import https from "https";
import logger from "../logger.js";
import { getDatabase } from "../database.js";
import config from "../config.js";

class WebhookManager {
  constructor() {
    this.deliveryStats = {
      total: 0,
      success: 0,
      failed: 0,
    };
  }

  /**
   * Convert UTC datetime string to IST (Indian Standard Time - UTC+5:30)
   * @param {string} utcDateTimeStr - UTC datetime string in format "YYYY-MM-DD HH:mm:ss"
   * @returns {string} IST datetime string in format "YYYY-MM-DD HH:mm:ss"
   */
  convertUtcToIst(utcDateTimeStr) {
    if (!utcDateTimeStr || typeof utcDateTimeStr !== 'string') {
      return utcDateTimeStr;
    }

    try {
      // Parse the UTC datetime string
      const utcDate = new Date(utcDateTimeStr);
      
      if (isNaN(utcDate.getTime())) {
        logger.warn("[WEBHOOK] Invalid UTC datetime format", { utcDateTimeStr });
        return utcDateTimeStr;
      }

      // IST is UTC+5:30
      const istDate = new Date(utcDate.getTime() + (5.5 * 60 * 60 * 1000));

      // Format as "YYYY-MM-DD HH:mm:ss"
      const year = istDate.getUTCFullYear();
      const month = String(istDate.getUTCMonth() + 1).padStart(2, '0');
      const day = String(istDate.getUTCDate()).padStart(2, '0');
      const hours = String(istDate.getUTCHours()).padStart(2, '0');
      const minutes = String(istDate.getUTCMinutes()).padStart(2, '0');
      const seconds = String(istDate.getUTCSeconds()).padStart(2, '0');

      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (error) {
      logger.error("[WEBHOOK] UTC to IST conversion failed", {
        error: error.message,
        utcDateTimeStr
      });
      return utcDateTimeStr;
    }
  }

  /**
   * Deliver CDR to all active webhooks for a tenant
   * @param {Object} cdr - CDR data to send
   * @param {string} tenantUuid - Tenant filter
   */
  async deliverCDR(cdr, tenantUuid = null) {
    try {
      const db = getDatabase();
      const webhooksCollection = db.collection("web_hook");

      // Build filter
      const filter = {
        is_active: true,
        webhook_type: "cdr",
      };

      if (tenantUuid) {
        filter.tenant_uuid = tenantUuid;
      }

      // Fetch matching webhooks directly from DB
      const webhooks = await webhooksCollection.find(filter).toArray();

      if (webhooks.length === 0) {
        logger.debug("[WEBHOOK] No matching active webhooks found", { tenantUuid });
        return;
      }

      logger.debug("[WEBHOOK] Delivering CDR to webhooks", {
        cdrUuid: cdr.uuid,
        webhookCount: webhooks.length,
      });

      // Prepare webhook configs and deliver in parallel
      const deliveryPromises = webhooks.map((webhook) => {
        const config = {
          uuid: webhook.uuid,
          url: (webhook.webhook_url || "").trim(),
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: webhook.max_retries || 3,
          retryDelay: webhook.retry_delay || 2000,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "Unnamed Webhook",
          bearerToken: webhook.bearer_token || null,
        };
        return this.deliverToWebhook(config, cdr);
      });

      await Promise.allSettled(deliveryPromises);
    } catch (error) {
      logger.error("[WEBHOOK] Delivery process failed", {
        error: error.message,
        tenantUuid,
      });
    }
  }

  /**
   * Process a webhook delivery task from the queue
   * @param {Object} task - { cdrUuid, collectionName, tenantUuid }
   */
  async processWebhookTask(task) {
    const { cdrUuid, collectionName, tenantUuid } = task;

    try {
      logger.info('[WEBHOOK] Processing task from queue', {
        cdrUuid,
        collectionName,
        tenantUuid,
      });

      // Apply delivery delay if configured
      if (config.webhooks.delay && config.webhooks.delay > 0) {
        logger.debug(`[WEBHOOK] Waiting for ${config.webhooks.delay}ms delay before processing...`);
        await new Promise(resolve => setTimeout(resolve, config.webhooks.delay));
      }

      // Fetch fresh CDR data from database
      const db = getDatabase();
      const collection = db.collection(collectionName);
      
      const freshCdr = await collection.findOne({ uuid: cdrUuid });

      if (!freshCdr) {
        logger.warn('[WEBHOOK] CDR not found in database, skipping delivery', {
          cdrUuid,
          collectionName,
        });
        return;
      }

      // Deliver fresh CDR to webhooks
      await this.deliverCDR(freshCdr, tenantUuid);

    } catch (error) {
      logger.error('[WEBHOOK] Queue task processing failed', {
        error: error.message,
        stack: error.stack,
        cdrUuid,
        collectionName,
      });
      throw error; // Rethrow to allow RabbitMQ retry
    }
  }

  /**
   * Deliver CDR to a specific webhook with retry logic
   * @param {Object} webhook - Webhook configuration
   * @param {Object} cdr - CDR data
   */
  async deliverToWebhook(webhook, cdr) {
    const startTime = Date.now();
    let lastError = null;
    let lastErrorDetails = null;

    for (let attempt = 1; attempt <= webhook.retries; attempt++) {
      try {
        logger.debug("[WEBHOOK] Attempting delivery", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          webhookUrl: webhook.url,
          attempt,
          maxRetries: webhook.retries,
          cdrUuid: cdr.uuid,
        });

        // Transform CDR to client format for delivery
        const transformedData = await this.transformCDR(cdr, webhook.uuid);

        // Prepare headers
        const headers = {
          "Content-Type": "application/json",
          "User-Agent": "curl/7.81.0",
          "Accept": "*/*",
          ...webhook.headers,
        };

        // Add Bearer Token if present
        if (webhook.bearerToken) {
          headers["Authorization"] = `Bearer ${webhook.bearerToken}`;
        }

        const loggedHeaders = { ...headers };
        if (loggedHeaders["Authorization"]) {
          loggedHeaders["Authorization"] = `Bearer ${webhook?.bearerToken}`;
        }

        logger.info("[WEBHOOK_DEBUG] Exhaustive request details", {
          webhookName: webhook.name,
          url: webhook.url,
          method: webhook.method,
          headers: loggedHeaders,
          rawCdr: {
            uuid: cdr.uuid,
            recording_path: cdr.recording_path,
            user_uuid: cdr.user_uuid,
            tenant_uuid: cdr.tenant_uuid
          },
          transformedData: transformedData
        });

        // Generate equivalent curl command for manual testing
        const curlHeaders = Object.entries(headers)
          .map(([key, value]) => `-H '${key}: ${value}'`)
          .join(" ");
        const curlPayload = JSON.stringify(transformedData);
        const curlCommand = `curl -X ${webhook.method} '${webhook.url}' ${curlHeaders} -d '${curlPayload}'`;
        
        logger.info("[WEBHOOK_CURL] Copy and test manually:", { curlCommand });

        const response = await axios({
          method: webhook.method,
          url: webhook.url,
          data: transformedData,
          headers: headers,
          timeout: webhook.timeout,
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          validateStatus: (status) => {
            return status === 200;
          },
        });

        const duration = Date.now() - startTime;

        logger.info("[WEBHOOK] Delivery successful", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          webhookUrl: webhook.url,
          cdrUuid: cdr.uuid,
          statusCode: response.status,
          durationMs: duration,
          attempt,
        });

        this.deliveryStats.total++;
        this.deliveryStats.success++;

        // Log successful delivery to database
        await this.logWebhookDelivery({
          webhookUuid: webhook.uuid,
          webhookName: webhook.name,
          cdrUuid: cdr.uuid,
          status: "success",
          statusCode: response.status,
          statusText: response.statusText,
          responseData: response.data,
          attempt,
          duration,
        });

        return {
          success: true,
          webhook: webhook.name,
          responseData: response.data,
          statusCode: response.status,
          statusText: response.statusText,
          durationMs: duration
        };
      } catch (error) {
        lastError = error;
        
        // Log basic error message safely without circular references
        logger.warn(`[WEBHOOK] Delivery attempt ${attempt} failed: ${error.message}`);

         lastErrorDetails  = {
          message: error.message,
          code: error.code,
          statusCode: error.response?.status,
          statusText: error.response?.statusText,
          responseData: error.response?.data,
          headers: error.response?.headers,
          isTimeout: error.code === "ECONNABORTED",
          isNetworkError:
            error.code === "ENOTFOUND" || error.code === "ECONNREFUSED",
          url: webhook.url,
        };

        logger.warn("[WEBHOOK] Delivery attempt failed", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          cdrUuid: cdr.uuid,
          attempt,
          maxRetries: webhook.retries,
          error: lastErrorDetails,
        });

        // Wait before retry (except on last attempt)
        if (attempt < webhook.retries) {
          const delay = webhook.retryDelay * attempt; // Exponential backoff
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All retries failed
    const duration = Date.now() - startTime;

    logger.error("[WEBHOOK] Delivery failed after all retries", lastErrorDetails);

    this.deliveryStats.total++;
    this.deliveryStats.failed++;

// Log failed delivery to database
    console.log("💾 [WEBHOOK] Saving failure to database...");
    
    try {
      await this.logWebhookDelivery({
        webhookUuid: webhook.uuid,
        webhookName: webhook.name,
        cdrUuid: cdr.uuid,
        status: "failed",
        statusCode: lastError.response?.status || 0,
        attempt: webhook.retries,
        duration,
        error: lastError.message,
        errorCode: lastError.code,
        errorType: lastError.name,
        statusText: lastError.response?.statusText,
        responseData: lastError.response?.data,
      });
      
      console.log("✅ [WEBHOOK] Failure logged to database");
    } catch (dbError) {
      console.error("❌ [WEBHOOK] Database save failed:", dbError.message);
    }
    return {
      success: false,
      webhook: webhook.name,
      error: lastError.message,
      responseData: lastError.response?.data,
      statusCode: lastError.response?.status,
      statusText: lastError.response?.statusText,
      durationMs: duration
    };
  }

  /**
   * Transform internal CDR to client-specific format
   * @param {Object} cdr - Internal CDR data
   * @param {string} webhookUuid - Webhook UUID for conditional transformation
   * @returns {Object} Transformed data
   */
  async transformCDR(cdr, webhookUuid = null) {
    try {
      const db = getDatabase();

      // Special case for specific tenant format - vyapar
      if (cdr.tenant_uuid === "f20b1a37-6234-4151-9a71-b2f99f1578fd") {
        const [hangupCause,lead] = await Promise.all([
          cdr.hangup_cause ? db.collection("disposition").findOne({ disposition_uuid: cdr.hangup_cause }) : null,
          cdr.lead_uuid ? db.collection("lead_management").findOne({ lead_management_uuid: cdr.lead_uuid }) : null,
        ]);

        const formatDuration = (seconds) => {
          if (!seconds || isNaN(seconds)) return "00:00:00";
          const h = Math.floor(seconds / 3600).toString().padStart(2, '0');
          const m = Math.floor((seconds % 3600) / 60).toString().padStart(2, '0');
          const s = Math.floor(seconds % 60).toString().padStart(2, '0');
          return `${h}:${m}:${s}`;
        };

        // Convert UTC timestamps to IST (Indian Standard Time)
        const callstart = this.convertUtcToIst(cdr.callstart) || "";
        const callend = cdr.callend || "";
        
        // Extract date and time from callstart (format: YYYY-MM-DD HH:mm:ss)
        const datePart = callstart.split(" ")[0] || "";
        const timePart = callstart.split(" ")[1] || "";

        // outbound -> caller_id_number
        // inbound - > destination_number
        let agent_number 
        if (cdr.direction === "outbound") {
          agent_number = cdr.caller_id_number;
        } else {
          agent_number = cdr.destination_number;
        }

        //  1 = inbound
        //  2 = outbound
        //  2 = default
        let event = "2";
        if (cdr.direction === "inbound") {
          event = "1";
        } else {
          event = "2";
        }

        const actual_talktime =
          cdr.direction === "inbound"
            ? cdr.actual_talktime
            : String(cdr.dial_method) === "1"
              ? cdr.user_uuid && String(cdr.user_uuid).trim() !== ""
                ? cdr.billsecond
                : 0
              : cdr.billsecond;

        const call_status =
          cdr.direction === "inbound"
            ? Number(cdr?.actual_talktime) > 0
              ? "Connected"
              : "Abandoned"
            : cdr.direction === "outbound"
              ? actual_talktime != "0"
                ? "Connected"
                : "Not Connected"
              : "Not Connected";
        return {
          "Incoming Webhook Payload": [
            {
              "call_date": datePart,
              "call_time": timePart,
              "customer_status": hangupCause?.name || "",
              "customer_number": cdr.direction === "inbound" ? (cdr.caller_id_number || "") : (cdr.destination_number || ""),
              "customer_call_duration": formatDuration(cdr.billsecond),//actual talktime need to send 
              "call_transfer_duration": "",//need these field
              "call_duration": formatDuration(cdr.total_duration),
              "call_uuid": cdr.uuid || "",
              "call_id": cdr._id || "",
              "call_status": call_status || "",
              "disposition": cdr?.disposition || "",
              "call_transfer_status": "",//need these field
              "agent_number": agent_number || "",
              "agent_uuid": cdr.user_uuid || "",
              "menu": "",//need these field
              "event": event,
              "start_time": callstart, // actualy time when agent pick the call
              "end_time": callend,
              "hangup_by": cdr.disconnected_by === "0" ? "Agent" : (cdr.disconnected_by === "1" ? "Customer" : (cdr.disconnected_by === "2" ? "System" : null)),
              "recording_url": cdr.recording_path ? (config.webhooks.recordingBaseUrl || "") + cdr.recording_path : "",
              "knowlarity_number": agent_number,
              "queue_id": cdr.pbx_feature === "call_queue" ? (cdr.pbx_feature_uuid || "") : "",
              "LegC_agent_number": "",//need these field
              "LegC_conversation_duration": "",//need these field
              "transfer_queue_name": "",//need these field
              "lead_uuid": cdr.lead_uuid || "",
              "custom_fields": lead?.custom_fields || {},
            }
          ]
        };
      }

      // Special case for convoz - V-Xpress NMC and percorner
      if ((cdr.tenant_uuid === "1f84dd0e-f557-4712-94a1-675d9b5e42e1" || cdr.tenant_uuid === "cf98c767-2404-484e-888b-e53897315d5a") && 
          (webhookUuid === "e5d92ded-4ade-4ca3-97df-7ac7edd9d91a" || webhookUuid === "6a6da557-4747-4d4b-b39f-5948e65b726e")) {
        const [user, note, campaignInbound, campaignOutbound, campaignBlended] = await Promise.all([
          cdr.user_uuid ? db.collection("user").findOne({ uuid: cdr.user_uuid }) : null,
          cdr.custom_callid ? db.collection("notes").findOne({ cdrs_uuid: cdr.custom_callid }) : null,
          cdr.campaign_uuid ? db.collection("inbound_campaign").findOne({ uuid: cdr.campaign_uuid }) : null,
          cdr.campaign_uuid ? db.collection("outbound_campaign").findOne({ uuid: cdr.campaign_uuid }) : null,
          cdr.campaign_uuid ? db.collection("blended_campaign").findOne({ uuid: cdr.campaign_uuid }) : null
        ]);

        const campaign = campaignInbound || campaignOutbound || campaignBlended;

        let disposition = "-";
        let subDisposition = "-";
        const dispositionId = cdr.hangup_cause || "-";

        if (cdr.hangup_cause && cdr.hangup_cause !== "") {
          try {
            const path = [];
            let currentUuid = cdr.hangup_cause;
            
            // Support up to 3 levels of hierarchy (Leaf -> Middle -> Root)
            for (let i = 0; i < 3; i++) {
              const disp = await db.collection("disposition").findOne({ disposition_uuid: currentUuid });
              if (!disp) break;
              path.unshift(disp.name); // Push to start to get Root -> Middle -> Leaf order
              if (!disp.parent_disposition || disp.parent_disposition === "") break;
              currentUuid = disp.parent_disposition;
            }

            if (path.length > 0) {
              disposition = path[0]; // Root level
              subDisposition = path[1] || "-"; // Next level
            }
          } catch (e) {
            logger.warn("[WEBHOOK] Disposition hierarchy lookup failed", { 
              error: e.message, 
              hangup_cause: cdr.hangup_cause 
            });
          }
        }

        return {
          rigningoData: [
            {
              dialerId: cdr.uuid || "-",
              uniqueId: cdr.uuid || "-",
              agentId: cdr.user_uuid || "-",
              agentName: user?.fullname || user?.username || "-",
              callDirection: cdr.direction || "-",
              callDate: this.convertUtcToIst(cdr.callstart) || "-",
              endDate: cdr.callend,
              callDuration: cdr.total_duration || "-",
              dispositionId: dispositionId,
              disposition: disposition,
              subDisposition: subDisposition,
              comment: note?.comment || "-",
              hangUpBy: cdr.disconnected_by === "0" ? "Agent" : 
                        cdr.disconnected_by === "1" ? "Customer" : 
                        cdr.disconnected_by === "2" ? "System" : 
                        cdr.disconnected_by || "-",
              campaignId: cdr.campaign_uuid || "-",
              campaignName: campaign?.name || "-",
              customerName: "-",
              recordingUrl: cdr.recording_path ? (config.webhooks.recordingBaseUrl || "") + cdr.recording_path : "",
              companyId: cdr.tenant_uuid || "-",
              campaignMobile: cdr.destination_number || "-"
            }
          ]
        };
      }
      // Special case for Sampann Finance
      if (cdr.tenant_uuid === "2d224c2f-2c6d-4769-8f3b-a67e54db3b80"){
        const [user, lead, campaign, notes] = await Promise.all([
          cdr.user_uuid ? db.collection("user").findOne({ uuid: cdr.user_uuid }) : null,
          cdr.lead_uuid ? db.collection("lead_management").findOne({ lead_management_uuid: cdr.lead_uuid }) : null,
          cdr.campaign_uuid ? db.collection("blended_campaign").findOne({ uuid: cdr.campaign_uuid }) || 
                              db.collection("inbound_campaign").findOne({ uuid: cdr.campaign_uuid }) ||
                              db.collection("outbound_campaign").findOne({ uuid: cdr.campaign_uuid }) : null,
          cdr.custom_callid ? db.collection("notes").findOne({ cdrs_uuid: cdr.custom_callid }) : null
        ]);

        // Format duration to HH:MM:SS
        const formatDuration = (seconds) => {
          if (!seconds || isNaN(seconds)) return "00:00:00";
          const sec = parseInt(seconds) || 0;
          const h = Math.floor(sec / 3600).toString().padStart(2, '0');
          const m = Math.floor((sec % 3600) / 60).toString().padStart(2, '0');
          const s = Math.floor(sec % 60).toString().padStart(2, '0');
          return `${h}:${m}:${s}`;
        };

        // Map dial method to readable format
        const dialMethodMap = {
          "1": "Auto Dial",
          "2": "Manual Dial",
          "3": "Preview Dial"
        };
        const dialMethod = dialMethodMap[String(cdr.dial_method)] || "-";

        // Map disconnected by to readable format
        const disconnectedByMap = {
          "0": "Agent",
          "1": "Customer",
          "2": "System"
        };
        const disconnectedBy = disconnectedByMap[String(cdr.disconnected_by)] || "-";

        // Get IVR/CQ Name
        let ivrCqName = "-";
        if (cdr.pbx_feature === "ivr" && cdr.pbx_feature_uuid) {
          const ivrRecord = await db.collection("ivr").findOne({ uuid: cdr.pbx_feature_uuid });
          ivrCqName = ivrRecord?.name || "-";
        } else if (cdr.pbx_feature === "call_queue" && cdr.pbx_feature_uuid) {
          const queueRecord = await db.collection("call_queue").findOne({ uuid: cdr.pbx_feature_uuid });
          ivrCqName = queueRecord?.name || "-";
        }

        // Get user group name
        let userGroupName = "-";
        if (user?.user_group_uuid) {
          const userGroup = await db.collection("user_group").findOne({ uuid: user.user_group_uuid });
          userGroupName = userGroup?.name || "-";
        }

        // Get lead group name
        let leadGroupName = "-";
        if (lead?.lead_group_uuid) {
          const leadGroup = await db.collection("lead_group").findOne({ uuid: lead.lead_group_uuid });
          leadGroupName = leadGroup?.name || "-";
        }

        // Extract disposition hierarchy (ROOT -> ... -> CHILD)
        let disposition = "-";
        let subDisposition1 = "-";
        let subDisposition2 = "-";
        if (cdr.hangup_cause && cdr.hangup_cause !== "") {
          try {
            const path = [];
            let currentUuid = cdr.hangup_cause;
            
            // Support up to 3 levels of hierarchy
            for (let i = 0; i < 3; i++) {
              const disp = await db.collection("disposition").findOne({ disposition_uuid: currentUuid });
              if (!disp) break;
              path.unshift(disp.name); // Push to start to get Root -> Middle -> Leaf order
              if (!disp.parent_disposition || disp.parent_disposition === "") break;
              currentUuid = disp.parent_disposition;
            }

            if (path.length > 0) {
              disposition = path[0] || "-";
              subDisposition1 = path[1] || "-";
              subDisposition2 = path[2] || "-";
            }
          } catch (e) {
            logger.warn("[WEBHOOK] Disposition hierarchy lookup failed for Sampann", { 
              error: e.message, 
              hangup_cause: cdr.hangup_cause 
            });
          }
        }

        return {
          "Start Date": this.convertUtcToIst(cdr.callstart) || "-",
          "End Date": this.convertUtcToIst(cdr.callend) || "-",
          "Caller ID Number": cdr.caller_id_number || "-",
          "DID / Phone Number": cdr.destination_number || "-",
          "User ID": cdr.sip_user || cdr.user_uuid || "-",
          "Full Name": user?.fullname || user?.username || "-",
          "User Group Name": userGroupName,
          "IVR/CQ Name": ivrCqName,
          "Duration": formatDuration(cdr.total_duration),
          "Answer Time": formatDuration(cdr.billsecond),
          "Hold Time": formatDuration(cdr.hold_time),
          "Waiting Time": formatDuration(cdr.waiting_time),
          "Ringing Time": formatDuration(cdr.ringing_time),
          "Campaign": campaign?.name || "-",
          "Lead Group Name": leadGroupName,
          "Dial Method": dialMethod,
          "Disconnected By": disconnectedBy,
          "Disposition": disposition,
          "Sub Disposition 1": subDisposition1,
          "Sub Disposition 2": subDisposition2,
          "Direction": cdr.direction ? (cdr.direction === "inbound" ? "Inbound" : "Outbound") : "-",
          "Comment": notes?.comment || "-",
          "Recording": cdr.recording_path ? (config.webhooks.recordingBaseUrl || "") + cdr.recording_path : "-"
        };
      } 

    const [user, tenant] = await Promise.all([
      cdr.user_uuid ? db.collection("user").findOne({ uuid: cdr.user_uuid }) : null,
      cdr.tenant_uuid ? db.collection("tenant").findOne({ uuid: cdr.tenant_uuid }) : null
    ]);

    const data = {
      time: this.convertUtcToIst(cdr.createdAt),
      call_id: cdr.uuid,
      call_status: cdr.disposition,
      agent_id: cdr.user_uuid,
      agent_name: user?.fullname || user?.username,
      recording_file: cdr.recording_path
        ? (config.webhooks.recordingBaseUrl || "") + cdr.recording_path
        : undefined,
      team_id: undefined,
      team_name: undefined,
      client_id: cdr.tenant_uuid,
      client_name: tenant?.tenant_name,
      client_source: undefined,
      client_status: undefined,
      client_last_transaction_date: undefined,
      client_converted_date: undefined,
      client_value: undefined,
      client_country: undefined,
      client_acquisition_date: undefined,
      client_is_converted: undefined,
      client_campaign_id: cdr.campaign_uuid,
      billsecond: cdr.billsecond,
      waiting_time: cdr.waiting_time,
      total_duration: cdr.total_duration,
      ringing_time: cdr.ringing_time
    };

    // remove undefined / null / "-" values
    const cleaned = Object.fromEntries(
      Object.entries(data).filter(
        ([_, v]) => v !== undefined && v !== null && v !== "" && v !== "-"
      )
    );

    return cleaned;

  } catch (error) {
    logger.error("[WEBHOOK] CDR transformation failed, sending original data", {
      error: error.message,
      cdrUuid: cdr.uuid
    });
    return cdr;
  }
}

  /**
   * Log webhook delivery to database
   * @param {Object} deliveryInfo - Delivery information
   */
  async logWebhookDelivery(deliveryInfo) {
    try {
      const db = getDatabase();
      const logsCollection = db.collection("webhook_delivery_logs");

      await logsCollection.insertOne({
        ...deliveryInfo,
        timestamp: new Date().toISOString(),
        createdAt: new Date(),
      });
    } catch (error) {
      logger.error("[WEBHOOK] Failed to log delivery", {
        error: error.message,
        deliveryInfo,
      });
    }
  }

  /**
   * Get delivery statistics
   */
  getStats() {
    const successRate =
      this.deliveryStats.total > 0
        ? (
            (this.deliveryStats.success / this.deliveryStats.total) *
            100
          ).toFixed(2)
        : "0.00";

    return {
      ...this.deliveryStats,
      successRate: `${successRate}%`,
    };
  }

}

// Export singleton instance
const webhookManager = new WebhookManager();
export default webhookManager;