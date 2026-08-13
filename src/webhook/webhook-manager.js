 
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
import { type } from "os";

// Transform strategies:
//   "legacy" -> hardcoded per-tenant if-blocks in transformCDR (being phased out)
//   "ui"     -> configurable template stored on the webhook doc
// UI pipeline: parseTransformTemplate -> expandTemplatePaths -> buildCdrTemplateContext
//              -> renderTemplate -> [stripEmptyValues] -> [wrap] -> deliver
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
    if (!utcDateTimeStr) {
      return utcDateTimeStr;
    }

    if (
      typeof utcDateTimeStr !== "string" &&
      !(utcDateTimeStr instanceof Date)
    ) {
      return utcDateTimeStr;
    }

    try {
      let utcMs;

      if (utcDateTimeStr instanceof Date) {
        // e.g. cdr.createdAt, which the Mongo driver returns as a native Date (BSON
        // dates are stored as UTC epoch ms with no timezone ambiguity).
        utcMs = utcDateTimeStr.getTime();
      } else {
        // Parse the date/time components explicitly and anchor them to UTC via Date.UTC.
        // `new Date("YYYY-MM-DD HH:mm:ss")` (space-separated, no offset) is parsed as
        // server-local time by the JS Date constructor, not UTC — on a server whose
        // timezone is already IST, the +5:30 added below and the getUTC*() readback
        // cancel out and this becomes a silent no-op. Matching components manually
        // avoids relying on the server's timezone.
        const match = utcDateTimeStr
          .trim()
          .match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);

        if (!match) {
          logger.warn("[WEBHOOK] Invalid UTC datetime format", {
            utcDateTimeStr,
          });
          return utcDateTimeStr;
        }

        const [
          ,
          matchYear,
          matchMonth,
          matchDay,
          matchHours,
          matchMinutes,
          matchSeconds,
        ] = match;
        utcMs = Date.UTC(
          Number(matchYear),
          Number(matchMonth) - 1,
          Number(matchDay),
          Number(matchHours),
          Number(matchMinutes),
          Number(matchSeconds),
        );
      }

      if (isNaN(utcMs)) {
        logger.warn("[WEBHOOK] Invalid UTC datetime format", {
          utcDateTimeStr,
        });
        return utcDateTimeStr;
      }

      // IST is UTC+5:30
      const istDate = new Date(utcMs + 5.5 * 60 * 60 * 1000);

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
   * Builds the recording URL sent in webhook payloads. Recordings are now uploaded
   * directly to S3/Spaces, so recording_path is already a full public URL in that
   * case — prefixing it with recordingBaseUrl (the backend's own download route)
   * would double-wrap it into a broken link. Only bare/relative paths (the older
   * local-storage layout) still need that prefix.
   * @param {string} recordingPath
   * @param {*} fallback - returned as-is when recordingPath is falsy
   */
  buildRecordingUrl(recordingPath, fallback = "") {
    if (!recordingPath) return fallback;
    return /^https?:\/\//i.test(recordingPath)
      ? recordingPath
      : (config.webhooks.recordingBaseUrl || "") + recordingPath;
  }

  /**
   * Picks the reminder_date_time of the most recently scheduled follow-up
   * @param {Array} followUps - follow_up documents joined on lead_management_uuid
   * @returns {string} reminder_date_time of the latest follow-up, or "" if none
   */
  getLatestFollowUpReminder(followUps) {
    if (!Array.isArray(followUps) || !followUps.length) return "";

    let latestReminder = "";
    let latestTime = 0;

    for (const item of followUps) {
      const time = new Date(item?.reminder_date_time).getTime();
      if (time > latestTime) {
        latestTime = time;
        latestReminder = item?.reminder_date_time || "";
      }
    }

    return latestReminder;
  }

  /**
   * Normalizes an Indian phone number to a bare 10-digit string — strips non-digits,
   * strips a leading 91/+91 country code, then keeps the last 10 digits.
   * @param {string} phone
   * @returns {string} normalized number, or "" if phone is falsy
   */
  normalizeIndianNumber(phone) {
    if (!phone) return "";
    let num = String(phone).replace(/\D/g, "");
    num = num.replace(/^\+?91/, "");
    if (num.length > 10) {
      num = num.slice(-10);
    }
    return num;
  }

  /**
   * Deliver voice broadcast data to all active webhooks for a tenant
   * Webhooks must have webhook_type = "voice_broadcast" to receive these events.
   * @param {Object} vbData - voicebroadcast_details document
   * @param {string} tenantUuid - Tenant UUID
   */
  async deliverVBData(vbData, tenantUuid = null) {
    try {
      const db = getDatabase();
      const webhooksCollection = db.collection("web_hook");

      const filter = {
        is_active: true,
        webhook_type: "voice_broadcast",
      };

      if (tenantUuid) {
        filter.tenant_uuid = tenantUuid;
      }

      const webhooks = await webhooksCollection.find(filter).toArray();

      if (webhooks.length === 0) {
        logger.debug("[VB_WEBHOOK] No active voice_broadcast webhooks found", { tenantUuid });
        return;
      }

      logger.info("[VB_WEBHOOK] Delivering to webhooks", {
        vbUuid: vbData.uuid,
        webhookCount: webhooks.length,
        tenantUuid,
      });

      // Enrich the raw VB document with lead, broadcast, disposition info
      const enrichedPayload = await this.transformVBData(vbData);

      const deliveryPromises = webhooks.map((webhook) => {
        const webhookConfig = {
          uuid: webhook.uuid,
          url: (webhook.webhook_url || "").trim(),
          method: webhook.method || "POST",
          headers: webhook.headers || {},
          timeout: webhook.timeout || 30000,
          retries: webhook.max_retries || 3,
          retryDelay: webhook.retry_delay || 2000,
          tenantUuid: webhook.tenant_uuid,
          name: webhook.name || "VB Webhook",
          bearerToken: webhook.bearer_token || null,
        };
        return this._httpDeliverWithRetry(webhookConfig, enrichedPayload, 'voice_broadcast');
      });

      await Promise.allSettled(deliveryPromises);
    } catch (error) {
      logger.error("[VB_WEBHOOK] Delivery failed", {
        error: error.message,
        tenantUuid,
      });
    }
  }

  /**
   * Transform raw voicebroadcast_details document into a full webhook payload.
   * Enriches with lead name, broadcast name, disposition name, and tenant name
   * so the receiving end has all UI-visible fields in one payload.
   * @param {Object} vbData - Raw voicebroadcast_details document from MongoDB
   * @returns {Object} Enriched payload
   */
  async transformVBData(vbData) {
    try {
      const db = getDatabase();

      const [lead, broadcast, disposition, tenant] = await Promise.all([
        vbData.lead_uuid
          ? db.collection("lead_management").findOne({ lead_management_uuid: vbData.lead_uuid })
          : null,
        vbData.voicebroadcast_uuid
          ? db.collection("voice_broadcast").findOne({ uuid: vbData.voicebroadcast_uuid })
          : null,
        vbData.hangup_cause
          ? db.collection("disposition").findOne({ disposition_uuid: vbData.hangup_cause })
          : null,
        vbData.tenant_uuid
          ? db.collection("tenant").findOne({ uuid: vbData.tenant_uuid })
          : null,
      ]);

      const typeLabels = {
        "0": "Survey",
        "1": "IVR",
        "2": "Not Interested",
        "3": "DNC",
        "":  "No Answer",
      };

      const formatDuration = (seconds) => {
        const s = parseInt(seconds) || 0;
        const h = Math.floor(s / 3600).toString().padStart(2, "0");
        const m = Math.floor((s % 3600) / 60).toString().padStart(2, "0");
        const sec = Math.floor(s % 60).toString().padStart(2, "0");
        return `${h}:${m}:${sec}`;
      };

      return {
        date:               this.convertUtcToIst(vbData.callstart),
        caller_id_number:   vbData.caller_id_number,
        caller_id_name:     vbData.caller_id_name || "",
        destination_number: vbData.destination_number,
        voice_broadcast:    broadcast?.name || "",
        duration:           formatDuration(vbData.total_duration),
        ringing_time:       formatDuration(vbData.ringing_time),
        answer_time:        formatDuration(vbData.billsecond),
        DTMF:               vbData.DTMF || "",
        disposition:        disposition?.name || "",
        lead:               lead ? `${lead.first_name || ""} ${lead.last_name || ""}`.trim() : "",
        lead_custom_fields: lead?.custom_fields || {},
        type:               typeLabels[vbData.type] ?? "Unknown",
        survey_info:        vbData.survey_info || "",
        ivr_feedback_score: vbData.ivr_feedback_score || "",
        recording_path:     this.buildRecordingUrl(vbData.recording_path),
      };
    } catch (error) {
      logger.error("[VB_WEBHOOK] Transform failed, sending raw data", {
        error: error.message,
        vbUuid: vbData.uuid,
      });
      return vbData;
    }
  }

  /**
   * Deliver a CDR to all active webhooks for a tenant. Each webhook applies its OWN
   * configured delay (webhook.delay, falling back to config.webhooks.delay) and
   * re-fetches the CDR fresh from the DB right after that wait — not once upfront
   * for the whole tenant — so a webhook configured for a 5-minute delay actually
   * captures whatever disposition/comment got set during ITS OWN window, independent
   * of any other webhook's (possibly much shorter, or unset/default) delay for the
   * same tenant. Previously a single tenant-wide delay was picked via an unsorted
   * findOne() over the tenant's webhooks — whichever webhook that returned "won"
   * for every other webhook too, regardless of what delay each one actually had
   * configured (this is exactly the bug: a tenant with 3 active CDR webhooks where
   * only one has a custom delay would silently use the DEFAULT delay for all three,
   * including the one that was actually configured).
   * @param {string} cdrUuid - CDR uuid to deliver
   * @param {string} collectionName - Mongo collection the CDR lives in
   * @param {string} tenantUuid - Tenant filter
   */
  async deliverCDR(cdrUuid, collectionName, tenantUuid = null) {
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

      // campaign_uuid is set at call time and never changes afterward (unlike
      // disposition/comment, which is why the full CDR is still re-fetched per
      // webhook after its own delay below) — so campaign-mismatched webhooks can be
      // dropped right now, before waiting out anyone's delay or fetching the full
      // document for a delivery that was never going to happen. Only pay for the
      // lookup when some webhook actually has campaign filtering configured.
      const hasCampaignFilters = webhooks.some(
        (webhook) => Array.isArray(webhook.campaign_uuids) && webhook.campaign_uuids.length > 0,
      );

      let matchingWebhooks = webhooks;
      if (hasCampaignFilters) {
        const cdrForCampaignCheck = await db
          .collection(collectionName)
          .findOne({ uuid: cdrUuid }, { projection: { campaign_uuid: 1 } });
        const cdrCampaignUuid = cdrForCampaignCheck?.campaign_uuid;

        matchingWebhooks = webhooks.filter((webhook) => {
          if (!Array.isArray(webhook.campaign_uuids) || webhook.campaign_uuids.length === 0) {
            return true;
          }
          const matches = webhook.campaign_uuids.includes(cdrCampaignUuid);
          if (!matches) {
            logger.debug("[WEBHOOK] Skipping delivery — CDR campaign not in webhook's configured campaigns", {
              webhookUuid: webhook.uuid,
              cdrCampaignUuid,
            });
          }
          return matches;
        });

        if (matchingWebhooks.length === 0) {
          logger.debug("[WEBHOOK] No webhooks match this CDR's campaign", { cdrUuid, tenantUuid });
          return;
        }
      }

      logger.debug("[WEBHOOK] Delivering CDR to webhooks", {
        cdrUuid,
        webhookCount: matchingWebhooks.length,
      });

      // Each webhook waits its own delay (in parallel — these run concurrently, not
      // serialized) then re-fetches the CDR fresh before transforming/delivering.
      const deliveryPromises = matchingWebhooks.map(async (webhook) => {
        const webhookDelay = Number(webhook.delay) || config.webhooks.delay;

        if (webhookDelay > 0) {
          logger.debug(`[WEBHOOK] Waiting for ${webhookDelay}ms delay before processing`, {
            webhookUuid: webhook.uuid,
            webhookName: webhook.name,
            delay: webhookDelay,
          });
          await new Promise((resolve) => setTimeout(resolve, webhookDelay));
        }

        const freshCdr = await db.collection(collectionName).findOne({ uuid: cdrUuid });
        if (!freshCdr) {
          logger.warn("[WEBHOOK] CDR not found in database at delivery time, skipping", {
            cdrUuid,
            webhookUuid: webhook.uuid,
          });
          return;
        }

        const webhookConfig = {
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
          transform_strategy:
            webhook.transform_strategy || webhook.transform_type || "legacy",
          transform_template: webhook.transform_template || null,
          wrapInJson: webhook.wrapInJson || false,
          wrapperKey: webhook.wrapperKey || null,
          wrapperType: webhook.wrapperType || "object",
          stripEmpty: webhook.stripEmpty || false,
        };
        return this.deliverToWebhook(webhookConfig, freshCdr);
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
    if (task.type === 'vb_webhook_delivery') {
      const { vbUuid, tenantUuid } = task;
      try {
        logger.info('[VB_WEBHOOK] Processing VB task from queue', { vbUuid, tenantUuid });
        const db = getDatabase();
        const vbData = await db.collection('voicebroadcast_details').findOne({ uuid: vbUuid });
        if (!vbData) {
          logger.warn('[VB_WEBHOOK] VB record not found, skipping', { vbUuid });
          return;
        }
        await this.deliverVBData(vbData, tenantUuid);
      } catch (error) {
        logger.error('[VB_WEBHOOK] Queue task processing failed', { error: error.message, vbUuid });
        throw error;
      }
      return;
    }

    const { cdrUuid, collectionName, tenantUuid } = task;

    try {
      logger.info('[WEBHOOK] Processing task from queue', {
        cdrUuid,
        collectionName,
        tenantUuid,
      });

      // Confirm the CDR exists before scheduling any per-webhook delivery work.
      // Each matching webhook applies its OWN configured delay and re-fetches the
      // CDR fresh right before delivering — see deliverCDR.
      const db = getDatabase();
      const cdrExists = await db
        .collection(collectionName)
        .findOne({ uuid: cdrUuid }, { projection: { _id: 1 } });

      if (!cdrExists) {
        logger.warn('[WEBHOOK] CDR not found in database, skipping delivery', {
          cdrUuid,
          collectionName,
        });
        return;
      }

      await this.deliverCDR(cdrUuid, collectionName, tenantUuid);

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
   * HTTP delivery with retry logic — sends an already-transformed payload.
   * Used by both deliverToWebhook (CDR) and deliverVBData (VB) so each can
   * apply its own transformation before handing off to this shared sender.
   * @param {Object} webhook - Webhook configuration
   * @param {Object} transformedData - Pre-transformed payload ready to send
   */
  async _httpDeliverWithRetry(webhook, transformedData, webhookType = 'cdr') {
    const startTime = Date.now();
    let lastError = null;
    let lastErrorDetails = null;

    // Headers and the equivalent curl command don't change between retry attempts —
    // computed once so it can be persisted on the delivery log (success or failure)
    // for the UI's "copy as cURL" action, instead of only ever reaching a log line.
    const headers = {
      "Content-Type": "application/json",
      "User-Agent": "curl/7.81.0",
      "Accept": "*/*",
      ...webhook.headers,
    };

    if (webhook.bearerToken) {
      headers["Authorization"] = `Bearer ${webhook.bearerToken}`;
    }

    const loggedHeaders = { ...headers };
    if (loggedHeaders["Authorization"]) {
      loggedHeaders["Authorization"] = `Bearer ${webhook?.bearerToken}`;
    }

    const curlHeaders = Object.entries(headers)
      .map(([key, value]) => `-H '${key}: ${value}'`)
      .join(" ");
    const curlPayload = JSON.stringify(transformedData);
    const curlCommand = `curl -X ${webhook.method} '${webhook.url}' ${curlHeaders} -d '${curlPayload}'`;

    for (let attempt = 1; attempt <= webhook.retries; attempt++) {
      try {
        logger.debug("[WEBHOOK] Attempting delivery", {
          webhookName: webhook.name,
          webhookUuid: webhook.uuid,
          webhookUrl: webhook.url,
          attempt,
          maxRetries: webhook.retries,
          payloadUuid: transformedData.uuid,
          webhookType,
        });

        logger.info("[WEBHOOK_DEBUG] Exhaustive request details", {
          webhookName: webhook.name,
          url: webhook.url,
          method: webhook.method,
          headers: loggedHeaders,
          payload: {
            uuid: transformedData.uuid,
            tenant_uuid: transformedData.tenant_uuid,
          },
          transformedData: transformedData,
        });

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
          payloadUuid: transformedData.uuid,
          webhookType,
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
          tenantUuid: webhook.tenantUuid,
          payloadUuid: transformedData.uuid,
          webhook_type: webhookType,
          status: "success",
          statusCode: response.status,
          statusText: response.statusText,
          responseData: response.data,
          payload: transformedData,
          curlCommand,
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
          payloadUuid: transformedData.uuid,
          webhookType,
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
        tenantUuid: webhook.tenantUuid,
        payloadUuid: transformedData.uuid,
        webhook_type: webhookType,
        status: "failed",
        statusCode: lastError.response?.status || 0,
        attempt: webhook.retries,
        duration,
        error: lastError.message,
        errorCode: lastError.code,
        errorType: lastError.name,
        statusText: lastError.response?.statusText,
        responseData: lastError.response?.data,
        payload: transformedData,
        curlCommand,
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
   * Deliver CDR to a specific webhook — transforms CDR first, then sends.
   * @param {Object} webhook - Webhook configuration
   * @param {Object} cdr - Raw CDR data (will be transformed via transformCDR)
   */
  async deliverToWebhook(webhook, cdr) {
    const transformedData = await this.transformCDR(cdr, webhook);
    return this._httpDeliverWithRetry(webhook, transformedData, "cdr");
  }

  // Walks a dotted path (e.g. "user.fullname") into the context; returns undefined
  // if any segment along the way is missing.
  resolveTemplatePath(path, context) {
    const parts = String(path || "")
      .split(".")
      .filter(Boolean);
    let current = context;
    for (const part of parts) {
      if (current === undefined || current === null) {
        return undefined;
      }
      current = current[part];
    }
    return current;
  }

  // Substitutes {{path}} and {{path||default}} placeholders in a single string;
  // objects are JSON-stringified, missing values fall back to the default or "".
  renderTemplateString(value, context) {
    return String(value).replace(
      /\{\{\s*([^}|\s]+)(?:\|\|([^}]+))?\s*\}\}/g,
      (_, path, defaultValue) => {
        const resolved = this.resolveTemplatePath(path, context);
        if (resolved === undefined || resolved === null) {
          return defaultValue !== undefined ? defaultValue : "";
        }
        if (typeof resolved === "object") {
          try {
            return JSON.stringify(resolved);
          } catch {
            return defaultValue !== undefined ? defaultValue : "";
          }
        }
        return String(resolved);
      },
    );
  }

  // Recursively renders every string in the template — both keys and values,
  // across arrays and nested objects — via renderTemplateString.
  //
  // Values that are a WHOLE single placeholder (e.g. "{{custom_fields}}", nothing
  // else in the string) pass through with their original type intact — object,
  // array, number, boolean — instead of being JSON.stringify'd into a string. Keys
  // always go through renderTemplateString and stay strings, since object keys
  // can't be anything else. Placeholders embedded inside a larger string (e.g.
  // "id-{{cdr.uuid}}") always stringify, same as before.
  renderTemplate(template, context) {
    if (Array.isArray(template)) {
      return template.map((item) => this.renderTemplate(item, context));
    }

    if (template && typeof template === "object") {
      return Object.fromEntries(
        Object.entries(template).map(([key, value]) => [
          typeof key === "string"
            ? this.renderTemplateString(key, context)
            : key,
          this.renderTemplate(value, context),
        ]),
      );
    }

    if (typeof template === "string") {
      const wholeValueMatch = template.match(
        /^\{\{\s*([^}|\s]+)(?:\|\|([^}]+))?\s*\}\}$/,
      );
      if (wholeValueMatch) {
        const [, path, defaultValue] = wholeValueMatch;
        const resolved = this.resolveTemplatePath(path, context);
        if (resolved !== undefined && resolved !== null) {
          return resolved;
        }
        return defaultValue !== undefined ? defaultValue : "";
      }

      return this.renderTemplateString(template, context);
    }

    return template;
  }

  // Recursively drops keys whose value is undefined/null/""/"-"; only applied
  // when the webhook has stripEmpty set.
  stripEmptyValues(value) {
    if (Array.isArray(value)) {
      return value.map((item) => this.stripEmptyValues(item));
    }

    if (value && typeof value === "object") {
      return Object.fromEntries(
        Object.entries(value)
          .filter(
            ([, v]) => v !== undefined && v !== null && v !== "" && v !== "-",
          )
          .map(([key, v]) => [key, this.stripEmptyValues(v)]),
      );
    }

    return value;
  }

  // Normalizes the stored template (JSON string or object) into an object,
  // or null if it's missing/invalid.
  parseTransformTemplate(transformTemplate) {
    if (transformTemplate === undefined || transformTemplate === null) {
      return null;
    }

    if (typeof transformTemplate === "string") {
      try {
        const parsed = JSON.parse(transformTemplate);
        return typeof parsed === "object" ? parsed : null;
      } catch {
        return null;
      }
    }

    if (typeof transformTemplate === "object") {
      return transformTemplate;
    }

    return null;
  }

  // Turns flat dotted/bracketed keys (e.g. "a.b", "items[0].id") into nested
  // objects/arrays, so the UI-authored template maps onto an arbitrarily shaped payload.
  expandTemplatePaths(template) {
    if (Array.isArray(template)) {
      return template.map((item) => this.expandTemplatePaths(item));
    }

    if (template && typeof template === "object") {
      const result = {};

      const parsePath = (key) => {
        const segments = [];
        const regex = /([^.[\]]+)|\[(\d+)\]/g;
        let match;
        while ((match = regex.exec(key))) {
          if (match[1] !== undefined) {
            segments.push(match[1]);
          } else if (match[2] !== undefined) {
            segments.push(Number(match[2]));
          }
        }
        return segments;
      };

      const setPathValue = (obj, pathSegments, value) => {
        let current = obj;

        for (let i = 0; i < pathSegments.length; i++) {
          const segment = pathSegments[i];
          const nextSegment = pathSegments[i + 1];
          const isLast = i === pathSegments.length - 1;
          const nextIsArrayIndex = typeof nextSegment === "number";

          if (isLast) {
            current[segment] = value;
            return;
          }

          if (current[segment] === undefined || current[segment] === null) {
            current[segment] = nextIsArrayIndex ? [] : {};
          } else if (nextIsArrayIndex && !Array.isArray(current[segment])) {
            current[segment] = [];
          } else if (!nextIsArrayIndex && Array.isArray(current[segment])) {
            current[segment] = {};
          }

          current = current[segment];
        }
      };

      for (const [key, value] of Object.entries(template)) {
        const pathSegments =
          key.includes(".") || key.includes("[") ? parsePath(key) : null;

        const expandedValue = this.expandTemplatePaths(value);

        if (pathSegments && pathSegments.length > 1) {
          setPathValue(result, pathSegments, expandedValue);
        } else if (pathSegments && pathSegments.length === 1) {
          result[pathSegments[0]] = expandedValue;
        } else {
          result[key] = expandedValue;
        }
      }

      return result;
    }

    return template;
  }

  // The single data source for UI templates: runs all DB joins and derives every
  // value a template can reference, then returns one flat object of template paths.
  async buildCdrTemplateContext(cdr) {
    const db = getDatabase();

    // (a) Core joins — independent of each other, safe to run in one Promise.all.
    const [
      user,
      tenant,
      lead,
      disposition,
      notes,
      campaignInbound,
      campaignOutbound,
      campaignBlended,
      followUps,
      extension,
    ] = await Promise.all([
      cdr.user_uuid
        ? db.collection("user").findOne({ uuid: cdr.user_uuid })
        : null,
      cdr.tenant_uuid
        ? db.collection("tenant").findOne({ uuid: cdr.tenant_uuid })
        : null,
      cdr.lead_uuid
        ? db
            .collection("lead_management")
            .findOne({ lead_management_uuid: cdr.lead_uuid })
        : null,
      cdr.hangup_cause
        ? db
            .collection("disposition")
            .findOne({ disposition_uuid: cdr.hangup_cause })
        : null,
      cdr.custom_callid
        ? db.collection("notes").findOne({ cdrs_uuid: cdr.custom_callid })
        : null,
      cdr.campaign_uuid
        ? db.collection("inbound_campaign").findOne({ uuid: cdr.campaign_uuid })
        : null,
      cdr.campaign_uuid
        ? db
            .collection("outbound_campaign")
            .findOne({ uuid: cdr.campaign_uuid })
        : null,
      cdr.campaign_uuid
        ? db.collection("blended_campaign").findOne({ uuid: cdr.campaign_uuid })
        : null,
      cdr.lead_uuid
        ? db
            .collection("follow_up")
            .find({ lead_management_uuid: cdr.lead_uuid })
            .toArray()
        : [],
      cdr.extension_uuid
        ? db.collection("extensions").findOne({ uuid: cdr.extension_uuid })
        : null,
    ]);

    // (b) Dependent joins — user_group/lead_group need the user/lead resolved in (a);
    // ivr/call_queue are conditional on cdr.pbx_feature. Must run after (a) resolves.
    const [userGroup, leadGroup, ivrRecord, callQueueRecord] =
      await Promise.all([
        user?.user_group_uuid
          ? db
              .collection("user_group")
              .findOne({ uuid: user.user_group_uuid })
          : null,
        lead?.lead_group_uuid
          ? db
              .collection("lead_group")
              .findOne({ uuid: lead.lead_group_uuid })
          : null,
        cdr.pbx_feature === "ivr" && cdr.pbx_feature_uuid
          ? db.collection("ivr").findOne({ uuid: cdr.pbx_feature_uuid })
          : null,
        cdr.pbx_feature === "call_queue" && cdr.pbx_feature_uuid
          ? db.collection("call_queue").findOne({ uuid: cdr.pbx_feature_uuid })
          : null,
      ]);

    const campaign =
      campaignInbound || campaignOutbound || campaignBlended || null;
    const callstartIst = this.convertUtcToIst(cdr.callstart);
    const dateTimeParts = String(callstartIst || "").split(" ");
    const formatDuration = (seconds) => {
      const s = parseInt(seconds) || 0;
      const h = Math.floor(s / 3600)
        .toString()
        .padStart(2, "0");
      const m = Math.floor((s % 3600) / 60)
        .toString()
        .padStart(2, "0");
      const sec = Math.floor(s % 60)
        .toString()
        .padStart(2, "0");
      return `${h}:${m}:${sec}`;
    };

    // (c) Disposition hierarchy walk — climbs parent_disposition up to 3 levels
    // (Leaf -> Middle -> Root) so templates can reference root/sub/sub2 independently.
    const resolveDispositionHierarchy = async (dispositionUuid) => {
      const path = [];
      let currentUuid = dispositionUuid;

      for (let i = 0; i < 3; i += 1) {
        if (!currentUuid) break;
        const disp = await db
          .collection("disposition")
          .findOne({ disposition_uuid: currentUuid });
        if (!disp) break;
        path.unshift(disp.name);
        if (!disp.parent_disposition || disp.parent_disposition === "") {
          break;
        }
        currentUuid = disp.parent_disposition;
      }

      return {
        root: path[0] || "",
        sub: path[1] || "",
        sub2: path[2] || "",
      };
    };

    const {
      root: disposition_root,
      sub: disposition_sub,
      sub2: disposition_sub2,
    } = await resolveDispositionHierarchy(cdr.hangup_cause);

    const dialMethodLabels = {
      0: "Manual Dial",
      1: "Auto Dial",
      2: "Preview Dial",
      3: "Predictive Dial",
    };
    const disconnectedByLabels = { 0: "Agent", 1: "Customer", 2: "System" };

    // (d) Derived Vyapar fields — replicate the conditional Connected/Abandoned/
    // Not Connected logic from Vyapar's hardcoded transformCDR branch, since this
    // can't be expressed as a plain template placeholder.
    const actualTalktimeVyapar =
      cdr.direction === "inbound"
        ? cdr.actual_talktime
        : String(cdr.dial_method) === "1"
          ? cdr.user_uuid && String(cdr.user_uuid).trim() !== ""
            ? cdr.billsecond
            : 0
          : cdr.billsecond;

    const callStatusVyapar =
      cdr.direction === "inbound"
        ? Number(cdr?.actual_talktime) > 0
          ? "Connected"
          : "Abandoned"
        : cdr.direction === "outbound"
          ? actualTalktimeVyapar != "0"
            ? "Connected"
            : "Not Connected"
          : "Not Connected";

    // (e) The returned context — every key here is a path a UI template can reference.
    return {
      cdr,
      user,
      tenant,
      lead,
      disposition,
      notes,
      campaign,
      agent_name: user?.fullname || user?.username || "",
      agent_display_name:
        user?.username && user?.fullname
          ? `${user.username} - ${user.fullname}`
          : user?.fullname || user?.username || "",
      extension_username: extension?.username || "",
      campaign_name: campaign?.name || "",
      comment: notes?.comment || "",
      // Most recent reminder_date_time across the lead's follow_up records
      follow_up_date: this.getLatestFollowUpReminder(followUps),
      disposition_root,
      disposition_sub,
      disposition_sub2,
      callstart_ist: callstartIst,
      call_date: dateTimeParts[0] || "",
      call_time: dateTimeParts[1] || "",
      // callend is genuinely IST-at-rest, NOT UTC — confirmed asymmetry, not a fluke:
      // callstart comes from the sip_h_P-callstart/custom_callstart dialplan variable,
      // which the FreeSWITCH Lua scripts stamp via os.date("!%Y-%m-%d %H:%M:%S") — the
      // "!" flag forces UTC. callend instead comes straight from FreeSWITCH's native
      // `end_stamp` CDR variable, which FreeSWITCH renders in the server's local system
      // time — and this deployment's FreeSWITCH boxes run on IST. So callstart is
      // deliberately UTC while callend is incidentally IST already. This mirrors every
      // legacy hardcoded block (Vyapar, convoz, Sampann), which all send cdr.callend
      // as-is. Do NOT wrap it in convertUtcToIst here — that would add a spurious extra
      // +5:30. If the FreeSWITCH server timezone is ever changed away from IST, this
      // assumption breaks and needs revisiting upstream, not patched here.
      callend_ist: cdr.callend || "",
      callend_date: String(cdr.callend || "").split(" ")[0] || "",
      callend_time: String(cdr.callend || "").split(" ")[1] || "",
      // createdAt is a genuine BSON Date (UTC-at-rest, unambiguous); convertUtcToIst
      // now accepts Date instances directly (see above) so this converts correctly.
      created_at_ist: this.convertUtcToIst(cdr.createdAt),
      recording_url: this.buildRecordingUrl(cdr.recording_path),
      total_duration_formatted: formatDuration(cdr.total_duration),
      billsecond_formatted: formatDuration(cdr.billsecond),
      ringing_time_formatted: formatDuration(cdr.ringing_time),
      waiting_time_formatted: formatDuration(cdr.waiting_time),
      hold_time_formatted: formatDuration(cdr.hold_time),
      acw_time_formatted: formatDuration(cdr.acw_time),
      actual_talktime_formatted: formatDuration(actualTalktimeVyapar),
      // "" (not null) so template placeholders always resolve to a string. Vyapar's own
      // hardcoded transformCDR branch still returns null natively and is unaffected by this;
      // use hangup_by_raw below when a UI template needs the raw-code fallback instead of "".
      hangup_by:
        cdr.disconnected_by === "0"
          ? "Agent"
          : cdr.disconnected_by === "1"
            ? "Customer"
            : cdr.disconnected_by === "2"
              ? "System"
              : "",
      hangup_by_raw:
        cdr.disconnected_by === "0"
          ? "Agent"
          : cdr.disconnected_by === "1"
            ? "Customer"
            : cdr.disconnected_by === "2"
              ? "System"
              : cdr.disconnected_by || "-",
      dial_method_label: dialMethodLabels[String(cdr.dial_method)] || "-",
      disconnected_by_label:
        disconnectedByLabels[String(cdr.disconnected_by)] || "-",
      direction_label:
        cdr.direction === "inbound"
          ? "Inbound"
          : cdr.direction === "outbound"
            ? "Outbound"
            : "-",
      user_group_name: userGroup?.name || "",
      lead_group_name: leadGroup?.name || "",
      ivr_cq_name:
        cdr.pbx_feature === "ivr"
          ? ivrRecord?.name || ""
          : cdr.pbx_feature === "call_queue"
            ? callQueueRecord?.name || ""
            : "",
      lead_name: lead?.lead_name || "",
      customer_number:
        cdr.direction === "inbound"
          ? cdr.caller_id_number || ""
          : cdr.destination_number || "",
      mobile_number_normalized: this.normalizeIndianNumber(cdr.destination_number),
      agent_number:
        cdr.direction === "outbound"
          ? cdr.caller_id_number || ""
          : cdr.destination_number || "",
      event: cdr.direction === "inbound" ? "1" : "2",
      call_status_vyapar: callStatusVyapar,
      queue_id:
        cdr.pbx_feature === "call_queue" ? cdr.pbx_feature_uuid || "" : "",
      // Stringified so renderTemplateString doesn't JSON.stringify the raw ObjectId;
      // matches what Vyapar's hardcoded call_id: cdr._id actually sends once axios
      // JSON-serializes the request body (ObjectId#toJSON returns the same hex string).
      call_id_objectid: cdr._id ? String(cdr._id) : "",
      // custom_fields is an object. A template value of exactly "{{custom_fields}}"
      // passes through as a real nested object (matching Vyapar/Shipeaso's hardcoded
      // "custom_fields": { ... } output) thanks to the whole-placeholder passthrough
      // in renderTemplate. Use custom_fields_json instead when a stringified form is
      // explicitly wanted.
      custom_fields: lead?.custom_fields || {},
      custom_fields_json: JSON.stringify(lead?.custom_fields || {}),
      // Raw CDR columns are spread LAST, so any real cdr.<field> silently overrides a
      // derived field of the same name above. New derived fields that could collide
      // with a real column name must be added AFTER this spread, not before it.
      ...cdr,
    };
  }

  /**
   * Orchestrates the UI-transform pipeline: builds the context, renders the template,
   * then applies the optional stripEmpty and wrapInJson settings.
   * This is a safe fallback for webhooks configured with transform_strategy='ui'.
   */
  async transformCDRUsingUI(cdr, webhook = {}) {
    const rawTemplate = this.parseTransformTemplate(webhook.transform_template);
    if (!rawTemplate) {
      logger.warn(
        "[WEBHOOK] UI transform selected but no valid transform_template found",
        {
          webhookUuid: webhook?.uuid,
        },
      );
      return undefined;
    }

    const template = this.expandTemplatePaths(rawTemplate);

    try {
      const context = await this.buildCdrTemplateContext(cdr);
      let result = this.renderTemplate(template, context);

      if (webhook?.stripEmpty) {
        result = this.stripEmptyValues(result);
      }

      if (webhook?.wrapInJson && webhook?.wrapperKey) {
        return webhook.wrapperType === "array"
          ? { [webhook.wrapperKey]: [result] }
          : { [webhook.wrapperKey]: result };
      }

      return result;
    } catch (error) {
      logger.error("[WEBHOOK] UI transform processing failed", {
        error: error.message,
        webhookUuid: webhook?.uuid,
      });
      return undefined;
    }
  }

  /**
   * Transform internal CDR to client-specific format
   * @param {Object} cdr - Internal CDR data
   * @param {Object|null} webhook - Webhook configuration
   * @returns {Object} Transformed data
   */
  async transformCDR(cdr, webhook = null) {
    try {
      // Used by the legacy convoz special-case below, which still branches on the
      // webhook's uuid rather than the webhook object itself.
      const webhookUuid = webhook?.uuid || null;
      const transformStrategy =
        webhook?.transform_strategy || webhook?.transform_type || "legacy";

      if (transformStrategy === "ui") {
        logger.debug("[WEBHOOK] Using UI-driven transform strategy for CDR", {
          webhookUuid: webhook?.uuid,
          tenantUuid: cdr?.tenant_uuid,
        });
        const uiResult = await this.transformCDRUsingUI(cdr, webhook);
        if (uiResult !== undefined) {
          return uiResult;
        }
        logger.warn(
          "[WEBHOOK] UI transform failed or template invalid; falling back to legacy transform",
          {
            webhookUuid: webhook?.uuid,
            tenantUuid: cdr?.tenant_uuid,
          },
        );
      }

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
          // "Incoming Webhook Payload": [
          //   {
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
              "recording_url": this.buildRecordingUrl(cdr.recording_path),
              "knowlarity_number": agent_number,
              "queue_id": cdr.pbx_feature === "call_queue" ? (cdr.pbx_feature_uuid || "") : "",
              "LegC_agent_number": "",//need these field
              "LegC_conversation_duration": "",//need these field
              "transfer_queue_name": "",//need these field
              "lead_uuid": cdr.lead_uuid || "",
              "custom_fields": lead?.custom_fields || {},
            }
        //   ]
        // };
      }

      // Special case for convoz - V-Xpress NMC, 1finance and percorner
      if ((cdr.tenant_uuid === "1f84dd0e-f557-4712-94a1-675d9b5e42e1" || cdr.tenant_uuid === "cf98c767-2404-484e-888b-e53897315d5a") || cdr.tenant_uuid === "c1794f5d-8cf3-4e3e-972c-eba0262bef40" && 
          (webhookUuid === "e5d92ded-4ade-4ca3-97df-7ac7edd9d91a" || webhookUuid === "6a6da557-4747-4d4b-b39f-5948e65b726e" || webhookUuid === "e63cfaeb-c199-4bf2-899b-8deb78baaf7d")) {
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
              recordingUrl: this.buildRecordingUrl(cdr.recording_path),
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

        // Map dial method to readable format — 0=Manual, 1=Auto, 2=Preview,
        // 3=Predictive, 
        const dialMethodMap = {
          "0": "Manual Dial",
          "1": "Auto Dial",
          "2": "Preview Dial",
          "3": "Predictive Dial"
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
          "End Date": cdr.callend || "-",
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
          "Recording": this.buildRecordingUrl(cdr.recording_path, "-")
        };
      }
      
      // Special case for Shipeaso
      if (cdr.tenant_uuid === "cbe2814f-0f5e-46ec-8f9c-256acd07190b"){
        const [user, lead, campaign, notes] = await Promise.all([
          cdr.user_uuid ? db.collection("user").findOne({ uuid: cdr.user_uuid }) : null,
          cdr.lead_uuid ? db.collection("lead_management").findOne({ lead_management_uuid: cdr.lead_uuid }) : null,
          cdr.campaign_uuid ? db.collection("blended_campaign").findOne({ uuid: cdr.campaign_uuid }) || 
                              db.collection("inbound_campaign").findOne({ uuid: cdr.campaign_uuid }) ||
                              db.collection("outbound_campaign").findOne({ uuid: cdr.campaign_uuid }) : null,
          cdr.custom_callid ? db.collection("notes").findOne({ cdrs_uuid: cdr.custom_callid }) : null
        ]);

        let disposition = "-";
        let subDisposition = "-";
        
        if (cdr.hangup_cause && cdr.hangup_cause !== "") {
          try {
            const path = [];
            let currentUuid = cdr.hangup_cause;
            
            for (let i = 0; i < 3; i++) {
              const disp = await db.collection("disposition").findOne({ disposition_uuid: currentUuid });
              if (!disp) break;
              path.unshift(disp.name);
              if (!disp.parent_disposition || disp.parent_disposition === "") break;
              currentUuid = disp.parent_disposition;
            }

            if (path.length > 0) {
              disposition = path[0] || "-";
              subDisposition = path[1] || "-";
            }
          } catch (e) {
            logger.warn("[WEBHOOK] Disposition hierarchy lookup failed for Shipeaso", { 
              error: e.message, 
              hangup_cause: cdr.hangup_cause 
            });
          }
        }

        return {
          call_uuid: cdr.uuid || "-",
          DialerDate: this.convertUtcToIst(cdr.callstart)?.split(" ")[0] || "-",
          dialerDisposition: disposition,
          dialerDispositionCode: "-",
          dialerSubdisposition: subDisposition,
          dialerRemark: notes?.comment || "-",
          dialerAgentID: cdr.user_uuid || "-",
          dialerAgentName: user?.fullname || user?.username || "-",
          dialerCampaignName: campaign?.name || "-",
          DialerLeadName: lead?.lead_name || "-",
          dialerLeadID: cdr.lead_uuid || "-",
          dialerCallStatus: cdr.disposition || "-",
          recordingURL: this.buildRecordingUrl(cdr.recording_path, "-"),
          agent_id: cdr.user_uuid || "-",
          waiting_time: cdr.waiting_time || 0,
          total_duration: cdr.total_duration || 0,
          time: cdr.createdAt || new Date().toISOString(),
          ringing_time: cdr.ringing_time || 0,
          custom_fields: lead?.custom_fields || {}
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
      recording_file: this.buildRecordingUrl(cdr.recording_path, undefined),
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

      // Callers pass tenantUuid (camelCase, matching the webhook config object's own
      // field name); strip it out of the spread so only tenant_uuid (snake_case) ends
      // up on the document, for schema consistency with web_hook.tenant_uuid — the
      // scoping field /api/webhook-logs filters on to keep tenants' logs isolated.
      const { tenantUuid, ...rest } = deliveryInfo;

      await logsCollection.insertOne({
        ...rest,
        tenant_uuid: tenantUuid || null,
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