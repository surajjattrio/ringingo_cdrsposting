/**
 * FreeSWITCH CDR Posting System - Main Server
 * Node.js conversion from Python Flask application
 * Handles CDR processing, lead management, campaigns, and webhooks
 */

import express from "express";
import { v4 as uuidv4 } from "uuid";
import axios from "axios";
import nodemailer from "nodemailer";
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import helmet from "helmet";
import cors from "cors";
import compression from "compression";
import { fileURLToPath } from "url";
import { dirname } from "path";

import config from "./config.js";
import logger from "./logger.js";
import { connectMongoDB, getCollections, closeMongoDB } from "./database.js";
import { checkRegistration } from "./register.js";
import rabbitMQ from "./rabbitmq.js";
import { wrapCollectionsWithQueue } from "./mongodb-queue-wrapper.js";
import webhookRoutes from "./webhook/webhook-routes.js";
import googleDriveRoutes from "./googledrive/google-drive-routes.js";
import googleDriveUploader from "./googledrive/google-drive-uploader.js";
import googleDriveSettingsManager from "./googledrive/google-drive-settings-manager.js";

const app = express();

// Global variables
let collections = null;
let firstLegStore = {};
let variablesValue = {};

// Middleware
app.use(helmet());
app.use(cors({ origin: config.security.corsOrigin }));
app.use(compression());
app.use(express.json({ limit: config.app.maxRequestSize }));
app.use(
  express.urlencoded({ extended: true, limit: config.app.maxRequestSize })
);
app.use("/api", webhookRoutes);
app.use("/api", googleDriveRoutes);

/**
 * Helper function to check if string starts with number
 */
function startsWithNumber(s) {
  try {
    parseInt(s[0]);
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Manage drop call flag and disposition
 */
async function manageDropCallFlag(leadUuid, hangupReason, variablesValue) {
  try {
    logger.debug("[MANAGE_DROP_CALL_FLAG] Starting", {
      leadUuid,
      hangupReason,
    });

    const autoHangup = variablesValue["sip_h_X-amd_hangup"] || "";
    let filterCode;

    if (autoHangup === "true") {
      filterCode = "AA";
    } else if (hangupReason === "ALLOTTED_TIMEOUT" || hangupReason === "USER_BUSY") {
      filterCode = "NA";
    } else if (hangupReason === "NORMAL_UNSPECIFIED") {
      filterCode = "Busy";
    } else if (hangupReason === "NO_USER_RESPONSE") {
      filterCode = "NAVL";
    } else {
      filterCode = "DROP";
    }

    const dispositionData = await collections.disposition.findOne({
      code: filterCode,
    });
    let dispositionUuid = "";

    if (dispositionData) {
      dispositionUuid = dispositionData.disposition_uuid;
      logger.debug("[MANAGE_DROP_CALL_FLAG] Updating lead status", {
        dispositionUuid,
        leadUuid,
      });

      await collections.leadManagement.updateOne(
        { lead_management_uuid: leadUuid },
        { $set: { lead_status: dispositionUuid } }
      );

      const leadManagementData = await collections.leadManagement.findOne({
        lead_management_uuid: leadUuid,
      });

      if (leadManagementData) {
        const newCallCount = leadManagementData.call_count + 1;
        const leadReportFilter = {
          lead_group_uuid: leadManagementData.lead_group_uuid,
          disposition_uuid: dispositionUuid,
          called_count: newCallCount,
        };

        const leadReportData = await collections.leadReport.findOne(
          leadReportFilter
        );

        if (leadReportData) {
          await collections.leadReport.updateOne(leadReportFilter, {
            $inc: { count: 1 },
          });
          logger.debug("[MANAGE_DROP_CALL_FLAG] Lead report updated");
        } else {
          const leadReportUuid = uuidv4();
          const insertLeadReport = {
            disposition_uuid: dispositionUuid,
            lead_group_uuid: leadManagementData.lead_group_uuid,
            uuid: leadReportUuid,
            called_count: 1,
            count: 1,
          };
          await collections.leadReport.insertOne(insertLeadReport);
          logger.debug("[MANAGE_DROP_CALL_FLAG] Lead report inserted");
        }
      }
    }

    return dispositionUuid;
  } catch (error) {
    logger.error("[MANAGE_DROP_CALL_FLAG] Error", {
      error: error.message,
      stack: error.stack,
      leadUuid,
      hangupReason,
    });
    return "";
  }
}

/**
 * Manage lead originated - cleanup
 */
async function manageLeadOriginated(variablesValue) {
  try {
    const autoLeadUuid = variablesValue["sip_h_P-lead_uuid"] || "";

    if (autoLeadUuid) {
      logger.debug("[MANAGE_LEAD_ORIGINATED] Deleting lead originate", {
        autoLeadUuid,
      });
      await collections.leadOriginate.deleteMany({
        lead_management_uuid: autoLeadUuid,
      });
    }
  } catch (error) {
    logger.error("[MANAGE_LEAD_ORIGINATED] Error", {
      error: error.message,
      stack: error.stack,
    });
  }
}

/**
 * Manage auto campaign
 */
async function manageAutoCampaign(variablesValue, autoCampaignOriginateUuid) {
  try {
    const currentTime = new Date();
    const formattedTime = currentTime
      .toISOString()
      .slice(0, 19)
      .replace("T", " ");

    const filter = {
      auto_campaign_originate_uuid: autoCampaignOriginateUuid,
      flag: "3",
    };

    const billsec = parseInt(variablesValue.billsec || 0);
    let update;

    if (billsec > 0) {
      logger.debug("[MANAGE_AUTO_CAMPAIGN] Duration > 0");
      update = { $set: { flag: "0", last_calldate: formattedTime } };
    } else {
      logger.debug("[MANAGE_AUTO_CAMPAIGN] Duration == 0");
      update = { $set: { flag: "0" } };
    }

    await collections.autoCampaignOriginate.updateOne(filter, update);
  } catch (error) {
    logger.error("[MANAGE_AUTO_CAMPAIGN] Error", {
      error: error.message,
      stack: error.stack,
      autoCampaignOriginateUuid,
    });
  }
}

/**
 * Manage auto campaign count
 */
async function manageAutoCampaignCount(
  variablesValue,
  autoCampaignOriginateUuid
) {
  try {
    const currentTime = new Date();
    const formattedTime = currentTime
      .toISOString()
      .slice(0, 19)
      .replace("T", " ");

    const filter = { auto_campaign_originate_uuid: autoCampaignOriginateUuid };
    const billsec = parseInt(variablesValue.billsec || 0);
    let update;

    if (billsec > 0) {
      logger.debug("[MANAGE_AUTO_CAMPAIGN_COUNT] Duration > 0", {
        autoCampaignOriginateUuid,
      });
      update = { $set: { flag: "0", last_calldate: formattedTime } };
    } else {
      logger.debug("[MANAGE_AUTO_CAMPAIGN_COUNT] Duration == 0", {
        autoCampaignOriginateUuid,
      });
      update = { $set: { flag: "0" } };
    }

    await collections.autoCampaignOriginate.updateOne(filter, update);
  } catch (error) {
    logger.error("[MANAGE_AUTO_CAMPAIGN_COUNT] Error", {
      error: error.message,
      stack: error.stack,
      autoCampaignOriginateUuid,
    });
  }
}

/**
 * Check number masking from campaign
 */
async function checkNumberMasking(campaignUuid, campaignFlag) {
  try {
    const filter = { uuid: campaignUuid };

    const oCampaign = await collections.outboundCampaign.findOne(filter);
    const bCampaign = await collections.blendedCampaign.findOne(filter);

    return oCampaign || bCampaign;
  } catch (error) {
    logger.error("[CHECK_NUMBER_MASKING] Error", {
      error: error.message,
      stack: error.stack,
      campaignUuid,
    });
    return null;
  }
}

/**
 * Manage lead retries
 */
async function manageLeadRetries(variablesValue, campaignUuid, campaignFlag) {
  try {
    const leadUuid = variablesValue["sip_h_P-lead_uuid"] || "";
    if (!leadUuid || !campaignUuid) {
      return;
    }
    const filter = { uuid: campaignUuid };
    const oCampaign = await collections.outboundCampaign.findOne(filter);
    const bCampaign = await collections.blendedCampaign.findOne(filter);
    const campaign = oCampaign || bCampaign;
    logger.debug("[MANAGE_LEAD_RETRIES] Campaign data", { campaign });

    if (campaign) {
      const maxRetries = campaign.max_retries;
      const retriesTime = campaign.retries_time;
      const currentDate = new Date();
      const newDate = new Date(
        currentDate.getTime() + parseInt(retriesTime) * 60000
      );
      const isoDate = `ISODate(${newDate.toISOString()}Z)`;

      const leadFilter = {
        campaign_uuid: campaignUuid,
        lead_management_uuid: leadUuid,
      };
      logger.debug("[MANAGE_LEAD_RETRIES] Lead filter", { leadFilter });
      const leadInQueue = await collections.leadsInQueue.findOne(leadFilter);
      logger.debug("[MANAGE_LEAD_RETRIES] Lead in queue", { leadInQueue });

      if (leadInQueue) {
        if ("lead_resent_count" in leadInQueue) {
          if (parseInt(leadInQueue.lead_resent_count) < parseInt(maxRetries)) {
            const leadType = parseInt(leadInQueue.lead_resent_count) + 1;

            if (parseInt(leadType) === parseInt(maxRetries)) {
              await collections.leadsInQueue.deleteOne(leadFilter);
              logger.debug(
                "[MANAGE_LEAD_RETRIES] Max retries reached, deleted"
              );
            } else {
              await collections.leadsInQueue.updateOne(leadFilter, {
                $set: {
                  lead_resent_count: leadType,
                  lead_sent: "1",
                  resend_lead_time: isoDate,
                },
              });
              logger.debug("[MANAGE_LEAD_RETRIES] Updated retry count", {
                leadType,
              });
            }
          }
        } else {
          await collections.leadsInQueue.updateOne(leadFilter, {
            $set: {
              lead_resent_count: "1",
              lead_sent: "1",
              resend_lead_time: isoDate,
            },
          });
          logger.debug(
            "[MANAGE_LEAD_RETRIES] Updated successfully - first retry"
          );
        }
      }
    }
  } catch (error) {
    logger.error("[MANAGE_LEAD_RETRIES] Error", {
      error: error.message,
      stack: error.stack,
      campaignUuid,
    });
  }
}

/**
 * Lead entry management
 */
async function leadEntryManagement(destinationNumber, userUuid, tenantUuid) {
  try {
    logger.debug("[LEAD_ENTRY_MANAGEMENT] Starting", {
      destinationNumber,
      userUuid,
      tenantUuid,
    });

    const destNumber = destinationNumber.replace(/^\+/, "");
    const tenantInfo = await collections.tenant.findOne({ uuid: tenantUuid });
    const leadUuid = uuidv4();

    const leadFilter = {
      phone_number: destNumber,
      tenant_uuid: tenantUuid,
    };
    const leadMgmt = await collections.leadManagement.findOne(leadFilter);

    if (!leadMgmt && tenantInfo && tenantInfo.add_anonymous_lead !== "1") {
      const insertLeadMgmt = {
        address: "",
        state: "",
        province: "",
        postal_code: "",
        dob: "",
        phone_code: "",
        alternate_phone_number: "",
        email: "",
        status: "0",
        gender: "0",
        created_flag: "2",
        country_uuid: "",
        city: "",
        first_name: "Anonymous",
        last_name: "",
        phone_number: destNumber,
        user_uuid: userUuid,
        tenant_uuid: tenantUuid,
        lead_management_uuid: leadUuid,
        lead_status: "3809f32d-9096-406b-b2ff-1207b3fbfbc5",
      };

      await collections.leadManagement.insertOne(insertLeadMgmt);
      logger.debug("[LEAD_ENTRY_MANAGEMENT] Lead inserted", { insertLeadMgmt });
      return leadUuid;
    }

    return leadMgmt ? leadMgmt.lead_management_uuid || "" : "";
  } catch (error) {
    logger.error("[LEAD_ENTRY_MANAGEMENT] Error", {
      error: error.message,
      stack: error.stack,
      destinationNumber,
      userUuid,
      tenantUuid,
    });
    return "";
  }
}

async function resolveTenantForMissedCall(tenantUuid, leadUuid, ipMapUuid) {
  try {
    if (tenantUuid) {
      return tenantUuid;
    }

    let resolvedTenantUuid = tenantUuid;

    if (!resolvedTenantUuid && leadUuid) {
      const lead = await collections.leadManagement.findOne({
        lead_management_uuid: leadUuid,
      });

      if (lead && lead.tenant_uuid) {
        resolvedTenantUuid = lead.tenant_uuid;
      }
    }

    if (!resolvedTenantUuid && ipMapUuid) {
      const ipMap = await collections.ipMap.findOne({ uuid: ipMapUuid });

      if (ipMap && ipMap.tenant_uuid) {
        resolvedTenantUuid = ipMap.tenant_uuid;
      }
    }

    return resolvedTenantUuid || tenantUuid;
  } catch (error) {
    logger.error("[RESOLVE_TENANT_FOR_MISSED_CALL] Error", {
      error: error.message,
      stack: error.stack,
      tenantUuid,
      leadUuid,
      ipMapUuid,
    });
    return tenantUuid;
  }
}
async function resolveAgentUuidByName(username, tenantUuid, context = {}) {
    try {
        logger.debug('[RESOLVE_AGENT_UUID_BY_NAME] Called', {
            username,
            tenantUuid,
        });

        if (!username || !tenantUuid) {
            logger.debug('[RESOLVE_AGENT_UUID_BY_NAME] Missing required parameters', {
                username,
                tenantUuid
            });
            return "";
        }

        const filter = {
            username: username,
            tenant_uuid: tenantUuid
        };

        logger.debug('[RESOLVE_AGENT_UUID_BY_NAME] Querying user with filter', { filter });

        const user = await collections.user.findOne(filter);

        if (user && user.uuid) {
            logger.debug('[RESOLVE_AGENT_UUID_BY_NAME] User found', {
                userUuid: user.uuid,
                username: user.username,
                tenantUuid: user.tenant_uuid
            });
            return user.uuid;
        } else {
            logger.debug('[RESOLVE_AGENT_UUID_BY_NAME] No user found for filter', { filter });
            return "";
        }
    } catch (error) {
        logger.error('[RESOLVE_AGENT_UUID_BY_NAME] Error', {
            error: error.message,
            stack: error.stack,
            username,
            tenantUuid
        });
        return "";
    }
}
/**
 * Get voice broadcast disposition
 */
async function getVoicebroadcastDisposition(
  vbType,
  leadUuid,
  voicebroadcastUuid,
  duration
) {
  try {
    const filterCodes = {
      0: "SURVEY",
      1: "OBDC",
      2: "NI",
      3: "DNC",
      CANCEL: "NA",
      "Busy Here": "Busy",
      "Temporarily Unavailable": "NAVL",
      send_bye: "NAVL",
    };

    const code = filterCodes[vbType] || (duration !== "0" ? "NDP" : "NA");
    const disposition = await collections.disposition.findOne({ code: code });

    if (disposition) {
      const dispositionUuid = String(disposition.disposition_uuid);
      logger.debug("[GET_VOICEBROADCAST_DISPOSITION] Updating lead", {
        dispositionUuid,
        leadUuid,
      });

      await collections.leadManagement.updateOne(
        { lead_management_uuid: leadUuid },
        { $set: { lead_status: dispositionUuid } }
      );

      const voicebroadcastData = await collections.voiceBroadcast.findOne({
        uuid: voicebroadcastUuid,
      });

      logger.debug("[GET_VOICEBROADCAST_DISPOSITION] Voicebroadcast data", {
        voicebroadcastData,
      });

      const leadManagementData = await collections.leadManagement.findOne({
        lead_management_uuid: leadUuid,
      });

      logger.debug("[GET_VOICEBROADCAST_DISPOSITION] Lead management data", {
        leadManagementData,
      });

      if (
        leadManagementData &&
        voicebroadcastData &&
        leadManagementData.retries_count <
        parseInt(voicebroadcastData.max_retries)
      ) {
        if (
          voicebroadcastData.max_retries_disposition_uuid.includes(
            dispositionUuid
          )
        ) {
          const maxCountDocument = await collections.leadBroadcastQueue
            .find({ lead_group_uuid: leadManagementData.lead_group_uuid })
            .sort({ count: -1 })
            .limit(1)
            .toArray();

          const maxCount = maxCountDocument[0] ? maxCountDocument[0].count : 0;
          const newCount = maxCount + 1;
          const leadBroadcastUuid = uuidv4();

          const leadBroadcastQueueInsert = {
            lead_group_uuid: leadManagementData.lead_group_uuid,
            tenant_uuid: leadManagementData.tenant_uuid,
            lead_management_uuid: leadManagementData.lead_management_uuid,
            count: newCount,
            uuid: leadBroadcastUuid,
          };

          await collections.leadBroadcastQueue.insertOne(
            leadBroadcastQueueInsert
          );
          logger.debug(
            "[GET_VOICEBROADCAST_DISPOSITION] Lead broadcast queue insert",
            {
              leadBroadcastQueueInsert,
            }
          );

          await collections.leadManagement.updateOne(
            { lead_management_uuid: leadUuid },
            { $inc: { retries_count: 1 } }
          );
        } else {
          logger.debug(
            "[GET_VOICEBROADCAST_DISPOSITION] Not going in disposition condition"
          );
        }
      } else {
        logger.debug(
          "[GET_VOICEBROADCAST_DISPOSITION] Not going in retries condition"
        );
      }

      return dispositionUuid;
    }

    return "";
  } catch (error) {
    logger.error("[GET_VOICEBROADCAST_DISPOSITION] Error", {
      error: error.message,
      stack: error.stack,
      vbType,
      leadUuid,
      voicebroadcastUuid,
    });
    return "";
  }
}

/**
 * Send notification email
 */
async function sendNotification(
  tenantUuid,
  callerIdName,
  callerIdNumber,
  destinationNumber,
  callstart,
  callType
) {
  try {
    logger.debug("[SEND_NOTIFICATION] Starting", {
      tenantUuid,
      callerIdName,
      callerIdNumber,
      destinationNumber,
      callstart,
      callType,
    });

    // Make caller ID name Anonymous if it starts with a number
    if (startsWithNumber(callerIdName)) {
      callerIdName = "Anonymous";
    }

    const tenantInfo = await collections.tenant.findOne({ uuid: tenantUuid });

    if (!tenantInfo) {
      logger.debug("[SEND_NOTIFICATION] Tenant not found");
      return;
    }

    logger.debug("[SEND_NOTIFICATION] Tenant info", { tenantInfo });

    const senderEmail = "7b26b2da1b254e6133d47b2cc3a2184a";
    const senderPassword = "1b7f00c11402cc3c26cacf592725ea87";
    const fromEmail = "notifications@test.io";
    const recipientEmail = String(tenantInfo.email);

    const finalCallType = callType === "local" ? "sip" : callType;
    const subject = "Missed call notification from COMPANY";

    const message = `
    <html>
    <body>
        <p>Hello,</p>
        <p>You have received a missed call.</p>
        <p>Caller name: ${callerIdName}<br>
        Caller number: ${callerIdNumber}<br>
        Destination number: ${destinationNumber}<br>
        Time: ${callstart}<br>
        Missed by: ${finalCallType}</p>
        <p>Thanks for using COMPANY</p>
        <br>
    </body>
    </html>
    `;

    const smtpServer = "in-v3.mailjet.com";
    const smtpPort = 587;

    const transporter = nodemailer.createTransport({
      host: smtpServer,
      port: smtpPort,
      secure: false,
      auth: {
        user: senderEmail,
        pass: senderPassword,
      },
    });

    await transporter.sendMail({
      from: fromEmail,
      to: recipientEmail,
      subject: subject,
      html: message,
    });

    logger.success("[SEND_NOTIFICATION] Email sent successfully");
  } catch (error) {
    logger.error("[SEND_NOTIFICATION] Failed to send email", {
      error: error.message,
      stack: error.stack,
    });
  }
}

/**
 * Set caller sticky
 */
async function setCallerSticky(userUuid, tenantUuid, callerIdNumber) {
  try {
    logger.debug("[SET_CALLER_STICKY] Starting", {
      userUuid,
      tenantUuid,
      callerIdNumber,
    });

    const userInfo = await collections.user.findOne({ uuid: userUuid });

    if (!userInfo) {
      logger.debug("[SET_CALLER_STICKY] User not found");
      return;
    }

    logger.debug("[SET_CALLER_STICKY] User info", {
      userRoleUuid: userInfo.user_role_uuid,
    });

    const userRoleInfo = await collections.userRole.findOne({
      uuid: userInfo.user_role_uuid,
    });

    logger.debug("[SET_CALLER_STICKY] User role info", { userRoleInfo });

    if (userRoleInfo && userRoleInfo.sticky_agent === "0") {
      const stickyNewUuid = uuidv4();
      const insertSticky = {
        uuid: stickyNewUuid,
        user_uuid: userUuid,
        tenant_uuid: tenantUuid,
        user_role_uuid: userRoleInfo.uuid,
        phone_number: callerIdNumber,
        sticky_user_role_status: "0",
      };

      await collections.stickyAgent.insertOne(insertSticky);
      logger.debug("[SET_CALLER_STICKY] Sticky data inserted successfully");
    }
  } catch (error) {
    logger.error("[SET_CALLER_STICKY] Error", {
      error: error.message,
      stack: error.stack,
      userUuid,
    });
  }
}

/**
 * Get telecom circle
 */
async function getTelecomCircle(tenantUuid, destinationNumber) {
  try {
    let destNumber = String(destinationNumber);
    const firstDigit = destNumber[0] || "";
    let prefixLen = 0;
    let longestPrefix = "";
    let longestPrefixName = "";

    while (destNumber) {
      const filter = {
        tenant_uuid: tenantUuid,
        prefix: { $regex: "^" + firstDigit.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") },
      };

      const results = await collections.telecomCircle.find(filter).toArray();

      for (const prefixDetails of results) {
        if (prefixLen === 0 || prefixLen < prefixDetails.prefix.length) {
          prefixLen = prefixDetails.prefix.length;
          longestPrefix = prefixDetails.prefix;
          longestPrefixName = prefixDetails.uuid;
          logger.debug("[GET_TELECOM_CIRCLE] Prefix base caller id match", {
            longestPrefix,
            longestPrefixName,
          });
        }
      }

      destNumber = destNumber.slice(0, -1);

      if (destinationNumber.startsWith(longestPrefix)) {
        return longestPrefixName;
      }
    }

    return "";
  } catch (error) {
    logger.error("[GET_TELECOM_CIRCLE] Error", {
      error: error.message,
      stack: error.stack,
      tenantUuid,
      destinationNumber,
    });
    return "";
  }
}

/**
 * Main CDR processing endpoint
 */
app.post("/cdrs", async (req, res) => {
  try {
    logger.info("==============================");
    logger.info("[CDR] Processing CDR Request");
    logger.info("==============================");

    const data = req.body;
    logger.info("CDR-DATA....", data);
    if (!data || !data.variables) {
      logger.error("[CDR_PROCESSING] Invalid or missing variables data");
      return res
        .status(400)
        .json({ error: "Invalid or missing variables data" });
    }

    variablesValue = data.variables;

    const feedbackScore = variablesValue.ivr_feedback_score || "N/A";
    logger.debug("[CDR] Feedback Score Received", { feedbackScore });

    logger.debug("[CDR] Variables received", {
      variablesCount: Object.keys(variablesValue).length,
    });

    let filePassFlag = 0;
    let inboundAnsweredUserUuid = "";
    let inboundAnsweredExtensionUuid = "";

    const profile = variablesValue.sip_profile_name;
    if (profile === "internal") {
      firstLegStore["holdsecond"] = parseInt(
        variablesValue.hold_accum_seconds || 0
      );
    }
    logger.debug("[CDR_PROCESSING] First leg store", { firstLegStore });

    const autoCampaign = variablesValue["sip_h_P-auto_campaign"] || "";
    const threewayAgentExtensionUuid =
      variablesValue.threeway_agent_extension_uuid || "";

    if (threewayAgentExtensionUuid) {
      await collections.customerCallerid.deleteMany({
        user_uuid: threewayAgentExtensionUuid,
      });
    }

    logger.debug("[CDR] Auto campaign", { autoCampaign });

    let voiceBroadcast = variablesValue["sip_h_P-voicebroadcast"] || "";
    if (voiceBroadcast === "false") {
      voiceBroadcast = "";
    }

    const stickyAgentFlag = variablesValue.sticky_agent_flag || "";
    const inboundHangupCause = variablesValue.hangup_cause || "";
    const successAnsweredUserUuid = variablesValue.user_uuid || "";
    const successAnsweredExtensionUuid = variablesValue.extension_uuid || "";

    if (
      inboundHangupCause === "NORMAL_CLEARING" &&
      successAnsweredUserUuid &&
      successAnsweredExtensionUuid
    ) {
      const tmpCallUuid = variablesValue.uuid || "";
      inboundAnsweredUserUuid = successAnsweredUserUuid;
      inboundAnsweredExtensionUuid = successAnsweredExtensionUuid;
      const tmpUuid = uuidv4();

      const insertRgString = {
        uuid: tmpUuid,
        call_uuid: tmpCallUuid,
        user_uuid: inboundAnsweredUserUuid,
        extension_uuid: inboundAnsweredExtensionUuid,
      };

      await collections.tmpRinggroup.insertOne(insertRgString);
      logger.debug("[CDR_PROCESSING] Tmp ringgroup inserted", {
        inboundAnsweredUserUuid,
        inboundAnsweredExtensionUuid,
      });
    }

    await manageLeadOriginated(variablesValue);

    const autoCampaignOriginateUuid =
      variablesValue["sip_h_P-auto_campaign_originate"] || "";
    if (autoCampaignOriginateUuid) {
      await manageAutoCampaignCount(variablesValue, autoCampaignOriginateUuid);
    }

    const autoAgentFlag = variablesValue["sip_h_P-auto_agent_flag"] || "";
    if (autoCampaignOriginateUuid && autoAgentFlag === "") {
      await manageAutoCampaign(variablesValue, autoCampaignOriginateUuid);
    }

    if (autoAgentFlag === "auto_agent") {
      filePassFlag = 1;
      logger.info("[CDR] Skipped auto_agent");
      return res.status(200).json({ status: "skipped auto_agent" });
    }

    let callstart =
      variablesValue.custom_callstart ||
      variablesValue["sip_h_P-callstart"] ||
      "";
    logger.debug("[CDR] Call start", { callstart });

    const createdAt = new Date();

    if (!callstart) {
      const epochTime = variablesValue.start_epoch || "";
      if (epochTime) {
        const dtUtc = new Date(parseInt(epochTime) * 1000);
        callstart = dtUtc.toISOString().slice(0, 19).replace("T", " ");
        logger.debug("[CDR] Auto campaign callstart", { callstart });
      }
    }

    const featureCodeFlag = variablesValue.feature_code_flag || "false";
    if (featureCodeFlag === "true") {
      filePassFlag = 1;
      logger.info("[CDR] Skipped feature code");
      return res.status(200).json({ status: "skipped feature code" });
    }

    if (!callstart) {
      filePassFlag = 1;
      logger.info("[CDR] Skipped no callstart");
      return res.status(200).json({ status: "skipped no callstart" });
    }

    const pbxFeature = variablesValue.pbx_feature || "";
    let pbxFeatureUuid = "";

    if (pbxFeature === "call_queue") {
      pbxFeatureUuid = variablesValue.call_queue_id || "";
    } else if (pbxFeature === "ivr") {
      pbxFeatureUuid = variablesValue.ivr_group_uuid || "";
    } else if (pbxFeature === "ring_group") {
      pbxFeatureUuid = variablesValue.ringgroup_id || "";
    } else if (pbxFeature === "time_condition") {
      pbxFeatureUuid = variablesValue.time_condition_uuid || "";
    }

    const ipMapUuid = variablesValue.ip_map_uuid || "";
    const customCallid =
      variablesValue.custom_callid ||
      variablesValue["sip_h_X-custom_callid"] ||
      variablesValue["sip_h_X-Custom-Callid"] ||
      "";
    const tenantUuid =
      variablesValue.tenant_uuid ||
      variablesValue["sip_h_X-tenant_uuid"] ||
      variablesValue["sip_h_P-tenant_uuid"] ||
      "";

    let direction = variablesValue.direction || "";
    let isInboundCall =
      variablesValue["sip_h_X-inbound_campaign_flag"] || "false";
    const bridgeUuid = variablesValue.bridge_uuid || uuidv4();

    let userUuid =
      variablesValue.user_uuid || variablesValue["sip_h_P-user_uuid"] || "";

    const authenticationType = variablesValue.authentication_type || "auth";
    let extensionUuid = variablesValue.extension_uuid || "";
    const trunkUuid = variablesValue.trunk_uuid || "";
    const sipUser = variablesValue.sip_user || "";
    const ivrGroup = variablesValue.ivr_group_uuid || "";
    let callerIdName =
      variablesValue.effective_caller_id_name ||
      variablesValue.caller_id_name ||
      "";
    let callerIdNumber =
      variablesValue.effective_caller_id_number ||
      variablesValue.caller_id_number ||
      "";
    const dialedUser = variablesValue.dialed_user || "";
    let leadUuid =
      variablesValue["sip_h_X-Leaduuid"] ||
      variablesValue["sip_h_X-lead_uuid"] ||
      variablesValue["sip_h_P-lead_uuid"] ||
      "";

    if (leadUuid === "null") {
      leadUuid = "";
    }

    const hangupReason = variablesValue.hangup_cause || "";
    logger.debug("[CDR] Hangup reason", { hangupReason });

    let autoCallFlag = "0";
    let hangupCause = "";
    let campaignUuid =
      variablesValue["sip_h_X-selectedcampaignuuid"] ||
      variablesValue["sip_h_P-campaign_uuid"] ||
      "";

    const campaignFlag = variablesValue["sip_h_P-campaign_flag"] || "";

    logger.debug("[CDR] Campaign flag", { campaignFlag });

    const callStickStatus = variablesValue.call_stick_status || 1;
    let billsecond = parseInt(variablesValue.billsec || 0);

    // New fields
    const waitsecond = parseInt(variablesValue.waitsec || 0);
    const ringingsecond = parseInt(variablesValue.answersec || 0);
    const callend = variablesValue.end_stamp || "";

    logger.debug("[CDR] End call", { callend });

    const totalduration = parseInt(variablesValue.duration || 0);

    let holdsecond;
    if (autoCampaign || ipMapUuid || pbxFeature) {
      holdsecond =
        (firstLegStore && firstLegStore.holdsecond) ||
        parseInt(variablesValue.hold_accum_seconds || 0);
    } else {
      holdsecond = parseInt(variablesValue.hold_accum_seconds || 0);
    }

    logger.debug("[CDR] Waiting time and ringing time", {
      waitsecond,
      ringingsecond,
      callend,
      totalduration,
      holdsecond,
    });

    // Campaign statistics handling
    if (campaignUuid) {
      const currentDate = new Date();
      const campaignDate = currentDate.toISOString().slice(0, 10);
      const managerUuid = variablesValue["sip_h_P-manager_uuid"] || "";

      const campaignStatisticsFilter = {
        tenant_uuid: tenantUuid,
        campaign_uuid: campaignUuid,
        campaign_date: campaignDate,
        manager_uuid: managerUuid,
      };

      const campaignDurationCheck = billsecond > 0 && billsecond < 30 ? 1 : 0;
      let campaignStatisticsDropCount = 0;
      let campaignStatisticsFailCount = 0;
      let campaignStatisticsAmdCount = 0;
      let campaignStatisticsSuccessCount = 0;

      if (autoCampaign) {
        logger.debug("[CDR] Checking lead originate issue");
        logger.debug("[CDR] Auto campaign", {
          autoCampaign,
          autoCampaignOriginateUuid,
          leadUuid,
        });

        if (autoCampaign && !autoCampaignOriginateUuid && leadUuid) {
          logger.debug("[CDR] Outbound");
          if (billsecond > 0) {
            campaignStatisticsDropCount = 1;
          }
          logger.debug("[CDR] Tenant UUID and hangup reason", {
            tenantUuid,
            hangupReason,
          });
          hangupCause = await manageDropCallFlag(
            leadUuid,
            hangupReason,
            variablesValue
          );
          autoCallFlag = "2";
        } else if (autoCampaign && autoCampaignOriginateUuid && leadUuid) {
          logger.debug("[CDR] This is success but fail");
          if (
            ["NO_USER_RESPONSE", "ORIGINATOR_CANCEL"].includes(hangupReason)
          ) {
            await manageDropCallFlag(leadUuid, hangupReason, variablesValue);
            if (billsecond > 0) {
              campaignStatisticsDropCount = 1;
            }
            autoCallFlag = "2";
          } else {
            campaignStatisticsSuccessCount = 1;
            autoCallFlag = "1";
          }
        }

        if (billsecond > 0) {
          campaignStatisticsSuccessCount = 1;
        } else {
          campaignStatisticsFailCount = 1;
        }
      } else {
        campaignStatisticsSuccessCount = billsecond > 0 ? 1 : 0;
        campaignStatisticsFailCount = billsecond > 0 ? 0 : 1;
      }

      const autoHangup = variablesValue["sip_h_X-amd_hangup"] || "";
      if (autoHangup === "true") {
        campaignStatisticsAmdCount = 1;
        campaignStatisticsDropCount = 0;
      }

      const campaignStatisticsUpdate = {
        $inc: {
          drop: campaignStatisticsDropCount,
          success: campaignStatisticsSuccessCount,
          fail: campaignStatisticsFailCount,
          short_call: campaignDurationCheck,
          amd: campaignStatisticsAmdCount,
          billsecond: billsecond,
        },
      };

      logger.debug("[CDR] Campaign statistics update", {
        campaignStatisticsUpdate,
      });

      await collections.campaignStatistics.findOneAndUpdate(
        campaignStatisticsFilter,
        campaignStatisticsUpdate,
        { upsert: true }
      );
    }

    let destinationNumber =
      variablesValue.custom_destination_number ||
      variablesValue.effective_destination_number ||
      dialedUser ||
      variablesValue["Caller-Destination-Number"] ||
      variablesValue["sip_h_P-cb_destination"];

    if (!destinationNumber || destinationNumber === "undefined") {
      destinationNumber =
        variablesValue["Caller-Destination-Number"] ||
        variablesValue["sip_h_P-cb_destination"] ||
        "";
      logger.warn("[CDR] Destination number was undefined, using fallback", {
        destinationNumber,
      });
    }
    
    let disposition = variablesValue.hangup_cause || "NORMAL_CLEARING";
    let callState = "normal";

    const originateFailedCause = variablesValue.originate_failed_cause || "";
    if (originateFailedCause === "ORIGINATOR_CANCEL") {
      disposition = originateFailedCause;
    }

    if (
      [
        "NO_ANSWER",
        "NO_USER_RESPONSE",
        "ORIGINATOR_CANCEL",
        "RECOVERY_ON_TIMER_EXPIRE",
      ].includes(disposition)
    ) {
      callState = "missed";
    }

    const ivrMenuStatus = variablesValue.ivr_menu_status || "";
    if (["failure", "timeout"].includes(ivrMenuStatus)) {
      callState = "missed";
      disposition = "IVR_TIMEOUT";
      pbxFeatureUuid = ivrGroup;
    }

    const ccCause = variablesValue.cc_cause || "";
    if (ccCause && ccCause !== "answered") {
      callState = "missed";
      disposition = "CALLQUEUE_TIMEOUT";
    }

    const callStateMain =
      disposition === "ORIGINATOR_CANCEL" ? "normal" : callState;

    let callType = variablesValue.call_type || "standard";
    // if (["standard", "did"].includes(callType)) {
    //   direction = "outbound";
    // }

    if (isInboundCall == "true") {
      direction = "inbound";
    }

    let recordingPath = "";

    logger.debug("[CDR] Bridge UUID", {
      bridgeUuid: variablesValue.bridge_uuid,
    });
    logger.debug("[CDR] Auto call type", {
      autoCallType: variablesValue["sip_h_X-Autocalltype"],
    });
    logger.debug("[CDR] Recording directory", {
      recordingDirectory: variablesValue.recording_directory,
    });

    if (
      variablesValue.custom_recording === "0" ||
      !variablesValue.custom_recording
    ) {
      logger.debug("[CDR] This is recording");

      if (variablesValue.current_application === "record_session") {
        recordingPath = variablesValue.current_application_data || "";
      } else if (
        variablesValue.execute_on_answer &&
        variablesValue.execute_on_answer.includes("record_session")
      ) {
        recordingPath = variablesValue.execute_on_answer.replace(
          "record_session ",
          ""
        );
        logger.debug("[CDR] Recording path in execute on answer", {
          recordingPath,
        });
      }

      if (variablesValue["sip_h_X-Autocalltype"] === "true") {
        const recordingDialedUser = variablesValue.sip_to_user || "";
        const recordingUuid = variablesValue.bridge_uuid || "";
        const recordingDirectory = variablesValue.recording_directory || "";

        if (recordingUuid) {
          // recordingPath = `${recordingDirectory}/${recordingDialedUser}_${recordingUuid}.wav`;
          recordingPath = `${recordingUuid}.wav`;
        }
      }
    }

    const receiverExtensionUuid = variablesValue.receiver_extension_uuid || "";
    const originateDisposition = variablesValue.originate_disposition || "";

    if (["NO_ANSWER", "USER_BUSY"].includes(originateDisposition)) {
      callState = "missed";
      disposition = originateDisposition;
      billsecond = 0;
    }

    // if (receiverExtensionUuid) {
    //   direction = "outbound";
    // }

    let incomingExtraEntryFlag = 0;
    const callMode = campaignUuid ? "1" : "0";
    let numberMasking = "1";
    let dialMethod = "";

    logger.debug("[CDR] Campaign", { campaignUuid, campaignFlag });

    if (campaignUuid) {
      const resultOutboundCampaign = await checkNumberMasking(
        campaignUuid,
        campaignFlag
      );

      logger.debug("[CDR] Result outbound campaign", {
        resultOutboundCampaign,
      });

      if (resultOutboundCampaign) {
        numberMasking = resultOutboundCampaign.number_masking || "1";
        dialMethod = resultOutboundCampaign.dial_method || "";
      }

      logger.debug("[CDR] Dial method of OC", { dialMethod });
    }

    if (
      campaignUuid &&
      billsecond === 0 &&
      disposition !== "ORIGINATOR_CANCEL"
    ) {
      await manageLeadRetries(variablesValue, campaignUuid, campaignFlag);
    }

    if (destinationNumber && !leadUuid && userUuid && callType !== "local") {
      leadUuid = await leadEntryManagement(
        destinationNumber,
        userUuid,
        tenantUuid
      );
    }

    const autoCampaignCheck = variablesValue["sip_h_P-auto_campaign"] || "";
    const predictiveCampaign =
      variablesValue["sip_h_P-predictive_campaign"] || "";

    if (predictiveCampaign === "true") {
      // destinationNumber =
      //   variablesValue["sip_h_P-original_number"] || destinationNumber;
      const predictiveFlag = autoCallFlag === "1" ? "0" : "1";
      const predictiveUuid = uuidv4();

      const insertPredictiveCampaign = {
        uuid: predictiveUuid,
        campaign_uuid: campaignUuid,
        agent_uuid: userUuid,
        tenant_uuid: tenantUuid,
        flag: predictiveFlag,
      };

      await collections.predictiveCalculation.insertOne(
        insertPredictiveCampaign
      );

      logger.debug("[CDR] Insert predictive campaign", {
        insertPredictiveCampaign,
      });
    }

    if (autoCampaignCheck === "true") {
      logger.debug("[CDR] Auto campaign CHANGE Destination");
      // const originalNumber = variablesValue["sip_h_P-original_number"];
      // Only update if originalNumber exists and is not "undefined" string
      // if (originalNumber && originalNumber !== "undefined") {
      //    destinationNumber = originalNumber;
      // }
      callerIdName = variablesValue["Hunt-Callee-ID-Name"] || callerIdName;
      callerIdNumber =
        variablesValue["Hunt-Orig-Caller-ID-Number"] || callerIdNumber;
      numberMasking = variablesValue["sip_h_P-number_masking"] || numberMasking;
    }

    const hangupPartyDisposition = variablesValue.sip_hangup_disposition || "";
    logger.debug("[CDR] Hangup party disposition", {
      hangupPartyDisposition,
    });
    logger.debug("[CDR] Hangup direction", { direction });

    let disconnectedBy = "";
    // if (hangupPartyDisposition === "send_bye") {
    //   disconnectedBy = direction === "inbound" ? "0" : "1";
    // } else if (hangupPartyDisposition === "recv_bye") {
    //   disconnectedBy = direction === "inbound" ? "1" : "0";
    // } else if (hangupPartyDisposition === "recv_cancel") {
    //   disconnectedBy = direction === "inbound" ? "1" : "0";
    // } else if (hangupPartyDisposition === "send_refuse") {
    //   disconnectedBy = direction === "inbound" ? "0" : "1";
    // }
    // Initialize variables (assuming these come from your call record object)
    //  0 - Agent , 1 - Customer
    if (hangupPartyDisposition === "recv_bye" && direction === "outbound") {
      disconnectedBy = "1";
    } else if (
      hangupPartyDisposition === "send_bye" &&
      direction === "outbound"
    ) {
      disconnectedBy = "0";
    } else if (
      hangupPartyDisposition === "recv_cancel" &&
      direction === "outbound"
    ) {
      disconnectedBy = "1";
    } else if (
      hangupPartyDisposition === "recv_refuse" &&
      direction === "outbound"
    ) {
      disconnectedBy = "1";
    } else if (
      hangupPartyDisposition === "recv_cancel" &&
      isInboundCall === "false" &&
      direction === "inbound"
    ) {
      disconnectedBy = "0";
      direction = "outbound";
    } else if (
      hangupPartyDisposition === "send_bye" &&
      direction === "inbound" &&
      isInboundCall === "false"
    ) {
      disconnectedBy = "1";
      direction = "outbound";
    } else if (
      hangupPartyDisposition === "recv_bye" &&
      direction === "inbound" &&
      isInboundCall === "false"
    ) {
      direction = "outbound";
      disconnectedBy = "0";
    } else if (
      hangupPartyDisposition === "send_refuse" &&
      isInboundCall === "false" &&
      direction === "inbound"
    ) {
      direction = "outbound";
      disconnectedBy = "2"; // auto hangup from agent side.
    } else if (
      hangupPartyDisposition === "recv_bye" &&
      direction === "inbound"
    ) {
      disconnectedBy = "1";
    } else if (
      hangupPartyDisposition === "send_bye" &&
      direction === "inbound"
    ) {
      disconnectedBy = "0";
    } else if (
      hangupPartyDisposition === "recv_refuse" &&
      direction === "inbound"
    ) {
      disconnectedBy = "0";
    }

    logger.debug("[CDR_PROCESSING] Disconnected by", { disconnectedBy });

    const telecomCircle = await getTelecomCircle(
      tenantUuid,
      direction === "outbound" ? destinationNumber : callerIdNumber,
    );

    // Inbound CDR insertion
    const isMissedCall = callState === "missed";
    if (ipMapUuid || !userUuid || voiceBroadcast !== "") {
      incomingExtraEntryFlag = 1;

      if (variablesValue.sip_from_user) {
        callerIdName = callerIdNumber = variablesValue.sip_from_user.replace(
          /^\+/,
          ""
        );
      }

      const insertInboundCdrJson = {
        uuid: bridgeUuid,
        user_uuid: userUuid,
        extension_uuid: extensionUuid,
        tenant_uuid: tenantUuid,
        trunk_uuid: trunkUuid,
        sip_user: sipUser,
        caller_id_name: callerIdName,
        caller_id_number: callerIdNumber,
        destination_number: destinationNumber,
        billsecond: billsecond,
        disposition: disposition,
        direction: direction,
        call_type: callType,
        callstart: callstart,
        recording_path: recordingPath,
        pbx_feature: pbxFeature,
        pbx_feature_uuid: pbxFeatureUuid,
        authentication_type: authenticationType,
        custom_callid: customCallid,
        ip_map_uuid: ipMapUuid,
        call_state: callStateMain,
        lead_uuid: leadUuid,
        campaign_uuid: campaignUuid,
        call_mode: callMode,
        hangup_cause: hangupCause,
        auto_call_flag: autoCallFlag,
        ivr_group: ivrGroup,
        disconnected_by: disconnectedBy,
        createdAt: createdAt,
        circle_uuid: telecomCircle,
        callend: callend,
        hold_time: holdsecond,
        waiting_time: waitsecond,
        total_duration: totalduration,
        ringing_time: ringingsecond,
        ivr_feedback_score: feedbackScore,
      };

      await collections.inboundCdrs.insertOne(insertInboundCdrJson);
      logger.debug("[CDR_PROCESSING] Inbound CDR Insert", {
        insertInboundCdrJson,
      });

 if (isMissedCall && direction === "inbound") {
    const resolvedTenantUuid = await resolveTenantForMissedCall(
      tenantUuid,
      leadUuid,
      ipMapUuid,
    );

    // Detect agent-leg from call queue
    const ccSide = variablesValue.cc_side || "";

    logger.debug("[MISSED_CALL] Checking agent-leg conditions", {
      ccSide,
      dialedUser,
      tenantUuid,
      destinationNumber,
      callerIdNumber,
    });

    // Resolve user_uuid if this is an agent leg
    if (ccSide === "agent" && dialedUser) {
      const agentUsername = dialedUser; // dialed_user is already clean

      logger.debug("[MISSED_CALL] Attempting to resolve agent UUID", {
        agentUsername,
        tenantUuid,
        ccSide,
      });

      // Call the enhanced resolveAgentUuidByName (detailed internal logging added separately)
      const resolvedUserUuid = await resolveAgentUuidByName(agentUsername, tenantUuid);

      if (resolvedUserUuid) {
        userUuid = resolvedUserUuid;
        logger.debug("[MISSED_CALL] Agent UUID resolved successfully", {
          userUuid,
          agentUsername,
        });
      } else {
        logger.warn("[MISSED_CALL] Agent UUID resolution failed – no user found", {
          agentUsername,
          tenantUuid,
        });
      }
    }

    logger.debug("Missed Call ccSide", { ccSide });
    logger.debug("Missed Call dialedUser", { dialedUser });

    // Fix destinationNumber if it was overwritten by agent username
    if (ccSide === "agent" && dialedUser === destinationNumber) {
      destinationNumber = callerIdNumber;
      logger.debug("[MISSED_CALL] Corrected destinationNumber to callerIdNumber", {
        destinationNumber,
      });
    }

    const insertMissedCdrJson = {
      ...insertInboundCdrJson,
      uuid: uuidv4(),
      tenant_uuid: resolvedTenantUuid,
      call_state: "missed",
      user_uuid: userUuid,
      destination_number: destinationNumber,
    };

    logger.debug("[MISSED_CALL] Final document to insert into missed_calls", {
      insertMissedCdrJson,
    });

    await collections.missedCalls.insertOne(insertMissedCdrJson);
    logger.debug("[CDR_PROCESSING] Missed Call CDR Insert", {
      insertMissedCdrJson,
    });
  }

      // Webhook processing
      if (tenantUuid) {
        const webhookData = await collections.webHook.findOne({
          tenant_uuid: tenantUuid,
        });
        logger.debug("[CDR_PROCESSING] Webhook data", { webhookData });

        if (webhookData) {
          const url = String(webhookData.web_hook_url);

          const webHookData = {
            description: "incoming calls notification from Belsmart",
            caller_id_name: callerIdName,
            caller_id_number: callerIdNumber,
            did: destinationNumber,
            date: callstart,
            duration: billsecond,
            call_status: callState,
            answered_call: dialedUser,
          };

          logger.debug("[CDR] Webhook data to send", { webHookData });

          try {
            const responseWebhooks = await axios.post(url, webHookData, {
              headers: { "Content-Type": "application/json" },
              timeout: 10000,
            });

            logger.debug("[CDR] Response webhooks", {
              status: responseWebhooks.status,
            });
          } catch (webhookError) {
            logger.error("[CDR] Webhook post failed", {
              error: webhookError,
            });
          }
        }
      }
    } else {
      // Regular CDR processing
      if (autoCampaignCheck) {
        logger.debug("[CDR] Auto Campaign call");
        if (variablesValue["sip_h_X-Autocalltype"] !== "true") {
          userUuid = "";
          logger.debug("[CDR] Remove user_uuid");
        }
      }

      const didPstn = variablesValue.did_pstn || "";

      const insertCdrJson = {
        uuid: bridgeUuid,
        user_uuid: userUuid,
        extension_uuid: extensionUuid,
        tenant_uuid: tenantUuid,
        trunk_uuid: trunkUuid,
        sip_user: sipUser,
        caller_id_name: callerIdName,
        caller_id_number: callerIdNumber,
        destination_number: destinationNumber,
        billsecond: billsecond,
        disposition: disposition,
        direction: direction,
        call_type: callType,
        callstart: callstart,
        recording_path: recordingPath,
        pbx_feature: pbxFeature,
        pbx_feature_uuid: pbxFeatureUuid,
        authentication_type: authenticationType,
        custom_callid: customCallid,
        ip_map_uuid: ipMapUuid,
        call_state: callStateMain,
        lead_uuid: leadUuid,
        campaign_uuid: campaignUuid,
        call_mode: callMode,
        number_masking: numberMasking,
        dial_method: dialMethod,
        hangup_cause: hangupCause,
        auto_call_flag: autoCallFlag,
        ivr_group: ivrGroup,
        disconnected_by: disconnectedBy,
        createdAt: createdAt,
        circle_uuid: telecomCircle,
        callend: callend,
        hold_time: holdsecond,
        waiting_time: waitsecond,
        total_duration: totalduration,
        ringing_time: ringingsecond,
        ivr_feedback_score: feedbackScore,
      };

      logger.debug("[CDR_PROCESSING] First harsh CDR Insert", {
        insertCdrJson,
      });
      console.log("inserting data in cdrs ........");
      await collections.cdrs.insertOne(insertCdrJson);
    }

    // Handle IVR timeout and call queue timeout
    if (
      ["IVR_TIMEOUT", "CALLQUEUE_TIMEOUT"].includes(disposition) ||
      (ivrMenuStatus && ivrMenuStatus !== "success")
    ) {
      // ,,,,,,,,,,,,,,,,

      logger.debug("insidedeee dispostion time out data", destinationNumber);

      // String manipulation
      let normalized = String(destinationNumber).split("_")[0];
      normalized = normalized.replace(/^\+/, ""); // equivalent to lstrip("+")

      const db_call_id = `+${normalized}`;
      logger.debug(
        "insidedeee dispostion callllllllllll",
        db_call_id,
        "normalllll",
        normalized,
      );

      // Find one with OR condition collections.cdrs.insertOne(insertCdrJsonIncoming)
      const blended_data = await collections.blendedCampaign.findOne({
        $or: [
          { call_id_value: db_call_id },
          { call_id_value: `+${normalized}` },
        ],
      });

      if (blended_data) {
        const campaign_uuid = blended_data.uuid;

        logger.debug(
          leadUuid,
          " insidedeee dispostion time out data ",
          campaign_uuid,
          " :lead group: ",
          blended_data.lead_group_uuid,
        );

        // Update with upsert
        await collections.leadsInQueue.updateOne(
          { lead_management_uuid: leadUuid },
          {
            $set: {
              tenant_uuid: tenantUuid,
              lead_group_uuid: blended_data.lead_group_uuid,
              campaign_uuid: campaign_uuid,
              lead_sent: "1",
              agent_uuid: [],
              createdAt: createdAt,
              updatedAt: createdAt,
            },
          },
          { upsert: true },
        );

        logger.debug("insidedeee dispostion time out data 2 leads in queueee");

        const filter = { code: "QUMS", tenant_uuid: tenantUuid };
        const disposition_collection_data =
          await collections.disposition.findOne(filter);

        let disposition_uuid = "";
        if (disposition_collection_data) {
          disposition_uuid = disposition_collection_data.disposition_uuid;
        }

        if (!hangupCause) {
          await collections.leadManagement.updateOne(
            { lead_management_uuid: leadUuid },
            {
              $set: {
                lead_status: disposition_uuid,
                lead_group_uuid: blended_data.lead_group_uuid,
                campaign_uuid: campaign_uuid,
              },
            },
          );
        }
      }

      // '''''''''''''''''''''
      const incomingBridgeUuid = bridgeUuid + "incoming";
      // direction = "inbound";

      const insertCdrJsonIncoming = {
        uuid: incomingBridgeUuid,
        user_uuid: userUuid,
        extension_uuid: extensionUuid,
        tenant_uuid: tenantUuid,
        trunk_uuid: trunkUuid,
        sip_user: sipUser,
        caller_id_name: callerIdName,
        caller_id_number: callerIdNumber,
        destination_number: destinationNumber,
        billsecond: billsecond,
        disposition: disposition,
        direction: direction,
        call_type: callType,
        callstart: callstart,
        recording_path: recordingPath,
        pbx_feature: pbxFeature,
        pbx_feature_uuid: pbxFeatureUuid,
        authentication_type: authenticationType,
        custom_callid: customCallid,
        ip_map_uuid: ipMapUuid,
        call_state: callStateMain,
        lead_uuid: leadUuid,
        campaign_uuid: campaignUuid,
        call_mode: callMode,
        number_masking: numberMasking,
        dial_method: dialMethod,
        hangup_cause: hangupCause,
        auto_call_flag: autoCallFlag,
        ivr_group: ivrGroup,
        disconnected_by: disconnectedBy,
        createdAt: createdAt,
        circle_uuid: telecomCircle,
        callend: callend,
        hold_time: holdsecond,
        waiting_time: waitsecond,
        total_duration: totalduration,
        ringing_time: ringingsecond,
        ivr_feedback_score: feedbackScore,
      };

      logger.debug(
        "[CDR] Second harsh CDR FOR INBOUND FAIL OR DIRECT OUTBOUND Insert",
        { insertCdrJsonIncoming }
      );

      await collections.cdrs.insertOne(insertCdrJsonIncoming);
    }

    // Insert recording if exists
    if (billsecond && billsecond !== 0 && recordingPath) {
      const insertRecordingJson = {
        uuid: bridgeUuid,
        user_uuid: userUuid,
        extension_uuid: extensionUuid,
        tenant_uuid: tenantUuid,
        caller_id_name: callerIdName,
        caller_id_number: callerIdNumber,
        destination_number: destinationNumber,
        billsecond: billsecond,
        recording_path: recordingPath,
        direction: direction,
        campaign_uuid: campaignUuid,
        callstart: callstart,
        call_state: callStateMain,
        createdAt: createdAt,
      };

      await collections.recordings.insertOne(insertRecordingJson);
      logger.debug("[CDR] Recording Insert", { insertRecordingJson });

      // Upload recording to Google Drive
      try {
        const driveSettings = await googleDriveSettingsManager.getSettings(
          tenantUuid
        );

        if (driveSettings && driveSettings.enabled) {
          logger.info("[CDR_PROCESSING] Starting Google Drive upload", {
            recordingPath,
            cdrUuid: bridgeUuid,
            tenant_uuid: tenantUuid,
          });

          const cdrData = {
            uuid: bridgeUuid,
            user_uuid: userUuid,
            tenant_uuid: tenantUuid,
            callstart: callstart,
            createdAt: createdAt,
          };

          const uploadResult = await googleDriveUploader.processRecording(
            insertRecordingJson,
            cdrData
          );

          if (uploadResult) {
            logger.success("[CDR_PROCESSING] Google Drive upload completed", {
              fileId: uploadResult.fileId,
              publicUrl: uploadResult.publicUrl,
            });
          } else {
            logger.warn(
              "[CDR_PROCESSING] Google Drive upload failed (CDR processing continues)"
            );
          }
        }
      } catch (uploadError) {
        logger.error("[CDR_PROCESSING] Google Drive upload error", {
          error: uploadError.message,
        });
      }
    }

    // Update CDR summary
    const todayDate = new Date().toISOString().slice(0, 10);
    const summaryFilter = {
      date: todayDate,
      tenant_uuid: tenantUuid,
      user_uuid: userUuid,
      direction: direction,
      createdAt: createdAt,
    };

    const failCount = billsecond > 0 ? 0 : 1;
    const successCount = billsecond > 0 ? 1 : 0;
    const totalCount = 1;

    const summaryUpdate = {
      $inc: {
        fail: failCount,
        success: successCount,
        total: totalCount,
        billsecond: billsecond,
      },
    };

    const summaryResult = await collections.cdrsSummary.findOneAndUpdate(
      summaryFilter,
      summaryUpdate,
      { upsert: true }
    );

    logger.debug("[CDR_PROCESSING] Summary", {
      message: summaryResult
        ? "Summary exists and was updated"
        : "Summary Record does not exist so updated",
      result: summaryResult || summaryUpdate,
    });

    // Handle receiver extension UUID (local call - second leg)
    if (receiverExtensionUuid) {
      // direction = "inbound";
      // disconnectedBy = "";

      // if (hangupPartyDisposition === "send_bye") {
      //   disconnectedBy = direction === "inbound" ? "0" : "1";
      // } else if (["recv_bye", "recv_cancel"].includes(hangupPartyDisposition)) {
      //   disconnectedBy = direction === "inbound" ? "1" : "0";
      // } else if (hangupPartyDisposition === "send_refuse") {
      //   disconnectedBy = direction === "inbound" ? "0" : "1";
      // }

      logger.debug("[CDR] Inbound disconnected by", { disconnectedBy });

      const sip_call_id = variablesValue.sip_call_id || customCallid;

      const insertSecondLegCdrJson = {
        uuid: bridgeUuid + "-receiver",
        user_uuid: receiverExtensionUuid,
        extension_uuid: receiverExtensionUuid,
        tenant_uuid: tenantUuid,
        trunk_uuid: trunkUuid,
        sip_user: sipUser,
        caller_id_name: callerIdName,
        caller_id_number: callerIdNumber,
        destination_number: destinationNumber,
        billsecond: billsecond,
        disposition: disposition,
        direction: direction,
        call_type: callType,
        callstart: callstart,
        recording_path: recordingPath,
        pbx_feature: pbxFeature,
        pbx_feature_uuid: pbxFeatureUuid,
        authentication_type: authenticationType,
        custom_callid: sip_call_id,
        ip_map_uuid: ipMapUuid,
        call_state: callState,
        lead_uuid: leadUuid,
        campaign_uuid: campaignUuid,
        call_mode: callMode,
        number_masking: numberMasking,
        dial_method: dialMethod,
        call_stick_status: callStickStatus,
        hangup_cause: hangupCause,
        auto_call_flag: autoCallFlag,
        ivr_group: ivrGroup,
        disconnected_by: disconnectedBy,
        createdAt: createdAt,
        circle_uuid: telecomCircle,
        callend: callend,
        hold_time: holdsecond,
        waiting_time: waitsecond,
        total_duration: totalduration,
        ringing_time: ringingsecond,
        ivr_feedback_score: feedbackScore,
      };

      await collections.cdrs.insertOne(insertSecondLegCdrJson);
      logger.debug("[CDR] Three Second Leg Insert", {
        insertSecondLegCdrJson,
      });

      if (billsecond && billsecond !== 0 && recordingPath) {
        const insertSecondLegRecordingJson = {
          uuid: bridgeUuid + "-receiver",
          user_uuid: receiverExtensionUuid,
          extension_uuid: receiverExtensionUuid,
          tenant_uuid: tenantUuid,
          caller_id_name: callerIdName,
          caller_id_number: callerIdNumber,
          destination_number: destinationNumber,
          billsecond: billsecond,
          recording_path: recordingPath,
          direction: direction,
          campaign_uuid: campaignUuid,
          callstart: callstart,
          call_state: callState,
          createdAt: createdAt,
        };

        await collections.recordings.insertOne(insertSecondLegRecordingJson);
        logger.debug("[CDR] Second Leg Recording Insert", {
          insertSecondLegRecordingJson,
        });
        // Upload recording to Google Drive (synchronous)
        if (config?.googleDrive?.enabled) {
          try {
            logger.info(
              "[CDR_PROCESSING] Starting Google Drive upload (Second Leg)",
              {
                recordingPath,
                cdrUuid: bridgeUuid + "-receiver",
              }
            );

            const cdrData = {
              uuid: bridgeUuid + "-receiver",
              user_uuid: receiverExtensionUuid,
              callstart: callstart,
            };

            const uploadResult = await googleDriveUploader.processRecording(
              insertSecondLegRecordingJson,
              cdrData
            );

            if (uploadResult) {
              logger.success(
                "[CDR_PROCESSING] Google Drive upload completed (Second Leg)",
                {
                  fileId: uploadResult.fileId,
                  publicUrl: uploadResult.publicUrl,
                }
              );
            } else {
              logger.warn(
                "[CDR_PROCESSING] Google Drive upload failed (Second Leg)"
              );
            }
          } catch (uploadError) {
            logger.error(
              "[CDR_PROCESSING] Google Drive upload error (Second Leg)",
              {
                error: uploadError.message,
              }
            );
          }
        }
      }

      const todayDateSecondLeg = new Date().toISOString().slice(0, 10);
      const summaryFilterSecondLeg = {
        date: todayDateSecondLeg,
        tenant_uuid: tenantUuid,
        user_uuid: receiverExtensionUuid,
        direction: direction,
        createdAt: createdAt,
      };

      const summaryUpdateSecondLeg = {
        $inc: {
          fail: failCount,
          success: successCount,
          total: totalCount,
          billsecond: billsecond,
        },
      };

      const summaryResultSecondLeg =
        await collections.cdrsSummary.findOneAndUpdate(
          summaryFilterSecondLeg,
          summaryUpdateSecondLeg,
          { upsert: true }
        );

      logger.debug("[CDR] Second Leg Summary", {
        message: summaryResultSecondLeg
          ? "Summary exists and was updated"
          : "Summary Record does not exist so updated",
        result: summaryResultSecondLeg || summaryUpdateSecondLeg,
      });
    }

    // Handle PBX features (ring group, call queue) - extra entries
    if (
      // !incomingExtraEntryFlag &&
      ["ring_group", "call_queue"].includes(pbxFeature)
    ) {
      logger.debug("[CDR] PBX Feature processing", { pbxFeature });

      let dialedUserForPbx = variablesValue.dialed_user || "";

      if (pbxFeature === "call_queue") {
        const ccAgent = variablesValue.cc_agent || "";
        dialedUserForPbx = ccAgent.replace("@default", "") || dialedUserForPbx;
        const dialedUserExplode = dialedUserForPbx.split(".");
        extensionUuid = dialedUserExplode[0];
      } else {
        extensionUuid = dialedUserForPbx;
      }

      const extensionSearchQuery = {
        username: extensionUuid,
        tenant_uuid: tenantUuid,
      };

      const extensionsResult = await collections.extensions.findOne(
        extensionSearchQuery
      );

      logger.debug("[CDR] IN PBX FEATURE EXTRA ENTRY", {
        pbxFeature,
        extensionSearchQuery,
      });

      if (extensionsResult && extensionsResult.uuid) {
        extensionUuid = extensionsResult.uuid;
        userUuid = extensionsResult.user_uuid;
        // direction = "inbound";
        const sipCallId = variablesValue.sip_call_id || customCallid;

        if (
          callType === "did" &&
          userUuid &&
          !leadUuid &&
          callType !== "local"
        ) {
          callerIdNumber = callerIdNumber.replace(/^\+/, "");
          callerIdName = callerIdName.replace(/^\+/, "");
          leadUuid = await leadEntryManagement(
            callerIdNumber,
            userUuid,
            tenantUuid
          );
        }

        if (callState === "missed") {
          await sendNotification(
            tenantUuid,
            callerIdName,
            callerIdNumber,
            destinationNumber,
            callstart,
            pbxFeature
          );
        }

        const harshUser = variablesValue.harsh_user || "";

        if (pbxFeature === "ring_group") {
          const lastBridgeUuid = variablesValue.last_bridge_to || "";

          if (lastBridgeUuid) {
            const callData = await collections.tmpRinggroup.findOne({
              call_uuid: lastBridgeUuid,
            });

            logger.debug("[CDR] Call data", { callData });

            if (callData) {
              userUuid = callData.user_uuid;
              extensionUuid = callData.extension_uuid;
              logger.debug("[CDR] User UUID and Extension UUID", {
                userUuid,
                extensionUuid,
              });
            }
          } else {
            logger.debug("[CDR] This is last bridge else");

            const ringgroupUuid = variablesValue.ringgroup_id || "";
            const ringgroupFromDomain =
              variablesValue.ringgroup_from_domain || "";

            logger.debug("[CDR] Ringgroup UUID", { ringgroupUuid });

            const ringgroupData = await collections.ringGroup.findOne({
              uuid: ringgroupUuid,
            });

            if (ringgroupData) {
              const extensionList = ringgroupData.extension_list || [];
              logger.debug("[CDR] Extension list", { extensionList });

              for (const extensionInfo of extensionList) {
                if (extensionInfo) {
                  const extensionType = extensionInfo.extension_type;
                  const extension = extensionInfo.extension;
                  const extensionUserUuid = extensionInfo.user_uuid;

                  if (parseInt(extensionType) === 0) {
                    try {
                      // Call register.js to check if extension is registered
                      const isRegistered = await checkRegistration(
                        extension,
                        ringgroupFromDomain
                      );

                      logger.debug("[CDR] Registration check", {
                        extension,
                        isRegistered,
                      });

                      if (isRegistered) {
                        userUuid = extensionUserUuid;
                        logger.debug("[CDR] User UUID updated", { userUuid });
                        break;
                      }
                    } catch (regError) {
                      logger.error("[CDR] Registration check failed", {
                        error: regError.message,
                        extension,
                        ringgroupFromDomain,
                      });
                    }
                  }
                }
              }
            }
          }
        }

        if (
          stickyAgentFlag === "1" &&
          callType !== "local" &&
          callState !== "missed"
        ) {
          logger.debug("[CDR] Sticky agent", {
            inboundAnsweredUserUuid,
            inboundAnsweredExtensionUuid,
            userUuid,
            harshUser,
          });
          await setCallerSticky(userUuid, tenantUuid, callerIdNumber);
        }

        const agentLiveData = await collections.agentLiveReport.findOne({
          agent_uuid: userUuid,
        });

        logger.debug("[CDR] Campaign uuid of inbound call", {
          agentLiveData,
        });

        campaignUuid = agentLiveData
          ? agentLiveData.campaign_uuid
          : campaignUuid;

        const insertCdrJson = {
          uuid: bridgeUuid + "-" + pbxFeature,
          user_uuid: userUuid,
          extension_uuid: extensionUuid,
          tenant_uuid: tenantUuid,
          trunk_uuid: trunkUuid,
          sip_user: sipUser,
          caller_id_name: callerIdName,
          caller_id_number: callerIdNumber,
          destination_number: destinationNumber,
          billsecond: billsecond,
          disposition: disposition,
          direction: direction,
          call_type: callType,
          callstart: callstart,
          recording_path: recordingPath,
          pbx_feature: pbxFeature,
          pbx_feature_uuid: pbxFeatureUuid,
          authentication_type: authenticationType,
          custom_callid: sipCallId,
          ip_map_uuid: ipMapUuid,
          call_state: callState,
          lead_uuid: leadUuid,
          campaign_uuid: campaignUuid,
          call_mode: callMode,
          number_masking: numberMasking,
          dial_method: dialMethod,
          call_stick_status: callStickStatus,
          hangup_cause: hangupCause,
          auto_call_flag: autoCallFlag,
          ivr_group: ivrGroup,
          disconnected_by: disconnectedBy,
          createdAt: createdAt,
          circle_uuid: telecomCircle,
          callend: callend,
          hold_time: holdsecond,
          waiting_time: waitsecond,
          total_duration: totalduration,
          ringing_time: ringingsecond,
          ivr_feedback_score: feedbackScore,
        };

        await collections.cdrs.insertOne(insertCdrJson);
        logger.debug("[CDR] Four harsh Extra PBX CDR Insert", {
          insertCdrJson,
        });

        if (billsecond && billsecond !== 0 && recordingPath) {
          const insertRecordingJson = {
            uuid: bridgeUuid + "-" + pbxFeature,
            user_uuid: userUuid,
            extension_uuid: extensionUuid,
            tenant_uuid: tenantUuid,
            caller_id_name: callerIdName,
            caller_id_number: callerIdNumber,
            destination_number: destinationNumber,
            billsecond: billsecond,
            recording_path: recordingPath,
            direction: direction,
            campaign_uuid: campaignUuid,
            callstart: callstart,
            call_state: callState,
            createdAt: createdAt,
          };

          await collections.recordings.insertOne(insertRecordingJson);
          logger.debug("[CDR_PROCESSING] Extra PBX Recording Insert", {
            insertRecordingJson,
          });

          // Upload recording to Google Drive (synchronous)
          if (config?.googleDrive?.enabled) {
            try {
              logger.info(
                "[CDR_PROCESSING] Starting Google Drive upload (PBX Feature)",
                {
                  recordingPath,
                  cdrUuid: bridgeUuid + "-" + pbxFeature,
                }
              );

              const cdrData = {
                uuid: bridgeUuid + "-" + pbxFeature,
                user_uuid: userUuid,
                callstart: callstart,
              };

              const uploadResult = await googleDriveUploader.processRecording(
                insertRecordingJson,
                cdrData
              );

              if (uploadResult) {
                logger.success(
                  "[CDR_PROCESSING] Google Drive upload completed (PBX Feature)",
                  {
                    fileId: uploadResult.fileId,
                    publicUrl: uploadResult.publicUrl,
                  }
                );
              } else {
                logger.warn(
                  "[CDR_PROCESSING] Google Drive upload failed (PBX Feature)"
                );
              }
            } catch (uploadError) {
              logger.error(
                "[CDR_PROCESSING] Google Drive upload error (PBX Feature)",
                {
                  error: uploadError.message,
                }
              );
            }
          }
        }

        const todayDateSecond = new Date().toISOString().slice(0, 10);
        const summaryFilterPbx = {
          date: todayDateSecond,
          tenant_uuid: tenantUuid,
          user_uuid: userUuid,
          direction: direction,
          createdAt: createdAt,
        };

        const summaryUpdatePbx = {
          $inc: {
            fail: failCount,
            success: successCount,
            total: totalCount,
            billsecond: billsecond,
          },
        };

        const summaryResultPbx = await collections.cdrsSummary.findOneAndUpdate(
          summaryFilterPbx,
          summaryUpdatePbx,
          { upsert: true }
        );

        logger.debug("[CDR] Extra Summary PBX", {
          message: summaryResultPbx
            ? "Extra Summary exists and was updated"
            : "Extra Summary Record does not exist so updated",
          result: summaryResultPbx || summaryUpdatePbx,
        });
      }
    }

    // voicebroadcast cdr processing
    if (voiceBroadcast) {
      const voicebroadcastType = variablesValue["sip_h_P-voicebroadcast_type"] || "";
      let insertBroadcastString = "";
      let mainLeadDisposition = "";
      const broadcastUuid = uuidv4();

      logger.debug("[CDR] Voicebroadcast type", { voicebroadcastType });

      if (voicebroadcastType === "0") {
        // Broadcast type = Survey
        const surveyString = variablesValue["sip_h-p_survey_string"] || "";
        const surveyDisposition = await getVoicebroadcastDisposition(
          voicebroadcastType,
          leadUuid,
          campaignUuid,
          billsecond
        );

        mainLeadDisposition = String(surveyDisposition);
        insertBroadcastString = {
          createdAt,
          callstart,
          destination_number: destinationNumber,
          caller_id_number: callerIdNumber,
          billsecond,
          total_duration: totalduration,
          ringing_time: ringingsecond,
          type: voicebroadcastType,
          disposition_uuid: mainLeadDisposition,
          voicebroadcast_uuid: campaignUuid,
          tenant_uuid: tenantUuid,
          lead_uuid: leadUuid,
          survey_details: surveyString,
          uuid: broadcastUuid,
        };
      } else if (voicebroadcastType === "1") {
        // Broadcast type = IVR
        const dialedDigits = String(variablesValue["digits_dialed"] || "");
        const skipFlag = String(variablesValue["sip_h_P-skip_flag"] || "");

        // let digits = skipFlag === "true" ? dialedDigits.slice(1) : dialedDigits;

        // Note: The Python logic used feedbackScore/pressed_digits interchangeably here
        const pressedDigits = feedbackScore || "";

        const ivrDisposition = await getVoicebroadcastDisposition(
          voicebroadcastType,
          leadUuid,
          campaignUuid,
          billsecond
        );

        mainLeadDisposition = String(ivrDisposition);
        insertBroadcastString = {
          callstart,
          destination_number: destinationNumber,
          caller_id_number: callerIdNumber,
          billsecond,
          total_duration: totalduration,
          ringing_time: ringingsecond,
          type: voicebroadcastType,
          DTMF: String(pressedDigits),
          hangup_cause: mainLeadDisposition,
          voicebroadcast_uuid: campaignUuid,
          tenant_uuid: tenantUuid,
          lead_uuid: leadUuid,
          uuid: broadcastUuid,
          createdAt,
        };
      } else if (voicebroadcastType === "2") {
        // Broadcast type = Not Interested
        const niDisposition = await getVoicebroadcastDisposition(
          voicebroadcastType,
          leadUuid,
          campaignUuid,
          billsecond
        );
        mainLeadDisposition = String(niDisposition);
        insertBroadcastString = {
          callstart,
          destination_number: destinationNumber,
          caller_id_number: callerIdNumber,
          billsecond,
          total_duration: totalduration,
          ringing_time: ringingsecond,
          type: voicebroadcastType,
          disposition_uuid: mainLeadDisposition,
          voicebroadcast_uuid: campaignUuid,
          tenant_uuid: tenantUuid,
          lead_uuid: leadUuid,
          uuid: broadcastUuid,
          createdAt,
        };
      } else if (voicebroadcastType === "3") {
        // Broadcast type = DNC
        const dncDisposition = await getVoicebroadcastDisposition(
          voicebroadcastType,
          leadUuid,
          campaignUuid,
          billsecond
        );
        mainLeadDisposition = String(dncDisposition);
        insertBroadcastString = {
          callstart,
          destination_number: destinationNumber,
          caller_id_number: callerIdNumber,
          billsecond,
          total_duration: totalduration,
          ringing_time: ringingsecond,
          type: voicebroadcastType,
          disposition_uuid: mainLeadDisposition,
          voicebroadcast_uuid: campaignUuid,
          tenant_uuid: tenantUuid,
          lead_uuid: leadUuid,
          uuid: broadcastUuid,
          createdAt,
        };
      } else if (voicebroadcastType === "") {
        let hangupCauseVar = variablesValue["sip_invite_failure_phrase"] || "";
        if (hangupCauseVar === "") {
          hangupCauseVar = variablesValue["sip_hangup_disposition"] || "";
        }

        if (hangupCauseVar !== "") {
          const hangupDisposition = await getVoicebroadcastDisposition(
            hangupCauseVar,
            leadUuid,
            campaignUuid,
            billsecond
          );
          mainLeadDisposition = String(hangupDisposition);
          insertBroadcastString = {
            callstart,
            destination_number: destinationNumber,
            caller_id_number: callerIdNumber,
            billsecond,
            type: "",
            disposition_uuid: mainLeadDisposition,
            voicebroadcast_uuid: campaignUuid,
            tenant_uuid: tenantUuid,
            lead_uuid: leadUuid,
            uuid: broadcastUuid,
            createdAt,
          };
        }
      }

      if (insertBroadcastString !== "") {
        await collections.voiceBroadcastDetails.insertOne(insertBroadcastString);
        logger.debug("[CDR] Voicebroadcast details inserted");

        await collections.leadManagement.updateOne(
          { lead_management_uuid: leadUuid },
          { $set: { lead_status: mainLeadDisposition } }
        );
      }
    }
    // voicebroadcast cdr processing end
    logger.info("==============================");
    logger.success("[CDR] CDR Processing Completed Successfully");
    logger.info("==============================\n");
    return res.status(200).json({
      status: "success",
      message: "CDR processed successfully",
    });
  } catch (error) {
    logger.info("==============================");
    logger.error("[CDR] Error Processing CDR", {
      error: error.message,
      stack: error.stack,
    });
    logger.info("==============================\n");
    return res.status(500).json({ error: error.message });
  }
});

/**
 * Health check endpoint
 */
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

/**
 * Start server
 */
async function startServer() {
  try {
    // Connect to MongoDB
    await connectMongoDB();
    const rawCollections = getCollections();

    // Connect to RabbitMQ
    await rabbitMQ.connect();
    logger.success("✅ RabbitMQ connection established");

    // Wrap collections with queue functionality
    collections = wrapCollectionsWithQueue(rawCollections);
    logger.success("✅ Collections wrapped with queue functionality");

    // Start Express server
    const server = app.listen(config.app.port, "0.0.0.0", () => {
      logger.success(`🚀 CDR Server running on port ${config.app.port}`);
      logger.info(`Environment: ${config.app.env}`);
      logger.info(`Log directory: ${config.logging.dir}`);
    });

    // Graceful shutdown
    const gracefulShutdown = async (signal) => {
      logger.info(`${signal} received, shutting down gracefully...`);

      server.close(async () => {
        logger.info("HTTP server closed");
        await rabbitMQ.close();
        logger.info("RabbitMQ connection closed");
        await closeMongoDB();
        logger.info("MongoDB connection closed");
        process.exit(0);
      });

      // Force close after 10 seconds
      setTimeout(() => {
        logger.error("Forced shutdown after timeout");
        process.exit(1);
      }, 10000);
    };

    process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
    process.on("SIGINT", () => gracefulShutdown("SIGINT"));
  } catch (error) {
    logger.error("[SERVER] Failed to start", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the server
startServer();
