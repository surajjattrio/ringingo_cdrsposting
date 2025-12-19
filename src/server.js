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

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const execAsync = promisify(exec);
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

    const autoHangup = variablesValue.sip_h_X_amd_hangup || "";
    let filterCode;

    if (autoHangup === "true") {
      filterCode = "AA";
    } else if (hangupReason === "ALLOTTED_TIMEOUT") {
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
    const autoLeadUuid = variablesValue.sip_h_P_lead_uuid || "";

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
    const leadUuid = variablesValue.sip_h_P_lead_uuid || "";
    const filter = { uuid: campaignUuid };

    const oCampaign = await collections.outboundCampaign.findOne(filter);
    const bCampaign = await collections.blendedCampaign.findOne(filter);
    const campaign = oCampaign || bCampaign;

    if (campaign) {
      const maxRetries = campaign.max_retries;
      const retriesTime = campaign.retries_time;
      const currentDate = new Date();
      const newDate = new Date(
        currentDate.getTime() + parseInt(retriesTime) * 60000
      );
      const isoDate = `ISODate(${newDate.toISOString()}Z)`;

      const leadInQueue = await collections.leadsInQueue.findOne({
        campaign_uuid: campaignUuid,
        lead_management_uuid: leadUuid,
      });

      if (leadInQueue) {
        if ("lead_resent_count" in leadInQueue) {
          if (parseInt(leadInQueue.lead_resent_count) < parseInt(maxRetries)) {
            const leadType = parseInt(leadInQueue.lead_resent_count) + 1;

            if (parseInt(leadType) === parseInt(maxRetries)) {
              await collections.leadsInQueue.deleteOne({
                campaign_uuid: campaignUuid,
                lead_management_uuid: leadUuid,
              });
            } else {
              await collections.leadsInQueue.updateOne(
                { campaign_uuid: campaignUuid, lead_management_uuid: leadUuid },
                {
                  $set: {
                    lead_resent_count: leadType,
                    lead_sent: "1",
                    resend_lead_time: isoDate,
                  },
                }
              );
            }
          }
        } else {
          await collections.leadsInQueue.updateOne(
            { campaign_uuid: campaignUuid, lead_management_uuid: leadUuid },
            {
              $set: {
                lead_resent_count: "1",
                lead_sent: "1",
                resend_lead_time: isoDate,
              },
            }
          );
          logger.debug("[MANAGE_LEAD_RETRIES] Updated successfully");
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

    const leadMgmt = await collections.leadManagement.findOne({
      phone_number: destNumber,
      tenant_uuid: tenantUuid,
    });

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
      logger.debug("[LEAD_ENTRY_MANAGEMENT] Lead inserted", { leadUuid });
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
      1: "IVR",
      2: "NI",
      3: "DNC",
      CANCEL: "NA",
      "Busy Here": "Busy",
      "Temporarily Unavailable": "NAVL",
      send_bye: "NAVL",
    };

    const code = filterCodes[vbType] || (duration !== "0" ? "NDP" : "NA");
    const disposition = await collections.disposition.findOne({ code });

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

      const leadManagementData = await collections.leadManagement.findOne({
        lead_management_uuid: leadUuid,
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
          const maxCountDoc = await collections.leadBroadcastQueue.findOne(
            { lead_group_uuid: leadManagementData.lead_group_uuid },
            { sort: { count: -1 } }
          );

          const maxCount = maxCountDoc ? maxCountDoc.count : 0;
          const newCount = maxCount + 1;
          const leadBroadcastUuid = uuidv4();

          const insertLeadBroadcastQueue = {
            lead_group_uuid: leadManagementData.lead_group_uuid,
            tenant_uuid: leadManagementData.tenant_uuid,
            lead_management_uuid: leadManagementData.lead_management_uuid,
            count: newCount,
            uuid: leadBroadcastUuid,
          };

          await collections.leadBroadcastQueue.insertOne(
            insertLeadBroadcastQueue
          );
          await collections.leadManagement.updateOne(
            { lead_management_uuid: leadUuid },
            { $inc: { retries_count: 1 } }
          );

          logger.debug(
            "[GET_VOICEBROADCAST_DISPOSITION] Lead broadcast queue inserted"
          );
        }
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
      callType,
    });

    if (startsWithNumber(callerIdName)) {
      callerIdName = "Anonymous";
    }

    const tenantInfo = await collections.tenant.findOne({ uuid: tenantUuid });

    if (!tenantInfo) {
      logger.debug("[SEND_NOTIFICATION] Tenant not found");
      return;
    }

    const senderEmail = "7b26b2da1b254e6133d47b2cc3a2184a";
    const senderPassword = "1b7f00c11402cc3c26cacf592725ea87";
    const fromEmail = "notifications@test.io";
    const recipientEmail = String(tenantInfo.email);
    callType = callType === "local" ? "sip" : callType;

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
                Missed by: ${callType}</p>
                <p>Thanks for using COMPANY</p>
                <br>
            </body>
            </html>
        `;

    const transporter = nodemailer.createTransporter({
      host: "in-v3.mailjet.com",
      port: 587,
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

    logger.success("Email sent successfully!");
  } catch (error) {
    logger.error("[SEND_NOTIFICATION] Failed to send email", {
      error: error.message,
      stack: error.stack,
      tenantUuid,
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

    const userRoleInfo = await collections.userRole.findOne({
      uuid: userInfo.user_role_uuid,
    });

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
    destinationNumber = String(destinationNumber);
    let firstDigit = destinationNumber[0] || "";
    let prefixLen = 0;
    let longestPrefix = "";
    let longestPrefixName = "";

    while (destinationNumber) {
      const results = await collections.telecomCircle
        .find({
          tenant_uuid: tenantUuid,
          prefix: { $regex: `^${firstDigit}` },
        })
        .toArray();

      for (const prefixDetails of results) {
        if (prefixLen === 0 || prefixLen < prefixDetails.prefix.length) {
          prefixLen = prefixDetails.prefix.length;
          longestPrefix = prefixDetails.prefix;
          longestPrefixName = prefixDetails.uuid;
          logger.debug("[GET_TELECOM_CIRCLE] Prefix match", {
            longestPrefix,
            longestPrefixName,
          });
        }
      }

      destinationNumber = destinationNumber.slice(0, -1);

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
    const data = req.body;
    logger.info("==============================");
    logger.info("[CDR] Incoming CDR Request");
    logger.info("==============================");
    if (!data || !data.variables) {
      logger.error("[CDR_PROCESSING] Invalid or missing variables data");
      return res
        .status(400)
        .json({ error: "Invalid or missing variables data" });
    }

    variablesValue = data.variables;
    logger.debug("[CDR_PROCESSING] Received CDR data", {
      uuid: variablesValue.uuid || "unknown",
      user_uuid: variablesValue.user_uuid || "N/A",
      tenant_uuid: variablesValue.tenant_uuid || "N/A",
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

    const autoCampaign = variablesValue.sip_h_P_auto_campaign || "";
    const threewayAgentExtensionUuid =
      variablesValue.threeway_agent_extension_uuid || "";

    if (threewayAgentExtensionUuid) {
      await collections.customerCallerid.deleteMany({
        user_uuid: threewayAgentExtensionUuid,
      });
    }

    let voiceBroadcast = variablesValue.sip_h_P_voicebroadcast || "";
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
      variablesValue.sip_h_P_auto_campaign_originate || "";
    if (autoCampaignOriginateUuid) {
      await manageAutoCampaignCount(variablesValue, autoCampaignOriginateUuid);
    }

    const autoAgentFlag = variablesValue.sip_h_P_auto_agent_flag || "";
    if (autoCampaignOriginateUuid && autoAgentFlag === "") {
      await manageAutoCampaign(variablesValue, autoCampaignOriginateUuid);
    }

    if (autoAgentFlag === "auto_agent") {
      filePassFlag = 1;
      return res.status(200).json({ status: "skipped auto_agent" });
    }

    let callstart =
      variablesValue.custom_callstart || variablesValue.sip_h_P_callstart || "";
    logger.debug("[CDR_PROCESSING] Call start", { callstart });

    const createdAt = new Date();

    if (!callstart) {
      const epochTime = variablesValue.start_epoch || "";
      if (epochTime) {
        const dtUtc = new Date(parseInt(epochTime) * 1000);
        callstart = dtUtc.toISOString().slice(0, 19).replace("T", " ");
        logger.debug("[CDR_PROCESSING] Auto campaign callstart", { callstart });
      }
    }

    const featureCodeFlag = variablesValue.feature_code_flag || "false";
    if (featureCodeFlag === "true") {
      filePassFlag = 1;
      return res.status(200).json({ status: "skipped feature code" });
    }

    if (!callstart) {
      filePassFlag = 1;
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
      variablesValue.sip_h_X_custom_callid ||
      variablesValue.sip_h_X_Custom_Callid ||
      "";
    const tenantUuid =
      variablesValue.tenant_uuid || variablesValue.sip_h_P_tenant_uuid || "";
    let direction = variablesValue.direction || "outbound";
    const bridgeUuid = variablesValue.bridge_uuid || uuidv4();
    let userUuid =
      variablesValue.user_uuid || variablesValue.sip_h_P_user_uuid || "";
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
      variablesValue.sip_h_X_Leaduuid ||
      variablesValue.sip_h_X_lead_uuid ||
      variablesValue.sip_h_P_lead_uuid ||
      "";

    if (leadUuid === "null") {
      leadUuid = "";
    }

    const hangupReason = variablesValue.hangup_cause || "";
    logger.debug("[CDR_PROCESSING] Hangup reason", { hangupReason });

    let autoCallFlag = "0";
    let hangupCause = "";
    let campaignUuid =
      variablesValue.sip_h_X_selectedcampaignuuid ||
      variablesValue.sip_h_P_campaign_uuid ||
      "";
    const campaignFlag = variablesValue.sip_h_P_campaign_flag || "";
    logger.debug("[CDR_PROCESSING] Campaign flag", { campaignFlag });

    const callStickStatus = variablesValue.call_stick_status || 1;
    const billsecond = parseInt(variablesValue.billsec || 0);

    // New fields
    const waitsecond = parseInt(variablesValue.waitsec || 0);
    const ringingsecond = parseInt(variablesValue.answersec || 0);
    const callend = variablesValue.end_stamp || "";
    logger.debug("[CDR_PROCESSING] Call end", { callend });

    const totalduration = parseInt(variablesValue.duration || 0);
    let holdsecond;

    if (autoCampaign || ipMapUuid || pbxFeature) {
      holdsecond =
        (firstLegStore && firstLegStore.holdsecond) ||
        parseInt(variablesValue.hold_accum_seconds || 0);
    } else {
      holdsecond = parseInt(variablesValue.hold_accum_seconds || 0);
    }

    logger.debug("[CDR_PROCESSING] Time details", {
      waitsecond,
      ringingsecond,
      callend,
      totalduration,
      holdsecond,
    });

    // Campaign statistics
    if (campaignUuid) {
      const currentDate = new Date();
      const campaignDate = currentDate.toISOString().slice(0, 10);
      const managerUuid = variablesValue.sip_h_P_manager_uuid || "";

      const campaignStatisticsFilter = {
        tenant_uuid: tenantUuid,
        campaign_uuid: campaignUuid,
        campaign_date: campaignDate,
        manager_uuid: managerUuid,
      };

      const campaignDurationCheck = 0 < billsecond && billsecond < 30 ? 1 : 0;
      let campaignStatisticsDropCount = 0;
      let campaignStatisticsFailCount = 0;
      let campaignStatisticsAmdCount = 0;
      let campaignStatisticsSuccessCount = 0;

      if (autoCampaign) {
        logger.debug("[CDR_PROCESSING] Checking lead originate issue");

        if (autoCampaign && !autoCampaignOriginateUuid && leadUuid) {
          logger.debug("[CDR_PROCESSING] Outbound auto campaign");
          if (billsecond > 0) {
            campaignStatisticsDropCount = 1;
          }
          hangupCause = await manageDropCallFlag(
            leadUuid,
            hangupReason,
            variablesValue
          );
          autoCallFlag = "2";
        } else if (autoCampaign && autoCampaignOriginateUuid && leadUuid) {
          logger.debug("[CDR_PROCESSING] Success but fail scenario");
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

      const autoHangup = variablesValue.sip_h_X_amd_hangup || "";
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

      logger.debug("[CDR_PROCESSING] Campaign statistics update", {
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
      dialedUser;
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

    const callType = variablesValue.call_type || "standard";
    if (["standard", "did"].includes(callType)) {
      direction = "outbound";
    }

    let recordingPath = "";
    logger.debug("[CDR_PROCESSING] Recording check", {
      bridge_uuid: variablesValue.bridge_uuid || "",
      autocalltype: variablesValue.sip_h_X_Autocalltype || "",
      recording_directory: variablesValue.recording_directory || "",
    });

    if ((variablesValue.custom_recording || "0") === "0") {
      logger.debug("[CDR_PROCESSING] Recording enabled");

      if (variablesValue.current_application === "record_session") {
        recordingPath = variablesValue.current_application_data || "";
      } else if (
        (variablesValue.execute_on_answer || "").includes("record_session")
      ) {
        recordingPath = variablesValue.execute_on_answer.replace(
          "record_session ",
          ""
        );
        logger.debug("[CDR_PROCESSING] Recording path in execute_on_answer", {
          recordingPath,
        });
      }

      if (variablesValue.sip_h_X_Autocalltype === "true") {
        const dialedUserForRec = variablesValue.sip_to_user || "";
        const recordingUuid = variablesValue.bridge_uuid || "";
        const recordingDirectory = variablesValue.recording_directory || "";

        if (recordingUuid) {
          recordingPath = `${recordingDirectory}/${dialedUserForRec}_${recordingUuid}.wav`;
        }
      }
    }

    const receiverExtensionUuid = variablesValue.receiver_extension_uuid || "";
    const originateDisposition = variablesValue.originate_disposition || "";

    if (["NO_ANSWER", "USER_BUSY"].includes(originateDisposition)) {
      callState = "missed";
      disposition = originateDisposition;
      // Note: billsecond is not reset to 0 here to match Python behavior
    }

    if (receiverExtensionUuid) {
      direction = "outbound";
    }

    let incomingExtraEntryFlag = 0;
    const callMode = campaignUuid ? "1" : "0";
    let numberMasking = "1";
    let dialMethod = "";

    logger.debug("[CDR_PROCESSING] Campaign check", {
      campaignUuid,
      campaignFlag,
    });

    if (campaignUuid) {
      const resultOutboundCampaign = await checkNumberMasking(
        campaignUuid,
        campaignFlag
      );
      logger.debug("[CDR_PROCESSING] Result outbound campaign", {
        resultOutboundCampaign,
      });

      if (resultOutboundCampaign) {
        numberMasking = resultOutboundCampaign.number_masking || "1";
        dialMethod = resultOutboundCampaign.dial_method || "";
      }
      logger.debug("[CDR_PROCESSING] Dial method of OC", { dialMethod });
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

    const autoCampaignCheck = variablesValue.sip_h_P_auto_campaign || "";
    const predictiveCampaign = variablesValue.sip_h_P_predictive_campaign || "";

    if (predictiveCampaign === "true") {
      destinationNumber =
        variablesValue.sip_h_P_original_number || destinationNumber;
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
      logger.debug("[CDR_PROCESSING] Insert predictive campaign", {
        insertPredictiveCampaign,
      });
    }

    if (autoCampaignCheck === "true") {
      logger.debug("[CDR_PROCESSING] Auto campaign CHANGE Destination");
      destinationNumber =
        variablesValue.sip_h_P_original_number || destinationNumber;
      callerIdName = variablesValue.Hunt_Callee_ID_Name || callerIdName;
      callerIdNumber =
        variablesValue.Hunt_Orig_Caller_ID_Number || callerIdNumber;
      numberMasking = variablesValue.sip_h_P_number_masking || numberMasking;
    }

    const hangupPartyDisposition = variablesValue.sip_hangup_disposition || "";
    logger.debug("[CDR_PROCESSING] Hangup party disposition", {
      hangupPartyDisposition,
      direction,
    });

    let disconnectedBy = "";
    if (hangupPartyDisposition === "send_bye") {
      disconnectedBy = direction === "inbound" ? "0" : "1";
    } else if (hangupPartyDisposition === "recv_bye") {
      disconnectedBy = direction === "inbound" ? "1" : "0";
    } else if (hangupPartyDisposition === "recv_cancel") {
      disconnectedBy = direction === "inbound" ? "1" : "0";
    } else if (hangupPartyDisposition === "send_refuse") {
      disconnectedBy = direction === "inbound" ? "0" : "1";
    }

    logger.debug("[CDR_PROCESSING] Disconnected by", { disconnectedBy });

    const telecomCircle = await getTelecomCircle(
      tenantUuid,
      direction === "outbound" ? destinationNumber : callerIdNumber
    );

    // Inbound CDR insertion
    if (ipMapUuid || !userUuid) {
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
      };

      await collections.inboundCdrs.insertOne(insertInboundCdrJson);
      logger.debug("[CDR_PROCESSING] Inbound CDR Insert", {
        insertInboundCdrJson,
      });

      // Webhook processing
      if (tenantUuid) {
        const webhookData = await collections.webHook.findOne({
          tenant_uuid: tenantUuid,
        });
        logger.debug("[CDR_PROCESSING] Webhook data", { webhookData });

        if (webhookData) {
          const url = String(webhookData.web_hook_url);
          let webHookData;

          if (webhookData.notify !== "0" && callState === "missed") {
            webHookData = {
              description: "incoming calls notification from Belsmart",
              caller_id_name: callerIdName,
              caller_id_number: callerIdNumber,
              did: destinationNumber,
              date: callstart,
              duration: billsecond,
              call_status: callState,
              answered_call: dialedUser,
            };
          } else {
            webHookData = {
              description: "incoming calls notification from Belsmart",
              caller_id_name: callerIdName,
              caller_id_number: callerIdNumber,
              did: destinationNumber,
              date: callstart,
              duration: billsecond,
              call_status: callState,
              answered_call: dialedUser,
            };
          }

          logger.debug("[CDR_PROCESSING] Webhook payload", { webHookData });

          try {
            const response = await axios.post(url, webHookData, {
              headers: { "Content-Type": "application/json" },
            });
            logger.debug("[CDR_PROCESSING] Webhook response", {
              status: response.status,
            });
          } catch (webhookError) {
            logger.error("[CDR_PROCESSING] Webhook failed", {
              error: webhookError.message,
              url,
            });
          }
        }
      }
    } else {
      // Standard CDR insertion
      if (autoCampaign) {
        logger.debug("[CDR_PROCESSING] Auto Campaign call");
        if (variablesValue.sip_h_X_Autocalltype !== "true") {
          userUuid = "";
          logger.debug("[CDR_PROCESSING] Remove user_uuid");
        }
      }

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
      };

      logger.debug("[CDR_PROCESSING] First harsh CDR Insert", {
        insertCdrJson,
      });
      console.log("inserting data in cdrs ........");
      await collections.cdrs.insertOne(insertCdrJson);
    }

    // Additional CDR for IVR/CallQueue timeout
    if (
      ["IVR_TIMEOUT", "CALLQUEUE_TIMEOUT"].includes(disposition) ||
      ivrMenuStatus
    ) {
      const bridgeUuidIncoming = bridgeUuid + "incoming";
      direction = "inbound";

      const insertCdrJson = {
        uuid: bridgeUuidIncoming,
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
      };

      logger.debug(
        "[CDR_PROCESSING] Second harsh CDR FOR INBOUND FAIL OR DIRECT OUTBOUND Insert",
        {
          insertCdrJson,
        }
      );
      await collections.cdrs.insertOne(insertCdrJson);
    }

    // Recording insertion
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
      logger.debug("[CDR_PROCESSING] Recording Insert", {
        insertRecordingJson,
      });
    }

    // CDR Summary
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

    // Receiver extension handling (local calls)
    if (receiverExtensionUuid) {
      direction = "inbound";
      disconnectedBy = "";

      if (hangupPartyDisposition === "send_bye") {
        disconnectedBy = direction === "inbound" ? "0" : "1";
      } else if (["recv_bye", "recv_cancel"].includes(hangupPartyDisposition)) {
        disconnectedBy = direction === "inbound" ? "1" : "0";
      } else if (hangupPartyDisposition === "send_refuse") {
        disconnectedBy = direction === "inbound" ? "0" : "1";
      }

      logger.debug("[CDR_PROCESSING] Inbound disconnected by", {
        disconnectedBy,
      });

      extensionUuid = receiverExtensionUuid;
      userUuid = variablesValue.receiver_user_uuid || "";
      const sipCallId = variablesValue.sip_call_id || customCallid;

      if (callType === "did" && userUuid && !leadUuid && callType !== "local") {
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
          callType
        );
      }

      const insertCdrJson = {
        uuid: bridgeUuid + "-local",
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
      };

      await collections.cdrs.insertOne(insertCdrJson);
      logger.debug("[CDR_PROCESSING] Third harsh Extra CDR Insert", {
        insertCdrJson,
      });

      if (billsecond && billsecond !== 0 && recordingPath) {
        const insertRecordingJson = {
          uuid: bridgeUuid + "-local",
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
        logger.debug("[CDR_PROCESSING] Extra Recording Insert", {
          insertRecordingJson,
        });
      }

      const todayDateSecond = new Date().toISOString().slice(0, 10);
      const summaryFilterSecond = {
        date: todayDateSecond,
        tenant_uuid: tenantUuid,
        user_uuid: userUuid,
        direction: direction,
        createdAt: createdAt,
      };

      const summaryUpdateSecond = {
        $inc: {
          fail: failCount,
          success: successCount,
          total: totalCount,
          billsecond: billsecond,
        },
      };

      const summaryResultSecond =
        await collections.cdrsSummary.findOneAndUpdate(
          summaryFilterSecond,
          summaryUpdateSecond,
          { upsert: true }
        );

      logger.debug("[CDR_PROCESSING] Extra Summary", {
        message: summaryResultSecond
          ? "Extra Summary exists and was updated"
          : "Extra Summary Record does not exist so updated",
        result: summaryResultSecond || summaryUpdateSecond,
      });
    }

    // PBX Feature handling (ring_group, call_queue)
    if (["ring_group", "call_queue"].includes(pbxFeature)) {
      let dialedUserPbx = variablesValue.dialed_user || "";
      let extensionUser;

      if (pbxFeature === "call_queue") {
        dialedUserPbx =
          (variablesValue.cc_agent || "").replace("@default", "") ||
          dialedUserPbx;
        const dialedUserExplode = dialedUserPbx.split(".");
        extensionUser = dialedUserExplode[0];
      } else {
        extensionUser = dialedUserPbx;
      }

      const extensionSearchQuery = {
        username: extensionUser,
        tenant_uuid: tenantUuid,
      };

      const extensionsResult = await collections.extensions.findOne(
        extensionSearchQuery
      );
      logger.debug("[CDR_PROCESSING] IN PBX FEATURE EXTRA ENTRY", {
        pbxFeature,
        extensionSearchQuery,
      });

      if (extensionsResult && extensionsResult.uuid) {
        extensionUuid = extensionsResult.uuid;
        userUuid = extensionsResult.user_uuid;
        direction = "inbound";
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
            logger.debug("[CDR_PROCESSING] Ring group call data", { callData });

            if (callData) {
              userUuid = callData.user_uuid;
              extensionUuid = callData.extension_uuid;
              logger.debug("[CDR_PROCESSING] Ring group user/extension", {
                userUuid,
                extensionUuid,
              });
            }
          } else {
            logger.debug("[CDR_PROCESSING] Last bridge else");
            const ringgroupUuid = variablesValue.ringgroup_id || "";
            const ringgroupFromDomain =
              variablesValue.ringgroup_from_domain || "";
            logger.debug("[CDR_PROCESSING] Ringgroup UUID", { ringgroupUuid });

            const ringgroupData = await collections.ringGroup.findOne({
              uuid: ringgroupUuid,
            });

            if (ringgroupData) {
              const extensionList = ringgroupData.extension_list || [];
              logger.debug("[CDR_PROCESSING] Extension list", {
                extensionList,
              });

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
                      logger.debug("[CDR_PROCESSING] Registration check", {
                        extension,
                        isRegistered,
                      });

                      if (isRegistered) {
                        userUuid = extensionUserUuid;
                        logger.debug("[CDR_PROCESSING] User UUID updated", {
                          userUuid,
                        });
                        break;
                      }
                    } catch (regError) {
                      logger.error(
                        "[CDR_PROCESSING] Registration check failed",
                        {
                          error: regError.message,
                          extension,
                          ringgroupFromDomain,
                        }
                      );
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
          logger.debug("[CDR_PROCESSING] Sticky agent", {
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
        logger.debug("[CDR_PROCESSING] Campaign uuid of inbound call", {
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
        };

        await collections.cdrs.insertOne(insertCdrJson);
        logger.debug("[CDR_PROCESSING] Four harsh Extra PBX CDR Insert", {
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

        logger.debug("[CDR_PROCESSING] Extra Summary PBX", {
          message: summaryResultPbx
            ? "Extra Summary exists and was updated"
            : "Extra Summary Record does not exist so updated",
          result: summaryResultPbx || summaryUpdatePbx,
        });
      }
    }
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
    collections = getCollections();

    // Start Express server
    const server = app.listen(config.app.port, () => {
      logger.success(`🚀 CDR Server running on port ${config.app.port}`);
      logger.info(`Environment: ${config.app.env}`);
      logger.info(`Log directory: ${config.logging.dir}`);
    });

    // Graceful shutdown
    const gracefulShutdown = async (signal) => {
      logger.info(`${signal} received, shutting down gracefully...`);

      server.close(async () => {
        logger.info("HTTP server closed");
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
