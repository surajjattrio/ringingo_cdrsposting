/**
 * Google Drive Settings API Routes
 * RESTful API for managing Google Drive configurations
 */

import express from "express";
import googleDriveSettingsManager from "./google-drive-settings-manager.js";
import logger from "../logger.js";
import { getDatabase } from "../database.js";

const router = express.Router();
router.use("/google-drive", router);

/**
 * Middleware to validate tenant_uuid
 */
const validateTenantUuid = (req, res, next) => {
  const tenantUuid = req.body.tenant_uuid || req.query.tenant_uuid || null;
  req.tenantUuid = tenantUuid;
  next();
};

/**
 * Helper: Parse date range from query params
 */
function parseDateRange(startDate, endDate) {
  const start = startDate ? new Date(startDate) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000); // Default: last 30 days
  const end = endDate ? new Date(endDate) : new Date();
  
  // Set time to start and end of day
  start.setHours(0, 0, 0, 0);
  end.setHours(23, 59, 59, 999);
  
  return { start, end };
}

/**
 * @route   GET /api/google-drive/analytics/upload-activity
 * @desc    Get upload activity over time (for graph)
 * @query   tenant_uuid (optional) - Filter by tenant
 * @query   start_date (optional) - Start date (YYYY-MM-DD)
 * @query   end_date (optional) - End date (YYYY-MM-DD)
 * @query   interval (optional) - day|week|month (default: day)
 */
router.get("/analytics/upload-activity", async (req, res) => {
  try {
    const { tenant_uuid, start_date, end_date, user_uuid ,interval = "day" } = req.query;
    const db = getDatabase();

    const { start, end } = parseDateRange(start_date, end_date);

    // Build filter
    const filter = {
      uploaded_at: { $gte: start, $lte: end },
      status: "success"
    };

    if (tenant_uuid) {
      filter.tenant_uuid = tenant_uuid;
    }

    if (user_uuid) {
      filter.agent_uuid = user_uuid; 
    }

    // Determine grouping based on interval
    let groupBy;
    if (interval === "month") {
      groupBy = {
        year: { $year: "$uploaded_at" },
        month: { $month: "$uploaded_at" }
      };
    } else if (interval === "week") {
      groupBy = {
        year: { $year: "$uploaded_at" },
        week: { $week: "$uploaded_at" }
      };
    } else {
      groupBy = {
        year: { $year: "$uploaded_at" },
        month: { $month: "$uploaded_at" },
        day: { $dayOfMonth: "$uploaded_at" }
      };
    }

    // Aggregate by date
    const activity = await db.collection("recording_upload_logs")
      .aggregate([
        { $match: filter },
        {
          $group: {
            _id: groupBy,
            count: { $sum: 1 }
          }
        },
        { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }
      ])
      .toArray();

    // Format response
    const formattedActivity = activity.map(item => {
      let date;
      if (interval === "month") {
        date = new Date(item._id.year, item._id.month - 1, 1);
      } else if (interval === "week") {
        // Approximate week start
        const firstDay = new Date(item._id.year, 0, 1);
        date = new Date(firstDay.setDate(firstDay.getDate() + (item._id.week * 7)));
      } else {
        date = new Date(item._id.year, item._id.month - 1, item._id.day);
      }

      return {
        date: date.toISOString().split('T')[0],
        count: item.count,
        formatted_date: date.toLocaleDateString('en-US', { 
          year: 'numeric', 
          month: 'short', 
          day: interval === "day" ? 'numeric' : undefined 
        })
      };
    });

    res.json({
      success: true,
      data: {
        interval,
        activity: formattedActivity,
        total: formattedActivity.reduce((sum, item) => sum + item.count, 0),
        date_range: {
          start: start.toISOString(),
          end: end.toISOString()
        }
      }
    });

  } catch (error) {
    logger.error("[GOOGLE_DRIVE_ANALYTICS] Upload activity failed", {
      error: error.message
    });
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

/**
 * @route   POST /api/google-drive/settings
 * @desc    Create Google Drive settings
 * @access  Admin
 * @body    {
 *            tenant_uuid: string (optional, null for global),
 *            client_id: string,
 *            client_secret: string,
 *            refresh_token: string,
 *            root_folder_id: string,
 *            redirect_uri: string (optional),
 *            enabled: boolean (optional, default: true)
 *          }
 */
router.post("/settings", validateTenantUuid, async (req, res) => {
  try {
    const {
      client_id,
      client_secret,
      refresh_token,
      root_folder_id,
      redirect_uri,
      enabled,
      tenant_uuid,
    } = req.body;

    // Validate required fields
    if (!client_id || !client_secret || !refresh_token || !root_folder_id) {
      return res.status(400).json({
        success: false,
        message:
          "Missing required fields: client_id, client_secret, refresh_token, root_folder_id",
      });
    }

    const result = await googleDriveSettingsManager.createSettings({
      tenant_uuid: tenant_uuid || null,
      client_id,
      client_secret,
      refresh_token,
      root_folder_id,
      redirect_uri: redirect_uri || "urn:ietf:wg:oauth:2.0:oob",
      enabled: enabled !== undefined ? enabled : true,
      created_by: req.user?.uuid || null, // If you have auth middleware
    });

    logger.info("[API] Google Drive settings created", {
      tenant_uuid: tenant_uuid || "global",
      settings_id: result.settings_id,
    });

    res.status(201).json({
      success: true,
      message: "Google Drive settings created successfully",
      data: result,
    });
  } catch (error) {
    logger.error("[API] Failed to create Google Drive settings", {
      error: error.message,
    });

    res.status(error.message.includes("already exist") ? 409 : 500).json({
      success: false,
      message: error.message,
    });
  }
});

/**
 * @route   GET /api/google-drive/settings
 * @desc    Get Google Drive settings (sensitive data masked)
 * @access  Admin
 * @query   tenant_uuid (optional)
 */
router.get("/settings", validateTenantUuid, async (req, res) => {
  try {
    const tenantUuid = req.query.tenant_uuid || null;

    const settings = await googleDriveSettingsManager.getSettings(tenantUuid);

    if (!settings) {
      return res.status(404).json({
        success: false,
        message: tenantUuid
          ? `Settings not found for tenant: ${tenantUuid}`
          : "Global settings not found",
      });
    }

    // Mask sensitive data for API response
    const maskedSettings = {
      _id: settings._id,
      tenant_uuid: settings.tenant_uuid,
      client_id: googleDriveSettingsManager.maskString(settings.client_id, 8),
      client_secret: googleDriveSettingsManager.maskString(settings.client_secret, 4),
      refresh_token: googleDriveSettingsManager.maskString(settings.refresh_token, 8),
      root_folder_id: settings.root_folder_id,
      redirect_uri: settings.redirect_uri,
      enabled: settings.enabled,
      created_at: settings.created_at,
      updated_at: settings.updated_at,
    };

    res.json({
      success: true,
      data: maskedSettings,
    });
  } catch (error) {
    logger.error("[API] Failed to get Google Drive settings", {
      error: error.message,
    });

    res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

/**
 * @route   GET /api/google-drive/settings/list
 * @desc    List all Google Drive settings (all tenants)
 * @access  Super Admin
 */
router.get("/settings/list", async (req, res) => {
  try {
    const allSettings = await googleDriveSettingsManager.listSettings();

    res.json({
      success: true,
      count: allSettings.length,
      data: allSettings,
    });
  } catch (error) {
    logger.error("[API] Failed to list Google Drive settings", {
      error: error.message,
    });

    res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

/**
 * @route   PUT /api/google-drive/settings
 * @desc    Update Google Drive settings
 * @access  Admin
 * @body    {
 *            tenant_uuid: string (optional),
 *            client_id: string (optional),
 *            client_secret: string (optional),
 *            refresh_token: string (optional),
 *            root_folder_id: string (optional),
 *            redirect_uri: string (optional),
 *            enabled: boolean (optional)
 *          }
 */
router.put("/settings", validateTenantUuid, async (req, res) => {
  try {
    const { tenant_uuid, ...updateData } = req.body;

    const result = await googleDriveSettingsManager.updateSettings(
      tenant_uuid || null,
      updateData
    );

    logger.info("[API] Google Drive settings updated", {
      tenant_uuid: tenant_uuid || "global",
      modified: result.modified,
    });

    res.json({
      success: true,
      message: result.modified
        ? "Settings updated successfully"
        : "No changes made",
      data: result,
    });
  } catch (error) {
    logger.error("[API] Failed to update Google Drive settings", {
      error: error.message,
    });

    res.status(error.message.includes("not found") ? 404 : 500).json({
      success: false,
      message: error.message,
    });
  }
});

/**
 * @route   DELETE /api/google-drive/settings
 * @desc    Delete Google Drive settings
 * @access  Admin
 * @query   tenant_uuid (optional)
 */
router.delete("/settings", validateTenantUuid, async (req, res) => {
  try {
    const tenantUuid = req.query.tenant_uuid

    if(!tenantUuid) {
      return res.status(400).json({
        success: false,
        message: "Missing required field: tenant_uuid",
      });
    }

    const result = await googleDriveSettingsManager.deleteSettings(tenantUuid);

    logger.info("[API] Google Drive settings deleted", {
      tenant_uuid: tenantUuid,
      deleted: result.deleted,
    });

    res.json({
      success: true,
      message: result.deleted
        ? "Settings deleted successfully"
        : "Settings not found",
      data: result,
    });
  } catch (error) {
    logger.error("[API] Failed to delete Google Drive settings", {
      error: error.message,
    });

    res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

/**
 * @route   POST /api/google-drive/test-connection
 * @desc    Test Google Drive connection
 * @access  Admin
 * @body    { tenant_uuid: string (optional) }
 */
router.post("/test-connection", validateTenantUuid, async (req, res) => {
  try {
    const { tenant_uuid } = req.body;

    const result = await googleDriveSettingsManager.testConnection(
      tenant_uuid || null
    );

    logger.info("[API] Google Drive connection test successful", {
      tenant_uuid: tenant_uuid || "global",
    });

    res.json({
      success: true,
      message: "Connection successful",
      data: result,
    });
  } catch (error) {
    logger.error("[API] Google Drive connection test failed", {
      error: error.message,
    });

    res.status(500).json({
      success: false,
      message: `Connection failed: ${error.message}`,
    });
  }
});

/**
 * @route   PATCH /api/google-drive/toggle
 * @desc    Enable/disable Google Drive integration
 * @access  Admin
 * @body    { tenant_uuid: string (optional), enabled: boolean }
 */
router.patch("/toggle", validateTenantUuid, async (req, res) => {
  try {
    const { tenant_uuid, enabled } = req.body;

    if (enabled === undefined) {
      return res.status(400).json({
        success: false,
        message: "Missing required field: enabled",
      });
    }

    const result = await googleDriveSettingsManager.updateSettings(
      tenant_uuid,
      { enabled }
    );

    logger.info("[API] Google Drive integration toggled", {
      tenant_uuid: tenant_uuid || "global",
      enabled,
    });

    res.json({
      success: true,
      message: `Google Drive ${enabled ? "enabled" : "disabled"} successfully`,
      data: result,
    });
  } catch (error) {
    logger.error("[API] Failed to toggle Google Drive", {
      error: error.message,
    });

    res.status(error.message.includes("not found") ? 404 : 500).json({
      success: false,
      message: error.message,
    });
  }
});



/**
 * @route   GET /api/google-drive/recordings
 * @desc    Get recording logs for table display
 * @query   page (optional) - Page number (default: 1)
 * @query   limit (optional) - Records per page (default: 20)
 * @query   tenant_uuid (optional) - Filter by tenant
 * @query   agent_name (optional) - Filter by agent name
 * @query   status (optional) - Filter by status: success|failed
 * @query   start_date (optional) - Start date (YYYY-MM-DD)
 * @query   end_date (optional) - End date (YYYY-MM-DD)
 * @query   sort_by (optional) - Sort field: uploaded_at|agent_name|upload_duration_ms|billsecond (default: uploaded_at)
 * @query   sort_order (optional) - Sort order: asc|desc (default: desc)
 * @query   search (optional) - Search in agent_name, cdr_uuid, recording_path
 */
router.get("/recordings", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 20,
      tenant_uuid,
      agent_uuid,
      status,
      start_date,
      end_date,
      user_uuid,
      sort_by = "uploaded_at",
      sort_order = "desc",
      search
    } = req.query;

    const db = getDatabase();
    
    // Build filter
    const filter = {};

    // Date range filter
    if (start_date || end_date) {
      filter.uploaded_at = {};
      if (start_date) {
        const start = new Date(start_date);
        start.setHours(0, 0, 0, 0);
        filter.uploaded_at.$gte = start;
      }
      if (end_date) {
        const end = new Date(end_date);
        end.setHours(23, 59, 59, 999);
        filter.uploaded_at.$lte = end;
      }
    }

    // Tenant filter
    if (tenant_uuid) {
      filter.tenant_uuid = tenant_uuid;
    }

    // Agent filter by UUID
    if (agent_uuid || user_uuid) {
      filter.agent_uuid = agent_uuid || user_uuid;
    }

    // Status filter
    if (status) {
      filter.status = status;
    }

    // Search filter
    if (search) {
      filter.$or = [
        { agent_uuid: { $regex: search, $options: "i" } },
        { cdr_uuid: { $regex: search, $options: "i" } },
        { recording_path: { $regex: search, $options: "i" } }
      ];
    }

    // Pagination
    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    // Sort
    const sortField = sort_by;
    const sortDirection = sort_order === "asc" ? 1 : -1;
    const sort = { [sortField]: sortDirection };

    // Get total count
    const totalRecords = await db.collection("recording_upload_logs")
      .countDocuments(filter);

    // Get paginated data
    const recordings = await db.collection("recording_upload_logs")
      .find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limitNum)
      .toArray();

    // Get all unique agent UUIDs to fetch user details in batch
    const agentUuids = [...new Set(recordings.map(r => r.agent_uuid).filter(Boolean))];
    
    // Fetch all users at once for better performance
    const users = await db.collection("user")
      .find({ uuid: { $in: agentUuids } })
      .project({ uuid: 1, username: 1, first_name: 1, last_name: 1 })
      .toArray();
    
    // Create a map for quick lookup
    const userMap = new Map();
    users.forEach(user => {
      userMap.set(user.uuid, {
        username: user.username || `${user.first_name || ''} ${user.last_name || ''}`.trim() || 'Unknown'
      });
    });

    // Enrich data with additional info from recordings collection
    const enrichedRecordings = await Promise.all(
      recordings.map(async (log) => {
        // Get recording details
        const recording = await db.collection("recording")
          .findOne({ uuid: log.cdr_uuid });

        // Get CDR details for more info
        const cdr = await db.collection("cdrs")
          .findOne({ uuid: log.cdr_uuid });

        // Get agent name from the map
        const agentInfo = userMap.get(log.agent_uuid) || { username: 'Unknown' };

        return {
          agent_uuid: log.agent_uuid || null,
          agent_name: agentInfo.username, // Include for display purposes only
          public_url: log.public_url,
          attempt: log.attempt,
          status: log.status,
          error: log.error || null,
          tenant_uuid: log.tenant_uuid || null,
          call_duration_formatted: recording 
            ? formatDuration(recording.billsecond) 
            : "0s",
          uploaded_at: log.uploaded_at,
        };
      })
    );

    // Calculate pagination info
    const totalPages = Math.ceil(totalRecords / limitNum);
    const hasNextPage = pageNum < totalPages;
    const hasPrevPage = pageNum > 1;

    // Response
    res.json({
      success: true,
      data: {
        recordings: enrichedRecordings,
        pagination: {
          current_page: pageNum,
          total_pages: totalPages,
          total_records: totalRecords,
          records_per_page: limitNum,
          has_next_page: hasNextPage,
          has_prev_page: hasPrevPage,
          next_page: hasNextPage ? pageNum + 1 : null,
          prev_page: hasPrevPage ? pageNum - 1 : null
        },
        filters: {
          tenant_uuid: tenant_uuid || null,
          agent_uuid: agent_uuid || null,
          status: status || null,
          start_date: start_date || null,
          end_date: end_date || null,
          search: search || null
        },
        sort: {
          sort_by: sortField,
          sort_order: sort_order
        }
      }
    });

  } catch (error) {
    logger.error("[GOOGLE_DRIVE_RECORDINGS] Fetch failed", {
      error: error.message
    });
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});
/**
 * @route   GET /api/google-drive/recordings/stats/summary
 * @desc    Get summary statistics for recordings table
 * @query   tenant_uuid (optional) - Filter by tenant
 * @query   start_date (optional) - Start date (YYYY-MM-DD)
 * @query   end_date (optional) - End date (YYYY-MM-DD)
 */
router.get("/recordings/stats/summary", async (req, res) => {
  try {
    const { tenant_uuid, start_date, end_date } = req.query;
    const db = getDatabase();

    // Build filter
    const filter = {};

    if (start_date || end_date) {
      filter.uploaded_at = {};
      if (start_date) {
        const start = new Date(start_date);
        start.setHours(0, 0, 0, 0);
        filter.uploaded_at.$gte = start;
      }
      if (end_date) {
        const end = new Date(end_date);
        end.setHours(23, 59, 59, 999);
        filter.uploaded_at.$lte = end;
      }
    }

    if (tenant_uuid) {
      filter.tenant_uuid = tenant_uuid;
    }

    // Get counts
    const totalRecordings = await db.collection("recording_upload_logs")
      .countDocuments(filter);

    const successfulUploads = await db.collection("recording_upload_logs")
      .countDocuments({ ...filter, status: "success" });

    const failedUploads = await db.collection("recording_upload_logs")
      .countDocuments({ ...filter, status: "failed" });

    // Get average upload time
    const avgUploadTime = await db.collection("recording_upload_logs")
      .aggregate([
        { $match: { ...filter, status: "success" } },
        {
          $group: {
            _id: null,
            avg_duration: { $avg: "$upload_duration_ms" }
          }
        }
      ])
      .toArray();

    const avgDurationMs = avgUploadTime.length > 0 ? avgUploadTime[0].avg_duration : 0;

    res.json({
      success: true,
      data: {
        total_recordings: totalRecordings,
        successful_uploads: successfulUploads,
        failed_uploads: failedUploads,
        success_rate: totalRecordings > 0 
          ? ((successfulUploads / totalRecordings) * 100).toFixed(1)
          : 0,
        avg_upload_time_sec: (avgDurationMs / 1000).toFixed(2),
      }
    });

  } catch (error) {
    logger.error("[GOOGLE_DRIVE_RECORDINGS] Stats summary failed", {
      error: error.message
    });
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

/**
 * Helper function to format duration
 */
function formatDuration(seconds) {
  if (!seconds || seconds === 0) return "0s";
  
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  
  const parts = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
  
  return parts.join(" ");
}
export default router;