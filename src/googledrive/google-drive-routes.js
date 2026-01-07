/**
 * Google Drive Settings API Routes
 * RESTful API for managing Google Drive configurations
 */

import express from "express";
import googleDriveSettingsManager from "./google-drive-settings-manager.js";
import logger from "../logger.js";

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

export default router;