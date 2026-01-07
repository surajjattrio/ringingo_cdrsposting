/**
 * Google Drive Settings Management
 * Stores and manages Google Drive OAuth credentials in database
 * Supports tenant-specific or global configurations
 */

import { getDatabase } from "../database.js";
import logger from "../logger.js";

class GoogleDriveSettingsManager {
  constructor() {
    this.collectionName = "google_drive_settings";
  }

  /**
   * Get database collection
   */
  getCollection() {
    const db = getDatabase();
    return db.collection(this.collectionName);
  }

  /**
   * Create Google Drive settings
   */
  async createSettings(settingsData) {
    try {
      const collection = this.getCollection();

      // Validate required fields
      const requiredFields = [
        "client_id",
        "client_secret",
        "refresh_token",
        "root_folder_id",
      ];

      for (const field of requiredFields) {
        if (!settingsData[field]) {
          throw new Error(`Missing required field: ${field}`);
        }
      }

      // Check if settings already exist for this tenant/global
      const existingSettings = await collection.findOne({
        tenant_uuid: settingsData.tenant_uuid 
      });

      if (existingSettings) {
        throw new Error(
          settingsData.tenant_uuid
            ? `Settings already exist for tenant: ${settingsData.tenant_uuid}`
            : "Global settings already exist"
        );
      }

      // Store settings directly (no encryption)
      const settings = {
        tenant_uuid: settingsData.tenant_uuid || null, // null = global settings
        client_id: settingsData.client_id,
        client_secret: settingsData.client_secret,
        refresh_token: settingsData.refresh_token,
        root_folder_id: settingsData.root_folder_id,
        redirect_uri: settingsData.redirect_uri || "urn:ietf:wg:oauth:2.0:oob",
        enabled: settingsData.enabled !== undefined ? settingsData.enabled : true,
        created_at: new Date(),
        updated_at: new Date(),
        created_by: settingsData.created_by || null,
      };

      const result = await collection.insertOne(settings);

      logger.info("[GOOGLE_DRIVE_SETTINGS] Settings created", {
        tenant_uuid: settingsData.tenant_uuid || "global",
        settingsId: result.insertedId,
      });

      return {
        success: true,
        settings_id: result.insertedId,
        tenant_uuid: settingsData.tenant_uuid || null,
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Failed to create settings", {
        error: error.message,
        tenant_uuid: settingsData.tenant_uuid,
      });
      throw error;
    }
  }

  /**
   * Get Google Drive settings (decrypted)
   */
  async getSettings(tenantUuid = null) {
    try {
      const collection = this.getCollection();

      const settings = await collection.findOne({
        tenant_uuid: tenantUuid,
      });

      if (!settings) {
        return null;
      }

      // Return settings directly (no decryption needed)
      return {
        _id: settings._id,
        tenant_uuid: settings.tenant_uuid,
        client_id: settings.client_id,
        client_secret: settings.client_secret,
        refresh_token: settings.refresh_token,
        root_folder_id: settings.root_folder_id,
        redirect_uri: settings.redirect_uri,
        enabled: settings.enabled,
        created_at: settings.created_at,
        updated_at: settings.updated_at,
        created_by: settings.created_by,
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Failed to get settings", {
        error: error.message,
        tenant_uuid: tenantUuid,
      });
      throw error;
    }
  }

  /**
   * Update Google Drive settings
   */
  async updateSettings(tenantUuid = null, updateData) {
    try {
      const collection = this.getCollection();

      // Get existing settings
      const existingSettings = await collection.findOne({
        tenant_uuid: tenantUuid,
      });

      if (!existingSettings) {
        throw new Error(
          tenantUuid
            ? `Settings not found for tenant: ${tenantUuid}`
            : "Global settings not found"
        );
      }

      // Build update object - store directly without encryption
      const updateFields = {
        updated_at: new Date(),
      };

      if (updateData.client_id) {
        updateFields.client_id = updateData.client_id;
      }

      if (updateData.client_secret) {
        updateFields.client_secret = updateData.client_secret;
      }

      if (updateData.refresh_token) {
        updateFields.refresh_token = updateData.refresh_token;
      }

      if (updateData.root_folder_id) {
        updateFields.root_folder_id = updateData.root_folder_id;
      }

      if (updateData.redirect_uri) {
        updateFields.redirect_uri = updateData.redirect_uri;
      }

      if (updateData.enabled !== undefined) {
        updateFields.enabled = updateData.enabled;
      }

      const result = await collection.updateOne(
        { tenant_uuid: tenantUuid },
        { $set: updateFields }
      );

      logger.info("[GOOGLE_DRIVE_SETTINGS] Settings updated", {
        tenant_uuid: tenantUuid || "global",
        modifiedCount: result.modifiedCount,
      });

      return {
        success: true,
        modified: result.modifiedCount > 0,
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Failed to update settings", {
        error: error.message,
        tenant_uuid: tenantUuid,
      });
      throw error;
    }
  }

  /**
   * Delete Google Drive settings
   */
  async deleteSettings(tenantUuid = null) {
    try {
      const collection = this.getCollection();

      const result = await collection.deleteOne({
        tenant_uuid: tenantUuid,
      });

      logger.info("[GOOGLE_DRIVE_SETTINGS] Settings deleted", {
        tenant_uuid: tenantUuid || "global",
        deletedCount: result.deletedCount,
      });

      return {
        success: true,
        deleted: result.deletedCount > 0,
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Failed to delete settings", {
        error: error.message,
        tenant_uuid: tenantUuid,
      });
      throw error;
    }
  }

  /**
   * List all settings (masked for security)
   */
  async listSettings() {
    try {
      const collection = this.getCollection();

      const allSettings = await collection.find({}).toArray();

      // Return with masked sensitive data for security
      const maskedSettings = allSettings.map((setting) => ({
        _id: setting._id,
        tenant_uuid: setting.tenant_uuid || "global",
        client_id: this.maskString(setting.client_id, 8),
        client_secret: this.maskString(setting.client_secret, 4),
        refresh_token: this.maskString(setting.refresh_token, 8),
        root_folder_id: setting.root_folder_id,
        redirect_uri: setting.redirect_uri,
        enabled: setting.enabled,
        created_at: setting.created_at,
        updated_at: setting.updated_at,
      }));

      return maskedSettings;
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Failed to list settings", {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Test Google Drive connection
   */
  async testConnection(tenantUuid = null) {
    try {
      const settings = await this.getSettings(tenantUuid);

      if (!settings) {
        throw new Error("Settings not found");
      }

      if (!settings.enabled) {
        throw new Error("Google Drive is disabled");
      }

      // Import Google APIs dynamically
      const { google } = await import("googleapis");

      // Create OAuth2 client
      const oauth2Client = new google.auth.OAuth2(
        settings.client_id,
        settings.client_secret,
        settings.redirect_uri
      );

      oauth2Client.setCredentials({
        refresh_token: settings.refresh_token,
      });

      // Test connection by getting root folder
      const drive = google.drive({ version: "v3", auth: oauth2Client });
      const rootFolder = await drive.files.get({
        fileId: settings.root_folder_id,
        fields: "id, name, webViewLink",
      });

      logger.info("[GOOGLE_DRIVE_SETTINGS] Connection test successful", {
        tenant_uuid: tenantUuid || "global",
        root_folder: rootFolder.data.name,
      });

      return {
        success: true,
        root_folder: {
          id: rootFolder.data.id,
          name: rootFolder.data.name,
          link: rootFolder.data.webViewLink,
        },
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE_SETTINGS] Connection test failed", {
        error: error.message,
        tenant_uuid: tenantUuid,
      });
      throw error;
    }
  }

  /**
   * Helper: Mask sensitive string for display
   */
  maskString(str, visibleChars = 4) {
    if (!str || str.length <= visibleChars) {
      return "***";
    }
    return "***" + str.slice(-visibleChars);
  }
}

// Export singleton instance
const googleDriveSettingsManager = new GoogleDriveSettingsManager();
export default googleDriveSettingsManager;