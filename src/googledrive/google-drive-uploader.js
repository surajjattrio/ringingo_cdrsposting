/**
 * Google Drive Recording Uploader
 * Handles recording uploads to Google Drive with OAuth authentication
 * Organizes files in Date/Agent Name/recording.wav structure
 */

import { google } from "googleapis";
import fs from "fs";
import path from "path";
import logger from "../logger.js";
import { getDatabase } from "../database.js";
import googleDriveSettingsManager from "./google-drive-settings-manager.js";

class GoogleDriveUploader {
  constructor() {
    this.isInitialized = false;
  }

  /**
   * Get or create Google Drive client for tenant
   */
  async getDriveClient(tenantUuid = null) {
    try {
      // Get settings from database
      const settings = await googleDriveSettingsManager.getSettings(tenantUuid);

      if (!settings) {
        throw new Error(
          tenantUuid
            ? `Google Drive settings not found for tenant: ${tenantUuid}`
            : "Global Google Drive settings not found"
        );
      }

      if (!settings.enabled) {
        throw new Error(
          tenantUuid
            ? `Google Drive is disabled for tenant: ${tenantUuid}`
            : "Google Drive is disabled globally"
        );
      }

      logger.info("[GOOGLE_DRIVE] Initializing Google Drive client", {
        tenant_uuid: tenantUuid || "global",
      });

      // Create OAuth2 client
      const oauth2Client = new google.auth.OAuth2(
        settings.client_id,
        settings.client_secret,
        settings.redirect_uri
      );

      // Set credentials with refresh token
      oauth2Client.setCredentials({
        refresh_token: settings.refresh_token,
      });

      // Create Drive API client
      const drive = google.drive({ version: "v3", auth: oauth2Client });

      // Verify connection by getting root folder info
      const rootFolder = await drive.files.get({
        fileId: settings.root_folder_id,
        fields: "id, name",
      });

      logger.success("[GOOGLE_DRIVE] Client initialized successfully", {
        tenant_uuid: tenantUuid || "global",
        root_folder: rootFolder.data.name,
      });

      return {
        drive,
        oauth2Client,
        rootFolderId: settings.root_folder_id,
      };
    } catch (error) {
      logger.error("[GOOGLE_DRIVE] Initialization failed", {
        error: error.message,
        stack: error.stack,
        tenant_uuid: tenantUuid,
      });
      throw error;
    }
  }

  /**
   * Find or create folder by name in parent folder
   */
  async findOrCreateFolder(drive, folderName, parentFolderId) {
    try {
      // Search for existing folder
      const query = `name='${folderName}' and '${parentFolderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`;

      const response = await drive.files.list({
        q: query,
        fields: "files(id, name)",
        spaces: "drive",
      });

      if (response.data.files && response.data.files.length > 0) {
        const folderId = response.data.files[0].id;
        logger.debug("[GOOGLE_DRIVE] Folder found", { folderName, folderId });
        return folderId;
      }

      // Create new folder if not found
      const folderMetadata = {
        name: folderName,
        mimeType: "application/vnd.google-apps.folder",
        parents: [parentFolderId],
      };

      const folder = await drive.files.create({
        requestBody: folderMetadata,
        fields: "id",
      });

      const folderId = folder.data.id;

      logger.debug("[GOOGLE_DRIVE] Folder created", { folderName, folderId });
      return folderId;
    } catch (error) {
      logger.error("[GOOGLE_DRIVE] Folder operation failed", {
        error: error.message,
        folderName,
        parentFolderId,
      });
      throw error;
    }
  }

  /**
   * Get agent name for folder creation only
   */
  async getAgentNameForFolder(userUuid) {
    try {
      const db = getDatabase();
      const user = await db
        .collection("user")
        .findOne({ uuid: userUuid }, { projection: { username: 1 } });

      if (user) {
        return user.username || "Unknown";
      }

      return "Unknown";
    } catch (error) {
      logger.error("[GOOGLE_DRIVE] Failed to get agent name for folder", {
        error: error.message,
        userUuid,
      });
      return "Unknown";
    }
  }

  /**
   * Upload recording to Google Drive
   */
  async uploadRecording(recordingData, cdrData) {
    const uploadStartTime = Date.now();
    let attempt = 0;
    const maxRetries = 3;

    while (attempt < maxRetries) {
      try {
        attempt++;

        // Get tenant UUID from CDR data
        const tenantUuid = cdrData.tenant_uuid || null;

        logger.info("[GOOGLE_DRIVE] Starting upload", {
          attempt,
          recordingPath: recordingData.recording_path,
          cdrUuid: cdrData.uuid,
          tenant_uuid: tenantUuid || "global",
        });

        // Get drive client for tenant
        const { drive, rootFolderId } = await this.getDriveClient(tenantUuid);

        // Check if file exists
        if (!fs.existsSync(recordingData.recording_path)) {
          throw new Error(
            `Recording file not found: ${recordingData.recording_path}`
          );
        }

        // Extract date from callstart (YYYY-MM-DD format)
        const callstartDate = new Date(cdrData.createdAt || cdrData.callstart);
        const dateFolder = callstartDate.toISOString().split("T")[0]; // YYYY-MM-DD

        // Get agent name only for folder creation
        const agentNameForFolder = await this.getAgentNameForFolder(cdrData.user_uuid);

        // Create folder structure: Date/Agent Name
        const dateFolderId = await this.findOrCreateFolder(
          drive,
          dateFolder,
          rootFolderId
        );

        const agentFolderId = await this.findOrCreateFolder(
          drive,
          agentNameForFolder,
          dateFolderId
        );

        // Get file name from recording path
        const fileName = path.basename(recordingData.recording_path);

        // Upload file
        const fileMetadata = {
          name: fileName,
          parents: [agentFolderId],
        };

        const media = {
          mimeType: "audio/wav",
          body: fs.createReadStream(recordingData.recording_path),
        };

        logger.debug("[GOOGLE_DRIVE] Uploading file...", {
          fileName,
          agentFolder: agentNameForFolder,
          dateFolder,
          tenant_uuid: tenantUuid || "global",
        });

        const file = await drive.files.create({
          requestBody: fileMetadata,
          media: media,
          fields: "id, name, webViewLink, webContentLink",
        });

        // Make file publicly accessible
        await drive.permissions.create({
          fileId: file.data.id,
          requestBody: {
            role: "reader",
            type: "anyone",
          },
        });

        // Generate public URL
        const publicUrl = `https://drive.google.com/uc?export=download&id=${file.data.id}`;

        const uploadDuration = Date.now() - uploadStartTime;

        logger.success("[GOOGLE_DRIVE] Upload successful", {
          fileId: file.data.id,
          fileName: file.data.name,
          publicUrl,
          durationMs: uploadDuration,
          tenant_uuid: tenantUuid || "global",
        });

        // Log upload to database
        await this.logUpload({
          cdrUuid: cdrData.uuid,
          recordingPath: recordingData.recording_path,
          fileId: file.data.id,
          publicUrl,
          agentUuid: cdrData.user_uuid,
          dateFolder,
          uploadDuration,
          attempt,
          status: "success",
          tenantUuid,
        });

        return {
          fileId: file.data.id,
          publicUrl,
          webViewLink: file.data.webViewLink,
          fileName: file.data.name,
        };
      } catch (error) {
        logger.error("[GOOGLE_DRIVE] Upload failed", {
          error: error.message,
          attempt,
          maxRetries,
          recordingPath: recordingData.recording_path,
        });

        // Log failed attempt
        await this.logUpload({
          cdrUuid: cdrData.uuid,
          recordingPath: recordingData.recording_path,
          fileId: null,
          publicUrl: null,
          agentUuid: null,
          dateFolder: null,
          uploadDuration: Date.now() - uploadStartTime,
          attempt,
          status: "failed",
          error: error.message,
          tenantUuid: cdrData.tenant_uuid || null,
        });

        if (attempt >= maxRetries) {
          logger.error("[GOOGLE_DRIVE] Max retries reached", {
            recordingPath: recordingData.recording_path,
            attempts: attempt,
          });
          throw error;
        }

        // Exponential backoff: 2^attempt seconds
        const backoffDelay = Math.pow(2, attempt) * 1000;
        logger.info("[GOOGLE_DRIVE] Retrying after delay", {
          delayMs: backoffDelay,
          nextAttempt: attempt + 1,
        });
        await new Promise((resolve) => setTimeout(resolve, backoffDelay));
      }
    }
  }

  /**
   * Log upload to database
   */
  async logUpload(logData) {
    try {
      const db = getDatabase();
      
      // Check if document already exists
      const existingLog = await db.collection("recording_upload_logs").findOne({
        cdr_uuid: logData.cdrUuid,
      });

      if (existingLog) {
        // Document exists - just add retry attempt
        await db.collection("recording_upload_logs").updateOne(
          { cdr_uuid: logData.cdrUuid },
          {
            $push: {
              retry_attempts: {
                attempt: logData.attempt,
                status: logData.status,
                error: logData.error || null,
                duration_ms: logData.uploadDuration,
                timestamp: new Date(),
              },
            },
            $set: {
              // Update main fields only on success
              ...(logData.status === "success" && {
                file_id: logData.fileId,
                public_url: logData.publicUrl,
                agent_uuid: logData.agentUuid,
                date_folder: logData.dateFolder,
                upload_duration_ms: logData.uploadDuration,
                status: logData.status,
                uploaded_at: new Date(),
              }),
              // Always update these
              attempt: logData.attempt,
              error: logData.error || null,
            },
          }
        );

        logger.debug("[GOOGLE_DRIVE] Upload log updated with retry", {
          cdrUuid: logData.cdrUuid,
          attempt: logData.attempt,
          status: logData.status,
        });
      } else {
        // First attempt - create new document with original structure
        const logEntry = {
          cdr_uuid: logData.cdrUuid,
          recording_path: logData.recordingPath,
          file_id: logData.fileId,
          public_url: logData.publicUrl,
          agent_uuid: logData.agentUuid,
          date_folder: logData.dateFolder,
          upload_duration_ms: logData.uploadDuration,
          attempt: logData.attempt,
          status: logData.status,
          error: logData.error || null,
          tenant_uuid: logData.tenantUuid || null,
          uploaded_at: new Date(),
          retry_attempts: [
            {
              attempt: logData.attempt,
              status: logData.status,
              error: logData.error || null,
              duration_ms: logData.uploadDuration,
              timestamp: new Date(),
            },
          ],
        };

        await db.collection("recording_upload_logs").insertOne(logEntry);

        logger.debug("[GOOGLE_DRIVE] Upload log created", {
          cdrUuid: logData.cdrUuid,
          attempt: logData.attempt,
          status: logData.status,
        });
      }
    } catch (error) {
      // Don't throw error for logging failures
      logger.error("[GOOGLE_DRIVE] Failed to log upload", {
        error: error.message,
        cdrUuid: logData.cdrUuid,
      });
    }
  }

  /**
   * Update CDR with cloud recording info
   */
  async updateCDR(cdrUuid, uploadResult) {
    try {
      const db = getDatabase();

      const updateData = {
        cloud_recording_url: uploadResult.publicUrl,
        cloud_file_id: uploadResult.fileId,
        cloud_uploaded_at: new Date(),
      };

      // Update both cdrs and inbound_cdrs collections
      await db
        .collection("cdrs")
        .updateOne({ uuid: cdrUuid }, { $set: updateData });

      await db
        .collection("inbound_cdrs")
        .updateOne({ uuid: cdrUuid }, { $set: updateData });

      logger.debug("[GOOGLE_DRIVE] CDR updated with cloud info", {
        cdrUuid,
        fileId: uploadResult.fileId,
      });
    } catch (error) {
      logger.error("[GOOGLE_DRIVE] Failed to update CDR", {
        error: error.message,
        cdrUuid,
      });
      // Don't throw - upload was successful
    }
  }

  /**
   * Main method: Upload and update CDR
   */
  async processRecording(recordingData, cdrData) {
    try {
      // Upload recording
      const uploadResult = await this.uploadRecording(recordingData, cdrData);

      // Update CDR
      await this.updateCDR(cdrData.uuid, uploadResult);

      return uploadResult;
    } catch (error) {
      logger.error("[GOOGLE_DRIVE] Process recording failed", {
        error: error.message,
        cdrUuid: cdrData.uuid,
      });
      // Don't throw - let CDR processing continue
      return null;
    }
  }
}

// Export singleton instance
const googleDriveUploader = new GoogleDriveUploader();
export default googleDriveUploader;