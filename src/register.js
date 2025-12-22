/**
 * FreeSWITCH Registration Checker
 * Checks if a user is registered on FreeSWITCH via ESL
 * Usage: node src/register.js <username> <realm>
 *
  */

import esl from "modesl";
import config from "./config.js";
import logger from "./logger.js";
import { fileURLToPath } from "url";

/**
 * Check if a user is registered
 * @param {string} targetUser - Username to check
 * @param {string} targetRealm - Realm/domain to check
 * @returns {Promise<boolean>} - True if registered, false otherwise
 */
async function checkRegistration(targetUser, targetRealm) {
  return new Promise((resolve, reject) => {
    // FIXED: Accept empty parameters like Python does
    // Python behavior: empty params result in "Not_Registered", not an error
    if (!targetUser || !targetRealm) {
      logger.debug(
        "[REGISTRATION_CHECK] Empty parameters provided, returning false",
        {
          targetUser,
          targetRealm,
        }
      );
      resolve(false); // Return false instead of error
      return;
    }

    // Create ESL connection
    const conn = new esl.Connection(
      config.esl.host,
      config.esl.port,
      config.esl.password,
      function () {
        logger.debug("[REGISTRATION_CHECK] ESL connection established", {
          host: config.esl.host,
          port: config.esl.port,
        });

        // Send API command to get registrations
        conn.api("show registrations as json", function (res) {
          const body = res.getBody();
          let isRegistered = false;

          try {
            // Parse JSON response
            const parsedData = JSON.parse(body);
            const rows = parsedData.rows || [];

            logger.debug("[REGISTRATION_CHECK] Retrieved registrations", {
              totalRegistrations: rows.length,
              searchUser: targetUser,
              searchRealm: targetRealm,
            });

            // Check if user is registered
            // FIXED: Using strict equality (===) instead of loose (==)
            for (const row of rows) {
              if (row.reg_user === targetUser && row.realm === targetRealm) {
                isRegistered = true;
                logger.debug("[REGISTRATION_CHECK] User found registered", {
                  user: targetUser,
                  realm: targetRealm,
                  contact: row.contact || "N/A",
                });
                break;
              }
            }

            if (!isRegistered) {
              logger.debug(
                "[REGISTRATION_CHECK] User not found in registrations",
                {
                  user: targetUser,
                  realm: targetRealm,
                }
              );
            }
          } catch (error) {
            logger.error("[REGISTRATION_CHECK] JSON parsing failed", {
              error: error.message,
              responseBody: body.substring(0, 200), // Log first 200 chars
            });
            // Continue with isRegistered = false (same as Python)
          }

          // Disconnect and resolve
          conn.disconnect();
          resolve(isRegistered);
        });
      }
    );

    // Handle connection errors
    conn.on("error", function (err) {
      logger.error("[REGISTRATION_CHECK] ESL connection failed", {
        host: config.esl.host,
        port: config.esl.port,
        error: err.message,
        targetUser,
        targetRealm,
      });
      reject(err);
    });
  });
}

/**
 * Main execution function
 */
async function main() {
  try {
    // Get command line arguments (matches Python: process.argv[2] = sys.argv[1])
    const targetUser = process.argv[2] || "";
    const targetRealm = process.argv[3] || "";

    logger.debug("[REGISTRATION_CHECK] Starting registration check", {
      user: targetUser,
      realm: targetRealm,
    });

    // Check registration
    const isRegistered = await checkRegistration(targetUser, targetRealm);

    // Output result (matches Python exactly)
    if (isRegistered) {
      console.log("Registered");
      logger.success(`User ${targetUser}@${targetRealm} is Registered`);
    } else {
      console.log("Not_Registered");
      logger.info(`User ${targetUser}@${targetRealm} is Not_Registered`);
    }

    // FIXED: Always exit with 0 for "not found" (matches Python behavior)
    process.exit(0);
  } catch (error) {
    // FIXED: Exit with 0 for "not found", 1 only for fatal errors
    // This matches Python which doesn't have explicit error handling
    logger.error("[REGISTRATION_CHECK] Fatal error", {
      error: error.message,
      stack: error.stack,
    });

    // Print Not_Registered even on error (matches Python behavior)
    console.log("Not_Registered");

    // Exit with 0 to match Python behavior (Python exits 0 even on connection failure)
    // Only exit 1 if you want to signal fatal errors differently from "not found"
    process.exit(0);
  }
}

// Export for use as module
export { checkRegistration };

// Run if called directly (FIXED: Better cross-platform detection)
const __filename = fileURLToPath(import.meta.url);
if (
  process.argv[1] === __filename ||
  import.meta.url === `file://${process.argv[1]}`
) {
  main();
}