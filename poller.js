import { google } from "googleapis";
import "dotenv/config"; // Keep for local development
import fetch from "node-fetch";
import { Storage } from "@google-cloud/storage"; // Import GCS library

// --- Configuration ---
const SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]; // Yes, this scope is sufficient for listing and exporting Google Sheets as CSV.

// Determine if running locally or in a Google Cloud environment
// Cloud Functions set FUNCTION_NAME, Cloud Run sets K_SERVICE.
const IS_CLOUD_ENVIRONMENT = process.env.FUNCTION_NAME;
const isLocal = !IS_CLOUD_ENVIRONMENT;

const POLL_INTERVAL_SECONDS = 30; // Only used for local setInterval
const auth = new google.auth.GoogleAuth({
  scopes: SCOPES,
  ...(isLocal && { keyFile: process.env.CREDENTIALS_PATH }),
});

const storage = new Storage();
const GCS_BUCKET_NAME = process.env.ASSETS_BUCKET_NAME || "tkd_assets"; // Use env var for cloud, fallback to literal for clarity
const LAST_CHECKED_FILE_NAME = "last_checked_time.txt";

const SHARED_FOLDER_ID = process.env.SHARED_FOLDER_ID; // Must be set as an environment variable in Cloud Function
const PROCESSOR_FUNCTION_URL = isLocal
  ? process.env.LOCAL_URL // For local testing against a local processor
  : process.env.PROCESSOR_FUNCTION_URL; // The HTTP trigger URL of your deployed processor CF

// Validate critical environment variables
if (!SHARED_FOLDER_ID) {
  throw new Error("SHARED_FOLDER_ID environment variable is not set.");
}
if (!PROCESSOR_FUNCTION_URL) {
  throw new Error("PROCESSOR_FUNCTION_URL environment variable is not set.");
}
if (!GCS_BUCKET_NAME) {
  throw new Error("GCS_BUCKET_NAME environment variable is not set.");
}

/**
 * Polls Google Drive for new/modified Google Sheets, exports them as CSV,
 * and sends the data to a processor function. Persists last checked time in GCS.
 *
 * @param {object} [message] Pub/Sub message (only for Cloud Function trigger)
 * @param {object} [context] Pub/Sub context (only for Cloud Function trigger)
 */
export async function TKDPollDrive(message, context) {
  console.log(`[${new Date().toISOString()}] Polling started...`);

  const bucket = storage.bucket(GCS_BUCKET_NAME);
  const file = bucket.file(LAST_CHECKED_FILE_NAME);

  let lastCheckedTime;
  // --- 1. Retrieve lastCheckedTime from GCS ---
  try {
    const [contents] = await file.download();
    lastCheckedTime = contents.toString().trim();
    console.log("Retrieved lastCheckedTime from GCS:", lastCheckedTime);
  } catch (err) {
    if (err.code === 404) {
      // GCS returns 404 for non-existent files
      console.warn(
        `'${LAST_CHECKED_FILE_NAME}' not found in bucket '${GCS_BUCKET_NAME}'. Initializing from epoch.`
      );
    } else {
      console.error(
        `Error retrieving '${LAST_CHECKED_FILE_NAME}' from GCS:`,
        err.message
      );
    }
    lastCheckedTime = new Date(0).toISOString(); // Epoch time (January 1, 1970 UTC)
  }

  const authClient = await auth.getClient();
  const drive = google.drive({ version: "v3", auth: authClient });

  const baseQuery = `"${SHARED_FOLDER_ID}" in parents and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false`;
  // Use lastCheckedTime retrieved from GCS for incremental polling
  const timeQuery = ` and modifiedTime > '${lastCheckedTime}'`;
  const query = baseQuery + timeQuery;

  try {
    const folderInfo = await drive.files.get({
      fileId: SHARED_FOLDER_ID,
      fields: "id, name, mimeType",
      supportsAllDrives: true,
    });

    console.log(
      `Polling folder: "${folderInfo.data.name}" (ID: ${folderInfo.data.id})`
    );

    const res = await drive.files.list({
      q: query,
      fields: "files(id, name, modifiedTime)",
      corpora: "user",
      includeItemsFromAllDrives: true,
      supportsAllDrives: true,
    });

    console.log("API Response (res.data.files):", res.data.files);

    const newFiles = res.data.files;
    if (newFiles && newFiles.length > 0) {
      console.log(`Found ${newFiles.length} new or modified Google Sheet(s).`);

      for (const file of newFiles) {
        console.log(`Exporting & triggering processor for: ${file.name}`);

        const exportUrl = `https://www.googleapis.com/drive/v3/files/${file.id}/export?mimeType=text/csv`;
        const token = await authClient.getAccessToken();

        const csvRes = await fetch(exportUrl, {
          headers: {
            Authorization: `Bearer ${token.token}`,
          },
        });

        if (!csvRes.ok) {
          console.error(
            `Failed to export file ${file.name}: ${csvRes.statusText} (Status: ${csvRes.status})`
          );
          continue; // Skip to the next file
        }

        const csvText = await csvRes.text();

        // --- Authentication for Processor Function (if not public) ---
        let headers = { "Content-Type": "application/json" };
        if (IS_CLOUD_ENVIRONMENT) {
          // If in cloud, generate OIDC token for secure invocation of Processor CF
          const targetAudience = PROCESSOR_FUNCTION_URL;
          // Use the Cloud Function's own service account to get the token
          const oAuthClient = new google.auth.GoogleAuth({
            scopes: ["https://www.googleapis.com/auth/cloud-platform"],
            // This scope grants broad access needed for token generation;
            // narrower scopes like 'https://www.googleapis.com/auth/userinfo.email' might also work.
          });
          const client = await oAuthClient.getClient();
          const idToken = await client.idToken.getToken(targetAudience);
          headers["Authorization"] = `Bearer ${idToken}`;
        }
        // else: For local, assuming LOCAL_URL is typically public or uses other local auth mechanism.

        await fetch(PROCESSOR_FUNCTION_URL, {
          method: "POST",
          headers: headers, // Use the dynamically created headers
          body: JSON.stringify({
            fileId: file.id,
            fileName: file.name,
            csvContent: csvText,
          }),
        });
        console.log(`Successfully sent '${file.name}' to processor.`);
      }

      const newLastCheckedTime = new Date().toISOString(); // Using current time for simplicity
      await file.save(newLastCheckedTime); // Overwrites the file
      console.log("Updated lastCheckedTime in GCS to:", newLastCheckedTime);
    } else {
      console.log("No new Google Sheets found.");
    }
  } catch (err) {
    console.error("Polling error:", err);
    // Re-throw the error to indicate function failure for Cloud Functions
    throw err;
  }
}

if (isLocal) {
  console.log("Running in local mode. Starting polling interval.");
  setInterval(TKDPollDrive, POLL_INTERVAL_SECONDS * 1000);
}
