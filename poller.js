import { google } from "googleapis";
import fetch from "node-fetch";
import { Storage } from "@google-cloud/storage";

// --- Configuration ---
const SCOPES = ["https://www.googleapis.com/auth/drive.readonly"];

// Google Auth client setup:
// For cloud, it automatically uses the Cloud Function's default service account.
const auth = new google.auth.GoogleAuth({
  scopes: SCOPES,
});

// GCS Client and Bucket/File names
const storage = new Storage();
const GCS_BUCKET_NAME = process.env.ASSETS_BUCKET_NAME; // Must be set as an environment variable in Cloud Function
const LAST_CHECKED_FILE_NAME = "last_checked_time.txt";

const SHARED_FOLDER_ID = process.env.SHARED_FOLDER_ID; // Must be set as an environment variable in Cloud Function
const PROCESSOR_FUNCTION_URL = process.env.PROCESSOR_FUNCTION_URL; // The HTTP trigger URL of your deployed processor CF

// Validate critical environment variables
// For Cloud Functions, these checks are typically done at deployment or startup.
// Instead of throwing errors that might crash the function, we'll log them.
if (!SHARED_FOLDER_ID) {
  console.error("SHARED_FOLDER_ID environment variable is not set.");
}
if (!PROCESSOR_FUNCTION_URL) {
  console.error("PROCESSOR_FUNCTION_URL environment variable is not set.");
}
if (!GCS_BUCKET_NAME) {
  console.error("GCS_BUCKET_NAME environment variable is not set.");
}

/**
 * Polls Google Drive for new/modified Google Sheets, exports them as CSV,
 * and sends the data to a processor function. Persists last checked time in GCS.
 *
 * This function is designed to be triggered by a Pub/Sub message via Eventarc.
 * The Functions Framework handles the HTTP server and delivers the Pub/Sub message
 * to this exported function.
 *
 * @param {object} message Pub/Sub message payload. For Eventarc-triggered
 * Pub/Sub functions, this object contains the Pub/Sub
 * message structure (e.g., `data`, `attributes`).
 * @param {object} context Pub/Sub context (event, timestamp, eventType, resource).
 * This will be an Eventarc context object for 2nd Gen.
 */
export async function pollDrive(message, context) {
  console.log(`[${new Date().toISOString()}] Polling started...`);

  // It's good practice to ensure critical environment variables are set before proceeding.
  // Although we log warnings above, a robust function might exit early here if they are missing.
  if (!SHARED_FOLDER_ID || !PROCESSOR_FUNCTION_URL || !GCS_BUCKET_NAME) {
    console.error(
      "Skipping polling due to missing critical environment variables."
    );
    return; // Exit early if configuration is incomplete
  }

  const bucket = storage.bucket(GCS_BUCKET_NAME);
  const file = bucket.file(LAST_CHECKED_FILE_NAME);

  let lastCheckedTime;
  // --- 1. Retrieve lastCheckedTime from GCS ---
  try {
    const [contents] = await file.download();
    lastCheckedTime = contents.toString().trim();
    console.log("Retrieved lastCheckedTime from GCS:", lastCheckedTime);
  } catch (err) {
    // If the file doesn't exist (first run) or an error occurs,
    // start from a very old date (epoch) to ensure all existing files are processed.
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

  const authClient = await auth.getClient(); // authClient is a google.auth.GoogleAuth instance
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
        const token = await authClient.getAccessToken(); // This gets a standard OAuth access token

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
        // If the PROCESSOR_FUNCTION_URL is public, no OIDC token is needed.
        // It's only needed if the processor function is private (requires authentication).
        let headers = { "Content-Type": "application/json" };
        // if (IS_CLOUD_ENVIRONMENT) { // This block is removed if the target is public
        //   const targetAudience = PROCESSOR_FUNCTION_URL;
        //   const idToken = await authClient.idTokenProvider.getIdentityToken(targetAudience);
        //   headers["Authorization"] = `Bearer ${idToken}`;
        // }

        await fetch(PROCESSOR_FUNCTION_URL, {
          method: "POST",
          headers: headers, // Use the dynamically created headers (now without Auth for public target)
          body: JSON.stringify({
            fileId: file.id,
            fileName: file.name,
            csvContent: csvText,
          }),
        });
        console.log(`Successfully sent '${file.name}' to processor.`);
      }
      // --- 2. Save new lastCheckedTime to GCS after successful processing ---
      // We update the timestamp only if we successfully processed new files.
      // Or, you could update it always after the query, even if no new files were found.
      // For robust "polling for new files", update with the modifiedTime of the LATEST file
      // or the current time after successful fetch.
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

// No local execution hook as this script is now purely for cloud deployment.
