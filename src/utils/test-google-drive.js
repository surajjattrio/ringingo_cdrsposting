import { google } from 'googleapis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ANSI colors
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

// Test configuration
const testConfig = {
  clientId: process.env.GOOGLE_CLIENT_ID,
  clientSecret: process.env.GOOGLE_CLIENT_SECRET,
  refreshToken: process.env.GOOGLE_REFRESH_TOKEN,
  rootFolderId: process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID,
  redirectUri: process.env.GOOGLE_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob',
};

/**
 * Helper: Generate a dummy WAV file buffer with a valid header
 */
function createDummyWavBuffer() {
  const header = Buffer.alloc(44);

  // RIFF chunk descriptor
  header.write('RIFF', 0);
  header.writeUInt32LE(36, 4); // ChunkSize
  header.write('WAVE', 8);

  // fmt sub-chunk
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16); // Subchunk1Size (16 for PCM)
  header.writeUInt16LE(1, 20);   // AudioFormat (1 for PCM)
  header.writeUInt16LE(1, 22);   // NumChannels (1 for Mono)
  header.writeUInt32LE(8000, 24); // SampleRate (8000 Hz)
  header.writeUInt32LE(8000 * 2, 28); // ByteRate
  header.writeUInt16LE(2, 32);   // BlockAlign
  header.writeUInt16LE(16, 34);  // BitsPerSample

  // data sub-chunk
  header.write('data', 36);
  header.writeUInt32LE(0, 40);   // Subchunk2Size (0 bytes of data)

  return header;
}

/**
 * Test 1: Validate environment variables
 */
function testEnvironmentVariables() {
  log('\n📋 Test 1: Validating Environment Variables', 'cyan');
  log('='.repeat(60), 'cyan');

  const requiredVars = [
    'GOOGLE_CLIENT_ID',
    'GOOGLE_CLIENT_SECRET',
    'GOOGLE_REFRESH_TOKEN',
    'GOOGLE_DRIVE_ROOT_FOLDER_ID',
  ];

  let allPresent = true;
  for (const varName of requiredVars) {
    const value = process.env[varName];
    if (value) {
      log(`✅ ${varName}: ${value.substring(0, 20)}...`, 'green');
    } else {
      log(`❌ ${varName}: NOT SET`, 'red');
      allPresent = false;
    }
  }

  if (allPresent) {
    log('\n✅ All environment variables present\n', 'green');
    return true;
  } else {
    log('\n❌ Missing required environment variables\n', 'red');
    return false;
  }
}

/**
 * Test 2: Validate OAuth credentials
 */
async function testOAuthCredentials() {
  log('\n🔐 Test 2: Validating OAuth Credentials', 'cyan');
  log('='.repeat(60), 'cyan');

  try {
    const oauth2Client = new google.auth.OAuth2(
      testConfig.clientId,
      testConfig.clientSecret,
      testConfig.redirectUri
    );

    oauth2Client.setCredentials({
      refresh_token: testConfig.refreshToken,
    });

    const { credentials } = await oauth2Client.refreshAccessToken();
    
    log('✅ OAuth2 client created', 'green');
    log('✅ Access token obtained', 'green');
    log(`   Token expires: ${new Date(credentials.expiry_date).toISOString()}`, 'blue');

    return { oauth2Client, success: true };
  } catch (error) {
    log('❌ OAuth credentials validation failed', 'red');
    log(`   Error: ${error.message}`, 'red');
    return { oauth2Client: null, success: false };
  }
}

/**
 * Test 3: Test Google Drive API access
 */
async function testDriveAPIAccess(oauth2Client) {
  log('\n☁️  Test 3: Testing Google Drive API Access', 'cyan');
  log('='.repeat(60), 'cyan');

  try {
    const drive = google.drive({ version: 'v3', auth: oauth2Client });
    const about = await drive.about.get({ fields: 'user, storageQuota' });
    
    log('✅ Successfully connected to Google Drive', 'green');
    log(`   User: ${about.data.user.emailAddress}`, 'blue');
    
    return { drive, success: true };
  } catch (error) {
    log('❌ Google Drive API access failed', 'red');
    log(`   Error: ${error.message}`, 'red');
    return { drive: null, success: false };
  }
}

/**
 * Test 4: Validate root folder
 */
async function testRootFolder(drive) {
  log('\n📁 Test 4: Validating Root Folder', 'cyan');
  log('='.repeat(60), 'cyan');

  try {
    const rootFolder = await drive.files.get({
      fileId: testConfig.rootFolderId,
      fields: 'id, name, webViewLink',
    });

    log('✅ Root folder found', 'green');
    log(`   Folder ID: ${rootFolder.data.id}`, 'blue');
    log(`   Folder Link: ${rootFolder.data.webViewLink}`, 'blue');
    return true;
  } catch (error) {
    log('❌ Root folder validation failed', 'red');
    log(`   Error: ${error.message}`, 'red');
    return false;
  }
}

/**
 * Test 5: Upload valid WAV file
 */
async function testFileUpload(drive) {
  log('\n📤 Test 5: Testing WAV File Upload', 'cyan');
  log('='.repeat(60), 'cyan');

  try {
    // 1. Create a valid dummy WAV file locally
    const testFileName = `test_audio_${Date.now()}.wav`;
    const testFilePath = path.join(__dirname, testFileName);
    
    fs.writeFileSync(testFilePath, createDummyWavBuffer());
    log(`   Local WAV file created: ${testFileName}`, 'blue');

    // 2. Upload to Google Drive
    const fileMetadata = {
      name: testFileName,
      parents: [testConfig.rootFolderId],
    };

    const media = {
      mimeType: 'audio/wav',
      body: fs.createReadStream(testFilePath),
    };

    const file = await drive.files.create({
      requestBody: fileMetadata,
      media: media,
      fields: 'id, name, webViewLink, webContentLink',
    });

    log('✅ WAV file uploaded successfully', 'green');
    log(`   File Name: ${file.data.name}`, 'blue');
    log(`   File ID: ${file.data.id}`, 'blue');
    
    // Clean up local file
    fs.unlinkSync(testFilePath);

    return file.data.id;
  } catch (error) {
    log('❌ File upload failed', 'red');
    log(`   Error: ${error.message}`, 'red');
    return null;
  }
}

/**
 * Test 6: Make file public and Verify
 */
async function testPublicAccess(drive, fileId) {
  log('\n🌐 Test 6: Testing Public Access', 'cyan');
  log('='.repeat(60), 'cyan');

  try {
    await drive.permissions.create({
      fileId: fileId,
      requestBody: { role: 'reader', type: 'anyone' },
    });

    const publicUrl = `https://drive.google.com/uc?export=download&id=${fileId}`;

    log('✅ File made publicly accessible', 'green');
    log(`   Public Download URL: ${publicUrl}`, 'blue');
    log('   (Paste this URL in browser to confirm it downloads a .wav file)', 'yellow');

    return publicUrl;
  } catch (error) {
    log('❌ Failed to make file public', 'red');
    log(`   Error: ${error.message}`, 'red');
    return null;
  }
}

/**
 * Test 7: Cleanup (Optional)
 */
async function testCleanup(drive, fileId) {
  log('\n🧹 Test 7: Cleanup', 'cyan');
  log('='.repeat(60), 'cyan');

  // NOTE: Cleanup is intentionally disabled for this run so you can check the file manually
  // If you want to delete it automatically, uncomment the code below.
  
  log('ℹ️  Skipping auto-deletion so you can test the link.', 'yellow');
  log('   You should manually delete the file from Google Drive after verification.', 'yellow');

  /*
  try {
    if (fileId) {
      await drive.files.delete({ fileId });
      log('✅ Test file deleted', 'green');
    }
  } catch (error) {
    log('⚠️  Cleanup failed', 'yellow');
  }
  */
}

/**
 * Main Runner
 */
async function runTests() {
  log('\n' + '='.repeat(60), 'cyan');
  log('   GOOGLE DRIVE WAV UPLOAD TEST', 'cyan');
  log('='.repeat(60), 'cyan');

  if (!testEnvironmentVariables()) process.exit(1);

  const { oauth2Client, success: oauthSuccess } = await testOAuthCredentials();
  if (!oauthSuccess) process.exit(1);

  const { drive, success: apiSuccess } = await testDriveAPIAccess(oauth2Client);
  if (!apiSuccess) process.exit(1);

  if (!(await testRootFolder(drive))) process.exit(1);

  const testFileId = await testFileUpload(drive);
  
  if (testFileId) {
    await testPublicAccess(drive, testFileId);
    await testCleanup(drive, testFileId);
  } else {
    log('❌ Upload step failed, skipping public access test.', 'red');
  }

  log('\n✨ Test Complete', 'cyan');
}

runTests().catch(console.error);