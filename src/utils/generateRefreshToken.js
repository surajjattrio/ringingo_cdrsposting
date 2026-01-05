import { google } from 'googleapis';
import readline from 'readline';
import dotenv from 'dotenv';

dotenv.config();

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

const CLIENT_ID = process.env.GOOGLE_CLIENT_ID 
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET

// Use urn:ietf:wg:oauth:2.0:oob for manual code entry (no redirect needed)
const REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob';

const SCOPES = [
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/drive.metadata.readonly',
];

async function getRefreshToken() {
  log('\n' + '='.repeat(70), 'cyan');
  log('   GOOGLE DRIVE REFRESH TOKEN GENERATOR (MANUAL MODE)', 'cyan');
  log('='.repeat(70), 'cyan');

  // Create OAuth2 client
  const oauth2Client = new google.auth.OAuth2(
    CLIENT_ID,
    CLIENT_SECRET,
    REDIRECT_URI
  );

  // Generate authorization URL
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
    prompt: 'consent',
  });

  log('\n📋 Step 1: Authorize the Application', 'cyan');
  log('='.repeat(70), 'cyan');
  log('\n1. Copy the URL below and open it in your browser:', 'yellow');
  log('\n' + authUrl + '\n', 'blue');
  log('2. Sign in with your Gmail account (rameshwebdev9@gmail.com)', 'yellow');
  log('3. Click "Allow" to grant permissions', 'yellow');
  log('4. Copy the authorization code shown on the page', 'yellow');
  
  // Create readline interface
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise((resolve, reject) => {
    rl.question('\n📝 Paste the authorization code here: ', async (code) => {
      rl.close();

      if (!code || code.trim().length === 0) {
        log('\n❌ No code provided', 'red');
        reject(new Error('No authorization code'));
        return;
      }

      try {
        log('\n🔄 Step 2: Exchanging code for tokens...', 'cyan');
        log('='.repeat(70), 'cyan');

        // Exchange code for tokens
        const { tokens } = await oauth2Client.getToken(code.trim());
        
        log('\n✅ Success! Tokens received.', 'green');

        // Display refresh token
        log('\n' + '='.repeat(70), 'cyan');
        log('   YOUR NEW REFRESH TOKEN', 'cyan');
        log('='.repeat(70), 'cyan');
        
        if (tokens.refresh_token) {
          log('\n📝 Add this line to your .env file:\n', 'green');
          log(`GOOGLE_REFRESH_TOKEN=${tokens.refresh_token}`, 'yellow');
          log('\n' + '='.repeat(70), 'cyan');
        } else {
          log('\n⚠️  No refresh token in response!', 'yellow');
          log('\nThis usually means:', 'yellow');
          log('1. You already authorized this app before', 'blue');
          log('2. Google reused the old refresh token', 'blue');
          log('\nTo force a new refresh token:', 'cyan');
          log('1. Go to: https://myaccount.google.com/permissions', 'blue');
          log('2. Find and remove this app', 'blue');
          log('3. Run this script again', 'blue');
          log('\n' + '='.repeat(70), 'cyan');
        }

        // Test the token
        if (tokens.refresh_token || tokens.access_token) {
          log('\n🧪 Step 3: Testing token...', 'cyan');
          log('='.repeat(70), 'cyan');

          oauth2Client.setCredentials(tokens);
          
          try {
            const drive = google.drive({ version: 'v3', auth: oauth2Client });
            const about = await drive.about.get({ fields: 'user, storageQuota' });
            
            log('\n✅ Token is valid!', 'green');
            log(`   Email: ${about.data.user.emailAddress}`, 'blue');
            log(`   Name: ${about.data.user.displayName}`, 'blue');
            
            if (about.data.storageQuota) {
              const usedGB = (about.data.storageQuota.usage / (1024 ** 3)).toFixed(2);
              const limitGB = about.data.storageQuota.limit 
                ? (about.data.storageQuota.limit / (1024 ** 3)).toFixed(2)
                : 'Unlimited';
              log(`   Storage: ${usedGB} GB / ${limitGB} GB`, 'blue');
            }

            log('\n' + '='.repeat(70), 'cyan');
            log('   SETUP COMPLETE!', 'cyan');
            log('='.repeat(70), 'cyan');
            
            if (tokens.refresh_token) {
              log('\n📝 Next Steps:', 'green');
              log('1. Copy the GOOGLE_REFRESH_TOKEN line above to your .env file', 'blue');
              log('2. Save the .env file', 'blue');
              log('3. Run: node test-google-drive.js', 'blue');
              log('4. If tests pass, set GOOGLE_DRIVE_ENABLED=true in .env', 'blue');
              log('5. Restart your server: pm2 restart cdr-producer', 'blue');
            }
            
            log('\n');

            resolve(tokens);
          } catch (error) {
            log(`\n⚠️  Token test failed: ${error.message}`, 'yellow');
            log('But the token might still work. Try adding it to .env anyway.', 'yellow');
            resolve(tokens);
          }
        } else {
          resolve(tokens);
        }
      } catch (error) {
        log(`\n❌ Error: ${error.message}`, 'red');
        
        if (error.message.includes('invalid_grant')) {
          log('\nThe authorization code is invalid or has expired.', 'yellow');
          log('Please try again and paste the code immediately after getting it.', 'yellow');
        }
        
        reject(error);
      }
    });
  });
}

// Run
log('\n🚀 Starting token generation process...\n', 'cyan');
getRefreshToken()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    log('\n❌ Failed to generate refresh token', 'red');
    log(`Error: ${error.message}\n`, 'red');
    process.exit(1);
  });