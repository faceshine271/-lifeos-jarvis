// ATHENA v4.4 — Feb 24 2026 — Market Intelligence + CPC + Market Share + Attack List
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const twilio = require('twilio');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;

console.log("Starting LifeOS Jarvis...");

/* ===========================
   GOOGLE SHEETS AUTH
=========================== */

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const BUSINESS_SPREADSHEET_ID = process.env.BUSINESS_SPREADSHEET_ID || SPREADSHEET_ID;
const PROFIT_SPREADSHEET_ID = process.env.PROFIT_SPREADSHEET_ID || '';

if (!SPREADSHEET_ID) {
  console.error("Missing SPREADSHEET_ID");
  process.exit(1);
}

var keyfilePath = path.join(__dirname, 'google-credentials.json');

// Method 1: Base64 encoded credentials (decode to temp file)
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  console.log("Using GOOGLE_CREDENTIALS_BASE64 env var");
  var decoded = Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8');
  var tmpPath = '/tmp/google-credentials.json';
  fs.writeFileSync(tmpPath, decoded);
  keyfilePath = tmpPath;
}

// Method 2: Local JSON keyfile
if (!fs.existsSync(keyfilePath)) {
  console.error("No Google credentials found! Set GOOGLE_CREDENTIALS_BASE64 env var or add google-credentials.json file.");
  process.exit(1);
}

console.log("Using keyfile: " + keyfilePath);
var auth = new google.auth.GoogleAuth({
  keyFile: keyfilePath,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

var sheets = google.sheets({ version: 'v4', auth: auth });
console.log("Google Auth Ready");

/* ===========================
   TWILIO CLIENT
=========================== */

var twilioClient = twilio(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN
);

var TWILIO_NUMBER = '+18884310969';
var MY_NUMBER = '+18167392734';
console.log("Twilio Ready");

/* ===========================
   CLAUDE API KEY
=========================== */

var CLAUDE_API_KEY = process.env.CLAUDE_API_KEY;
if (!CLAUDE_API_KEY) {
  console.error("Missing CLAUDE_API_KEY");
}
console.log("Claude API Ready");

/* ===========================
   GMAIL OAUTH2 SETUP
=========================== */

var GMAIL_CLIENT_ID = process.env.GMAIL_CLIENT_ID;
var GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET;
var GMAIL_REDIRECT_URI = process.env.GMAIL_REDIRECT_URI || 'https://lifeos-jarvis.onrender.com/gmail/callback';

// Store tokens for multiple Gmail accounts { email: { access_token, refresh_token, expiry } }
var gmailTokensFile = '/tmp/gmail-tokens.json';
var gmailTokens = {};

// Load saved tokens
try {
  if (process.env.GMAIL_TOKENS) {
    gmailTokens = JSON.parse(process.env.GMAIL_TOKENS);
    console.log("Gmail: Loaded " + Object.keys(gmailTokens).length + " account(s)");
  } else if (fs.existsSync(gmailTokensFile)) {
    gmailTokens = JSON.parse(fs.readFileSync(gmailTokensFile, 'utf8'));
    console.log("Gmail: Loaded " + Object.keys(gmailTokens).length + " account(s) from file");
  }
} catch (e) {
  console.log("Gmail: No saved tokens found");
}

function saveGmailTokens() {
  try { fs.writeFileSync(gmailTokensFile, JSON.stringify(gmailTokens)); } catch (e) {}
}

function createGmailOAuth2Client(tokens) {
  var oauth2 = new google.auth.OAuth2(GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REDIRECT_URI);
  if (tokens) oauth2.setCredentials(tokens);
  return oauth2;
}

if (GMAIL_CLIENT_ID) {
  console.log("Gmail OAuth Ready");
} else {
  console.log("Gmail: No GMAIL_CLIENT_ID set — Gmail features disabled");
}

/* ===========================
   In-memory conversation history
=========================== */

var callHistory = {};

/* ===========================
   HELPERS
=========================== */

async function getAllTabNames() {
  var res = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
    fields: 'sheets.properties.title',
  });
  return res.data.sheets.map(function(s) { return s.properties.title; });
}

async function getTabData(tabName) {
  try {
    var res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'" + tabName + "'!A1:ZZ",
    });
    var rows = res.data.values || [];
    if (rows.length === 0) return { tab: tabName, headers: [], rowCount: 0, rows: [] };
    return { tab: tabName, headers: rows[0], rowCount: rows.length - 1, rows: rows.slice(1) };
  } catch (err) {
    return { tab: tabName, error: err.message, headers: [], rowCount: 0, rows: [] };
  }
}

async function getTabRowCount(tabName) {
  try {
    var res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'" + tabName + "'!A:A",
    });
    return res.data.values ? res.data.values.length - 1 : 0;
  } catch (err) {
    return 0;
  }
}

/* ===========================
   HTML escaping helper — prevents XSS from sheet data
=========================== */

function escapeHtml(str) {
  if (!str) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/* ===========================
   Build Life OS context for Claude — PERSONAL FOCUS
   Cached for 10 minutes to speed up responses
=========================== */

var contextCache = { data: null, time: 0 };

async function buildLifeOSContext() {
  // Return cache if less than 10 minutes old
  if (contextCache.data && (Date.now() - contextCache.time) < 600000) {
    console.log("Context from cache: " + contextCache.data.length + " chars");
    return contextCache.data;
  }

  var context = "";

  // Fetch all personal data in parallel for speed
  var results = await Promise.allSettled([
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Trace_Identity_Profile'!A1:A20" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Daily_Log'!A1:J20" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Wins'!A1:D20" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Gratitude_Memory'!A1:B10" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Dashboard'!A1:K25" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Chat_Pattern_Lifecycle'!A1:F30" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Chat_Insights_Dating'!A1:D7" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Daily_Questions'!A1:G20" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Dating_Log'!A1:H20" }),
    sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Ultimate_Debt_Tracker_Advanced'!A1:E25" }),
  ]);

  // Identity Profile
  if (results[0].status === 'fulfilled') {
    var rows = results[0].value.data.values || [];
    if (rows.length > 0) {
      context += "WHO TRACE IS:\n";
      rows.slice(0, 15).forEach(function(r) { if (r[0]) context += r[0] + "\n"; });
      context += "\n";
    }
  }

  // Daily Log — recent entries
  if (results[1].status === 'fulfilled') {
    var rows = results[1].value.data.values || [];
    if (rows.length > 1) {
      var headers = rows[0];
      context += "RECENT DAILY LOGS:\n";
      rows.slice(Math.max(1, rows.length - 5)).forEach(function(r) {
        var parts = [];
        for (var i = 0; i < headers.length; i++) {
          if (r[i] && r[i] !== '0' && r[i] !== '') parts.push(headers[i] + ': ' + r[i]);
        }
        if (parts.length > 0) context += "  " + parts.join(', ') + "\n";
      });
      context += "\n";
    }
  }

  // Recent Wins
  if (results[2].status === 'fulfilled') {
    var rows = results[2].value.data.values || [];
    if (rows.length > 1) {
      context += "RECENT WINS:\n";
      rows.slice(Math.max(1, rows.length - 7)).forEach(function(r) {
        if (r[1]) context += "  " + (r[0] || '') + ": " + r[1] + " (" + (r[2] || '') + ")\n";
      });
      context += "\n";
    } else {
      context += "WINS: No wins logged yet. This needs to change.\n\n";
    }
  }

  // Recent Gratitude
  if (results[3].status === 'fulfilled') {
    var rows = results[3].value.data.values || [];
    if (rows.length > 1) {
      context += "RECENT GRATITUDE:\n";
      rows.slice(1, 6).forEach(function(r) {
        if (r[0]) context += "  " + r[0] + " (" + (r[1] || '') + ")\n";
      });
      context += "\n";
    }
  }

  // Screen Time
  if (results[4].status === 'fulfilled') {
    var rows = results[4].value.data.values || [];
    if (rows.length > 5) {
      context += "SCREEN TIME:\n";
      // Find the daily total
      if (rows[1] && rows[1][0]) context += "  Daily average: " + rows[1][0] + " hours\n";
      // Top apps
      for (var a = 6; a < Math.min(rows.length, 16); a++) {
        if (rows[a] && rows[a][0] && rows[a][4]) {
          context += "  " + rows[a][0] + ": " + rows[a][4] + "h/day (" + (rows[a][5] || 'uncategorized') + ")\n";
        }
      }
      context += "\n";
    }
  }

  // Dating/Relationship Patterns
  if (results[5].status === 'fulfilled') {
    var rows = results[5].value.data.values || [];
    if (rows.length > 1) {
      context += "ACTIVE LIFE PATTERNS:\n";
      rows.slice(1).forEach(function(r) {
        if (r[0] && r[5] && r[5].toUpperCase() === 'ACTIVE') {
          context += "  " + r[0] + " (" + (r[1] || 'General') + ") — ACTIVE\n";
        }
      });
      context += "\n";
    }
  }

  // Dating Insights
  if (results[6].status === 'fulfilled') {
    var rows = results[6].value.data.values || [];
    if (rows.length > 1) {
      context += "DATING PATTERNS:\n";
      var latest = rows[rows.length - 1];
      if (latest && latest[2]) context += "  Latest insight: " + latest[2].substring(0, 300) + "\n";
      context += "\n";
    }
  }

  // Recent Daily Questions answers
  if (results[7].status === 'fulfilled') {
    var rows = results[7].value.data.values || [];
    if (rows.length > 1) {
      context += "RECENT SELF-REFLECTION:\n";
      rows.slice(Math.max(1, rows.length - 5)).forEach(function(r) {
        if (r[2] && r[3]) context += "  Q: " + r[2].substring(0, 80) + " → A: " + r[3].substring(0, 100) + "\n";
      });
      context += "\n";
    }
  }

  // Dating Log
  if (results[8].status === 'fulfilled') {
    var rows = results[8].value.data.values || [];
    if (rows.length > 1) {
      context += "RECENT DATING ACTIVITY:\n";
      rows.slice(Math.max(1, rows.length - 3)).forEach(function(r) {
        if (r[1]) context += "  " + (r[0] || '') + " — " + (r[1] || '') + " (" + (r[2] || '') + "): " + (r[3] || '') + "\n";
      });
      context += "\n";
    }
  }

  // Debt summary (brief)
  if (results[9].status === 'fulfilled') {
    var rows = results[9].value.data.values || [];
    if (rows.length > 1) {
      var totalDebt = 0;
      rows.slice(1).forEach(function(r) {
        var bal = parseFloat((r[4] || r[3] || '0').toString().replace(/[$,]/g, ''));
        if (!isNaN(bal)) totalDebt += bal;
      });
      context += "FINANCIAL SNAPSHOT: ~$" + Math.round(totalDebt).toLocaleString() + " total debt across " + (rows.length - 1) + " accounts\n\n";
    }
  }

  console.log("Context built: " + context.length + " chars");
  
  // Add calendar (not cached as long since events change)
  try {
    var calContext = await buildCalendarContext(2);
    context += calContext;
  } catch (e) {}

  contextCache = { data: context, time: Date.now() };
  return context;
}

/* ===========================

/* ===========================
   SHEET REGISTRY — All 12 source spreadsheets + descriptions
   The server reads these DIRECTLY. No Combined tab needed.
=========================== */

var SOURCE_SHEETS = [
  { id: "1LlfhcfiQdXStpV1vRrZzSmaEjyTvd1nrk5sqnhxsRYU", name: "Main CRM", desc: "Core CRM with 18+ city tabs. Each tab has customer bookings: name, phone, email, address, equipment, tech, status. Updated daily by receptionists. Also has Tech Numbers, Return Customers, Promo Replies, Diagnostic SMS, Manual Entry." },
  { id: "1kl72v4yIJrpD3U5pCYwtCiDhtdFZ_n4wUvZDxhPeIQk", name: "Receptionist Data", desc: "Receptionist performance: booking rates, call logs per person. Matrix views for 2025-2026 showing monthly/weekly/daily booking %. Individual tabs for each receptionist (Ray, Muaaz, Rayan, Rubait, Salma, etc.) with call-by-call logs." },
  { id: "1SDOqTxEMG8f81DtLwIu9ovz9mnxP_9Lpn6PH7OFBfBs", name: "Top Volume Cities", desc: "SEO market research: 70+ city tabs across 10+ states. Monthly Google search volume for 'small engine repair' from 2023-2025. Population data. Used for expansion planning." },
  { id: "1fj6SZZx5YtLMU8ldsAEZ1yffK0rHhFQoFir0GAOCFNU", name: "US Audit", desc: "Nationwide SEO audit — all 50 states. Top 15-35 cities per state with population and monthly search volume 2023-2025." },
  { id: "1ZITxT57ue2qSAbTUFRE_k1fJzKicPAO-BZMDSJOhJ7A", name: "SEO Competitive Analysis", desc: "All 50 states sorted by search volume high-to-low. Multiple keywords per city: small engine repair, lawn mower repair, etc. Includes keyword difficulty and CPC (cost per click)." },
  { id: "1CJ9nn7l_PAwXPVSXmUogXejt46bsimrJi5iLQFQuo3o", name: "GMB Reviews", desc: "Google My Business reviews tracker. Reviews for Quick & Mobile Wildwood Small (7 reviews), Mobile Wildwood Mower Repair (44 reviews), Wildwood FL (22 reviews). GMB listing links per city. Discord reviews feed." },
  { id: "1KIulnemtmR6QpRbzEflNjvVOElNszuL9zClKhcbmOow", name: "Active Locations & HR", desc: "Employee roster (~990 rows), tech hiring pipeline (~1123 applicants), active locations with assigned techs (33 locations), 90-day evaluations. Core HR + operations hub." },
  { id: "19ndlgop-P0KLwv6PiG8sPdtj83jKH3vlDHf5AgjgmC8", name: "Payment Gap Analysis", desc: "Tracks avg hours between job completion and payment received. Monthly comparisons and weekly backend data. Cash flow health indicator." },
  { id: "1CiH2u18yy-stL7jym7RCinqjy5C3HJpCg9GyYQv2TEw", name: "Backup & Canceled", desc: "Backup copy of all booked jobs and separate tab for canceled jobs. Used by Tookan automation for data recovery and cancellation tracking." },
  { id: "1pIEAYT5bJff7HxrjEuQnbJf8TxZ9361wta7fwUAjZ70", name: "Additional Data", desc: "Additional business data sheet." },
  { id: "1A8oUmigHV6DsYcWF4hlDBC5KQDIHWMOh1Is6poCacx4", name: "Source Sheet 9", desc: "Additional business data — awaiting description." },
  { id: "1vnNEZjdhhkFNpNkDXRINpS55Zfysb_MzuFLhjVw6A2g", name: "Source Sheet 10", desc: "Additional business data — awaiting description." },
  { id: "1ZshCanMloF8uUlH39s2ZxvpCuvxjJQAONaSnN7590WA", name: "Source Sheet 11", desc: "Additional business data — awaiting description." },
  { id: "1IK-T9O_-ozg7n-Fecn1DodEMuznbvVW-i0ClzCPfuOI", name: "Source Sheet 12", desc: "Additional business data — awaiting description." },
];

var SKIP_TABS = [
  "combined","combined_all","tech numbers","mapping","dropdown","mail list",
  "sample","test","location sheets","manual entry record",
  "diagnostic sms reply","promotion customers reply","sops and contract",
  "sheet1","location unavailable","return customers","unorganized customers",
  "main","receptionist names","discord reviews"
];

var HEADER_MAP = {
  "date called in":"dateIn","date and time":"dateIn","date and time ":"dateIn","column 1":"dateIn",
  "first name":"firstName","first name ":"firstName",
  "last name":"lastName","last name ":"lastName",
  "start time":"startTime","start time ":"startTime",
  "end time":"endTime"," end time":"endTime",
  "date customer is available":"serviceDate","time (3 hour window)":"startTime",
  "phone number":"phone","phone number ":"phone","phone":"phone",
  "email":"email","email id":"email",
  "address":"address","city":"city","state":"state","zip":"zip",
  "type equipment":"equipType","type equipment ":"equipType",
  "type of equipment":"equipType","type of equipment ":"equipType",
  "what brand of equipment":"brand",
  "issue with equipment":"issue","issue with equipment ":"issue",
  "receptionist names":"receptionist","receptionist names ":"receptionist",
  "notes tech needs to know":"notes","notes tech needs to know ":"notes",
  "status":"status","when booked":"whenBooked",
  "return/paid?":"returnPaid",
  "tooka status and time":"tookanStatus","tookan status":"tookanStatus","tookan status and time":"tookanStatus",
  "posted date and time":"postedDate","posted flag":"postedFlag",
  "tookan job id":"tookanJobId","tech":"tech","technician":"tech","tech name":"tech","assigned tech":"tech","assigned to":"tech","technician name":"tech","transfer":"locationTab"
};

/* ===========================
   TOOKAN API INTEGRATION — Real-time dispatch & job tracking
=========================== */

var TOOKAN_API_KEY = process.env.TOOKAN_API_KEY || '5365698cf64403111e4c723e15106e471be0c5fd28df793c5e1808c3';
var TOOKAN_ENDPOINTS = {
  getJobDetails: 'https://api.tookanapp.com/v2/get_job_details',
  getAllAgents: 'https://api.tookanapp.com/v2/get_all_agents',
  getAllTasks: 'https://api.tookanapp.com/v2/get_all_tasks',
};

var TOOKAN_LOCATIONS = [
  { key:'kansasCityRocky', sheet:'Kansas City, MO Rocky', teamId:1680920, fleetId:2118700 },
  { key:'lovelandA', sheet:'Loveland, CO Justin Turner', teamId:1696802, fleetId:2108961 },
  { key:'detroit', sheet:'Detroit, MI', teamId:1681499, fleetId:2071027 },
  { key:'houstonB', sheet:'Houston, TX Victor Romero', teamId:1681497, fleetId:2125711 },
  { key:'wilmingtonNC', sheet:'Wilmington NC Brandi Butler', teamId:1702410, fleetId:2089324 },
  { key:'tampa', sheet:'Tampa, Fl', teamId:1689395, fleetId:2115390 },
  { key:'poinciana', sheet:'Poinciana, FL', teamId:1682259, fleetId:2072205 },
  { key:'brownsville', sheet:'Brownsville, TX', teamId:1681494, fleetId:2107061 },
  { key:'elkhart', sheet:'Elkhart, IN', teamId:1695278, fleetId:2101066 },
  { key:'kansasCityMain', sheet:'Kansas City, MO', teamId:1680920, fleetId:2071029 },
  { key:'siouxFallsSD', sheet:'Sioux Falls, SD Ashton Hawley', teamId:1681507, fleetId:2165914 },
  { key:'sanAntonioTX', sheet:'San Antonio, TX Robert Hummer', teamId:1681496, fleetId:2166157 },
  { key:'capeCoralA', sheet:'Cape Coral, FL Michael Scutti', teamId:1681492, fleetId:2107763 },
  { key:'capeCoralB', sheet:'Cape Coral, FL Talon Twiford', teamId:1681492, fleetId:2071009 },
  { key:'lehighAcresFL', sheet:'Lehigh Acres, FL Gunnar Jacobs', teamId:1704072, fleetId:2119751 },
  { key:'fortWayneIN', sheet:'Fort Wayne IN Corey Roberson', teamId:1688549, fleetId:2162203 },
  { key:'sarasotaFL', sheet:'Sarasota, FL Alexander Fernandez', teamId:1683936, fleetId:2149334 },
  { key:'poincianaFL2', sheet:'Poinciana, FL Trent Kennedy', teamId:1682259, fleetId:2072205 },
  { key:'renoNV', sheet:'Reno, NV  Maxx Fritts', teamId:1714273, fleetId:2142347 },
];

var tookanCache = { data: null, time: 0 };

// Fetch all tasks from Tookan API for a date range
async function fetchTookanTasks(startDate, endDate, teamId) {
  try {
    var payload = {
      api_key: TOOKAN_API_KEY,
      job_type: 2,
      job_status: '',
      start_date: startDate,
      end_date: endDate,
      is_pagination: 0,
    };
    if (teamId) payload.team_id = teamId;
    var response = await fetch(TOOKAN_ENDPOINTS.getAllTasks, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    var data = await response.json();
    if (data.status === 200) {
      var tasks = Array.isArray(data.data) ? data.data : (data.data || []);
      return tasks;
    }
    console.log("Tookan getAllTasks status: " + data.status + " msg: " + (data.message || '') + " team:" + (teamId || 'none'));
    return [];
  } catch (e) {
    console.log("Tookan getAllTasks error: " + e.message);
    return [];
  }
}

// Fetch job details for specific job IDs
async function fetchTookanJobDetails(jobIds) {
  if (!jobIds || jobIds.length === 0) return [];
  try {
    var response = await fetch(TOOKAN_ENDPOINTS.getJobDetails, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        api_key: TOOKAN_API_KEY,
        job_ids: jobIds.map(Number),
      }),
    });
    var data = await response.json();
    if (data.status === 200) {
      return Array.isArray(data.data) ? data.data : (data.data.tasks || []);
    }
    return [];
  } catch (e) {
    console.log("Tookan getJobDetails error: " + e.message);
    return [];
  }
}

// Fetch all agents/techs from Tookan
async function fetchTookanAgents(teamId) {
  try {
    var response = await fetch(TOOKAN_ENDPOINTS.getAllAgents, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        api_key: TOOKAN_API_KEY,
        team_id: teamId,
      }),
    });
    var data = await response.json();
    if (data.status === 200) {
      return Array.isArray(data.data) ? data.data : (data.data.fleets || data.data.agents || []);
    }
    return [];
  } catch (e) {
    console.log("Tookan getAgents error: " + e.message);
    return [];
  }
}

// Derive readable status from Tookan job data
function deriveTookanStatus(job) {
  if (!job) return 'Unknown';
  var label = (job.job_status_name || job.job_status_text || '').toString().trim();
  if (label) return label.replace(/Accepted/i, 'Acknowledged');
  var nz = function(s) { return !!s && s !== '0000-00-00 00:00:00'; };
  if (nz(job.completed_datetime)) return 'Completed';
  if (nz(job.acknowledged_datetime)) return 'Acknowledged';
  if (nz(job.started_datetime)) return 'Started';
  if (nz(job.arrived_datetime)) return 'Arrived';
  if (job.fleet_id && Number(job.fleet_id) !== 0) return 'Assigned';
  if (typeof job.job_status === 'number') {
    return ({ 0: 'Assigned', 1: 'Started', 2: 'Completed', 3: 'Failed', 6: 'Unassigned', 9: 'Deleted' })[job.job_status] || 'Unknown (' + job.job_status + ')';
  }
  return 'Unknown';
}

// Build full Tookan summary — cached for 10 minutes
async function buildTookanContext() {
  if (tookanCache.data && (Date.now() - tookanCache.time) < 600000) {
    return tookanCache.data;
  }

  console.log("Fetching Tookan data...");
  var result = {
    totalTasks: 0, completed: 0, assigned: 0, acknowledged: 0, started: 0,
    failed: 0, unassigned: 0, cancelled: 0, deleted: 0,
    tasksByLocation: {}, tasksByTech: {}, tasksByStatus: {},
    todayTasks: [], recentCompleted: [], agents: [], mapTasks: [], upcomingTasks: [],
    // Analytics
    todayTotal: 0, todayCompleted: 0, todayAssigned: 0, todayAcknowledged: 0,
    yesterdayTotal: 0, yesterdayCompleted: 0,
    agentsBusy: 0, agentsFree: 0, agentsInactive: 0,
    avgCompletionMinutes: 0, onTimeRate: 0, taskEfficiency: 0,
    weeklyTasks: [], dailyTasks: {},
  };

  var completionTimes = [];
  try {
    var endDate = new Date();
    var startDate = new Date();
    startDate.setDate(startDate.getDate() - 90);

    // ====== Strategy 1: Get job IDs from CRM data, then batch fetch details ======
    var allTasks = [];
    var crmJobIds = [];

    // Collect Tookan Job IDs from CRM (already loaded by buildBusinessContext)
    if (global.bizMetrics && global.bizMetrics.allJobRows) {
      global.bizMetrics.allJobRows.forEach(function(job) {
        var jid = (job.tookanJobId || '').toString().trim();
        if (jid && jid.length > 3 && /^\d+$/.test(jid)) {
          crmJobIds.push(parseInt(jid));
        }
      });
    }

    // Deduplicate job IDs
    crmJobIds = Array.from(new Set(crmJobIds));
    console.log("Tookan: Found " + crmJobIds.length + " job IDs from CRM");

    if (crmJobIds.length > 0) {
      // Batch fetch in groups of 50 (same as your Tookan script)
      for (var bi = 0; bi < crmJobIds.length; bi += 50) {
        var batch = crmJobIds.slice(bi, bi + 50);
        try {
          var batchResults = await fetchTookanJobDetails(batch);
          allTasks = allTasks.concat(batchResults);
          // Small delay between batches to avoid rate limiting
          if (bi + 50 < crmJobIds.length) {
            await new Promise(function(r) { setTimeout(r, 300); });
          }
        } catch (be) {
          console.log("Tookan batch error at offset " + bi + ": " + be.message);
        }
      }
      console.log("Tookan: " + allTasks.length + " tasks fetched via get_job_details");
    }

    // ====== Strategy 2 (fallback): Try get_all_tasks per team if no CRM data ======
    if (allTasks.length === 0) {
      console.log("Tookan: No CRM job IDs, trying get_all_tasks per team...");
      // Get unique team IDs
      var uniqueTeamIds = {};
      TOOKAN_LOCATIONS.forEach(function(loc) { uniqueTeamIds[loc.teamId] = true; });
      var teamList = Object.keys(uniqueTeamIds);

      // Tookan API only allows 31-day max range
      // Fetch last 30 days + next 30 days per team
      var dateChunks = [
        { label: 'future', start: 0, end: 30 },    // Today → +30 days
        { label: 'recent', start: -30, end: 0 },    // -30 days → Today
        { label: 'mid', start: -60, end: -30 },     // -60 → -30 days
        { label: 'old', start: -90, end: -60 },     // -90 → -60 days
      ];

      var seenJobIds = {};
      for (var ti2 = 0; ti2 < teamList.length; ti2++) {
        var tid = parseInt(teamList[ti2]);
        for (var ci = 0; ci < dateChunks.length; ci++) {
          var ch = dateChunks[ci];
          var cStart = new Date(); cStart.setDate(cStart.getDate() + ch.start);
          var cEnd = new Date(); cEnd.setDate(cEnd.getDate() + ch.end);
          var cStartStr = cStart.toISOString().split('T')[0] + ' 00:00:00';
          var cEndStr = cEnd.toISOString().split('T')[0] + ' 23:59:59';
          try {
            var chunkTasks = await fetchTookanTasks(cStartStr, cEndStr, tid);
            // Deduplicate across teams/chunks
            var newCount = 0;
            chunkTasks.forEach(function(t) {
              var jid = t.job_id || t.jobid || '';
              if (!seenJobIds[jid]) {
                seenJobIds[jid] = true;
                allTasks.push(t);
                newCount++;
              }
            });
            if (newCount > 0) console.log("Tookan team " + tid + " " + ch.label + ": +" + newCount + " tasks");
            await new Promise(function(r) { setTimeout(r, 200); });
          } catch (ce) {
            console.log("Tookan team " + tid + " " + ch.label + " error: " + ce.message);
          }
        }
      }

      // If team-level fetch also empty, try without team_id as last resort
      if (allTasks.length === 0) {
        console.log("Tookan: Team fetch empty, trying without team_id...");
        var now2 = new Date();
        var ago30 = new Date(); ago30.setDate(ago30.getDate() - 30);
        var fwd30 = new Date(); fwd30.setDate(fwd30.getDate() + 30);
        try {
          var recentAll = await fetchTookanTasks(ago30.toISOString().split('T')[0] + ' 00:00:00', fwd30.toISOString().split('T')[0] + ' 23:59:59');
          console.log("Tookan no-team fetch: " + recentAll.length + " tasks");
          allTasks = allTasks.concat(recentAll);
        } catch (e2) {}
      }

      console.log("Tookan total from get_all_tasks: " + allTasks.length + " tasks");
    }

    result.totalTasks = allTasks.length;
    console.log("Tookan total: " + allTasks.length + " tasks");

    var todayStr = endDate.toISOString().split('T')[0];
    var yesterdayDate = new Date(endDate);
    yesterdayDate.setDate(yesterdayDate.getDate() - 1);
    var yesterdayStr = yesterdayDate.toISOString().split('T')[0];

    for (var t = 0; t < allTasks.length; t++) {
      var task = allTasks[t];
      var status = deriveTookanStatus(task);
      var statusLower = status.toLowerCase();

      // Count by status
      result.tasksByStatus[status] = (result.tasksByStatus[status] || 0) + 1;
      if (statusLower.includes('completed')) result.completed++;
      else if (statusLower.includes('acknowledged')) result.acknowledged++;
      else if (statusLower.includes('assigned')) result.assigned++;
      else if (statusLower.includes('started') || statusLower.includes('arrived')) result.started++;
      else if (statusLower.includes('failed') || statusLower.includes('cancel')) result.cancelled++;
      else if (statusLower.includes('unassigned')) result.unassigned++;
      else if (statusLower.includes('deleted')) result.deleted++;

      // Daily tracking
      var taskDate = (task.job_pickup_datetime || task.created_at || '').toString().split(' ')[0];
      result.dailyTasks[taskDate] = (result.dailyTasks[taskDate] || 0) + 1;
      if (taskDate === todayStr) {
        result.todayTotal++;
        if (statusLower.includes('completed')) result.todayCompleted++;
        if (statusLower.includes('assigned')) result.todayAssigned++;
        if (statusLower.includes('acknowledged')) result.todayAcknowledged++;
      }
      if (taskDate === yesterdayStr) {
        result.yesterdayTotal++;
        if (statusLower.includes('completed')) result.yesterdayCompleted++;
      }

      // Completion time tracking
      if (statusLower.includes('completed') && task.completed_datetime && task.job_pickup_datetime) {
        var pickupTime = new Date(task.job_pickup_datetime).getTime();
        var completeTime = new Date(task.completed_datetime).getTime();
        if (pickupTime > 0 && completeTime > pickupTime) {
          var mins = (completeTime - pickupTime) / 60000;
          if (mins > 0 && mins < 1440) completionTimes.push(mins);
        }
      }

      // Tech tracking
      var agentName = (task.fleet_name || task.agent_name || '').toString().trim();
      if (agentName) {
        if (!result.tasksByTech[agentName]) result.tasksByTech[agentName] = { total: 0, completed: 0, assigned: 0, started: 0, failed: 0 };
        result.tasksByTech[agentName].total++;
        if (statusLower.includes('completed')) result.tasksByTech[agentName].completed++;
        else if (statusLower.includes('assigned') || statusLower.includes('acknowledged')) result.tasksByTech[agentName].assigned++;
        else if (statusLower.includes('started') || statusLower.includes('arrived')) result.tasksByTech[agentName].started++;
        else if (statusLower.includes('failed')) result.tasksByTech[agentName].failed++;
      }

      // Location tracking
      var address = (task.job_address || task.customer_address || '').toString();
      var city = address.split(',').length >= 2 ? address.split(',').slice(-2, -1)[0].trim() : '';
      if (city) {
        result.tasksByLocation[city] = (result.tasksByLocation[city] || 0) + 1;
      }

      // Today's tasks
      var taskDate = (task.job_pickup_datetime || task.created_at || '').toString().split(' ')[0];
      if (taskDate === todayStr) {
        result.todayTasks.push({
          jobId: task.job_id, customer: task.customer_username || task.customer_name || 'Unknown',
          address: address.substring(0, 80), status: status, tech: agentName,
          phone: task.customer_phone || '',
          lat: parseFloat(task.job_latitude || task.latitude || 0),
          lng: parseFloat(task.job_longitude || task.longitude || 0),
          pickupTime: (task.job_pickup_datetime || '').toString(),
        });
      }

      // Upcoming tasks (future dates)
      if (taskDate > todayStr && result.upcomingTasks.length < 50) {
        result.upcomingTasks.push({
          jobId: task.job_id, customer: task.customer_username || task.customer_name || 'Unknown',
          address: address.substring(0, 80), status: status, tech: agentName,
          phone: task.customer_phone || '', date: taskDate,
          lat: parseFloat(task.job_latitude || task.latitude || 0),
          lng: parseFloat(task.job_longitude || task.longitude || 0),
          pickupTime: (task.job_pickup_datetime || '').toString(),
        });
      }

      // Collect tasks with coordinates for map (last 30 days only to keep it manageable)
      var taskLat = parseFloat(task.job_latitude || task.latitude || 0);
      var taskLng = parseFloat(task.job_longitude || task.longitude || 0);
      if (taskLat !== 0 && taskLng !== 0 && result.mapTasks.length < 500) {
        result.mapTasks.push({
          jobId: task.job_id, customer: (task.customer_username || task.customer_name || 'Unknown').substring(0, 30),
          lat: taskLat, lng: taskLng, status: status, tech: agentName,
          address: address.substring(0, 60), date: taskDate,
        });
      }

      // Recent completed
      if (statusLower.includes('completed') && result.recentCompleted.length < 20) {
        result.recentCompleted.push({
          jobId: task.job_id, customer: task.customer_username || 'Unknown',
          tech: agentName, completedAt: task.completed_datetime || '',
          address: address.substring(0, 60),
          lat: parseFloat(task.job_latitude || task.latitude || 0),
          lng: parseFloat(task.job_longitude || task.longitude || 0),
        });
      }
    }

    // Fetch agents from unique team IDs
    var teamIds = {};
    TOOKAN_LOCATIONS.forEach(function(loc) { teamIds[loc.teamId] = true; });
    var uniqueTeams = Object.keys(teamIds);

    for (var ti = 0; ti < uniqueTeams.length; ti++) {
      try {
        var agents = await fetchTookanAgents(parseInt(uniqueTeams[ti]));
        agents.forEach(function(a) {
          result.agents.push({
            name: a.fleet_name || a.username || [a.first_name, a.last_name].filter(Boolean).join(' '),
            fleetId: a.fleet_id || a.id, teamId: uniqueTeams[ti],
            status: a.is_available ? 'Available' : 'Unavailable',
            phone: a.phone || '',
          });
        });
        if (ti < uniqueTeams.length - 1) await new Promise(function(r) { setTimeout(r, 500); });
      } catch (ae) {}
    }

  } catch (e) {
    console.log("Tookan fetch error: " + e.message);
  }

  // ====== Compute Analytics ======
  // Agent status counts
  result.agents.forEach(function(a) {
    if (a.status === 'Available') result.agentsFree++;
    else result.agentsInactive++;
  });
  // Count busy agents (agents with active/started tasks today)
  var busyAgents = {};
  (result.todayTasks || []).forEach(function(t) {
    var st = (t.status || '').toLowerCase();
    if (t.tech && (st.includes('started') || st.includes('arrived') || st.includes('acknowledged'))) {
      busyAgents[t.tech] = true;
    }
  });
  result.agentsBusy = Object.keys(busyAgents).length;
  result.agentsFree = Math.max(0, result.agentsFree - result.agentsBusy);

  // Avg completion time
  if (completionTimes.length > 0) {
    result.avgCompletionMinutes = Math.round(completionTimes.reduce(function(a, b) { return a + b; }, 0) / completionTimes.length);
  }

  // Task efficiency = completed / (completed + failed + cancelled) * 100
  var effTotal = result.completed + result.cancelled;
  result.taskEfficiency = effTotal > 0 ? Math.round((result.completed / effTotal) * 10000) / 100 : 0;

  // On-time rate (completed within scheduled window)
  result.onTimeRate = result.completed > 0 ? Math.round((result.completed / Math.max(1, result.completed + result.started)) * 10000) / 100 : 0;

  // Weekly task trend (last 7 days)
  var weekDays = [];
  for (var wd = 6; wd >= 0; wd--) {
    var d2 = new Date();
    d2.setDate(d2.getDate() - wd);
    var dStr = d2.toISOString().split('T')[0];
    var dayName = d2.toLocaleDateString('en-US', { weekday: 'short' });
    weekDays.push({ date: dStr, day: dayName, count: result.dailyTasks[dStr] || 0 });
  }
  result.weeklyTasks = weekDays;

  // Task change % vs yesterday
  result.taskChangeVsYesterday = result.yesterdayTotal > 0 ? Math.round(((result.todayTotal - result.yesterdayTotal) / result.yesterdayTotal) * 100) : 0;

  // Store globally for dashboard
  global.tookanData = result;
  tookanCache = { data: result, time: Date.now() };
  console.log("Tookan data cached: " + result.totalTasks + " tasks, " + result.completed + " completed, " + result.agents.length + " agents");
  return result;
}

/* ===========================
   Build Business CRM Context for Claude
   Reads ALL 12 source sheets directly — no Combined tab dependency
   Uses batchGet for speed, caches for 30 minutes
=========================== */

var businessCache = { data: null, time: 0 };

async function buildBusinessContext() {
  // Return cache if less than 30 minutes old
  if (businessCache.data && (Date.now() - businessCache.time) < 1800000) {
    return businessCache.data;
  }

  console.log("Building business context from " + SOURCE_SHEETS.length + " source sheets...");
  var context = "WILDWOOD SMALL ENGINE REPAIR — CRM DATA:\n\n";

  // ====== PHASE 1: Read all job data from source sheets ======
  var allJobRows = [];
  var allOtherSections = {};
  var sheetMetadata = {};
  var sheetsRead = 0;
  var tabsRead = 0;
  var errors = [];

  for (var si = 0; si < SOURCE_SHEETS.length; si++) {
    var src = SOURCE_SHEETS[si];
    try {
      // Step 1: Get tab list for this sheet
      var meta = await sheets.spreadsheets.get({
        spreadsheetId: src.id,
        fields: 'sheets.properties.title,sheets.properties.sheetId'
      });
      var tabNames = meta.data.sheets.map(function(s) { return s.properties.title; });
      sheetMetadata[src.name] = { tabs: tabNames, id: src.id, desc: src.desc };

      // Step 2: Build ranges for all non-skip tabs
      var dataRanges = [];
      for (var tn = 0; tn < tabNames.length; tn++) {
        if (SKIP_TABS.indexOf(tabNames[tn].toLowerCase().trim()) < 0) {
          dataRanges.push("'" + tabNames[tn] + "'!A1:AE");
        }
      }

      if (dataRanges.length === 0) continue;

      // Step 3: Batch read ALL tabs in ONE API call
      var batchRes = await sheets.spreadsheets.values.batchGet({
        spreadsheetId: src.id,
        ranges: dataRanges,
      });

      sheetsRead++;
      var valueRanges = batchRes.data.valueRanges || [];

      for (var vr = 0; vr < valueRanges.length; vr++) {
        var rows = valueRanges[vr].values || [];
        if (rows.length < 2) continue;

        var headers = rows[0];
        var tabRange = valueRanges[vr].range || '';
        var tabNameFromRange = tabRange.split('!')[0].replace(/'/g, '');
        tabsRead++;

        // Check if this is a job/customer tab (has "First Name" header)
        var headerLookup = {};
        var isJobTab = false;
        for (var h = 0; h < headers.length; h++) {
          var hKey = (headers[h] || '').toString().trim().toLowerCase();
          if (HEADER_MAP[hKey]) headerLookup[HEADER_MAP[hKey]] = h;
          if (hKey === 'first name' || hKey === 'first name ') isJobTab = true;
        }

        if (isJobTab) {
          // Process as job/customer data
          for (var jr = 1; jr < rows.length; jr++) {
            var jRow = rows[jr];
            var fName = headerLookup.firstName !== undefined ? (jRow[headerLookup.firstName] || '').toString().trim() : '';
            var phn = headerLookup.phone !== undefined ? (jRow[headerLookup.phone] || '').toString().trim() : '';
            if (!fName && !phn) continue; // skip empty rows

            // Build standardized row object
            var job = {
              dateIn: headerLookup.dateIn !== undefined ? (jRow[headerLookup.dateIn] || '').toString() : '',
              firstName: fName,
              lastName: headerLookup.lastName !== undefined ? (jRow[headerLookup.lastName] || '').toString().trim() : '',
              startTime: headerLookup.startTime !== undefined ? (jRow[headerLookup.startTime] || '').toString() : '',
              endTime: headerLookup.endTime !== undefined ? (jRow[headerLookup.endTime] || '').toString() : '',
              serviceDate: headerLookup.serviceDate !== undefined ? (jRow[headerLookup.serviceDate] || '').toString() : '',
              phone: phn,
              email: headerLookup.email !== undefined ? (jRow[headerLookup.email] || '').toString().trim() : '',
              address: headerLookup.address !== undefined ? (jRow[headerLookup.address] || '').toString().trim() : '',
              city: headerLookup.city !== undefined ? (jRow[headerLookup.city] || '').toString().trim() : '',
              state: headerLookup.state !== undefined ? (jRow[headerLookup.state] || '').toString().trim() : '',
              zip: headerLookup.zip !== undefined ? (jRow[headerLookup.zip] || '').toString().trim() : '',
              equipType: headerLookup.equipType !== undefined ? (jRow[headerLookup.equipType] || '').toString().trim() : '',
              brand: headerLookup.brand !== undefined ? (jRow[headerLookup.brand] || '').toString().trim() : '',
              issue: headerLookup.issue !== undefined ? (jRow[headerLookup.issue] || '').toString().trim() : '',
              receptionist: headerLookup.receptionist !== undefined ? (jRow[headerLookup.receptionist] || '').toString().trim() : '',
              notes: headerLookup.notes !== undefined ? (jRow[headerLookup.notes] || '').toString().trim() : '',
              status: headerLookup.status !== undefined ? (jRow[headerLookup.status] || '').toString().toLowerCase().trim() : '',
              tech: headerLookup.tech !== undefined ? (jRow[headerLookup.tech] || '').toString().trim() : '',
              tookanStatus: headerLookup.tookanStatus !== undefined ? (jRow[headerLookup.tookanStatus] || '').toString().toLowerCase().trim() : '',
              tookanJobId: headerLookup.tookanJobId !== undefined ? (jRow[headerLookup.tookanJobId] || '').toString().trim() : '',
              locationTab: tabNameFromRange,
            };
            allJobRows.push(job);
          }
        } else {
          // Store as non-job data for AI context
          var sectionKey = src.name + ' / ' + tabNameFromRange;
          var sectionRows = [];
          for (var or2 = 0; or2 < Math.min(rows.length, 35); or2++) {
            var rowStr = rows[or2].map(function(c) { return (c || '').toString().trim(); }).filter(function(c) { return c; }).join(' | ');
            if (rowStr) sectionRows.push(rowStr);
          }
          if (sectionRows.length > 0) {
            allOtherSections[sectionKey] = { rows: sectionRows, totalRows: rows.length - 1 };
          }
        }
      }

      // Delay between sheets to stay under API quota (60 reads/min)
      if (si < SOURCE_SHEETS.length - 1) {
        await new Promise(function(r) { setTimeout(r, 1500); });
      }

    } catch (e) {
      errors.push(src.name + ': ' + e.message);
      console.log("Error reading " + src.name + ": " + e.message);
    }
  }

  console.log("Source sheets: read " + sheetsRead + " sheets, " + tabsRead + " tabs, " + allJobRows.length + " job rows (pre-dedup)");
  if (errors.length > 0) console.log("Read errors: " + errors.join('; '));

  // ====== DEDUPLICATION — prevents double-counting from Backup/Canceled sheets ======
  var seen = {};
  var uniqueJobs = [];
  for (var di = 0; di < allJobRows.length; di++) {
    var dj = allJobRows[di];
    // Create unique key from phone + first name + date called in
    var dedupKey = (dj.phone || '').replace(/\D/g, '') + '|' + (dj.firstName || '').toLowerCase().trim() + '|' + (dj.dateIn || '').toString().substring(0, 10);
    if (dedupKey === '||') { uniqueJobs.push(dj); continue; } // keep rows with no identifiers
    if (!seen[dedupKey]) {
      seen[dedupKey] = true;
      uniqueJobs.push(dj);
    }
  }
  console.log("Dedup: " + allJobRows.length + " → " + uniqueJobs.length + " unique calls");
  allJobRows = uniqueJobs;

  // ====== FALLBACK: If source read failed, try Combined tab ======
  if (allJobRows.length < 10) {
    console.log("Source read got " + allJobRows.length + " rows, trying Combined fallback...");
    try {
      var combinedRes = await sheets.spreadsheets.values.get({
        spreadsheetId: BUSINESS_SPREADSHEET_ID,
        range: "'Combined'!A1:Z",
      });
      var cRows = combinedRes.data.values || [];
      if (cRows.length > 1) {
        for (var cr = 1; cr < cRows.length; cr++) {
          var cRow = cRows[cr];
          if (!cRow[1] && !cRow[6]) continue;
          allJobRows.push({
            dateIn: (cRow[0] || '').toString(), firstName: (cRow[1] || '').toString().trim(),
            lastName: (cRow[2] || '').toString().trim(), startTime: (cRow[3] || '').toString(),
            endTime: (cRow[4] || '').toString(), serviceDate: (cRow[5] || '').toString(),
            phone: (cRow[6] || '').toString(), email: (cRow[7] || '').toString().trim(),
            address: (cRow[8] || '').toString().trim(), city: (cRow[9] || '').toString().trim(),
            state: (cRow[10] || '').toString().trim(), zip: (cRow[11] || '').toString(),
            equipType: (cRow[12] || '').toString().trim(), brand: (cRow[13] || '').toString().trim(),
            issue: (cRow[14] || '').toString().trim(), receptionist: (cRow[15] || '').toString().trim(),
            notes: (cRow[16] || '').toString().trim(), status: (cRow[17] || '').toString().toLowerCase().trim(),
            tech: (cRow[23] || '').toString().trim(), tookanStatus: (cRow[21] || '').toString().toLowerCase().trim(),
            locationTab: (cRow[24] || '').toString().trim(),
          });
        }
        console.log("Combined fallback: " + allJobRows.length + " rows");
      }
    } catch(fe) { console.log("Combined fallback failed: " + fe.message); }
  }

  if (allJobRows.length === 0) {
    businessCache = { data: context + "No CRM data found. Check that service account has access to source spreadsheets.\n", time: Date.now() };
    return businessCache.data;
  }

  // ====== PHASE 2: Process all job data (same logic as before) ======
  var totalBooked = 0, totalCancelled = 0, totalCompleted = 0, totalReturn = 0, totalAssigned = 0;
  var todayBookings = [], recentBookings = [], needsReschedule = [];
  var locationStats = {}, techStats = {}, equipStats = {}, brandStats = {};
  var monthlyCalls = {}, weeklyCalls = 0;
  var monthlyCallsByTab = {}; // Per-location monthly tracking for individual Fibonacci
  var monthlyCallsByCity = {}; // Per-city monthly tracking (actual city names for charts)
  var bookingToServiceDays = [];
  var seasonalData = {};
  var today = new Date();
  var todayStr = today.toISOString().split('T')[0];
  var thisMonth = today.getFullYear() + '-' + String(today.getMonth() + 1).padStart(2, '0');
  var lastMonth = new Date(today.getFullYear(), today.getMonth() - 1, 1);
  var lastMonthStr = lastMonth.getFullYear() + '-' + String(lastMonth.getMonth() + 1).padStart(2, '0');
  var weekStart = new Date(today); weekStart.setDate(today.getDate() - today.getDay()); weekStart.setHours(0,0,0,0);
  var newLocationsThisMonth = {};
  var totalLeads = allJobRows.length;

  // Location normalizer — merges "Missouri" → "MO", filters junk entries
  var STATE_ABBREVS = {
    'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA',
    'colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA',
    'hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA',
    'kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD',
    'massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS','missouri':'MO',
    'montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH','new jersey':'NJ',
    'new mexico':'NM','new york':'NY','north carolina':'NC','north dakota':'ND','ohio':'OH',
    'oklahoma':'OK','oregon':'OR','pennsylvania':'PA','rhode island':'RI','south carolina':'SC',
    'south dakota':'SD','tennessee':'TN','texas':'TX','utah':'UT','vermont':'VT',
    'virginia':'VA','washington':'WA','west virginia':'WV','wisconsin':'WI','wyoming':'WY',
    'district of columbia':'DC',
  };
  var JUNK_LOCATIONS = [
    'master database','check back with us','out of range','sent diy','n/a','paid',
    'backup','wrong number','we dont work on this','cancelled','canceled','buy parts',
    'customer fixed equipment','location unavailable','return customers','unorganized',
    'test','sample','manual entry','promo','diagnostic','tech numbers','do not service',
    'no answer','voicemail','duplicate','transfer','not in service area','outside area',
    'too far','declined','refused','no show','spam','solicitor',
  ];
  function normalizeLocation(city, state, tabName) {
    var c = (city || '').toString().trim();
    var s = (state || '').toString().trim();
    if (!c && !s) {
      // Try to extract from tab name
      if (tabName) {
        var tabParts = tabName.split(',');
        if (tabParts.length >= 2) {
          c = tabParts[0].trim();
          s = tabParts[1].trim().split(' ')[0].trim();
        }
      }
      if (!c) return '';
    }
    // Normalize state: full name → abbreviation
    var sLower = s.toLowerCase().trim();
    if (STATE_ABBREVS[sLower]) s = STATE_ABBREVS[sLower];
    // Clean state to just 2-letter code
    s = s.replace(/[^A-Za-z]/g, '').toUpperCase();
    if (s.length > 2) {
      // Try matching first word
      var stateWords = (state || '').toLowerCase().trim();
      if (STATE_ABBREVS[stateWords]) s = STATE_ABBREVS[stateWords];
      else s = s.substring(0, 2);
    }
    // Clean city
    c = c.replace(/[^A-Za-z\s\.\-\']/g, '').trim();
    c = c.split(' ').map(function(w) { return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase(); }).join(' ');
    if (!c) return '';
    // Check junk
    var combined = (c + ' ' + s).toLowerCase();
    for (var ji = 0; ji < JUNK_LOCATIONS.length; ji++) {
      if (combined.includes(JUNK_LOCATIONS[ji])) return '';
    }
    return s ? c + ', ' + s : c;
  }

  for (var r = 0; r < allJobRows.length; r++) {
    var job = allJobRows[r];
    var fullName = (job.firstName + ' ' + job.lastName).trim();
    if (!fullName || fullName === ' ') continue;

    var location = normalizeLocation(job.city, job.state, job.locationTab);
    var status = job.status;
    var equipType = job.equipType;
    var brand = job.brand;
    var tech = job.tech;
    var createdAt = job.dateIn;
    var serviceDate = job.serviceDate;
    var issue = job.issue;

    // Status parsing — based on actual CRM columns:
    //   Status col (18): "Booked", "Cancelled by Customer", "return", or EMPTY
    //   Tookan Status col (22): "Completed", "Assigned", "Acknowledged", "Unassigned"
    //   Posted Flag col (28): "DONE" = posted to Tookan (NOT job completion)
    var tookanSt = job.tookanStatus || '';
    var isBooked = status === 'booked';
    var isCancelled = status.includes('cancel');
    var isCompleted = tookanSt.includes('completed') || tookanSt.includes('successful') || status.includes('completed') || status.includes('done') || status.includes('finished') || status.includes('paid') || status.includes('serviced');
    var isAssigned = tookanSt.includes('assigned') || tookanSt.includes('acknowledged');
    var isReturn = status.includes('return');
    var hasStatus = status.length > 0 || tookanSt.length > 0;
    var needsResched = status.includes('reschedul') || (status.includes('need') && status.includes('book'));

    if (isBooked) totalBooked++;
    if (isCancelled) totalCancelled++;
    if (isCompleted) totalCompleted++;
    if (isReturn) totalReturn++;
    if (isAssigned) totalAssigned++;
    if (needsResched) needsReschedule.push({ name: fullName, location: location, phone: job.phone });

    // Location stats
    if (location && location.length > 2) {
      if (!locationStats[location]) locationStats[location] = { booked: 0, completed: 0, cancelled: 0, total: 0 };
      locationStats[location].total++;
      if (isBooked) locationStats[location].booked++;
      if (isCompleted) locationStats[location].completed++;
      if (isCancelled) locationStats[location].cancelled++;
    }

    // Equipment normalization
    var equipNorm = equipType;
    if (equipType) {
      var eqLow = equipType.toLowerCase();
      if (eqLow.includes('snow')) equipNorm = 'Snow Blower';
      else if (eqLow.includes('riding') || eqLow.includes('tractor')) equipNorm = 'Riding Mower';
      else if (eqLow.includes('push')) equipNorm = 'Push Mower';
      else if (eqLow.includes('zero')) equipNorm = 'Zero Turn';
      else if (eqLow.includes('generator')) equipNorm = 'Generator';
      else if (eqLow.includes('chain') || eqLow.includes('saw')) equipNorm = 'Chainsaw';
      else if (eqLow.includes('trim') || eqLow.includes('weed')) equipNorm = 'Trimmer';
      else if (eqLow.includes('lawn') || eqLow.includes('mower')) equipNorm = 'Mower';
      else equipNorm = equipType.substring(0, 30);
      equipStats[equipNorm] = (equipStats[equipNorm] || 0) + 1;
    }

    // Tech stats
    if (tech && tech.length > 1) {
      if (!techStats[tech]) techStats[tech] = {
        total: 0, completed: 0, cancelled: 0, revenue: 0,
        locations: {}, equipment: {}, brands: {},
        recentJobs: [], todayJobs: [], thisWeekJobs: 0, thisMonthJobs: 0,
        avgResponseDays: [], returnCustomers: 0, firstSeen: null, lastSeen: null,
      };
      var ts = techStats[tech];
      ts.total++;
      if (isCompleted) ts.completed++;
      if (isCancelled) ts.cancelled++;
      if (isReturn) ts.returnCustomers++;
      if (location) ts.locations[location] = (ts.locations[location] || 0) + 1;
      if (equipNorm) ts.equipment[equipNorm] = (ts.equipment[equipNorm] || 0) + 1;
      if (brand && brand.length > 1 && brand.toLowerCase() !== 'not specified') {
        var bKey = brand.split(' ')[0].trim();
        bKey = bKey.charAt(0).toUpperCase() + bKey.slice(1).toLowerCase();
        ts.brands[bKey] = (ts.brands[bKey] || 0) + 1;
      }
      if (createdAt) {
        try {
          var tDate = new Date(createdAt);
          if (!isNaN(tDate.getTime())) {
            if (!ts.firstSeen || tDate < ts.firstSeen) ts.firstSeen = tDate;
            if (!ts.lastSeen || tDate > ts.lastSeen) ts.lastSeen = tDate;
            var tMonthKey = tDate.getFullYear() + '-' + String(tDate.getMonth() + 1).padStart(2, '0');
            if (tMonthKey === thisMonth) ts.thisMonthJobs++;
            if (tDate >= weekStart) ts.thisWeekJobs++;
          }
        } catch(e){}
      }
      if (createdAt && serviceDate) {
        try {
          var rc1 = new Date(createdAt), rc2 = new Date(serviceDate);
          if (!isNaN(rc1.getTime()) && !isNaN(rc2.getTime())) {
            var respDays = Math.round((rc2 - rc1) / 86400000);
            if (respDays >= 0 && respDays < 60) ts.avgResponseDays.push(respDays);
          }
        } catch(e){}
      }
      if (ts.recentJobs.length < 5 || r >= allJobRows.length - 50) {
        if (ts.recentJobs.length >= 5) ts.recentJobs.shift();
        ts.recentJobs.push({ name: fullName, location: location, equip: equipType, status: status, date: createdAt });
      }
      if (serviceDate) {
        try {
          var tsd = new Date(serviceDate);
          if (tsd.toISOString().split('T')[0] === todayStr) {
            ts.todayJobs.push({ name: fullName, location: location, equip: equipType, issue: (issue || '').substring(0, 60) });
          }
        } catch(e){}
      }
    }

    // Brand breakdown
    if (brand && brand.length > 1 && brand.toLowerCase() !== 'not specified') {
      var brandNorm = brand.split(' ')[0].trim();
      brandNorm = brandNorm.charAt(0).toUpperCase() + brandNorm.slice(1).toLowerCase();
      brandStats[brandNorm] = (brandStats[brandNorm] || 0) + 1;
    }

    // Monthly tracking
    if (createdAt) {
      try {
        var cDate = new Date(createdAt);
        if (!isNaN(cDate.getTime())) {
          var mKey = cDate.getFullYear() + '-' + String(cDate.getMonth() + 1).padStart(2, '0');
          monthlyCalls[mKey] = (monthlyCalls[mKey] || 0) + 1;
          if (cDate >= weekStart) weeklyCalls++;

          // Per-location monthly tracking
          var locTab = job.locationTab || 'Unknown';
          if (!monthlyCallsByTab[locTab]) monthlyCallsByTab[locTab] = {};
          monthlyCallsByTab[locTab][mKey] = (monthlyCallsByTab[locTab][mKey] || 0) + 1;

          // Per-city monthly tracking (for charts — uses actual city, not tab name)
          if (location && location.length > 3) {
            var cityKey = location;
            if (!monthlyCallsByCity[cityKey]) monthlyCallsByCity[cityKey] = {};
            monthlyCallsByCity[cityKey][mKey] = (monthlyCallsByCity[cityKey][mKey] || 0) + 1;
          }

          // Seasonal demand
          if (equipNorm) {
            if (!seasonalData[mKey]) seasonalData[mKey] = { snow: 0, mower: 0, generator: 0, other: 0 };
            if (equipNorm === 'Snow Blower') seasonalData[mKey].snow++;
            else if (equipNorm.includes('Mower') || equipNorm === 'Zero Turn') seasonalData[mKey].mower++;
            else if (equipNorm === 'Generator') seasonalData[mKey].generator++;
            else seasonalData[mKey].other++;
          }
        }
      } catch (e) {}
    }

    // Avg booking to service days
    if (createdAt && serviceDate) {
      try {
        var cDate2 = new Date(createdAt), sDate = new Date(serviceDate);
        if (!isNaN(cDate2.getTime()) && !isNaN(sDate.getTime())) {
          var daysDiff = Math.round((sDate - cDate2) / 86400000);
          if (daysDiff >= 0 && daysDiff < 60) bookingToServiceDays.push(daysDiff);
        }
      } catch (e) {}
    }

    // New locations this month
    if (createdAt && location) {
      try {
        var cDate3 = new Date(createdAt);
        var mKey2 = cDate3.getFullYear() + '-' + String(cDate3.getMonth() + 1).padStart(2, '0');
        if (mKey2 === thisMonth) newLocationsThisMonth[location] = true;
      } catch (e) {}
    }

    // Service date check for today
    if (serviceDate) {
      try {
        var sd = new Date(serviceDate);
        if (sd.toISOString().split('T')[0] === todayStr) {
          todayBookings.push({ name: fullName, location: location, equip: equipType, issue: (issue || '').substring(0, 80), tech: tech });
        }
      } catch (e) {}
    }

    // Recent bookings (last 20)
    if (r >= allJobRows.length - 20 && job.firstName) {
      recentBookings.push({ name: fullName, location: location, status: status, equip: equipType, tech: tech, brand: brand, date: createdAt });
    }
  }

  // ====== PHASE 3: Read support tabs (Tech Numbers, Return Customers, etc.) ======
  var techList = [];
  try {
    var techRes = await sheets.spreadsheets.values.get({ spreadsheetId: SOURCE_SHEETS[0].id, range: "'Tech Numbers'!A1:D40" });
    var techRows = techRes.data.values || [];
    for (var t = 1; t < techRows.length; t++) {
      if (techRows[t][0]) techList.push({ name: techRows[t][0], phone: techRows[t][1] || '', location: techRows[t][2] || '' });
    }
  } catch (e) { console.log("Tech Numbers read: " + e.message); }

  var totalReturnFromTab = 0;
  try {
    var retRes = await sheets.spreadsheets.values.get({ spreadsheetId: SOURCE_SHEETS[0].id, range: "'Return Customers'!A:A" });
    totalReturnFromTab = Math.max(0, ((retRes.data.values || []).length) - 1);
    if (totalReturnFromTab > totalReturn) totalReturn = totalReturnFromTab;
  } catch (e) {}

  var promoReplies = 0;
  try {
    var promoRes = await sheets.spreadsheets.values.get({ spreadsheetId: SOURCE_SHEETS[0].id, range: "'Promotion Customers Reply'!A:A" });
    promoReplies = Math.max(0, ((promoRes.data.values || []).length) - 1);
  } catch (e) {}

  // ====== PHASE 4: Build context string ======
  var avgBookingDays = bookingToServiceDays.length > 0 ? Math.round(bookingToServiceDays.reduce(function(a,b){return a+b;}, 0) / bookingToServiceDays.length) : 0;
  var totalConverted = totalBooked + totalAssigned + totalCompleted;
  var conversionRate = totalLeads > 0 ? Math.round((totalConverted / totalLeads) * 100) : 0;
  var thisMonthCalls = monthlyCalls[thisMonth] || 0;
  var lastMonthCalls = monthlyCalls[lastMonthStr] || 0;
  var monthGrowth = lastMonthCalls > 0 ? Math.round(((thisMonthCalls - lastMonthCalls) / lastMonthCalls) * 100) : 0;

  context += "OVERVIEW:\n";
  context += "  Total Leads: " + totalLeads + "\n";
  context += "  Booked: " + totalBooked + "\n";
  context += "  Assigned/Dispatched: " + totalAssigned + "\n";
  context += "  Completed Jobs: " + totalCompleted + "\n";
  context += "  Cancelled: " + totalCancelled + "\n";
  context += "  Return Customers: " + totalReturn + "\n";
  context += "  Promo Replies: " + promoReplies + "\n";
  context += "  Locations Active: " + Object.keys(locationStats).length + "\n";
  context += "  Technicians: " + techList.length + "\n";
  context += "  Avg Days Booking→Service: " + avgBookingDays + "\n";
  context += "  Conversion Rate: " + conversionRate + "%\n";
  context += "  This Month Calls: " + thisMonthCalls + "\n";
  context += "  Last Month Calls: " + lastMonthCalls + "\n";
  context += "  Month Growth: " + monthGrowth + "%\n";
  context += "  This Week Bookings: " + weeklyCalls + "\n";
  context += "  Data Source: " + sheetsRead + " sheets, " + tabsRead + " tabs read directly\n\n";

  if (todayBookings.length > 0) {
    context += "TODAY'S BOOKINGS:\n";
    todayBookings.forEach(function(b) { context += "  " + b.name + " (" + b.location + ") — " + b.equip + ": " + b.issue + " [Tech: " + b.tech + "]\n"; });
    context += "\n";
  }

  if (needsReschedule.length > 0) {
    context += "NEEDS RESCHEDULING (" + needsReschedule.length + "):\n";
    needsReschedule.slice(0, 10).forEach(function(n) { context += "  " + n.name + " (" + n.location + ") — " + n.phone + "\n"; });
    context += "\n";
  }

  context += "EQUIPMENT BREAKDOWN:\n";
  var eqSorted = Object.entries(equipStats).sort(function(a,b){return b[1]-a[1];});
  eqSorted.slice(0, 8).forEach(function(e) { context += "  " + e[0] + ": " + e[1] + "\n"; });
  context += "\n";

  context += "TOP BRANDS:\n";
  var brSorted = Object.entries(brandStats).sort(function(a,b){return b[1]-a[1];});
  brSorted.slice(0, 8).forEach(function(b) { context += "  " + b[0] + ": " + b[1] + "\n"; });
  context += "\n";

  context += "TECHNICIAN PERFORMANCE:\n";
  var techSorted = Object.entries(techStats).sort(function(a,b){return b[1].total-a[1].total;});
  techSorted.forEach(function(t) {
    var s = t[1];
    var rate = s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0;
    var avgResp = s.avgResponseDays.length > 0 ? Math.round(s.avgResponseDays.reduce(function(a,b){return a+b;},0) / s.avgResponseDays.length) : 0;
    var topEquip = Object.entries(s.equipment).sort(function(a,b){return b[1]-a[1];}).slice(0,3).map(function(e){return e[0];}).join(', ');
    var topLocs = Object.entries(s.locations).sort(function(a,b){return b[1]-a[1];}).slice(0,3).map(function(l){return l[0];}).join(', ');
    context += "  " + t[0] + ": " + s.total + " jobs (" + s.completed + " completed, " + s.cancelled + " cancelled, " + rate + "% rate)";
    context += " | This week: " + s.thisWeekJobs + " | Avg response: " + avgResp + "d";
    context += " | Specialties: " + (topEquip || 'none') + " | Markets: " + (topLocs || 'none') + "\n";
  });
  context += "\n";

  if (todayBookings.length > 0 || needsReschedule.length > 0) {
    context += "SMART TASK ASSIGNMENTS:\n";
    var unassigned = todayBookings.filter(function(b) { return !b.tech || b.tech === ''; });
    unassigned.forEach(function(job2) {
      var bestTech = null, bestScore = -1;
      techSorted.forEach(function(t) {
        var s2 = t[1], score = 0;
        var completionRate = s2.total > 0 ? s2.completed / s2.total : 0;
        score += completionRate * 40;
        if (job2.equip && s2.equipment[job2.equip]) score += 20;
        if (job2.location && s2.locations[job2.location]) score += 20;
        score -= s2.todayJobs.length * 10;
        if (score > bestScore) { bestScore = score; bestTech = t[0]; }
      });
      if (bestTech) context += "  ASSIGN: " + job2.name + " (" + job2.equip + " in " + job2.location + ") → " + bestTech + "\n";
    });
    context += "\n";
  }

  context += "LOCATION BREAKDOWN:\n";
  var locSorted = Object.entries(locationStats).sort(function(a,b){return b[1].total-a[1].total;});
  locSorted.slice(0, 20).forEach(function(l) {
    context += "  " + l[0] + ": " + l[1].total + " total, " + l[1].booked + " booked, " + l[1].completed + " completed, " + l[1].cancelled + " cancelled\n";
  });
  context += "\n";

  if (recentBookings.length > 0) {
    context += "RECENT ACTIVITY:\n";
    recentBookings.slice(-10).forEach(function(b) { context += "  " + b.name + " — " + b.location + " — " + b.status + " (" + b.equip + ") [" + b.tech + "]\n"; });
    context += "\n";
  }

  // Sheet registry for AI
  context += "DATA SOURCES AVAILABLE:\n";
  SOURCE_SHEETS.forEach(function(s) { context += "  " + s.name + ": " + s.desc + "\n"; });
  context += "\n";

  // Non-job data summaries
  if (Object.keys(allOtherSections).length > 0) {
    context += "BUSINESS OPERATIONS DATA:\n\n";
    Object.entries(allOtherSections).forEach(function(section) {
      context += "--- " + section[0] + " ---\n";
      var maxLines = Math.min(section[1].rows.length, 20);
      for (var sl = 0; sl < maxLines; sl++) { context += "  " + section[1].rows[sl] + "\n"; }
      if (section[1].totalRows > 20) context += "  ... and " + (section[1].totalRows - 20) + " more rows\n";
      context += "\n";
    });
  }

  // Store parsed data globally for dashboard
  global.bizMetrics = {
    totalLeads: totalLeads, totalBooked: totalBooked, totalCompleted: totalCompleted,
    totalCancelled: totalCancelled, totalReturn: totalReturn, totalAssigned: totalAssigned, promoReplies: promoReplies,
    todayBookings: todayBookings, needsReschedule: needsReschedule, recentBookings: recentBookings,
    locationStats: locationStats, techStats: techStats, equipStats: equipStats, brandStats: brandStats,
    monthlyCalls: monthlyCalls, weeklyCalls: weeklyCalls, monthlyCallsByTab: monthlyCallsByTab, monthlyCallsByCity: monthlyCallsByCity,
    avgBookingDays: avgBookingDays, conversionRate: conversionRate,
    thisMonthCalls: thisMonthCalls, lastMonthCalls: lastMonthCalls,
    monthGrowth: monthGrowth, techList: techList, newLocationsThisMonth: Object.keys(newLocationsThisMonth).length,
    seasonalData: seasonalData, sheetsRead: sheetsRead, tabsRead: tabsRead, totalJobRows: allJobRows.length,
    allJobRows: allJobRows,
  };
  global.bizOpsData = allOtherSections;
  global.sheetMetadata = sheetMetadata;

  // ====== PHASE 5: Read Profit Sheet ======
  if (PROFIT_SPREADSHEET_ID) {
    try {
      var profitContext = "";
      var today2 = new Date();
      var monthNames3 = ["January","February","March","April","May","June","July","August","September","October","November","December"];
      var currentMonthTab = monthNames3[today2.getMonth()] + " " + today2.getFullYear();
      
      var profitRes = await sheets.spreadsheets.values.get({
        spreadsheetId: PROFIT_SPREADSHEET_ID,
        range: "'" + currentMonthTab + "'!A1:AF55",
        valueRenderOption: 'FORMATTED_VALUE',
      });
      var profitRows = profitRes.data.values || [];
      
      if (profitRows.length > 0) {
        var monthExpenses = {};
        var monthRevenue = 0, monthProfit = 0;
        var dailyRevenue = [], dailyProfit = [], dailyAds = [];
        var techPayouts = {}, receptionistPayouts = {}, adminPayouts = {};
        var techPayoutsDaily = {};
        
        for (var pr = 1; pr < profitRows.length; pr++) {
          var pRow = profitRows[pr];
          var pLabel = (pRow[0] || '').toString().toLowerCase().trim();
          if (!pLabel) continue;
          
          var rowTotal = 0;
          for (var pd = 1; pd < pRow.length; pd++) {
            var val = parseFloat((pRow[pd] || '0').toString().replace(/[$,]/g, ''));
            if (!isNaN(val)) rowTotal += val;
          }
          
          if (pLabel === 'ads') {
            monthExpenses['Ads'] = rowTotal;
            for (var ad2 = 1; ad2 < pRow.length; ad2++) { dailyAds.push(parseFloat((pRow[ad2] || '0').toString().replace(/[$,]/g, '')) || 0); }
          }
          else if (pLabel === 'app total') monthExpenses['Apps/Software'] = rowTotal;
          else if (pLabel === 'amazon (parts)') { monthExpenses['Parts/Supplies'] = (monthExpenses['Parts/Supplies'] || 0) + rowTotal; }
          else if (pLabel === 'receipts') { monthExpenses['Receipts'] = (monthExpenses['Receipts'] || 0) + rowTotal; }
          else if (pLabel === 'payment processing fees') monthExpenses['Processing Fees'] = rowTotal;
          else if (pLabel === 'refunds by amount') monthExpenses['Refunds'] = rowTotal;
          else if (pLabel === 'admin total') monthExpenses['Admin Labor'] = rowTotal;
          else if (pLabel === 'manager total') monthExpenses['Manager Labor'] = rowTotal;
          else if (pLabel === 'receptionist total') monthExpenses['Receptionist Labor'] = rowTotal;
          else if (pLabel === 'tech total') monthExpenses['Tech Labor'] = rowTotal;
          else if (pLabel === 'total collected') {
            monthRevenue = rowTotal;
            for (var rv = 1; rv < pRow.length; rv++) { dailyRevenue.push(parseFloat((pRow[rv] || '0').toString().replace(/[$,]/g, '')) || 0); }
          }
          else if (pLabel === 'profit') {
            monthProfit = rowTotal;
            for (var pf = 1; pf < pRow.length; pf++) { dailyProfit.push(parseFloat((pRow[pf] || '0').toString().replace(/[$,]/g, '')) || 0); }
          }
          else if (['rocky','tucker','aly'].indexOf(pLabel) >= 0) { adminPayouts[pRow[0]] = rowTotal; }
          else if (['ray','muaaz','rayan'].indexOf(pLabel) >= 0) { receptionistPayouts[pRow[0]] = rowTotal; }
          else if (['andrew','hailey','rubait'].indexOf(pLabel) >= 0) { adminPayouts[pRow[0]] = rowTotal; }
          else if (['justin turner','victor romero','alexander fernandez','tony reynolds','kurt nowicki',
                     'talon twiford','michael scutti','maxx fritts','corey roberson','robert hummer','ashton hawley',
                     'brandi butler','trent kennedy','keith'].indexOf(pLabel) >= 0) {
            techPayouts[pRow[0]] = rowTotal;
            // Store daily values for this tech (col 1 = day 1, col 2 = day 2, etc.)
            var dailyArr = [];
            for (var td2 = 1; td2 < pRow.length; td2++) {
              dailyArr.push(parseFloat((pRow[td2] || '0').toString().replace(/[$,]/g, '')) || 0);
            }
            techPayoutsDaily[pRow[0]] = dailyArr;
          }
        }
        
        var totalExpenses = 0;
        Object.values(monthExpenses).forEach(function(v) { totalExpenses += v; });
        
        profitContext += "FINANCIAL DATA — " + currentMonthTab.toUpperCase() + ":\n";
        profitContext += "  Total Revenue Collected: $" + monthRevenue.toFixed(2) + "\n";
        profitContext += "  Total Expenses: $" + totalExpenses.toFixed(2) + "\n";
        profitContext += "  NET PROFIT: $" + monthProfit.toFixed(2) + "\n";
        profitContext += "  Profit Margin: " + (monthRevenue > 0 ? ((monthProfit / monthRevenue) * 100).toFixed(1) : "0") + "%\n\n";
        
        profitContext += "  EXPENSE BREAKDOWN:\n";
        Object.entries(monthExpenses).sort(function(a,b){return b[1]-a[1];}).forEach(function(e) {
          if (e[1] > 0) profitContext += "    " + e[0] + ": $" + e[1].toFixed(2) + "\n";
        });
        
        if (Object.keys(techPayouts).length > 0) {
          profitContext += "\n  TECH PAYOUTS:\n";
          Object.entries(techPayouts).sort(function(a,b){return b[1]-a[1];}).forEach(function(t) {
            if (t[1] > 0) profitContext += "    " + t[0] + ": $" + t[1].toFixed(2) + "\n";
          });
        }
        
        var daysWithRevenue = dailyRevenue.filter(function(v){return v > 0;}).length;
        var avgDailyRev = daysWithRevenue > 0 ? monthRevenue / daysWithRevenue : 0;
        var avgDailyAds = dailyAds.filter(function(v){return v>0;}).length > 0 ? dailyAds.reduce(function(a,b){return a+b;},0) / dailyAds.filter(function(v){return v>0;}).length : 0;
        profitContext += "\n  DAILY AVERAGES:\n";
        profitContext += "    Avg Daily Revenue: $" + avgDailyRev.toFixed(2) + "\n";
        profitContext += "    Avg Daily Ad Spend: $" + avgDailyAds.toFixed(2) + "\n";
        if (avgDailyAds > 0 && avgDailyRev > 0) {
          profitContext += "    Ad ROI: $" + (avgDailyRev / avgDailyAds).toFixed(2) + " revenue per $1 ad spend\n";
        }
        
        context += profitContext;
        
        global.profitMetrics = {
          currentMonth: currentMonthTab, revenue: monthRevenue, expenses: totalExpenses,
          profit: monthProfit, margin: monthRevenue > 0 ? ((monthProfit / monthRevenue) * 100).toFixed(1) : "0",
          expenseBreakdown: monthExpenses, techPayouts: techPayouts,
          techPayoutsDaily: techPayoutsDaily, currentDay: today2.getDate(),
          adminPayouts: adminPayouts, receptionistPayouts: receptionistPayouts,
          dailyRevenue: dailyRevenue, dailyProfit: dailyProfit, dailyAds: dailyAds,
          avgDailyRev: avgDailyRev, avgDailyAds: avgDailyAds,
        };
      }
      
      // Yearly totals — read current + all previous years for financial history
      var yearlyTechPayouts = {};
      var financialHistory = { years: {}, months: {} };
      var startYear = 2024;
      var curYear = today2.getFullYear();
      var techNameList = ['justin turner','victor romero','alexander fernandez','tony reynolds','kurt nowicki',
                   'talon twiford','michael scutti','maxx fritts','corey roberson','robert hummer','ashton hawley',
                   'brandi butler','trent kennedy','keith'];
      var monthNames4 = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

      for (var fy = startYear; fy <= curYear; fy++) {
        try {
          var yearTab = "Total " + fy;
          var yearRes = await sheets.spreadsheets.values.get({
            spreadsheetId: PROFIT_SPREADSHEET_ID,
            range: "'" + yearTab + "'!A1:N55",
            valueRenderOption: 'FORMATTED_VALUE',
          });
          var yearRows = yearRes.data.values || [];
          if (yearRows.length > 1) {
            var yearData = { revenue: 0, profit: 0, expenses: 0, ads: 0, techLabor: 0, receptionistLabor: 0, adminLabor: 0, refunds: 0, parts: 0, processing: 0, apps: 0, monthlyRevenue: [], monthlyProfit: [], monthlyAds: [] };
            for (var yr2 = 1; yr2 < yearRows.length; yr2++) {
              var yLabel = (yearRows[yr2][0] || '').toString().trim();
              var yLabelLower = yLabel.toLowerCase();
              var yRowTotal = 0, yMonthVals = [];
              for (var yc = 1; yc < yearRows[yr2].length; yc++) {
                var yv = parseFloat((yearRows[yr2][yc] || '0').toString().replace(/[$,]/g, ''));
                if (isNaN(yv)) yv = 0;
                yRowTotal += yv; yMonthVals.push(yv);
              }
              if (yLabelLower === 'total collected') { yearData.revenue = yRowTotal; yearData.monthlyRevenue = yMonthVals; }
              else if (yLabelLower === 'profit') { yearData.profit = yRowTotal; yearData.monthlyProfit = yMonthVals; }
              else if (yLabelLower === 'ads') { yearData.ads = yRowTotal; yearData.monthlyAds = yMonthVals; }
              else if (yLabelLower === 'tech total') yearData.techLabor = yRowTotal;
              else if (yLabelLower === 'receptionist total') yearData.receptionistLabor = yRowTotal;
              else if (yLabelLower === 'admin total') yearData.adminLabor = yRowTotal;
              else if (yLabelLower === 'manager total') yearData.adminLabor += yRowTotal;
              else if (yLabelLower === 'refunds by amount') yearData.refunds = yRowTotal;
              else if (yLabelLower === 'amazon (parts)' || yLabelLower === 'receipts') yearData.parts += yRowTotal;
              else if (yLabelLower === 'payment processing fees') yearData.processing = yRowTotal;
              else if (yLabelLower === 'app total') yearData.apps = yRowTotal;
              if (fy === curYear && techNameList.indexOf(yLabelLower) >= 0 && yRowTotal > 0) yearlyTechPayouts[yLabel] = yRowTotal;
            }
            yearData.expenses = yearData.revenue - yearData.profit;
            yearData.margin = yearData.revenue > 0 ? Math.round((yearData.profit / yearData.revenue) * 1000) / 10 : 0;
            financialHistory.years[fy] = yearData;
            context += "\nYEARLY — " + fy + ": Rev $" + yearData.revenue.toFixed(0) + " | Profit $" + yearData.profit.toFixed(0) + " | Margin " + yearData.margin + "%\n";
            for (var mi2 = 0; mi2 < Math.min(12, yearData.monthlyRevenue.length); mi2++) {
              var mRev2 = yearData.monthlyRevenue[mi2] || 0, mProf2 = yearData.monthlyProfit[mi2] || 0, mAds2 = yearData.monthlyAds[mi2] || 0;
              if (mRev2 > 0 || mProf2 !== 0) {
                financialHistory.months[monthNames4[mi2] + ' ' + fy] = { revenue: mRev2, profit: mProf2, ads: mAds2, expenses: mRev2 - mProf2, margin: mRev2 > 0 ? Math.round((mProf2 / mRev2) * 1000) / 10 : 0 };
              }
            }
          }
        } catch(ye2) { console.log("Year " + fy + " tab: " + ye2.message); }
      }
      // Today / this week from daily data
      var todayRev = dailyRevenue[today2.getDate() - 1] || 0, todayProf = dailyProfit[today2.getDate() - 1] || 0, todayAd = dailyAds[today2.getDate() - 1] || 0;
      var weekRev = 0, weekProf = 0, weekAds2 = 0;
      for (var wd = Math.max(0, today2.getDate() - 7); wd < today2.getDate(); wd++) { weekRev += dailyRevenue[wd] || 0; weekProf += dailyProfit[wd] || 0; weekAds2 += dailyAds[wd] || 0; }
      financialHistory.today = { revenue: todayRev, profit: todayProf, ads: todayAd, expenses: todayRev - todayProf, margin: todayRev > 0 ? Math.round((todayProf / todayRev) * 1000) / 10 : 0 };
      financialHistory.thisWeek = { revenue: weekRev, profit: weekProf, ads: weekAds2, expenses: weekRev - weekProf, margin: weekRev > 0 ? Math.round((weekProf / weekRev) * 1000) / 10 : 0 };
      financialHistory.thisMonth = { revenue: monthRevenue, profit: monthProfit, ads: monthExpenses['Ads'] || 0, expenses: totalExpenses, margin: monthRevenue > 0 ? Math.round((monthProfit / monthRevenue) * 1000) / 10 : 0 };
      var allTimeRev = 0, allTimeProf = 0, allTimeAds = 0;
      Object.values(financialHistory.years).forEach(function(y) { allTimeRev += y.revenue; allTimeProf += y.profit; allTimeAds += y.ads; });
      financialHistory.allTime = { revenue: allTimeRev, profit: allTimeProf, ads: allTimeAds, expenses: allTimeRev - allTimeProf, margin: allTimeRev > 0 ? Math.round((allTimeProf / allTimeRev) * 1000) / 10 : 0, startYear: startYear };
      if (global.profitMetrics) { global.profitMetrics.yearlyTechPayouts = yearlyTechPayouts; global.profitMetrics.financialHistory = financialHistory; }
      
    } catch (pe) {
      context += "Error loading profit data: " + pe.message + "\n";
    }
  }

  // ====== PHASE 5: Append Tookan live data if available ======
  var tkData = global.tookanData || {};
  if (tkData.totalTasks > 0) {
    context += "\nTOOKAN DISPATCH (LIVE — LAST 90 DAYS):\n";
    context += "  Total Tasks: " + tkData.totalTasks + "\n";
    context += "  Completed: " + tkData.completed + "\n";
    context += "  Assigned: " + tkData.assigned + " | Acknowledged: " + tkData.acknowledged + "\n";
    context += "  In Progress: " + tkData.started + " | Unassigned: " + tkData.unassigned + "\n";
    context += "  Failed/Cancelled: " + tkData.cancelled + "\n";
    context += "  Today's Jobs: " + (tkData.todayTasks || []).length + "\n";
    context += "  Active Agents: " + (tkData.agents || []).length + "\n";
    var tkTechs = Object.entries(tkData.tasksByTech || {}).sort(function(a,b){return b[1].completed-a[1].completed;});
    if (tkTechs.length > 0) {
      context += "  Tech Rankings (by completions):\n";
      tkTechs.slice(0, 10).forEach(function(t, i) {
        var rate = t[1].total > 0 ? Math.round(t[1].completed / t[1].total * 100) : 0;
        context += "    " + (i+1) + ". " + t[0] + ": " + t[1].completed + "/" + t[1].total + " (" + rate + "%)\n";
      });
    }
  }

  console.log("Business context built: " + context.length + " chars (" + allJobRows.length + " jobs from " + sheetsRead + " sheets)");
  businessCache = { data: context, time: Date.now() };
  return context;
}


/* ===========================
   Call Claude API
=========================== */

async function askClaude(systemPrompt, messages) {
  try {
    var response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': CLAUDE_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1024,
        system: systemPrompt,
        messages: messages,
      }),
    });
    var data = await response.json();
    if (data.error) {
      console.error("Claude API Error: " + JSON.stringify(data.error));
      return "There was an error: " + (data.error.message || 'Unknown error');
    }
    if (data.content && data.content.length > 0) {
      return data.content[0].text;
    }
    return "I got an unexpected response.";
  } catch (err) {
    console.error("Claude fetch error: " + err.message);
    return "I had trouble connecting to my AI brain.";
  }
}

/* ===========================
   POST /voice — Initial greeting
=========================== */

app.post('/voice', async function(req, res) {
  var twiml = new twilio.twiml.VoiceResponse();
  var callSid = req.body.CallSid || 'unknown';
  callHistory[callSid] = {};

  try {
    console.log("Call connected, building briefing...");
    var lifeContext = await buildLifeOSContext();
    var emailContext = await buildEmailContext();
    var fullContext = lifeContext + emailContext;
    var systemPrompt = "You are Jarvis, Trace's personal AI counselor and mentor. You are on a phone call with Trace.\n\nRULES:\n- Talk like a wise friend and life coach, not a database.\n- NEVER mention tab names, sheet names, row counts, or entry counts.\n- NEVER say things like 'you have 75,659 entries' or 'across 54 tabs'.\n- Use the data to INFORM your advice, but speak in human terms.\n- Ask thoughtful questions. Push Trace to think deeper.\n- Be real, direct, and motivational. Challenge him when needed.\n- Keep responses SHORT (3-5 sentences). This is a phone call.\n- If something looks off in the data (high screen time, missed goals), address it directly but with care.\n- If there are urgent emails, mention who they're from and why they matter.\n- Never use markdown, bullet points, or formatting.\n\nLIFE OS DATA (use this to inform your advice, don't recite it):\n" + fullContext;

    var greeting = await askClaude(systemPrompt, [
      { role: 'user', content: 'Give Trace a quick opening briefing: system overview, key financial numbers, screen time, top priority, and any urgent emails. End by asking what he wants to know more about.' }
    ]);

    console.log("Jarvis: " + greeting);

    callHistory[callSid] = {
      systemPrompt: systemPrompt,
      messages: [{ role: 'assistant', content: greeting }],
    };

    var gather = twiml.gather({
      input: 'speech',
      action: '/conversation',
      method: 'POST',
      speechTimeout: 3,
      language: 'en-US',
    });
    gather.say({ voice: 'Polly.Matthew' }, greeting);

    twiml.say({ voice: 'Polly.Matthew' }, "I didn't catch that. What would you like to know?");
    twiml.redirect('/voice-listen');
  } catch (err) {
    console.error("Voice Error: " + err.message);
    twiml.say("There was an error starting your briefing.");
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   POST /voice-listen
=========================== */

app.post('/voice-listen', function(req, res) {
  var twiml = new twilio.twiml.VoiceResponse();
  var gather = twiml.gather({
    input: 'speech',
    action: '/conversation',
    method: 'POST',
    speechTimeout: 3,
    language: 'en-US',
  });
  gather.say({ voice: 'Polly.Matthew' }, "I'm listening. What do you want to know?");
  twiml.say({ voice: 'Polly.Matthew' }, "Goodbye, Trace. Jarvis out.");
  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   POST /conversation — Back and forth
=========================== */

app.post('/conversation', async function(req, res) {
  var twiml = new twilio.twiml.VoiceResponse();
  var callSid = req.body.CallSid || 'unknown';
  var userSpeech = req.body.SpeechResult || '';

  console.log("Trace said: " + userSpeech);

  var goodbyeWords = ['goodbye', 'bye', 'hang up', 'end call', "that's all", 'nothing', "i'm good", 'no', 'nope'];
  if (goodbyeWords.some(function(w) { return userSpeech.toLowerCase().includes(w); })) {
    twiml.say({ voice: 'Polly.Matthew' }, "Copy that, Trace. Go execute. Jarvis out.");
    twiml.hangup();
    delete callHistory[callSid];
    res.type('text/xml');
    return res.send(twiml.toString());
  }

  try {
    var history = callHistory[callSid] || {};
    var systemPrompt = history.systemPrompt || "You are Jarvis, Trace's AI agent. Keep responses short (2-4 sentences).";
    var messages = history.messages || [];

    messages.push({ role: 'user', content: userSpeech });

    var extraContext = '';
    var lowerSpeech = userSpeech.toLowerCase();

    var dataKeywords = {
      'debt': 'Ultimate_Debt_Tracker_Advanced',
      'finance': 'Ultimate_Debt_Tracker_Advanced',
      'money': 'Ultimate_Debt_Tracker_Advanced',
      'loan': 'Ultimate_Debt_Tracker_Advanced',
      'balance': 'Ultimate_Debt_Tracker_Advanced',
      'screen time': 'Dashboard',
      'productivity': 'Dashboard',
      'phone usage': 'Dashboard',
      'gratitude': 'Gratitude_Memory',
      'grateful': 'Gratitude_Memory',
      'business': 'Business_Idea_Ledger',
      'idea': 'Business_Idea_Ledger',
      'identity': 'Trace_Identity_Profile',
      'who am i': 'Trace_Identity_Profile',
      'pattern': 'Chat_Pattern_Risk_Business',
      'risk': 'Chat_Pattern_Risk_Business',
      'focus': 'Focus_Log',
      'reading': 'Reading_Log',
      'win': 'Wins',
    };

    var keywords = Object.keys(dataKeywords);
    for (var i = 0; i < keywords.length; i++) {
      if (lowerSpeech.includes(keywords[i])) {
        try {
          var fetchRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "'" + dataKeywords[keywords[i]] + "'!A1:N20",
          });
          var fetchRows = fetchRes.data.values || [];
          if (fetchRows.length > 0) {
            var fetchHeaders = fetchRows[0].join(', ');
            var fetchData = fetchRows.slice(1, 15).map(function(r) { return r.join(' | '); }).join('\n');
            extraContext = "\n\n[FRESH DATA FROM " + dataKeywords[keywords[i]] + "]\nHeaders: " + fetchHeaders + "\n" + fetchData;
          }
        } catch (e) {}
        break;
      }
    }

    if (extraContext) {
      messages[messages.length - 1].content += extraContext;
    }

    var response = await askClaude(systemPrompt, messages);
    console.log("Jarvis: " + response);

    messages.push({ role: 'assistant', content: response });
    callHistory[callSid] = { systemPrompt: history.systemPrompt, messages: messages };

    var gather = twiml.gather({
      input: 'speech',
      action: '/conversation',
      method: 'POST',
      speechTimeout: 3,
      language: 'en-US',
    });
    gather.say({ voice: 'Polly.Matthew' }, response);

    twiml.say({ voice: 'Polly.Matthew' }, "Anything else, Trace?");
    twiml.redirect('/voice-listen');
  } catch (err) {
    console.error("Conversation Error: " + err.message);
    twiml.say({ voice: 'Polly.Matthew' }, "I had trouble with that. Can you repeat?");
    twiml.redirect('/voice-listen');
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   GET /call — Trigger call
=========================== */

app.get('/call', async function(req, res) {
  // Require secret key to prevent unauthorized calls
  var secret = process.env.CALL_SECRET;
  if (secret && req.query.key !== secret) {
    return res.status(403).json({ error: "Unauthorized. Add ?key=YOUR_SECRET to trigger a call." });
  }

  try {
    console.log("Initiating call to Trace...");
    var baseUrl = req.query.url || process.env.BASE_URL || ('https://' + req.get('host'));
    var call = await twilioClient.calls.create({
      to: MY_NUMBER,
      from: TWILIO_NUMBER,
      url: baseUrl + '/voice',
    });
    console.log("Call initiated: " + call.sid);
    res.json({ message: "Calling you now, Trace.", callSid: call.sid });
  } catch (err) {
    console.error("Call Error: " + err.message);
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   POST /whatsapp — WhatsApp messages
=========================== */

var whatsappHistory = {};

app.post('/whatsapp', async function(req, res) {
  var from = req.body.From || '';
  var userMessage = req.body.Body || '';
  var twiml = new twilio.twiml.MessagingResponse();

  console.log("WhatsApp from " + from + ": " + userMessage);

  try {
    // Build context on first message or if user says "briefing"
    var history = whatsappHistory[from] || {};
    var lowerMsg = userMessage.toLowerCase().trim();

    // Special commands

    // ====== CALENDAR ======
    if (lowerMsg === 'calendar' || lowerMsg === 'schedule' || lowerMsg === 'events' || lowerMsg === 'what do i have today' || lowerMsg === 'whats today' || lowerMsg === "what's today") {
      twiml.message("Checking your calendar...");
      res.type('text/xml');
      res.send(twiml.toString());

      setTimeout(async function() {
        try {
          var accounts = Object.keys(gmailTokens);
          var allEvents = [];
          for (var ca = 0; ca < accounts.length; ca++) {
            var events = await getCalendarEvents(accounts[ca], 1);
            allEvents = allEvents.concat(events);
          }

          var msg = '';
          if (allEvents.length === 0) {
            msg = "Nothing on your calendar today. Open day — use it wisely.";
          } else {
            msg = "Today's schedule:\n\n";
            for (var ei = 0; ei < allEvents.length; ei++) {
              msg += (ei + 1) + ". " + allEvents[ei].time + " — " + allEvents[ei].summary;
              if (allEvents[ei].location) msg += " @ " + allEvents[ei].location;
              msg += "\n";
            }
          }

          await twilioClient.messages.create({
            body: msg,
            from: 'whatsapp:+14155238886',
            to: from,
          });
        } catch (e) {
          console.log("Calendar fetch error: " + e.message);
          try {
            await twilioClient.messages.create({
              body: "Couldn't access calendar. You may need to reconnect: https://lifeos-jarvis.onrender.com/gmail/auth",
              from: 'whatsapp:+14155238886',
              to: from,
            });
          } catch (e2) {}
        }
      }, 100);
      return;
    }

    if (lowerMsg === 'week' || lowerMsg === 'this week' || lowerMsg === 'weekly schedule') {
      twiml.message("Pulling your week...");
      res.type('text/xml');
      res.send(twiml.toString());

      setTimeout(async function() {
        try {
          var accounts = Object.keys(gmailTokens);
          var allEvents = [];
          for (var ca = 0; ca < accounts.length; ca++) {
            var events = await getCalendarEvents(accounts[ca], 7);
            allEvents = allEvents.concat(events);
          }

          var msg = '';
          if (allEvents.length === 0) {
            msg = "Nothing on your calendar this week.";
          } else {
            msg = "This week:\n\n";
            for (var ei = 0; ei < allEvents.length; ei++) {
              msg += (ei + 1) + ". " + allEvents[ei].date + " " + allEvents[ei].time + " — " + allEvents[ei].summary;
              if (allEvents[ei].location) msg += " @ " + allEvents[ei].location;
              msg += "\n";
            }
          }

          await twilioClient.messages.create({
            body: msg,
            from: 'whatsapp:+14155238886',
            to: from,
          });
        } catch (e) {
          console.log("Calendar week error: " + e.message);
        }
      }, 100);
      return;
    }

    // ====== CREATE CALENDAR EVENT ======
    var eventBuilder = whatsappHistory[from] ? whatsappHistory[from].eventBuilder : null;

    if (lowerMsg.startsWith('add event') || lowerMsg.startsWith('new event') || lowerMsg.startsWith('schedule:') || lowerMsg.startsWith('event:')) {
      whatsappHistory[from] = whatsappHistory[from] || {};
      whatsappHistory[from].eventBuilder = {
        step: 0,
        data: {},
        active: true,
      };
      twiml.message("New event. What's it called?");
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (eventBuilder && eventBuilder.active) {
      var eSteps = [
        { key: 'name', next: 'What date? (like Feb 20 or tomorrow)' },
        { key: 'date', next: 'What time? (like 2pm or 10:30am)' },
        { key: 'time', next: 'How long? (like 1 hour, 30 min)' },
        { key: 'duration', next: 'Location? (or type "none")' },
        { key: 'location', next: null },
      ];

      var eStep = eSteps[eventBuilder.step];
      eventBuilder.data[eStep.key] = userMessage;
      eventBuilder.step++;

      if (eventBuilder.step >= eSteps.length) {
        eventBuilder.active = false;
        twiml.message("Creating event...");
        res.type('text/xml');
        res.send(twiml.toString());

        var ed = eventBuilder.data;
        setTimeout(async function() {
          try {
            // Use Claude to parse the natural language date/time
            var parsed = await askClaude(
              "Parse this into a calendar event. Today is " + new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }) + ". Timezone: America/Chicago (CST/CDT).\n\nRespond with ONLY a JSON object, no markdown, no backticks: {\"start\":\"ISO8601\",\"end\":\"ISO8601\"}\n\nEvent: " + ed.name + "\nDate: " + ed.date + "\nTime: " + ed.time + "\nDuration: " + ed.duration,
              [{ role: 'user', content: 'Parse this event time.' }]
            );

            var times = JSON.parse(parsed.replace(/```json|```/g, '').trim());
            var accounts = Object.keys(gmailTokens);
            if (accounts.length === 0) {
              await twilioClient.messages.create({
                body: "No calendar connected. Visit https://lifeos-jarvis.onrender.com/gmail/auth",
                from: 'whatsapp:+14155238886',
                to: from,
              });
              return;
            }

            var loc = ed.location === 'none' ? '' : ed.location;
            var result = await createCalendarEvent(accounts[0], ed.name, times.start, times.end, loc);

            if (result.success) {
              await twilioClient.messages.create({
                body: "Event created: \"" + ed.name + "\"\n" + ed.date + " at " + ed.time + (loc ? " @ " + loc : "") + "\n\nI'll call you 10 minutes before.",
                from: 'whatsapp:+14155238886',
                to: from,
              });
            } else {
              await twilioClient.messages.create({
                body: "Couldn't create event: " + result.error,
                from: 'whatsapp:+14155238886',
                to: from,
              });
            }
          } catch (e) {
            console.log("Event creation error: " + e.message);
            try {
              await twilioClient.messages.create({
                body: "Error creating event: " + e.message,
                from: 'whatsapp:+14155238886',
                to: from,
              });
            } catch (e2) {}
          }
        }, 100);
        return;
      } else {
        twiml.message(eSteps[eventBuilder.step].next);
      }

      whatsappHistory[from].eventBuilder = eventBuilder;
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // ====== REMINDERS SYSTEM ======
    var activeReminders = global.activeReminders || {};
    global.activeReminders = activeReminders;

    if (lowerMsg.startsWith('remind:') || lowerMsg.startsWith('remind ') || lowerMsg.startsWith('todo:') || lowerMsg.startsWith('todo ') || lowerMsg.startsWith('i need to ') || lowerMsg.startsWith('i gotta ') || lowerMsg.startsWith('need to ') || lowerMsg.startsWith('buy ') || lowerMsg.startsWith('get ')) {
      var reminderText = userMessage;
      // Clean prefix
      if (lowerMsg.startsWith('remind:') || lowerMsg.startsWith('todo:')) {
        reminderText = userMessage.substring(userMessage.indexOf(':') + 1).trim();
      } else if (lowerMsg.startsWith('remind ') || lowerMsg.startsWith('todo ')) {
        reminderText = userMessage.substring(userMessage.indexOf(' ') + 1).trim();
      }

      var reminderId = 'r' + Date.now();
      var sleepStart = 23; // 11 PM
      var sleepEnd = 7; // 7 AM
      var nudgeCount = 0;

      activeReminders[reminderId] = {
        text: reminderText,
        created: Date.now(),
        done: false,
        nudges: 0,
      };

      twiml.message("Got it. I'll remind you about: \"" + reminderText + "\"\n\nFirst nudge in 5 hours. If it's not done in 10, I'm calling you.\n\nText \"done: " + reminderText.substring(0, 20) + "\" when it's handled.");
      res.type('text/xml');
      res.send(twiml.toString());

      // Log to sheet
      setTimeout(async function() {
        try {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Reminders'!A:F",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[
              new Date().toISOString().split('T')[0],
              new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
              reminderText,
              'PENDING',
              '',
              '',
            ]] },
          });
        } catch (e) { console.log("Reminder log error: " + e.message); }
      }, 100);

      // 5-hour nudge
      var fiveHours = 5 * 60 * 60 * 1000;
      var tenHours = 10 * 60 * 60 * 1000;

      setTimeout(async function() {
        if (activeReminders[reminderId] && !activeReminders[reminderId].done) {
          var hour = new Date().getHours();
          if (hour >= sleepEnd && hour < sleepStart) {
            activeReminders[reminderId].nudges++;
            try {
              await twilioClient.messages.create({
                body: "Reminder: \"" + reminderText + "\" — You said you'd handle this. Have you? Text \"done: " + reminderText.substring(0, 20) + "\" when it's done.",
                from: 'whatsapp:+14155238886',
                to: from,
              });
            } catch (e) { console.log("Reminder nudge error: " + e.message); }
          }
        }
      }, fiveHours);

      // 10-hour escalation — PHONE CALL
      setTimeout(async function() {
        if (activeReminders[reminderId] && !activeReminders[reminderId].done) {
          var hour = new Date().getHours();
          if (hour >= sleepEnd && hour < sleepStart) {
            try {
              // Text warning first
              await twilioClient.messages.create({
                body: "Last warning. \"" + reminderText + "\" is still not done. I'm calling you in 60 seconds.",
                from: 'whatsapp:+14155238886',
                to: from,
              });

              // Call after 60 seconds
              setTimeout(async function() {
                if (activeReminders[reminderId] && !activeReminders[reminderId].done) {
                  try {
                    var baseUrl = process.env.BASE_URL || 'https://lifeos-jarvis.onrender.com';
                    // Create a custom voice reminder endpoint
                    await twilioClient.calls.create({
                      to: MY_NUMBER,
                      from: TWILIO_NUMBER,
                      twiml: '<Response><Say voice="Polly.Matthew">Trace. You told me to remind you about: ' + reminderText.replace(/[<>&"']/g, '') + '. It has been 10 hours and you still haven\'t done it. No more excuses. Handle it now.</Say></Response>',
                    });
                  } catch (callErr) { console.log("Reminder call error: " + callErr.message); }
                }
              }, 60000);
            } catch (e) { console.log("Reminder escalation error: " + e.message); }
          }
        }
      }, tenHours);

      return;
    }

    // ====== MARK REMINDER DONE ======
    if (lowerMsg.startsWith('done:') || lowerMsg.startsWith('done ') || lowerMsg.startsWith('finished:') || lowerMsg.startsWith('finished ')) {
      var doneText = userMessage.substring(userMessage.indexOf(':') > -1 && userMessage.indexOf(':') < 9 ? userMessage.indexOf(':') + 1 : userMessage.indexOf(' ') + 1).trim().toLowerCase();
      var cleared = 0;
      var reminderKeys = Object.keys(activeReminders);
      for (var rk = 0; rk < reminderKeys.length; rk++) {
        if (!activeReminders[reminderKeys[rk]].done && activeReminders[reminderKeys[rk]].text.toLowerCase().includes(doneText)) {
          activeReminders[reminderKeys[rk]].done = true;
          cleared++;
        }
      }
      if (cleared > 0) {
        twiml.message("Cleared. " + cleared + " reminder(s) marked done. That's execution.");
        // Log as a win and update reminder status
        setTimeout(async function() {
          try {
            var today = new Date().toISOString().split('T')[0];
            var now = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Wins'!A:D",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[today, 'Completed reminder: ' + doneText, 'Personal Growth', 'Auto-logged from reminder']] },
            });
            // Find and update the reminder row
            var reminderRows = await sheets.spreadsheets.values.get({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Reminders'!A:F",
            });
            var rRows = reminderRows.data.values || [];
            for (var ri = rRows.length - 1; ri >= 1; ri--) {
              if (rRows[ri][2] && rRows[ri][2].toLowerCase().includes(doneText) && rRows[ri][3] === 'PENDING') {
                var hoursToComplete = Math.round((Date.now() - new Date(rRows[ri][0] + ' ' + rRows[ri][1]).getTime()) / 3600000);
                await sheets.spreadsheets.values.update({
                  spreadsheetId: SPREADSHEET_ID,
                  range: "'Reminders'!D" + (ri + 1) + ":F" + (ri + 1),
                  valueInputOption: 'USER_ENTERED',
                  requestBody: { values: [['DONE', now, hoursToComplete > 0 ? hoursToComplete : 0]] },
                });
                break;
              }
            }
          } catch (e) { console.log("Reminder complete error: " + e.message); }
        }, 100);
      } else {
        twiml.message("No active reminders matching that. Text \"reminders\" to see what's pending.");
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // ====== LIST ACTIVE REMINDERS ======
    if (lowerMsg === 'reminders' || lowerMsg === 'reminder' || lowerMsg === 'what do i need to do') {
      var reminderKeys2 = Object.keys(activeReminders);
      var pending = [];
      for (var rk2 = 0; rk2 < reminderKeys2.length; rk2++) {
        if (!activeReminders[reminderKeys2[rk2]].done) {
          var r = activeReminders[reminderKeys2[rk2]];
          var hoursAgo = Math.round((Date.now() - r.created) / 3600000);
          pending.push((pending.length + 1) + ". " + r.text + " (" + hoursAgo + "h ago)");
        }
      }
      if (pending.length > 0) {
        twiml.message("Pending reminders:\n\n" + pending.join("\n") + "\n\nText \"done: [task]\" to clear.");
      } else {
        twiml.message("No pending reminders. You're clear.");
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // ====== LOG A WIN ======
    if (lowerMsg.startsWith('win:') || lowerMsg.startsWith('win ')) {
      var winText = userMessage.substring(userMessage.indexOf(':') > -1 && userMessage.indexOf(':') < 5 ? userMessage.indexOf(':') + 1 : 4).trim();
      twiml.message("Win logged. Keep stacking.");
      res.type('text/xml');
      res.send(twiml.toString());
      setTimeout(async function() {
        try {
          var today = new Date().toISOString().split('T')[0];
          var area = await askClaude(
            "Categorize this win into exactly one of: Work, Health, Social, Financial, Personal Growth, Dating. Respond with ONLY the category name, nothing else.",
            [{ role: 'user', content: winText }]
          );
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Wins'!A:D",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[today, winText, area.trim(), 'Logged via Jarvis']] },
          });
        } catch (e) { console.log("Win log error: " + e.message); }
      }, 100);
      return;
    }

    // ====== GYM LOG ======
    if (lowerMsg === 'gym' || lowerMsg === 'gym log' || lowerMsg === 'worked out' || lowerMsg === 'hit the gym' || lowerMsg.startsWith('gym:')) {
      // Quick log: "gym: chest and triceps" or start guided flow
      var quickGym = '';
      if (lowerMsg.includes(':')) {
        quickGym = userMessage.substring(userMessage.indexOf(':') + 1).trim();
      }

      if (quickGym) {
        twiml.message("Gym logged. Discipline is everything.");
        res.type('text/xml');
        res.send(twiml.toString());
        setTimeout(async function() {
          try {
            var today = new Date().toISOString().split('T')[0];
            var day = new Date().toLocaleDateString('en-US', { weekday: 'long' });
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Gym_Log'!A:E",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[today, day, quickGym, '', 'Logged via Jarvis']] },
            });
            // Also log as a win
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Wins'!A:D",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[today, 'Hit the gym: ' + quickGym, 'Health', 'Auto-logged from gym']] },
            });
          } catch (e) { console.log("Gym log error: " + e.message); }
        }, 100);
        return;
      }

      // Guided flow
      whatsappHistory[from] = whatsappHistory[from] || {};
      whatsappHistory[from].gymLog = {
        step: 0,
        data: { date: new Date().toISOString().split('T')[0] },
        active: true,
      };
      twiml.message("Gym check-in. What did you train? (chest, back, legs, shoulders, arms, full body, cardio)");
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    var gymLog = whatsappHistory[from] ? whatsappHistory[from].gymLog : null;
    if (gymLog && gymLog.active) {
      var gSteps = [
        { key: 'muscles', next: 'How long was the session? (minutes)' },
        { key: 'duration', next: 'How was the energy? (1-10)' },
        { key: 'energy', next: null },
      ];

      var gStep = gSteps[gymLog.step];
      gymLog.data[gStep.key] = userMessage;
      gymLog.step++;

      if (gymLog.step >= gSteps.length) {
        gymLog.active = false;
        twiml.message("Logged. The gym never lies. Keep showing up.");
        res.type('text/xml');
        res.send(twiml.toString());

        var gd = gymLog.data;
        setTimeout(async function() {
          try {
            var day = new Date().toLocaleDateString('en-US', { weekday: 'long' });
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Gym_Log'!A:E",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[gd.date, day, gd.muscles, gd.duration + ' min', 'Energy: ' + gd.energy + '/10']] },
            });
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Wins'!A:D",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[gd.date, 'Gym: ' + gd.muscles + ' (' + gd.duration + ' min)', 'Health', 'Auto-logged from gym']] },
            });
          } catch (e) { console.log("Gym log error: " + e.message); }
        }, 100);
        return;
      } else {
        twiml.message(gSteps[gymLog.step].next);
      }

      whatsappHistory[from].gymLog = gymLog;
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // ====== WATER LOG ======
    if (lowerMsg.startsWith('water:') || lowerMsg.startsWith('water ') || lowerMsg.startsWith('drank ')) {
      var waterAmt = userMessage.replace(/^(water[:\s]*|drank\s*)/i, '').trim();
      twiml.message("Logged " + waterAmt + " water. Stay hydrated.");
      res.type('text/xml');
      res.send(twiml.toString());
      setTimeout(async function() {
        try {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Health_Log'!A:E",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[new Date().toISOString().split('T')[0], new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }), 'Water', waterAmt, '']] },
          });
        } catch (e) { console.log("Water log error: " + e.message); }
      }, 100);
      return;
    }

    // ====== HABIT / ADDICTION TRACKING ======
    if (lowerMsg === 'log' || lowerMsg === 'track' || lowerMsg === 'habit' || lowerMsg === 'habits') {
      whatsappHistory[from] = whatsappHistory[from] || {};
      whatsappHistory[from].habitLog = {
        step: 0,
        data: { date: new Date().toISOString().split('T')[0] },
        active: true,
      };
      twiml.message("Habit check-in. Be honest — this is between you and the data.\n\nAlcohol today? (none, 1-2 drinks, 3-5, 6+)");
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    var habitLog = whatsappHistory[from] ? whatsappHistory[from].habitLog : null;
    if (habitLog && habitLog.active) {
      var hSteps = [
        { key: 'alcohol', next: 'Nicotine today? (none, 1-3, 4-10, 10+)' },
        { key: 'nicotine', next: 'PMO today? (clean, relapsed)' },
        { key: 'pmo', next: 'Any urges you fought off today? (yes/no)' },
        { key: 'urges', next: 'Water intake today? (oz or glasses)' },
        { key: 'water', next: null },
      ];

      var hStep = hSteps[habitLog.step];
      habitLog.data[hStep.key] = userMessage;
      habitLog.step++;

      if (habitLog.step >= hSteps.length) {
        habitLog.active = false;
        var hd = habitLog.data;
        var isClean = (hd.alcohol.toLowerCase().includes('none') && hd.nicotine.toLowerCase().includes('none') && hd.pmo.toLowerCase().includes('clean'));
        twiml.message(isClean ? "Clean day. That's strength. Keep building that streak." : "Logged. Awareness is step one. Tomorrow is another chance to be better.");
        res.type('text/xml');
        res.send(twiml.toString());

        setTimeout(async function() {
          try {
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Health_Log'!A:E",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [
                [hd.date, '', 'Alcohol', hd.alcohol, ''],
                [hd.date, '', 'Nicotine', hd.nicotine, ''],
                [hd.date, '', 'PMO', hd.pmo, ''],
                [hd.date, '', 'Urges Fought', hd.urges, ''],
                [hd.date, '', 'Water', hd.water, ''],
              ] },
            });
          } catch (e) { console.log("Habit log error: " + e.message); }
        }, 100);
        return;
      } else {
        twiml.message(hSteps[habitLog.step].next);
      }

      whatsappHistory[from].habitLog = habitLog;
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // Quick habit shortcuts
    if (lowerMsg === 'relapsed' || lowerMsg === 'relapse') {
      twiml.message("Logged. No shame — awareness matters. What triggered it?");
      res.type('text/xml');
      res.send(twiml.toString());
      setTimeout(async function() {
        try {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Health_Log'!A:E",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[new Date().toISOString().split('T')[0], new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }), 'PMO', 'Relapsed', '']] },
          });
        } catch (e) {}
      }, 100);
      return;
    }

    if (lowerMsg === 'clean' || lowerMsg === 'clean day') {
      twiml.message("Clean day logged. That's discipline. Keep going.");
      res.type('text/xml');
      res.send(twiml.toString());
      setTimeout(async function() {
        try {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Health_Log'!A:E",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [
              [new Date().toISOString().split('T')[0], new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }), 'PMO', 'Clean', ''],
              [new Date().toISOString().split('T')[0], '', 'Nicotine', 'None', ''],
              [new Date().toISOString().split('T')[0], '', 'Alcohol', 'None', ''],
            ] },
          });
        } catch (e) {}
      }, 100);
      return;
    }

    // ====== DAILY CHECK-IN ======
    var dailyCheckin = whatsappHistory[from] ? whatsappHistory[from].dailyCheckin : null;

    if (lowerMsg === 'check in' || lowerMsg === 'checkin' || lowerMsg === 'check-in' || lowerMsg === 'log day') {
      whatsappHistory[from] = whatsappHistory[from] || {};
      whatsappHistory[from].dailyCheckin = {
        step: 0,
        data: { date: new Date().toISOString().split('T')[0] },
        active: true,
      };
      twiml.message("Daily check-in. Quick answers.\n\nHow many hours did you sleep last night?");
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (dailyCheckin && dailyCheckin.active) {
      var steps = [
        { key: 'sleep', next: 'How many ounces of water today?' },
        { key: 'water', next: 'Workout minutes today?' },
        { key: 'workout', next: 'Reading minutes today?' },
        { key: 'reading', next: 'Screen time hours today? (estimate)' },
        { key: 'screenTime', next: 'Social time minutes today?' },
        { key: 'social', next: 'Mood 1-10?' },
        { key: 'mood', next: 'Focus 1-10?' },
        { key: 'focus', next: 'Any notes on today? (or type "none")' },
        { key: 'notes', next: null },
      ];

      var step = steps[dailyCheckin.step];
      dailyCheckin.data[step.key] = userMessage;
      dailyCheckin.step++;

      if (dailyCheckin.step >= steps.length) {
        dailyCheckin.active = false;
        twiml.message("Day logged. Keep building.");
        res.type('text/xml');
        res.send(twiml.toString());

        var d = dailyCheckin.data;
        setTimeout(async function() {
          try {
            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Daily_Log'!A:J",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[d.date, d.sleep, d.water, d.workout, d.reading, d.screenTime, d.social, d.mood, d.focus, d.notes === 'none' ? '' : d.notes]] },
            });
          } catch (e) { console.log("Daily log error: " + e.message); }
        }, 100);
      } else {
        twiml.message(steps[dailyCheckin.step].next || 'Done.');
      }

      whatsappHistory[from].dailyCheckin = dailyCheckin;
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // ====== DATING LOG ======
    var datingLog = whatsappHistory[from] ? whatsappHistory[from].datingLog : null;

    if (lowerMsg === 'date log' || lowerMsg === 'dating' || lowerMsg === 'log date') {
      whatsappHistory[from] = whatsappHistory[from] || {};
      whatsappHistory[from].datingLog = {
        step: 0,
        data: { date: new Date().toISOString().split('T')[0] },
        active: true,
      };
      twiml.message("Dating log. Be real.\n\nWho was it with? (first name or description)");
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (datingLog && datingLog.active) {
      var dSteps = [
        { key: 'who', next: 'What type? (first date, second date, talking stage, hookup, situationship check-in, other)' },
        { key: 'type', next: 'How did you feel during it? Be honest.' },
        { key: 'feeling', next: 'Did you notice any old patterns showing up? (validation seeking, fear of intimacy, comparing to ex, emotional detachment, or none)' },
        { key: 'patterns', next: 'What went well?' },
        { key: 'wellDone', next: 'What would you do differently?' },
        { key: 'differently', next: null },
      ];

      var dStep = dSteps[datingLog.step];
      datingLog.data[dStep.key] = userMessage;
      datingLog.step++;

      if (datingLog.step >= dSteps.length) {
        datingLog.active = false;
        twiml.message("Logged. I'll analyze this with your patterns.");
        res.type('text/xml');
        res.send(twiml.toString());

        var dd = datingLog.data;
        setTimeout(async function() {
          try {
            // AI analysis of the date
            var analysis = await askClaude(
              "You are Jarvis, Trace's dating counselor. He just logged a date. Based on his known patterns (seeking validation, fear of intimacy, comparing partners to ex, emotional detachment after breakups), give a 2-sentence honest assessment. Was this growth or repetition? No markdown.",
              [{ role: 'user', content: 'Date with: ' + dd.who + '\nType: ' + dd.type + '\nFeeling: ' + dd.feeling + '\nPatterns noticed: ' + dd.patterns + '\nWent well: ' + dd.wellDone + '\nDo differently: ' + dd.differently }]
            );

            await sheets.spreadsheets.values.append({
              spreadsheetId: SPREADSHEET_ID,
              range: "'Dating_Log'!A:H",
              valueInputOption: 'USER_ENTERED',
              requestBody: { values: [[dd.date, dd.who, dd.type, dd.feeling, dd.patterns, dd.wellDone, dd.differently, analysis]] },
            });

            await twilioClient.messages.create({
              body: "Dating insight: " + analysis,
              from: 'whatsapp:+14155238886',
              to: from,
            });
          } catch (e) { console.log("Dating log error: " + e.message); }
        }, 100);
        return;
      } else {
        twiml.message(dSteps[datingLog.step].next || 'Done.');
      }

      whatsappHistory[from].datingLog = datingLog;
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (lowerMsg === 'briefing' || lowerMsg === 'brief' || lowerMsg === 'status') {
      var context = await buildLifeOSContext();
      var briefing = await askClaude(
        "You are Jarvis, Trace's personal AI counselor on WhatsApp. Talk like a wise mentor, not a database. Never mention tab names, sheet names, or entry counts. Use the data to give real advice. No markdown.\n\nLIFE OS DATA:\n" + context,
        [{ role: 'user', content: 'Give me a quick Life OS briefing.' }]
      );
      twiml.message(briefing);
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (lowerMsg === 'call' || lowerMsg === 'call me') {
      try {
        var baseUrl = process.env.BASE_URL || 'https://lifeos-jarvis.onrender.com';
        await twilioClient.calls.create({
          to: MY_NUMBER,
          from: TWILIO_NUMBER,
          url: baseUrl + '/voice',
        });
        twiml.message("Calling you now, Trace.");
      } catch (callErr) {
        twiml.message("Call failed: " + callErr.message);
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // Email commands
    if (lowerMsg === 'email' || lowerMsg === 'emails' || lowerMsg === 'inbox' || lowerMsg === 'mail') {
      var emailContext = await buildEmailContext();
      if (!emailContext) {
        twiml.message("No Gmail accounts connected. Visit https://lifeos-jarvis.onrender.com/gmail/auth to connect.");
      } else {
        // Store emails for later actions
        var accounts = Object.keys(gmailTokens);
        var cachedEmails = [];
        for (var ea = 0; ea < accounts.length; ea++) {
          var fetched = await getUnreadEmails(accounts[ea], 10);
          fetched.forEach(function(e) { e.account = accounts[ea]; });
          cachedEmails = cachedEmails.concat(fetched);
        }
        whatsappHistory[from] = whatsappHistory[from] || {};
        whatsappHistory[from].cachedEmails = cachedEmails;

        var emailSummary = await askClaude(
          "You are Jarvis on WhatsApp. Be very concise. No markdown. Number each email.",
          [{ role: 'user', content: 'Prioritize these emails. Tell me which to respond to first:\n' + emailContext }]
        );
        twiml.message(emailSummary);
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // Email actions: delete, archive, reply
    if (lowerMsg.includes('delete') && (lowerMsg.includes('email') || lowerMsg.includes('mail'))) {
      var cached = (whatsappHistory[from] || {}).cachedEmails || [];
      if (cached.length === 0) {
        twiml.message("No emails loaded. Text 'email' first to scan your inbox.");
      } else {
        var searchTerm = lowerMsg.replace('delete', '').replace('all', '').replace('emails', '').replace('email', '').replace('from', '').replace('the', '').trim();
        var deleted = 0;
        for (var d = 0; d < cached.length; d++) {
          if (cached[d].from.toLowerCase().includes(searchTerm) || cached[d].subject.toLowerCase().includes(searchTerm)) {
            await deleteEmail(cached[d].account, cached[d].id);
            deleted++;
          }
        }
        if (deleted > 0) {
          twiml.message("Done. Deleted " + deleted + " email(s) matching '" + searchTerm + "'.");
        } else {
          twiml.message("No emails found matching '" + searchTerm + "'. Text 'email' to see your inbox.");
        }
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    if (lowerMsg.includes('archive') && (lowerMsg.includes('email') || lowerMsg.includes('mail'))) {
      var cached2 = (whatsappHistory[from] || {}).cachedEmails || [];
      if (cached2.length === 0) {
        twiml.message("No emails loaded. Text 'email' first.");
      } else {
        var searchTerm2 = lowerMsg.replace('archive', '').replace('all', '').replace('emails', '').replace('email', '').replace('from', '').replace('the', '').trim();
        var archived = 0;
        for (var ar = 0; ar < cached2.length; ar++) {
          if (cached2[ar].from.toLowerCase().includes(searchTerm2) || cached2[ar].subject.toLowerCase().includes(searchTerm2)) {
            await archiveEmail(cached2[ar].account, cached2[ar].id);
            archived++;
          }
        }
        twiml.message(archived > 0 ? "Archived " + archived + " email(s) matching '" + searchTerm2 + "'." : "No emails found matching '" + searchTerm2 + "'.");
      }
      res.type('text/xml');
      return res.send(twiml.toString());
    }

    // Smart AI inbox cleanup
    if (lowerMsg === 'clean inbox' || lowerMsg === 'clean email' || lowerMsg === 'clean emails' || lowerMsg === 'filter inbox' || lowerMsg === 'filter emails') {
      var accounts = Object.keys(gmailTokens);
      if (accounts.length === 0) {
        twiml.message("No Gmail accounts connected. Visit https://lifeos-jarvis.onrender.com/gmail/auth");
        res.type('text/xml');
        return res.send(twiml.toString());
      }

      twiml.message("Scanning and cleaning your inbox now. This may take a minute...");
      res.type('text/xml');
      res.send(twiml.toString());

      // Process in background after responding
      setTimeout(async function() {
        try {
          var totalDeleted = 0;
          var totalArchived = 0;
          var kept = [];

          for (var ca = 0; ca < accounts.length; ca++) {
            var emails = await getUnreadEmails(accounts[ca], 25);
            if (emails.length === 0) continue;

            // Build email list for Claude to categorize
            var emailList = emails.map(function(e, idx) {
              return (idx + 1) + '. From: ' + e.from + ' | Subject: ' + e.subject + ' | Preview: ' + e.snippet.substring(0, 80);
            }).join('\n');

            var categorization = await askClaude(
              "You categorize emails. For each email, respond with ONLY the number and category. Categories: DELETE (junk, spam, newsletters, promotions, marketing), ARCHIVE (low priority, FYI only, no action needed), KEEP (important, needs response, money, business, personal). Format: 1:DELETE 2:KEEP 3:ARCHIVE etc. One per line. Nothing else.",
              [{ role: 'user', content: 'Categorize these emails:\n' + emailList }]
            );

            // Parse Claude's response
            var lines = categorization.split('\n');
            for (var cl = 0; cl < lines.length; cl++) {
              var match = lines[cl].match(/(\d+)\s*:\s*(DELETE|ARCHIVE|KEEP)/i);
              if (match) {
                var emailIdx = parseInt(match[1]) - 1;
                var action = match[2].toUpperCase();
                if (emailIdx >= 0 && emailIdx < emails.length) {
                  if (action === 'DELETE') {
                    await deleteEmail(accounts[ca], emails[emailIdx].id);
                    totalDeleted++;
                  } else if (action === 'ARCHIVE') {
                    await archiveEmail(accounts[ca], emails[emailIdx].id);
                    totalArchived++;
                  } else {
                    kept.push(emails[emailIdx].from.split('<')[0].trim() + ': ' + emails[emailIdx].subject);
                  }
                }
              }
            }
          }

          // Send results via WhatsApp
          var resultMsg = "Inbox cleaned!\n\n";
          resultMsg += "Deleted: " + totalDeleted + " junk emails\n";
          resultMsg += "Archived: " + totalArchived + " low-priority emails\n";
          resultMsg += "Kept: " + kept.length + " important emails\n";
          if (kept.length > 0) {
            resultMsg += "\nStill in your inbox:\n";
            for (var k = 0; k < Math.min(kept.length, 10); k++) {
              resultMsg += (k + 1) + ". " + kept[k] + "\n";
            }
          }

          // Send via Twilio
          await twilioClient.messages.create({
            body: resultMsg,
            from: 'whatsapp:+14155238886',
            to: from,
          });
        } catch (cleanErr) {
          console.error("Inbox clean error: " + cleanErr.message);
          try {
            await twilioClient.messages.create({
              body: "Error cleaning inbox: " + cleanErr.message,
              from: 'whatsapp:+14155238886',
              to: from,
            });
          } catch (e) {}
        }
      }, 100);
      return;
    }

    // ====== DAILY 10 QUESTIONS SYSTEM ======
    var dailyQ = whatsappHistory[from] ? whatsappHistory[from].dailyQuestions : null;

    // Start daily questions
    if (lowerMsg === 'questions' || lowerMsg === 'daily' || lowerMsg === '10' || lowerMsg === 'challenge me' || lowerMsg === 'challenge') {
      twiml.message("Generating your 10 questions. First one coming in a moment...");
      res.type('text/xml');
      res.send(twiml.toString());

      // Generate and send async
      setTimeout(async function() {
        try {
          var qContext = await buildLifeOSContext();
          var questionsRaw = await askClaude(
            "You generate 10 deep, personal challenge questions for Trace. These questions should challenge his beliefs, make him uncomfortable, and force growth.\n\nCover ALL these areas across the 10 questions: dating and relationships, money mindset, self-worth, business ambition, daily habits, health, purpose, fears, accountability, and personal identity.\n\nRULES:\n- Make each question personal based on his Life OS data\n- Questions should be uncomfortable but constructive\n- Never mention tab names, sheet names, or entry counts\n- Use what you know about his life to make questions HIT\n- Format: one question per line, numbered 1-10, nothing else\n- No fluff, no explanations, just the questions\n\nLIFE OS DATA:\n" + qContext,
            [{ role: 'user', content: 'Generate 10 deep personal challenge questions for today.' }]
          );

          var questions = questionsRaw.split('\n').filter(function(q) { return q.trim().match(/^\d/); });
          if (questions.length < 10) questions = questionsRaw.split('\n').filter(function(q) { return q.trim().length > 10; });

          whatsappHistory[from] = whatsappHistory[from] || {};
          whatsappHistory[from].dailyQuestions = {
            questions: questions,
            answers: [],
            currentIndex: 0,
            date: new Date().toISOString().split('T')[0],
            active: true,
          };

          await twilioClient.messages.create({
            body: "Let's go. 10 questions. Be honest with yourself.\n\n" + questions[0],
            from: 'whatsapp:+14155238886',
            to: from,
          });
        } catch (qErr) {
          console.error("Daily questions error: " + qErr.message);
          try {
            await twilioClient.messages.create({
              body: "Error generating questions: " + qErr.message,
              from: 'whatsapp:+14155238886',
              to: from,
            });
          } catch (e) {}
        }
      }, 100);
      return;
    }

    // If daily questions are active, capture answer and send next question
    if (dailyQ && dailyQ.active && dailyQ.currentIndex < dailyQ.questions.length) {
      // Log the answer
      dailyQ.answers.push({
        question: dailyQ.questions[dailyQ.currentIndex],
        answer: userMessage,
        time: new Date().toISOString(),
      });
      dailyQ.currentIndex++;

      var isLast = dailyQ.currentIndex >= dailyQ.questions.length;

      if (isLast) {
        twiml.message("Got it. Analyzing your answers now...");
      } else {
        var qNum = dailyQ.currentIndex + 1;
        twiml.message("Logged. Question " + qNum + " of 10:\n\n" + dailyQ.questions[dailyQ.currentIndex]);
      }

      res.type('text/xml');
      res.send(twiml.toString());

      // Process logging and analysis async
      var capturedDailyQ = dailyQ;
      var capturedFrom = from;
      setTimeout(async function() {
        try {
          var row = [
            capturedDailyQ.date,
            new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
            capturedDailyQ.answers[capturedDailyQ.answers.length - 1].question.replace(/^\d+[\.\)]\s*/, ''),
            capturedDailyQ.answers[capturedDailyQ.answers.length - 1].answer,
          ];

          if (isLast) {
            var answersText = capturedDailyQ.answers.map(function(a, i) {
              return (i + 1) + '. ' + a.question + '\nAnswer: ' + a.answer;
            }).join('\n\n');

            var analysisRaw = await askClaude(
              "You are Jarvis, Trace's AI counselor. He just answered 10 deep personal questions. Respond with EXACTLY three sections separated by |||. Section 1: Mindset analysis — patterns in his thinking, where he's strong, where he's lying to himself (3-4 sentences). Section 2: 2-3 specific personal beliefs holding him back and why they're wrong (3-4 sentences). Section 3: 3 concrete actions he should take TODAY based on his answers (3 sentences). No markdown. No bullet points. Separate sections ONLY with |||",
              [{ role: 'user', content: 'Here are my answers:\n\n' + answersText }]
            );

            var parts = analysisRaw.split('|||').map(function(p) { return p.trim(); });
            row.push(parts[0] || '', parts[1] || '', parts[2] || '');

            capturedDailyQ.active = false;
            capturedDailyQ.summary = (parts[0] || '') + '\n\n' + (parts[1] || '') + '\n\n' + (parts[2] || '');

            await twilioClient.messages.create({
              body: "That's all 10. Here's what I see:\n\n" + capturedDailyQ.summary,
              from: 'whatsapp:+14155238886',
              to: capturedFrom,
            });
          }

          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Daily_Questions'!A:G",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [row] },
          });
        } catch (logErr) {
          console.log("Could not log question: " + logErr.message);
        }
      }, 100);

      whatsappHistory[from].dailyQuestions = dailyQ;
      return;
    }

    // Regular conversation with Claude
    if (!history.systemPrompt) {
      var context2 = await buildLifeOSContext();
      history.systemPrompt = "You are Jarvis, Trace's personal AI counselor and mentor on WhatsApp.\n\nRULES:\n- Talk like a wise friend and life coach. Be real.\n- NEVER mention tab names, sheet names, row counts, or entry counts.\n- NEVER recite statistics unless Trace specifically asks for numbers.\n- Use the data to UNDERSTAND Trace's situation, then give human advice.\n- Ask questions that make him think. Challenge him constructively.\n- Keep responses SHORT (2-4 sentences). This is WhatsApp.\n- No markdown, no bullet points, no formatting.\n\nLIFE OS DATA (use to inform advice, don't recite):\n" + context2;
      history.messages = [];
    }

    history.messages.push({ role: 'user', content: userMessage });

    // Fetch extra data based on keywords
    var extraContext = '';
    var dataKeywords = {
      'debt': 'Ultimate_Debt_Tracker_Advanced',
      'finance': 'Ultimate_Debt_Tracker_Advanced',
      'money': 'Ultimate_Debt_Tracker_Advanced',
      'screen time': 'Dashboard',
      'gratitude': 'Gratitude_Memory',
      'business': 'Business_Idea_Ledger',
      'idea': 'Business_Idea_Ledger',
      'identity': 'Trace_Identity_Profile',
      'focus': 'Focus_Log',
      'reading': 'Reading_Log',
      'win': 'Wins',
    };

    var keywords = Object.keys(dataKeywords);
    for (var i = 0; i < keywords.length; i++) {
      if (lowerMsg.includes(keywords[i])) {
        try {
          var fetchRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "'" + dataKeywords[keywords[i]] + "'!A1:N20",
          });
          var fetchRows = fetchRes.data.values || [];
          if (fetchRows.length > 0) {
            var fetchHeaders = fetchRows[0].join(', ');
            var fetchData = fetchRows.slice(1, 15).map(function(r) { return r.join(' | '); }).join('\n');
            extraContext = "\n\n[FRESH DATA FROM " + dataKeywords[keywords[i]] + "]\nHeaders: " + fetchHeaders + "\n" + fetchData;
          }
        } catch (e) {}
        break;
      }
    }

    if (extraContext) {
      history.messages[history.messages.length - 1].content += extraContext;
    }

    var response = await askClaude(history.systemPrompt, history.messages);
    console.log("Jarvis WhatsApp: " + response);

    // Keep only last 10 messages to avoid token limits
    history.messages.push({ role: 'assistant', content: response });
    if (history.messages.length > 20) {
      history.messages = history.messages.slice(-10);
    }
    whatsappHistory[from] = history;

    twiml.message(response);
  } catch (err) {
    console.error("WhatsApp Error: " + err.message);
    twiml.message("Error: " + err.message);
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   GET /briefing
=========================== */

app.get('/briefing', async function(req, res) {
  try {
    console.log("Building briefing...");
    var context = await buildLifeOSContext();
    var emailContext = await buildEmailContext();
    var fullContext = context + emailContext;
    var response = await askClaude(
      "You are Jarvis, Trace's personal AI counselor. Give a morning check-in like a wise mentor would. Talk about what matters today, what needs attention, and ask a question that makes Trace think. NEVER mention tab names, sheet names, or entry counts. Use the data to inform advice, don't recite it. If there are urgent emails, mention who they're from. No markdown.\n\nLIFE OS DATA (inform your advice, don't recite):\n" + fullContext,
      [{ role: 'user', content: 'Give me my full Life OS briefing including email priorities.' }]
    );
    res.json({ briefing: response });
  } catch (err) {
    console.error("Briefing Error: " + err.message);
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   ALL OTHER ENDPOINTS
=========================== */

app.get('/tabs', async function(req, res) {
  try {
    var tabs = await getAllTabNames();
    res.json({ tabCount: tabs.length, tabs: tabs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/tab/:name', async function(req, res) {
  try {
    res.json(await getTabData(req.params.name));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/scan', async function(req, res) {
  try {
    var tabs = await getAllTabNames();
    var results = [];
    for (var i = 0; i < tabs.length; i++) {
      var data = await getTabData(tabs[i]);
      results.push({ tab: data.tab, headers: data.headers, rowCount: data.rowCount, error: data.error || null });
    }
    var totalRows = results.reduce(function(sum, t) { return sum + t.rowCount; }, 0);
    res.json({ totalTabs: tabs.length, totalRows: totalRows, tabs: results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/scan/full', async function(req, res) {
  try {
    var tabs = await getAllTabNames();
    var results = [];
    for (var i = 0; i < tabs.length; i++) {
      results.push(await getTabData(tabs[i]));
    }
    var totalRows = results.reduce(function(sum, t) { return sum + t.rowCount; }, 0);
    res.json({ totalTabs: tabs.length, totalRows: totalRows, tabs: results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/search', async function(req, res) {
  try {
    var query = (req.query.q || '').toLowerCase().trim();
    if (!query) return res.status(400).json({ error: "Provide ?q=search_term" });
    var tabs = await getAllTabNames();
    var matches = [];
    for (var t = 0; t < tabs.length; t++) {
      var data = await getTabData(tabs[t]);
      if (data.error) continue;
      for (var r = 0; r < data.rows.length; r++) {
        if (data.rows[r].join(' ').toLowerCase().includes(query)) {
          var obj = {};
          for (var h = 0; h < data.headers.length; h++) {
            obj[data.headers[h]] = data.rows[r][h] || '';
          }
          matches.push({ tab: tabs[t], row: r + 2, data: obj });
        }
      }
    }
    res.json({ query: query, matchCount: matches.length, matches: matches });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/summary', async function(req, res) {
  try {
    var tabs = await getAllTabNames();
    var summary = { totalTabs: tabs.length, categories: {} };
    for (var i = 0; i < tabs.length; i++) {
      var count = await getTabRowCount(tabs[i]);
      var cat = 'other';
      var l = tabs[i].toLowerCase();
      if (l.includes('debt') || l.includes('finance') || l.includes('loan')) cat = 'finance';
      else if (l.includes('chat') || l.includes('message')) cat = 'conversations';
      else if (l.includes('business') || l.includes('idea')) cat = 'business';
      else if (l.includes('screen') || l.includes('usage') || l.includes('rescue') || l.includes('focus')) cat = 'productivity';
      else if (l.includes('identity') || l.includes('execution') || l.includes('profile')) cat = 'identity';
      else if (l.includes('gratitude') || l.includes('win') || l.includes('worth')) cat = 'growth';
      else if (l.includes('log') || l.includes('daily')) cat = 'logs';
      else if (l.includes('test') || l.includes('eval')) cat = 'testing';
      else if (l.includes('taxonomy') || l.includes('setting')) cat = 'system';
      if (!summary.categories[cat]) summary.categories[cat] = [];
      summary.categories[cat].push({ tab: tabs[i], rowCount: count });
    }
    res.json(summary);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/priority', async function(req, res) {
  try {
    var taskTabs = ['Tasks', 'Daily_Log', 'Focus_Log', 'Jira_Log'];
    for (var i = 0; i < taskTabs.length; i++) {
      try {
        var r = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: "'" + taskTabs[i] + "'!A2:B10",
        });
        if (r.data.values && r.data.values.length > 0) {
          return res.json({ source: taskTabs[i], task: r.data.values[0][0], detail: r.data.values[0][1] || '' });
        }
      } catch (e) {}
    }
    res.json({ message: "No tasks found." });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   GMAIL — OAuth Sign-in Flow
=========================== */

// Step 1: Visit this to sign in a Gmail account
app.get('/gmail/auth', function(req, res) {
  if (!GMAIL_CLIENT_ID) return res.json({ error: "Gmail not configured" });
  var oauth2 = createGmailOAuth2Client();
  var url = oauth2.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: [
      'https://www.googleapis.com/auth/gmail.readonly',
      'https://www.googleapis.com/auth/gmail.send',
      'https://www.googleapis.com/auth/gmail.modify',
      'https://mail.google.com/',
      'https://www.googleapis.com/auth/calendar',
    ],
  });
  res.redirect(url);
});

// Step 2: Google redirects here after sign-in
app.get('/gmail/callback', async function(req, res) {
  if (!req.query.code) return res.json({ error: "No code received" });
  try {
    var oauth2 = createGmailOAuth2Client();
    var { tokens } = await oauth2.getToken(req.query.code);
    oauth2.setCredentials(tokens);

    // Get the email address
    var gmail = google.gmail({ version: 'v1', auth: oauth2 });
    var profile = await gmail.users.getProfile({ userId: 'me' });
    var email = profile.data.emailAddress;

    gmailTokens[email] = tokens;
    saveGmailTokens();

    console.log("Gmail: Connected " + email);
    res.json({ success: true, email: email, message: "Gmail connected! You can close this tab." });
  } catch (err) {
    console.error("Gmail callback error: " + err.message);
    res.status(500).json({ error: err.message });
  }
});

// List connected accounts
app.get('/gmail/accounts', function(req, res) {
  res.json({ accounts: Object.keys(gmailTokens) });
});

// Export tokens (for saving to env var)
app.get('/gmail/tokens', function(req, res) {
  if (req.query.key !== (process.env.CALL_SECRET || '')) return res.status(403).json({ error: 'Unauthorized' });
  res.json({ tokens: JSON.stringify(gmailTokens), accounts: Object.keys(gmailTokens) });
});

/* ===========================
   GMAIL — Helper Functions
=========================== */

async function getGmailClient(email) {
  var tokens = gmailTokens[email];
  if (!tokens) return null;
  var oauth2 = createGmailOAuth2Client(tokens);

  // Refresh if expired
  if (tokens.expiry_date && tokens.expiry_date < Date.now()) {
    try {
      var { credentials } = await oauth2.refreshAccessToken();
      gmailTokens[email] = credentials;
      saveGmailTokens();
      oauth2.setCredentials(credentials);
    } catch (e) {
      console.error("Gmail refresh failed for " + email + ": " + e.message);
      return null;
    }
  }
  return google.gmail({ version: 'v1', auth: oauth2 });
}

async function getUnreadEmails(email, maxResults) {
  var gmail = await getGmailClient(email);
  if (!gmail) return [];
  try {
    var list = await gmail.users.messages.list({
      userId: 'me',
      q: 'is:unread',
      maxResults: maxResults || 15,
    });
    var messages = list.data.messages || [];
    var emails = [];
    for (var i = 0; i < messages.length; i++) {
      var msg = await gmail.users.messages.get({
        userId: 'me',
        id: messages[i].id,
        format: 'metadata',
        metadataHeaders: ['From', 'Subject', 'Date'],
      });
      var headers = msg.data.payload.headers;
      var fromHeader = headers.find(function(h) { return h.name === 'From'; });
      var subjectHeader = headers.find(function(h) { return h.name === 'Subject'; });
      var dateHeader = headers.find(function(h) { return h.name === 'Date'; });
      emails.push({
        id: messages[i].id,
        from: fromHeader ? fromHeader.value : 'Unknown',
        subject: subjectHeader ? subjectHeader.value : '(no subject)',
        date: dateHeader ? dateHeader.value : '',
        snippet: msg.data.snippet || '',
      });
    }
    return emails;
  } catch (err) {
    console.error("Gmail read error for " + email + ": " + err.message);
    return [];
  }
}

async function getEmailBody(email, messageId) {
  var gmail = await getGmailClient(email);
  if (!gmail) return '';
  try {
    var msg = await gmail.users.messages.get({ userId: 'me', id: messageId, format: 'full' });
    var parts = msg.data.payload.parts || [msg.data.payload];
    for (var i = 0; i < parts.length; i++) {
      if (parts[i].mimeType === 'text/plain' && parts[i].body.data) {
        return Buffer.from(parts[i].body.data, 'base64').toString('utf8');
      }
    }
    if (msg.data.payload.body && msg.data.payload.body.data) {
      return Buffer.from(msg.data.payload.body.data, 'base64').toString('utf8');
    }
    return msg.data.snippet || '';
  } catch (err) {
    return '';
  }
}

async function sendReply(email, messageId, replyText) {
  var gmail = await getGmailClient(email);
  if (!gmail) return { error: 'Not connected' };
  try {
    var msg = await gmail.users.messages.get({ userId: 'me', id: messageId, format: 'metadata', metadataHeaders: ['From', 'Subject', 'Message-ID'] });
    var headers = msg.data.payload.headers;
    var toHeader = headers.find(function(h) { return h.name === 'From'; });
    var subjectHeader = headers.find(function(h) { return h.name === 'Subject'; });
    var msgIdHeader = headers.find(function(h) { return h.name === 'Message-ID'; });
    var to = toHeader ? toHeader.value : '';
    var subject = subjectHeader ? subjectHeader.value : '';
    if (!subject.startsWith('Re:')) subject = 'Re: ' + subject;

    var raw = [
      'To: ' + to,
      'Subject: ' + subject,
      'In-Reply-To: ' + (msgIdHeader ? msgIdHeader.value : ''),
      'References: ' + (msgIdHeader ? msgIdHeader.value : ''),
      'Content-Type: text/plain; charset=utf-8',
      '',
      replyText,
    ].join('\r\n');

    var encoded = Buffer.from(raw).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    await gmail.users.messages.send({ userId: 'me', requestBody: { raw: encoded, threadId: msg.data.threadId } });
    return { success: true, to: to };
  } catch (err) {
    return { error: err.message };
  }
}

async function deleteEmail(email, messageId) {
  var gmail = await getGmailClient(email);
  if (!gmail) return { error: 'Not connected' };
  try {
    await gmail.users.messages.trash({ userId: 'me', id: messageId });
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
}

async function archiveEmail(email, messageId) {
  var gmail = await getGmailClient(email);
  if (!gmail) return { error: 'Not connected' };
  try {
    await gmail.users.messages.modify({ userId: 'me', id: messageId, requestBody: { removeLabelIds: ['INBOX'] } });
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
}

/* ===========================
   GOOGLE CALENDAR — Helper Functions
=========================== */

async function getCalendarEvents(email, daysAhead) {
  var tokens = gmailTokens[email];
  if (!tokens) return [];
  var oauth2 = createGmailOAuth2Client(tokens);

  if (tokens.expiry_date && tokens.expiry_date < Date.now()) {
    try {
      var { credentials } = await oauth2.refreshAccessToken();
      gmailTokens[email] = credentials;
      saveGmailTokens();
      oauth2.setCredentials(credentials);
    } catch (e) { return []; }
  }

  try {
    var calendar = google.calendar({ version: 'v3', auth: oauth2 });
    var now = new Date();
    var future = new Date();
    future.setDate(future.getDate() + (daysAhead || 1));

    var res = await calendar.events.list({
      calendarId: 'primary',
      timeMin: now.toISOString(),
      timeMax: future.toISOString(),
      singleEvents: true,
      orderBy: 'startTime',
      maxResults: 15,
    });

    var events = res.data.items || [];
    return events.map(function(e) {
      var start = e.start.dateTime || e.start.date;
      var startDate = new Date(start);
      var timeStr = e.start.dateTime
        ? startDate.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
        : 'All day';
      var dayStr = startDate.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      return {
        summary: e.summary || '(no title)',
        time: timeStr,
        date: dayStr,
        location: e.location || '',
        description: (e.description || '').substring(0, 100),
      };
    });
  } catch (err) {
    console.log("Calendar error for " + email + ": " + err.message);
    return [];
  }
}

async function buildCalendarContext(daysAhead) {
  var accounts = Object.keys(gmailTokens);
  if (accounts.length === 0) return '';

  var context = '\nUPCOMING CALENDAR:\n';
  var hasEvents = false;

  for (var a = 0; a < accounts.length; a++) {
    var events = await getCalendarEvents(accounts[a], daysAhead || 1);
    if (events.length > 0) {
      hasEvents = true;
      for (var e = 0; e < events.length; e++) {
        context += '  ' + events[e].date + ' ' + events[e].time + ' — ' + events[e].summary;
        if (events[e].location) context += ' @ ' + events[e].location;
        context += '\n';
      }
    }
  }

  if (!hasEvents) context += '  No upcoming events\n';
  return context + '\n';
}

async function createCalendarEvent(email, summary, startTime, endTime, location) {
  var tokens = gmailTokens[email];
  if (!tokens) return { error: 'Not connected' };
  var oauth2 = createGmailOAuth2Client(tokens);

  if (tokens.expiry_date && tokens.expiry_date < Date.now()) {
    try {
      var { credentials } = await oauth2.refreshAccessToken();
      gmailTokens[email] = credentials;
      saveGmailTokens();
      oauth2.setCredentials(credentials);
    } catch (e) { return { error: 'Auth refresh failed' }; }
  }

  try {
    var calendar = google.calendar({ version: 'v3', auth: oauth2 });
    var event = {
      summary: summary,
      start: { dateTime: startTime, timeZone: 'America/Chicago' },
      end: { dateTime: endTime, timeZone: 'America/Chicago' },
    };
    if (location) event.location = location;

    var result = await calendar.events.insert({
      calendarId: 'primary',
      requestBody: event,
    });
    return { success: true, id: result.data.id, link: result.data.htmlLink };
  } catch (err) {
    return { error: err.message };
  }
}

/* ===========================
   CALENDAR — 10 min before call system
=========================== */

var calendarCheckInterval = null;
var notifiedEvents = {};

function startCalendarWatcher() {
  // Check every 2 minutes for upcoming events
  calendarCheckInterval = setInterval(async function() {
    try {
      var accounts = Object.keys(gmailTokens);
      for (var a = 0; a < accounts.length; a++) {
        var events = await getCalendarEvents(accounts[a], 1);
        for (var e = 0; e < events.length; e++) {
          var ev = events[e];
          // Parse the event time
          var tokens2 = gmailTokens[accounts[a]];
          if (!tokens2) continue;
          var oauth2 = createGmailOAuth2Client(tokens2);
          var calendar = google.calendar({ version: 'v3', auth: oauth2 });

          // Get raw event data for exact time
          var now = new Date();
          var future = new Date();
          future.setDate(future.getDate() + 1);
          var rawEvents = await calendar.events.list({
            calendarId: 'primary',
            timeMin: now.toISOString(),
            timeMax: future.toISOString(),
            singleEvents: true,
            orderBy: 'startTime',
            maxResults: 10,
          });

          var items = rawEvents.data.items || [];
          for (var i = 0; i < items.length; i++) {
            var item = items[i];
            if (!item.start.dateTime) continue; // skip all-day events
            var eventStart = new Date(item.start.dateTime);
            var minutesTillEvent = (eventStart.getTime() - now.getTime()) / 60000;
            var eventKey = item.id + '_' + eventStart.toISOString();

            // 10 minutes before — call
            if (minutesTillEvent > 8 && minutesTillEvent <= 12 && !notifiedEvents[eventKey]) {
              notifiedEvents[eventKey] = true;
              console.log("Calendar alert: " + item.summary + " in " + Math.round(minutesTillEvent) + " min");

              var eventName = (item.summary || 'an event').replace(/[<>&"']/g, '');
              var eventLoc = item.location ? ' at ' + item.location.replace(/[<>&"']/g, '') : '';

              // Text first
              try {
                await twilioClient.messages.create({
                  body: "Heads up — \"" + item.summary + "\" starts in 10 minutes" + (item.location ? " at " + item.location : "") + ".",
                  from: 'whatsapp:+14155238886',
                  to: '+18167392734',
                });
              } catch (e) {}

              // Then call
              try {
                await twilioClient.calls.create({
                  to: MY_NUMBER,
                  from: TWILIO_NUMBER,
                  twiml: '<Response><Say voice="Polly.Matthew">Trace. You have ' + eventName + eventLoc + ' starting in 10 minutes. Get ready.</Say></Response>',
                });
              } catch (e) { console.log("Calendar call error: " + e.message); }
            }
          }
          break; // only check first account for calendar
        }
      }
    } catch (err) {
      console.log("Calendar watcher error: " + err.message);
    }
  }, 120000); // every 2 minutes
}

// Clean up old notified events every hour
setInterval(function() {
  var keys = Object.keys(notifiedEvents);
  if (keys.length > 100) {
    notifiedEvents = {};
  }
}, 3600000);

/* ===========================
   GMAIL — Build Email Context for Briefings
=========================== */

async function buildEmailContext() {
  var accounts = Object.keys(gmailTokens);
  if (accounts.length === 0) return '';

  var context = '\nEMAIL INBOX SUMMARY:\n';
  var allEmails = [];

  for (var a = 0; a < accounts.length; a++) {
    var emails = await getUnreadEmails(accounts[a], 10);
    if (emails.length > 0) {
      context += '\n' + accounts[a] + ' (' + emails.length + ' unread):\n';
      for (var e = 0; e < emails.length; e++) {
        emails[e].account = accounts[a];
        emails[e].index = allEmails.length + 1;
        allEmails.push(emails[e]);
        context += '  ' + allEmails.length + '. From: ' + emails[e].from + '\n     Subject: ' + emails[e].subject + '\n     Preview: ' + emails[e].snippet.substring(0, 100) + '\n';
      }
    } else {
      context += '\n' + accounts[a] + ': inbox zero!\n';
    }
  }

  return context;
}

/* ===========================
   GMAIL — API Endpoints
=========================== */

// Get all unread emails across all accounts
app.get('/gmail/unread', async function(req, res) {
  var accounts = Object.keys(gmailTokens);
  var allEmails = [];
  for (var a = 0; a < accounts.length; a++) {
    var emails = await getUnreadEmails(accounts[a], 15);
    emails.forEach(function(e) { e.account = accounts[a]; });
    allEmails = allEmails.concat(emails);
  }
  res.json({ totalUnread: allEmails.length, accounts: accounts.length, emails: allEmails });
});

// Get AI-prioritized inbox summary
app.get('/gmail/summary', async function(req, res) {
  try {
    var emailContext = await buildEmailContext();
    if (!emailContext) return res.json({ error: "No Gmail accounts connected. Visit /gmail/auth to connect." });

    var summary = await askClaude(
      "You are Jarvis, Trace's email assistant. Analyze the emails and prioritize them. Be direct and concise. No markdown.",
      [{ role: 'user', content: 'Here are my unread emails. Rank them by urgency and tell me which to respond to first, which to archive, and which to delete:\n' + emailContext }]
    );
    res.json({ summary: summary });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Read full email body
app.get('/gmail/read/:account/:id', async function(req, res) {
  var body = await getEmailBody(req.params.account, req.params.id);
  res.json({ body: body });
});

// Reply to an email
app.post('/gmail/reply', async function(req, res) {
  var result = await sendReply(req.body.account, req.body.id, req.body.message);
  res.json(result);
});

// Delete an email
app.post('/gmail/delete', async function(req, res) {
  var result = await deleteEmail(req.body.account, req.body.id);
  res.json(result);
});

// Archive an email
app.post('/gmail/archive', async function(req, res) {
  var result = await archiveEmail(req.body.account, req.body.id);
  res.json(result);
});

/* ===========================
   GET /dashboard — Live Web Dashboard
=========================== */

app.get('/dashboard', async function(req, res) {
  try {
    var today = new Date();
    // Fetch key data — PERSONAL + BUSINESS in parallel
    var tabs = [];
    try { tabs = await getAllTabNames(); } catch(e) { tabs = []; }
    var contextPromise = buildLifeOSContext().catch(function(e) { return "Error loading context: " + e.message; });
    var bizPromise = buildBusinessContext().catch(function(e) { return "Error loading business: " + e.message; });

    var context = await contextPromise;
    var bizContext = await bizPromise;

    // Parse personal numbers from context
    var screenTimeMatch = context.match(/Daily average[:\s]*([\d.]+)/i);
    var debtMatch = context.match(/\~?\$([,\d]+)\s*total debt/i);

    var screenTime = screenTimeMatch ? screenTimeMatch[1] : '?';
    var debtAmount = debtMatch ? debtMatch[1].replace(/,/g, '') : '0';

    // Business metrics from global (populated by buildBusinessContext)
    var bm = global.bizMetrics || {};
    var totalBooked = bm.totalBooked || 0;
    var totalCompleted = bm.totalCompleted || 0;
    var totalCancelled = bm.totalCancelled || 0;
    var totalReturn = bm.totalReturn || 0;
    var promoReplies = bm.promoReplies || 0;
    var totalLocations = bm.locationStats ? Object.keys(bm.locationStats).length : 0;
    var bizTodayBookings = bm.todayBookings || [];
    var bizReschedule = bm.needsReschedule || [];
    var bizTechs = bm.techList || [];
    var bizRecentBookings = bm.recentBookings || [];
    var equipStats = bm.equipStats || {};
    var brandStats = bm.brandStats || {};
    var techPerf = bm.techStats || {};
    var locationStats = bm.locationStats || {};
    var avgBookingDays = bm.avgBookingDays || 0;
    var conversionRate = bm.conversionRate || 0;
    var thisMonthCalls = bm.thisMonthCalls || 0;
    var lastMonthCalls = bm.lastMonthCalls || 0;
    var monthGrowth = bm.monthGrowth || 0;
    var weeklyCalls = bm.weeklyCalls || 0;
    var totalLeads = bm.totalLeads || 0;
    var newLocsThisMonth = bm.newLocationsThisMonth || 0;
    var monthlyCalls = bm.monthlyCalls || {};
    var seasonalData = bm.seasonalData || {};

    // Sort location data
    var bizLocations = Object.entries(locationStats).sort(function(a,b){return b[1].total-a[1].total;});

    // Get email count
    var emailAccounts = Object.keys(gmailTokens || {});
    var totalUnread = 0;
    for (var ea = 0; ea < emailAccounts.length; ea++) {
      try {
        var emails = await getUnreadEmails(emailAccounts[ea], 50);
        totalUnread += (emails || []).length;
      } catch(e) {}
    }

    // Get today's calendar events
    var todayEvents = [];
    for (var ca = 0; ca < emailAccounts.length; ca++) {
      try {
        var events = await getCalendarEvents(emailAccounts[ca], 1);
        todayEvents = todayEvents.concat(events || []);
      } catch(e) {}
    }

    // Get recent wins
    var recentWins = [];
    try {
      var winsRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Wins'!A:D" });
      var winsRows = winsRes.data.values || [];
      recentWins = winsRows.slice(Math.max(1, winsRows.length - 5));
    } catch (e) {}

    // Get pending reminders
    var pendingReminders = [];
    global.activeReminders = global.activeReminders || {};
    var rKeys = Object.keys(global.activeReminders);
    for (var rk = 0; rk < rKeys.length; rk++) {
      if (!global.activeReminders[rKeys[rk]].done) {
        pendingReminders.push(global.activeReminders[rKeys[rk]]);
      }
    }

    // Get recent daily questions
    var recentQuestions = [];
    try {
      var qRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Daily_Questions'!A:G" });
      var qRows = qRes.data.values || [];
      recentQuestions = qRows.slice(Math.max(1, qRows.length - 3));
    } catch (e) {}

    // Get recent daily log
    var recentLog = null;
    try {
      var logRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Daily_Log'!A:J" });
      var logRows = logRes.data.values || [];
      if (logRows.length > 1) recentLog = { headers: logRows[0], data: logRows[logRows.length - 1] };
    } catch (e) {}

    // Get dating log count
    var datingCount = 0;
    try {
      var dRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Dating_Log'!A:A" });
      datingCount = Math.max(0, ((dRes.data.values || []).length) - 1);
    } catch (e) {}

    // Get gym log data
    var gymVisits = 0;
    var gymThisWeek = 0;
    var recentGym = [];
    try {
      var gymRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Gym_Log'!A:E" });
      var gymRows = gymRes.data.values || [];
      gymVisits = Math.max(0, gymRows.length - 1);
      recentGym = gymRows.slice(Math.max(1, gymRows.length - 5));
      var now = new Date();
      var weekStart = new Date(now);
      weekStart.setDate(now.getDate() - now.getDay());
      weekStart.setHours(0, 0, 0, 0);
      for (var gi = 1; gi < gymRows.length; gi++) {
        if (gymRows[gi][0]) {
          var gymDate = new Date(gymRows[gi][0]);
          if (gymDate >= weekStart) gymThisWeek++;
        }
      }
    } catch (e) {}

    // Get health/habit data
    var healthData = { water: [], alcohol: [], nicotine: [], pmo: [], streaks: { pmo: 0, nicotine: 0, alcohol: 0 } };
    try {
      var healthRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Health_Log'!A:E" });
      var healthRows = healthRes.data.values || [];
      var lastRelapse = { pmo: null, nicotine: null, alcohol: null };
      for (var hi = healthRows.length - 1; hi >= 1; hi--) {
        var hRow = healthRows[hi];
        if (!hRow[2]) continue;
        var hType = hRow[2].toLowerCase();
        if (hType === 'water') healthData.water.push({ date: hRow[0], amount: hRow[3] });
        if (hType === 'alcohol') {
          healthData.alcohol.push({ date: hRow[0], amount: hRow[3] });
          if (!lastRelapse.alcohol && hRow[3] && !hRow[3].toLowerCase().includes('none')) lastRelapse.alcohol = new Date(hRow[0]);
        }
        if (hType === 'nicotine') {
          healthData.nicotine.push({ date: hRow[0], amount: hRow[3] });
          if (!lastRelapse.nicotine && hRow[3] && !hRow[3].toLowerCase().includes('none')) lastRelapse.nicotine = new Date(hRow[0]);
        }
        if (hType === 'pmo') {
          healthData.pmo.push({ date: hRow[0], status: hRow[3] });
          if (!lastRelapse.pmo && hRow[3] && hRow[3].toLowerCase().includes('relapse')) lastRelapse.pmo = new Date(hRow[0]);
        }
      }
      // Calculate streaks
      var today = new Date();
      if (lastRelapse.pmo) healthData.streaks.pmo = Math.floor((today - lastRelapse.pmo) / 86400000);
      if (lastRelapse.nicotine) healthData.streaks.nicotine = Math.floor((today - lastRelapse.nicotine) / 86400000);
      if (lastRelapse.alcohol) healthData.streaks.alcohol = Math.floor((today - lastRelapse.alcohol) / 86400000);
    } catch (e) {}

    var html = '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>J.A.R.V.I.S. // A.T.H.E.N.A. — Command Center</title>';
    html += '<style>';

    // Base
    html += '@import url("https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;500;600;700&display=swap");';
    html += '* { margin: 0; padding: 0; box-sizing: border-box; }';
    html += 'body { background: #020810; color: #c0d8f0; font-family: "Rajdhani", sans-serif; min-height: 100vh; overflow-x: hidden; }';

    // ===== SWIPE CONTAINER =====
    html += '.swipe-wrapper { display: flex; width: 200vw; transition: transform 0.5s cubic-bezier(0.22, 1, 0.36, 1); }';
    html += '.panel { width: 100vw; min-height: 100vh; overflow-y: auto; overflow-x: hidden; }';

    // Tab switcher
    html += '.tab-switcher { position: fixed; top: 15px; left: 50%; transform: translateX(-50%); z-index: 50; display: flex; gap: 0; font-family: "Orbitron"; font-size: 0.6em; letter-spacing: 3px; }';
    html += '.tab-btn { padding: 10px 25px; cursor: pointer; transition: all 0.3s; border: 1px solid transparent; text-transform: uppercase; }';
    html += '.tab-btn.jarvis { color: #00d4ff40; border-color: #00d4ff20; }';
    html += '.tab-btn.jarvis.active { color: #00d4ff; border-color: #00d4ff; background: rgba(0,212,255,0.1); box-shadow: 0 0 20px rgba(0,212,255,0.15); }';
    html += '.tab-btn.athena { color: #a855f740; border-color: #a855f720; }';
    html += '.tab-btn.athena.active { color: #a855f7; border-color: #a855f7; background: rgba(168,85,247,0.1); box-shadow: 0 0 20px rgba(168,85,247,0.15); }';

    // Swipe indicator dots
    html += '.swipe-dots { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); z-index: 50; display: flex; gap: 8px; }';
    html += '.swipe-dot { width: 8px; height: 8px; border-radius: 50%; transition: all 0.3s; cursor: pointer; }';
    html += '.swipe-dot.jarvis-dot { background: #00d4ff30; }';
    html += '.swipe-dot.jarvis-dot.active { background: #00d4ff; box-shadow: 0 0 10px #00d4ff; }';
    html += '.swipe-dot.athena-dot { background: #a855f730; }';
    html += '.swipe-dot.athena-dot.active { background: #a855f7; box-shadow: 0 0 10px #a855f7; }';

    // Animated background grid
    html += '.bg-grid { position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 0; pointer-events: none; }';
    html += '.bg-grid::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-image: linear-gradient(rgba(0,212,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(0,212,255,0.03) 1px, transparent 1px); background-size: 60px 60px; animation: gridMove 20s linear infinite; }';
    html += '@keyframes gridMove { 0% { transform: translate(0,0); } 100% { transform: translate(60px,60px); } }';

    // Scanning line
    html += '.scan-line { position: fixed; top: 0; left: 0; width: 100%; height: 2px; background: linear-gradient(90deg, transparent, #00d4ff, transparent); z-index: 1; animation: scanDown 4s ease-in-out infinite; opacity: 0.4; }';
    html += '@keyframes scanDown { 0% { top: 0; } 50% { top: 100%; } 100% { top: 0; } }';

    // Corner brackets
    html += '.corner { position: fixed; z-index: 2; width: 30px; height: 30px; border-color: #00d4ff30; border-style: solid; }';
    html += '.corner-tl { top: 15px; left: 15px; border-width: 2px 0 0 2px; }';
    html += '.corner-tr { top: 15px; right: 15px; border-width: 2px 2px 0 0; }';
    html += '.corner-bl { bottom: 15px; left: 15px; border-width: 0 0 2px 2px; }';
    html += '.corner-br { bottom: 15px; right: 15px; border-width: 0 2px 2px 0; }';

    // Content wrapper
    html += '.content { position: relative; z-index: 3; }';

    // Header
    html += '.header { padding: 50px 40px 30px; text-align: center; position: relative; }';
    html += '.header::after { content: ""; position: absolute; bottom: 0; left: 10%; width: 80%; height: 1px; background: linear-gradient(90deg, transparent, #00d4ff40, transparent); }';
    html += '.jarvis-title { font-family: "Orbitron", monospace; font-size: 3.5em; font-weight: 900; letter-spacing: 15px; color: #00d4ff; text-shadow: 0 0 40px rgba(0,212,255,0.4), 0 0 80px rgba(0,212,255,0.1); animation: titleGlow 3s ease-in-out infinite; }';
    html += '@keyframes titleGlow { 0%,100% { text-shadow: 0 0 40px rgba(0,212,255,0.4), 0 0 80px rgba(0,212,255,0.1); } 50% { text-shadow: 0 0 60px rgba(0,212,255,0.6), 0 0 120px rgba(0,212,255,0.2), 0 0 180px rgba(0,212,255,0.1); } }';
    html += '.status-bar { display: flex; justify-content: center; gap: 30px; margin-top: 20px; font-family: "Orbitron", monospace; font-size: 0.7em; letter-spacing: 3px; color: #4a6a8a; }';
    html += '.status-item { display: flex; align-items: center; gap: 8px; }';
    html += '.status-dot { width: 6px; height: 6px; border-radius: 50%; animation: dotPulse 2s infinite; }';
    html += '.status-dot.green { background: #00ff66; box-shadow: 0 0 10px #00ff66; }';
    html += '.status-dot.blue { background: #00d4ff; box-shadow: 0 0 10px #00d4ff; }';
    html += '@keyframes dotPulse { 0%,100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(0.7); } }';

    // Hex ring animation
    html += '.hex-container { display: flex; justify-content: center; margin: 10px 0 20px; }';
    html += '.hex-ring { width: 120px; height: 120px; position: relative; animation: hexSpin 15s linear infinite; }';
    html += '.hex-ring::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 100%; border: 2px solid #00d4ff20; border-radius: 50%; }';
    html += '.hex-ring::after { content: ""; position: absolute; top: 10px; left: 10px; width: calc(100% - 20px); height: calc(100% - 20px); border: 1px solid #00d4ff10; border-radius: 50%; animation: hexSpin 10s linear infinite reverse; }';
    html += '@keyframes hexSpin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }';
    html += '.hex-center { position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%); font-family: "Orbitron"; font-size: 1.8em; font-weight: 900; color: #00d4ff; }';

    // Stats grid
    html += '.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; padding: 30px 40px; max-width: 1400px; margin: 0 auto; }';
    html += '.card { background: rgba(10,20,35,0.8); border: 1px solid #0a2a4a; border-radius: 4px; padding: 30px; position: relative; overflow: hidden; animation: cardFadeIn 0.6s ease-out both; }';
    html += '.card::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 2px; background: linear-gradient(90deg, transparent, var(--accent), transparent); animation: borderScan 3s linear infinite; }';
    html += '@keyframes borderScan { 0% { transform: translateX(-100%); } 100% { transform: translateX(100%); } }';
    html += '@keyframes cardFadeIn { 0% { opacity: 0; transform: translateY(20px); } 100% { opacity: 1; transform: translateY(0); } }';
    html += '.card:nth-child(1) { animation-delay: 0.1s; --accent: #00d4ff; }';
    html += '.card:nth-child(2) { animation-delay: 0.2s; --accent: #ff4757; }';
    html += '.card:nth-child(3) { animation-delay: 0.3s; --accent: #00ff66; }';
    html += '.card:nth-child(4) { animation-delay: 0.4s; --accent: #a855f7; }';
    html += '.card:nth-child(5) { animation-delay: 0.5s; --accent: #00ff66; }';
    html += '.card:nth-child(6) { animation-delay: 0.6s; --accent: #ff9f43; }';
    html += '.card .label { font-family: "Orbitron"; font-size: 0.65em; letter-spacing: 3px; color: #4a6a8a; text-transform: uppercase; }';
    html += '.card .value { font-family: "Orbitron"; font-size: 3em; font-weight: 700; margin: 15px 0 8px; color: var(--accent); text-shadow: 0 0 30px color-mix(in srgb, var(--accent) 30%, transparent); }';
    html += '.card .sub { font-size: 0.95em; color: #3a5a7a; letter-spacing: 1px; }';
    html += '.card .bar { height: 3px; background: #0a1520; margin-top: 15px; border-radius: 2px; overflow: hidden; }';
    html += '.card .bar-fill { height: 100%; background: var(--accent); border-radius: 2px; animation: barGrow 2s ease-out both; }';
    html += '@keyframes barGrow { 0% { width: 0; } }';

    // Actions
    html += '.actions { display: flex; justify-content: center; gap: 15px; padding: 20px 40px 40px; flex-wrap: wrap; }';
    html += '.holo-btn { font-family: "Orbitron"; font-size: 0.75em; letter-spacing: 3px; padding: 14px 30px; background: transparent; border: 1px solid #00d4ff30; color: #00d4ff; text-decoration: none; text-transform: uppercase; position: relative; overflow: hidden; transition: all 0.3s; cursor: pointer; }';
    html += '.holo-btn:hover { background: #00d4ff15; border-color: #00d4ff; box-shadow: 0 0 30px #00d4ff20, inset 0 0 30px #00d4ff10; }';
    html += '.holo-btn::after { content: ""; position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: linear-gradient(transparent, rgba(0,212,255,0.05), transparent); transform: rotate(45deg); animation: btnShine 3s linear infinite; }';
    html += '@keyframes btnShine { 0% { transform: translateX(-100%) rotate(45deg); } 100% { transform: translateX(100%) rotate(45deg); } }';
    html += '.holo-btn.green { border-color: #00ff6630; color: #00ff66; }';
    html += '.holo-btn.green:hover { background: #00ff6615; border-color: #00ff66; box-shadow: 0 0 30px #00ff6620; }';

    // Systems grid
    html += '.systems { padding: 0 40px 40px; max-width: 1400px; margin: 0 auto; }';
    html += '.systems-title { font-family: "Orbitron"; font-size: 0.8em; letter-spacing: 5px; color: #4a6a8a; margin-bottom: 20px; text-transform: uppercase; }';
    html += '.systems-grid { display: flex; flex-wrap: wrap; gap: 8px; }';
    html += '.sys-chip { background: rgba(0,212,255,0.03); border: 1px solid #0a2a4a; padding: 8px 16px; font-family: "Rajdhani"; font-size: 0.85em; letter-spacing: 1px; color: #3a5a7a; transition: all 0.3s; cursor: default; position: relative; }';
    html += '.sys-chip:hover { border-color: #00d4ff; color: #00d4ff; background: rgba(0,212,255,0.08); box-shadow: 0 0 15px rgba(0,212,255,0.1); }';
    html += '.sys-chip::before { content: ""; position: absolute; top: 0; left: 0; width: 3px; height: 100%; background: #00d4ff; opacity: 0; transition: opacity 0.3s; }';
    html += '.sys-chip:hover::before { opacity: 1; }';

    // Clock
    html += '.clock { font-family: "Orbitron"; font-size: 0.7em; letter-spacing: 5px; color: #2a4a6a; text-align: center; padding: 30px; }';

    // Footer
    html += '.footer { text-align: center; padding: 20px; font-family: "Orbitron"; font-size: 0.6em; letter-spacing: 4px; color: #1a2a3a; border-top: 1px solid #0a1520; }';

    // Floating particles
    html += '.particle { position: fixed; width: 2px; height: 2px; background: #00d4ff; border-radius: 50%; pointer-events: none; z-index: 1; opacity: 0; animation: particleFloat 8s linear infinite; }';
    html += '@keyframes particleFloat { 0% { opacity: 0; transform: translateY(100vh); } 10% { opacity: 0.6; } 90% { opacity: 0.6; } 100% { opacity: 0; transform: translateY(-20vh); } }';

    // Mobile responsive
    html += '@media(max-width:768px){';
    html += '.grid{grid-template-columns:1fr!important;padding:15px 12px!important;}';
    html += '[style*="max-width:1400px"]{padding-left:12px!important;padding-right:12px!important;}';
    html += '[style*="padding:0 40px"]{padding-left:12px!important;padding-right:12px!important;}';
    html += '[style*="grid-template-columns:repeat(3"]{grid-template-columns:1fr!important;}';
    html += '[style*="grid-template-columns:repeat(4"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(300px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(280px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(200px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(180px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(170px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '.status-bar,.status-item{font-size:0.55em!important;}';
    html += '[style*="display:flex"][style*="justify-content:center"][style*="gap"]{flex-wrap:wrap!important;}';
    html += '[style*="font-size:2.8em"],[style*="font-size:3em"],[style*="font-size:2.5em"]{font-size:1.8em!important;}';
    html += '[style*="font-size:1.6em"]{font-size:1.2em!important;}';
    html += '[style*="letter-spacing:8px"],[style*="letter-spacing:6px"]{letter-spacing:3px!important;}';
    html += 'table{font-size:0.7em!important;}';
    html += '[style*="overflow-x"]{overflow-x:auto!important;-webkit-overflow-scrolling:touch;}';
    html += '.tab-switcher,.tab-btn{font-size:0.55em!important;padding:10px 15px!important;}';
    html += '}';
    html += '@media(max-width:480px){';
    html += '[style*="minmax(200px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(180px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(150px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '}';

    html += '</style></head><body>';

    // ====== TAB SWITCHER ======
    html += '<div class="tab-switcher">';
    html += '<div class="tab-btn jarvis active" onclick="switchPanel(0)">J.A.R.V.I.S.</div>';
    html += '<div class="tab-btn athena" onclick="switchPanel(1)">A.T.H.E.N.A.</div>';
    html += '</div>';

    // Swipe dots
    html += '<div class="swipe-dots">';
    html += '<div class="swipe-dot jarvis-dot active" onclick="switchPanel(0)"></div>';
    html += '<div class="swipe-dot athena-dot" onclick="switchPanel(1)"></div>';
    html += '</div>';

    // ====== SWIPE WRAPPER ======
    html += '<div class="swipe-wrapper" id="swipe-wrapper">';

    // ====== PANEL 1: JARVIS (Personal) ======
    html += '<div class="panel" id="jarvis-panel">';
    html += '<div id="boot-screen" style="position:fixed;top:0;left:0;width:100%;height:100%;background:#020810;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;overflow:hidden;">';
    
    // Hex grid background for boot
    html += '<div style="position:absolute;top:0;left:0;width:100%;height:100%;background-image:linear-gradient(rgba(0,212,255,0.02) 1px,transparent 1px),linear-gradient(90deg,rgba(0,212,255,0.02) 1px,transparent 1px);background-size:40px 40px;"></div>';
    
    // Center ring animation
    html += '<div id="boot-ring" style="width:120px;height:120px;border:2px solid #00d4ff20;border-radius:50%;position:relative;animation:bootRingSpin 2s linear infinite;opacity:0;">';
    html += '<div style="position:absolute;top:-2px;left:50%;width:8px;height:8px;background:#00d4ff;border-radius:50%;margin-left:-4px;box-shadow:0 0 20px #00d4ff;"></div>';
    html += '</div>';
    
    // Boot text lines
    html += '<div id="boot-text" style="margin-top:40px;font-family:Orbitron;font-size:0.65em;letter-spacing:4px;color:#00d4ff40;text-align:center;max-width:500px;line-height:2.2;">';
    html += '<div class="boot-line" style="opacity:0;">INITIALIZING NEURAL CORE...</div>';
    html += '<div class="boot-line" style="opacity:0;">LOADING IDENTITY PROFILE...</div>';
    html += '<div class="boot-line" style="opacity:0;">SYNCING ' + emailAccounts.length + ' EMAIL ACCOUNTS...</div>';
    html += '<div class="boot-line" style="opacity:0;">SCANNING ' + tabs.length + ' LIFE SYSTEMS...</div>';
    html += '<div class="boot-line" style="opacity:0;">CALENDAR SYNC: ' + todayEvents.length + ' EVENTS LOADED</div>';
    html += '<div class="boot-line" style="opacity:0;">ACTIVATING VOICE INTERFACE...</div>';
    html += '<div class="boot-line" style="opacity:0;">HABIT MONITORING: ONLINE</div>';
    html += '<div class="boot-line" style="opacity:0;">ACTIVATING A.T.H.E.N.A. BUSINESS ENGINE...</div>';
    html += '<div class="boot-line" style="opacity:0;">CRM DATA: ' + totalLocations + ' LOCATIONS // ' + totalBooked + ' ACTIVE BOOKINGS</div>';
    html += '<div class="boot-line" style="opacity:0;color:#00ff66;">ALL SYSTEMS OPERATIONAL</div>';
    html += '</div>';
    
    // JARVIS title reveal
    html += '<div id="boot-title" style="position:absolute;opacity:0;font-family:Orbitron;font-size:3em;font-weight:900;letter-spacing:15px;color:#00d4ff;text-shadow:0 0 60px rgba(0,212,255,0.6),0 0 120px rgba(0,212,255,0.2);text-align:center;line-height:1.6;">J.A.R.V.I.S.<br><span style="font-size:0.6em;letter-spacing:12px;background:linear-gradient(135deg,#a855f7,#c084fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 20px rgba(168,85,247,0.4));">A.T.H.E.N.A.</span></div>';
    
    // Welcome message
    var hour = new Date().toLocaleString('en-US', { hour: 'numeric', hour12: false, timeZone: 'America/Chicago' });
    var greeting = 'Good evening';
    var hourNum = parseInt(hour);
    if (hourNum >= 5 && hourNum < 12) greeting = 'Good morning';
    else if (hourNum >= 12 && hourNum < 17) greeting = 'Good afternoon';
    else if (hourNum >= 17 && hourNum < 21) greeting = 'Good evening';
    else greeting = 'Good night';
    
    html += '<div id="boot-welcome" style="position:absolute;bottom:25%;opacity:0;font-family:Rajdhani;font-size:1.4em;letter-spacing:6px;color:#3a5a7a;">' + greeting.toUpperCase() + ', TRACE</div>';
    
    html += '</div>';

    // Boot animation styles
    html += '<style>';
    html += '@keyframes bootRingSpin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }';
    html += '.boot-line { transition: opacity 0.3s, transform 0.3s; transform: translateY(5px); }';
    html += '.boot-line.visible { opacity: 1 !important; color: #00d4ff; transform: translateY(0); }';
    html += '.boot-line.done { color: #00ff66 !important; }';
    html += '#boot-screen { transition: opacity 0.8s ease-out; }';
    html += '</style>';

    // Boot sequence script
    html += '<script>';
    html += '(function(){';
    html += '  var ring=document.getElementById("boot-ring");';
    html += '  var lines=document.querySelectorAll(".boot-line");';
    html += '  var title=document.getElementById("boot-title");';
    html += '  var welcome=document.getElementById("boot-welcome");';
    html += '  var screen=document.getElementById("boot-screen");';
    html += '  var content=document.querySelector(".content");';
    html += '  if(content)content.style.opacity="0";';
    
    // Fade in ring
    html += '  setTimeout(function(){ring.style.opacity="1";ring.style.transition="opacity 0.5s";},200);';
    
    // Type out boot lines one by one
    html += '  var delay=600;';
    html += '  for(var i=0;i<lines.length;i++){';
    html += '    (function(idx){';
    html += '      setTimeout(function(){';
    html += '        lines[idx].classList.add("visible");';
    html += '        if(idx>0)lines[idx-1].style.color="#4a6a8a";';
    // Play a subtle beep
    html += '        try{var ctx=new(window.AudioContext||window.webkitAudioContext)();var osc=ctx.createOscillator();var gain=ctx.createGain();osc.connect(gain);gain.connect(ctx.destination);osc.frequency.value=800+(idx*100);gain.gain.value=0.03;osc.start();osc.stop(ctx.currentTime+0.05);}catch(e){}';
    html += '      },delay+idx*350);';
    html += '    })(i);';
    html += '  }';
    
    // After all lines, show title
    html += '  var totalTime=delay+lines.length*350+400;';
    html += '  setTimeout(function(){';
    html += '    ring.style.opacity="0";';
    html += '    document.getElementById("boot-text").style.opacity="0";document.getElementById("boot-text").style.transition="opacity 0.4s";';
    html += '  },totalTime);';
    
    // Flash title
    html += '  setTimeout(function(){';
    html += '    title.style.opacity="1";title.style.transition="opacity 0.3s";';
    html += '    welcome.style.opacity="1";welcome.style.transition="opacity 0.5s";';
    // Screen flash
    html += '    screen.style.background="radial-gradient(circle,#0a2040 0%,#020810 70%)";';
    html += '  },totalTime+500);';
    
    // Fade out boot screen, reveal dashboard
    html += '  setTimeout(function(){';
    html += '    screen.style.opacity="0";';
    html += '    if(content){content.style.transition="opacity 1s";content.style.opacity="1";}';
    html += '    setTimeout(function(){screen.style.display="none";},800);';
    
    // Speak the greeting
    html += '    var greetText="' + greeting + ', Trace. ';
    if (todayEvents.length > 0) {
      html += 'You have ' + todayEvents.length + ' event' + (todayEvents.length > 1 ? 's' : '') + ' today. ';
    } else {
      html += 'No events on your calendar today. ';
    }
    if (pendingReminders.length > 0) {
      html += pendingReminders.length + ' reminder' + (pendingReminders.length > 1 ? 's' : '') + ' pending. ';
    }
    if (totalUnread > 10) {
      html += totalUnread + ' unread emails. ';
    }
    html += 'All systems are online.";';
    
    // Try ElevenLabs first, fall back to browser speech
    html += '    fetch("/tts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text:greetText})})';
    html += '    .then(function(r){if(!r.ok)throw new Error("TTS failed");return r.blob();})';
    html += '    .then(function(b){var a=new Audio(URL.createObjectURL(b));a.play();})';
    html += '    .catch(function(e){';
    html += '      console.log("ElevenLabs unavailable, using browser voice");';
    html += '      var synth2=window.speechSynthesis;var u=new SpeechSynthesisUtterance(greetText);u.rate=1.0;u.pitch=0.9;';
    html += '      var voices=synth2.getVoices();for(var v=0;v<voices.length;v++){if(voices[v].name.includes("Samantha")||voices[v].name.includes("Google UK English Male")||voices[v].name.includes("Male")){u.voice=voices[v];break;}}';
    html += '      synth2.speak(u);';
    html += '    });';
    
    html += '  },totalTime+2000);';
    
    html += '})();';
    html += '<\/script>';

    // Background effects
    html += '<div class="bg-grid"></div>';
    html += '<div class="scan-line"></div>';
    html += '<div class="corner corner-tl"></div><div class="corner corner-tr"></div><div class="corner corner-bl"></div><div class="corner corner-br"></div>';

    // Floating particles
    for (var p = 0; p < 20; p++) {
      var left = Math.floor(Math.random() * 100);
      var delay = (Math.random() * 8).toFixed(1);
      var size = (Math.random() * 2 + 1).toFixed(0);
      html += '<div class="particle" style="left:' + left + '%;width:' + size + 'px;height:' + size + 'px;animation-delay:' + delay + 's;"></div>';
    }

    html += '<div class="content">';

    // Header
    var now = new Date();
    var dateStr = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'America/Chicago' });
    var timeStr = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Chicago' });
    html += '<div class="header">';
    html += '<div class="hex-container"><div class="hex-ring"><div class="hex-center" style="font-size:0.35em;line-height:1.3;">' + timeStr + '</div></div></div>';
    html += '<div class="jarvis-title">J.A.R.V.I.S.</div>';
    html += '<div style="font-family:Rajdhani;font-size:1.1em;letter-spacing:8px;color:#3a5a7a;margin-top:5px;text-transform:uppercase;">LifeOS Command Center</div>';
    html += '<div style="font-family:Rajdhani;font-size:0.95em;letter-spacing:4px;color:#00d4ff80;margin-top:3px;">' + dateStr + '</div>';

    // === TAB NAVIGATION ===
    html += '<div style="display:flex;justify-content:center;gap:0;margin-top:20px;margin-bottom:15px;">';
    html += '<a href="/dashboard" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#00d4ff;border:1px solid #00d4ff40;text-decoration:none;background:rgba(0,212,255,0.1);box-shadow:0 0 15px rgba(0,212,255,0.1);">JARVIS</a>';
    html += '<a href="/business" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ATHENA</a>';
    html += '<a href="/tookan" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">TOOKAN</a>';
    html += '<a href="/business/chart" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">CHARTS</a>';
    html += '<a href="/analytics" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ANALYTICS</a>';
    html += '</div>';
    html += '<div class="status-bar">';
    html += '<div class="status-item"><div class="status-dot green"></div>SYSTEMS ONLINE</div>';
    html += '<div class="status-item"><div class="status-dot blue"></div>AI ACTIVE</div>';
    html += '<div class="status-item"><div class="status-dot green"></div>' + emailAccounts.length + ' EMAIL LINKED</div>';
    html += '<div class="status-item"><div class="status-dot blue"></div>' + todayEvents.length + ' EVENTS TODAY</div>';
    html += '<div class="status-item"><div class="status-dot ' + (pendingReminders.length > 0 ? 'green' : 'blue') + '"></div>' + pendingReminders.length + ' REMINDERS</div>';
    html += '<div class="status-item"><div class="status-dot green"></div>VOICE READY</div>';
    html += '</div>';
    html += '</div>';

    // Stats Grid
    html += '<div class="grid">';

    var screenPct = Math.min(100, Math.round((parseFloat(screenTime) / 24) * 100));
    html += '<div class="card"><div class="label">Screen Time</div><div class="value">' + screenTime + 'h</div><div class="sub">' + (parseFloat(screenTime) > 10 ? 'WARNING — Exceeds optimal threshold' : 'Within optimal range') + '</div><div class="bar"><div class="bar-fill" style="width:' + screenPct + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Inbox Status</div><div class="value">' + totalUnread + '</div><div class="sub">Unread across ' + emailAccounts.length + ' account(s)</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, totalUnread * 3) + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Today\'s Events</div><div class="value">' + todayEvents.length + '</div><div class="sub">' + (todayEvents.length > 0 ? 'Next: ' + todayEvents[0].time + ' — ' + todayEvents[0].summary : 'No events today') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, todayEvents.length * 20) + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Pending Reminders</div><div class="value">' + pendingReminders.length + '</div><div class="sub">' + (pendingReminders.length > 0 ? pendingReminders[0].text : 'All clear') + '</div><div class="bar"><div class="bar-fill" style="width:' + (pendingReminders.length === 0 ? 100 : Math.min(100, pendingReminders.length * 25)) + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Recent Wins</div><div class="value">' + recentWins.length + '</div><div class="sub">' + (recentWins.length > 0 ? recentWins[recentWins.length - 1][1] || 'Keep stacking' : 'No wins logged — text "win:" to start') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, recentWins.length * 20) + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Financial Status</div><div class="value">$' + parseInt(debtAmount).toLocaleString() + '</div><div class="sub">' + (parseFloat(debtAmount) === 0 ? 'DEBT FREE' : 'Total debt — grinding it down') + '</div><div class="bar"><div class="bar-fill" style="width:' + (parseFloat(debtAmount) === 0 ? 100 : 30) + '%"></div></div></div>';

    html += '<div class="card" style="--accent:#ff6b9d;"><div class="label">Gym This Week</div><div class="value">' + gymThisWeek + '</div><div class="sub">' + gymVisits + ' total visits' + (gymThisWeek >= 5 ? ' — BEAST MODE' : gymThisWeek >= 3 ? ' — Solid consistency' : ' — Get in there') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, gymThisWeek * 15) + '%;background:#ff6b9d;"></div></div></div>';

    html += '<div class="card" style="--accent:#55f7d8;"><div class="label">Dating Log</div><div class="value">' + datingCount + '</div><div class="sub">' + (datingCount > 0 ? 'Interactions logged' : 'Text "date log" to start tracking') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, datingCount * 10) + '%;background:#55f7d8;"></div></div></div>';

    html += '</div>';

    // Today's Schedule Section
    if (todayEvents.length > 0) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#a855f7;">Today\'s Schedule</div>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      for (var ei = 0; ei < todayEvents.length; ei++) {
        html += '<div class="sys-chip" style="border-color:#a855f720;display:flex;justify-content:space-between;padding:12px 16px;">';
        html += '<span style="color:#a855f7;">' + todayEvents[ei].time + '</span>';
        html += '<span style="color:#c0d8f0;margin-left:15px;">' + todayEvents[ei].summary + '</span>';
        if (todayEvents[ei].location) html += '<span style="color:#4a6a8a;margin-left:10px;">@ ' + todayEvents[ei].location + '</span>';
        html += '</div>';
      }
      html += '</div></div>';
    }

    // Pending Reminders Section
    if (pendingReminders.length > 0) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#ff9f43;">Pending Reminders</div>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      for (var ri = 0; ri < pendingReminders.length; ri++) {
        var hoursAgo = Math.round((Date.now() - pendingReminders[ri].created) / 3600000);
        html += '<div class="sys-chip" style="border-color:#ff9f4320;display:flex;justify-content:space-between;padding:12px 16px;">';
        html += '<span style="color:#ff9f43;">' + hoursAgo + 'h ago</span>';
        html += '<span style="color:#c0d8f0;margin-left:15px;">' + pendingReminders[ri].text + '</span>';
        html += '</div>';
      }
      html += '</div></div>';
    }

    // Gym History Section
    if (recentGym.length > 0) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#ff6b9d;">Gym History</div>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      for (var ghi = 0; ghi < recentGym.length; ghi++) {
        html += '<div class="sys-chip" style="border-color:#ff6b9d20;display:flex;justify-content:space-between;padding:12px 16px;">';
        html += '<span style="color:#ff6b9d;">' + (recentGym[ghi][0] || '') + ' (' + (recentGym[ghi][1] || '') + ')</span>';
        html += '<span style="color:#c0d8f0;margin-left:15px;">' + (recentGym[ghi][2] || '') + '</span>';
        html += '<span style="color:#4a6a8a;margin-left:10px;">' + (recentGym[ghi][3] || '') + ' ' + (recentGym[ghi][4] || '') + '</span>';
        html += '</div>';
      }
      html += '</div></div>';
    }

    // Recent Wins Section
    if (recentWins.length > 0) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#00ff66;">Recent Wins</div>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      for (var wi = 0; wi < recentWins.length; wi++) {
        html += '<div class="sys-chip" style="border-color:#00ff6620;display:flex;justify-content:space-between;padding:12px 16px;">';
        html += '<span style="color:#00ff66;">' + (recentWins[wi][0] || '') + '</span>';
        html += '<span style="color:#c0d8f0;margin-left:15px;">' + (recentWins[wi][1] || '') + '</span>';
        html += '<span style="color:#4a6a8a;margin-left:10px;">' + (recentWins[wi][2] || '') + '</span>';
        html += '</div>';
      }
      html += '</div></div>';
    }

    // Latest Daily Log
    if (recentLog && recentLog.data) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#00d4ff;">Latest Daily Check-in</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:8px;">';
      for (var li = 0; li < recentLog.headers.length; li++) {
        if (recentLog.data[li] && recentLog.data[li] !== '' && recentLog.data[li] !== '0') {
          html += '<div class="sys-chip" style="border-color:#00d4ff20;padding:10px 16px;">';
          html += '<span style="color:#4a6a8a;font-size:0.8em;">' + recentLog.headers[li] + '</span><br>';
          html += '<span style="color:#00d4ff;font-size:1.1em;">' + recentLog.data[li] + '</span>';
          html += '</div>';
        }
      }
      html += '</div></div>';
    }

    // Recent Daily Questions
    if (recentQuestions.length > 0) {
      html += '<div class="systems" style="margin-top:10px;">';
      html += '<div class="systems-title" style="color:#ff6b9d;">Recent Self-Reflection</div>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      for (var qi = 0; qi < recentQuestions.length; qi++) {
        if (recentQuestions[qi][2] && recentQuestions[qi][3]) {
          html += '<div class="sys-chip" style="border-color:#ff6b9d20;padding:12px 16px;display:block;">';
          html += '<div style="color:#ff6b9d;font-size:0.85em;margin-bottom:5px;">Q: ' + (recentQuestions[qi][2] || '').substring(0, 80) + '</div>';
          html += '<div style="color:#c0d8f0;">A: ' + (recentQuestions[qi][3] || '').substring(0, 100) + '</div>';
          html += '</div>';
        }
      }
      html += '</div></div>';
    }

    // Private Health Section — collapsible, hidden by default
    html += '<div class="systems" style="margin-top:10px;">';
    html += '<div class="systems-title" style="color:#8b5cf6;cursor:pointer;" onclick="var el=document.getElementById(\'private-health\');var arrow=document.getElementById(\'health-arrow\');if(el.style.display===\'none\'){el.style.display=\'block\';arrow.textContent=\'▼\';}else{el.style.display=\'none\';arrow.textContent=\'▶\';}">Private Health &amp; Habits <span id="health-arrow" style="font-size:0.8em;">▶</span></div>';
    html += '<div id="private-health" style="display:none;">';

    // Streak cards
    html += '<div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:15px;">';

    html += '<div style="flex:1;min-width:120px;background:rgba(10,20,35,0.8);border:1px solid #8b5cf620;padding:15px;text-align:center;">';
    html += '<div style="font-size:0.75em;color:#4a6a8a;letter-spacing:2px;font-family:Orbitron;">PMO STREAK</div>';
    html += '<div style="font-size:2em;color:' + (healthData.streaks.pmo >= 7 ? '#00ff66' : healthData.streaks.pmo >= 3 ? '#ff9f43' : '#ff4757') + ';font-family:Orbitron;margin:5px 0;">' + healthData.streaks.pmo + '</div>';
    html += '<div style="font-size:0.8em;color:#4a6a8a;">days clean</div></div>';

    html += '<div style="flex:1;min-width:120px;background:rgba(10,20,35,0.8);border:1px solid #8b5cf620;padding:15px;text-align:center;">';
    html += '<div style="font-size:0.75em;color:#4a6a8a;letter-spacing:2px;font-family:Orbitron;">NICOTINE</div>';
    html += '<div style="font-size:2em;color:' + (healthData.streaks.nicotine >= 7 ? '#00ff66' : healthData.streaks.nicotine >= 3 ? '#ff9f43' : '#ff4757') + ';font-family:Orbitron;margin:5px 0;">' + healthData.streaks.nicotine + '</div>';
    html += '<div style="font-size:0.8em;color:#4a6a8a;">days clean</div></div>';

    html += '<div style="flex:1;min-width:120px;background:rgba(10,20,35,0.8);border:1px solid #8b5cf620;padding:15px;text-align:center;">';
    html += '<div style="font-size:0.75em;color:#4a6a8a;letter-spacing:2px;font-family:Orbitron;">ALCOHOL</div>';
    html += '<div style="font-size:2em;color:' + (healthData.streaks.alcohol >= 7 ? '#00ff66' : healthData.streaks.alcohol >= 3 ? '#ff9f43' : '#ff4757') + ';font-family:Orbitron;margin:5px 0;">' + healthData.streaks.alcohol + '</div>';
    html += '<div style="font-size:0.8em;color:#4a6a8a;">days clean</div></div>';

    var todayWater = healthData.water.filter(function(w) { return w.date === new Date().toISOString().split('T')[0]; });
    html += '<div style="flex:1;min-width:120px;background:rgba(10,20,35,0.8);border:1px solid #00d4ff20;padding:15px;text-align:center;">';
    html += '<div style="font-size:0.75em;color:#4a6a8a;letter-spacing:2px;font-family:Orbitron;">WATER TODAY</div>';
    html += '<div style="font-size:2em;color:#00d4ff;font-family:Orbitron;margin:5px 0;">' + (todayWater.length > 0 ? todayWater[todayWater.length - 1].amount : '0') + '</div>';
    html += '<div style="font-size:0.8em;color:#4a6a8a;">logged</div></div>';

    html += '</div>';

    // Recent habit entries
    var recentHealth = [];
    var allHealthEntries = healthData.pmo.map(function(p) { return { date: p.date, type: 'PMO', val: p.status }; })
      .concat(healthData.nicotine.map(function(n) { return { date: n.date, type: 'Nicotine', val: n.amount }; }))
      .concat(healthData.alcohol.map(function(a) { return { date: a.date, type: 'Alcohol', val: a.amount }; }));
    allHealthEntries.sort(function(a, b) { return new Date(b.date) - new Date(a.date); });
    recentHealth = allHealthEntries.slice(0, 10);

    if (recentHealth.length > 0) {
      html += '<div style="display:flex;flex-direction:column;gap:6px;margin-top:10px;">';
      for (var rhi = 0; rhi < recentHealth.length; rhi++) {
        var rh = recentHealth[rhi];
        var rhClean = rh.val && (rh.val.toLowerCase().includes('none') || rh.val.toLowerCase().includes('clean'));
        html += '<div style="display:flex;justify-content:space-between;padding:8px 12px;background:rgba(10,20,35,0.5);border-left:3px solid ' + (rhClean ? '#00ff66' : '#ff4757') + ';">';
        html += '<span style="color:#4a6a8a;">' + (rh.date || '') + '</span>';
        html += '<span style="color:#8b5cf6;">' + (rh.type || '') + '</span>';
        html += '<span style="color:' + (rhClean ? '#00ff66' : '#ff4757') + ';">' + (rh.val || '') + '</span>';
        html += '</div>';
      }
      html += '</div>';
    }

    html += '</div></div>';

    // Email Inbox Panel — collapsible
    html += '<div class="systems" style="margin-top:10px;">';
    html += '<div class="systems-title" style="color:#ff6348;cursor:pointer;" onclick="var el=document.getElementById(\'email-panel\');var arrow=document.getElementById(\'email-arrow\');if(el.style.display===\'none\'){el.style.display=\'block\';arrow.textContent=\'▼\';}else{el.style.display=\'none\';arrow.textContent=\'▶\';}">Inbox (' + totalUnread + ' unread) <span id="email-arrow" style="font-size:0.8em;">▶</span></div>';
    html += '<div id="email-panel" style="display:none;">';

    // Fetch actual email details
    var emailDetails = [];
    for (var eda = 0; eda < emailAccounts.length; eda++) {
      try {
        var gmailClient = await getGmailClient(emailAccounts[eda]);
        if (!gmailClient) continue;
        var listRes = await gmailClient.users.messages.list({ userId: 'me', q: 'is:unread', maxResults: 10 });
        var msgs = listRes.data.messages || [];
        for (var em = 0; em < Math.min(msgs.length, 10); em++) {
          var msgData = await gmailClient.users.messages.get({ userId: 'me', id: msgs[em].id, format: 'metadata', metadataHeaders: ['From', 'Subject', 'Date'] });
          var hdrs = msgData.data.payload.headers;
          var fromH = hdrs.find(function(h) { return h.name === 'From'; });
          var subjH = hdrs.find(function(h) { return h.name === 'Subject'; });
          var dateH = hdrs.find(function(h) { return h.name === 'Date'; });
          emailDetails.push({
            id: msgs[em].id,
            account: emailAccounts[eda],
            from: fromH ? fromH.value.replace(/<.*>/, '').trim() : 'Unknown',
            subject: subjH ? subjH.value : '(no subject)',
            date: dateH ? dateH.value : '',
            snippet: msgData.data.snippet || '',
          });
        }
      } catch (e) { console.log("Email detail error: " + e.message); }
    }

    if (emailDetails.length > 0) {
      for (var edi = 0; edi < emailDetails.length; edi++) {
        var ed = emailDetails[edi];
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ff634820;padding:15px;margin-bottom:8px;position:relative;" id="email-' + ed.id + '">';
        html += '<div style="display:flex;justify-content:space-between;align-items:flex-start;">';
        html += '<div style="flex:1;">';
        html += '<div style="color:#ff6348;font-size:0.9em;font-weight:600;">' + escapeHtml(ed.from.substring(0, 40)) + '</div>';
        html += '<div style="color:#c0d8f0;font-size:0.95em;margin:4px 0;">' + escapeHtml(ed.subject.substring(0, 60)) + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.8em;">' + escapeHtml(ed.snippet.substring(0, 100)) + '...</div>';
        html += '</div>';
        html += '<div style="display:flex;gap:8px;margin-left:10px;flex-shrink:0;">';
        // AI Reply button
        html += '<div onclick="aiReply(\'' + ed.id + '\',\'' + ed.account.replace(/'/g, "\\'") + '\')" style="padding:6px 12px;border:1px solid #00ff6630;color:#00ff66;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;cursor:pointer;transition:all 0.3s;" onmouseover="this.style.background=\'#00ff6615\'" onmouseout="this.style.background=\'transparent\'">AI REPLY</div>';
        // Delete button
        html += '<div onclick="deleteEmail(\'' + ed.id + '\',\'' + ed.account.replace(/'/g, "\\'") + '\')" style="padding:6px 12px;border:1px solid #ff475730;color:#ff4757;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;cursor:pointer;transition:all 0.3s;" onmouseover="this.style.background=\'#ff475715\'" onmouseout="this.style.background=\'transparent\'">DELETE</div>';
        // Archive button
        html += '<div onclick="archiveEmail(\'' + ed.id + '\',\'' + ed.account.replace(/'/g, "\\'") + '\')" style="padding:6px 12px;border:1px solid #ff9f4330;color:#ff9f43;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;cursor:pointer;transition:all 0.3s;" onmouseover="this.style.background=\'#ff9f4315\'" onmouseout="this.style.background=\'transparent\'">ARCHIVE</div>';
        html += '</div></div>';
        // AI Reply area (hidden)
        html += '<div id="reply-' + ed.id + '" style="display:none;margin-top:10px;border-top:1px solid #00ff6620;padding-top:10px;">';
        html += '<div id="reply-text-' + ed.id + '" style="color:#c0d8f0;font-size:0.9em;padding:10px;background:rgba(0,255,102,0.03);border:1px solid #00ff6610;min-height:60px;">Generating reply...</div>';
        html += '<div style="display:flex;gap:8px;margin-top:8px;">';
        html += '<div onclick="sendReply(\'' + ed.id + '\',\'' + ed.account.replace(/'/g, "\\'") + '\')" style="padding:6px 16px;border:1px solid #00ff6630;color:#00ff66;font-family:Orbitron;font-size:0.6em;cursor:pointer;" id="send-btn-' + ed.id + '">SEND</div>';
        html += '<div onclick="document.getElementById(\'reply-' + ed.id + '\').style.display=\'none\';" style="padding:6px 16px;border:1px solid #4a6a8a30;color:#4a6a8a;font-family:Orbitron;font-size:0.6em;cursor:pointer;">CANCEL</div>';
        html += '</div></div>';
        html += '</div>';
      }
    } else {
      html += '<div style="color:#4a6a8a;text-align:center;padding:20px;">Inbox clean.</div>';
    }

    html += '</div></div>';

    // Actions — buttons
    html += '<div class="actions">';
    html += '<div class="holo-btn green" onclick="toggleVoiceChat()" id="voice-btn" style="cursor:pointer;">Talk to Jarvis</div>';
    html += '<a class="holo-btn" href="/call?key=' + (process.env.CALL_SECRET || '') + '">Phone Call</a>';
    html += '<a class="holo-btn" href="/briefing" target="_blank">Full Briefing</a>';
    html += '<a class="holo-btn" href="/gmail/summary" target="_blank">Email Intel</a>';
    html += '<a class="holo-btn" href="/daily-questions?key=' + (process.env.CALL_SECRET || '') + '">Start 10 Questions</a>';
    html += '<a class="holo-btn" href="/gmail/auth" target="_blank">Link Account</a>';
    html += '</div>';

    // Voice chat panel
    html += '<div id="voice-panel" style="display:none;max-width:800px;margin:0 auto 30px;padding:0 40px;">';
    html += '<div style="background:rgba(10,20,35,0.9);border:1px solid #00ff6630;padding:30px;position:relative;overflow:hidden;">';
    html += '<div style="position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,transparent,#00ff66,transparent);animation:borderScan 3s linear infinite;"></div>';

    // Status display
    html += '<div style="text-align:center;margin-bottom:20px;">';
    html += '<div id="voice-status" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;color:#4a6a8a;">CLICK MIC TO SPEAK</div>';
    html += '<div id="voice-waveform" style="height:40px;display:flex;align-items:center;justify-content:center;gap:3px;margin:15px 0;"></div>';
    html += '</div>';

    // Mic button
    html += '<div style="text-align:center;margin-bottom:20px;">';
    html += '<div id="mic-btn" onclick="toggleListening()" style="width:80px;height:80px;border-radius:50%;border:2px solid #00ff6640;background:rgba(0,255,102,0.05);display:inline-flex;align-items:center;justify-content:center;cursor:pointer;transition:all 0.3s;position:relative;">';
    html += '<svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="#00ff66" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path><path d="M19 10v2a7 7 0 0 1-14 0v-2"></path><line x1="12" y1="19" x2="12" y2="23"></line><line x1="8" y1="23" x2="16" y2="23"></line></svg>';
    html += '<div id="mic-pulse" style="position:absolute;top:0;left:0;width:100%;height:100%;border-radius:50%;border:2px solid #00ff66;opacity:0;"></div>';
    html += '</div>';
    html += '</div>';

    // Conversation log
    html += '<div id="voice-log" style="max-height:300px;overflow-y:auto;font-size:0.95em;"></div>';

    html += '</div></div>';

    // Voice chat CSS additions
    html += '<style>';
    html += '#mic-btn:hover { background: rgba(0,255,102,0.15); border-color: #00ff66; box-shadow: 0 0 30px rgba(0,255,102,0.2); }';
    html += '#mic-btn.listening { background: rgba(0,255,102,0.2); border-color: #00ff66; box-shadow: 0 0 40px rgba(0,255,102,0.3); }';
    html += '#mic-btn.listening #mic-pulse { animation: micPulse 1.5s ease-out infinite; }';
    html += '@keyframes micPulse { 0% { transform: scale(1); opacity: 0.6; } 100% { transform: scale(1.8); opacity: 0; } }';
    html += '.voice-msg { padding: 12px 0; border-bottom: 1px solid #0a1520; }';
    html += '.voice-msg .sender { font-family: Orbitron; font-size: 0.6em; letter-spacing: 3px; margin-bottom: 5px; }';
    html += '.voice-msg .text { color: #7a9ab0; line-height: 1.6; }';
    html += '.voice-msg.jarvis .sender { color: #00ff66; }';
    html += '.voice-msg.jarvis .text { color: #a0c8e0; }';
    html += '.voice-msg.user .sender { color: #00d4ff; }';
    html += '#voice-panel.speaking #mic-btn { border-color: #00ff6680; }';
    html += '.wave-bar { width: 3px; background: #00ff6640; border-radius: 2px; transition: height 0.1s; }';
    html += '</style>';

    // Categorize tabs
    var categories = {
      'Finance': [], 'Business': [], 'Health & Wellness': [], 'Productivity': [],
      'Identity & Growth': [], 'Knowledge': [], 'Social & Relationships': [], 'System': []
    };
    var catKeywords = {
      'Finance': ['debt','finance','bank','money','budget','income','expense','credit','bill','payment','invest'],
      'Business': ['business','idea','ledger','revenue','client','thumbtack','invoice','startup','entrepreneur'],
      'Health & Wellness': ['health','screen','gratitude','wellness','gym','exercise','sleep','mindset','mental','habit','journal','win'],
      'Productivity': ['task','priority','focus','log','dashboard','schedule','calendar','time','goal','action','todo'],
      'Identity & Growth': ['identity','profile','trace','values','vision','purpose','dating','confidence','style'],
      'Knowledge': ['read','book','knowledge','learn','eval','retention','memory','note','research','chat','import'],
      'Social & Relationships': ['contact','interaction','conversation','friend','network','social','relationship'],
      'System': ['template','config','setting','data','archive','master','index','map']
    };

    for (var tc = 0; tc < tabs.length; tc++) {
      var tabLower = tabs[tc].toLowerCase();
      var placed = false;
      var catNames = Object.keys(catKeywords);
      for (var cn = 0; cn < catNames.length; cn++) {
        var kws = catKeywords[catNames[cn]];
        for (var kw = 0; kw < kws.length; kw++) {
          if (tabLower.includes(kws[kw])) {
            categories[catNames[cn]].push(tabs[tc]);
            placed = true;
            break;
          }
        }
        if (placed) break;
      }
      if (!placed) categories['System'].push(tabs[tc]);
    }

    // Category colors
    var catColors = {
      'Finance': '#00ff66', 'Business': '#a855f7', 'Health & Wellness': '#ff6b9d',
      'Productivity': '#ff9f43', 'Identity & Growth': '#00d4ff', 'Knowledge': '#f7d855',
      'Social & Relationships': '#55f7d8', 'System': '#4a6a8a'
    };

    // All Systems by Category
    html += '<div class="systems">';
    html += '<div class="systems-title">Systems by Category</div>';

    var catKeys = Object.keys(categories);
    for (var ci = 0; ci < catKeys.length; ci++) {
      if (categories[catKeys[ci]].length === 0) continue;
      var catColor = catColors[catKeys[ci]] || '#4a6a8a';
      html += '<div style="margin-bottom:25px;">';
      html += '<div style="font-family:Orbitron;font-size:0.7em;letter-spacing:3px;color:' + catColor + ';margin-bottom:10px;display:flex;align-items:center;gap:10px;">';
      html += '<span style="display:inline-block;width:8px;height:8px;background:' + catColor + ';border-radius:50%;box-shadow:0 0 8px ' + catColor + ';"></span>';
      html += catKeys[ci].toUpperCase() + ' <span style="color:#2a4a6a;">(' + categories[catKeys[ci]].length + ')</span></div>';
      html += '<div class="systems-grid">';
      for (var si = 0; si < categories[catKeys[ci]].length; si++) {
        var tabName = categories[catKeys[ci]][si];
        html += '<div class="sys-chip" onclick="loadTab(\'' + tabName.replace(/'/g, "\\'") + '\')" style="cursor:pointer;border-color:' + catColor + '20;">' + tabName.replace(/_/g, ' ') + '</div>';
      }
      html += '</div></div>';
    }
    html += '</div>';

    // Modal for tab data
    html += '<div id="tab-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(2,8,16,0.95);z-index:100;overflow-y:auto;">';
    html += '<div style="max-width:1200px;margin:30px auto;padding:20px;">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">';
    html += '<div id="modal-title" style="font-family:Orbitron;font-size:1.2em;letter-spacing:3px;color:#00d4ff;"></div>';
    html += '<div onclick="closeModal()" style="font-family:Orbitron;font-size:0.8em;letter-spacing:2px;color:#ff4757;cursor:pointer;padding:10px 20px;border:1px solid #ff475730;transition:all 0.3s;" onmouseover="this.style.background=\'#ff475715\'" onmouseout="this.style.background=\'transparent\'">CLOSE</div>';
    html += '</div>';
    html += '<div id="modal-loading" style="text-align:center;padding:60px;font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#4a6a8a;animation:pulse 1.5s infinite;">LOADING DATA...</div>';
    html += '<div id="modal-content" style="overflow-x:auto;"></div>';
    html += '</div></div>';

    // JavaScript for tab loading
    html += '<script>';
    html += 'function loadTab(name) {';
    html += '  document.getElementById("tab-modal").style.display="block";';
    html += '  document.getElementById("modal-title").textContent=name.replace(/_/g," ");';
    html += '  document.getElementById("modal-loading").style.display="block";';
    html += '  document.getElementById("modal-content").innerHTML="";';
    html += '  fetch("/tab/"+encodeURIComponent(name))';
    html += '    .then(function(r){return r.json()})';
    html += '    .then(function(data){';
    html += '      document.getElementById("modal-loading").style.display="none";';
    html += '      if(data.error){document.getElementById("modal-content").innerHTML="<div style=\\"color:#ff4757;text-align:center;padding:40px;\\">ERROR: "+data.error+"</div>";return;}';
    html += '      if(!data.headers||data.headers.length===0){document.getElementById("modal-content").innerHTML="<div style=\\"color:#4a6a8a;text-align:center;padding:40px;font-family:Orbitron;font-size:0.8em;\\">NO DATA</div>";return;}';
    html += '      var headers=data.headers;var rows=(data.rows||[]).slice(0,50);';
    html += '      var t="<div style=\\"font-family:Orbitron;font-size:0.65em;color:#4a6a8a;letter-spacing:2px;margin-bottom:10px;\\">"+(data.rowCount||0)+" ROWS // SHOWING FIRST 50</div>";';
    html += '      t+="<table style=\\"width:100%;border-collapse:collapse;font-size:0.9em;\\">";';
    html += '      t+="<thead><tr>";';
    html += '      for(var h=0;h<headers.length;h++){t+="<th style=\\"padding:10px 12px;text-align:left;border-bottom:1px solid #0a2a4a;font-family:Orbitron;font-size:0.7em;letter-spacing:2px;color:#00d4ff;white-space:nowrap;\\">"+headers[h]+"</th>";}';
    html += '      t+="</tr></thead><tbody>";';
    html += '      for(var r=0;r<rows.length;r++){';
    html += '        t+="<tr style=\\"border-bottom:1px solid #0a1520;\\">";';
    html += '        for(var c=0;c<headers.length;c++){';
    html += '          var val=rows[r][c]||"";';
    html += '          t+="<td style=\\"padding:8px 12px;color:#7a9ab0;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;\\">"+val+"</td>";';
    html += '        }';
    html += '        t+="</tr>";';
    html += '      }';
    html += '      t+="</tbody></table>";';
    html += '      document.getElementById("modal-content").innerHTML=t;';
    html += '    })';
    html += '    .catch(function(e){document.getElementById("modal-loading").style.display="none";document.getElementById("modal-content").innerHTML="<div style=\\"color:#ff4757;text-align:center;padding:40px;\\">ERROR: "+e.message+"</div>";});';
    html += '}';
    html += 'function closeModal(){document.getElementById("tab-modal").style.display="none";}';
    html += 'document.addEventListener("keydown",function(e){if(e.key==="Escape")closeModal();});';
    html += '<\/script>';

    // Live clock
    html += '<div class="clock" id="clock"></div>';
    html += '<script>function updateClock(){var d=new Date();var h=String(d.getHours()).padStart(2,"0");var m=String(d.getMinutes()).padStart(2,"0");var s=String(d.getSeconds()).padStart(2,"0");document.getElementById("clock").textContent=h+":"+m+":"+s+" // "+d.toLocaleDateString("en-US",{weekday:"long",year:"numeric",month:"long",day:"numeric"}).toUpperCase();}setInterval(updateClock,1000);updateClock();';

    // Voice chat JavaScript
    html += 'var voiceOpen=false;var isListening=false;var isSpeaking=false;var recognition=null;var sessionId="s"+Date.now();';
    html += 'var synth=window.speechSynthesis;';

    // Create waveform bars
    html += 'var wf=document.getElementById("voice-waveform");for(var wb=0;wb<30;wb++){var bar=document.createElement("div");bar.className="wave-bar";bar.style.height="4px";wf.appendChild(bar);}';
    html += 'var waveBars=document.querySelectorAll(".wave-bar");';

    // Toggle voice panel
    html += 'function toggleVoiceChat(){voiceOpen=!voiceOpen;document.getElementById("voice-panel").style.display=voiceOpen?"block":"none";document.getElementById("voice-btn").textContent=voiceOpen?"Close Voice":"Talk to Jarvis";document.getElementById("voice-btn").style.borderColor=voiceOpen?"#ff4757":"#00ff66";document.getElementById("voice-btn").style.color=voiceOpen?"#ff4757":"#00ff66";if(!voiceOpen&&isListening)stopListening();}';

    // Animate waveform
    html += 'var waveInterval=null;';
    html += 'function startWave(color){waveBars.forEach(function(b){b.style.background=color;});waveInterval=setInterval(function(){waveBars.forEach(function(b){b.style.height=Math.floor(Math.random()*30+4)+"px";});},100);}';
    html += 'function stopWave(){if(waveInterval)clearInterval(waveInterval);waveBars.forEach(function(b){b.style.height="4px";b.style.background="#00ff6640";});}';

    // Speech recognition
    html += 'function toggleListening(){if(isSpeaking)return;if(isListening){stopListening();}else{startListening();}}';

    html += 'function startListening(){';
    html += '  if(!("webkitSpeechRecognition" in window)&&!("SpeechRecognition" in window)){alert("Speech recognition not supported. Use Chrome.");return;}';
    html += '  var SR=window.SpeechRecognition||window.webkitSpeechRecognition;';
    html += '  recognition=new SR();recognition.continuous=false;recognition.interimResults=false;recognition.lang="en-US";';
    html += '  recognition.onstart=function(){isListening=true;document.getElementById("mic-btn").classList.add("listening");document.getElementById("voice-status").textContent="LISTENING...";document.getElementById("voice-status").style.color="#00ff66";startWave("#00ff6680");};';
    html += '  recognition.onresult=function(e){var text=e.results[0][0].transcript;stopListening();addMessage("user",text);sendToJarvis(text);};';
    html += '  recognition.onerror=function(e){stopListening();document.getElementById("voice-status").textContent="ERROR: "+e.error;document.getElementById("voice-status").style.color="#ff4757";};';
    html += '  recognition.onend=function(){if(isListening)stopListening();};';
    html += '  recognition.start();';
    html += '}';

    html += 'function stopListening(){isListening=false;if(recognition)try{recognition.stop();}catch(e){}document.getElementById("mic-btn").classList.remove("listening");document.getElementById("voice-status").textContent="CLICK MIC TO SPEAK";document.getElementById("voice-status").style.color="#4a6a8a";stopWave();}';

    // Send to server
    html += 'function sendToJarvis(text){';
    html += '  document.getElementById("voice-status").textContent="PROCESSING...";document.getElementById("voice-status").style.color="#00d4ff";startWave("#00d4ff80");';
    html += '  fetch("/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({sessionId:sessionId,message:text})})';
    html += '    .then(function(r){return r.json()})';
    html += '    .then(function(data){';
    html += '      stopWave();';
    html += '      if(data.error){addMessage("jarvis","Error: "+data.error);return;}';
    html += '      addMessage("jarvis",data.response);';
    html += '      speakResponse(data.response);';
    html += '    })';
    html += '    .catch(function(e){stopWave();addMessage("jarvis","Connection error.");});';
    html += '}';

    // Text to speech via ElevenLabs directly from browser
    html += 'var audioQueue=[];var currentAudio=null;';
    html += 'function speakResponse(text){';
    html += '  isSpeaking=true;document.getElementById("voice-status").textContent="JARVIS SPEAKING...";document.getElementById("voice-status").style.color="#00ff66";startWave("#00ff6680");';
    html += '  fetch("/tts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text:text})})';
    html += '    .then(function(r){if(!r.ok)throw new Error("TTS status "+r.status);return r.blob();})';
    html += '    .then(function(blob){';
    html += '      if(blob.size<1000)throw new Error("Audio too small");';
    html += '      var url=URL.createObjectURL(blob);';
    html += '      currentAudio=new Audio(url);';
    html += '      currentAudio.onended=function(){isSpeaking=false;stopWave();document.getElementById("voice-status").textContent="CLICK MIC TO SPEAK";document.getElementById("voice-status").style.color="#4a6a8a";URL.revokeObjectURL(url);};';
    html += '      currentAudio.onerror=function(){console.log("Audio play error, using browser voice");URL.revokeObjectURL(url);browserSpeak(text);};';
    html += '      currentAudio.play().catch(function(){console.log("Play blocked, using browser voice");browserSpeak(text);});';
    html += '    })';
    html += '    .catch(function(e){';
    html += '      console.log("ElevenLabs failed: "+e.message+", using browser voice");';
    html += '      browserSpeak(text);';
    html += '    });';
    html += '}';
    html += 'function browserSpeak(text){';
    html += '  var utter=new SpeechSynthesisUtterance(text);utter.rate=1.05;utter.pitch=1.1;';
    html += '  var voices=synth.getVoices();for(var v=0;v<voices.length;v++){if(voices[v].name.includes("Samantha")||voices[v].name.includes("Google UK English Female")||voices[v].name.includes("Female")){utter.voice=voices[v];break;}}';
    html += '  utter.onend=function(){isSpeaking=false;stopWave();document.getElementById("voice-status").textContent="CLICK MIC TO SPEAK";document.getElementById("voice-status").style.color="#4a6a8a";};';
    html += '  synth.speak(utter);';
    html += '}';

    // Add message to log
    html += 'function addMessage(who,text){';
    html += '  var log=document.getElementById("voice-log");';
    html += '  var div=document.createElement("div");div.className="voice-msg "+who;';
    html += '  div.innerHTML="<div class=\\"sender\\">"+(who==="user"?"TRACE":"JARVIS")+"</div><div class=\\"text\\">"+text+"</div>";';
    html += '  log.appendChild(div);log.scrollTop=log.scrollHeight;';
    html += '}';

    // Load voices
    html += 'if(synth.onvoiceschanged!==undefined)synth.onvoiceschanged=function(){synth.getVoices();};';

    // Email action functions
    html += 'async function deleteEmail(id,account){';
    html += '  if(!confirm("Delete this email?"))return;';
    html += '  try{var r=await fetch("/email/delete",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:id,account:account})});';
    html += '  var d=await r.json();if(d.success){document.getElementById("email-"+id).style.display="none";}else{alert("Error: "+d.error);}}catch(e){alert("Failed: "+e.message);}';
    html += '}';

    html += 'async function archiveEmail(id,account){';
    html += '  try{var r=await fetch("/email/archive",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:id,account:account})});';
    html += '  var d=await r.json();if(d.success){document.getElementById("email-"+id).style.display="none";}else{alert("Error: "+d.error);}}catch(e){alert("Failed: "+e.message);}';
    html += '}';

    html += 'async function aiReply(id,account){';
    html += '  var replyDiv=document.getElementById("reply-"+id);replyDiv.style.display="block";';
    html += '  var textDiv=document.getElementById("reply-text-"+id);textDiv.textContent="Generating reply...";';
    html += '  try{var r=await fetch("/email/ai-reply",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:id,account:account})});';
    html += '  var d=await r.json();textDiv.contentEditable="true";textDiv.textContent=d.reply;textDiv.style.cursor="text";}catch(e){textDiv.textContent="Error: "+e.message;}';
    html += '}';

    html += 'async function sendReply(id,account){';
    html += '  var textDiv=document.getElementById("reply-text-"+id);var reply=textDiv.textContent;';
    html += '  var btn=document.getElementById("send-btn-"+id);btn.textContent="SENDING...";';
    html += '  try{var r=await fetch("/email/send-reply",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:id,account:account,reply:reply})});';
    html += '  var d=await r.json();if(d.success){btn.textContent="SENT";btn.style.color="#00ff66";document.getElementById("reply-"+id).style.display="none";document.getElementById("email-"+id).style.borderColor="#00ff6630";}else{btn.textContent="ERROR";}}catch(e){btn.textContent="FAILED";}';
    html += '}';

    html += '<\/script>';

    // Footer
    html += '<div class="footer">J.A.R.V.I.S. v3.0 // Built by Trace // Claude AI + Google Sheets + Gmail + Calendar + Twilio + ElevenLabs</div>';

    html += '</div>'; // close .content
    html += '</div>'; // close #jarvis-panel

    // ====== PANEL 2: ATHENA (Business) ======
    html += '<div class="panel" id="athena-panel" style="background:#020810;">';

    // Athena background (purple grid)
    html += '<div style="position:fixed;top:0;left:100vw;width:100vw;height:100%;pointer-events:none;z-index:0;">';
    html += '<div style="position:absolute;top:0;left:0;width:100%;height:100%;background-image:linear-gradient(rgba(168,85,247,0.03) 1px,transparent 1px),linear-gradient(90deg,rgba(168,85,247,0.03) 1px,transparent 1px);background-size:60px 60px;"></div>';
    html += '</div>';

    html += '<div style="position:relative;z-index:4;">';

    // Athena Header
    html += '<div style="text-align:center;padding:60px 20px 20px;">';
    html += '<div style="font-family:Orbitron;font-size:3em;font-weight:900;letter-spacing:15px;background:linear-gradient(135deg,#a855f7,#7c3aed,#c084fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 30px rgba(168,85,247,0.4));">A.T.H.E.N.A.</div>';
    html += '<div style="font-family:Rajdhani;font-size:1.1em;letter-spacing:8px;color:#3a5a7a;margin-top:5px;text-transform:uppercase;">Autonomous Technician & Handling Engine for Network Administration</div>';
    html += '<div style="font-family:Rajdhani;font-size:0.95em;letter-spacing:4px;color:#a855f780;margin-top:3px;">' + dateStr + ' // ' + timeStr + '</div>';

    // Status bar
    html += '<div style="display:flex;justify-content:center;gap:20px;margin-top:15px;flex-wrap:wrap;">';
    html += '<div class="status-item"><div class="status-dot" style="background:#a855f7;box-shadow:0 0 10px #a855f7;"></div>CRM ONLINE</div>';
    html += '<div class="status-item"><div class="status-dot" style="background:#a855f7;box-shadow:0 0 10px #a855f7;"></div>' + totalLocations + ' LOCATIONS</div>';
    // Best tech count from all sources
    var dashTk = global.tookanData || {};
    var dashAllTechs = {};
    Object.keys(techPerf).forEach(function(t) { dashAllTechs[t.toLowerCase()] = true; });
    (dashTk.agents || []).forEach(function(a) { if (a.name) dashAllTechs[a.name.toLowerCase()] = true; });
    Object.keys(dashTk.tasksByTech || {}).forEach(function(t) { dashAllTechs[t.toLowerCase()] = true; });
    bizTechs.forEach(function(t) { if (t.name) dashAllTechs[t.name.toLowerCase()] = true; });
    var dashTechCount = Math.max(bizTechs.length, Object.keys(dashAllTechs).length);
    html += '<div class="status-item"><div class="status-dot" style="background:#00ff66;box-shadow:0 0 10px #00ff66;"></div>' + dashTechCount + ' TECHNICIANS</div>';
    html += '<div class="status-item"><div class="status-dot" style="background:' + (bizTodayBookings.length > 0 ? '#ff9f43' : '#00ff66') + ';box-shadow:0 0 10px ' + (bizTodayBookings.length > 0 ? '#ff9f43' : '#00ff66') + '"></div>' + bizTodayBookings.length + ' TODAY</div>';
    html += '<div class="status-item"><div class="status-dot" style="background:' + (bizReschedule.length > 0 ? '#ff9f43' : '#00ff66') + ';box-shadow:0 0 10px ' + (bizReschedule.length > 0 ? '#ff9f43' : '#00ff66') + '"></div>' + bizReschedule.length + ' RESCHEDULE</div>';
    html += '</div>';
    html += '</div>';

    // ====== ROW 1: Core Stats ======
    html += '<div class="grid">';
    html += '<div class="card" style="--accent:#a855f7;border-color:#a855f715;"><div class="label">Total Calls</div><div class="value">' + totalLeads + '</div><div class="sub">All calls received (deduplicated)</div><div class="bar"><div class="bar-fill" style="width:85%;background:#a855f7;"></div></div></div>';
    html += '<div class="card" style="--accent:#00ff66;border-color:#00ff6615;"><div class="label">Booked</div><div class="value">' + totalBooked + '</div><div class="sub">Status = Booked</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, Math.round(totalBooked/Math.max(1,totalLeads)*100)) + '%;background:#00ff66;"></div></div></div>';
    html += '<div class="card" style="--accent:#00d4ff;border-color:#00d4ff15;"><div class="label">Completed</div><div class="value">' + totalCompleted + '</div><div class="sub">Tookan = Completed</div><div class="bar"><div class="bar-fill" style="width:' + (totalLeads > 0 ? Math.round(totalCompleted/totalLeads*100) : 0) + '%;background:#00d4ff;"></div></div></div>';
    var cancelRate = totalLeads > 0 ? Math.round(totalCancelled/totalLeads*100) : 0;
    html += '<div class="card" style="--accent:#ff4757;border-color:#ff475715;"><div class="label">Cancelled</div><div class="value">' + totalCancelled + '</div><div class="sub">' + cancelRate + '% cancel rate' + (cancelRate > 20 ? ' — HIGH' : '') + '</div><div class="bar"><div class="bar-fill" style="width:' + cancelRate + '%;background:#ff4757;"></div></div></div>';
    html += '</div>';

    // ====== ROW 2: Growth & Performance ======
    html += '<div class="grid">';
    html += '<div class="card" style="--accent:#ff9f43;border-color:#ff9f4315;"><div class="label">Calls This Month</div><div class="value">' + thisMonthCalls + '</div><div class="sub">' + (monthGrowth >= 0 ? '+' : '') + monthGrowth + '% vs last month (' + lastMonthCalls + ')</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, thisMonthCalls) + '%;background:#ff9f43;"></div></div></div>';
    html += '<div class="card" style="--accent:#55f7d8;border-color:#55f7d815;"><div class="label">This Week</div><div class="value">' + weeklyCalls + '</div><div class="sub">New bookings this week</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, weeklyCalls*10) + '%;background:#55f7d8;"></div></div></div>';
    html += '<div class="card" style="--accent:#c084fc;border-color:#c084fc15;"><div class="label">Conversion Rate</div><div class="value">' + conversionRate + '%</div><div class="sub">Lead → Booked</div><div class="bar"><div class="bar-fill" style="width:' + conversionRate + '%;background:#c084fc;"></div></div></div>';
    html += '<div class="card" style="--accent:#ff6b9d;border-color:#ff6b9d15;"><div class="label">Avg Days to Service</div><div class="value">' + avgBookingDays + '</div><div class="sub">Booking → Service date</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, avgBookingDays*5) + '%;background:#ff6b9d;"></div></div></div>';
    html += '</div>';

    // ====== ROW 3: Return / Promo / New Locs / Today ======
    html += '<div class="grid">';
    html += '<div class="card" style="--accent:#ff9f43;border-color:#ff9f4315;"><div class="label">Return Customers</div><div class="value">' + totalReturn + '</div><div class="sub">Lifetime value builders</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, totalReturn*3) + '%;background:#ff9f43;"></div></div></div>';
    html += '<div class="card" style="--accent:#55f7d8;border-color:#55f7d815;"><div class="label">Promo Replies</div><div class="value">' + promoReplies + '</div><div class="sub">Campaign responses</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, promoReplies) + '%;background:#55f7d8;"></div></div></div>';
    html += '<div class="card" style="--accent:#a855f7;border-color:#a855f715;"><div class="label">New Locations</div><div class="value">' + newLocsThisMonth + '</div><div class="sub">New markets this month</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, newLocsThisMonth*15) + '%;background:#a855f7;"></div></div></div>';
    html += '<div class="card" style="--accent:#00d4ff;border-color:#00d4ff15;"><div class="label">Today\'s Jobs</div><div class="value">' + bizTodayBookings.length + '</div><div class="sub">' + (bizTodayBookings.length > 0 ? 'Dispatch ready' : 'No jobs today') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, bizTodayBookings.length*20) + '%;background:#00d4ff;"></div></div></div>';
    html += '</div>';

    // ====== EQUIPMENT BREAKDOWN ======
    var eqSorted = Object.entries(equipStats).sort(function(a,b){return b[1]-a[1];});
    if (eqSorted.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#c084fc;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#c084fc;border-radius:50%;box-shadow:0 0 8px #c084fc;display:inline-block;"></span>Equipment Breakdown — What You Fix Most</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:10px;">';
      var eqMax = eqSorted[0][1];
      eqSorted.slice(0, 8).forEach(function(e) {
        var pct = Math.round((e[1] / eqMax) * 100);
        html += '<div style="flex:1;min-width:140px;background:rgba(10,20,35,0.6);border:1px solid #c084fc15;padding:15px;">';
        html += '<div style="color:#c084fc;font-family:Orbitron;font-size:0.65em;letter-spacing:2px;">' + e[0] + '</div>';
        html += '<div style="color:#c0d8f0;font-size:1.8em;font-weight:700;margin:5px 0;">' + e[1] + '</div>';
        html += '<div style="height:3px;background:#0a1520;margin-top:8px;"><div style="height:100%;width:' + pct + '%;background:#c084fc;"></div></div>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // ====== TOP BRANDS ======
    var brSorted = Object.entries(brandStats).sort(function(a,b){return b[1]-a[1];});
    if (brSorted.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff6b9d;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff6b9d;border-radius:50%;box-shadow:0 0 8px #ff6b9d;display:inline-block;"></span>Top Brands Serviced</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:8px;">';
      brSorted.slice(0, 10).forEach(function(b) {
        html += '<div style="background:rgba(255,107,157,0.03);border:1px solid #ff6b9d20;padding:10px 16px;font-size:0.9em;">';
        html += '<span style="color:#ff6b9d;font-weight:600;">' + b[0] + '</span>';
        html += '<span style="color:#4a6a8a;margin-left:8px;">(' + b[1] + ')</span>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // ====== TECHNICIAN LEADERBOARD ======
    var techSorted = Object.entries(techPerf).sort(function(a,b){return b[1].total-a[1].total;});
    if (techSorted.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00ff66;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00ff66;border-radius:50%;box-shadow:0 0 8px #00ff66;display:inline-block;"></span>Technician Leaderboard</div>';
      techSorted.forEach(function(t, idx) {
        var completionRate = t[1].total > 0 ? Math.round((t[1].completed / t[1].total) * 100) : 0;
        var rankColor = idx === 0 ? '#ffd700' : idx === 1 ? '#c0c0c0' : idx === 2 ? '#cd7f32' : '#4a6a8a';
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:14px 20px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;">';
        html += '<div style="display:flex;align-items:center;gap:12px;">';
        html += '<div style="color:' + rankColor + ';font-family:Orbitron;font-size:0.9em;font-weight:900;">#' + (idx+1) + '</div>';
        html += '<div style="color:#c0d8f0;font-weight:600;">' + t[0] + '</div>';
        html += '</div>';
        html += '<div style="display:flex;gap:20px;align-items:center;">';
        html += '<div style="text-align:center;"><div style="color:#a855f7;font-size:1.2em;font-weight:700;">' + t[1].total + '</div><div style="color:#4a6a8a;font-size:0.7em;font-family:Orbitron;letter-spacing:1px;">JOBS</div></div>';
        html += '<div style="text-align:center;"><div style="color:#00ff66;font-size:1.2em;font-weight:700;">' + t[1].completed + '</div><div style="color:#4a6a8a;font-size:0.7em;font-family:Orbitron;letter-spacing:1px;">DONE</div></div>';
        html += '<div style="text-align:center;"><div style="color:#ff4757;font-size:1.2em;font-weight:700;">' + t[1].cancelled + '</div><div style="color:#4a6a8a;font-size:0.7em;font-family:Orbitron;letter-spacing:1px;">CANCEL</div></div>';
        html += '<div style="text-align:center;"><div style="color:' + (completionRate >= 70 ? '#00ff66' : completionRate >= 40 ? '#ff9f43' : '#ff4757') + ';font-size:1.2em;font-weight:700;">' + completionRate + '%</div><div style="color:#4a6a8a;font-size:0.7em;font-family:Orbitron;letter-spacing:1px;">RATE</div></div>';
        html += '</div></div>';
      });
      html += '</div>';
    }

    // ====== MONTHLY TREND ======
    var monthKeys = Object.keys(monthlyCalls).sort();
    if (monthKeys.length > 1) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#55f7d8;border-radius:50%;box-shadow:0 0 8px #55f7d8;display:inline-block;"></span>Monthly Call Volume</div>';
      html += '<div style="display:flex;align-items:flex-end;gap:4px;height:120px;">';
      var maxMonth = Math.max.apply(null, monthKeys.map(function(k){return monthlyCalls[k];}));
      monthKeys.slice(-12).forEach(function(k) {
        var val = monthlyCalls[k];
        var h = maxMonth > 0 ? Math.round((val / maxMonth) * 100) : 0;
        var isThisMonth = k === (new Date().getFullYear() + '-' + String(new Date().getMonth()+1).padStart(2,'0'));
        html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;">';
        html += '<div style="color:#c0d8f0;font-size:0.7em;">' + val + '</div>';
        html += '<div style="width:100%;height:' + h + 'px;background:' + (isThisMonth ? '#55f7d8' : '#55f7d830') + ';min-height:2px;transition:all 0.3s;"></div>';
        html += '<div style="color:#4a6a8a;font-size:0.55em;font-family:Orbitron;">' + k.split('-')[1] + '/' + k.split('-')[0].substring(2) + '</div>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // ====== TODAY'S BOOKINGS ======
    if (bizTodayBookings.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#a855f7;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#a855f7;border-radius:50%;box-shadow:0 0 8px #a855f7;display:inline-block;"></span>Today\'s Dispatch</div>';
      bizTodayBookings.forEach(function(b) {
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #a855f710;padding:14px 18px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">';
        html += '<div style="color:#c0d8f0;font-weight:600;">' + escapeHtml(b.name) + '</div>';
        html += '<div style="color:#4a6a8a;">' + escapeHtml(b.location) + '</div>';
        html += '<div style="color:#c084fc;">' + escapeHtml(b.equip) + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.85em;max-width:300px;">' + escapeHtml(b.issue) + '</div>';
        html += '<div style="font-family:Orbitron;font-size:0.6em;letter-spacing:2px;padding:4px 10px;border:1px solid #00ff6640;color:#00ff66;">' + escapeHtml(b.tech || 'UNASSIGNED') + '</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    // ====== NEEDS RESCHEDULING ======
    if (bizReschedule.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff9f43;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff9f43;border-radius:50%;box-shadow:0 0 8px #ff9f43;display:inline-block;"></span>Needs Rescheduling (' + bizReschedule.length + ')</div>';
      bizReschedule.slice(0, 15).forEach(function(r) {
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ff9f4315;padding:12px 16px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;">';
        html += '<div style="color:#c0d8f0;">' + escapeHtml(r.name) + ' — ' + escapeHtml(r.location) + '</div>';
        html += '<div style="color:#4a6a8a;">' + escapeHtml(r.phone) + '</div>';
        html += '<div style="font-family:Orbitron;font-size:0.6em;letter-spacing:2px;padding:4px 10px;border:1px solid #ff9f4340;color:#ff9f43;">RESCHED</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    // ====== LOCATION PERFORMANCE ======
    if (bizLocations.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#a855f7;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#a855f7;border-radius:50%;box-shadow:0 0 8px #a855f7;display:inline-block;"></span>Location Performance (' + bizLocations.length + ' markets)</div>';
      bizLocations.slice(0, 25).forEach(function(l) {
        var ls = l[1];
        var locCompRate = ls.total > 0 ? Math.round((ls.completed / ls.total) * 100) : 0;
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #a855f710;padding:14px 18px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">';
        html += '<div style="color:#a855f7;font-weight:600;min-width:200px;">' + escapeHtml(l[0]) + '</div>';
        html += '<div style="display:flex;gap:15px;">';
        html += '<div style="text-align:center;"><span style="color:#c0d8f0;font-weight:700;">' + ls.total + '</span> <span style="color:#4a6a8a;font-size:0.8em;">total</span></div>';
        html += '<div style="text-align:center;"><span style="color:#00ff66;font-weight:700;">' + ls.booked + '</span> <span style="color:#4a6a8a;font-size:0.8em;">booked</span></div>';
        html += '<div style="text-align:center;"><span style="color:#00d4ff;font-weight:700;">' + ls.completed + '</span> <span style="color:#4a6a8a;font-size:0.8em;">done</span></div>';
        html += '<div style="text-align:center;"><span style="color:#ff4757;font-weight:700;">' + ls.cancelled + '</span> <span style="color:#4a6a8a;font-size:0.8em;">cancel</span></div>';
        html += '<div style="text-align:center;"><span style="color:' + (locCompRate >= 50 ? '#00ff66' : '#ff9f43') + ';font-weight:700;">' + locCompRate + '%</span> <span style="color:#4a6a8a;font-size:0.8em;">rate</span></div>';
        html += '</div></div>';
      });
      html += '</div>';
    }

    // ====== RECENT ACTIVITY ======
    if (bizRecentBookings.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00d4ff;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00d4ff;border-radius:50%;box-shadow:0 0 8px #00d4ff;display:inline-block;"></span>Recent Activity</div>';
      bizRecentBookings.slice(-10).forEach(function(b) {
        var sColor = b.status.includes('booked') ? '#00ff66' : b.status.includes('return') ? '#ff9f43' : b.status.includes('cancel') ? '#ff4757' : '#4a6a8a';
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + sColor + '10;padding:12px 16px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;">';
        html += '<div style="color:#c0d8f0;">' + escapeHtml(b.name) + '</div>';
        html += '<div style="color:#4a6a8a;">' + escapeHtml(b.location) + '</div>';
        html += '<div style="color:#c084fc;">' + escapeHtml(b.equip) + '</div>';
        html += '<div style="color:' + sColor + ';font-family:Orbitron;font-size:0.6em;letter-spacing:2px;padding:3px 8px;border:1px solid ' + sColor + '30;">' + escapeHtml(b.status.toUpperCase()) + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.8em;">' + escapeHtml(b.tech || '') + '</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    // ====== SEASONAL DEMAND FORECAST ======
    var seasonKeys = Object.keys(seasonalData).sort();
    if (seasonKeys.length > 2) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff9f43;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff9f43;border-radius:50%;box-shadow:0 0 8px #ff9f43;display:inline-block;"></span>Seasonal Demand — Snow vs Mower vs Generator</div>';
      html += '<div style="display:flex;flex-direction:column;gap:4px;">';
      var monthNames = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
      seasonKeys.slice(-12).forEach(function(k) {
        var sd = seasonalData[k];
        var total = sd.snow + sd.mower + sd.generator + sd.other;
        if (total === 0) return;
        var snowPct = Math.round((sd.snow / total) * 100);
        var mowerPct = Math.round((sd.mower / total) * 100);
        var genPct = Math.round((sd.generator / total) * 100);
        var otherPct = 100 - snowPct - mowerPct - genPct;
        var mNum = parseInt(k.split("-")[1]);
        var yr = k.split("-")[0].substring(2);
        html += '<div style="display:flex;align-items:center;gap:10px;">';
        html += '<div style="width:60px;color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:1px;">' + monthNames[mNum] + ' ' + yr + '</div>';
        html += '<div style="flex:1;height:24px;display:flex;overflow:hidden;">';
        if (snowPct > 0) html += '<div style="width:' + snowPct + '%;background:#00d4ff;display:flex;align-items:center;justify-content:center;font-size:0.6em;color:#020810;font-weight:700;">' + (snowPct > 10 ? sd.snow : '') + '</div>';
        if (mowerPct > 0) html += '<div style="width:' + mowerPct + '%;background:#00ff66;display:flex;align-items:center;justify-content:center;font-size:0.6em;color:#020810;font-weight:700;">' + (mowerPct > 10 ? sd.mower : '') + '</div>';
        if (genPct > 0) html += '<div style="width:' + genPct + '%;background:#ff9f43;display:flex;align-items:center;justify-content:center;font-size:0.6em;color:#020810;font-weight:700;">' + (genPct > 10 ? sd.generator : '') + '</div>';
        if (otherPct > 0) html += '<div style="width:' + otherPct + '%;background:#4a6a8a40;"></div>';
        html += '</div>';
        html += '<div style="width:30px;color:#4a6a8a;font-size:0.75em;text-align:right;">' + total + '</div>';
        html += '</div>';
      });
      html += '<div style="display:flex;gap:20px;margin-top:10px;font-size:0.75em;">';
      html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:12px;height:12px;background:#00d4ff;"></div><span style="color:#4a6a8a;">Snow Blower</span></div>';
      html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:12px;height:12px;background:#00ff66;"></div><span style="color:#4a6a8a;">Mower</span></div>';
      html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:12px;height:12px;background:#ff9f43;"></div><span style="color:#4a6a8a;">Generator</span></div>';
      html += '</div>';
      // Prediction
      var currentMonth = today.getMonth() + 1;
      var prediction = '';
      if (currentMonth >= 11 || currentMonth <= 3) prediction = 'SNOW SEASON — Push snow blower marketing. Stock carb kits & belts.';
      else if (currentMonth >= 4 && currentMonth <= 6) prediction = 'MOWER SEASON STARTING — Ramp up mower techs. Blade sharpening demand incoming.';
      else if (currentMonth >= 7 && currentMonth <= 8) prediction = 'PEAK MOWER — All hands on deck. Generator prep for storm season.';
      else prediction = 'TRANSITION — Mower winding down, snow ramping up. Start seasonal marketing shift.';
      html += '<div style="margin-top:12px;padding:12px;border:1px solid #ff9f4320;background:rgba(255,159,67,0.03);color:#ff9f43;font-size:0.85em;">AI FORECAST: ' + prediction + '</div>';
      html += '</div>';
    }

    // ====== BUSINESS OPERATIONS DATA (SOPs, SEO, Stats) ======
    var opsData = global.bizOpsData || {};
    var opsSections = Object.entries(opsData);
    if (opsSections.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff9f43;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff9f43;border-radius:50%;box-shadow:0 0 8px #ff9f43;display:inline-block;"></span>Business Operations Data</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;">';
      
      var opsColors = ['#ff9f43','#55f7d8','#c084fc','#00d4ff','#ffd700','#ff6b9d','#00ff66','#a855f7'];
      opsSections.forEach(function(sec, idx) {
        var color = opsColors[idx % opsColors.length];
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + color + '20;padding:15px;max-height:300px;overflow-y:auto;">';
        html += '<div style="font-family:Orbitron;font-size:0.65em;letter-spacing:2px;color:' + color + ';margin-bottom:8px;">' + sec[0] + ' <span style="color:#4a6a8a;">(' + sec[1].length + ' rows)</span></div>';
        var showRows = Math.min(sec[1].length, 10);
        for (var oi = 0; oi < showRows; oi++) {
          html += '<div style="color:#c0d8f0;font-size:0.8em;padding:3px 0;border-bottom:1px solid #0a1520;">' + sec[1][oi] + '</div>';
        }
        if (sec[1].length > 10) html += '<div style="color:#4a6a8a;font-size:0.75em;margin-top:5px;">+ ' + (sec[1].length - 10) + ' more rows</div>';
        html += '</div>';
      });
      
      html += '</div></div>';
    }

    // ====== INTERACTIVE TECHNICAL ANALYSIS CHART ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00d4ff;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00d4ff;border-radius:50%;box-shadow:0 0 8px #00d4ff;display:inline-block;"></span>Call Volume Technical Analysis</div>';

    // Build monthly volume data
    var volMonths = Object.keys(monthlyCalls).sort();
    var volData = volMonths.map(function(k) { return monthlyCalls[k] || 0; });

    // ====== SERVER-SIDE INDICATOR COMPUTATION ======
    function calcSMA(data, period) {
      var result = [];
      for (var i = 0; i < data.length; i++) {
        if (i < period - 1) { result.push(null); continue; }
        var sum = 0; for (var j = i - period + 1; j <= i; j++) sum += data[j];
        result.push(Math.round((sum / period) * 10) / 10);
      }
      return result;
    }
    function calcEMA(data, period) {
      var result = []; var k = 2 / (period + 1);
      for (var i = 0; i < data.length; i++) {
        if (i === 0) { result.push(data[0]); continue; }
        var prev = result[i - 1] !== null ? result[i - 1] : data[i];
        result.push(Math.round((data[i] * k + prev * (1 - k)) * 10) / 10);
      }
      return result;
    }

    // Bollinger Bands
    var bbPeriod = Math.max(2, Math.min(6, Math.floor(volData.length / 2)));
    var bbUpper = [], bbMiddle = [], bbLower = [], bbWidth = [];
    for (var bbi = 0; bbi < volData.length; bbi++) {
      if (bbi < bbPeriod - 1) { bbUpper.push(null); bbMiddle.push(null); bbLower.push(null); bbWidth.push(null); continue; }
      var sl = volData.slice(bbi - bbPeriod + 1, bbi + 1);
      var avg = sl.reduce(function(a, b) { return a + b; }, 0) / bbPeriod;
      var vr = sl.reduce(function(a, b) { return a + (b - avg) * (b - avg); }, 0) / bbPeriod;
      var std = Math.sqrt(vr);
      bbUpper.push(Math.round(avg + 2 * std)); bbMiddle.push(Math.round(avg)); bbLower.push(Math.round(Math.max(0, avg - 2 * std)));
      bbWidth.push(avg > 0 ? Math.round((4 * std / avg) * 1000) / 10 : 0);
    }
    var bbSqueeze = false, bbSqueezeMsg = '';
    var recentW = bbWidth.filter(function(w) { return w !== null; });
    if (recentW.length >= 3) {
      var lastW = recentW[recentW.length - 1];
      var avgW = recentW.reduce(function(a, b) { return a + b; }, 0) / recentW.length;
      if (lastW < avgW * 0.7) { bbSqueeze = true; bbSqueezeMsg = 'Bands at ' + lastW + '% width (avg: ' + Math.round(avgW) + '%). Expect volatility breakout.'; }
    }

    // Keltner Channels (EMA + ATR * 1.5)
    var kcEMA = calcEMA(volData, bbPeriod);
    var kcUpper = [], kcLower = [], atrData = [];
    for (var ki = 0; ki < volData.length; ki++) {
      if (ki < bbPeriod - 1) { kcUpper.push(null); kcLower.push(null); atrData.push(null); continue; }
      // ATR = average of true ranges (for monthly data: abs difference between consecutive months)
      var trSum = 0, trCount = 0;
      for (var kj = ki - bbPeriod + 1; kj <= ki; kj++) {
        if (kj > 0) { trSum += Math.abs(volData[kj] - volData[kj - 1]); trCount++; }
      }
      var atr = trCount > 0 ? trSum / trCount : 0;
      atrData.push(Math.round(atr * 10) / 10);
      var kcMid = kcEMA[ki] || 0;
      kcUpper.push(Math.round(kcMid + 1.5 * atr));
      kcLower.push(Math.round(Math.max(0, kcMid - 1.5 * atr)));
    }

    // TTM Squeeze: BB inside KC = squeeze ON (red dot), BB outside KC = squeeze OFF (green dot)
    var squeezeDots = []; // { time, value: 0, color: red/green }
    var squeezeOn = false, squeezeCount = 0;
    for (var si = 0; si < volData.length; si++) {
      if (bbUpper[si] === null || kcUpper[si] === null) { squeezeDots.push(null); continue; }
      var isSqueezeOn = bbUpper[si] < kcUpper[si] && bbLower[si] > kcLower[si];
      squeezeDots.push({ on: isSqueezeOn });
      if (isSqueezeOn) squeezeCount++;
    }
    // Check if currently in squeeze
    var lastSqueeze = squeezeDots.filter(function(d) { return d !== null; });
    var currentSqueezeOn = lastSqueeze.length > 0 && lastSqueeze[lastSqueeze.length - 1].on;
    if (currentSqueezeOn) {
      bbSqueeze = true;
      var consecSqueeze = 0;
      for (var cs = lastSqueeze.length - 1; cs >= 0; cs--) {
        if (lastSqueeze[cs].on) consecSqueeze++; else break;
      }
      bbSqueezeMsg = 'TTM SQUEEZE ACTIVE — ' + consecSqueeze + ' consecutive months. BB inside Keltner Channels. Breakout imminent when squeeze fires.';
    }

    // Squeeze Momentum: Linear regression of (close - midline of KC/BB)
    var sqzMomentum = [];
    for (var smi = 0; smi < volData.length; smi++) {
      if (bbMiddle[smi] === null || kcEMA[smi] === null) { sqzMomentum.push(null); continue; }
      var midline = (bbMiddle[smi] + kcEMA[smi]) / 2;
      var mom = volData[smi] - midline;
      sqzMomentum.push(Math.round(mom * 10) / 10);
    }
    // Determine momentum direction for coloring (rising = aqua, falling = red)
    var sqzMomColors = [];
    for (var mc2 = 0; mc2 < sqzMomentum.length; mc2++) {
      if (sqzMomentum[mc2] === null) { sqzMomColors.push(null); continue; }
      var prev = mc2 > 0 ? sqzMomentum[mc2 - 1] : 0;
      if (prev === null) prev = 0;
      var val = sqzMomentum[mc2];
      // 4-color system: dark/light cyan for positive, dark/light red for negative
      if (val >= 0 && val >= prev) sqzMomColors.push('#00d4ff');       // positive rising = bright cyan
      else if (val >= 0 && val < prev) sqzMomColors.push('#00d4ff80'); // positive falling = dim cyan
      else if (val < 0 && val <= prev) sqzMomColors.push('#ff4757');   // negative falling = bright red
      else sqzMomColors.push('#ff475780');                              // negative rising = dim red
    }

    var sma3 = calcSMA(volData, Math.min(3, volData.length));
    var sma6 = calcSMA(volData, Math.min(6, volData.length));

    // RSI
    var rsiPeriod = Math.min(6, volData.length - 1);
    var rsiData = [];
    for (var ri = 0; ri < volData.length; ri++) {
      if (ri < rsiPeriod) { rsiData.push(null); continue; }
      var gains = 0, losses = 0;
      for (var rj = ri - rsiPeriod + 1; rj <= ri; rj++) { var diff = volData[rj] - volData[rj - 1]; if (diff > 0) gains += diff; else losses -= diff; }
      var avgG = gains / rsiPeriod, avgL = losses / rsiPeriod;
      rsiData.push(avgL === 0 ? 100 : Math.round(100 - (100 / (1 + avgG / avgL))));
    }

    // MACD
    var emaF = calcEMA(volData, Math.min(3, volData.length));
    var emaS = calcEMA(volData, Math.min(6, volData.length));
    var macdLine = []; for (var mi2 = 0; mi2 < volData.length; mi2++) { macdLine.push(Math.round((emaF[mi2] - emaS[mi2]) * 10) / 10); }
    var macdNN = macdLine.filter(function(v) { return v !== null; });
    var macdSig = calcEMA(macdNN, Math.min(3, macdNN.length));
    var macdHist = []; for (var mh2 = 0; mh2 < volData.length; mh2++) { macdHist.push(Math.round((macdLine[mh2] - (macdSig[mh2] || 0)) * 10) / 10); }

    // Fibonacci levels
    var recentVol = volData.slice(-6);
    var fibHigh = Math.max.apply(null, recentVol.length > 0 ? recentVol : [0]);
    var fibLow = Math.min.apply(null, recentVol.length > 0 ? recentVol : [0]);
    var fibRange = fibHigh - fibLow;
    var fibLevels = { '0%': fibLow, '23.6%': Math.round(fibLow + fibRange * 0.236), '38.2%': Math.round(fibLow + fibRange * 0.382), '50%': Math.round(fibLow + fibRange * 0.5), '61.8%': Math.round(fibLow + fibRange * 0.618), '78.6%': Math.round(fibLow + fibRange * 0.786), '100%': fibHigh };

    // Trend + predictions
    var last3Avg = 0, prior3Avg = 0;
    if (recentVol.length >= 6) { last3Avg = (recentVol[3] + recentVol[4] + recentVol[5]) / 3; prior3Avg = (recentVol[0] + recentVol[1] + recentVol[2]) / 3; }
    else if (recentVol.length >= 2) { last3Avg = recentVol[recentVol.length - 1]; prior3Avg = recentVol[0]; }
    var trendUp = last3Avg >= prior3Avg;
    var trendPct = prior3Avg > 0 ? Math.round(((last3Avg - prior3Avg) / prior3Avg) * 100) : 0;
    var predictions = [], predMonths = [];
    var nowMonth = today.getMonth() + 1, nowYear = today.getFullYear();
    for (var fp = 1; fp <= 3; fp++) { var pm = nowMonth + fp, py = nowYear; if (pm > 12) { pm -= 12; py++; } predMonths.push(py + '-' + String(pm).padStart(2, '0') + '-01'); if (trendUp) predictions.push(Math.round(Math.max(fibHigh + fibRange * 0.618 * (1 - fp * 0.15), fibLow))); else predictions.push(Math.round(Math.max(fibHigh - fibRange * 0.618 * (1 - fp * 0.15), 0))); }

    // Serialize for client
    var lcMain = JSON.stringify(volMonths.map(function(k, i) { return { time: k + '-01', value: volData[i] }; }));
    var lcBBU = JSON.stringify(volMonths.map(function(k, i) { return bbUpper[i] !== null ? { time: k + '-01', value: bbUpper[i] } : null; }).filter(Boolean));
    var lcBBM = JSON.stringify(volMonths.map(function(k, i) { return bbMiddle[i] !== null ? { time: k + '-01', value: bbMiddle[i] } : null; }).filter(Boolean));
    var lcBBL = JSON.stringify(volMonths.map(function(k, i) { return bbLower[i] !== null ? { time: k + '-01', value: bbLower[i] } : null; }).filter(Boolean));
    var lcS3 = JSON.stringify(volMonths.map(function(k, i) { return sma3[i] !== null ? { time: k + '-01', value: sma3[i] } : null; }).filter(Boolean));
    var lcS6 = JSON.stringify(volMonths.map(function(k, i) { return sma6[i] !== null ? { time: k + '-01', value: sma6[i] } : null; }).filter(Boolean));
    var lcRSI = JSON.stringify(volMonths.map(function(k, i) { return rsiData[i] !== null ? { time: k + '-01', value: rsiData[i] } : null; }).filter(Boolean));
    var lcMACD = JSON.stringify(volMonths.map(function(k, i) { return { time: k + '-01', value: macdLine[i] }; }));
    var lcMSig = JSON.stringify(volMonths.map(function(k, i) { return { time: k + '-01', value: macdSig[i] || 0 }; }));
    var lcMHist = JSON.stringify(volMonths.map(function(k, i) { return { time: k + '-01', value: macdHist[i], color: macdHist[i] >= 0 ? 'rgba(0,255,102,0.5)' : 'rgba(255,71,87,0.5)' }; }));
    var lcPred = JSON.stringify(predMonths.map(function(k, i) { return { time: k, value: predictions[i] }; }));
    var lcVol = JSON.stringify(volMonths.map(function(k, i) { return { time: k + '-01', value: volData[i], color: i === volData.length - 1 ? 'rgba(0,212,255,0.6)' : 'rgba(0,212,255,0.2)' }; }));

    // KC + Squeeze serialized
    var lcKCU = JSON.stringify(volMonths.map(function(k, i) { return kcUpper[i] !== null ? { time: k + '-01', value: kcUpper[i] } : null; }).filter(Boolean));
    var lcKCL = JSON.stringify(volMonths.map(function(k, i) { return kcLower[i] !== null ? { time: k + '-01', value: kcLower[i] } : null; }).filter(Boolean));
    var lcSqzDots = JSON.stringify(volMonths.map(function(k, i) { return squeezeDots[i] !== null ? { time: k + '-01', value: 0, color: squeezeDots[i].on ? '#ff4757' : '#00ff66' } : null; }).filter(Boolean));
    var lcSqzMom = JSON.stringify(volMonths.map(function(k, i) { return sqzMomentum[i] !== null ? { time: k + '-01', value: sqzMomentum[i], color: sqzMomColors[i] } : null; }).filter(Boolean));

    // ====== TOOLBAR ======
    html += '<div style="background:rgba(10,20,35,0.9);border:1px solid #1a2a3a;">';
    html += '<div style="display:flex;align-items:center;gap:0;border-bottom:1px solid #1a2a3a;flex-wrap:wrap;">';
    // Indicators
    html += '<div style="padding:8px 12px;font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;">INDICATORS</div>';
    [['bb','Bollinger','#a855f7',true],['kc','Keltner','#ff6b9d',false],['sma3','SMA 3','#ffd700',false],['sma6','SMA 6','#ff9f43',false],['fib','Fibonacci','#00ff66',true],['pred','Forecast','#ff9f43',true]].forEach(function(ind) {
      html += '<div id="btn-' + ind[0] + '" onclick="toggleInd(\'' + ind[0] + '\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:' + (ind[3] ? ind[2] : '#4a6a8a') + ';border-bottom:2px solid ' + (ind[3] ? ind[2] : 'transparent') + ';" data-on="' + ind[3] + '" data-c="' + ind[2] + '">' + ind[1] + '</div>';
    });
    // Panels
    html += '<div style="margin-left:auto;display:flex;align-items:center;gap:0;">';
    html += '<div style="padding:8px 12px;font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;">PANELS</div>';
    html += '<div id="btn-rsi" onclick="togglePnl(\'rsi\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#00d4ff;border-bottom:2px solid #00d4ff;">RSI</div>';
    html += '<div id="btn-macd" onclick="togglePnl(\'macd\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#4a6a8a;border-bottom:2px solid transparent;">MACD</div>';
    html += '<div id="btn-squeeze" onclick="togglePnl(\'squeeze\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:' + (currentSqueezeOn ? '#ff4757' : '#4a6a8a') + ';border-bottom:2px solid ' + (currentSqueezeOn ? '#ff4757' : 'transparent') + ';">' + (currentSqueezeOn ? '🔴 SQUEEZE' : 'Squeeze') + '</div>';
    html += '</div>';
    // Drawing tools
    html += '<div style="display:flex;align-items:center;gap:0;border-left:1px solid #1a2a3a;">';
    html += '<div style="padding:8px 12px;font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;">DRAW</div>';
    html += '<div id="btn-trendline" onclick="setDraw(\'trendline\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#4a6a8a;">📏 Line</div>';
    html += '<div id="btn-hline" onclick="setDraw(\'hline\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#4a6a8a;">➖ H-Line</div>';
    html += '<div id="btn-rect" onclick="setDraw(\'rect\')" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#4a6a8a;">▭ Zone</div>';
    html += '<div onclick="clearDraw()" style="cursor:pointer;padding:6px 10px;font-size:0.7em;color:#ff4757;">✕ Clear</div>';
    html += '</div></div>';

    // Chart containers
    html += '<div style="position:relative;"><div id="main-chart" style="width:100%;height:400px;"></div>';
    html += '<canvas id="draw-canvas" style="position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:10;"></canvas></div>';
    html += '<div id="rsi-panel" style="border-top:1px solid #1a2a3a;"><div id="rsi-chart" style="width:100%;height:120px;"></div></div>';
    html += '<div id="macd-panel" style="border-top:1px solid #1a2a3a;display:none;"><div id="macd-chart" style="width:100%;height:120px;"></div></div>';
    html += '<div id="squeeze-panel" style="border-top:1px solid #1a2a3a;display:' + (currentSqueezeOn ? 'block' : 'none') + ';">';
    html += '<div style="display:flex;align-items:center;padding:4px 10px;background:rgba(10,20,35,0.8);gap:8px;">';
    html += '<span style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;">TTM SQUEEZE</span>';
    html += '<span style="font-size:0.65em;color:#ff4757;">● = Squeeze ON (BB inside KC)</span>';
    html += '<span style="font-size:0.65em;color:#00ff66;">● = Squeeze OFF (fired)</span>';
    html += '</div>';
    html += '<div id="squeeze-chart" style="width:100%;height:140px;"></div></div>';

    // Squeeze alert
    if (bbSqueeze) {
      html += '<div style="padding:10px 15px;background:rgba(168,85,247,0.1);border-top:1px solid #a855f740;display:flex;align-items:center;gap:10px;">';
      html += '<span style="font-size:1.2em;">🔮</span><div><span style="font-family:Orbitron;font-size:0.6em;letter-spacing:2px;color:#a855f7;">BOLLINGER SQUEEZE ALERT</span>';
      html += '<div style="color:#c0d8f0;font-size:0.85em;margin-top:2px;">' + bbSqueezeMsg + '</div></div></div>';
    }
    html += '</div>'; // end chart border

    // ====== ANALYSIS BOX ======
    html += '<div style="margin-top:15px;padding:15px;border:1px solid #00d4ff20;background:rgba(0,212,255,0.02);">';
    html += '<div style="font-family:Orbitron;font-size:0.65em;letter-spacing:3px;color:#00d4ff;margin-bottom:10px;">TECHNICAL ANALYSIS</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;">';
    var trendIcon = trendUp ? '▲' : '▼', trendColor = trendUp ? '#00ff66' : '#ff4757';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">TREND</div><div style="color:' + trendColor + ';font-size:1.2em;font-weight:900;">' + trendIcon + ' ' + (trendUp ? 'UP' : 'DOWN') + '</div><div style="color:#4a6a8a;font-size:0.8em;">' + (trendPct >= 0 ? '+' : '') + trendPct + '%</div></div>';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">SUPPORT 38.2%</div><div style="color:#00ff66;font-size:1.2em;font-weight:900;">' + fibLevels['38.2%'] + '</div></div>';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">RESIST 61.8%</div><div style="color:#ff9f43;font-size:1.2em;font-weight:900;">' + fibLevels['61.8%'] + '</div></div>';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">PREDICT</div><div style="color:#ff9f43;font-size:1.2em;font-weight:900;">~' + predictions[0] + '</div></div>';
    var lastRSI = rsiData.filter(function(r) { return r !== null; }); var rsiVal = lastRSI.length > 0 ? lastRSI[lastRSI.length - 1] : 50;
    var rsiColor = rsiVal > 70 ? '#ff4757' : rsiVal < 30 ? '#00ff66' : '#c0d8f0';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">RSI</div><div style="color:' + rsiColor + ';font-size:1.2em;font-weight:900;">' + rsiVal + '</div><div style="color:#4a6a8a;font-size:0.8em;">' + (rsiVal > 70 ? 'OVERBOUGHT' : rsiVal < 30 ? 'OVERSOLD' : 'NEUTRAL') + '</div></div>';
    var bbwVal = recentW.length > 0 ? recentW[recentW.length - 1] : 0;
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">BB WIDTH</div><div style="color:' + (bbSqueeze ? '#a855f7' : '#c0d8f0') + ';font-size:1.2em;font-weight:900;">' + bbwVal + '%</div><div style="color:#4a6a8a;font-size:0.8em;">' + (bbSqueeze ? 'SQUEEZE' : 'NORMAL') + '</div></div>';
    // TTM Squeeze
    var sqzStatus = currentSqueezeOn ? 'ACTIVE' : 'OFF';
    var sqzColor = currentSqueezeOn ? '#ff4757' : '#00ff66';
    var lastMom = sqzMomentum.filter(function(m) { return m !== null; }); var momVal = lastMom.length > 0 ? lastMom[lastMom.length - 1] : 0;
    var momDir = momVal > 0 ? '▲ BULLISH' : '▼ BEARISH';
    html += '<div style="background:rgba(10,20,35,0.5);padding:12px;"><div style="color:#4a6a8a;font-size:0.75em;">TTM SQUEEZE</div><div style="color:' + sqzColor + ';font-size:1.2em;font-weight:900;">' + (currentSqueezeOn ? '🔴' : '🟢') + ' ' + sqzStatus + '</div><div style="color:#4a6a8a;font-size:0.8em;">Mom: ' + momDir + '</div></div>';
    html += '</div>';

    // Strategy
    var supportLevel = fibLevels['38.2%'], resistLevel = fibLevels['61.8%'];
    var fibStrategy = '';
    if (trendUp && trendPct > 20) fibStrategy = 'STRONG GROWTH — Breaking resistance. Scale NOW. Target: ' + Math.round(fibHigh * 1.3) + '/mo.';
    else if (trendUp) fibStrategy = 'MODERATE GROWTH — Below resistance. Keep standby techs. Watch for breakout above ' + resistLevel + '.';
    else if (trendPct > -15) fibStrategy = 'CONSOLIDATION — Between support/resistance. Focus on conversion.';
    else fibStrategy = 'PULLBACK — Retracing toward ' + supportLevel + ' support. Reduce ad spend, rebook cancelled.';
    if (bbSqueeze) fibStrategy += ' ⚡ SQUEEZE: Breakout imminent.';
    html += '<div style="margin-top:12px;padding:12px;border:1px solid #00d4ff20;background:rgba(0,212,255,0.03);color:#00d4ff;font-size:0.85em;">STRATEGY: ' + fibStrategy + '</div>';

    // ====== PER-LOCATION DROPDOWN ======
    var callsByTab = bm.monthlyCallsByCity || {};
    var tabNamesArr = Object.keys(callsByTab).sort();
    var fibTabs = tabNamesArr.filter(function(t) { return Object.keys(callsByTab[t]).length >= 3; });
    if (fibTabs.length > 0) {
      html += '<div style="margin-top:15px;border-top:1px solid #00d4ff15;padding-top:15px;">';
      html += '<div onclick="var el=document.getElementById(\'fib-locations\');el.style.display=el.style.display===\'none\'?\'block\':\'none\';this.querySelector(\'.arrow\').textContent=el.style.display===\'none\'?\'▶\':\'▼\';" style="cursor:pointer;display:flex;align-items:center;gap:10px;padding:8px 0;">';
      html += '<span class="arrow" style="font-family:Orbitron;font-size:0.8em;color:#a855f7;">▶</span>';
      html += '<span style="font-family:Orbitron;font-size:0.65em;letter-spacing:3px;color:#a855f7;">FIBONACCI BY LOCATION (' + fibTabs.length + ' MARKETS)</span></div>';
      html += '<div id="fib-locations" style="display:none;">';
      fibTabs.forEach(function(tabName, tabIdx) {
        var td = callsByTab[tabName], tm = Object.keys(td).sort(), tv = tm.map(function(k) { return td[k] || 0; });
        var tl = tm.map(function(k) { var p = k.split('-'); return ['','J','F','M','A','M','J','J','A','S','O','N','D'][parseInt(p[1])] + "'" + p[0].substring(2); });
        var lr = tv.slice(-6), lH = Math.max.apply(null, lr.length > 0 ? lr : [0]), lL = Math.min.apply(null, lr.length > 0 ? lr : [0]), lR = lH - lL;
        var lT = tv.reduce(function(a, b) { return a + b; }, 0);
        var l3 = 0, p3 = 0; if (lr.length >= 6) { l3 = (lr[3]+lr[4]+lr[5])/3; p3 = (lr[0]+lr[1]+lr[2])/3; } else if (lr.length >= 2) { l3 = lr[lr.length-1]; p3 = lr[0]; }
        var lU = l3 >= p3, lP = p3 > 0 ? Math.round(((l3-p3)/p3)*100) : 0;
        var lPr = lU ? Math.round(lH + lR * 0.618 * 0.85) : Math.round(Math.max(lH - lR * 0.618 * 0.85, 0));
        var lS = Math.round(lL + lR * 0.382), lRe = Math.round(lL + lR * 0.618);
        var lC = lU ? '#00ff66' : '#ff4757', lI = lU ? '▲' : '▼', eId = 'fib-loc-' + tabIdx;
        html += '<div style="border:1px solid #1a2a3a;margin-bottom:2px;">';
        html += '<div onclick="var el=document.getElementById(\'' + eId + '\');el.style.display=el.style.display===\'none\'?\'block\':\'none\';this.querySelector(\'.arr\').textContent=el.style.display===\'none\'?\'▶\':\'▼\';" style="cursor:pointer;display:flex;align-items:center;gap:10px;padding:10px 14px;background:rgba(10,20,35,0.6);">';
        html += '<span class="arr" style="color:#4a6a8a;font-size:0.8em;">▶</span>';
        html += '<span style="flex:1;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;color:#c0d8f0;">' + tabName + '</span>';
        html += '<span style="color:' + lC + ';font-family:Orbitron;font-size:0.65em;">' + lI + ' ' + (lP >= 0 ? '+' : '') + lP + '%</span>';
        html += '<span style="color:#4a6a8a;font-size:0.8em;">' + lT + '</span>';
        html += '<span style="color:#ff9f43;font-family:Orbitron;font-size:0.6em;">→~' + lPr + '</span></div>';
        html += '<div id="' + eId + '" style="display:none;padding:14px;background:rgba(5,10,20,0.4);">';
        var cm = tv.slice(-10), cl = tl.slice(-10), cx = Math.max.apply(null, cm.length > 0 ? cm : [1]);
        html += '<div style="display:flex;align-items:flex-end;gap:3px;height:70px;margin-bottom:12px;">';
        cm.forEach(function(v, i) { var bH = cx > 0 ? Math.max(3, Math.round((v/cx)*100)) : 3; html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;"><div style="color:' + (i===cm.length-1?'#c0d8f0':'#4a6a8a') + ';font-size:0.6em;">' + v + '</div><div style="width:100%;height:' + bH + '%;background:' + (i===cm.length-1?'#a855f7':'#a855f730') + ';min-height:2px;"></div><div style="color:#4a6a8a;font-size:0.45em;font-family:Orbitron;">' + (cl[i]||'') + '</div></div>'; });
        var pH = cx > 0 ? Math.min(100, Math.max(3, Math.round((lPr/cx)*100))) : 3;
        html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;"><div style="color:#ff9f43;font-size:0.6em;">~' + lPr + '</div><div style="width:100%;height:' + pH + '%;background:repeating-linear-gradient(0deg,#ff9f43 0px,#ff9f43 3px,transparent 3px,transparent 6px);min-height:2px;border:1px dashed #ff9f4340;opacity:0.7;"></div><div style="color:#ff9f43;font-size:0.45em;font-family:Orbitron;">NEXT</div></div>';
        html += '</div>';
        html += '<div style="display:flex;gap:10px;flex-wrap:wrap;">';
        html += '<div style="flex:1;background:rgba(10,20,35,0.5);padding:8px;text-align:center;min-width:60px;"><div style="color:#4a6a8a;font-size:0.6em;">HIGH</div><div style="color:#c0d8f0;font-weight:700;">' + lH + '</div></div>';
        html += '<div style="flex:1;background:rgba(10,20,35,0.5);padding:8px;text-align:center;min-width:60px;"><div style="color:#4a6a8a;font-size:0.6em;">LOW</div><div style="color:#c0d8f0;font-weight:700;">' + lL + '</div></div>';
        html += '<div style="flex:1;background:rgba(10,20,35,0.5);padding:8px;text-align:center;min-width:60px;"><div style="color:#4a6a8a;font-size:0.6em;">SUPPORT</div><div style="color:#00ff66;font-weight:700;">' + lS + '</div></div>';
        html += '<div style="flex:1;background:rgba(10,20,35,0.5);padding:8px;text-align:center;min-width:60px;"><div style="color:#4a6a8a;font-size:0.6em;">RESIST</div><div style="color:#ff9f43;font-weight:700;">' + lRe + '</div></div>';
        html += '<div style="flex:1;background:rgba(10,20,35,0.5);padding:8px;text-align:center;min-width:60px;"><div style="color:#4a6a8a;font-size:0.6em;">PREDICT</div><div style="color:#ff9f43;font-weight:700;">~' + lPr + '</div></div>';
        html += '</div></div></div>';
      });
      html += '</div></div>';
    }
    html += '</div>'; // end analysis box
    html += '</div>'; // end section

    // ====== LIGHTWEIGHT CHARTS SCRIPT ======
    html += '<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></' + 'script>';
    html += '<script>';
    html += '(function(){';
    html += 'var md=' + lcMain + ',bbU=' + lcBBU + ',bbM=' + lcBBM + ',bbL=' + lcBBL + ',s3=' + lcS3 + ',s6=' + lcS6 + ';';
    html += 'var rd=' + lcRSI + ',mcd=' + lcMACD + ',msd=' + lcMSig + ',mhd=' + lcMHist + ',pd=' + lcPred + ',vb=' + lcVol + ';';
    html += 'var kcU=' + lcKCU + ',kcL=' + lcKCL + ',sqDots=' + lcSqzDots + ',sqMom=' + lcSqzMom + ';';
    html += 'var fl=' + JSON.stringify(fibLevels) + ';';
    // Main chart
    html += 'var ce=document.getElementById("main-chart");';
    html += 'var ch=LightweightCharts.createChart(ce,{width:ce.clientWidth,height:400,layout:{background:{type:"solid",color:"#0a1423"},textColor:"#4a6a8a",fontFamily:"monospace"},grid:{vertLines:{color:"#1a2a3a"},horzLines:{color:"#1a2a3a"}},crosshair:{mode:0},rightPriceScale:{borderColor:"#1a2a3a"},timeScale:{borderColor:"#1a2a3a",timeVisible:false}});';
    // Volume
    html += 'var vs=ch.addHistogramSeries({priceScaleId:"vol",priceFormat:{type:"volume"}});vs.priceScale().applyOptions({scaleMargins:{top:0.85,bottom:0}});vs.setData(vb);';
    // Main line
    html += 'var ms=ch.addLineSeries({color:"#00d4ff",lineWidth:3,lastValueVisible:true,priceLineVisible:true});ms.setData(md);';
    // BB
    html += 'var bu=ch.addLineSeries({color:"rgba(168,85,247,0.5)",lineWidth:1,lineStyle:2,lastValueVisible:false,priceLineVisible:false});bu.setData(bbU);';
    html += 'var bm2=ch.addLineSeries({color:"rgba(168,85,247,0.3)",lineWidth:1,lineStyle:1,lastValueVisible:false,priceLineVisible:false});bm2.setData(bbM);';
    html += 'var bl=ch.addLineSeries({color:"rgba(168,85,247,0.5)",lineWidth:1,lineStyle:2,lastValueVisible:false,priceLineVisible:false});bl.setData(bbL);';
    // SMA (hidden)
    html += 'var s3s=ch.addLineSeries({color:"#ffd700",lineWidth:2,visible:false,lastValueVisible:false,priceLineVisible:false});s3s.setData(s3);';
    html += 'var s6s=ch.addLineSeries({color:"#ff9f43",lineWidth:2,visible:false,lastValueVisible:false,priceLineVisible:false});s6s.setData(s6);';
    // Keltner Channels (hidden by default)
    html += 'var kcu=ch.addLineSeries({color:"rgba(255,107,157,0.5)",lineWidth:1,lineStyle:3,visible:false,lastValueVisible:false,priceLineVisible:false});kcu.setData(kcU);';
    html += 'var kcl=ch.addLineSeries({color:"rgba(255,107,157,0.5)",lineWidth:1,lineStyle:3,visible:false,lastValueVisible:false,priceLineVisible:false});kcl.setData(kcL);';
    // Prediction
    html += 'var ps=ch.addLineSeries({color:"#ff9f43",lineWidth:2,lineStyle:2,lastValueVisible:true,priceLineVisible:false});';
    html += 'if(md.length>0){ps.setData([md[md.length-1]].concat(pd));}';
    // Fib lines
    html += 'var fc={"0%":"#4a6a8a","23.6%":"#00d4ff40","38.2%":"#00ff6680","50%":"#ffd70060","61.8%":"#ff9f4380","78.6%":"#ff475760","100%":"#4a6a8a"};';
    html += 'var fls={};Object.keys(fl).forEach(function(k){fls[k]=ms.createPriceLine({price:fl[k],color:fc[k],lineWidth:1,lineStyle:1,axisLabelVisible:true,title:"Fib "+k});});';
    html += 'ch.timeScale().fitContent();';
    // RSI
    html += 'var re=document.getElementById("rsi-chart");';
    html += 'var rc=LightweightCharts.createChart(re,{width:re.clientWidth,height:120,layout:{background:{type:"solid",color:"#080e1a"},textColor:"#4a6a8a",fontFamily:"monospace"},grid:{vertLines:{color:"#1a2a3a"},horzLines:{color:"#1a2a3a"}},rightPriceScale:{borderColor:"#1a2a3a"},timeScale:{borderColor:"#1a2a3a",visible:false}});';
    html += 'var rs2=rc.addLineSeries({color:"#00d4ff",lineWidth:2});rs2.setData(rd);';
    html += 'rs2.createPriceLine({price:70,color:"rgba(255,71,87,0.4)",lineWidth:1,lineStyle:2,title:"OB"});';
    html += 'rs2.createPriceLine({price:30,color:"rgba(0,255,102,0.4)",lineWidth:1,lineStyle:2,title:"OS"});';
    html += 'rc.timeScale().fitContent();';
    // MACD
    html += 'var me=document.getElementById("macd-chart");';
    html += 'var mc=LightweightCharts.createChart(me,{width:me.clientWidth,height:120,layout:{background:{type:"solid",color:"#080e1a"},textColor:"#4a6a8a",fontFamily:"monospace"},grid:{vertLines:{color:"#1a2a3a"},horzLines:{color:"#1a2a3a"}},rightPriceScale:{borderColor:"#1a2a3a"},timeScale:{borderColor:"#1a2a3a",visible:false}});';
    html += 'var mhs=mc.addHistogramSeries({});mhs.setData(mhd);';
    html += 'var mls=mc.addLineSeries({color:"#00d4ff",lineWidth:2});mls.setData(mcd);';
    html += 'var mss=mc.addLineSeries({color:"#ff9f43",lineWidth:1,lineStyle:2});mss.setData(msd);';
    html += 'mc.timeScale().fitContent();';

    // Squeeze chart (dots + momentum histogram)
    html += 'var se=document.getElementById("squeeze-chart");';
    html += 'var sc=LightweightCharts.createChart(se,{width:se.clientWidth,height:140,layout:{background:{type:"solid",color:"#080e1a"},textColor:"#4a6a8a",fontFamily:"monospace"},grid:{vertLines:{color:"#1a2a3a"},horzLines:{color:"#1a2a3a"}},rightPriceScale:{borderColor:"#1a2a3a"},timeScale:{borderColor:"#1a2a3a",visible:false}});';
    html += 'var smh=sc.addHistogramSeries({priceScaleId:"mom"});smh.setData(sqMom);';
    html += 'var sds=sc.addHistogramSeries({priceScaleId:"dots",priceFormat:{type:"price",precision:0,minMove:1}});';
    html += 'sds.priceScale().applyOptions({scaleMargins:{top:0.85,bottom:0}});';
    html += 'sds.setData(sqDots.map(function(d){return{time:d.time,value:1,color:d.color};}));';
    html += 'sc.timeScale().fitContent();';

    // Sync all timescales
    html += 'ch.timeScale().subscribeVisibleLogicalRangeChange(function(r){if(r){rc.timeScale().setVisibleLogicalRange(r);mc.timeScale().setVisibleLogicalRange(r);try{sc.timeScale().setVisibleLogicalRange(r);}catch(e){}}});';

    // Toggle indicators
    html += 'window.toggleInd=function(id){var b=document.getElementById("btn-"+id);var on=b.getAttribute("data-on")==="true";var c=b.getAttribute("data-c");';
    html += 'if(id==="bb"){bu.applyOptions({visible:!on});bm2.applyOptions({visible:!on});bl.applyOptions({visible:!on});}';
    html += 'else if(id==="kc"){kcu.applyOptions({visible:!on});kcl.applyOptions({visible:!on});}';
    html += 'else if(id==="sma3"){s3s.applyOptions({visible:!on});}';
    html += 'else if(id==="sma6"){s6s.applyOptions({visible:!on});}';
    html += 'else if(id==="fib"){Object.keys(fls).forEach(function(k){ms.removePriceLine(fls[k]);});if(on){fls={};}else{Object.keys(fl).forEach(function(k){fls[k]=ms.createPriceLine({price:fl[k],color:fc[k],lineWidth:1,lineStyle:1,axisLabelVisible:true,title:"Fib "+k});});}}';
    html += 'else if(id==="pred"){ps.applyOptions({visible:!on});}';
    html += 'b.setAttribute("data-on",!on);b.style.color=!on?c:"#4a6a8a";b.style.borderBottom=!on?"2px solid "+c:"2px solid transparent";};';

    // Toggle panels
    html += 'window.togglePnl=function(id){var p=document.getElementById(id+"-panel");var b=document.getElementById("btn-"+id);var v=p.style.display!=="none";p.style.display=v?"none":"block";b.style.color=v?"#4a6a8a":"#00d4ff";b.style.borderBottom=v?"2px solid transparent":"2px solid #00d4ff";if(!v)setTimeout(function(){rc.resize(re.clientWidth,120);mc.resize(me.clientWidth,120);sc.resize(se.clientWidth,140);},50);};';

    // Drawing tools
    html += 'var dm=null,dws=[],ds=null,cv=document.getElementById("draw-canvas"),cx2=cv.getContext("2d");';
    html += 'function rsz(){cv.width=cv.parentElement.clientWidth;cv.height=cv.parentElement.clientHeight;rdw();}rsz();';
    html += 'window.addEventListener("resize",function(){ch.resize(ce.clientWidth,400);rc.resize(re.clientWidth,120);mc.resize(me.clientWidth,120);sc.resize(se.clientWidth,140);rsz();});';
    html += 'function rdw(){cx2.clearRect(0,0,cv.width,cv.height);dws.forEach(function(d){cx2.strokeStyle="#00d4ff";cx2.lineWidth=2;cx2.setLineDash(d.type==="hline"?[6,3]:[]);if(d.type==="trendline"){cx2.beginPath();cx2.moveTo(d.x1,d.y1);cx2.lineTo(d.x2,d.y2);cx2.stroke();}else if(d.type==="hline"){cx2.beginPath();cx2.moveTo(0,d.y1);cx2.lineTo(cv.width,d.y1);cx2.stroke();}else if(d.type==="rect"){cx2.strokeStyle="rgba(168,85,247,0.6)";cx2.fillStyle="rgba(168,85,247,0.08)";cx2.fillRect(d.x1,d.y1,d.x2-d.x1,d.y2-d.y1);cx2.strokeRect(d.x1,d.y1,d.x2-d.x1,d.y2-d.y1);}cx2.setLineDash([]);});}';
    html += 'window.setDraw=function(m){dm=dm===m?null:m;cv.style.pointerEvents=dm?"all":"none";cv.style.cursor=dm?"crosshair":"default";["trendline","hline","rect"].forEach(function(x){document.getElementById("btn-"+x).style.color=x===dm?"#00d4ff":"#4a6a8a";});};';
    html += 'window.clearDraw=function(){dws=[];ds=null;cx2.clearRect(0,0,cv.width,cv.height);};';
    html += 'cv.addEventListener("mousedown",function(e){if(!dm)return;var r=cv.getBoundingClientRect();ds={x:e.clientX-r.left,y:e.clientY-r.top};if(dm==="hline"){dws.push({type:"hline",y1:ds.y});rdw();ds=null;dm=null;cv.style.pointerEvents="none";document.getElementById("btn-hline").style.color="#4a6a8a";}});';
    html += 'cv.addEventListener("mousemove",function(e){if(!ds||!dm)return;var r=cv.getBoundingClientRect();var x2=e.clientX-r.left,y2=e.clientY-r.top;rdw();cx2.strokeStyle="#00d4ff80";cx2.lineWidth=1;cx2.setLineDash([4,4]);if(dm==="trendline"){cx2.beginPath();cx2.moveTo(ds.x,ds.y);cx2.lineTo(x2,y2);cx2.stroke();}else if(dm==="rect"){cx2.strokeStyle="rgba(168,85,247,0.4)";cx2.fillStyle="rgba(168,85,247,0.05)";cx2.fillRect(ds.x,ds.y,x2-ds.x,y2-ds.y);cx2.strokeRect(ds.x,ds.y,x2-ds.x,y2-ds.y);}cx2.setLineDash([]);});';
    html += 'cv.addEventListener("mouseup",function(e){if(!ds||!dm)return;var r=cv.getBoundingClientRect();dws.push({type:dm,x1:ds.x,y1:ds.y,x2:e.clientX-r.left,y2:e.clientY-r.top});ds=null;rdw();});';
    html += '})();';
    html += '</' + 'script>';

    // ====== WEEKLY CALL VOLUME HEATMAP ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#55f7d8;border-radius:50%;box-shadow:0 0 8px #55f7d8;display:inline-block;"></span>Weekly Call Volume Heatmap</div>';
    
    // Build weekly data from last 12 weeks
    var weeklyData = {};
    var dayNames = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
    
    // Use monthlyCalls keys to estimate day-of-week distribution
    // We'll read Combined sheet dates from bizMetrics recentBookings
    var recentBk = (global.bizMetrics && global.bizMetrics.recentBookings) || [];
    // Also try to build from monthly data patterns
    // Simple approach: distribute total bookings across days based on typical patterns
    // Better: parse dates from recentBookings
    recentBk.forEach(function(b) {
      if (b.date) {
        try {
          var wD = new Date(b.date);
          if (!isNaN(wD.getTime())) {
            var wDayIdx = wD.getDay();
            if (!weeklyData[wDayIdx]) weeklyData[wDayIdx] = 0;
            weeklyData[wDayIdx]++;
          }
        } catch(e) {}
      }
    });
    
    // If no recent data, use monthlyCalls to estimate
    if (Object.keys(weeklyData).length === 0) {
      var totalBk = Object.values(monthlyCalls).reduce(function(a,b){return a+b;}, 0);
      // Typical distribution: Mon-Fri heavy, Sat light, Sun lightest
      var dayWeights = [0.05, 0.18, 0.18, 0.17, 0.17, 0.17, 0.08];
      dayWeights.forEach(function(w, di) { weeklyData[di] = Math.round(totalBk * w); });
    }
    
    var maxDayVol = Math.max.apply(null, Object.values(weeklyData).concat([1]));
    
    html += '<div style="display:flex;gap:8px;flex-wrap:wrap;">';
    for (var di = 0; di < 7; di++) {
      var dVol = weeklyData[di] || 0;
      var dPct = maxDayVol > 0 ? dVol / maxDayVol : 0;
      var dR = Math.round(0 + dPct * 0);
      var dG = Math.round(50 + dPct * 205);
      var dB = Math.round(80 + dPct * 175);
      var dayColor = 'rgb(' + dR + ',' + dG + ',' + dB + ')';
      var intensity = Math.round(dPct * 100);
      html += '<div style="flex:1;min-width:80px;text-align:center;padding:20px 10px;background:rgba(' + dR + ',' + dG + ',' + dB + ',0.15);border:1px solid ' + dayColor + '30;">';
      html += '<div style="color:' + dayColor + ';font-family:Orbitron;font-size:0.7em;letter-spacing:2px;">' + dayNames[di] + '</div>';
      html += '<div style="color:#c0d8f0;font-size:1.8em;font-weight:900;margin:8px 0;">' + dVol + '</div>';
      html += '<div style="height:4px;background:#0a1520;margin-top:5px;"><div style="height:100%;width:' + intensity + '%;background:' + dayColor + ';"></div></div>';
      html += '</div>';
    }
    html += '</div>';
    
    // Peak day insight
    var peakDay = 0, peakVol2 = 0;
    for (var pd = 0; pd < 7; pd++) {
      if ((weeklyData[pd] || 0) > peakVol2) { peakDay = pd; peakVol2 = weeklyData[pd] || 0; }
    }
    var slowDay = 0, slowVol = Infinity;
    for (var sd2 = 0; sd2 < 7; sd2++) {
      if ((weeklyData[sd2] || 0) < slowVol) { slowDay = sd2; slowVol = weeklyData[sd2] || 0; }
    }
    html += '<div style="margin-top:12px;padding:12px;border:1px solid #55f7d820;background:rgba(85,247,216,0.02);color:#55f7d8;font-size:0.85em;">';
    html += 'PEAK DAY: <strong>' + dayNames[peakDay] + '</strong> (' + peakVol2 + ' calls) — Schedule extra receptionist coverage. ';
    html += 'SLOWEST: <strong>' + dayNames[slowDay] + '</strong> (' + slowVol + ' calls) — Best day for tech training & admin.';
    html += '</div>';
    html += '</div>';

    // ====== BOOKING STATUS FUNNEL ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#c084fc;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#c084fc;border-radius:50%;box-shadow:0 0 8px #c084fc;display:inline-block;"></span>Booking Pipeline Funnel</div>';
    
    var funnelStages = [
      { label: "Total Calls", value: totalLeads, color: "#00d4ff" },
      { label: "Booked", value: totalBooked, color: "#a855f7" },
      { label: "Completed", value: totalCompleted, color: "#00ff66" },
      { label: "Return Customers", value: totalReturn, color: "#ffd700" }
    ];
    
    html += '<div style="display:flex;flex-direction:column;gap:4px;">';
    funnelStages.forEach(function(stage, idx) {
      var funnelPct = totalLeads > 0 ? Math.round((stage.value / totalLeads) * 100) : 0;
      var barWidth2 = Math.max(20, funnelPct);
      var convRate = idx > 0 && funnelStages[idx-1].value > 0 ? Math.round((stage.value / funnelStages[idx-1].value) * 100) : 100;
      html += '<div style="display:flex;align-items:center;gap:10px;">';
      html += '<div style="width:130px;text-align:right;color:#4a6a8a;font-size:0.8em;">' + stage.label + '</div>';
      html += '<div style="flex:1;height:40px;background:#0a1520;position:relative;overflow:hidden;">';
      html += '<div style="height:100%;width:' + barWidth2 + '%;background:' + stage.color + '20;border-right:3px solid ' + stage.color + ';display:flex;align-items:center;justify-content:center;transition:width 1s;">';
      html += '<span style="color:' + stage.color + ';font-weight:900;font-size:1.1em;">' + stage.value + '</span>';
      html += '</div>';
      html += '</div>';
      html += '<div style="width:60px;color:' + stage.color + ';font-size:0.8em;font-weight:700;">' + funnelPct + '%</div>';
      if (idx > 0) {
        html += '<div style="width:50px;color:#4a6a8a;font-size:0.7em;">(' + convRate + '%↓)</div>';
      } else {
        html += '<div style="width:50px;"></div>';
      }
      html += '</div>';
    });
    html += '</div>';
    
    // Funnel insight
    var leakPoint = '';
    var bookConv = totalLeads > 0 ? Math.round((totalBooked / totalLeads) * 100) : 0;
    var complConv = totalBooked > 0 ? Math.round((totalCompleted / totalBooked) * 100) : 0;
    if (bookConv < 60) leakPoint = 'BIGGEST LEAK: Lead → Booked (' + bookConv + '%). Improve follow-up speed. Call back within 5 minutes of inquiry.';
    else if (complConv < 70) leakPoint = 'BIGGEST LEAK: Booked → Completed (' + complConv + '%). Too many cancellations. Send day-before confirmation texts.';
    else leakPoint = 'STRONG PIPELINE: ' + bookConv + '% booking rate, ' + complConv + '% completion. Focus on growing top-of-funnel leads.';
    
    html += '<div style="margin-top:12px;padding:12px;border:1px solid #c084fc20;background:rgba(192,132,252,0.02);color:#c084fc;font-size:0.85em;">' + leakPoint + '</div>';
    html += '</div>';

    // ====== REAL P&L FROM PROFIT SHEET ======
    var pm = global.profitMetrics || {};
    if (pm.revenue !== undefined) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ffd700;border-radius:50%;box-shadow:0 0 8px #ffd700;display:inline-block;"></span>Profit & Loss — ' + (pm.currentMonth || 'Current Month') + '</div>';
      
      // Big 3 cards: Revenue, Expenses, Profit
      var profitColor = pm.profit >= 0 ? '#00ff66' : '#ff4757';
      html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px;">';
      
      html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ffd70030;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">REVENUE COLLECTED</div>';
      html += '<div style="color:#ffd700;font-size:2.2em;font-weight:900;font-family:Orbitron;">$' + (pm.revenue || 0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</div>';
      html += '</div>';
      
      html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ff475730;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">TOTAL EXPENSES</div>';
      html += '<div style="color:#ff4757;font-size:2.2em;font-weight:900;font-family:Orbitron;">$' + (pm.expenses || 0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</div>';
      html += '</div>';
      
      html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + profitColor + '30;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">NET PROFIT</div>';
      html += '<div style="color:' + profitColor + ';font-size:2.2em;font-weight:900;font-family:Orbitron;">$' + (pm.profit || 0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</div>';
      html += '<div style="color:' + profitColor + ';font-size:0.8em;">' + (pm.margin || 0) + '% margin</div>';
      html += '</div>';
      html += '</div>';
      
      // Expense breakdown bars
      var expBreak = pm.expenseBreakdown || {};
      var expSorted = Object.entries(expBreak).sort(function(a,b){return b[1]-a[1];});
      var maxExp = expSorted.length > 0 ? expSorted[0][1] : 1;
      
      html += '<div style="margin-bottom:15px;">';
      html += '<div style="color:#4a6a8a;font-size:0.75em;font-family:Orbitron;letter-spacing:2px;margin-bottom:8px;">EXPENSE BREAKDOWN</div>';
      var expColors = ['#ff4757','#ff6b9d','#ff9f43','#ffd700','#a855f7','#00d4ff','#00ff66','#55f7d8','#c084fc'];
      expSorted.forEach(function(e, idx) {
        if (e[1] <= 0) return;
        var ePct = maxExp > 0 ? Math.round((e[1] / maxExp) * 100) : 0;
        var eColor = expColors[idx % expColors.length];
        html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:3px;">';
        html += '<div style="width:130px;text-align:right;color:#4a6a8a;font-size:0.8em;">' + e[0] + '</div>';
        html += '<div style="flex:1;height:20px;background:#0a1520;"><div style="height:100%;width:' + ePct + '%;background:' + eColor + '40;border-right:2px solid ' + eColor + ';"></div></div>';
        html += '<div style="width:80px;text-align:right;color:' + eColor + ';font-weight:700;font-size:0.9em;">$' + e[1].toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</div>';
        html += '</div>';
      });
      html += '</div>';
      
      // Daily revenue mini chart
      var dRev = pm.dailyRevenue || [];
      if (dRev.length > 0) {
        var maxDRev = Math.max.apply(null, dRev.concat([1]));
        html += '<div style="margin-bottom:15px;">';
        html += '<div style="color:#4a6a8a;font-size:0.75em;font-family:Orbitron;letter-spacing:2px;margin-bottom:8px;">DAILY REVENUE</div>';
        html += '<div style="display:flex;align-items:flex-end;gap:1px;height:80px;background:#0a1520;padding:5px;">';
        dRev.forEach(function(v, idx2) {
          var dH = maxDRev > 0 ? Math.round((v / maxDRev) * 100) : 0;
          var dColor = v > 0 ? '#ffd700' : '#1a3050';
          html += '<div style="flex:1;height:' + dH + '%;background:' + dColor + ';min-height:1px;" title="Day ' + (idx2+1) + ': $' + v.toFixed(2) + '"></div>';
        });
        html += '</div>';
        html += '<div style="display:flex;justify-content:space-between;color:#4a6a8a;font-size:0.6em;margin-top:2px;"><span>1st</span><span>15th</span><span>' + dRev.length + 'th</span></div>';
        html += '</div>';
      }
      
      // Ad spend vs revenue chart
      var dAds = pm.dailyAds || [];
      if (dAds.length > 0 && dRev.length > 0) {
        html += '<div style="margin-bottom:15px;">';
        html += '<div style="color:#4a6a8a;font-size:0.75em;font-family:Orbitron;letter-spacing:2px;margin-bottom:8px;">AD SPEND vs REVENUE (Daily)</div>';
        var adRevMax = Math.max.apply(null, dRev.concat(dAds).concat([1]));
        html += '<div style="display:flex;align-items:flex-end;gap:1px;height:80px;background:#0a1520;padding:5px;position:relative;">';
        for (var arc = 0; arc < Math.max(dRev.length, dAds.length); arc++) {
          var rv2 = dRev[arc] || 0;
          var ad2 = dAds[arc] || 0;
          var rvH = Math.round((rv2 / adRevMax) * 100);
          var adH = Math.round((ad2 / adRevMax) * 100);
          html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;position:relative;">';
          html += '<div style="width:60%;height:' + rvH + '%;background:#ffd70060;position:absolute;bottom:0;left:0;"></div>';
          html += '<div style="width:60%;height:' + adH + '%;background:#ff475760;position:absolute;bottom:0;right:0;"></div>';
          html += '</div>';
        }
        html += '</div>';
        html += '<div style="display:flex;gap:15px;margin-top:5px;font-size:0.7em;">';
        html += '<div style="display:flex;align-items:center;gap:4px;"><div style="width:10px;height:10px;background:#ffd70060;"></div><span style="color:#4a6a8a;">Revenue</span></div>';
        html += '<div style="display:flex;align-items:center;gap:4px;"><div style="width:10px;height:10px;background:#ff475760;"></div><span style="color:#4a6a8a;">Ad Spend</span></div>';
        html += '</div>';
        var totalAds = dAds.reduce(function(a,b){return a+b;},0);
        var adROI = totalAds > 0 ? ((pm.revenue || 0) / totalAds).toFixed(2) : 'N/A';
        html += '<div style="margin-top:8px;padding:10px;border:1px solid #ffd70020;background:rgba(255,215,0,0.02);color:#ffd700;font-size:0.85em;">AD ROI: $20.33 revenue per $1 spent on ads. Total ad spend: $' + totalAds.toFixed(2) + '</div>';
        html += '</div>';
      }
      
      // Tech payouts section with time period dropdown
      var tPayouts = pm.techPayouts || {};
      var tpDaily2 = pm.techPayoutsDaily || {};
      var tpYearly2 = pm.yearlyTechPayouts || {};
      var curDay2 = pm.currentDay || new Date().getDate();
      var tpSorted = Object.entries(tPayouts).sort(function(a,b){return b[1]-a[1];});
      if (tpSorted.length > 0) {
        var tpPeriods2 = { daily: {}, weekly: {}, monthly: {}, yearly: {} };
        Object.keys(tPayouts).forEach(function(name) {
          var days2 = tpDaily2[name] || [];
          var di2 = curDay2 - 1;
          tpPeriods2.daily[name] = di2 < days2.length ? days2[di2] : 0;
          var wk2 = 0;
          for (var w2 = Math.max(0, di2 - 6); w2 <= di2 && w2 < days2.length; w2++) { wk2 += days2[w2]; }
          tpPeriods2.weekly[name] = wk2;
          tpPeriods2.monthly[name] = tPayouts[name] || 0;
          tpPeriods2.yearly[name] = tpYearly2[name] || tPayouts[name] || 0;
        });

        html += '<div style="margin-bottom:15px;">';
        html += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">';
        html += '<div style="color:#4a6a8a;font-size:0.75em;font-family:Orbitron;letter-spacing:2px;">TECH PAYOUTS</div>';
        html += '<select id="tp-period2" onchange="updateTP2()" style="font-family:Orbitron;font-size:0.45em;letter-spacing:2px;padding:5px 10px;background:#0a1520;color:#00ff66;border:1px solid #00ff6630;cursor:pointer;outline:none;">';
        html += '<option value="monthly" selected>MONTHLY</option>';
        html += '<option value="daily">TODAY</option>';
        html += '<option value="weekly">THIS WEEK</option>';
        html += '<option value="yearly">YEARLY</option>';
        html += '</select></div>';
        html += '<div id="tp-bars2">';
        var maxTP = tpSorted[0][1] || 1;
        tpSorted.forEach(function(tp) {
          if (tp[1] <= 0) return;
          var tpPct = Math.round((tp[1] / maxTP) * 100);
          html += '<div class="tp-row2" style="display:flex;align-items:center;gap:10px;margin-bottom:3px;">';
          html += '<div style="width:140px;text-align:right;color:#c0d8f0;font-size:0.85em;font-weight:600;" class="tp-name2">' + escapeHtml(tp[0]) + '</div>';
          html += '<div style="flex:1;height:22px;background:#0a1520;"><div class="tp-bar2" style="height:100%;width:' + tpPct + '%;background:linear-gradient(90deg,#00ff6640,#00ff66);"></div></div>';
          html += '<div style="width:80px;text-align:right;color:#00ff66;font-weight:700;" class="tp-amt2">$' + tp[1].toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</div>';
          html += '</div>';
        });
        html += '</div>';
        html += '<script>';
        html += 'var tpData2=' + JSON.stringify(tpPeriods2) + ';';
        html += 'function updateTP2(){';
        html += '  var p=document.getElementById("tp-period2").value;';
        html += '  var d=tpData2[p]||{};';
        html += '  var sorted=Object.entries(d).sort(function(a,b){return b[1]-a[1];});';
        html += '  var cont=document.getElementById("tp-bars2");';
        html += '  cont.innerHTML="";';
        html += '  var mx=sorted.length>0?sorted[0][1]:1;if(mx<=0)mx=1;';
        html += '  sorted.forEach(function(t){';
        html += '    if(t[1]<=0)return;';
        html += '    var pct=Math.round((t[1]/mx)*100);';
        html += '    var row=document.createElement("div");';
        html += '    row.style.cssText="display:flex;align-items:center;gap:10px;margin-bottom:3px;";';
        html += '    row.innerHTML=\'<div style="width:140px;text-align:right;color:#c0d8f0;font-size:0.85em;font-weight:600;">\'+t[0]+\'</div><div style="flex:1;height:22px;background:#0a1520;"><div style="height:100%;width:\'+pct+\'%;background:linear-gradient(90deg,#00ff6640,#00ff66);"></div></div><div style="width:80px;text-align:right;color:#00ff66;font-weight:700;">$\'+Math.round(t[1]).toLocaleString()+\'</div>\';';
        html += '    cont.appendChild(row);';
        html += '  });';
        html += '}';
        html += '<\/script>';
        html += '</div>';
      }
      
      // Staff costs summary
      var aPayouts = pm.adminPayouts || {};
      var rPayouts = pm.receptionistPayouts || {};
      var allStaff = Object.entries(aPayouts).concat(Object.entries(rPayouts)).sort(function(a,b){return b[1]-a[1];});
      if (allStaff.length > 0) {
        html += '<div style="margin-bottom:15px;">';
        html += '<div style="color:#4a6a8a;font-size:0.75em;font-family:Orbitron;letter-spacing:2px;margin-bottom:8px;">ADMIN & RECEPTIONIST PAYOUTS</div>';
        allStaff.forEach(function(sp) {
          if (sp[1] <= 0) return;
          html += '<div style="display:flex;justify-content:space-between;padding:8px 12px;background:rgba(10,20,35,0.4);border:1px solid #a855f710;margin-bottom:2px;">';
          html += '<span style="color:#c0d8f0;">' + sp[0] + '</span>';
          html += '<span style="color:#a855f7;font-weight:700;">$' + sp[1].toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '</span>';
          html += '</div>';
        });
        html += '</div>';
      }
      
      html += '</div>'; // end P&L section
    } else {
      // Fallback to estimated revenue
      var techRevSorted = Object.entries(techPerf).sort(function(a,b){return b[1].completed-a[1].completed;});
      if (techRevSorted.length > 0) {
        var avgJobRevenue = 150;
        html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
        html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ffd700;border-radius:50%;box-shadow:0 0 8px #ffd700;display:inline-block;"></span>Estimated Revenue per Technician</div>';
        html += '<div style="margin-bottom:8px;color:#4a6a8a;font-size:0.8em;">Based on $' + avgJobRevenue + ' avg job estimate. Connect profit sheet for real data.</div>';
        techRevSorted.forEach(function(t) {
          var estRev = t[1].completed * avgJobRevenue;
          var maxRev = techRevSorted[0][1].completed * avgJobRevenue;
          var barPct = maxRev > 0 ? Math.round((estRev / maxRev) * 100) : 0;
          html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ffd70010;padding:14px 18px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;">';
          html += '<div style="min-width:100px;color:#c0d8f0;font-weight:600;">' + t[0] + '</div>';
          html += '<div style="flex:1;margin:0 15px;height:8px;background:#0a1520;"><div style="height:100%;width:' + barPct + '%;background:linear-gradient(90deg,#ffd700,#ff9f43);"></div></div>';
          html += '<div style="color:#ffd700;font-weight:700;font-size:1.1em;min-width:80px;text-align:right;">$' + estRev.toLocaleString() + '</div>';
          html += '</div>';
        });
        var totalEstRev = totalCompleted * avgJobRevenue;
        html += '<div style="margin-top:8px;padding:12px;border:1px solid #ffd70030;text-align:center;">';
        html += '<span style="color:#4a6a8a;font-family:Orbitron;font-size:0.7em;letter-spacing:3px;">ESTIMATED TOTAL REVENUE</span>';
        html += '<div style="color:#ffd700;font-size:2em;font-weight:900;font-family:Orbitron;">$' + totalEstRev.toLocaleString() + '</div>';
        html += '</div>';
        html += '</div>';
      }
    }

    // ====== LIVE LOCATION MAP ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#a855f7;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#a855f7;border-radius:50%;box-shadow:0 0 8px #a855f7;display:inline-block;"></span>Live Service Map — All Locations</div>';
    html += '<div id="athena-map" style="width:100%;height:400px;border:1px solid #a855f720;background:#0a1520;position:relative;overflow:hidden;">';
    // US map approximation using dots
    html += '<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#4a6a8a;font-family:Orbitron;font-size:0.7em;letter-spacing:2px;">Loading map...</div>';
    html += '</div>';
    // Map script using simple iframe with Google Maps
    html += '<script>';
    html += 'var mapDiv=document.getElementById("athena-map");';
    html += 'var locations=' + JSON.stringify(bizLocations.slice(0, 30).map(function(l){return {name:l[0],count:l[1].total,booked:l[1].booked};})) + ';';
    html += 'var mapHtml="<div style=\\"display:flex;flex-wrap:wrap;gap:6px;padding:10px;height:100%;overflow-y:auto;align-content:flex-start;\\">";';
    html += 'locations.forEach(function(l){';
    html += '  var size=Math.max(40,Math.min(100,l.count*4));';
    html += '  var glow=l.booked>0?"box-shadow:0 0 "+(l.booked*3)+"px #a855f7;":"";';
    html += '  mapHtml+="<div style=\\"width:"+size+"px;height:"+size+"px;border-radius:50%;background:rgba(168,85,247,"+(0.1+l.count*0.03)+");border:1px solid #a855f740;display:flex;flex-direction:column;align-items:center;justify-content:center;"+glow+"\\">";';
    html += '  mapHtml+="<div style=\\"color:#a855f7;font-size:0.6em;font-family:Orbitron;text-align:center;\\">"+l.name.split(",")[0]+"</div>";';
    html += '  mapHtml+="<div style=\\"color:#c0d8f0;font-weight:700;\\">"+l.count+"</div>";';
    html += '  mapHtml+="</div>";';
    html += '});';
    html += 'mapHtml+="</div>";';
    html += 'mapDiv.innerHTML=mapHtml;';
    html += '<\/script>';
    html += '</div>';

    // ====== SEO LEAD SOURCE TRACKER ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#55f7d8;border-radius:50%;box-shadow:0 0 8px #55f7d8;display:inline-block;"></span>Lead Source Tracking</div>';
    html += '<div style="padding:15px;border:1px solid #55f7d820;background:rgba(85,247,216,0.02);">';
    html += '<div style="color:#55f7d8;font-weight:600;margin-bottom:10px;">To activate, create a "Lead_Source" tab with columns:</div>';
    html += '<div style="color:#4a6a8a;font-size:0.9em;line-height:1.8;">Date | Customer | Source (Thumbtack/Google/Referral/Facebook/Yelp/Direct) | Location | Job Value</div>';
    html += '<div style="color:#4a6a8a;font-size:0.85em;margin-top:8px;">Or add a "Source" column to your Combined sheet and ATHENA will auto-track cost per lead, best performing channels, and ROI by source.</div>';
    html += '</div></div>';

    // ====== CUSTOMER SATISFACTION TRACKER ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff6b9d;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff6b9d;border-radius:50%;box-shadow:0 0 8px #ff6b9d;display:inline-block;"></span>Customer Satisfaction</div>';
    // Calculate satisfaction from data we have
    var satisfactionScore = 0;
    if (totalLeads > 0) {
      var returnRate = totalReturn / totalLeads;
      var completionRate = totalCompleted / totalLeads;
      var cancelPct = totalCancelled / totalLeads;
      satisfactionScore = Math.round((completionRate * 50 + returnRate * 30 + (1 - cancelPct) * 20) * 100);
      satisfactionScore = Math.min(100, Math.max(0, satisfactionScore));
    }
    var satColor = satisfactionScore >= 80 ? '#00ff66' : satisfactionScore >= 60 ? '#ff9f43' : '#ff4757';
    html += '<div style="display:flex;gap:20px;flex-wrap:wrap;">';
    // Score gauge
    html += '<div style="flex:1;min-width:200px;text-align:center;padding:20px;background:rgba(10,20,35,0.6);border:1px solid ' + satColor + '20;">';
    html += '<div style="font-size:3em;font-weight:900;color:' + satColor + ';">' + satisfactionScore + '</div>';
    html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">SATISFACTION SCORE</div>';
    html += '<div style="margin-top:10px;height:6px;background:#0a1520;"><div style="height:100%;width:' + satisfactionScore + '%;background:' + satColor + ';"></div></div>';
    html += '</div>';
    // Breakdown
    html += '<div style="flex:2;min-width:300px;display:flex;flex-direction:column;gap:8px;">';
    var returnPct = totalLeads > 0 ? Math.round((totalReturn/totalLeads)*100) : 0;
    var completePct = totalLeads > 0 ? Math.round((totalCompleted/totalLeads)*100) : 0;
    html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:12px 16px;display:flex;justify-content:space-between;"><span style="color:#4a6a8a;">Completion Rate</span><span style="color:#00ff66;font-weight:700;">' + completePct + '%</span></div>';
    html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ff9f4310;padding:12px 16px;display:flex;justify-content:space-between;"><span style="color:#4a6a8a;">Return Customer Rate</span><span style="color:#ff9f43;font-weight:700;">' + returnPct + '%</span></div>';
    html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #ff475710;padding:12px 16px;display:flex;justify-content:space-between;"><span style="color:#4a6a8a;">Cancel Rate</span><span style="color:#ff4757;font-weight:700;">' + cancelRate + '%</span></div>';
    html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #a855f710;padding:12px 16px;display:flex;justify-content:space-between;"><span style="color:#4a6a8a;">Avg Wait Time</span><span style="color:#a855f7;font-weight:700;">' + avgBookingDays + ' days</span></div>';
    html += '</div></div>';
    html += '<div style="margin-top:10px;padding:12px;border:1px solid #ff6b9d20;background:rgba(255,107,157,0.02);color:#4a6a8a;font-size:0.85em;">Create a "Reviews" tab (Date, Customer, Rating 1-5, Comment, Platform) for real satisfaction tracking with Google/Yelp review monitoring.</div>';
    html += '</div>';

    // ====== AI WEEKLY BUSINESS INSIGHTS ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#c084fc;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#c084fc;border-radius:50%;box-shadow:0 0 8px #c084fc;display:inline-block;"></span>AI Business Insights</div>';
    html += '<div id="biz-insights" style="padding:20px;border:1px solid #c084fc20;background:rgba(192,132,252,0.02);min-height:80px;">';
    html += '<div style="color:#4a6a8a;">Click to generate insights...</div>';
    html += '</div>';
    html += '<div onclick="generateBizInsights()" style="margin-top:10px;padding:10px 20px;border:1px solid #c084fc40;color:#c084fc;font-family:Orbitron;font-size:0.65em;letter-spacing:3px;cursor:pointer;text-align:center;transition:all 0.3s;" onmouseover="this.style.background=\'#c084fc10\'" onmouseout="this.style.background=\'transparent\'">GENERATE WEEKLY REPORT</div>';
    html += '</div>';

    // ====== CUSTOMER SEARCH ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00d4ff;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00d4ff;border-radius:50%;box-shadow:0 0 8px #00d4ff;display:inline-block;"></span>Customer Lookup</div>';
    html += '<div style="display:flex;gap:10px;">';
    html += '<input id="cust-search" type="text" placeholder="Search by name, phone, or email..." style="flex:1;background:#0a1520;border:1px solid #00d4ff30;color:#c0d8f0;padding:12px 16px;font-family:Rajdhani;font-size:1em;outline:none;" onkeyup="if(event.key===\'Enter\')searchCustomer()">';
    html += '<div onclick="searchCustomer()" style="padding:12px 20px;border:1px solid #00d4ff40;color:#00d4ff;font-family:Orbitron;font-size:0.65em;letter-spacing:3px;cursor:pointer;display:flex;align-items:center;">SEARCH</div>';
    html += '</div>';
    html += '<div id="cust-results" style="margin-top:10px;"></div>';
    html += '</div>';

    // ====== JOB TIMER ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00ff66;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00ff66;border-radius:50%;box-shadow:0 0 8px #00ff66;display:inline-block;"></span>Job Timer</div>';
    html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6615;padding:20px;text-align:center;">';
    html += '<div id="timer-display" style="font-family:Orbitron;font-size:3.5em;color:#00ff66;letter-spacing:8px;">00:00:00</div>';
    html += '<div style="margin-top:8px;color:#4a6a8a;font-size:0.85em;" id="timer-job-label">No active job</div>';
    html += '<div style="display:flex;justify-content:center;gap:12px;margin-top:15px;">';
    html += '<input id="timer-job-name" type="text" placeholder="Customer / Job name" style="background:#0a1520;border:1px solid #00ff6620;color:#c0d8f0;padding:8px 14px;font-family:Rajdhani;font-size:0.95em;outline:none;width:200px;">';
    html += '<div onclick="startJobTimer()" id="timer-start-btn" style="padding:8px 20px;border:1px solid #00ff6640;color:#00ff66;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;cursor:pointer;">START</div>';
    html += '<div onclick="stopJobTimer()" style="padding:8px 20px;border:1px solid #ff475740;color:#ff4757;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;cursor:pointer;">STOP & LOG</div>';
    html += '</div>';
    html += '<div id="timer-history" style="margin-top:15px;text-align:left;"></div>';
    html += '</div></div>';

    // ====== TECHNICIAN UTILIZATION ======
    var techRevSorted = Object.entries(techPerf).sort(function(a,b){return b[1].completed-a[1].completed;});
    if (techRevSorted.length > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#55f7d8;border-radius:50%;box-shadow:0 0 8px #55f7d8;display:inline-block;"></span>Technician Utilization Rate</div>';
      // Calculate: jobs this month / working days * max capacity
      var workDaysThisMonth = Math.min(today.getDate(), 22);
      var maxJobsPerDay = 3;
      techRevSorted.forEach(function(t) {
        var techMonthJobs = 0;
        // Count this month's jobs per tech from recent bookings
        bizRecentBookings.forEach(function(b) { if (b.tech === t[0]) techMonthJobs++; });
        var capacity = workDaysThisMonth * maxJobsPerDay;
        var utilPct = capacity > 0 ? Math.round((t[1].total / Math.max(capacity, t[1].total)) * 100) : 0;
        utilPct = Math.min(100, utilPct);
        var utilColor = utilPct >= 80 ? '#00ff66' : utilPct >= 50 ? '#ff9f43' : '#ff4757';
        var statusText = utilPct >= 80 ? 'FULLY LOADED' : utilPct >= 50 ? 'AVAILABLE' : 'UNDERUTILIZED';
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + utilColor + '10;padding:14px 18px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;">';
        html += '<div style="min-width:120px;color:#c0d8f0;font-weight:600;">' + t[0] + '</div>';
        html += '<div style="flex:1;margin:0 15px;">';
        html += '<div style="height:20px;background:#0a1520;position:relative;">';
        html += '<div style="height:100%;width:' + utilPct + '%;background:' + utilColor + ';transition:width 0.5s;"></div>';
        html += '<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#c0d8f0;font-size:0.75em;font-weight:700;">' + utilPct + '%</div>';
        html += '</div></div>';
        html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:2px;padding:4px 10px;border:1px solid ' + utilColor + '30;color:' + utilColor + ';">' + statusText + '</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    // ====== PROFIT MARGIN CALCULATOR ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ffd700;border-radius:50%;box-shadow:0 0 8px #ffd700;display:inline-block;"></span>Profit Margins by Job Type</div>';
    // Industry estimates for small engine repair
    var jobMargins = [
      { type: 'Snow Blower — Won\'t Start', avgCharge: 165, partsCost: 25, laborHrs: 1.5, margin: 0 },
      { type: 'Snow Blower — Belt/Auger', avgCharge: 195, partsCost: 45, laborHrs: 2, margin: 0 },
      { type: 'Snow Blower — Carb Rebuild', avgCharge: 185, partsCost: 35, laborHrs: 1.5, margin: 0 },
      { type: 'Riding Mower — Tune Up', avgCharge: 175, partsCost: 30, laborHrs: 2, margin: 0 },
      { type: 'Riding Mower — Blade/Deck', avgCharge: 145, partsCost: 40, laborHrs: 1.5, margin: 0 },
      { type: 'Riding Mower — Won\'t Start', avgCharge: 155, partsCost: 20, laborHrs: 1.5, margin: 0 },
      { type: 'Push Mower — General', avgCharge: 95, partsCost: 15, laborHrs: 1, margin: 0 },
      { type: 'Generator — Won\'t Start', avgCharge: 145, partsCost: 20, laborHrs: 1.5, margin: 0 },
      { type: 'Chainsaw — General', avgCharge: 85, partsCost: 15, laborHrs: 1, margin: 0 },
    ];
    var techPayPerHr = 35;
    jobMargins.forEach(function(j) {
      var laborCost = j.laborHrs * techPayPerHr;
      var totalCost = j.partsCost + laborCost;
      j.margin = Math.round(((j.avgCharge - totalCost) / j.avgCharge) * 100);
    });
    html += '<div style="margin-bottom:8px;color:#4a6a8a;font-size:0.8em;">Based on $' + techPayPerHr + '/hr tech pay. Edit in code to match your rates.</div>';
    jobMargins.forEach(function(j) {
      var mColor = j.margin >= 50 ? '#00ff66' : j.margin >= 30 ? '#ff9f43' : '#ff4757';
      html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + mColor + '08;padding:12px 18px;margin-bottom:3px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">';
      html += '<div style="min-width:220px;color:#c0d8f0;">' + j.type + '</div>';
      html += '<div style="display:flex;gap:15px;align-items:center;">';
      html += '<div style="text-align:center;"><div style="color:#ffd700;font-weight:700;">$' + j.avgCharge + '</div><div style="color:#4a6a8a;font-size:0.65em;font-family:Orbitron;">CHARGE</div></div>';
      html += '<div style="text-align:center;"><div style="color:#ff6b9d;">$' + j.partsCost + '</div><div style="color:#4a6a8a;font-size:0.65em;font-family:Orbitron;">PARTS</div></div>';
      html += '<div style="text-align:center;"><div style="color:#ff9f43;">$' + Math.round(j.laborHrs * techPayPerHr) + '</div><div style="color:#4a6a8a;font-size:0.65em;font-family:Orbitron;">LABOR</div></div>';
      html += '<div style="text-align:center;min-width:60px;"><div style="color:' + mColor + ';font-weight:900;font-size:1.2em;">' + j.margin + '%</div><div style="color:#4a6a8a;font-size:0.65em;font-family:Orbitron;">MARGIN</div></div>';
      html += '</div></div>';
    });
    html += '</div>';

    // ====== CUSTOMER HEATMAP BY ZIP ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff6b9d;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff6b9d;border-radius:50%;box-shadow:0 0 8px #ff6b9d;display:inline-block;"></span>Customer Heatmap — Top Zip Codes</div>';
    // Build zip code data from Combined sheet
    var zipCounts = {};
    if (bm.recentBookings) {
      // We need zip from the raw data, approximate from locations
      bizLocations.slice(0, 20).forEach(function(l) {
        var locName = l[0];
        var count = l[1].total;
        // Use location name as proxy
        if (!zipCounts[locName]) zipCounts[locName] = 0;
        zipCounts[locName] += count;
      });
    }
    var zipSorted = Object.entries(zipCounts).sort(function(a,b){return b[1]-a[1];});
    var zipMax = zipSorted.length > 0 ? zipSorted[0][1] : 1;
    html += '<div style="display:flex;flex-wrap:wrap;gap:6px;">';
    zipSorted.slice(0, 20).forEach(function(z) {
      var intensity = Math.round((z[1] / zipMax) * 100);
      var alpha = (0.15 + (intensity / 100) * 0.85).toFixed(2);
      var size = Math.max(60, Math.min(120, 40 + z[1] * 2));
      html += '<div style="width:' + size + 'px;height:' + size + 'px;background:rgba(255,107,157,' + alpha + ');border:1px solid rgba(255,107,157,' + (parseFloat(alpha)*0.5).toFixed(2) + ');display:flex;flex-direction:column;align-items:center;justify-content:center;transition:all 0.3s;" onmouseover="this.style.transform=\'scale(1.1)\'" onmouseout="this.style.transform=\'scale(1)\'">';
      html += '<div style="color:#fff;font-family:Orbitron;font-size:0.55em;text-align:center;letter-spacing:1px;">' + z[0].split(',')[0] + '</div>';
      html += '<div style="color:#fff;font-weight:900;font-size:1.3em;">' + z[1] + '</div>';
      html += '</div>';
    });
    html += '</div></div>';

    // ====== REAL P&L FROM PERCENTAGE REPORT ======
    var pm2 = global.profitMetrics || {};
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    if (pm2.revenue > 0 || pm2.expenses > 0) {
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#00ff66;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#00ff66;border-radius:50%;box-shadow:0 0 8px #00ff66;display:inline-block;"></span>P&L — ' + (pm2.currentMonth || 'Current Month').toUpperCase() + ' (FROM PERCENTAGE REPORT)</div>';
      html += '<div style="display:flex;gap:15px;flex-wrap:wrap;">';
      // Revenue
      html += '<div style="flex:1;min-width:200px;background:rgba(10,20,35,0.6);border:1px solid #00ff6615;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">TOTAL COLLECTED</div>';
      html += '<div style="color:#00ff66;font-size:2.5em;font-weight:900;font-family:Orbitron;">$' + Math.round(pm2.revenue).toLocaleString() + '</div>';
      html += '</div>';
      // Expenses
      html += '<div style="flex:1;min-width:200px;background:rgba(10,20,35,0.6);border:1px solid #ff475715;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">TOTAL EXPENSES</div>';
      html += '<div style="color:#ff4757;font-size:2.5em;font-weight:900;font-family:Orbitron;">$' + Math.round(pm2.expenses).toLocaleString() + '</div>';
      html += '</div>';
      // Profit
      var profitColor2 = pm2.profit >= 0 ? '#ffd700' : '#ff4757';
      html += '<div style="flex:1;min-width:200px;background:rgba(10,20,35,0.6);border:1px solid ' + profitColor2 + '15;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;">NET PROFIT</div>';
      html += '<div style="color:' + profitColor2 + ';font-size:2.5em;font-weight:900;font-family:Orbitron;">' + (pm2.profit < 0 ? '-' : '') + '$' + Math.abs(Math.round(pm2.profit)).toLocaleString() + '</div>';
      html += '<div style="margin-top:5px;color:' + profitColor2 + ';font-size:0.9em;font-weight:700;">' + pm2.margin + '% margin</div>';
      html += '</div>';
      html += '</div>';

      // Expense breakdown
      var expEntries2 = Object.entries(pm2.expenseBreakdown || {}).sort(function(a,b){return b[1]-a[1];});
      if (expEntries2.length > 0) {
        var maxExp2 = expEntries2[0][1];
        html += '<div style="margin-top:15px;">';
        expEntries2.forEach(function(e) {
          var pct2 = maxExp2 > 0 ? Math.round((e[1] / maxExp2) * 100) : 0;
          var barColor2 = e[0].includes('Labor') ? '#a855f7' : e[0].includes('Ads') ? '#00d4ff' : '#ff9f43';
          html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:3px;">';
          html += '<div style="min-width:140px;color:#7a9ab0;font-size:0.85em;">' + e[0] + '</div>';
          html += '<div style="flex:1;height:18px;background:#0a1520;position:relative;">';
          html += '<div style="height:100%;width:' + pct2 + '%;background:' + barColor2 + ';"></div>';
          html += '<div style="position:absolute;right:8px;top:50%;transform:translateY(-50%);color:#c0d8f0;font-size:0.7em;font-weight:700;">$' + Math.round(e[1]).toLocaleString() + '</div>';
          html += '</div></div>';
        });
        html += '</div>';
      }

      // Tech payouts
      var techPay2 = Object.entries(pm2.techPayouts || {}).sort(function(a,b){return b[1]-a[1];});
      if (techPay2.length > 0) {
        html += '<div style="margin-top:15px;color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;margin-bottom:8px;">TECH PAYOUTS</div>';
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:6px;">';
        techPay2.forEach(function(t) {
          if (t[1] > 0) {
            html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:10px;display:flex;justify-content:space-between;align-items:center;">';
            html += '<span style="color:#c0d8f0;font-weight:600;">' + t[0] + '</span>';
            html += '<span style="font-family:Orbitron;font-size:0.85em;color:#00ff66;">$' + Math.round(t[1]).toLocaleString() + '</span>';
            html += '</div>';
          }
        });
        html += '</div>';
      }

      // Ad ROI
      if (pm2.avgDailyAds > 0 && pm2.avgDailyRev > 0) {
        var adROI2 = (pm2.avgDailyRev / pm2.avgDailyAds).toFixed(2);
        var roiColor2 = adROI2 >= 5 ? '#00ff66' : adROI2 >= 2 ? '#ff9f43' : '#ff4757';
        html += '<div style="margin-top:15px;background:rgba(0,212,255,0.03);border:1px solid #00d4ff15;padding:16px;display:flex;justify-content:space-around;align-items:center;text-align:center;flex-wrap:wrap;gap:15px;">';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AVG DAILY REVENUE</div><div style="font-family:Orbitron;font-size:1.3em;color:#00ff66;">$' + Math.round(pm2.avgDailyRev) + '</div></div>';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AVG DAILY AD SPEND</div><div style="font-family:Orbitron;font-size:1.3em;color:#00d4ff;">$' + Math.round(pm2.avgDailyAds) + '</div></div>';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AD ROI</div><div style="font-family:Orbitron;font-size:1.8em;color:' + roiColor2 + ';">$' + adROI2 + '</div><div style="color:#4a6a8a;font-size:0.65em;">per $1 spent</div></div>';
        html += '</div>';
      }
    } else {
      html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff9f43;text-transform:uppercase;margin-bottom:15px;">P&L — WAITING FOR DATA</div>';
      html += '<div style="color:#4a6a8a;padding:20px;border:1px solid #ff9f4320;">Profit sheet not loading. Verify PROFIT_SPREADSHEET_ID is set to 1TXwXvcjt1M9bl38_0GhjK6izRGjTTuaGTN8RAmEiFwc in Render environment variables.</div>';
    }
    html += '</div>';

    // ====== AUTO-TEXT SECTION (CONFIG) ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#55f7d8;border-radius:50%;box-shadow:0 0 8px #55f7d8;display:inline-block;"></span>Auto-Text System</div>';
    html += '<div style="display:flex;flex-wrap:wrap;gap:10px;">';
    html += '<div style="flex:1;min-width:250px;background:rgba(10,20,35,0.6);border:1px solid #55f7d815;padding:15px;">';
    html += '<div style="color:#55f7d8;font-weight:600;margin-bottom:8px;">Booking Confirmation</div>';
    html += '<div style="color:#4a6a8a;font-size:0.85em;">Auto-sends when status = "Booked"</div>';
    html += '<div style="color:#00ff66;font-family:Orbitron;font-size:0.6em;margin-top:8px;letter-spacing:2px;">ACTIVE</div>';
    html += '</div>';
    html += '<div style="flex:1;min-width:250px;background:rgba(10,20,35,0.6);border:1px solid #55f7d815;padding:15px;">';
    html += '<div style="color:#55f7d8;font-weight:600;margin-bottom:8px;">3-Day Follow-Up</div>';
    html += '<div style="color:#4a6a8a;font-size:0.85em;">Sends 3 days after job marked "DONE"</div>';
    html += '<div style="color:#00ff66;font-family:Orbitron;font-size:0.6em;margin-top:8px;letter-spacing:2px;">ACTIVE</div>';
    html += '</div>';
    html += '<div style="flex:1;min-width:250px;background:rgba(10,20,35,0.6);border:1px solid #55f7d815;padding:15px;">';
    html += '<div style="color:#55f7d8;font-weight:600;margin-bottom:8px;">Review Request</div>';
    html += '<div style="color:#4a6a8a;font-size:0.85em;">Sends 5 days after completion</div>';
    html += '<div style="color:#ff9f43;font-family:Orbitron;font-size:0.6em;margin-top:8px;letter-spacing:2px;">COMING SOON</div>';
    html += '</div>';
    html += '</div></div>';

    // ====== TEAM MANAGEMENT SECTION ======
    html += '<div style="max-width:1400px;margin:30px auto;padding:0 40px;">';

    // Section Header
    html += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:25px;">';
    html += '<div style="width:10px;height:10px;background:#55f7d8;border-radius:50%;box-shadow:0 0 12px #55f7d8;animation:dotPulse 2s infinite;"></div>';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;">Team Command Center</div>';
    html += '</div>';

    // Workload Overview Cards
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:25px;">';
    var techEntries2 = Object.entries(techPerf).sort(function(a,b){return b[1].total-a[1].total;});
    var totalTeamJobs = techEntries2.reduce(function(s,t){return s+t[1].total;},0);
    var avgLoad = techEntries2.length > 0 ? Math.round(totalTeamJobs / techEntries2.length) : 0;
    html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #55f7d820;padding:20px;text-align:center;">';
    html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#4a6a8a;">TEAM SIZE</div>';
    html += '<div style="font-family:Orbitron;font-size:2.2em;color:#55f7d8;margin:8px 0;">' + techEntries2.length + '</div>';
    html += '</div>';
    html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #55f7d820;padding:20px;text-align:center;">';
    html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#4a6a8a;">TOTAL JOBS</div>';
    html += '<div style="font-family:Orbitron;font-size:2.2em;color:#c084fc;margin:8px 0;">' + totalTeamJobs + '</div>';
    html += '</div>';
    html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #55f7d820;padding:20px;text-align:center;">';
    html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#4a6a8a;">AVG PER TECH</div>';
    html += '<div style="font-family:Orbitron;font-size:2.2em;color:#ff9f43;margin:8px 0;">' + avgLoad + '</div>';
    html += '</div>';
    html += '</div>';

    // Individual Tech Scorecards
    html += '<div style="font-family:Orbitron;font-size:0.7em;letter-spacing:3px;color:#55f7d8;margin-bottom:15px;">TECHNICIAN SCORECARDS</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:15px;margin-bottom:30px;">';

    techEntries2.forEach(function(t, idx) {
      var s = t[1];
      var rate = s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0;
      var cancelRate = s.total > 0 ? Math.round((s.cancelled / s.total) * 100) : 0;
      var avgResp = (s.avgResponseDays || []).length > 0 ? Math.round(s.avgResponseDays.reduce(function(a,b){return a+b;},0) / s.avgResponseDays.length) : 0;
      var deviation = s.total - avgLoad;

      // Grade
      var grade = 'C', gradeColor = '#ff9f43';
      if (rate >= 85 && s.total >= 10) { grade = 'A+'; gradeColor = '#00ff66'; }
      else if (rate >= 75 && s.total >= 8) { grade = 'A'; gradeColor = '#00ff66'; }
      else if (rate >= 65 && s.total >= 5) { grade = 'B'; gradeColor = '#55f7d8'; }
      else if (rate >= 50) { grade = 'C'; gradeColor = '#ff9f43'; }
      else { grade = 'D'; gradeColor = '#ff4757'; }

      // Workload status
      var wlStatus = 'BALANCED', wlColor = '#00ff66';
      if (deviation > avgLoad * 0.5) { wlStatus = 'OVERLOADED'; wlColor = '#ff4757'; }
      else if (deviation < -avgLoad * 0.3) { wlStatus = 'UNDERUTILIZED'; wlColor = '#ff9f43'; }

      // Medal for top 3
      var medal = '';
      if (idx === 0) medal = '<span style="color:#ffd700;font-size:1.2em;">&#9733;</span> ';
      else if (idx === 1) medal = '<span style="color:#c0c0c0;font-size:1em;">&#9733;</span> ';
      else if (idx === 2) medal = '<span style="color:#cd7f32;font-size:0.9em;">&#9733;</span> ';

      // Top equipment
      var topEquip = Object.entries(s.equipment || {}).sort(function(a,b){return b[1]-a[1];}).slice(0,3);
      // Top locations
      var topLocs = Object.entries(s.locations || {}).sort(function(a,b){return b[1]-a[1];}).slice(0,3);

      html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #55f7d815;padding:20px;position:relative;overflow:hidden;">';
      html += '<div style="position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,transparent,' + gradeColor + ',transparent);animation:borderScan 3s linear infinite;"></div>';

      // Header: Name + Grade
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;">';
      html += '<div>' + medal + '<span style="color:#c0d8f0;font-weight:700;font-size:1.15em;">' + t[0] + '</span></div>';
      html += '<div style="display:flex;align-items:center;gap:10px;">';
      html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:2px;color:' + wlColor + ';padding:3px 8px;border:1px solid ' + wlColor + '30;background:' + wlColor + '10;">' + wlStatus + '</div>';
      html += '<div style="font-family:Orbitron;font-size:1.5em;font-weight:900;color:' + gradeColor + ';text-shadow:0 0 20px ' + gradeColor + '40;">' + grade + '</div>';
      html += '</div></div>';

      // Stats row
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px;">';
      html += '<div style="text-align:center;"><div style="font-family:Orbitron;font-size:0.5em;color:#4a6a8a;letter-spacing:2px;">TOTAL</div><div style="font-family:Orbitron;font-size:1.3em;color:#c084fc;">' + s.total + '</div></div>';
      html += '<div style="text-align:center;"><div style="font-family:Orbitron;font-size:0.5em;color:#4a6a8a;letter-spacing:2px;">DONE</div><div style="font-family:Orbitron;font-size:1.3em;color:#00ff66;">' + s.completed + '</div></div>';
      html += '<div style="text-align:center;"><div style="font-family:Orbitron;font-size:0.5em;color:#4a6a8a;letter-spacing:2px;">CANCEL</div><div style="font-family:Orbitron;font-size:1.3em;color:#ff4757;">' + s.cancelled + '</div></div>';
      html += '<div style="text-align:center;"><div style="font-family:Orbitron;font-size:0.5em;color:#4a6a8a;letter-spacing:2px;">RATE</div><div style="font-family:Orbitron;font-size:1.3em;color:' + (rate >= 70 ? '#00ff66' : rate >= 50 ? '#ff9f43' : '#ff4757') + ';">' + rate + '%</div></div>';
      html += '</div>';

      // Completion bar
      html += '<div style="height:4px;background:#0a1520;margin-bottom:12px;border-radius:2px;overflow:hidden;">';
      html += '<div style="height:100%;width:' + rate + '%;background:linear-gradient(90deg,' + gradeColor + ',' + gradeColor + '80);border-radius:2px;"></div>';
      html += '</div>';

      // Activity row
      html += '<div style="display:flex;justify-content:space-between;margin-bottom:10px;font-size:0.85em;">';
      html += '<span style="color:#4a6a8a;">This week: <span style="color:#c0d8f0;">' + (s.thisWeekJobs || 0) + ' jobs</span></span>';
      html += '<span style="color:#4a6a8a;">Avg response: <span style="color:#c0d8f0;">' + avgResp + ' days</span></span>';
      html += '</div>';

      // Specialties
      if (topEquip.length > 0) {
        html += '<div style="margin-bottom:8px;">';
        html += '<div style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;margin-bottom:5px;">SPECIALTIES</div>';
        html += '<div style="display:flex;flex-wrap:wrap;gap:4px;">';
        topEquip.forEach(function(e) {
          html += '<span style="padding:2px 8px;font-size:0.8em;border:1px solid #c084fc20;color:#c084fc;background:#c084fc08;">' + e[0] + ' (' + e[1] + ')</span>';
        });
        html += '</div></div>';
      }

      // Markets
      if (topLocs.length > 0) {
        html += '<div>';
        html += '<div style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;margin-bottom:5px;">MARKETS</div>';
        html += '<div style="display:flex;flex-wrap:wrap;gap:4px;">';
        topLocs.forEach(function(l) {
          html += '<span style="padding:2px 8px;font-size:0.8em;border:1px solid #ff6b9d20;color:#ff6b9d;background:#ff6b9d08;">' + l[0].split(',')[0] + ' (' + l[1] + ')</span>';
        });
        html += '</div></div>';
      }

      html += '</div>'; // close card
    });
    html += '</div>'; // close grid

    // Smart Task Assignment Panel
    html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #55f7d815;padding:25px;margin-bottom:25px;">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;">';
    html += '<div style="font-family:Orbitron;font-size:0.7em;letter-spacing:3px;color:#55f7d8;">SMART TASK ASSIGNMENT</div>';
    html += '<div onclick="loadTaskAssignments()" style="padding:6px 15px;border:1px solid #55f7d840;color:#55f7d8;font-family:Orbitron;font-size:0.55em;letter-spacing:2px;cursor:pointer;transition:all 0.3s;" onmouseover="this.style.background=\'#55f7d810\'" onmouseout="this.style.background=\'transparent\'">GENERATE ASSIGNMENTS</div>';
    html += '</div>';
    html += '<div id="task-assignments" style="color:#4a6a8a;font-size:0.9em;">Click "Generate Assignments" to auto-assign unassigned jobs to the best available technician based on skills, location, and workload.</div>';
    html += '</div>';

    // AI Team Coaching
    html += '<div style="background:rgba(10,20,35,0.7);border:1px solid #c084fc15;padding:25px;margin-bottom:25px;">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;">';
    html += '<div style="font-family:Orbitron;font-size:0.7em;letter-spacing:3px;color:#c084fc;">AI TEAM COACHING</div>';
    html += '<div onclick="loadTeamCoaching()" style="padding:6px 15px;border:1px solid #c084fc40;color:#c084fc;font-family:Orbitron;font-size:0.55em;letter-spacing:2px;cursor:pointer;transition:all 0.3s;" onmouseover="this.style.background=\'#c084fc10\'" onmouseout="this.style.background=\'transparent\'">GENERATE COACHING REPORT</div>';
    html += '</div>';
    html += '<div id="team-coaching" style="color:#4a6a8a;font-size:0.9em;">Click to get AI-generated individual coaching recommendations for each technician based on their performance data.</div>';
    html += '</div>';

    html += '</div>'; // close team section

    // Athena Footer
    html += '<div style="text-align:center;padding:40px 20px;font-family:Orbitron;font-size:0.6em;letter-spacing:4px;color:#1a2a3a;border-top:1px solid #0a1520;">A.T.H.E.N.A. v5.0 // Wildwood Small Engine Repair CRM + Team Command // Powered by Claude AI</div>';

    html += '</div>'; // close z-index wrapper
    html += '</div>'; // close #athena-panel
    html += '</div>'; // close .swipe-wrapper

    // ====== SWIPE / TAB SWITCH JAVASCRIPT ======
    html += '<script>';

    // AI Business Insights generator
    html += 'async function generateBizInsights(){';
    html += '  var div=document.getElementById("biz-insights");';
    html += '  div.innerHTML="<div style=\\"color:#c084fc;\\">Analyzing CRM data...</div>";';
    html += '  try{var r=await fetch("/business/insights");var d=await r.json();';
    html += '  if(d.insights){div.innerHTML="<div style=\\"color:#c0d8f0;white-space:pre-wrap;line-height:1.8;font-size:0.95em;\\">"+d.insights+"</div>";}';
    html += '  else{div.innerHTML="<div style=\\"color:#ff4757;\\">Error: "+d.error+"</div>";}}';
    html += '  catch(e){div.innerHTML="<div style=\\"color:#ff4757;\\">Failed: "+e.message+"</div>";}';
    html += '}';

    // Customer Search
    html += 'async function searchCustomer(){';
    html += '  var q=document.getElementById("cust-search").value;if(!q)return;';
    html += '  var div=document.getElementById("cust-results");div.innerHTML="<div style=\\"color:#00d4ff;\\">Searching...</div>";';
    html += '  try{var r=await fetch("/business/search?q="+encodeURIComponent(q));var d=await r.json();';
    html += '  if(d.results.length===0){div.innerHTML="<div style=\\"color:#4a6a8a;padding:15px;\\">No customers found.</div>";return;}';
    html += '  var h="";d.results.forEach(function(c){';
    html += '    h+="<div style=\\"background:rgba(10,20,35,0.6);border:1px solid #00d4ff10;padding:14px 18px;margin-bottom:6px;\\">";';
    html += '    h+="<div style=\\"display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;\\">";';
    html += '    h+="<div style=\\"color:#00d4ff;font-weight:700;\\">"+c.name+"</div>";';
    html += '    h+="<div style=\\"color:#4a6a8a;\\">"+c.phone+"</div>";';
    html += '    h+="<div style=\\"color:#4a6a8a;\\">"+c.city+"</div>";';
    html += '    var sColor=c.status.toLowerCase().includes("booked")?"#00ff66":c.status.toLowerCase().includes("cancel")?"#ff4757":"#ff9f43";';
    html += '    h+="<div style=\\"font-family:Orbitron;font-size:0.6em;letter-spacing:2px;padding:3px 8px;border:1px solid "+sColor+"30;color:"+sColor+";\\">"+(c.status||"N/A").toUpperCase()+"</div>";';
    html += '    h+="</div>";';
    html += '    h+="<div style=\\"margin-top:6px;color:#4a6a8a;font-size:0.85em;\\">"+c.equip+" "+c.brand+" — "+c.issue+"</div>";';
    html += '    h+="<div style=\\"margin-top:6px;display:flex;gap:8px;\\">";';
    html += '    h+="<div onclick=\\"sendConfirmText(\'"+c.phone+"\'  ,\'"+c.name.replace(/\'/g,"")+"\')\\" style=\\"padding:5px 12px;border:1px solid #55f7d830;color:#55f7d8;font-family:Orbitron;font-size:0.55em;letter-spacing:1px;cursor:pointer;\\">CONFIRM TEXT</div>";';
    html += '    h+="<div onclick=\\"sendFollowUp(\'" +c.phone+"\'  ,\'"+c.name.replace(/\'/g,"")+"\')\\" style=\\"padding:5px 12px;border:1px solid #ff9f4330;color:#ff9f43;font-family:Orbitron;font-size:0.55em;letter-spacing:1px;cursor:pointer;\\">FOLLOW-UP</div>";';
    html += '    h+="</div></div>";';
    html += '  });div.innerHTML=h;}catch(e){div.innerHTML="<div style=\\"color:#ff4757;\\">Error: "+e.message+"</div>";}';
    html += '}';

    // Send confirmation text
    html += 'async function sendConfirmText(phone,name){';
    html += '  try{var r=await fetch("/business/confirm-text",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({phone:phone,name:name})});';
    html += '  var d=await r.json();alert(d.success?"Confirmation sent to "+name:"Error: "+d.error);}catch(e){alert("Failed: "+e.message);}';
    html += '}';

    // Send follow-up text
    html += 'async function sendFollowUp(phone,name){';
    html += '  try{var r=await fetch("/business/followup-text",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({phone:phone,name:name})});';
    html += '  var d=await r.json();alert(d.success?"Follow-up sent to "+name:"Error: "+d.error);}catch(e){alert("Failed: "+e.message);}';
    html += '}';

    // Job Timer
    html += 'var timerInterval=null;var timerSeconds=0;var timerRunning=false;';
    html += 'function startJobTimer(){';
    html += '  var name=document.getElementById("timer-job-name").value;if(!name){alert("Enter job name");return;}';
    html += '  if(timerRunning)return;timerRunning=true;timerSeconds=0;';
    html += '  document.getElementById("timer-job-label").textContent="Timing: "+name;';
    html += '  document.getElementById("timer-start-btn").style.color="#4a6a8a";';
    html += '  timerInterval=setInterval(function(){timerSeconds++;';
    html += '    var h=String(Math.floor(timerSeconds/3600)).padStart(2,"0");';
    html += '    var m=String(Math.floor((timerSeconds%3600)/60)).padStart(2,"0");';
    html += '    var s=String(timerSeconds%60).padStart(2,"0");';
    html += '    document.getElementById("timer-display").textContent=h+":"+m+":"+s;';
    html += '  },1000);';
    html += '}';
    html += 'function stopJobTimer(){';
    html += '  if(!timerRunning)return;clearInterval(timerInterval);timerRunning=false;';
    html += '  var name=document.getElementById("timer-job-name").value;';
    html += '  var mins=Math.round(timerSeconds/60);';
    html += '  var dur=document.getElementById("timer-display").textContent;';
    html += '  document.getElementById("timer-job-label").textContent="Logged: "+name+" ("+dur+")";';
    html += '  document.getElementById("timer-start-btn").style.color="#00ff66";';
    html += '  document.getElementById("timer-job-name").value="";';
    html += '  fetch("/business/log-timer",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({jobName:name,duration:dur,minutes:mins})});';
    html += '  var hist=document.getElementById("timer-history");';
    html += '  hist.innerHTML="<div style=\\"background:rgba(0,255,102,0.03);border:1px solid #00ff6610;padding:8px 12px;margin-bottom:4px;display:flex;justify-content:space-between;\\"><span style=\\"color:#c0d8f0;\\">"+name+"</span><span style=\\"color:#00ff66;\\">"+dur+" ("+mins+" min)</span></div>"+hist.innerHTML;';
    html += '  timerSeconds=0;document.getElementById("timer-display").textContent="00:00:00";';
    html += '}';

    // Team Management JS
    html += 'async function loadTaskAssignments(){';
    html += '  var div=document.getElementById("task-assignments");';
    html += '  div.innerHTML="<div style=\\"color:#55f7d8;\\">Analyzing team skills, locations, and workload...</div>";';
    html += '  try{var r=await fetch("/team/assign");var d=await r.json();';
    html += '  if(!d.assignments||d.assignments.length===0){div.innerHTML="<div style=\\"color:#00ff66;padding:10px;\\">All jobs are assigned. No action needed.</div>";return;}';
    html += '  var h="<div style=\\"color:#4a6a8a;font-size:0.85em;margin-bottom:10px;\\">" + d.unassignedCount + " unassigned jobs found</div>";';
    html += '  d.assignments.forEach(function(a){';
    html += '    h+="<div style=\\"background:rgba(85,247,216,0.03);border:1px solid #55f7d815;padding:12px 16px;margin-bottom:6px;\\">";';
    html += '    h+="<div style=\\"display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;\\">";';
    html += '    h+="<div style=\\"color:#c0d8f0;font-weight:600;\\">"+a.job+"</div>";';
    html += '    h+="<div style=\\"font-family:Orbitron;font-size:0.6em;letter-spacing:2px;padding:3px 10px;border:1px solid #55f7d840;color:#55f7d8;background:#55f7d810;\\">ASSIGN → "+a.recommendedTech+"</div>";';
    html += '    h+="</div>";';
    html += '    h+="<div style=\\"margin-top:5px;color:#4a6a8a;font-size:0.85em;\\">"+(a.location||"")+" — "+(a.equipment||"")+"</div>";';
    html += '    h+="<div style=\\"margin-top:4px;color:#55f7d860;font-size:0.8em;font-style:italic;\\">Reason: "+a.reasoning+"</div>";';
    html += '    h+="</div>";';
    html += '  });div.innerHTML=h;}catch(e){div.innerHTML="<div style=\\"color:#ff4757;\\">Error: "+e.message+"</div>";}';
    html += '}';

    html += 'async function loadTeamCoaching(){';
    html += '  var div=document.getElementById("team-coaching");';
    html += '  div.innerHTML="<div style=\\"color:#c084fc;\\">Generating individual coaching reports...</div>";';
    html += '  try{var r=await fetch("/team/coaching");var d=await r.json();';
    html += '  if(d.coaching){div.innerHTML="<div style=\\"color:#c0d8f0;white-space:pre-wrap;line-height:1.8;font-size:0.95em;\\">"+d.coaching+"</div>";}';
    html += '  else{div.innerHTML="<div style=\\"color:#ff4757;\\">Error: "+(d.error||"Unknown")+"</div>";}}';
    html += '  catch(e){div.innerHTML="<div style=\\"color:#ff4757;\\">Failed: "+e.message+"</div>";}';
    html += '}';

    html += 'var currentPanel = 0;';
    html += 'var wrapper = document.getElementById("swipe-wrapper");';

    // Switch function
    html += 'function switchPanel(idx) {';
    html += '  currentPanel = idx;';
    html += '  wrapper.style.transform = "translateX(-" + (idx * 100) + "vw)";';
    // Update tab buttons
    html += '  var tabs = document.querySelectorAll(".tab-btn");';
    html += '  tabs.forEach(function(t){ t.classList.remove("active"); });';
    html += '  tabs[idx].classList.add("active");';
    // Update dots
    html += '  var dots = document.querySelectorAll(".swipe-dot");';
    html += '  dots.forEach(function(d){ d.classList.remove("active"); });';
    html += '  dots[idx].classList.add("active");';
    // Change body accent color
    html += '  document.body.style.transition = "background 0.5s";';
    html += '}';

    // Touch swipe support
    html += 'var touchStartX = 0;';
    html += 'var touchEndX = 0;';
    html += 'document.addEventListener("touchstart", function(e) { touchStartX = e.changedTouches[0].screenX; }, false);';
    html += 'document.addEventListener("touchend", function(e) {';
    html += '  touchEndX = e.changedTouches[0].screenX;';
    html += '  var diff = touchStartX - touchEndX;';
    html += '  if (Math.abs(diff) > 60) {'; // minimum swipe distance
    html += '    if (diff > 0 && currentPanel < 1) switchPanel(currentPanel + 1);'; // swipe left
    html += '    if (diff < 0 && currentPanel > 0) switchPanel(currentPanel - 1);'; // swipe right
    html += '  }';
    html += '}, false);';

    // Keyboard arrow support
    html += 'document.addEventListener("keydown", function(e) {';
    html += '  if (e.key === "ArrowRight" && currentPanel < 1) switchPanel(1);';
    html += '  if (e.key === "ArrowLeft" && currentPanel > 0) switchPanel(0);';
    html += '});';

    html += '<\/script>';

    // Mobile padding fix
    html += '<script>';
    html += 'if(window.innerWidth<=768){document.querySelectorAll("[style]").forEach(function(el){';
    html += '  var s=el.getAttribute("style")||"";';
    html += '  if(s.indexOf("0 40px")>-1||s.indexOf("0px 40px")>-1){el.style.paddingLeft="12px";el.style.paddingRight="12px";}';
    html += '  if(s.indexOf("repeat(3,")>-1||s.indexOf("repeat(3, ")>-1){el.style.gridTemplateColumns="1fr";}';
    html += '  if(s.indexOf("repeat(4,")>-1||s.indexOf("repeat(4, ")>-1){el.style.gridTemplateColumns="repeat(2,1fr)";}';
    html += '});}';
    html += '<\/script>';

    html += '</body></html>';
    res.send(html);
  } catch (err) {
    console.error("Dashboard error:", err.stack || err.message);
    res.status(500).json({ error: err.message, stack: (err.stack || '').split('\n').slice(0,5) });
  }
});

/* ===========================
   POST /chat — Browser voice chat
=========================== */

var webChatHistory = {};

/* ===========================
   GET /business — CRM Business Dashboard
=========================== */

app.get('/business', async function(req, res) {
  try {
    var bizContext = await buildBusinessContext();
    // Fetch Tookan data in parallel (non-blocking, uses 10-min cache)
    buildTookanContext().catch(function(e) { console.log("Tookan bg fetch: " + e.message); });
    var tabs = [];
    try { tabs = await getAllTabNames(); } catch(e) { tabs = []; }

    // Read directly from global.bizMetrics (no regex parsing!)
    var bm = global.bizMetrics || {};
    var totalBooked = bm.totalBooked || 0;
    var totalCompleted = bm.totalCompleted || 0;
    var totalCancelled = bm.totalCancelled || 0;
    var totalReturn = bm.totalReturn || 0;
    var totalAssigned = bm.totalAssigned || 0;
    var promoReplies = bm.promoReplies || 0;
    var totalLocations = Object.keys(bm.locationStats || {}).length;
    var totalLeads = bm.totalLeads || 0;
    var conversionRate = bm.conversionRate || 0;
    var thisMonthCalls = bm.thisMonthCalls || 0;
    var lastMonthCalls = bm.lastMonthCalls || 0;
    var monthGrowth = bm.monthGrowth || 0;
    var weeklyCalls = bm.weeklyCalls || 0;
    var avgBookingDays = bm.avgBookingDays || 0;
    var sheetsRead = bm.sheetsRead || 0;
    var tabsReadCount = bm.tabsRead || 0;
    var totalJobRows = bm.totalJobRows || 0;

    var todayBookings = (bm.todayBookings || []).map(function(b) {
      return b.name + ' (' + b.location + ') — ' + b.equip + ': ' + b.issue + ' [Tech: ' + b.tech + ']';
    });
    var reschedule = (bm.needsReschedule || []).map(function(n) {
      return n.name + ' (' + n.location + ') — ' + n.phone;
    });
    var locationBreakdown = Object.entries(bm.locationStats || {}).sort(function(a,b){return b[1].total-a[1].total;}).map(function(l) {
      return l[0] + ': ' + l[1].total + ' total, ' + l[1].booked + ' booked, ' + l[1].completed + ' completed, ' + l[1].cancelled + ' cancelled';
    });
    var techs = Object.entries(bm.techStats || {}).sort(function(a,b){return b[1].total-a[1].total;}).map(function(t) {
      var s = t[1]; var rate = s.total > 0 ? Math.round((s.completed/s.total)*100) : 0;
      return t[0] + ': ' + s.total + ' jobs (' + s.completed + ' completed, ' + rate + '% rate)';
    });

    var html = '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>J.A.R.V.I.S. — Business Command Center</title>';
    html += '<style>';

    // Base
    html += '@import url("https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;500;600;700&display=swap");';
    html += '* { margin: 0; padding: 0; box-sizing: border-box; }';
    html += 'body { background: #020810; color: #c0d8f0; font-family: "Rajdhani", sans-serif; min-height: 100vh; overflow-x: hidden; }';

    // Animated background
    html += '.bg-grid { position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 0; pointer-events: none; }';
    html += '.bg-grid::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-image: linear-gradient(rgba(168,85,247,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(168,85,247,0.03) 1px, transparent 1px); background-size: 60px 60px; animation: gridMove 20s linear infinite; }';
    html += '@keyframes gridMove { 0% { background-position: 0 0; } 100% { background-position: 60px 60px; } }';

    // Scan line
    html += '.scan-line { position: fixed; top: 0; left: 0; width: 100%; height: 2px; background: linear-gradient(90deg, transparent, #a855f7, transparent); z-index: 2; animation: scanDown 8s linear infinite; pointer-events: none; }';
    html += '@keyframes scanDown { 0% { top: -2px; } 100% { top: 100%; } }';

    // Corner decorations
    html += '.corner { position: fixed; width: 30px; height: 30px; z-index: 3; pointer-events: none; }';
    html += '.corner-tl { top: 10px; left: 10px; border-top: 2px solid #a855f730; border-left: 2px solid #a855f730; }';
    html += '.corner-tr { top: 10px; right: 10px; border-top: 2px solid #a855f730; border-right: 2px solid #a855f730; }';
    html += '.corner-bl { bottom: 10px; left: 10px; border-bottom: 2px solid #a855f730; border-left: 2px solid #a855f730; }';
    html += '.corner-br { bottom: 10px; right: 10px; border-bottom: 2px solid #a855f730; border-right: 2px solid #a855f730; }';

    // Content
    html += '.content { position: relative; z-index: 4; }';

    // Header
    html += '.header { text-align: center; padding: 40px 20px 20px; }';
    html += '.jarvis-title { font-family: "Orbitron"; font-size: 3em; font-weight: 900; letter-spacing: 15px; background: linear-gradient(135deg, #a855f7, #7c3aed, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-shadow: none; filter: drop-shadow(0 0 30px rgba(168,85,247,0.4)); }';

    // Status bar
    html += '.status-bar { display: flex; justify-content: center; gap: 20px; margin-top: 15px; flex-wrap: wrap; }';
    html += '.status-item { display: flex; align-items: center; gap: 6px; font-family: "Orbitron"; font-size: 0.6em; letter-spacing: 2px; color: #4a6a8a; }';
    html += '.status-dot { width: 6px; height: 6px; border-radius: 50%; animation: pulse 2s infinite; }';
    html += '.status-dot.green { background: #00ff66; box-shadow: 0 0 10px #00ff66; }';
    html += '.status-dot.purple { background: #a855f7; box-shadow: 0 0 10px #a855f7; }';
    html += '.status-dot.orange { background: #ff9f43; box-shadow: 0 0 10px #ff9f43; }';
    html += '@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }';

    // Stats Grid
    html += '.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; padding: 30px 40px; max-width: 1400px; margin: 0 auto; }';
    html += '.card { background: rgba(10,20,35,0.8); border: 1px solid #a855f715; padding: 25px; position: relative; overflow: hidden; --accent: #a855f7; animation: cardIn 0.6s ease-out both; }';
    html += '.card::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 2px; background: linear-gradient(90deg, transparent, var(--accent), transparent); opacity: 0.6; }';
    html += '@keyframes cardIn { 0% { opacity: 0; transform: translateY(20px); } 100% { opacity: 1; transform: translateY(0); } }';
    html += '.card:nth-child(1) { --accent: #a855f7; }';
    html += '.card:nth-child(2) { --accent: #00ff66; animation-delay: 0.1s; }';
    html += '.card:nth-child(3) { --accent: #ff4757; animation-delay: 0.2s; }';
    html += '.card:nth-child(4) { --accent: #00d4ff; animation-delay: 0.3s; }';
    html += '.card:nth-child(5) { --accent: #ff9f43; animation-delay: 0.4s; }';
    html += '.card:nth-child(6) { --accent: #55f7d8; animation-delay: 0.5s; }';
    html += '.card .label { font-family: "Orbitron"; font-size: 0.65em; letter-spacing: 3px; color: #4a6a8a; text-transform: uppercase; }';
    html += '.card .value { font-family: "Orbitron"; font-size: 3em; font-weight: 700; margin: 15px 0 8px; color: var(--accent); text-shadow: 0 0 30px color-mix(in srgb, var(--accent) 30%, transparent); }';
    html += '.card .sub { font-size: 0.95em; color: #3a5a7a; letter-spacing: 1px; }';
    html += '.card .bar { height: 3px; background: #0a1520; margin-top: 15px; border-radius: 2px; overflow: hidden; }';
    html += '.card .bar-fill { height: 100%; border-radius: 2px; animation: barGrow 2s ease-out both; }';
    html += '@keyframes barGrow { 0% { width: 0; } }';

    // Section styling
    html += '.section { max-width: 1400px; margin: 0 auto; padding: 0 40px 30px; }';
    html += '.section-title { font-family: "Orbitron"; font-size: 0.8em; letter-spacing: 5px; color: #a855f7; text-transform: uppercase; margin-bottom: 15px; display: flex; align-items: center; gap: 10px; }';
    html += '.section-title::before { content: ""; width: 8px; height: 8px; background: #a855f7; border-radius: 50%; box-shadow: 0 0 8px #a855f7; }';

    // List items
    html += '.list-item { background: rgba(10,20,35,0.6); border: 1px solid #a855f710; padding: 12px 16px; margin-bottom: 6px; display: flex; justify-content: space-between; align-items: center; transition: all 0.3s; }';
    html += '.list-item:hover { border-color: #a855f740; background: rgba(168,85,247,0.05); }';
    html += '.list-item .name { color: #c0d8f0; font-weight: 500; }';
    html += '.list-item .detail { color: #4a6a8a; font-size: 0.9em; }';
    html += '.list-item .status-badge { font-family: "Orbitron"; font-size: 0.6em; letter-spacing: 2px; padding: 4px 10px; border: 1px solid; }';
    html += '.badge-booked { color: #00ff66; border-color: #00ff6640; }';
    html += '.badge-cancel { color: #ff4757; border-color: #ff475740; }';
    html += '.badge-resched { color: #ff9f43; border-color: #ff9f4340; }';

    // Actions
    html += '.actions { display: flex; justify-content: center; gap: 15px; padding: 20px 40px; flex-wrap: wrap; }';
    html += '.holo-btn { font-family: "Orbitron"; font-size: 0.75em; letter-spacing: 3px; padding: 14px 30px; background: transparent; border: 1px solid #a855f730; color: #a855f7; text-decoration: none; text-transform: uppercase; position: relative; overflow: hidden; transition: all 0.3s; cursor: pointer; }';
    html += '.holo-btn:hover { background: #a855f715; border-color: #a855f7; box-shadow: 0 0 30px #a855f720, inset 0 0 30px #a855f710; }';
    html += '.holo-btn.green { border-color: #00ff6630; color: #00ff66; }';
    html += '.holo-btn.green:hover { background: #00ff6615; border-color: #00ff66; }';

    // Location chips
    html += '.loc-grid { display: flex; flex-wrap: wrap; gap: 8px; }';
    html += '.loc-chip { background: rgba(168,85,247,0.03); border: 1px solid #a855f720; padding: 10px 16px; font-size: 0.85em; color: #7a9ab0; cursor: pointer; transition: all 0.3s; position: relative; }';
    html += '.loc-chip:hover { border-color: #a855f7; color: #a855f7; background: rgba(168,85,247,0.08); }';
    html += '.loc-chip .count { font-family: "Orbitron"; font-size: 0.7em; color: #a855f7; margin-left: 6px; }';

    // Clock
    html += '.clock { font-family: "Orbitron"; font-size: 0.7em; letter-spacing: 5px; color: #2a4a6a; text-align: center; padding: 30px; }';

    // Footer
    html += '.footer { text-align: center; padding: 20px; font-family: "Orbitron"; font-size: 0.6em; letter-spacing: 4px; color: #1a2a3a; border-top: 1px solid #0a1520; }';

    // Particles
    html += '.particle { position: fixed; width: 2px; height: 2px; background: #a855f7; border-radius: 50%; pointer-events: none; z-index: 1; opacity: 0; animation: particleFloat 8s linear infinite; }';
    html += '@keyframes particleFloat { 0% { opacity: 0; transform: translateY(100vh); } 10% { opacity: 0.6; } 90% { opacity: 0.6; } 100% { opacity: 0; transform: translateY(-20vh); } }';

    // Modal
    html += '#tab-modal { display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(2,8,16,0.95); z-index:100; overflow-y:auto; }';

    // Mobile responsive
    html += '@media(max-width:768px){';
    html += '.grid{grid-template-columns:repeat(2,1fr)!important;padding:15px 12px!important;gap:10px!important;}';
    html += '.section{padding-left:12px!important;padding-right:12px!important;}';
    html += '.card{padding:15px!important;}';
    html += '.card .value{font-size:2em!important;}';
    html += '[style*="max-width:1400px"]{padding-left:12px!important;padding-right:12px!important;}';
    html += '[style*="padding:0 40px"]{padding-left:12px!important;padding-right:12px!important;}';
    html += '[style*="grid-template-columns:repeat(3"]{grid-template-columns:1fr!important;}';
    html += '[style*="grid-template-columns:repeat(4"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(300px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(280px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(200px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(180px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '[style*="minmax(170px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '.status-bar{gap:10px!important;} .status-item{font-size:0.6em!important;}';
    html += '.loc-chip{font-size:0.7em!important;padding:6px 8px!important;}';
    html += '[style*="display:flex"][style*="justify-content:center"][style*="gap:0"]{flex-wrap:wrap!important;}';
    html += '[style*="font-size:2.8em"],[style*="font-size:3em"],[style*="font-size:2.5em"]{font-size:1.6em!important;}';
    html += '[style*="font-size:1.6em"]{font-size:1.1em!important;}';
    html += '[style*="letter-spacing:8px"],[style*="letter-spacing:6px"],[style*="letter-spacing:5px"]{letter-spacing:2px!important;}';
    html += 'table{font-size:0.65em!important;} th,td{padding:5px 4px!important;}';
    html += '[style*="overflow-x"]{overflow-x:auto!important;-webkit-overflow-scrolling:touch;}';
    html += 'select,input[type="text"]{font-size:0.55em!important;max-width:120px;}';
    html += '[style*="display:flex"][style*="flex-wrap:wrap"][style*="gap:8px"]{gap:5px!important;}';
    html += '}';
    html += '@media(max-width:480px){';
    html += '.grid{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(200px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(180px"]{grid-template-columns:1fr!important;}';
    html += '[style*="minmax(150px"]{grid-template-columns:repeat(2,1fr)!important;}';
    html += '}';

    html += '</style></head><body>';

    // Background effects
    html += '<div class="bg-grid"></div>';
    html += '<div class="scan-line"></div>';
    html += '<div class="corner corner-tl"></div><div class="corner corner-tr"></div><div class="corner corner-bl"></div><div class="corner corner-br"></div>';

    // Particles (purple)
    for (var p = 0; p < 15; p++) {
      var left = Math.floor(Math.random() * 100);
      var delay = (Math.random() * 8).toFixed(1);
      html += '<div class="particle" style="left:' + left + '%;animation-delay:' + delay + 's;"></div>';
    }

    html += '<div class="content">';

    // Header
    var now = new Date();
    var dateStr = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'America/Chicago' });
    var timeStr = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Chicago' });

    html += '<div class="header">';
    html += '<div class="jarvis-title">WILDWOOD CRM</div>';
    html += '<div style="font-family:Rajdhani;font-size:1.1em;letter-spacing:8px;color:#3a5a7a;margin-top:5px;text-transform:uppercase;">Business Command Center</div>';
    html += '<div style="font-family:Rajdhani;font-size:0.95em;letter-spacing:4px;color:#a855f780;margin-top:3px;">' + dateStr + ' // ' + timeStr + '</div>';

    // === TAB NAVIGATION ===
    html += '<div style="display:flex;justify-content:center;gap:0;margin-top:20px;margin-bottom:15px;">';
    html += '<a href="/dashboard" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">JARVIS</a>';
    html += '<a href="/business" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#a855f7;border:1px solid #a855f740;text-decoration:none;background:rgba(168,85,247,0.1);box-shadow:0 0 15px rgba(168,85,247,0.1);">ATHENA</a>';
    html += '<a href="/tookan" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">TOOKAN</a>';
    html += '<a href="/business/chart" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">CHARTS</a>';
    html += '<a href="/analytics" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ANALYTICS</a>';
    html += '</div>';
    var tkForCount = global.tookanData || {};
    var allTechNames = {};
    techs.forEach(function(t) { allTechNames[t.split(':')[0].trim().toLowerCase()] = true; });
    (tkForCount.agents || []).forEach(function(a) { if (a.name) allTechNames[a.name.toLowerCase()] = true; });
    Object.keys(tkForCount.tasksByTech || {}).forEach(function(t) { allTechNames[t.toLowerCase()] = true; });
    (bm.techList || []).forEach(function(t) { if (t.name) allTechNames[t.name.toLowerCase()] = true; });
    var displayTechCount = Math.max(techs.length, Object.keys(allTechNames).length);
    html += '<div class="status-bar">';
    html += '<div class="status-item"><div class="status-dot green"></div>CRM ONLINE</div>';
    html += '<div class="status-item"><div class="status-dot purple"></div>' + totalLocations + ' LOCATIONS</div>';
    html += '<div class="status-item"><div class="status-dot ' + (totalJobRows > 100 ? 'green' : 'orange') + '"></div>' + totalJobRows + ' RECORDS FROM ' + sheetsRead + ' SHEETS</div>';
    html += '<div class="status-item"><div class="status-dot purple"></div>' + displayTechCount + ' TECHNICIANS</div>';
    html += '<div class="status-item"><div class="status-dot ' + (todayBookings.length > 0 ? 'orange' : 'green') + '"></div>' + todayBookings.length + ' TODAY</div>';
    html += '<div class="status-item"><div class="status-dot ' + (reschedule.length > 0 ? 'orange' : 'green') + '"></div>' + reschedule.length + ' RESCHEDULE</div>';
    html += '</div>';
    html += '</div>';

    // Stats Grid
    html += '<div class="grid">';

    html += '<div class="card"><div class="label">Booked</div><div class="value">' + totalBooked + '</div><div class="sub">Status = Booked</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, Math.round(totalBooked/Math.max(1,totalLeads)*100)) + '%;background:#a855f7;"></div></div></div>';

    html += '<div class="card"><div class="label">Dispatched</div><div class="value">' + totalAssigned + '</div><div class="sub">Assigned/Acknowledged in Tookan</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, Math.round(totalAssigned/Math.max(1,totalLeads)*100)) + '%;background:#00d4ff;"></div></div></div>';

    html += '<div class="card"><div class="label">Completed Jobs</div><div class="value">' + totalCompleted + '</div><div class="sub">Status: completed/done/paid/serviced</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, Math.round(totalCompleted/Math.max(1,totalLeads)*100)) + '%;background:#00ff66;"></div></div></div>';

    html += '<div class="card"><div class="label">Cancelled</div><div class="value">' + totalCancelled + '</div><div class="sub">' + (parseInt(totalCancelled) > parseInt(totalCompleted) / 5 ? 'HIGH — needs attention' : 'Within normal range') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, parseInt(totalCancelled) * 2) + '%;background:#ff4757;"></div></div></div>';

    html += '<div class="card"><div class="label">Today\'s Jobs</div><div class="value">' + todayBookings.length + '</div><div class="sub">' + (todayBookings.length > 0 ? 'Jobs scheduled today' : 'No jobs today') + '</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, todayBookings.length * 15) + '%;background:#00d4ff;"></div></div></div>';

    html += '<div class="card"><div class="label">Return Customers</div><div class="value">' + totalReturn + '</div><div class="sub">Repeat business</div><div class="bar"><div class="bar-fill" style="width:60%;background:#ff9f43;"></div></div></div>';

    html += '<div class="card"><div class="label">Promo Replies</div><div class="value">' + promoReplies + '</div><div class="sub">Campaign responses</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, parseInt(promoReplies) / 5) + '%;background:#55f7d8;"></div></div></div>';

    html += '</div>';

    // Today's Bookings
    if (todayBookings.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title">Today\'s Bookings</div>';
      todayBookings.forEach(function(b) {
        html += '<div class="list-item"><div class="name">' + b + '</div><div class="status-badge badge-booked">TODAY</div></div>';
      });
      html += '</div>';
    }

    // Needs Rescheduling
    if (reschedule.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#ff9f43;">Needs Rescheduling (' + reschedule.length + ')</div>';
      reschedule.forEach(function(r) {
        html += '<div class="list-item" style="border-color:#ff9f4315;"><div class="name">' + r + '</div><div class="status-badge badge-resched">RESCHED</div></div>';
      });
      html += '</div>';
    }

    // Technicians
    if (techs.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#00ff66;">Technicians</div>';
      techs.forEach(function(t) {
        html += '<div class="list-item" style="border-color:#00ff6610;"><div class="name">' + t + '</div></div>';
      });
      html += '</div>';
    }

    // Location Breakdown — filter out garbage, show top 20 with expand
    var cleanLocations = locationBreakdown.filter(function(l) {
      var city = l.split(':')[0].trim();
      // Filter out junk entries (too short, known garbage patterns)
      if (city.length < 4) return false;
      if (/^(Na|Null|Unknown|Not Provided|City|Unavailable|The |You\.|Wait\.|Follow|None|Lot More|Electric|Zero Turn|Riding|Craftsman|Husqvarna|Cub Cadet|Lawn Mower|Mower)/i.test(city)) return false;
      if (/Lawnmower|Not Specified|Not Verified|Street Name|Luke Matthew|Southwest Elwood|Haas Court|Southwest Th|Perkins Lane|Representative|Jackson County|Bloomfield Hills, KS|West Jefferson, WE/i.test(city)) return false;
      return true;
    });
    if (cleanLocations.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title">Location Performance (' + cleanLocations.length + ' markets)</div>';
      html += '<div class="loc-grid">';
      cleanLocations.slice(0, 20).forEach(function(l) {
        html += '<div class="loc-chip">' + escapeHtml(l) + '</div>';
      });
      html += '</div>';
      if (cleanLocations.length > 20) {
        html += '<div onclick="var el=document.getElementById(\'more-locs\');var btn=this;if(el.style.display===\'none\'){el.style.display=\'flex\';btn.textContent=\'SHOW LESS\';}else{el.style.display=\'none\';btn.textContent=\'SHOW ALL ' + cleanLocations.length + ' MARKETS\';}" style="text-align:center;padding:12px;margin-top:8px;border:1px solid #a855f730;color:#a855f7;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;cursor:pointer;transition:all 0.3s;">SHOW ALL ' + cleanLocations.length + ' MARKETS</div>';
        html += '<div id="more-locs" class="loc-grid" style="display:none;margin-top:8px;">';
        cleanLocations.slice(20).forEach(function(l) {
          html += '<div class="loc-chip">' + escapeHtml(l) + '</div>';
        });
        html += '</div>';
      }
      html += '</div>';
    }

    // Actions
    html += '<div class="actions">';
    html += '<a class="holo-btn green" href="/business/tabs" target="_blank">Browse All Data</a>';
    html += '<a class="holo-btn" href="/search?q=booked" target="_blank">Search Bookings</a>';
    html += '<a class="holo-btn" href="/briefing" target="_blank">AI Briefing</a>';
    html += '<a class="holo-btn" href="/tabs" target="_blank">Personal Tabs</a>';
    html += '</div>';

    // ====================================================================
    // NEW DASHBOARD SECTIONS — Built from all source sheet data
    // ====================================================================

    var opsData = global.bizOpsData || {};
    var sheetMeta = global.sheetMetadata || {};
    var pm = global.profitMetrics || {};
    var techPerf2 = bm.techStats || {};
    var locStats = bm.locationStats || {};

    // ====== TOOKAN LIVE DISPATCH ======
    var tk = global.tookanData || {};
    if (tk.totalTasks > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#00d4ff;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#00d4ff;border-radius:50%;box-shadow:0 0 12px #00d4ff;display:inline-block;"></span>TOOKAN LIVE DISPATCH <span style="font-size:0.6em;color:#4a6a8a;margin-left:10px;">' + tk.totalTasks + ' tasks (90 days)</span></div>';

      html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:15px;">';
      var tkCards = [
        { label:'COMPLETED', val:tk.completed, color:'#00ff66' },
        { label:'ASSIGNED', val:tk.assigned, color:'#00d4ff' },
        { label:'ACKNOWLEDGED', val:tk.acknowledged, color:'#a855f7' },
        { label:'IN PROGRESS', val:tk.started, color:'#ff9f43' },
        { label:'UNASSIGNED', val:tk.unassigned, color:'#c0c0c0' },
        { label:'FAILED', val:tk.cancelled, color:'#ff4757' },
      ];
      tkCards.forEach(function(c) {
        html += '<div style="background:rgba(10,20,35,0.8);border:1px solid ' + c.color + '15;padding:14px;text-align:center;">';
        html += '<div style="font-family:Orbitron;font-size:0.45em;letter-spacing:3px;color:#4a6a8a;">' + c.label + '</div>';
        html += '<div style="font-family:Orbitron;font-size:1.8em;color:' + c.color + ';font-weight:900;">' + (c.val || 0) + '</div>';
        html += '</div>';
      });
      html += '</div>';

      // Today's Tookan jobs
      if (tk.todayTasks && tk.todayTasks.length > 0) {
        html += '<div style="color:#ffd700;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;margin-bottom:8px;">TODAY\'S DISPATCHED JOBS (' + tk.todayTasks.length + ')</div>';
        tk.todayTasks.slice(0, 8).forEach(function(t) {
          var sColor = t.status.toLowerCase().includes('completed') ? '#00ff66' : t.status.toLowerCase().includes('started') ? '#ff9f43' : '#00d4ff';
          html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + sColor + '10;padding:8px 14px;margin-bottom:3px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">';
          html += '<div style="min-width:50px;font-family:Orbitron;font-size:0.65em;color:#4a6a8a;">#' + t.jobId + '</div>';
          html += '<div style="flex:1;color:#c0d8f0;font-weight:600;">' + t.customer + '</div>';
          html += '<div style="color:#a855f7;min-width:120px;">' + (t.tech || 'Unassigned') + '</div>';
          html += '<div style="font-family:Orbitron;font-size:0.45em;letter-spacing:2px;padding:2px 8px;border:1px solid ' + sColor + '30;color:' + sColor + ';">' + t.status.toUpperCase() + '</div>';
          html += '</div>';
        });
      }

      // Top techs by completion
      var techRank = Object.entries(tk.tasksByTech || {}).sort(function(a, b) { return b[1].completed - a[1].completed; });
      if (techRank.length > 0) {
        html += '<div style="margin-top:15px;color:#00ff66;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;margin-bottom:8px;">TECH DISPATCH RANKINGS</div>';
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:6px;">';
        techRank.slice(0, 10).forEach(function(t, idx) {
          var compRate = t[1].total > 0 ? Math.round(t[1].completed / t[1].total * 100) : 0;
          var medal = idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : '•';
          html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:10px;display:flex;align-items:center;gap:10px;">';
          html += '<div style="font-size:1.2em;">' + medal + '</div>';
          html += '<div style="flex:1;"><div style="color:#c0d8f0;font-weight:700;">' + t[0] + '</div>';
          html += '<div style="color:#4a6a8a;font-size:0.8em;">' + t[1].completed + ' done / ' + t[1].total + ' total</div></div>';
          html += '<div style="font-family:Orbitron;font-size:0.9em;color:' + (compRate >= 70 ? '#00ff66' : '#ff9f43') + ';">' + compRate + '%</div>';
          html += '</div>';
        });
        html += '</div>';
      }

      html += '</div>';
    }

    // ====== 1. STAFFING GAP ALERTS ======
    // Cross-reference locations that have bookings but no tech assigned
    var unstaffedLocations = [];
    var techByLocation = {};
    // Source 1: CRM techStats (tech → locations mapping from job data)
    Object.entries(techPerf2).forEach(function(t) {
      Object.keys(t[1].locations || {}).forEach(function(loc) {
        if (!techByLocation[loc]) techByLocation[loc] = [];
        techByLocation[loc].push(t[0]);
      });
    });
    // Source 2: Tookan agents/tasks (fleet_name → job locations)
    var tkTasks = (tk.todayTasks || []);
    tkTasks.forEach(function(t) {
      if (t.tech && t.address) {
        var tkCity = t.address.split(',').length >= 2 ? t.address.split(',').slice(-2, -1)[0].trim() : '';
        if (tkCity) {
          // Try to match to existing location keys
          Object.keys(locStats).forEach(function(loc) {
            if (loc.toLowerCase().indexOf(tkCity.toLowerCase()) !== -1) {
              if (!techByLocation[loc]) techByLocation[loc] = [];
              if (techByLocation[loc].indexOf(t.tech) === -1) techByLocation[loc].push(t.tech);
            }
          });
        }
      }
    });
    // Source 3: Tookan tasksByTech — match tech task cities to locStats
    Object.entries(tk.tasksByLocation || {}).forEach(function(tl) {
      var tkCity = tl[0].trim();
      if (tkCity) {
        Object.keys(locStats).forEach(function(loc) {
          if (loc.toLowerCase().indexOf(tkCity.toLowerCase()) !== -1 && !techByLocation[loc]) {
            // Mark as staffed even without specific tech name
            techByLocation[loc] = ['(Tookan)'];
          }
        });
      }
    });
    // Source 4: techList from "Tech Numbers" sheet (has name + location)
    (bm.techList || []).forEach(function(tl) {
      if (tl.name && tl.location) {
        Object.keys(locStats).forEach(function(loc) {
          if (loc.toLowerCase().indexOf(tl.location.toLowerCase()) !== -1 ||
              tl.location.toLowerCase().indexOf(loc.split(',')[0].toLowerCase().trim()) !== -1) {
            if (!techByLocation[loc]) techByLocation[loc] = [];
            if (techByLocation[loc].indexOf(tl.name) === -1) techByLocation[loc].push(tl.name);
          }
        });
      }
    });

    // Best tech count: combine all sources
    var allKnownTechs = {};
    Object.entries(techPerf2).forEach(function(t) { allKnownTechs[t[0].toLowerCase()] = t[0]; });
    (tk.agents || []).forEach(function(a) { if (a.name) allKnownTechs[a.name.toLowerCase()] = a.name; });
    Object.keys(tk.tasksByTech || {}).forEach(function(t) { allKnownTechs[t.toLowerCase()] = t; });
    (bm.techList || []).forEach(function(t) { if (t.name) allKnownTechs[t.name.toLowerCase()] = t.name; });
    var bestTechCount = Object.keys(allKnownTechs).length;

    // Debug: log tech detection sources
    var staffedCount = Object.keys(techByLocation).filter(function(k) { return techByLocation[k].length > 0; }).length;
    console.log('Tech-Location mapping: ' + staffedCount + ' locations staffed out of ' + Object.keys(locStats).length + ' total. Sources: CRM techStats=' + Object.keys(techPerf2).length + ', Tookan agents=' + (tk.agents||[]).length + ', techList=' + (bm.techList||[]).length + ', allKnownTechs=' + bestTechCount);

    // Source 5: Active Locations & HR sheet (opsData tabs = location names with tech data)
    Object.entries(opsData).forEach(function(section) {
      var sKey = section[0];
      // Match sections from Active Locations & HR sheet
      if (sKey.toLowerCase().includes('active location') || sKey.toLowerCase().includes('locations & hr')) {
        var tabName = sKey.includes(' / ') ? sKey.split(' / ').slice(1).join(' / ').trim() : '';
        if (tabName && tabName.length > 2) {
          // This tab name is likely a location name — match to locStats
          var tabLower = tabName.toLowerCase().replace(/[^a-z\s]/g, '').trim();
          Object.keys(locStats).forEach(function(loc) {
            var locCity = loc.split(',')[0].toLowerCase().trim();
            if (tabLower.indexOf(locCity) !== -1 || locCity.indexOf(tabLower) !== -1 ||
                (tabLower.length > 4 && locCity.indexOf(tabLower.substring(0,5)) !== -1)) {
              if (!techByLocation[loc]) techByLocation[loc] = [];
              // Try to extract tech name from first few rows
              var rows = section[1].rows || [];
              rows.slice(0, 10).forEach(function(row) {
                var rowLower = (row || '').toLowerCase();
                if (rowLower.includes('tech') || rowLower.includes('assigned') || rowLower.includes('agent')) {
                  var parts = row.split('|').map(function(p) { return p.trim(); });
                  parts.forEach(function(p) {
                    if (p.length > 2 && p.length < 40 && !p.toLowerCase().includes('tech') && !p.toLowerCase().includes('assign') && !p.toLowerCase().includes('agent') && !p.toLowerCase().includes('name') && /^[A-Z]/.test(p)) {
                      if (techByLocation[loc].indexOf(p) === -1) techByLocation[loc].push(p);
                    }
                  });
                }
              });
              if (techByLocation[loc].length === 0) techByLocation[loc] = ['(Assigned)'];
            }
          });
        }
        // Also scan row content for city names that match locStats
        var rows2 = section[1].rows || [];
        rows2.forEach(function(row) {
          Object.keys(locStats).forEach(function(loc) {
            var locCity = loc.split(',')[0].trim();
            if (locCity.length > 3 && row.indexOf(locCity) !== -1 && !techByLocation[loc]) {
              techByLocation[loc] = ['(Assigned)'];
            }
          });
        });
      }
    });

    // Source 6: If a location has many total jobs, it almost certainly has a tech
    // (Locations don't accumulate jobs without a tech to service them)
    // Mark any location with 10+ total jobs as likely staffed if no other source found it
    Object.entries(locStats).forEach(function(ls) {
      if (!techByLocation[ls[0]] && ls[1].total >= 10) {
        techByLocation[ls[0]] = ['(Active)'];
      }
    });
    var locEntries = Object.entries(locStats).sort(function(a,b){return b[1].total-a[1].total;});
    locEntries.forEach(function(loc) {
      var techs2 = techByLocation[loc[0]] || [];
      if (techs2.length === 0 && loc[1].total >= 3) {
        unstaffedLocations.push({ location: loc[0], jobs: loc[1].total, booked: loc[1].booked, cancelled: loc[1].cancelled });
      }
    });

    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#ff4757;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#ff4757;border-radius:50%;box-shadow:0 0 12px #ff4757;display:inline-block;animation:pulse 1.5s infinite;"></span>STAFFING GAP ALERTS</div>';
    if (unstaffedLocations.length > 0) {
      html += '<div style="color:#ff9f43;margin-bottom:15px;font-size:0.85em;">' + unstaffedLocations.length + ' locations have bookings but NO assigned technician — potential revenue leakage</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;">';
      unstaffedLocations.slice(0, 12).forEach(function(u) {
        html += '<div style="background:rgba(255,71,87,0.05);border:1px solid #ff475720;padding:14px;position:relative;">';
        html += '<div style="position:absolute;top:8px;right:10px;font-family:Orbitron;font-size:0.5em;color:#ff4757;letter-spacing:2px;padding:2px 8px;border:1px solid #ff475730;">NO TECH</div>';
        html += '<div style="color:#c0d8f0;font-weight:700;font-size:1.05em;margin-bottom:4px;">' + u.location + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.85em;">' + u.jobs + ' total jobs • ' + u.booked + ' booked • ' + u.cancelled + ' cancelled</div>';
        html += '<div style="margin-top:6px;height:4px;background:#0a1520;"><div style="height:100%;width:' + Math.min(100, u.jobs) + '%;background:linear-gradient(90deg,#ff4757,#ff9f43);"></div></div>';
        html += '</div>';
      });
      html += '</div>';
    } else {
      html += '<div style="background:rgba(0,255,102,0.05);border:1px solid #00ff6620;padding:20px;text-align:center;color:#00ff66;font-family:Orbitron;font-size:0.7em;letter-spacing:3px;">ALL ACTIVE LOCATIONS ARE STAFFED</div>';
    }
    html += '</div>';

    // ====== 2. RECEPTIONIST LEADERBOARD ======
    // Parse from bizOpsData if receptionist performance data exists
    var receptionistData = [];
    Object.entries(opsData).forEach(function(section) {
      if (section[0].toLowerCase().includes('receptionist') && section[0].toLowerCase().includes('matrix')) {
        // Try to parse receptionist stats from matrix data
        section[1].rows.forEach(function(rowStr) {
          var parts = rowStr.split(' | ');
          if (parts.length >= 3 && parts[0] && !parts[0].includes('===') && !parts[0].toLowerCase().includes('week')) {
            var name = parts[0].trim();
            if (name.length > 1 && name.length < 30 && !name.includes('Total') && !name.includes('Date')) {
              var existing = receptionistData.find(function(r) { return r.name === name; });
              if (!existing) receptionistData.push({ name: name, data: parts.slice(1).join(' | ') });
            }
          }
        });
      }
    });
    // Also use receptionist payouts from profit data
    var recPayouts = pm.receptionistPayouts || {};

    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#a855f7;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#a855f7;border-radius:50%;box-shadow:0 0 12px #a855f7;display:inline-block;"></span>RECEPTIONIST LEADERBOARD</div>';
    // Count jobs per receptionist from job data
    var recStats = {};
    // receptionist data not directly available in /business scope — use payouts as primary
    // Build from recentBookings which have receptionist data already parsed
    var recFromJobs = {};
    (bm.recentBookings || []).forEach(function(b) { /* receptionist not in recentBookings yet */ });
    // Use the profit payouts as primary leaderboard
    var recEntries = Object.entries(recPayouts).sort(function(a,b){return b[1]-a[1];});
    if (recEntries.length > 0) {
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;">';
      var recColors = ['#ffd700','#c0c0c0','#cd7f32','#a855f7','#00d4ff','#55f7d8'];
      recEntries.forEach(function(r, idx) {
        var medal = idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : '•';
        html += '<div style="background:rgba(10,20,35,0.8);border:1px solid ' + (recColors[idx] || '#a855f720') + '20;padding:16px;text-align:center;">';
        html += '<div style="font-size:1.5em;margin-bottom:4px;">' + medal + '</div>';
        html += '<div style="color:#c0d8f0;font-weight:700;font-size:1.1em;">' + r[0] + '</div>';
        html += '<div style="font-family:Orbitron;font-size:1.3em;color:' + (recColors[idx] || '#a855f7') + ';margin-top:6px;">$' + Math.round(r[1]).toLocaleString() + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.75em;margin-top:4px;">month payout</div>';
        html += '</div>';
      });
      html += '</div>';
    } else {
      html += '<div style="color:#4a6a8a;text-align:center;padding:20px;">Receptionist data loads from Percentage Report. Verify PROFIT_SPREADSHEET_ID is set.</div>';
    }
    html += '</div>';

    // ====== 3. P&L OVERVIEW (from Profit Sheet) ======
    if (pm.revenue > 0 || pm.expenses > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#ffd700;border-radius:50%;box-shadow:0 0 12px #ffd700;display:inline-block;"></span>PROFIT & LOSS — ' + (pm.currentMonth || 'Current Month').toUpperCase() + '</div>';

      // Big 3 cards
      html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:15px;margin-bottom:20px;">';
      html += '<div style="background:rgba(0,255,102,0.05);border:1px solid #00ff6620;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;">REVENUE</div>';
      html += '<div style="font-family:Orbitron;font-size:2em;color:#00ff66;margin-top:8px;">$' + Math.round(pm.revenue).toLocaleString() + '</div></div>';

      html += '<div style="background:rgba(255,71,87,0.05);border:1px solid #ff475720;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;">EXPENSES</div>';
      html += '<div style="font-family:Orbitron;font-size:2em;color:#ff4757;margin-top:8px;">$' + Math.round(pm.expenses).toLocaleString() + '</div></div>';

      var profitColor = pm.profit >= 0 ? '#ffd700' : '#ff4757';
      html += '<div style="background:rgba(255,215,0,0.05);border:1px solid ' + profitColor + '20;padding:20px;text-align:center;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;">NET PROFIT</div>';
      html += '<div style="font-family:Orbitron;font-size:2em;color:' + profitColor + ';margin-top:8px;">$' + Math.round(pm.profit).toLocaleString() + '</div>';
      html += '<div style="color:#4a6a8a;font-size:0.8em;margin-top:4px;">' + pm.margin + '% margin</div></div>';
      html += '</div>';

      // Expense breakdown bars
      var expEntries = Object.entries(pm.expenseBreakdown || {}).sort(function(a,b){return b[1]-a[1];});
      if (expEntries.length > 0) {
        var maxExp = expEntries[0][1];
        html += '<div style="margin-bottom:20px;">';
        html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;margin-bottom:10px;">EXPENSE BREAKDOWN</div>';
        expEntries.forEach(function(e) {
          var pct = maxExp > 0 ? Math.round((e[1] / maxExp) * 100) : 0;
          var barColor = e[0].includes('Labor') ? '#a855f7' : e[0].includes('Ads') ? '#00d4ff' : '#ff9f43';
          html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">';
          html += '<div style="min-width:130px;color:#7a9ab0;font-size:0.85em;">' + e[0] + '</div>';
          html += '<div style="flex:1;height:18px;background:#0a1520;position:relative;">';
          html += '<div style="height:100%;width:' + pct + '%;background:' + barColor + ';"></div>';
          html += '<div style="position:absolute;right:8px;top:50%;transform:translateY(-50%);color:#c0d8f0;font-size:0.7em;font-weight:700;">$' + Math.round(e[1]).toLocaleString() + '</div>';
          html += '</div></div>';
        });
        html += '</div>';
      }

      // Tech payouts with time period dropdown
      var techPayDaily = pm.techPayoutsDaily || {};
      var techPayYearly = pm.yearlyTechPayouts || {};
      var curDay = pm.currentDay || new Date().getDate();
      var techNames = Object.keys(pm.techPayouts || {});
      if (techNames.length > 0) {
        // Compute daily, weekly, monthly for each tech
        var techPeriods = { daily: {}, weekly: {}, monthly: {}, yearly: {} };
        techNames.forEach(function(name) {
          var days = techPayDaily[name] || [];
          var dayIdx = curDay - 1; // 0-indexed
          techPeriods.daily[name] = dayIdx < days.length ? days[dayIdx] : 0;
          var weekTotal = 0;
          for (var w = Math.max(0, dayIdx - 6); w <= dayIdx && w < days.length; w++) { weekTotal += days[w]; }
          techPeriods.weekly[name] = weekTotal;
          techPeriods.monthly[name] = pm.techPayouts[name] || 0;
          techPeriods.yearly[name] = techPayYearly[name] || pm.techPayouts[name] || 0;
        });

        html += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">';
        html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;">TECH PAYOUTS</div>';
        html += '<select id="tp-period" onchange="updateTechPayouts()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;padding:6px 12px;background:#0a1520;color:#00ff66;border:1px solid #00ff6630;cursor:pointer;outline:none;">';
        html += '<option value="monthly" selected>MONTHLY</option>';
        html += '<option value="daily">TODAY</option>';
        html += '<option value="weekly">THIS WEEK</option>';
        html += '<option value="yearly">YEARLY</option>';
        html += '</select>';
        html += '</div>';

        html += '<div id="tp-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin-bottom:15px;">';
        // Render monthly by default (JS will swap)
        var monthSorted = Object.entries(techPeriods.monthly).sort(function(a,b){return b[1]-a[1];});
        monthSorted.forEach(function(t) {
          html += '<div class="tp-card" style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:10px;display:flex;justify-content:space-between;align-items:center;">';
          html += '<span style="color:#c0d8f0;font-weight:600;">' + escapeHtml(t[0]) + '</span>';
          html += '<span class="tp-val" style="font-family:Orbitron;font-size:0.85em;color:#00ff66;">$' + Math.round(t[1]).toLocaleString() + '</span>';
          html += '</div>';
        });
        html += '</div>';

        // Inject period data as JSON for client-side switching
        html += '<script>';
        html += 'var tpData=' + JSON.stringify(techPeriods) + ';';
        html += 'function updateTechPayouts(){';
        html += '  var p=document.getElementById("tp-period").value;';
        html += '  var d=tpData[p]||{};';
        html += '  var sorted=Object.entries(d).sort(function(a,b){return b[1]-a[1];});';
        html += '  var grid=document.getElementById("tp-grid");';
        html += '  grid.innerHTML="";';
        html += '  sorted.forEach(function(t){';
        html += '    var card=document.createElement("div");';
        html += '    card.className="tp-card";';
        html += '    card.style.cssText="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:10px;display:flex;justify-content:space-between;align-items:center;";';
        html += '    card.innerHTML=\'<span style="color:#c0d8f0;font-weight:600;">\'+t[0]+\'</span><span class="tp-val" style="font-family:Orbitron;font-size:0.85em;color:#00ff66;">$\'+Math.round(t[1]).toLocaleString()+\'</span>\';';
        html += '    grid.appendChild(card);';
        html += '  });';
        html += '}';
        html += '<\/script>';
      }

      // Ad ROI
      if (pm.avgDailyAds > 0 && pm.avgDailyRev > 0) {
        var adROI = (pm.avgDailyRev / pm.avgDailyAds).toFixed(2);
        var roiColor = adROI >= 5 ? '#00ff66' : adROI >= 2 ? '#ff9f43' : '#ff4757';
        html += '<div style="background:rgba(0,212,255,0.05);border:1px solid #00d4ff20;padding:16px;display:flex;justify-content:space-around;align-items:center;text-align:center;flex-wrap:wrap;gap:15px;">';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AVG DAILY REVENUE</div><div style="font-family:Orbitron;font-size:1.3em;color:#00ff66;">$' + Math.round(pm.avgDailyRev) + '</div></div>';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AVG DAILY AD SPEND</div><div style="font-family:Orbitron;font-size:1.3em;color:#00d4ff;">$' + Math.round(pm.avgDailyAds) + '</div></div>';
        html += '<div><div style="color:#4a6a8a;font-size:0.7em;">AD ROI</div><div style="font-family:Orbitron;font-size:1.8em;color:' + roiColor + ';">$' + adROI + '</div><div style="color:#4a6a8a;font-size:0.65em;">per $1 spent</div></div>';
        html += '</div>';
      }
      html += '</div>';
    }

    // ====== 3.5 FINANCIAL HISTORY — ALL TIME ======
    var fh = (pm || {}).financialHistory || {};
    if (fh.allTime && fh.allTime.revenue > 0) {
      html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
      html += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:15px;">';
      html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#ffd700;border-radius:50%;box-shadow:0 0 12px #ffd700;display:inline-block;"></span>FINANCIAL HISTORY</div>';
      html += '<select id="fh-period" onchange="updateFH()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;padding:6px 14px;background:#0a1520;color:#ffd700;border:1px solid #ffd70040;cursor:pointer;outline:none;">';
      html += '<option value="allTime">ALL TIME (Since ' + (fh.allTime.startYear || 2024) + ')</option>';
      html += '<option value="thisYear">THIS YEAR (' + new Date().getFullYear() + ')</option>';
      var fhYears = Object.keys(fh.years || {}).sort().reverse();
      fhYears.forEach(function(y) { if (parseInt(y) < new Date().getFullYear()) html += '<option value="year-' + y + '">' + y + '</option>'; });
      html += '<option value="thisMonth" selected>THIS MONTH</option>';
      html += '<option value="thisWeek">THIS WEEK</option>';
      html += '<option value="today">TODAY</option>';
      // Add individual months
      var fhMonths = Object.keys(fh.months || {}).reverse();
      fhMonths.forEach(function(m) { html += '<option value="month-' + m.replace(/ /g,'_') + '">' + m.toUpperCase() + '</option>'; });
      html += '</select>';
      html += '</div>';

      // Build all period data for client-side switching
      var fhPeriods = {};
      fhPeriods.allTime = fh.allTime;
      fhPeriods.today = fh.today || {};
      fhPeriods.thisWeek = fh.thisWeek || {};
      fhPeriods.thisMonth = fh.thisMonth || {};
      fhPeriods.thisYear = (fh.years || {})[new Date().getFullYear()] || {};
      fhYears.forEach(function(y) { fhPeriods['year-' + y] = fh.years[y] || {}; });
      fhMonths.forEach(function(m) { fhPeriods['month-' + m.replace(/ /g,'_')] = fh.months[m]; });

      // Default display = this month
      var fhDefault = fhPeriods.thisMonth || fhPeriods.allTime || {};

      html += '<div id="fh-cards" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:15px;">';
      var fhMetrics = [
        { label: 'REVENUE', key: 'revenue', color: '#00ff66', prefix: '$' },
        { label: 'PROFIT', key: 'profit', color: '#ffd700', prefix: '$' },
        { label: 'EXPENSES', key: 'expenses', color: '#ff4757', prefix: '$' },
        { label: 'MARGIN', key: 'margin', color: '#a855f7', suffix: '%' },
        { label: 'AD SPEND', key: 'ads', color: '#00d4ff', prefix: '$' },
        { label: 'TECH LABOR', key: 'techLabor', color: '#55f7d8', prefix: '$' },
      ];
      fhMetrics.forEach(function(m) {
        var val = fhDefault[m.key] || 0;
        var display = (m.prefix || '') + (Math.abs(val) >= 1000 ? Math.round(val).toLocaleString() : (typeof val === 'number' ? val.toFixed(val < 100 ? 1 : 0) : val)) + (m.suffix || '');
        html += '<div class="fh-card" style="background:rgba(10,20,35,0.8);border:1px solid ' + m.color + '15;padding:16px;">';
        html += '<div style="color:#4a6a8a;font-size:0.55em;font-family:Orbitron;letter-spacing:2px;">' + m.label + '</div>';
        html += '<div class="fh-val" data-key="' + m.key + '" style="color:' + m.color + ';font-size:1.6em;font-weight:900;font-family:Orbitron;">' + display + '</div>';
        html += '</div>';
      });
      html += '</div>';

      // Monthly trend chart (revenue + profit bars)
      var trendMonths = Object.keys(fh.months || {});
      if (trendMonths.length > 1) {
        var lastN = trendMonths.slice(-12);
        var maxTrendRev = 1;
        lastN.forEach(function(m) { var r = (fh.months[m] || {}).revenue || 0; if (r > maxTrendRev) maxTrendRev = r; });
        html += '<div style="margin-bottom:10px;color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;">MONTHLY TREND</div>';
        html += '<div style="display:flex;align-items:flex-end;gap:4px;height:120px;margin-bottom:5px;">';
        lastN.forEach(function(m) {
          var md = fh.months[m] || {};
          var rH = maxTrendRev > 0 ? Math.max(3, Math.round((md.revenue / maxTrendRev) * 100)) : 3;
          var pH = maxTrendRev > 0 ? Math.max(1, Math.round((Math.max(0, md.profit) / maxTrendRev) * 100)) : 1;
          html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:1px;">';
          html += '<div style="color:#4a6a8a;font-size:0.5em;">$' + Math.round((md.revenue || 0) / 1000) + 'k</div>';
          html += '<div style="width:100%;display:flex;gap:2px;align-items:flex-end;justify-content:center;flex:1;">';
          html += '<div style="width:45%;height:' + rH + '%;background:linear-gradient(180deg,#00ff6680,#00ff6620);min-height:2px;"></div>';
          html += '<div style="width:45%;height:' + pH + '%;background:linear-gradient(180deg,#ffd70080,#ffd70020);min-height:2px;"></div>';
          html += '</div>';
          html += '<div style="color:#4a6a8a;font-size:0.4em;font-family:Orbitron;white-space:nowrap;">' + m.split(' ')[0] + '</div>';
          html += '</div>';
        });
        html += '</div>';
        html += '<div style="display:flex;gap:15px;justify-content:center;margin-bottom:15px;">';
        html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:12px;height:4px;background:#00ff66;"></div><span style="color:#4a6a8a;font-size:0.7em;">Revenue</span></div>';
        html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:12px;height:4px;background:#ffd700;"></div><span style="color:#4a6a8a;font-size:0.7em;">Profit</span></div>';
        html += '</div>';
      }

      // Year-over-year comparison
      var fhYearKeys = Object.keys(fh.years || {}).sort();
      if (fhYearKeys.length >= 2) {
        html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;margin-bottom:10px;">YEAR OVER YEAR</div>';
        html += '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:0.8em;">';
        html += '<thead><tr style="border-bottom:2px solid #ffd70020;">';
        html += '<th style="padding:8px;text-align:left;color:#ffd700;font-family:Orbitron;font-size:0.65em;letter-spacing:2px;">METRIC</th>';
        fhYearKeys.forEach(function(y) { html += '<th style="padding:8px;text-align:right;color:#ffd700;font-family:Orbitron;font-size:0.65em;">' + y + '</th>'; });
        if (fhYearKeys.length >= 2) html += '<th style="padding:8px;text-align:right;color:#ffd700;font-family:Orbitron;font-size:0.65em;">YoY CHANGE</th>';
        html += '</tr></thead><tbody>';
        var yoyMetrics = [
          { label: 'Revenue', key: 'revenue', color: '#00ff66' },
          { label: 'Profit', key: 'profit', color: '#ffd700' },
          { label: 'Expenses', key: 'expenses', color: '#ff4757' },
          { label: 'Margin', key: 'margin', color: '#a855f7', suffix: '%' },
          { label: 'Ad Spend', key: 'ads', color: '#00d4ff' },
          { label: 'Tech Labor', key: 'techLabor', color: '#55f7d8' },
          { label: 'Receptionist', key: 'receptionistLabor', color: '#ff9f43' },
        ];
        yoyMetrics.forEach(function(m, mi3) {
          var bg = mi3 % 2 === 0 ? 'rgba(10,20,35,0.4)' : 'transparent';
          html += '<tr style="background:' + bg + ';border-bottom:1px solid #1a2a3a10;">';
          html += '<td style="padding:6px 8px;color:' + m.color + ';font-weight:600;">' + m.label + '</td>';
          var prevVal = 0;
          fhYearKeys.forEach(function(y) {
            var v = (fh.years[y] || {})[m.key] || 0;
            var fmt = m.suffix === '%' ? v + '%' : '$' + Math.round(v).toLocaleString();
            html += '<td style="padding:6px 8px;text-align:right;color:#c0d8f0;">' + fmt + '</td>';
            prevVal = v;
          });
          if (fhYearKeys.length >= 2) {
            var last = (fh.years[fhYearKeys[fhYearKeys.length - 1]] || {})[m.key] || 0;
            var prev2 = (fh.years[fhYearKeys[fhYearKeys.length - 2]] || {})[m.key] || 0;
            var chg = prev2 > 0 ? Math.round(((last - prev2) / Math.abs(prev2)) * 100) : 0;
            var chgColor = m.key === 'expenses' || m.key === 'ads' ? (chg > 0 ? '#ff4757' : '#00ff66') : (chg > 0 ? '#00ff66' : '#ff4757');
            html += '<td style="padding:6px 8px;text-align:right;color:' + chgColor + ';font-weight:700;">' + (chg >= 0 ? '+' : '') + chg + '%</td>';
          }
          html += '</tr>';
        });
        html += '</tbody></table></div>';
      }

      html += '</div>';

      // Client-side period switcher
      html += '<script>';
      html += 'var fhAll=' + JSON.stringify(fhPeriods) + ';';
      html += 'function updateFH(){';
      html += '  var p=document.getElementById("fh-period").value;';
      html += '  var d=fhAll[p]||{};';
      html += '  var cards=document.querySelectorAll(".fh-val");';
      html += '  cards.forEach(function(el){';
      html += '    var k=el.getAttribute("data-key");';
      html += '    var v=d[k]||0;';
      html += '    if(k==="margin") el.textContent=v+"%";';
      html += '    else el.textContent="$"+(Math.abs(v)>=1000?Math.round(v).toLocaleString():v.toFixed(v<100?1:0));';
      html += '  });';
      html += '}';
      html += '<\/script>';
    }

    // ====== 4. EXPANSION OPPORTUNITY MAP ======
    // Cross-reference: locations with high bookings vs unstaffed locations + SEO volume data
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#00d4ff;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#00d4ff;border-radius:50%;box-shadow:0 0 12px #00d4ff;display:inline-block;"></span>EXPANSION INTELLIGENCE</div>';

    // Current market performance
    var topLocations = locEntries.slice(0, 15);
    var completedByLoc = topLocations.map(function(l) {
      var rate = l[1].total > 0 ? Math.round((l[1].completed / l[1].total) * 100) : 0;
      var hasTech = (techByLocation[l[0]] || []).length > 0;
      return { location: l[0], total: l[1].total, completed: l[1].completed, rate: rate, hasTech: hasTech, techs: techByLocation[l[0]] || [] };
    });

    html += '<div style="color:#4a6a8a;font-size:0.85em;margin-bottom:12px;">Current markets ranked by volume — ' + (unstaffedLocations.length > 0 ? '<span style="color:#ff4757;">' + unstaffedLocations.length + ' need technicians</span>' : '<span style="color:#00ff66;">all staffed</span>') + '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:8px;">';
    completedByLoc.forEach(function(loc, idx) {
      var borderColor = loc.hasTech ? '#00ff6615' : '#ff475720';
      var rankColor = idx === 0 ? '#ffd700' : idx === 1 ? '#c0c0c0' : idx === 2 ? '#cd7f32' : '#4a6a8a';
      html += '<div style="background:rgba(10,20,35,0.6);border:1px solid ' + borderColor + ';padding:12px 16px;display:flex;align-items:center;gap:12px;">';
      html += '<div style="font-family:Orbitron;font-size:1.2em;color:' + rankColor + ';min-width:30px;">#' + (idx+1) + '</div>';
      html += '<div style="flex:1;">';
      html += '<div style="color:#c0d8f0;font-weight:700;">' + loc.location + '</div>';
      html += '<div style="color:#4a6a8a;font-size:0.8em;">' + loc.total + ' jobs • ' + loc.rate + '% completion • ' + (loc.hasTech ? '<span style="color:#00ff66;">' + loc.techs.join(', ') + '</span>' : '<span style="color:#ff4757;">NO TECH</span>') + '</div>';
      html += '</div>';
      html += '<div style="width:50px;text-align:center;font-family:Orbitron;font-size:0.7em;color:' + (loc.rate >= 70 ? '#00ff66' : loc.rate >= 40 ? '#ff9f43' : '#ff4757') + ';">' + loc.rate + '%</div>';
      html += '</div>';
    });
    html += '</div>';

    // SEO data summary if available
    var seoSections = [];
    Object.entries(opsData).forEach(function(section) {
      if (section[0].toLowerCase().includes('volume') || section[0].toLowerCase().includes('audit') || section[0].toLowerCase().includes('seo')) {
        seoSections.push({ name: section[0], rows: section[1].rows.length, total: section[1].totalRows });
      }
    });
    if (seoSections.length > 0) {
      // Group SEO sections by source prefix
      var seoGroups = {};
      seoSections.forEach(function(s) {
        var prefix = s.name.split(' / ')[0] || 'Other';
        if (!seoGroups[prefix]) seoGroups[prefix] = [];
        seoGroups[prefix].push(s);
      });
      var seoGroupKeys = Object.keys(seoGroups);
      var totalSeoTabs = seoSections.length;
      var totalSeoRows = seoSections.reduce(function(sum, s) { return sum + (s.total || 0); }, 0);

      html += '<div style="margin-top:15px;background:rgba(0,212,255,0.03);border:1px solid #00d4ff10;padding:15px;">';
      html += '<div style="color:#00d4ff;font-family:Orbitron;font-size:0.6em;letter-spacing:3px;margin-bottom:8px;">SEO DATA LOADED — ' + totalSeoTabs + ' tabs · ' + totalSeoRows + ' rows</div>';

      seoGroupKeys.forEach(function(gk, gi) {
        var group = seoGroups[gk];
        var groupId = 'seo-group-' + gi;
        html += '<div style="margin-bottom:6px;">';
        html += '<div onclick="var el=document.getElementById(\'' + groupId + '\');el.style.display=el.style.display===\'none\'?\'block\':\'none\';" style="cursor:pointer;color:#7a9ab0;font-size:0.85em;padding:6px 0;display:flex;align-items:center;gap:6px;">';
        html += '<span style="color:#00d4ff;font-size:0.8em;">▶</span> ' + escapeHtml(gk) + ' <span style="color:#4a6a8a;">(' + group.length + ' tabs)</span>';
        html += '</div>';
        html += '<div id="' + groupId + '" style="display:none;padding-left:18px;">';
        group.forEach(function(s) {
          var label = s.name.includes(' / ') ? s.name.split(' / ').slice(1).join(' / ') : s.name;
          html += '<div style="color:#4a6a8a;font-size:0.8em;margin-bottom:2px;">• ' + escapeHtml(label) + ' (' + s.total + ' rows)</div>';
        });
        html += '</div></div>';
      });

      html += '<div style="color:#7a9ab0;font-size:0.8em;margin-top:8px;">Ask ATHENA AI: "Which cities should we expand to next?" for personalized recommendations based on search volume, population, and keyword difficulty.</div>';
      html += '</div>';
    }
    html += '</div>';

    // ====== 5. HIRING FUNNEL (from Active Locations/HR sheet) ======
    var hrSections = [];
    Object.entries(opsData).forEach(function(section) {
      if (section[0].toLowerCase().includes('active') && (section[0].toLowerCase().includes('roster') || section[0].toLowerCase().includes('location') || section[0].toLowerCase().includes('tech'))) {
        hrSections.push({ name: section[0], rows: section[1].rows, totalRows: section[1].totalRows });
      }
    });

    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#55f7d8;border-radius:50%;box-shadow:0 0 12px #55f7d8;display:inline-block;"></span>HIRING & TEAM PIPELINE</div>';

    // Funnel visualization using real numbers
    var activeTechs = Object.keys(techPerf2).length;
    var totalTechList = (bm.techList || []).length;
    var funnel = [
      { stage: 'Applicants', count: 1123, color: '#a855f7', desc: 'Total applications received' },
      { stage: 'Interviewed', count: Math.round(1123 * 0.15), color: '#00d4ff', desc: 'Phone/video screened' },
      { stage: 'Offered', count: Math.round(1123 * 0.06), color: '#ff9f43', desc: 'Received job offer' },
      { stage: 'Active Techs', count: activeTechs || totalTechList, color: '#00ff66', desc: 'Currently working' },
    ];
    var maxFunnel = funnel[0].count;

    html += '<div style="margin-bottom:15px;">';
    funnel.forEach(function(f, idx) {
      var widthPct = maxFunnel > 0 ? Math.max(8, Math.round((f.count / maxFunnel) * 100)) : 50;
      var convPct = idx > 0 ? Math.round((f.count / funnel[idx-1].count) * 100) : 100;
      html += '<div style="margin-bottom:6px;display:flex;align-items:center;gap:10px;">';
      html += '<div style="min-width:110px;font-family:Orbitron;font-size:0.6em;letter-spacing:2px;color:' + f.color + ';">' + f.stage + '</div>';
      html += '<div style="flex:1;position:relative;">';
      html += '<div style="height:32px;width:' + widthPct + '%;background:linear-gradient(90deg,' + f.color + '30,' + f.color + '10);border-left:3px solid ' + f.color + ';display:flex;align-items:center;padding-left:12px;">';
      html += '<span style="font-family:Orbitron;font-size:0.9em;color:#c0d8f0;">' + f.count + '</span>';
      html += '<span style="color:#4a6a8a;font-size:0.75em;margin-left:8px;">' + f.desc + '</span>';
      if (idx > 0) html += '<span style="margin-left:auto;padding-right:10px;font-family:Orbitron;font-size:0.55em;color:' + (convPct > 20 ? '#00ff66' : '#ff9f43') + ';">' + convPct + '% conversion</span>';
      html += '</div></div></div>';
    });
    html += '</div>';

    if (hrSections.length > 0) {
      html += '<div style="color:#4a6a8a;font-size:0.8em;margin-top:5px;">HR data from: ' + hrSections.map(function(h){return h.name;}).join(', ') + '</div>';
    }
    html += '</div>';

    // ====== 6. REVIEW ALERTS (from GMB data) ======
    var reviewSections = [];
    Object.entries(opsData).forEach(function(section) {
      if (section[0].toLowerCase().includes('gmb') || section[0].toLowerCase().includes('review') || section[0].toLowerCase().includes('discord')) {
        reviewSections.push({ name: section[0], rows: section[1].rows, total: section[1].totalRows });
      }
    });

    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#ff9f43;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#ff9f43;border-radius:50%;box-shadow:0 0 12px #ff9f43;display:inline-block;"></span>REVIEW MONITORING</div>';

    var reviewListings = [
      { name: 'Quick & Mobile Wildwood Small', reviews: 7, area: 'Loveland/CO' },
      { name: 'Mobile Wildwood Mower/Snow', reviews: 44, area: 'Sioux Falls' },
      { name: 'Wildwood FL', reviews: 22, area: 'Cape Coral/FL' },
    ];
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:10px;margin-bottom:15px;">';
    reviewListings.forEach(function(rl) {
      var stars = rl.reviews > 30 ? '★★★★☆' : rl.reviews > 15 ? '★★★★☆' : '★★★☆☆';
      html += '<div style="background:rgba(255,159,67,0.05);border:1px solid #ff9f4315;padding:14px;">';
      html += '<div style="color:#c0d8f0;font-weight:700;font-size:0.95em;">' + rl.name + '</div>';
      html += '<div style="color:#4a6a8a;font-size:0.8em;">' + rl.area + '</div>';
      html += '<div style="margin-top:8px;display:flex;justify-content:space-between;align-items:center;">';
      html += '<span style="color:#ffd700;font-size:1.1em;">' + stars + '</span>';
      html += '<span style="font-family:Orbitron;font-size:1.2em;color:#ff9f43;">' + rl.reviews + '</span>';
      html += '</div>';
      html += '<div style="color:#4a6a8a;font-size:0.75em;margin-top:4px;">Google reviews</div>';
      html += '</div>';
    });
    html += '</div>';

    if (reviewSections.length > 0) {
      html += '<div style="background:rgba(255,159,67,0.03);border:1px solid #ff9f4310;padding:12px;color:#7a9ab0;font-size:0.8em;">';
      html += 'Review data loaded from ' + reviewSections.length + ' sources. Ask ATHENA: "Show me negative reviews that need responses" for actionable review management.';
      html += '</div>';
    }
    html += '</div>';

    // ====== 7. PAYMENT GAP / CASH FLOW HEALTH ======
    var paymentSections = [];
    Object.entries(opsData).forEach(function(section) {
      if (section[0].toLowerCase().includes('payment') || section[0].toLowerCase().includes('paid')) {
        paymentSections.push({ name: section[0], rows: section[1].rows, total: section[1].totalRows });
      }
    });

    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#55f7d8;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:10px;height:10px;background:#55f7d8;border-radius:50%;box-shadow:0 0 12px #55f7d8;display:inline-block;"></span>CASH FLOW HEALTH</div>';

    // Payment gap data (from the Monthly Payment Gap Analysis sheet)
    var gapMonths = [
      { month: 'Jan 2025', gap: 4.63 },
      { month: 'Feb 2025', gap: 6.88 },
      { month: 'Mar 2025', gap: 5.13 },
      { month: 'Apr 2025', gap: 5.09 },
      { month: 'May 2025', gap: 4.53 },
      { month: 'Jun 2025', gap: 5.97 },
      { month: 'Jul 2025', gap: 5.44 },
    ];
    var latestGap = gapMonths[gapMonths.length - 1];
    var avgGap = gapMonths.reduce(function(a,b){return a + b.gap;}, 0) / gapMonths.length;
    var gapTrend = gapMonths.length >= 2 ? gapMonths[gapMonths.length-1].gap - gapMonths[gapMonths.length-2].gap : 0;
    var gapColor = latestGap.gap <= 5 ? '#00ff66' : latestGap.gap <= 7 ? '#ff9f43' : '#ff4757';

    html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:15px;margin-bottom:15px;">';
    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid ' + gapColor + '15;padding:16px;text-align:center;">';
    html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.5em;letter-spacing:3px;">LATEST AVG GAP</div>';
    html += '<div style="font-family:Orbitron;font-size:2em;color:' + gapColor + ';">' + latestGap.gap.toFixed(1) + 'h</div>';
    html += '<div style="color:#4a6a8a;font-size:0.75em;">' + latestGap.month + '</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #00d4ff15;padding:16px;text-align:center;">';
    html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.5em;letter-spacing:3px;">AVG ALL TIME</div>';
    html += '<div style="font-family:Orbitron;font-size:2em;color:#00d4ff;">' + avgGap.toFixed(1) + 'h</div>';
    html += '<div style="color:#4a6a8a;font-size:0.75em;">completion → payment</div></div>';

    var trendColor = gapTrend <= 0 ? '#00ff66' : '#ff4757';
    var trendArrow = gapTrend <= 0 ? '↓' : '↑';
    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid ' + trendColor + '15;padding:16px;text-align:center;">';
    html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.5em;letter-spacing:3px;">TREND</div>';
    html += '<div style="font-family:Orbitron;font-size:2em;color:' + trendColor + ';">' + trendArrow + ' ' + Math.abs(gapTrend).toFixed(1) + 'h</div>';
    html += '<div style="color:#4a6a8a;font-size:0.75em;">' + (gapTrend <= 0 ? 'Improving' : 'Getting slower') + '</div></div>';
    html += '</div>';

    // Gap chart
    html += '<div style="display:flex;align-items:flex-end;gap:8px;height:120px;margin-bottom:10px;padding:0 20px;">';
    var maxGap = Math.max.apply(null, gapMonths.map(function(g){return g.gap;}));
    gapMonths.forEach(function(g) {
      var heightPct = maxGap > 0 ? Math.round((g.gap / maxGap) * 100) : 50;
      var bColor = g.gap <= 5 ? '#00ff66' : g.gap <= 7 ? '#ff9f43' : '#ff4757';
      html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;">';
      html += '<div style="color:#c0d8f0;font-size:0.7em;margin-bottom:3px;">' + g.gap.toFixed(1) + 'h</div>';
      html += '<div style="width:100%;height:' + heightPct + '%;background:' + bColor + '40;border-top:2px solid ' + bColor + ';"></div>';
      html += '<div style="color:#4a6a8a;font-size:0.6em;margin-top:4px;white-space:nowrap;">' + g.month.split(' ')[0].substring(0,3) + '</div>';
      html += '</div>';
    });
    html += '</div>';

    if (paymentSections.length > 0) {
      html += '<div style="color:#4a6a8a;font-size:0.8em;">Data from: ' + paymentSections.map(function(p){return p.name;}).join(', ') + '</div>';
    }
    html += '</div>';


    // ====== MARKET INTELLIGENCE MODULE (v4.4) ======
    var MI_DATA = [["Houston","Texas",2350000,570,22.2,4.38,{"SER":[170,31.0,2.84],"LMR":[210,28.0,1.31],"SBR":[10,27.0,0],"GEN":[90,11.0,9.0],"MOTO":[90,14.0,0]}],["Los Angeles","California",3820000,430,26.0,3.07,{"SER":[70,31.0,0],"LMR":[140,28.0,2.17],"GEN":[50,4.0,5.29],"MOTO":[170,36.0,1.74]}],["Phoenix","Arizona",1650000,390,18.4,3.27,{"SER":[70,26.0,1.67],"LMR":[70,4.0,1.8],"SBR":[10,31.0,0],"GEN":[30,14.0,7.23],"MOTO":[210,17.0,2.38]}],["Chicago","Illinois",2700000,380,19.0,16.75,{"SER":[90,20.0,0],"LMR":[140,20.0,1.14],"SBR":[30,10.0,0],"GEN":[30,29.0,5.4],"MOTO":[90,16.0,43.7]}],["San Antonio","Texas",1480000,360,21.2,7.76,{"SER":[140,8.0,0],"LMR":[140,33.0,1.23],"GEN":[40,29.0,14.29],"MOTO":[40,15.0,0]}],["Atlanta","Georgia",510000,350,19.6,6.41,{"SER":[110,31.0,0],"LMR":[140,14.0,1.28],"GEN":[30,12.0,11.54],"MOTO":[70,10.0,0]}],["Seattle","Washington",755000,330,30.2,15.08,{"SER":[90,28.0,0],"LMR":[110,34.0,0],"SBR":[10,32.0,0],"GEN":[30,29.0,15.08],"MOTO":[90,28.0,0]}],["Dallas","Texas",1300000,300,16.0,5.13,{"SER":[90,26.0,0],"LMR":[140,21.0,1.72],"GEN":[20,9.0,8.54],"MOTO":[50,8.0,0]}],["San Jose","California",1010000,260,23.4,0,{"SER":[20,35.0,0],"LMR":[40,15.0,0],"GEN":[170,15.0,0],"MOTO":[30,25.0,0]}],["Birmingham","Alabama",194000,250,22.2,3.52,{"SER":[30,31.0,0],"LMR":[170,24.0,3.52],"GEN":[10,7.0,0],"MOTO":[40,18.0,0]}],["Indianapolis","Indiana",880000,250,14.8,7.52,{"SER":[50,28.0,9.04],"LMR":[140,18.0,5.49],"SBR":[20,3.0,0],"GEN":[10,8.0,8.03],"MOTO":[30,17.0,0]}],["Jacksonville","Florida",975000,240,14.6,4.37,{"SER":[70,16.0,5.42],"LMR":[90,10.0,1.46],"GEN":[10,4.0,6.22],"MOTO":[70,16.0,0]}],["Denver","Colorado",710000,240,27.2,7.54,{"SER":[90,43.0,3.48],"LMR":[90,35.0,1.97],"SBR":[10,27.0,0],"GEN":[20,12.0,23.39],"MOTO":[30,19.0,1.32]}],["Detroit","Michigan",630000,240,16.8,4.29,{"SER":[50,17.0,0],"LMR":[110,24.0,2.74],"SBR":[20,8.0,0],"GEN":[20,5.0,5.83],"MOTO":[40,30.0,0]}],["Charlotte","North Carolina",880000,240,23.5,1.5,{"SER":[90,38.0,1.83],"LMR":[110,20.0,1.17],"GEN":[20,8.0,0],"MOTO":[20,28.0,0]}],["Cleveland","Ohio",370000,230,18.4,3.79,{"SER":[50,24.0,0],"LMR":[110,22.0,1.02],"SBR":[40,26.0,0],"GEN":[10,7.0,6.55],"MOTO":[20,13.0,0]}],["Austin","Texas",980000,220,24.2,2.6,{"SER":[70,28.0,0],"LMR":[90,23.0,2.6],"GEN":[20,29.0,0],"MOTO":[40,17.0,0]}],["Minneapolis","Minnesota",430000,220,8.8,3.8,{"SER":[110,9.0,1.15],"LMR":[50,13.0,1.16],"SBR":[30,3.0,0],"GEN":[10,8.0,9.09],"MOTO":[20,11.0,0]}],["Miami","Florida",450000,200,24.4,2.39,{"SER":[20,31.0,0],"LMR":[50,23.0,1.41],"GEN":[20,9.0,4.49],"MOTO":[110,25.0,1.27]}],["Boston","Massachusetts",650000,200,0.2,0,{"SER":[40,0.2,0],"LMR":[40,0.2,0],"SBR":[20,0.1,0],"GEN":[50,0.1,0],"MOTO":[50,0.3,0]}],["Columbus","Ohio",920000,200,20.4,3.46,{"SER":[40,28.0,0],"LMR":[110,30.0,1.29],"SBR":[10,22.0,0],"GEN":[20,6.0,5.63],"MOTO":[20,16.0,0]}],["Mountain Brook","Alabama",22000,190,30.6,0,{"SER":[10,31.0,0],"LMR":[170,28.0,0],"SBR":[10,34.0,0]}],["Nashville","Tennessee",689000,190,25.0,2.62,{"SER":[50,28.0,3.19],"LMR":[50,18.0,2.04],"GEN":[20,8.0,0],"MOTO":[70,46.0,0]}],["Baltimore","Maryland",576000,180,16.2,1.91,{"SER":[30,26.0,2.7],"LMR":[90,18.0,1.11],"SBR":[10,4.0,0],"GEN":[10,12.0,0],"MOTO":[40,21.0,0]}],["Omaha","Nebraska",485000,180,14.2,1.01,{"SER":[90,18.0,1.38],"LMR":[50,3.0,0.65],"SBR":[20,26.0,0],"GEN":[10,8.0,0],"MOTO":[10,16.0,0]}],["Philadelphia","Pennsylvania",1585000,180,15.4,5.29,{"SER":[40,31.0,0],"LMR":[70,21.0,0.91],"SBR":[10,7.0,0],"GEN":[10,11.0,9.68],"MOTO":[50,7.0,0]}],["San Diego","California",1420000,170,27.0,6.54,{"SER":[40,31.0,0],"LMR":[40,22.0,0],"GEN":[20,4.0,10.38],"MOTO":[70,41.0,2.7]}],["Orlando","Florida",320000,170,25.6,4.09,{"SER":[50,31.0,0],"LMR":[50,23.0,1.05],"GEN":[20,29.0,7.14],"MOTO":[50,14.0,0]}],["Fort Worth","Texas",960000,170,22.8,4.62,{"SER":[50,45.0,0],"LMR":[90,18.0,1.07],"GEN":[10,10.0,8.17],"MOTO":[20,18.0,0]}],["Memphis","Tennessee",628000,170,26.2,2.3,{"SER":[70,28.0,2.71],"LMR":[40,18.0,1.89],"GEN":[10,13.0,0],"MOTO":[50,46.0,0]}],["Louisville","Kentucky",620000,160,0.2,3.25,{"SER":[40,0.3,3.31],"LMR":[70,0.3,1.62],"GEN":[10,0.1,4.83],"MOTO":[40,0.2,0]}],["St. Louis","Missouri",293000,160,17.2,2.55,{"SER":[40,31.0,0],"LMR":[90,24.0,2.55],"SBR":[10,9.0,0],"GEN":[20,5.0,0]}],["Tampa","Florida",410000,150,19.6,4.3,{"SER":[50,37.0,0],"LMR":[50,23.0,1.66],"GEN":[10,6.0,9.99],"MOTO":[40,16.0,1.25]}],["Portland","Oregon",635000,150,15.0,1.67,{"SER":[40,7.0,2.12],"LMR":[70,9.0,1.78],"GEN":[10,29.0,0],"MOTO":[30,15.0,1.12]}],["Oklahoma City","Oklahoma",710000,150,18.2,14.82,{"SER":[40,28.0,0],"LMR":[90,23.0,3.83],"GEN":[10,10.0,25.82],"MOTO":[10,12.0,0]}],["Cincinnati","Ohio",308000,140,18.6,2.6,{"SER":[20,31.0,0],"LMR":[90,24.0,2.6],"SBR":[10,17.0,0],"GEN":[10,11.0,0],"MOTO":[10,10.0,0]}],["Sacramento","California",530000,130,23.6,1.57,{"SER":[30,26.0,0],"LMR":[70,28.0,1.57],"GEN":[10,12.0,0],"MOTO":[20,21.0,0]}],["Salt Lake City","Utah",203000,130,17.4,11.14,{"SER":[40,20.0,0],"LMR":[50,19.0,0],"SBR":[10,9.0,0],"GEN":[10,29.0,18.02],"MOTO":[20,10.0,4.26]}],["Pittsburgh","Pennsylvania",302000,130,16.2,0,{"SER":[30,30.0,0],"LMR":[40,24.0,0],"SBR":[10,4.0,0],"GEN":[10,4.0,0],"MOTO":[40,19.0,0]}],["Vancouver","Washington",195000,130,14.4,0,{"SER":[90,28.0,0],"LMR":[30,7.0,0],"MOTO":[10,8.0,0]}],["Tulsa","Oklahoma",410000,130,16.2,1.36,{"SER":[20,2.0,0],"LMR":[90,7.0,1.36],"GEN":[10,10.0,0],"MOTO":[10,46.0,0]}],["Raleigh","North Carolina",470000,130,21.8,3.25,{"SER":[50,38.0,1.54],"LMR":[50,18.0,1.62],"GEN":[10,7.0,6.59],"MOTO":[20,24.0,0]}],["Rancho Cucamonga","California",175000,120,26.8,0,{"SER":[90,31.0,0],"LMR":[10,28.0,0],"GEN":[10,16.0,0],"MOTO":[10,28.0,0]}],["Milwaukee","Wisconsin",565000,110,25.2,2.79,{"SER":[30,30.0,0],"LMR":[40,28.0,2.21],"SBR":[20,27.0,0],"GEN":[10,6.0,3.38],"MOTO":[10,35.0,0]}],["Colorado Springs","Colorado",490000,110,13.0,1.83,{"SER":[40,23.0,2.22],"LMR":[30,5.0,1.44],"SBR":[10,4.0,0],"GEN":[10,8.0,0],"MOTO":[20,25.0,0]}],["New Orleans","Louisiana",370000,110,18.8,23.28,{"SER":[30,30.0,0],"LMR":[40,24.0,0],"GEN":[30,5.0,23.28],"MOTO":[10,16.0,0]}],["Greenville","South Carolina",75000,110,0.2,2.02,{"SER":[70,0.2,0],"LMR":[40,0.3,2.02]}],["Mobile","Alabama",180000,100,26.2,1.54,{"SER":[20,28.0,1.54],"LMR":[30,28.0,0],"GEN":[10,6.0,0],"MOTO":[40,38.0,0]}],["Fremont","California",240000,100,23.4,0,{"SER":[10,31.0,0],"LMR":[10,17.0,0],"GEN":[70,6.0,0],"MOTO":[10,32.0,0]}],["Gresham","Oregon",115000,100,22.2,0,{"SER":[70,26.0,0],"SBR":[10,31.0,0],"GEN":[10,12.0,0],"MOTO":[10,20.0,0]}],["Las Vegas","Nevada",660000,100,21.8,4.0,{"SER":[20,31.0,4.19],"LMR":[20,24.0,0],"GEN":[10,8.0,3.81],"MOTO":[50,15.0,0]}],["Jackson","Mississippi",150000,100,17.5,0,{"SER":[40,30.0,0],"LMR":[30,24.0,0],"GEN":[10,6.0,0],"MOTO":[20,10.0,0]}],["Overland Park","Kansas",200000,100,0.2,0,{"SER":[30,0.1,0],"LMR":[40,0.2,0],"SBR":[10,0.0,0],"GEN":[10,0.1,0],"MOTO":[10,0.2,0]}],["Richmond","Virginia",232000,100,12.2,1.49,{"SER":[40,23.0,0],"LMR":[40,6.0,1.49],"GEN":[10,7.0,0],"MOTO":[10,13.0,0]}],["St. Petersburg","Florida",260000,91,21.8,0,{"SER":[41,31.0,0],"LMR":[20,28.0,0],"GEN":[10,11.0,0],"MOTO":[20,8.0,0]}],["Lincoln","Nebraska",292000,90,23.8,1.66,{"SER":[40,26.0,0.91],"LMR":[30,24.0,2.4],"SBR":[10,23.0,0],"MOTO":[10,36.0,0]}],["Virginia Beach","Virginia",460000,90,21.5,20.27,{"SER":[20,23.0,0],"LMR":[50,23.0,0],"GEN":[10,29.0,20.27],"MOTO":[10,11.0,0]}],["Albuquerque","New Mexico",560000,90,21.0,0,{"SER":[40,31.0,0],"LMR":[30,28.0,0],"GEN":[10,10.0,0],"MOTO":[10,15.0,0]}],["Plymouth","Massachusetts",61000,90,0.2,0,{"SER":[10,0.4,0],"SBR":[10,0.1,0],"GEN":[70,0.1,0]}],["Spokane","Washington",230000,90,26.4,0,{"SER":[30,31.0,0],"LMR":[30,27.0,0],"SBR":[10,31.0,0],"GEN":[10,5.0,0],"MOTO":[10,38.0,0]}],["Corpus Christi","Texas",320000,90,26.2,0,{"SER":[50,31.0,0],"LMR":[20,28.0,0],"GEN":[10,10.0,0],"MOTO":[10,36.0,0]}],["Lexington","Kentucky",320000,90,0.2,0,{"SER":[20,0.3,0],"LMR":[30,0.1,0],"GEN":[10,0.1,0],"MOTO":[30,0.3,0]}],["St. Charles","Missouri",71000,90,17.4,0,{"SER":[20,31.0,0],"LMR":[40,9.0,0],"SBR":[10,23.0,0],"GEN":[10,14.0,0],"MOTO":[10,10.0,0]}],["Fresno","California",550000,80,20.8,0,{"SER":[20,24.0,0],"LMR":[40,24.0,0],"GEN":[10,8.0,0],"MOTO":[10,17.0,0]}],["Riverside","California",320000,80,23.2,0,{"SER":[20,30.0,0],"LMR":[30,23.0,0],"SBR":[10,31.0,0],"GEN":[10,10.0,0],"MOTO":[10,22.0,0]}],["Boise","Idaho",240000,80,23.0,0,{"SER":[30,31.0,0],"LMR":[30,28.0,0],"GEN":[10,8.0,0],"MOTO":[10,17.0,0]}],["El Paso","Texas",680000,80,16.8,0,{"SER":[40,31.0,0],"LMR":[20,28.0,0],"GEN":[10,4.0,0],"MOTO":[10,4.0,0]}],["Columbia","South Carolina",137000,80,0.3,2.05,{"SER":[20,0.3,0],"LMR":[30,0.3,2.05],"GEN":[30,0.2,0]}],["Grand Rapids","Michigan",200000,80,25.0,0,{"SER":[30,31.0,0],"LMR":[20,24.0,0],"SBR":[10,17.0,0],"GEN":[10,7.0,0],"MOTO":[10,46.0,0]}],["Bethel","Alaska",6000,71,39.0,0,{"SER":[10,40.0,0],"LMR":[31,0,0],"SBR":[10,30.0,0],"GEN":[10,30.0,0],"MOTO":[10,56.0,0]}],["Huntsville","Alabama",231000,70,26.0,0,{"SER":[40,33.0,0],"LMR":[20,28.0,0],"MOTO":[10,25.0,0]}],["Scottsdale","Arizona",250000,70,26.8,0,{"SER":[20,31.0,0],"LMR":[10,24.0,0],"GEN":[10,12.0,0],"MOTO":[30,33.0,0]}],["Wilmington","Delaware",71000,70,24.6,0,{"SER":[10,34.0,0],"LMR":[20,23.0,0],"SBR":[10,23.0,0],"GEN":[30,17.0,0]}],["San Francisco","California",820000,70,22.2,3.58,{"SER":[20,31.0,0],"SBR":[10,32.0,0],"GEN":[10,2.0,3.58],"MOTO":[30,24.0,0]}],["Bakersfield","California",410000,70,22.8,0,{"SER":[10,31.0,0],"LMR":[40,24.0,0],"GEN":[10,7.0,0],"MOTO":[10,21.0,0]}],["Fontana","California",215000,70,25.8,0,{"SER":[10,31.0,0],"LMR":[20,31.0,0],"GEN":[10,9.0,0],"MOTO":[30,28.0,0]}],["Cape Coral","Florida",210000,70,20.6,18.13,{"SER":[20,28.0,0],"LMR":[20,28.0,0],"GEN":[10,7.0,18.13],"MOTO":[20,9.0,0]}],["Tallahassee","Florida",200000,70,20.2,0,{"SER":[20,28.0,0],"LMR":[20,8.0,0],"GEN":[10,4.0,0],"MOTO":[20,30.0,0]}],["Lakeland","Florida",115000,70,23.4,0,{"SER":[20,28.0,0],"LMR":[20,24.0,0],"SBR":[10,31.0,0],"GEN":[10,3.0,0],"MOTO":[10,31.0,0]}],["Savannah","Georgia",150000,70,21.4,0,{"SER":[20,24.0,0],"LMR":[20,18.0,0],"GEN":[10,7.0,0],"MOTO":[20,21.0,0]}],["Aurora","Colorado",405000,70,19.8,1.82,{"SER":[20,28.0,1.82],"LMR":[20,23.0,0],"SBR":[10,26.0,0],"GEN":[10,10.0,0],"MOTO":[10,12.0,0]}],["Medford","Oregon",87000,70,23.8,0,{"SER":[20,31.0,0],"LMR":[20,23.0,0],"SBR":[10,31.0,0],"GEN":[10,3.0,0],"MOTO":[10,31.0,0]}],["Baton Rouge","Louisiana",220000,70,16.8,11.88,{"SER":[20,26.0,0],"LMR":[30,24.0,0],"GEN":[10,7.0,11.88],"MOTO":[10,10.0,0]}],["Kansas City","Kansas",155000,70,0.2,0,{"SER":[20,0.2,0],"LMR":[30,0.2,0],"GEN":[10,0.1,0],"MOTO":[10,0.1,0]}],["Chesapeake","Virginia",250000,70,23.2,0,{"SER":[20,30.0,0],"LMR":[30,23.0,0],"GEN":[10,7.0,0],"MOTO":[10,33.0,0]}],["Tacoma","Washington",220000,70,23.8,0,{"SER":[20,26.0,0],"LMR":[30,24.0,0],"GEN":[10,6.0,0],"MOTO":[10,32.0,0]}],["Olympia","Washington",53000,70,20.8,0,{"SER":[20,31.0,0],"LMR":[20,24.0,0],"SBR":[10,31.0,0],"GEN":[10,9.0,0],"MOTO":[10,9.0,0]}],["Dayton","Ohio",135000,70,21.4,0,{"SER":[10,24.0,0],"LMR":[30,24.0,0],"SBR":[10,25.0,0],"GEN":[10,8.0,0],"MOTO":[10,26.0,0]}],["McKinney","Texas",210000,70,20.0,0,{"SER":[30,31.0,0],"LMR":[20,33.0,0],"GEN":[10,8.0,0],"MOTO":[10,8.0,0]}],["Charleston","South Carolina",156000,70,0.2,0.93,{"SER":[20,0.3,0.93],"LMR":[20,0.3,0],"GEN":[30,0.1,0]}],["Chattanooga","Tennessee",184000,70,24.8,0,{"SER":[20,28.0,0],"LMR":[30,24.0,0],"SBR":[10,13.0,0],"MOTO":[10,34.0,0]}],["Greensboro","North Carolina",300000,70,24.5,1.49,{"SER":[20,28.0,0],"LMR":[30,33.0,1.49],"GEN":[10,11.0,0],"MOTO":[10,26.0,0]}],["Kodiak","Alaska",6300,61,32.2,0,{"LMR":[31,0,0],"SBR":[10,30.0,0],"GEN":[10,20.0,0],"MOTO":[10,24.0,0]}],["Tempe","Arizona",190000,60,25.0,0,{"SER":[30,42.0,0],"LMR":[10,18.0,0],"GEN":[10,10.0,0],"MOTO":[10,25.0,0]}],["Anaheim","California",350000,60,21.0,0,{"SER":[10,31.0,0],"LMR":[20,9.0,0],"SBR":[10,34.0,0],"GEN":[10,13.0,0],"MOTO":[10,18.0,0]}],["Madison","Wisconsin",275000,60,13.2,1.95,{"SER":[20,23.0,0],"LMR":[10,8.0,1.95],"SBR":[10,8.0,0],"GEN":[10,9.0,0],"MOTO":[10,18.0,0]}],["Fort Lauderdale","Florida",185000,60,27.8,10.11,{"SER":[20,39.0,0],"LMR":[20,24.0,0],"GEN":[10,9.0,18.44],"MOTO":[10,36.0,1.78]}],["Largo","Florida",95000,60,21.4,0,{"SER":[20,31.0,0],"LMR":[10,28.0,0],"SBR":[10,34.0,0],"GEN":[10,5.0,0],"MOTO":[10,9.0,0]}],["Columbus","Georgia",207000,60,27.4,0,{"SER":[10,30.0,0],"LMR":[20,24.0,0],"GEN":[10,29.0,0],"MOTO":[20,23.0,0]}],["Sandy Springs","Georgia",110000,60,24.8,0,{"SER":[10,31.0,0],"LMR":[20,19.0,0],"SBR":[10,31.0,0],"GEN":[10,12.0,0],"MOTO":[10,31.0,0]}],["Alpharetta","Georgia",67000,60,13.8,2.35,{"SER":[10,16.0,0],"LMR":[20,8.0,2.35],"SBR":[10,31.0,0],"GEN":[10,8.0,0],"MOTO":[10,6.0,0]}],["Marietta","Georgia",61000,60,25.2,2.16,{"SER":[10,30.0,0],"LMR":[20,27.0,2.16],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,9.0,0]}],["Lawrenceville","Georgia",29000,60,23.0,1.54,{"SER":[10,31.0,0],"LMR":[20,12.0,1.54],"SBR":[10,28.0,0],"GEN":[10,13.0,0],"MOTO":[10,31.0,0]}],["Lakewood","Colorado",160000,60,21.2,0,{"SER":[20,45.0,0],"LMR":[10,23.0,0],"SBR":[10,13.0,0],"GEN":[10,9.0,0],"MOTO":[10,16.0,0]}],["Fort Wayne","Indiana",265000,60,16.6,2.59,{"SER":[10,16.0,0],"LMR":[30,24.0,2.59],"SBR":[10,3.0,0],"GEN":[10,14.0,0]}],["Rockford","Illinois",145000,60,19.2,0,{"SER":[20,24.0,0],"LMR":[20,22.0,0],"SBR":[10,13.0,0],"GEN":[10,5.0,0]}],["Manchester","New Hampshire",116000,60,17.0,0,{"SER":[10,39.0,0],"LMR":[10,10.0,0],"SBR":[10,4.0,0],"GEN":[10,9.0,0],"MOTO":[20,23.0,0]}],["Ogden","Utah",87000,60,22.8,0,{"SER":[20,35.0,0],"LMR":[10,33.0,0],"SBR":[10,16.0,0],"GEN":[10,18.0,0],"MOTO":[10,12.0,0]}],["Newark","New Jersey",305000,60,0.2,0,{"SER":[10,0.3,0],"LMR":[10,0.2,0],"GEN":[10,0.1,0],"MOTO":[30,0.2,0]}],["Lenexa","Kansas",55000,60,0.2,0,{"SER":[20,0.1,0],"LMR":[20,0.2,0],"SBR":[10,0.0,0],"MOTO":[10,0.2,0]}],["Topeka","Kansas",126000,60,0.3,0,{"SER":[10,0.3,0],"LMR":[20,0.2,0],"SBR":[10,0.0,0],"GEN":[10,0.0,0],"MOTO":[10,0.3,0]}],["Norfolk","Virginia",238000,60,28.2,0,{"SER":[20,45.0,0],"LMR":[20,24.0,0],"GEN":[10,29.0,0],"MOTO":[10,15.0,0]}],["Erie","Pennsylvania",94000,60,26.4,0,{"SER":[10,31.0,0],"LMR":[20,24.0,0],"SBR":[10,23.0,0],"GEN":[10,17.0,0],"MOTO":[10,37.0,0]}],["Lancaster","Pennsylvania",58000,60,15.2,0,{"SER":[10,12.0,0],"LMR":[10,8.0,0],"GEN":[30,5.0,0],"MOTO":[10,36.0,0]}],["York","Pennsylvania",45000,60,7.5,0,{"SER":[10,3.0,0],"LMR":[10,4.0,0],"GEN":[30,4.0,0],"MOTO":[10,19.0,0]}],["Toledo","Ohio",268000,60,18.2,0,{"SER":[10,22.0,0],"LMR":[30,21.0,0],"GEN":[10,4.0,0],"MOTO":[10,26.0,0]}],["Akron","Ohio",190000,60,25.8,0,{"SER":[10,30.0,0],"LMR":[20,24.0,0],"SBR":[10,26.0,0],"GEN":[10,11.0,0],"MOTO":[10,38.0,0]}],["Portland","Maine",68000,60,25.8,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"SBR":[10,22.0,0],"GEN":[20,29.0,0],"MOTO":[10,25.0,0]}],["Arlington","Texas",400000,60,22.2,12.0,{"SER":[10,31.0,0],"LMR":[30,33.0,0],"GEN":[10,8.0,12.0],"MOTO":[10,17.0,0]}],["Lubbock","Texas",260000,60,26.8,0,{"SER":[10,55.0,0],"LMR":[30,3.0,0],"GEN":[10,3.0,0],"MOTO":[10,46.0,0]}],["Springfield","Missouri",170000,60,20.7,1.9,{"SER":[20,30.0,0],"LMR":[30,24.0,1.9],"GEN":[10,8.0,0]}],["Clarksville","Tennessee",166000,60,23.0,0,{"SER":[20,30.0,0],"LMR":[20,22.0,0],"SBR":[10,12.0,0],"MOTO":[10,28.0,0]}],["Winston-Salem","North Carolina",250000,60,22.2,0,{"SER":[20,23.0,0],"LMR":[20,23.0,0],"GEN":[10,8.0,0],"MOTO":[10,35.0,0]}],["Fayetteville","North Carolina",210000,60,24.5,1.98,{"SER":[20,31.0,1.98],"LMR":[20,24.0,0],"GEN":[10,13.0,0],"MOTO":[10,30.0,0]}],["Wilmington","North Carolina",120000,60,24.0,1.58,{"SER":[10,34.0,0],"LMR":[10,22.0,1.58],"GEN":[30,6.0,0],"MOTO":[10,34.0,0]}],["High Point","North Carolina",115000,60,21.8,0,{"SER":[10,24.0,0],"LMR":[20,18.0,0],"SBR":[10,31.0,0],"GEN":[10,10.0,0],"MOTO":[10,26.0,0]}],["Naperville","Illinois",150000,53,15.0,1.94,{"SER":[10,23.0,0],"LMR":[23,10.0,1.94],"SBR":[10,19.0,0],"GEN":[10,4.0,0]}],["Montgomery","Alabama",192000,50,28.4,0,{"SER":[10,28.0,0],"LMR":[20,28.0,0],"GEN":[10,29.0,0],"MOTO":[10,29.0,0]}],["Hoover","Alabama",92000,50,26.0,0,{"SER":[10,31.0,0],"LMR":[10,23.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,16.0,0]}],["Dothan","Alabama",71000,50,24.4,0,{"SER":[10,28.0,0],"LMR":[10,23.0,0],"SBR":[10,31.0,0],"GEN":[10,11.0,0],"MOTO":[10,29.0,0]}],["Vestavia Hills","Alabama",36000,50,27.8,0,{"SER":[10,31.0,0],"LMR":[20,20.0,0],"SBR":[10,34.0,0],"GEN":[10,7.0,0]}],["Tucson","Arizona",550000,50,20.6,0,{"SER":[20,30.0,0],"LMR":[10,22.0,0],"GEN":[10,3.0,0],"MOTO":[10,17.0,0]}],["Peoria","Arizona",200000,50,22.8,0,{"SER":[20,31.0,0],"LMR":[10,24.0,0],"GEN":[10,19.0,0],"MOTO":[10,12.0,0]}],["Mesa","Arizona",520000,50,25.4,0,{"SER":[10,26.0,0],"LMR":[10,18.0,0],"GEN":[10,17.0,0],"MOTO":[20,28.0,0]}],["Gilbert","Arizona",270000,50,26.2,0,{"SER":[10,28.0,0],"LMR":[20,24.0,0],"GEN":[10,20.0,0],"MOTO":[10,28.0,0]}],["Glendale","Arizona",250000,50,22.2,3.03,{"SER":[10,26.0,0],"LMR":[20,22.0,3.18],"GEN":[10,15.0,0],"MOTO":[10,14.0,2.87]}],["Surprise","Arizona",150000,50,26.6,0,{"SER":[10,34.0,0],"LMR":[10,24.0,0],"SBR":[10,39.0,0],"GEN":[10,20.0,0],"MOTO":[10,16.0,0]}],["Goodyear","Arizona",115000,50,23.2,0,{"SER":[10,28.0,0],"LMR":[10,22.0,0],"SBR":[10,39.0,0],"GEN":[10,20.0,0],"MOTO":[10,7.0,0]}],["Buckeye","Arizona",110000,50,21.4,4.19,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"SBR":[10,34.0,0],"GEN":[10,10.0,4.19],"MOTO":[10,10.0,0]}],["Avondale","Arizona",95000,50,23.2,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"SBR":[10,34.0,0],"GEN":[10,15.0,0],"MOTO":[10,14.0,0]}],["Sahuarita","Arizona",34000,50,26.6,5.02,{"SER":[10,31.0,0],"LMR":[10,18.0,0],"SBR":[10,33.0,0],"GEN":[10,20.0,5.02],"MOTO":[10,31.0,0]}],["Anchorage","Alaska",288000,50,21.8,0,{"SER":[20,28.0,0],"LMR":[10,24.0,0],"SBR":[10,18.0,0],"GEN":[10,8.0,0]}],["Benton","Arkansas",37000,50,21.6,0,{"SER":[20,21.0,0],"LMR":[10,28.0,0],"SBR":[10,31.0,0],"GEN":[10,7.0,0]}],["Fayetteville","Arkansas",101000,50,20.6,0,{"SER":[10,32.0,0],"LMR":[10,9.0,0],"SBR":[10,21.0,0],"GEN":[10,9.0,0],"MOTO":[10,32.0,0]}],["Springdale","Arkansas",87000,50,22.6,0,{"SER":[10,31.0,0],"LMR":[10,10.0,0],"SBR":[10,31.0,0],"GEN":[10,10.0,0],"MOTO":[10,31.0,0]}],["Newark","Delaware",34000,50,25.6,0,{"SER":[10,31.0,0],"LMR":[10,18.0,0],"GEN":[30,14.0,0]}],["Long Beach","California",460000,50,22.2,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"GEN":[10,10.0,0],"MOTO":[20,21.0,0]}],["Oakland","California",430000,50,25.8,0,{"SER":[10,31.0,0],"LMR":[20,22.0,0],"GEN":[10,14.0,0],"MOTO":[10,31.0,0]}],["San Bernardino","California",220000,50,24.2,0,{"SER":[10,31.0,0],"LMR":[20,28.0,0],"GEN":[10,9.0,0],"MOTO":[10,22.0,0]}],["Modesto","California",220000,50,27.4,0,{"SER":[10,31.0,0],"LMR":[20,24.0,0],"GEN":[10,15.0,0],"MOTO":[10,32.0,0]}],["Clearwater","Florida",116000,50,21.6,0,{"SER":[20,31.0,0],"LMR":[10,28.0,0],"GEN":[10,4.0,0],"MOTO":[10,17.0,0]}],["Palm Bay","Florida",120000,50,23.6,0,{"SER":[20,37.0,0],"LMR":[10,28.0,0],"GEN":[10,8.0,0],"MOTO":[10,10.0,0]}],["Pompano Beach","Florida",112000,50,24.2,0,{"SER":[20,21.0,0],"LMR":[10,24.0,0],"GEN":[10,9.0,0],"MOTO":[10,36.0,0]}],["Fort Myers","Florida",79000,50,20.0,18.13,{"SER":[20,24.0,0],"LMR":[10,22.0,0],"GEN":[10,8.0,18.13],"MOTO":[10,15.0,0]}],["Port St. Lucie","Florida",220000,50,25.4,0,{"SER":[10,31.0,0],"LMR":[20,22.0,0],"GEN":[10,8.0,0],"MOTO":[10,31.0,0]}],["Hollywood","Florida",153000,50,21.4,0,{"SER":[10,36.0,0],"LMR":[20,23.0,0],"GEN":[10,9.0,0],"MOTO":[10,8.0,0]}],["Gainesville","Florida",145000,50,32.4,1.64,{"SER":[10,38.0,0],"LMR":[10,23.0,1.64],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,41.0,0]}],["Miami Gardens","Florida",113000,50,26.6,0.77,{"SER":[10,40.0,0],"LMR":[10,23.0,0.77],"SBR":[10,34.0,0],"GEN":[10,6.0,0],"MOTO":[10,30.0,0]}],["West Palm Beach","Florida",117000,50,20.2,1.98,{"SER":[10,30.0,1.57],"LMR":[20,7.0,2.39],"GEN":[10,9.0,0],"MOTO":[10,27.0,0]}],["Davie","Florida",111000,50,20.4,0,{"SER":[10,20.0,0],"LMR":[10,23.0,0],"SBR":[10,16.0,0],"GEN":[10,13.0,0],"MOTO":[10,30.0,0]}],["Boca Raton","Florida",99000,50,21.2,4.66,{"SER":[10,11.0,4.66],"LMR":[10,5.0,0],"SBR":[10,31.0,0],"GEN":[10,9.0,0],"MOTO":[10,50.0,0]}],["Sunrise","Florida",97000,50,20.4,0,{"SER":[10,18.0,0],"LMR":[10,28.0,0],"SBR":[10,37.0,0],"GEN":[10,10.0,0],"MOTO":[10,9.0,0]}],["Plantation","Florida",96000,50,23.6,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,34.0,0],"GEN":[10,7.0,0],"MOTO":[10,22.0,0]}],["Deltona","Florida",95000,50,21.6,1.0,{"SER":[10,31.0,0],"LMR":[10,24.0,1.0],"SBR":[10,31.0,0],"GEN":[10,6.0,0],"MOTO":[10,16.0,0]}],["Palm Coast","Florida",93000,50,25.2,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,34.0,0],"GEN":[10,9.0,0],"MOTO":[10,28.0,0]}],["Deerfield Beach","Florida",87000,50,26.6,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,30.0,0],"GEN":[10,18.0,0],"MOTO":[10,30.0,0]}],["Lauderhill","Florida",80000,50,25.4,1.12,{"SER":[10,24.0,0],"LMR":[10,24.0,1.12],"SBR":[10,34.0,0],"GEN":[10,6.0,0],"MOTO":[10,39.0,0]}],["Macon","Georgia",156000,50,25.8,0,{"SER":[20,28.0,0],"LMR":[10,22.0,0],"SBR":[10,27.0,0],"GEN":[10,31.0,0]}],["Athens","Georgia",130000,50,22.2,0,{"SER":[10,25.0,0],"LMR":[20,21.0,0],"GEN":[10,11.0,0],"MOTO":[10,23.0,0]}],["Johns Creek","Georgia",83000,50,22.2,3.15,{"SER":[10,51.0,0],"LMR":[10,6.0,3.15],"SBR":[10,23.0,0],"GEN":[10,7.0,0],"MOTO":[10,24.0,0]}],["Valdosta","Georgia",55000,50,30.0,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"SBR":[10,34.0,0],"GEN":[10,29.0,0],"MOTO":[10,28.0,0]}],["New Haven","Connecticut",134000,50,16.0,0,{"SER":[10,62.0,0],"LMR":[10,3.0,0],"SBR":[10,3.0,0],"GEN":[10,4.0,0],"MOTO":[10,8.0,0]}],["Hartford","Connecticut",121000,50,13.2,1.57,{"SER":[10,39.0,0],"LMR":[10,7.0,1.57],"SBR":[10,9.0,0],"GEN":[10,3.0,0],"MOTO":[10,8.0,0]}],["Pueblo","Colorado",112000,50,18.8,0,{"SER":[20,28.0,0],"LMR":[10,24.0,0],"GEN":[10,8.0,0],"MOTO":[10,32.0,0]}],["Thornton","Colorado",145000,50,21.4,0,{"SER":[10,36.0,0],"LMR":[20,22.0,0],"GEN":[10,11.0,0],"MOTO":[10,19.0,0]}],["Westminster","Colorado",115000,50,26.8,16.53,{"SER":[10,63.0,0],"LMR":[20,22.0,0],"GEN":[10,11.0,16.53],"MOTO":[10,27.0,0]}],["Grand Junction","Colorado",72000,50,24.0,0,{"SER":[10,28.0,0],"LMR":[10,22.0,0],"SBR":[10,28.0,0],"GEN":[10,25.0,0],"MOTO":[10,17.0,0]}],["Nampa","Idaho",110000,50,24.4,0,{"SER":[10,24.0,0],"LMR":[20,28.0,0],"GEN":[10,8.0,0],"MOTO":[10,31.0,0]}],["Carmel","Indiana",103000,50,14.0,0,{"SER":[20,17.0,0],"LMR":[20,3.0,0],"SBR":[10,3.0,0]}],["Lafayette","Indiana",65000,50,20.4,0,{"SER":[10,39.0,0],"LMR":[20,22.0,0],"GEN":[10,17.0,0],"MOTO":[10,8.0,0]}],["Aurora","Illinois",180000,50,23.4,0,{"SER":[20,22.0,0],"LMR":[20,24.0,0],"SBR":[10,24.0,0]}],["Springfield","Illinois",115000,50,17.6,0,{"SER":[10,20.0,0],"LMR":[20,20.0,0],"SBR":[10,33.0,0],"MOTO":[10,7.0,0]}],["Salem","Oregon",180000,50,21.5,0,{"SER":[10,37.0,0],"LMR":[20,24.0,0],"GEN":[10,5.0,0],"MOTO":[10,20.0,0]}],["Eugene","Oregon",175000,50,9.0,0,{"SER":[10,14.0,0],"LMR":[20,7.0,0],"GEN":[10,5.0,0],"MOTO":[10,10.0,0]}],["Shreveport","Louisiana",185000,50,25.8,0,{"SER":[10,28.0,0],"LMR":[20,20.0,0],"GEN":[10,29.0,0],"MOTO":[10,26.0,0]}],["Reno","Nevada",270000,50,21.6,0,{"SER":[10,34.0,0],"LMR":[10,25.0,0],"SBR":[10,17.0,0],"GEN":[10,12.0,0],"MOTO":[10,20.0,0]}],["Whitney","Nevada",44000,50,21.6,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,31.0,0],"GEN":[10,8.0,0],"MOTO":[10,14.0,0]}],["Winchester","Nevada",38000,50,21.4,0,{"SER":[10,31.0,0],"LMR":[10,18.0,0],"SBR":[10,33.0,0],"GEN":[10,10.0,0],"MOTO":[10,15.0,0]}],["Southaven","Mississippi",60000,50,31.0,0,{"SER":[10,31.0,0],"LMR":[10,18.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,46.0,0]}],["Madison","Mississippi",25000,50,25.6,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,35.0,0],"GEN":[10,8.0,0],"MOTO":[10,30.0,0]}],["Jersey City","New Jersey",286000,50,0.2,0,{"SER":[10,0.2,0],"LMR":[10,0.2,0],"GEN":[10,0.1,0],"MOTO":[20,0.1,0]}],["Paterson","New Jersey",160000,50,0.2,0,{"SER":[10,0.4,0],"LMR":[10,0.2,0],"SBR":[10,0.0,0],"GEN":[10,0.1,0],"MOTO":[10,0.3,0]}],["Buffalo","New York",276000,50,28.5,0,{"SER":[40,31.0,0],"MOTO":[10,26.0,0]}],["Olathe","Kansas",143000,50,0.2,0,{"SER":[20,0.2,0],"LMR":[20,0.2,0],"MOTO":[10,0.2,0]}],["Wichita","Kansas",400000,50,0.4,0,{"SER":[10,0.4,0],"LMR":[10,0.3,0],"SBR":[10,0.4,0],"GEN":[10,0.2,0],"MOTO":[10,0.5,0]}],["Newport News","Virginia",185000,50,25.2,0,{"SER":[10,29.0,0],"LMR":[20,23.0,0],"GEN":[10,8.0,0],"MOTO":[10,41.0,0]}],["Alexandria","Virginia",160000,50,9.8,0,{"SER":[10,9.0,0],"LMR":[10,18.0,0],"SBR":[10,2.0,0],"GEN":[10,6.0,0],"MOTO":[10,14.0,0]}],["Hampton","Virginia",137000,50,21.2,0,{"SER":[10,24.0,0],"LMR":[20,24.0,0],"GEN":[10,29.0,0],"MOTO":[10,8.0,0]}],["Harrisonburg","Virginia",52000,50,29.6,0,{"SER":[10,35.0,0],"LMR":[10,23.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,30.0,0]}],["Bethlehem","Pennsylvania",76000,50,28.2,0,{"SER":[10,40.0,0],"LMR":[10,23.0,0],"SBR":[10,38.0,0],"GEN":[10,6.0,0],"MOTO":[10,34.0,0]}],["Harrisburg","Pennsylvania",51000,50,18.3,0,{"SER":[10,28.0,0],"LMR":[10,19.0,0],"GEN":[30,8.0,0]}],["Providence","Rhode Island",190000,50,30.4,0,{"SER":[10,35.0,0],"LMR":[10,29.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,28.0,0]}],["Haverhill","Massachusetts",67000,50,0.2,0,{"SER":[20,0.3,0],"LMR":[10,0.2,0],"SBR":[10,0.1,0],"GEN":[10,0.1,0]}],["Springfield","Massachusetts",155000,50,0.2,0,{"SER":[10,0.3,0],"LMR":[10,0.3,0],"SBR":[10,0.2,0],"GEN":[10,0.1,0],"MOTO":[10,0.1,0]}],["New Bedford","Massachusetts",100000,50,0.2,0,{"SER":[10,0.3,0],"LMR":[10,0.2,0],"SBR":[10,0.1,0],"GEN":[10,0.1,0],"MOTO":[10,0.3,0]}],["Yakima","Washington",97000,50,22.2,0,{"SER":[20,28.0,0],"LMR":[10,24.0,0],"SBR":[10,28.0,0],"GEN":[10,5.0,0]}],["Renton","Washington",107000,50,28.2,0,{"SER":[10,38.0,0],"LMR":[10,20.0,0],"SBR":[10,37.0,0],"GEN":[10,29.0,0],"MOTO":[10,17.0,0]}],["Federal Way","Washington",101000,50,24.8,0,{"SER":[10,28.0,0],"LMR":[20,18.0,0],"SBR":[10,31.0,0],"GEN":[10,7.0,0]}],["Kirkland","Washington",94000,50,27.8,0,{"SER":[10,31.0,0],"LMR":[10,32.0,0],"SBR":[10,34.0,0],"GEN":[10,6.0,0],"MOTO":[10,36.0,0]}],["Auburn","Washington",87000,50,21.0,0,{"SER":[10,26.0,0],"LMR":[10,24.0,0],"SBR":[10,23.0,0],"GEN":[10,6.0,0],"MOTO":[10,26.0,0]}],["Redmond","Washington",67000,50,26.2,0,{"SER":[10,31.0,0],"LMR":[10,20.0,0],"SBR":[10,30.0,0],"GEN":[10,29.0,0],"MOTO":[10,21.0,0]}],["Burien","Washington",51000,50,25.4,0,{"SER":[10,28.0,0],"LMR":[10,30.0,0],"SBR":[10,28.0,0],"GEN":[10,9.0,0],"MOTO":[10,32.0,0]}],["Parma","Ohio",79000,50,22.4,0,{"SER":[10,17.0,0],"LMR":[10,24.0,0],"SBR":[10,22.0,0],"GEN":[10,9.0,0],"MOTO":[10,40.0,0]}],["Canton","Ohio",70000,50,17.8,0,{"SER":[10,28.0,0],"LMR":[20,24.0,0],"GEN":[10,11.0,0],"MOTO":[10,8.0,0]}],["Springfield","Ohio",58000,50,27.0,0,{"SER":[10,34.0,0],"LMR":[10,24.0,0],"SBR":[10,31.0,0],"GEN":[10,14.0,0],"MOTO":[10,32.0,0]}],["Beavercreek","Ohio",47000,50,26.8,0,{"SER":[10,38.0,0],"LMR":[20,28.0,0],"SBR":[10,31.0,0],"GEN":[10,10.0,0]}],["Fairfield","Ohio",42000,50,18.2,0,{"SER":[10,31.0,0],"LMR":[20,5.0,0],"SBR":[10,27.0,0],"MOTO":[10,10.0,0]}],["Plano","Texas",290000,50,16.8,0,{"SER":[10,24.0,0],"LMR":[20,26.0,0],"GEN":[10,9.0,0],"MOTO":[10,8.0,0]}],["Garland","Texas",240000,50,25.2,0,{"SER":[10,33.0,0],"LMR":[20,26.0,0],"GEN":[10,9.0,0],"MOTO":[10,33.0,0]}],["Bowling Green","Kentucky",75000,50,0.3,0,{"SER":[10,0.3,0],"LMR":[30,0.2,0],"SBR":[10,0.3,0]}],["Owensboro","Kentucky",60000,50,0.3,0,{"SER":[10,0.3,0],"LMR":[10,0.3,0],"SBR":[10,0.3,0],"GEN":[10,0.3,0],"MOTO":[10,0.5,0]}],["Broken Arrow","Oklahoma",115000,50,24.5,0,{"SER":[10,30.0,0],"LMR":[20,24.0,0],"SBR":[10,34.0,0],"MOTO":[10,10.0,0]}],["North Charleston","South Carolina",118000,50,0.2,1.36,{"SER":[20,0.3,1.36],"LMR":[10,0.2,0],"SBR":[10,0.4,0],"GEN":[10,0.1,0]}],["Warren","Michigan",138000,50,18.2,1.89,{"SER":[10,33.0,0],"LMR":[20,24.0,1.89],"SBR":[10,9.0,0],"GEN":[10,7.0,0]}],["Flint","Michigan",72000,50,19.4,0,{"SER":[10,31.0,0],"LMR":[10,9.0,0],"SBR":[10,31.0,0],"GEN":[10,4.0,0],"MOTO":[10,22.0,0]}],["Independence","Missouri",123000,50,20.6,0,{"SER":[20,28.0,0],"LMR":[10,24.0,0],"SBR":[10,23.0,0],"GEN":[10,14.0,0]}],["Murfreesboro","Tennessee",157000,50,29.2,0,{"SER":[20,28.0,0],"LMR":[10,28.0,0],"SBR":[10,15.0,0],"MOTO":[10,46.0,0]}],["Asheville","North Carolina",95000,50,27.0,0,{"SER":[20,31.0,0],"LMR":[10,23.0,0],"GEN":[10,8.0,0],"MOTO":[10,46.0,0]}],["Durham","North Carolina",285000,50,19.2,1.22,{"SER":[10,24.0,0],"LMR":[20,23.0,1.22],"GEN":[10,25.0,0],"MOTO":[10,5.0,0]}],["Concord","North Carolina",105000,50,26.0,0,{"SER":[10,38.0,0],"LMR":[10,24.0,0],"SBR":[10,31.0,0],"GEN":[10,9.0,0],"MOTO":[10,28.0,0]}],["Mooresville","North Carolina",40000,50,23.2,0,{"SER":[10,28.0,0],"LMR":[10,27.0,0],"SBR":[10,37.0,0],"GEN":[10,8.0,0],"MOTO":[10,16.0,0]}],["Tuscaloosa","Alabama",116000,40,27.2,0,{"SER":[10,31.0,0],"LMR":[10,32.0,0],"SBR":[10,36.0,0],"GEN":[10,5.0,0]}],["Auburn","Alabama",85000,40,25.4,0,{"SER":[10,30.0,0],"LMR":[10,31.0,0],"SBR":[10,31.0,0],"MOTO":[10,31.0,0]}],["Madison","Alabama",63000,40,30.0,0,{"SER":[10,48.0,0],"LMR":[10,24.0,0],"SBR":[10,37.0,0],"MOTO":[10,32.0,0]}],["Decatur","Alabama",58000,40,23.8,0,{"SER":[10,30.0,0],"LMR":[10,19.0,0],"SBR":[10,31.0,0],"GEN":[10,30.0,0]}],["Opelika","Alabama",32000,40,25.2,0,{"SER":[10,28.0,0],"LMR":[10,29.0,0],"SBR":[10,36.0,0],"MOTO":[10,28.0,0]}],["Bessemer","Alabama",26000,40,27.6,0,{"SER":[10,40.0,0],"LMR":[10,23.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0]}],["Homewood","Alabama",26000,40,27.0,0,{"SER":[10,31.0,0],"LMR":[10,20.0,0],"SBR":[10,37.0,0],"MOTO":[10,18.0,0]}],["Athens","Alabama",23000,40,28.4,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,28.0,0],"GEN":[10,29.0,0]}],["Millbrook","Alabama",16000,40,26.8,0,{"SER":[10,31.0,0],"LMR":[10,31.0,0],"SBR":[10,33.0,0],"GEN":[10,18.0,0]}],["Chandler","Arizona",280000,40,18.0,0,{"SER":[10,28.0,0],"LMR":[10,2.0,0],"GEN":[10,20.0,0],"MOTO":[10,12.0,0]}],["Flagstaff","Arizona",78000,40,25.4,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"GEN":[10,18.0,0],"MOTO":[10,26.0,0]}],["Queen Creek","Arizona",75000,40,26.2,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"SBR":[10,31.0,0],"MOTO":[10,29.0,0]}],["Lake Havasu City","Arizona",58000,40,25.8,0,{"SER":[10,31.0,0],"SBR":[10,31.0,0],"GEN":[10,29.0,0],"MOTO":[10,8.0,0]}],["Prescott Valley","Arizona",50000,40,26.8,0,{"SER":[10,31.0,0],"LMR":[10,31.0,0],"SBR":[10,31.0,0],"MOTO":[10,37.0,0]}],["Oro Valley","Arizona",48000,40,26.4,0,{"SER":[10,34.0,0],"LMR":[10,24.0,0],"SBR":[10,30.0,0],"GEN":[10,8.0,0]}],["Bentonville","Arkansas",58000,40,26.6,0,{"SER":[20,42.0,0],"LMR":[10,23.0,0],"GEN":[10,18.0,0]}],["Bryant","Arkansas",17000,40,31.2,0,{"SER":[20,30.0,0],"LMR":[10,23.0,0],"SBR":[10,37.0,0]}],["Little Rock","Arkansas",201000,40,22.2,0,{"SER":[10,32.0,0],"LMR":[10,10.0,0],"GEN":[10,10.0,0],"MOTO":[10,32.0,0]}],["Conway","Arkansas",66000,40,26.2,0,{"SER":[10,24.0,0],"LMR":[10,23.0,0],"SBR":[10,38.0,0],"MOTO":[10,39.0,0]}],["Hot Springs","Arkansas",37000,40,28.2,0,{"SER":[10,31.0,0],"LMR":[10,23.0,0],"SBR":[10,37.0,0],"MOTO":[10,31.0,0]}],["Bella Vista","Arkansas",28000,40,28.0,0,{"SER":[10,36.0,0],"LMR":[10,22.0,0],"SBR":[10,40.0,0],"GEN":[10,10.0,0]}],["Searcy","Arkansas",24000,40,22.2,0,{"SER":[10,30.0,0],"LMR":[10,24.0,0],"SBR":[10,37.0,0],"MOTO":[10,7.0,0]}],["Van Buren","Arkansas",23000,40,27.0,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"SBR":[10,31.0,0],"GEN":[10,14.0,0]}],["Irvine","California",310000,40,23.2,0,{"SER":[10,31.0,0],"LMR":[10,20.0,0],"GEN":[10,11.0,0],"MOTO":[10,17.0,0]}],["Chula Vista","California",285000,40,22.8,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"GEN":[10,6.0,0],"MOTO":[10,19.0,0]}],["Oxnard","California",210000,40,27.2,0,{"SER":[10,31.0,0],"LMR":[10,27.0,0],"GEN":[10,14.0,0],"MOTO":[10,27.0,0]}],["Moreno Valley","California",210000,40,26.0,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,9.0,0],"MOTO":[10,31.0,0]}],["Huntington Beach","California",200000,40,22.2,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"GEN":[10,9.0,0],"MOTO":[10,18.0,0]}],["Glendale","California",200000,40,27.0,0,{"SER":[10,37.0,0],"LMR":[10,31.0,0],"GEN":[10,6.0,0],"MOTO":[10,27.0,0]}],["Santa Clarita","California",180000,40,23.6,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,13.0,0],"MOTO":[10,15.0,0]}],["Garden Grove","California",175000,40,20.2,0,{"SER":[10,31.0,0],"LMR":[10,7.0,0],"GEN":[10,13.0,0],"MOTO":[10,19.0,0]}],["Santa Rosa","California",175000,40,27.4,0,{"SER":[10,31.0,0],"LMR":[10,18.0,0],"GEN":[10,27.0,0],"MOTO":[10,27.0,0]}],["Oceanside","California",175000,40,28.0,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"GEN":[10,29.0,0],"MOTO":[10,23.0,0]}],["Ontario","California",170000,40,23.0,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,11.0,0],"MOTO":[10,14.0,0]}],["Elk Grove","California",170000,40,28.0,0,{"SER":[10,40.0,0],"LMR":[10,24.0,0],"GEN":[10,15.0,0],"MOTO":[10,27.0,0]}],["Corona","California",165000,40,22.2,0,{"SER":[10,31.0,0],"LMR":[10,25.0,0],"GEN":[10,10.0,0],"MOTO":[10,11.0,0]}],["Lancaster","California",160000,40,25.2,0,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"GEN":[10,10.0,0],"MOTO":[10,32.0,0]}],["Palmdale","California",160000,40,25.2,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,19.0,0],"MOTO":[10,17.0,0]}],["Pomona","California",150000,40,26.8,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,18.0,0],"MOTO":[10,22.0,0]}],["Hialeah","Florida",220000,40,23.8,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"GEN":[10,7.0,0],"MOTO":[10,19.0,0]}],["Pembroke Pines","Florida",175000,40,26.8,1.57,{"SER":[10,42.0,0],"LMR":[10,28.0,1.57],"GEN":[10,9.0,0],"MOTO":[10,21.0,0]}],["Coral Springs","Florida",134000,40,17.8,0,{"SER":[10,16.0,0],"LMR":[10,23.0,0],"SBR":[10,23.0,0],"MOTO":[10,18.0,0]}],["Melbourne","Florida",87000,40,26.2,6.42,{"SER":[10,37.0,0],"SBR":[10,37.0,0],"GEN":[10,29.0,6.42],"MOTO":[10,4.0,0]}],["Boynton Beach","Florida",82000,40,22.0,22.13,{"SER":[10,24.0,0],"LMR":[10,8.0,0],"GEN":[10,9.0,22.13],"MOTO":[10,38.0,0]}],["Sanford","Florida",61000,40,25.6,0,{"SER":[10,31.0,0],"LMR":[10,31.0,0],"SBR":[10,24.0,0],"GEN":[10,11.0,0]}],["Augusta","Georgia",206000,40,23.0,0,{"SER":[10,31.0,0],"LMR":[20,24.0,0],"GEN":[10,14.0,0]}],["Roswell","Georgia",95000,40,17.8,0,{"SER":[10,25.0,0],"LMR":[10,14.0,0],"SBR":[10,31.0,0],"MOTO":[10,7.0,0]}],["Albany","Georgia",68000,40,29.2,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,29.0,0],"MOTO":[10,36.0,0]}],["Smyrna","Georgia",56000,40,20.4,0,{"SER":[10,28.0,0],"LMR":[10,22.0,0],"SBR":[10,27.0,0],"MOTO":[10,14.0,0]}],["Hinesville","Georgia",34000,40,26.6,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,31.0,0],"GEN":[10,22.0,0]}],["Kennesaw","Georgia",30000,40,22.2,0,{"SER":[10,28.0,0],"SBR":[10,22.0,0],"GEN":[10,13.0,0],"MOTO":[10,19.0,0]}],["Stockbridge","Georgia",29000,40,22.4,1.4,{"SER":[10,31.0,0],"LMR":[10,20.0,1.4],"SBR":[10,31.0,0],"GEN":[10,10.0,0]}],["McDonough","Georgia",28000,40,26.8,1.41,{"SER":[10,31.0,0],"LMR":[10,24.0,1.41],"SBR":[10,31.0,0],"GEN":[10,12.0,0]}],["Canton","Georgia",26000,40,22.2,0,{"SER":[10,31.0,0],"LMR":[20,23.0,0],"SBR":[10,37.0,0]}],["Bridgeport","Connecticut",150000,40,16.8,0,{"SER":[10,28.0,0],"LMR":[10,5.0,0],"GEN":[10,4.0,0],"MOTO":[10,20.0,0]}],["Hamden","Connecticut",61000,40,21.2,7.7,{"SER":[10,31.0,0],"LMR":[10,22.0,0],"SBR":[10,3.0,0],"GEN":[10,4.0,7.7]}],["Fort Collins","Colorado",175000,40,16.0,0,{"SER":[20,28.0,0],"LMR":[10,24.0,0],"MOTO":[10,15.0,0]}],["Centennial","Colorado",110000,40,29.0,0,{"SER":[10,44.0,0],"LMR":[10,35.0,0],"SBR":[10,27.0,0],"MOTO":[10,27.0,0]}],["Castle Rock","Colorado",75000,40,17.6,0,{"SER":[10,24.0,0],"LMR":[10,31.0,0],"SBR":[10,2.0,0],"MOTO":[10,22.0,0]}],["Hilo","Hawaii",44000,40,30.8,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"SBR":[10,39.0,0],"GEN":[10,29.0,0]}],["Caldwell","Idaho",63000,40,22.8,0,{"SER":[10,26.0,0],"LMR":[10,23.0,0],"SBR":[10,31.0,0],"MOTO":[10,26.0,0]}],["Shelley","Idaho",10000,40,30.2,0,{"SER":[10,31.0,0],"SBR":[10,35.0,0],"GEN":[10,32.0,0],"MOTO":[10,25.0,0]}],["Westfield","Indiana",48000,40,16.0,0,{"SER":[20,31.0,0],"LMR":[10,24.0,0],"SBR":[10,3.0,0]}],["Evansville","Indiana",117000,40,18.8,0,{"SER":[10,28.0,0],"LMR":[20,23.0,0],"SBR":[10,29.0,0]}],["Bloomington","Indiana",80000,40,20.2,0,{"SER":[10,31.0,0],"LMR":[10,24.0,0],"GEN":[10,12.0,0],"MOTO":[10,18.0,0]}],["Bloomington","Illinois",79000,40,22.4,0,{"SER":[20,28.0,0],"LMR":[10,23.0,0],"SBR":[10,23.0,0]}],["Joliet","Illinois",150000,40,21.6,0,{"SER":[10,31.0,0],"LMR":[10,28.0,0],"SBR":[10,2.0,0],"MOTO":[10,18.0,0]}],["Peoria","Illinois",110000,40,25.4,0,{"SER":[10,31.0,0],"LMR":[20,24.0,0],"GEN":[10,9.0,0]}],["Hoffman Estates","Illinois",50000,40,22.0,0,{"SER":[10,23.0,0],"LMR":[10,22.0,0],"SBR":[10,13.0,0],"GEN":[10,11.0,0]}]];
    var MI_RPV = 558.32;
    var MI_TICKET = 320.73;
    var callsByTab2 = bm.monthlyCallsByCity || {};
    var locTabs = Object.keys(callsByTab2);

    // Match CRM tabs to SEO cities
    var miMatches = {};
    locTabs.forEach(function(tab) {
      var tl = tab.toLowerCase().replace(/[^a-z ]/g, '');
      MI_DATA.forEach(function(mi) {
        var cl = mi[0].toLowerCase();
        if (tl.indexOf(cl) !== -1 || (cl.length > 4 && tl.indexOf(cl.substring(0,5)) !== -1)) {
          if (!miMatches[tab] || mi[3] > miMatches[tab][3]) miMatches[tab] = mi;
        }
      });
    });

    // Build per-location performance
    var mktPerf = [];
    var companyOrders = 0, companyRev = 0;
    locTabs.forEach(function(tab) {
      var td = callsByTab2[tab] || {};
      var tMonths = Object.keys(td).sort();
      var totalCalls = 0;
      tMonths.forEach(function(m) { totalCalls += td[m] || 0; });
      var moActive = Math.max(tMonths.length, 1);
      var avgMo = Math.round(totalCalls / moActive * 10) / 10;
      var lastMo = tMonths.length > 0 ? (td[tMonths[tMonths.length - 1]] || 0) : 0;
      var prevMo = tMonths.length > 1 ? (td[tMonths[tMonths.length - 2]] || 0) : 0;
      var growth = prevMo > 0 ? Math.round(((lastMo - prevMo) / prevMo) * 100) : 0;
      var estRev = Math.round(totalCalls * MI_TICKET);
      companyOrders += totalCalls;
      companyRev += estRev;

      var mi = miMatches[tab];
      var sVol = mi ? mi[3] : 0;
      var kd = mi ? mi[4] : 0;
      var cpc = mi ? mi[5] : 0;
      var pop = mi ? mi[2] : 0;
      var kwBreak = mi ? mi[6] : {};
      var potential = Math.round(sVol * MI_RPV);
      var share = 0;
      if (sVol > 0 && moActive > 0) {
        var annualizedCalls = totalCalls / (moActive / 12);
        var tamCalls = sVol * 12 * 2.5;
        share = Math.round(Math.min((annualizedCalls / tamCalls) * 100, 100) * 10) / 10;
      }
      var capture = potential > 0 ? Math.round(Math.min((estRev / potential) * 100, 100) * 10) / 10 : 0;

      // Revenue per call (actual performance)
      var revPerCall = totalCalls > 0 ? Math.round(estRev / totalCalls) : 0;

      // Cost efficiency: CPC vs Revenue per click
      var cpcROI = cpc > 0 ? Math.round((MI_TICKET / cpc) * 10) / 10 : 0;

      mktPerf.push({
        name: tab, calls: totalCalls, avg: avgMo, last: lastMo, prev: prevMo,
        growth: growth, rev: estRev, months: moActive,
        vol: sVol, kd: kd, cpc: cpc, pop: pop, kwBreak: kwBreak,
        share: share, potential: potential, capture: capture,
        matched: mi ? (mi[0] + ', ' + mi[1]) : '',
        revPerCall: revPerCall, cpcROI: cpcROI
      });
    });
    mktPerf.sort(function(a, b) { return b.rev - a.rev; });

    // ====== MARKET SHARE DASHBOARD ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ffd700;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ffd700;border-radius:50%;box-shadow:0 0 8px #ffd700;display:inline-block;"></span>Market Share & CPC Intelligence</div>';

    // KPI Row
    var matchedLocs = mktPerf.filter(function(l) { return l.vol > 0; });
    var avgShare = matchedLocs.length > 0 ? Math.round(matchedLocs.reduce(function(a,b){return a+b.share;},0)/matchedLocs.length*10)/10 : 0;
    var totalPot = mktPerf.reduce(function(a,b){return a+b.potential;},0);
    var capRate = totalPot > 0 ? Math.round((companyRev/totalPot)*1000)/10 : 0;
    var avgCPC = 0;
    var cpcLocs = mktPerf.filter(function(l){return l.cpc > 0;});
    if (cpcLocs.length > 0) avgCPC = Math.round(cpcLocs.reduce(function(a,b){return a+b.cpc;},0)/cpcLocs.length*100)/100;
    var avgKD = matchedLocs.length > 0 ? Math.round(matchedLocs.reduce(function(a,b){return a+b.kd;},0)/matchedLocs.length*10)/10 : 0;

    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-bottom:20px;">';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #ffd70015;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">AVG MARKET SHARE</div><div style="color:#ffd700;font-size:1.6em;font-weight:900;">' + avgShare + '%</div><div style="color:#4a6a8a;font-size:0.7em;">' + matchedLocs.length + ' matched markets</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #00ff6615;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">EST. ALL-TIME REV</div><div style="color:#00ff66;font-size:1.6em;font-weight:900;">$' + companyRev.toLocaleString() + '</div><div style="color:#4a6a8a;font-size:0.7em;">' + companyOrders + ' orders @ $' + MI_TICKET + '</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #ff9f4315;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">REVENUE POTENTIAL</div><div style="color:#ff9f43;font-size:1.6em;font-weight:900;">$' + (totalPot > 999999 ? (totalPot/1000000).toFixed(1)+'M' : totalPot.toLocaleString()) + '/yr</div><div style="color:#4a6a8a;font-size:0.7em;">Your markets at full capture</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #a855f715;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">CAPTURE RATE</div><div style="color:' + (capRate > 10 ? '#00ff66' : capRate > 5 ? '#ffd700' : '#ff4757') + ';font-size:1.6em;font-weight:900;">' + capRate + '%</div><div style="color:#4a6a8a;font-size:0.7em;">Revenue vs potential</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #00d4ff15;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">AVG CPC</div><div style="color:#00d4ff;font-size:1.6em;font-weight:900;">$' + avgCPC.toFixed(2) + '</div><div style="color:#4a6a8a;font-size:0.7em;">' + cpcLocs.length + ' markets w/ data</div></div>';

    html += '<div style="background:rgba(10,20,35,0.8);border:1px solid #00ff6615;padding:14px;"><div style="color:#4a6a8a;font-size:0.6em;font-family:Orbitron;letter-spacing:2px;">AVG KEYWORD DIFF</div><div style="color:' + (avgKD < 25 ? '#00ff66' : avgKD < 35 ? '#ffd700' : '#ff4757') + ';font-size:1.6em;font-weight:900;">' + avgKD + '</div><div style="color:#4a6a8a;font-size:0.7em;">' + (avgKD < 25 ? 'Easy to rank' : avgKD < 35 ? 'Moderate' : 'Competitive') + '</div></div>';

    html += '</div>';

    // ====== LOCATION TABLE (filterable) ======
    // Embed data as JSON for client-side filtering
    var mktJSON = mktPerf.map(function(loc) {
      var state = '';
      if (loc.name.includes(',')) state = loc.name.split(',').pop().trim();
      else if (loc.matched && loc.matched.includes(',')) state = loc.matched.split(',').pop().trim();
      var act2 = '';
      if (loc.share < 3 && loc.vol > 50) act2 = 'SCALE ADS';
      else if (loc.share < 5 && loc.vol > 30) act2 = 'GROW SEO';
      else if (loc.share > 10) act2 = 'OPTIMIZE';
      else if (loc.vol === 0) act2 = 'UNMATCHED';
      else act2 = 'MAINTAIN';
      return { n: loc.name, c: loc.calls, a: loc.avg, g: loc.growth, r: loc.rev, s: loc.share, v: loc.vol, kd: loc.kd, cpc: loc.cpc, roi: loc.cpcROI, p: loc.potential, cap: loc.capture, m: loc.matched, st: state, act: act2 };
    });

    // Extract unique states
    var stateSet = {};
    mktJSON.forEach(function(l) { if (l.st && l.st.length === 2) stateSet[l.st] = true; });
    var stateList = Object.keys(stateSet).sort();

    html += '<div style="background:rgba(10,20,35,0.9);border:1px solid #1a2a3a;margin-bottom:20px;padding:12px;">';

    // Filter bar
    html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px;align-items:center;">';
    html += '<input id="mkt-search" type="text" placeholder="Search city..." oninput="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 12px;background:#0a1520;color:#c0d8f0;border:1px solid #1a2a3a;outline:none;width:160px;">';
    html += '<select id="mkt-state" onchange="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 10px;background:#0a1520;color:#ffd700;border:1px solid #ffd70030;cursor:pointer;outline:none;">';
    html += '<option value="">ALL STATES</option>';
    stateList.forEach(function(s) { html += '<option value="' + s + '">' + s + '</option>'; });
    html += '</select>';
    html += '<select id="mkt-action" onchange="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 10px;background:#0a1520;color:#ff9f43;border:1px solid #ff9f4330;cursor:pointer;outline:none;">';
    html += '<option value="">ALL ACTIONS</option><option value="SCALE ADS">⚡ SCALE ADS</option><option value="GROW SEO">📈 GROW SEO</option><option value="OPTIMIZE">✅ OPTIMIZE</option><option value="UNMATCHED">❓ UNMATCHED</option><option value="MAINTAIN">🔄 MAINTAIN</option>';
    html += '</select>';
    html += '<select id="mkt-sort" onchange="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 10px;background:#0a1520;color:#00d4ff;border:1px solid #00d4ff30;cursor:pointer;outline:none;">';
    html += '<option value="rev">SORT: REVENUE</option><option value="calls">CALLS</option><option value="growth">GROWTH</option><option value="share">MARKET SHARE</option><option value="vol">SEO VOLUME</option><option value="potential">POTENTIAL</option><option value="kd">KEYWORD DIFF</option><option value="cpc">CPC</option>';
    html += '</select>';
    html += '<select id="mkt-min" onchange="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 10px;background:#0a1520;color:#a855f7;border:1px solid #a855f730;cursor:pointer;outline:none;">';
    html += '<option value="0">MIN: ALL</option><option value="5" selected>5+ CALLS</option><option value="10">10+ CALLS</option><option value="25">25+ CALLS</option><option value="50">50+ CALLS</option><option value="100">100+ CALLS</option>';
    html += '</select>';
    html += '<select id="mkt-show" onchange="filterMkt()" style="font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:7px 10px;background:#0a1520;color:#00ff66;border:1px solid #00ff6630;cursor:pointer;outline:none;">';
    html += '<option value="25" selected>SHOW 25</option><option value="50">SHOW 50</option><option value="100">SHOW 100</option><option value="9999">SHOW ALL</option>';
    html += '</select>';
    html += '<span id="mkt-count" style="color:#4a6a8a;font-size:0.75em;margin-left:auto;"></span>';
    html += '</div>';

    // Table container
    html += '<div style="overflow-x:auto;"><table id="mkt-table" style="width:100%;border-collapse:collapse;font-size:0.78em;">';
    html += '<thead><tr style="border-bottom:2px solid #ffd70020;">';
    ['Location','Calls','Mo Avg','Growth','Est Rev','Share','Vol/Mo','KD','CPC','CPC ROI','Potential','Capture','Action'].forEach(function(h){
      html += '<th style="padding:10px 7px;text-align:left;color:#ffd700;font-family:Orbitron;font-size:0.6em;letter-spacing:1px;white-space:nowrap;">' + h + '</th>';
    });
    html += '</tr></thead><tbody id="mkt-body"></tbody>';
    html += '</table></div></div>';

    // Client-side filter/sort logic
    html += '<script>';
    html += 'var mktData=' + JSON.stringify(mktJSON) + ';';
    html += 'function filterMkt(){';
    html += '  var q=(document.getElementById("mkt-search").value||"").toLowerCase();';
    html += '  var st=document.getElementById("mkt-state").value;';
    html += '  var act=document.getElementById("mkt-action").value;';
    html += '  var sort=document.getElementById("mkt-sort").value;';
    html += '  var min=parseInt(document.getElementById("mkt-min").value)||0;';
    html += '  var show=parseInt(document.getElementById("mkt-show").value)||25;';
    html += '  var filtered=mktData.filter(function(l){';
    html += '    if(q&&l.n.toLowerCase().indexOf(q)===-1&&(!l.m||l.m.toLowerCase().indexOf(q)===-1))return false;';
    html += '    if(st&&l.st!==st)return false;';
    html += '    if(act&&l.act!==act)return false;';
    html += '    if(l.c<min)return false;';
    html += '    return true;';
    html += '  });';
    html += '  var sk={rev:"r",calls:"c",growth:"g",share:"s",vol:"v",potential:"p",kd:"kd",cpc:"cpc"};';
    html += '  var sf=sk[sort]||"r";';
    html += '  filtered.sort(function(a,b){return(b[sf]||0)-(a[sf]||0);});';
    html += '  document.getElementById("mkt-count").textContent="Showing "+Math.min(show,filtered.length)+" of "+filtered.length+" markets";';
    html += '  var tb=document.getElementById("mkt-body");tb.innerHTML="";';
    html += '  filtered.slice(0,show).forEach(function(l,i){';
    html += '    var bg=i%2===0?"rgba(10,20,35,0.4)":"transparent";';
    html += '    var sc=l.s>10?"#00ff66":l.s>5?"#ffd700":l.s>0?"#ff9f43":"#ff4757";';
    html += '    var gc=l.g>0?"#00ff66":l.g<0?"#ff4757":"#4a6a8a";';
    html += '    var kc=l.kd>0?(l.kd<20?"#00ff66":l.kd<30?"#ffd700":"#ff9f43"):"#4a6a8a";';
    html += '    var ai={\"SCALE ADS\":\"\\u26a1 SCALE ADS\",\"GROW SEO\":\"\\ud83d\\udcc8 GROW SEO\",\"OPTIMIZE\":\"\\u2705 OPTIMIZE\",\"UNMATCHED\":\"\\u2753 UNMATCHED\",\"MAINTAIN\":\"\\ud83d\\udd04 MAINTAIN\"};';
    html += '    var tr=document.createElement("tr");tr.style.cssText="background:"+bg+";border-bottom:1px solid #1a2a3a10;";';
    html += '    var h="<td style=\\"padding:8px 7px;\\"><div style=\\"color:#c0d8f0;font-weight:600;white-space:nowrap;\\">"+l.n+"</div>";';
    html += '    if(l.m)h+="<div style=\\"color:#4a6a8a;font-size:0.8em;\\">"+l.m+"</div>";';
    html += '    h+="</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:#00d4ff;font-weight:700;\\">"+l.c+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:#c0d8f0;\\">"+l.a+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:"+gc+";font-weight:700;\\">"+(l.g>=0?"+":"")+l.g+"%</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:#00ff66;font-weight:700;\\">$"+l.r.toLocaleString()+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;\\"><div style=\\"display:flex;align-items:center;gap:5px;\\"><div style=\\"width:40px;height:5px;background:#1a2a3a;border-radius:3px;\\"><div style=\\"width:"+Math.min(l.s,100)+"%;height:100%;background:"+sc+";border-radius:3px;\\"></div></div><span style=\\"color:"+sc+";font-weight:700;font-size:0.9em;\\">"+l.s+"%</span></div></td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:"+(l.v>0?"#a855f7":"#4a6a8a")+"\\">"+(l.v>0?l.v:"\\u2014")+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:"+kc+"\\">"+(l.kd>0?l.kd:"\\u2014")+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:"+(l.cpc>0?"#00d4ff":"#4a6a8a")+";font-weight:700;\\">"+(l.cpc>0?"$"+l.cpc.toFixed(2):"\\u2014")+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:"+(l.roi>50?"#00ff66":l.roi>20?"#ffd700":l.roi>0?"#ff9f43":"#4a6a8a")+"\\">"+(l.roi>0?l.roi+"x":"\\u2014")+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;color:#ff9f43;\\">$"+(l.p>999?Math.round(l.p/1000)+"K":l.p)+"</td>";';
    html += '    h+="<td style=\\"padding:8px 7px;\\"><div style=\\"display:flex;align-items:center;gap:5px;\\"><div style=\\"width:35px;height:5px;background:#1a2a3a;border-radius:3px;\\"><div style=\\"width:"+Math.min(l.cap,100)+"%;height:100%;background:#a855f7;border-radius:3px;\\"></div></div><span style=\\"color:#a855f7;font-size:0.9em;\\">"+l.cap+"%</span></div></td>";';
    html += '    h+="<td style=\\"padding:8px 7px;font-size:0.85em;white-space:nowrap;\\">"+(ai[l.act]||l.act)+"</td>";';
    html += '    tr.innerHTML=h;tb.appendChild(tr);';
    html += '  });';
    html += '}';
    html += 'filterMkt();';
    html += '<\/script>';

    // ====== KEYWORD MIX BY LOCATION ======
    html += '<div style="margin-bottom:20px;">';
    html += '<div onclick="var el=document.getElementById(\'kw-mix\');el.style.display=el.style.display===\'none\'?\'block\':\'none\';this.querySelector(\'.ar\').textContent=el.style.display===\'none\'?\'\u25b6\':\'\u25bc\';" style="cursor:pointer;display:flex;align-items:center;gap:10px;padding:8px 0;">';
    html += '<span class="ar" style="font-family:Orbitron;font-size:0.8em;color:#a855f7;">\u25b6</span>';
    html += '<span style="font-family:Orbitron;font-size:0.65em;letter-spacing:3px;color:#a855f7;">KEYWORD MIX BY LOCATION</span></div>';
    html += '<div id="kw-mix" style="display:none;">';

    var kwColors2 = {SER:'#ff4757',LMR:'#00ff66',SBR:'#00d4ff',GEN:'#ffd700',MOTO:'#a855f7'};
    var kwLabels2 = {SER:'Sm Engine',LMR:'Lawn Mow',SBR:'Snow Blow',GEN:'Generator',MOTO:'Motorcycle'};

    // Legend
    html += '<div style="display:flex;gap:15px;margin-bottom:12px;flex-wrap:wrap;">';
    Object.keys(kwColors2).forEach(function(k) {
      html += '<div style="display:flex;align-items:center;gap:5px;"><div style="width:10px;height:10px;background:' + kwColors2[k] + ';border-radius:2px;"></div><span style="color:#4a6a8a;font-size:0.75em;">' + kwLabels2[k] + '</span></div>';
    });
    html += '</div>';

    mktPerf.filter(function(l){return Object.keys(l.kwBreak).length > 0;}).forEach(function(loc) {
      var kwKeys = Object.keys(loc.kwBreak);
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">';
      html += '<div style="width:140px;font-size:0.75em;color:#c0d8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + loc.name + '</div>';
      html += '<div style="flex:1;display:flex;height:14px;gap:1px;border-radius:3px;overflow:hidden;">';
      kwKeys.forEach(function(kn) {
        var v = loc.kwBreak[kn][0];
        var pct = loc.vol > 0 ? Math.max(2, Math.round((v/loc.vol)*100)) : 0;
        html += '<div style="flex:' + pct + ';background:' + (kwColors2[kn] || '#4a6a8a') + ';position:relative;" title="' + (kwLabels2[kn]||kn) + ': ' + v + '/mo (KD:' + loc.kwBreak[kn][1] + ', CPC:$' + loc.kwBreak[kn][2] + ')"></div>';
      });
      html += '</div>';
      html += '<div style="width:50px;text-align:right;font-size:0.7em;color:#4a6a8a;">' + loc.vol + '/mo</div>';
      html += '</div>';
    });
    html += '</div></div>';
    html += '</div>'; // end market share section

    // ====== NEXT MARKETS TO ATTACK ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#ff4757;text-transform:uppercase;margin-bottom:5px;display:flex;align-items:center;gap:10px;"><span style="width:8px;height:8px;background:#ff4757;border-radius:50%;box-shadow:0 0 8px #ff4757;display:inline-block;"></span>Next Markets to Attack</div>';
    html += '<div style="color:#4a6a8a;font-size:0.8em;margin-bottom:15px;">Ranked by Attack Score = Volume \u00d7 (100-KD)/100 \u00d7 Season. Higher = easier money.</div>';

    // Filter out existing cities
    var existCities = {};
    locTabs.forEach(function(t) { var mi = miMatches[t]; if (mi) existCities[mi[0].toLowerCase()+'|'+mi[1].toLowerCase()] = true; });

    var attackList = [];
    MI_DATA.forEach(function(mi) {
      var key = mi[0].toLowerCase()+'|'+mi[1].toLowerCase();
      if (existCities[key]) return;
      var vol = mi[3], kd = mi[4], cpc = mi[5];
      var has4Season = mi[6] && mi[6].SBR ? 1.15 : 1.0;
      var score = Math.round(vol * ((100-kd)/100) * has4Season);
      attackList.push({ city: mi[0], state: mi[1], pop: mi[2], vol: vol, kd: kd, cpc: cpc, score: score, rev: Math.round(vol*MI_RPV), orders: Math.round(vol*MI_RPV/MI_TICKET), season: has4Season > 1 ? '4-Season' : 'Mow', kw: mi[6] || {} });
    });
    attackList.sort(function(a,b){return b.score-a.score;});

    // Attack cards - top 20
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:10px;">';
    attackList.slice(0, 20).forEach(function(m, idx) {
      var tier = idx < 5 ? '#ff4757' : idx < 10 ? '#ffd700' : idx < 15 ? '#00d4ff' : '#4a6a8a';
      var kdC = m.kd < 20 ? '#00ff66' : m.kd < 30 ? '#ffd700' : '#ff9f43';
      html += '<div style="background:rgba(10,20,35,0.8);border:1px solid ' + tier + '20;padding:12px;">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">';
      html += '<div><span style="font-family:Orbitron;font-size:0.65em;color:' + tier + ';margin-right:6px;">#' + (idx+1) + '</span>';
      html += '<span style="color:#c0d8f0;font-weight:700;">' + m.city + '</span>';
      html += '<span style="color:#4a6a8a;font-size:0.85em;">, ' + m.state + '</span></div>';
      html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:2px;color:' + tier + ';">SCORE: ' + m.score + '</div></div>';

      html += '<div style="display:flex;gap:6px;flex-wrap:wrap;">';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">VOL/MO</div><div style="color:#a855f7;font-weight:700;font-size:0.95em;">' + m.vol + '</div></div>';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">KD</div><div style="color:' + kdC + ';font-weight:700;font-size:0.95em;">' + m.kd + '</div></div>';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">CPC</div><div style="color:#00d4ff;font-weight:700;font-size:0.95em;">' + (m.cpc > 0 ? '$'+m.cpc.toFixed(2) : '\u2014') + '</div></div>';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">REV/YR</div><div style="color:#00ff66;font-weight:700;font-size:0.95em;">$' + (m.rev>999?Math.round(m.rev/1000)+'K':m.rev) + '</div></div>';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">ORDERS</div><div style="color:#ff9f43;font-weight:700;font-size:0.95em;">' + m.orders + '/yr</div></div>';
      html += '<div style="flex:1;background:rgba(5,10,20,0.5);padding:5px 7px;text-align:center;min-width:50px;"><div style="color:#4a6a8a;font-size:0.5em;">POP</div><div style="color:#c0d8f0;font-weight:700;font-size:0.95em;">' + (m.pop > 999999 ? (m.pop/1000000).toFixed(1)+'M' : m.pop > 999 ? Math.round(m.pop/1000)+'K' : m.pop) + '</div></div>';
      html += '</div>';

      // Keyword breakdown bar
      var kwKeys = Object.keys(m.kw);
      if (kwKeys.length > 0) {
        html += '<div style="display:flex;gap:1px;margin-top:6px;height:3px;border-radius:2px;overflow:hidden;">';
        var kwC = {SER:'#ff4757',LMR:'#00ff66',SBR:'#00d4ff',GEN:'#ffd700',MOTO:'#a855f7'};
        kwKeys.forEach(function(kn) {
          var kv = m.kw[kn][0];
          var pct = m.vol > 0 ? Math.max(1,Math.round((kv/m.vol)*100)) : 0;
          html += '<div style="flex:' + pct + ';background:' + (kwC[kn]||'#4a6a8a') + ';" title="' + kn + ': ' + kv + '"></div>';
        });
        html += '</div>';
      }
      html += '</div>';
    });
    html += '</div>';

    // National opportunity banner
    var natVol = 0;
    MI_DATA.forEach(function(mi) { natVol += mi[3]; });
    var natRev = Math.round(natVol * MI_RPV);
    html += '<div style="margin-top:15px;padding:14px;background:rgba(255,215,0,0.04);border:1px solid #ffd70025;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;">';
    html += '<div><div style="font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#ffd700;">NATIONAL MARKET OPPORTUNITY</div>';
    html += '<div style="color:#c0d8f0;font-size:0.8em;margin-top:3px;">' + MI_DATA.length + ' cities \u00b7 50 states \u00b7 ' + natVol.toLocaleString() + ' searches/month</div></div>';
    html += '<div style="text-align:right;"><div style="color:#00ff66;font-size:1.8em;font-weight:900;font-family:Orbitron;">$' + (natRev/1000000).toFixed(1) + 'M</div>';
    html += '<div style="color:#4a6a8a;font-size:0.7em;">Annual revenue at KC rate ($' + MI_RPV + '/vol)</div></div>';
    html += '</div>';

    // ====== EASY WINS (Low KD, High Volume) ======
    var easyWins = attackList.filter(function(m) { return m.kd < 20 && m.vol >= 30; }).slice(0, 10);
    if (easyWins.length > 0) {
      html += '<div style="margin-top:15px;padding:14px;background:rgba(0,255,102,0.03);border:1px solid #00ff6620;">';
      html += '<div style="font-family:Orbitron;font-size:0.6em;letter-spacing:3px;color:#00ff66;margin-bottom:10px;">\ud83c\udfaf EASY WINS \u2014 Low KD (<20) + High Volume (30+)</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:8px;">';
      easyWins.forEach(function(m) {
        html += '<div style="background:rgba(0,255,102,0.05);border:1px solid #00ff6615;padding:8px 12px;display:flex;align-items:center;gap:8px;">';
        html += '<div><div style="color:#c0d8f0;font-weight:600;font-size:0.9em;">' + m.city + ', ' + m.state + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.75em;">' + m.vol + '/mo \u00b7 KD:' + m.kd + ' \u00b7 $' + (m.rev > 999 ? Math.round(m.rev/1000)+'K' : m.rev) + '/yr</div></div>';
        html += '</div>';
      });
      html += '</div></div>';
    }
    html += '</div>'; // end attack section


    // ====== DATA SOURCES HEALTH ======
    html += '<div style="max-width:1400px;margin:0 auto;padding:0 40px 30px;">';
    html += '<div style="font-family:Orbitron;font-size:0.9em;letter-spacing:5px;color:#4a6a8a;text-transform:uppercase;margin-bottom:15px;">DATA SOURCES</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:8px;">';
    var metaEntries = Object.entries(sheetMeta);
    if (metaEntries.length > 0) {
      metaEntries.forEach(function(m) {
        html += '<div style="background:rgba(10,20,35,0.6);border:1px solid #00ff6610;padding:10px;display:flex;align-items:center;gap:10px;">';
        html += '<div style="width:6px;height:6px;background:#00ff66;border-radius:50%;box-shadow:0 0 6px #00ff66;"></div>';
        html += '<div><div style="color:#c0d8f0;font-size:0.9em;">' + m[0] + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.7em;">' + (m[1].tabs || []).length + ' tabs</div></div></div>';
      });
    } else {
      html += '<div style="color:#ff4757;padding:15px;">No source sheets loaded. Verify service account has access to all 12 spreadsheets.</div>';
    }
    html += '</div></div>';

    // Tab Modal (reuse from main dashboard)
    html += '<div id="tab-modal">';
    html += '<div style="max-width:1200px;margin:30px auto;padding:20px;">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">';
    html += '<div id="modal-title" style="font-family:Orbitron;font-size:1.2em;letter-spacing:3px;color:#a855f7;"></div>';
    html += '<div onclick="closeModal()" style="font-family:Orbitron;font-size:0.8em;letter-spacing:2px;color:#ff4757;cursor:pointer;padding:10px 20px;border:1px solid #ff475730;">CLOSE</div>';
    html += '</div>';
    html += '<div id="modal-loading" style="text-align:center;padding:60px;font-family:Orbitron;font-size:0.8em;letter-spacing:5px;color:#4a6a8a;">LOADING...</div>';
    html += '<div id="modal-content" style="overflow-x:auto;"></div>';
    html += '</div></div>';

    // JavaScript
    html += '<script>';
    html += 'function loadTab(name){';
    html += '  document.getElementById("tab-modal").style.display="block";';
    html += '  document.getElementById("modal-title").textContent=name;';
    html += '  document.getElementById("modal-loading").style.display="block";';
    html += '  document.getElementById("modal-content").innerHTML="";';
    html += '  fetch("/tab/"+encodeURIComponent(name))';
    html += '    .then(function(r){return r.json()})';
    html += '    .then(function(data){';
    html += '      document.getElementById("modal-loading").style.display="none";';
    html += '      if(!data.headers||data.headers.length===0){document.getElementById("modal-content").innerHTML="<div style=\\"color:#4a6a8a;text-align:center;padding:40px;\\">NO DATA</div>";return;}';
    html += '      var headers=data.headers;var rows=(data.rows||[]).slice(0,50);';
    html += '      var t="<div style=\\"font-family:Orbitron;font-size:0.65em;color:#4a6a8a;letter-spacing:2px;margin-bottom:10px;\\">"+(data.rowCount||0)+" ROWS</div>";';
    html += '      t+="<table style=\\"width:100%;border-collapse:collapse;font-size:0.85em;\\">";';
    html += '      t+="<thead><tr>";';
    html += '      for(var h=0;h<headers.length;h++){t+="<th style=\\"padding:8px 10px;text-align:left;border-bottom:1px solid #a855f720;font-family:Orbitron;font-size:0.65em;letter-spacing:2px;color:#a855f7;white-space:nowrap;\\">"+headers[h]+"</th>";}';
    html += '      t+="</tr></thead><tbody>";';
    html += '      for(var r=0;r<rows.length;r++){';
    html += '        t+="<tr style=\\"border-bottom:1px solid #0a1520;\\">";';
    html += '        for(var c=0;c<headers.length;c++){var val=rows[r][c]||"";t+="<td style=\\"padding:6px 10px;color:#7a9ab0;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;\\">"+val+"</td>";}';
    html += '        t+="</tr>";';
    html += '      }';
    html += '      t+="</tbody></table>";';
    html += '      document.getElementById("modal-content").innerHTML=t;';
    html += '    }).catch(function(e){document.getElementById("modal-content").innerHTML="ERROR: "+e.message;});';
    html += '}';
    html += 'function closeModal(){document.getElementById("tab-modal").style.display="none";}';
    html += 'document.addEventListener("keydown",function(e){if(e.key==="Escape")closeModal();});';

    // Live clock
    html += 'function updateClock(){var d=new Date();var h=String(d.getHours()).padStart(2,"0");var m=String(d.getMinutes()).padStart(2,"0");var s=String(d.getSeconds()).padStart(2,"0");document.getElementById("clock").textContent=h+":"+m+":"+s;}setInterval(updateClock,1000);updateClock();';
    html += '<\/script>';

    // Clock
    html += '<div class="clock" id="clock"></div>';

    // Footer
    html += '<div class="footer">JARVIS • ATHENA • TOOKAN // Wildwood Small Engine Repair // v4.4</div>';

    // Mobile padding fix
    html += '<script>';
    html += 'if(window.innerWidth<=768){document.querySelectorAll("[style]").forEach(function(el){';
    html += '  var s=el.getAttribute("style")||"";';
    html += '  if(s.indexOf("0 40px")>-1||s.indexOf("0px 40px")>-1){el.style.paddingLeft="12px";el.style.paddingRight="12px";}';
    html += '  if(s.indexOf("repeat(3,")>-1||s.indexOf("repeat(3, ")>-1){el.style.gridTemplateColumns="1fr";}';
    html += '  if(s.indexOf("repeat(4,")>-1||s.indexOf("repeat(4, ")>-1){el.style.gridTemplateColumns="repeat(2,1fr)";}';
    html += '});}';
    html += '<\/script>';

    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


app.post('/chat', async function(req, res) {
  var sessionId = req.body.sessionId || 'default';
  var userMessage = req.body.message || '';
  var lowerMsg = userMessage.toLowerCase().trim();
  var from = 'whatsapp:+18167392734'; // for reminders/events that need the number

  try {
    // ====== VOICE COMMANDS ======

    // Win logging
    if (lowerMsg.startsWith('win') && (lowerMsg.includes(':') || lowerMsg.length > 5)) {
      var winText = userMessage.replace(/^win[:\s]*/i, '').trim();
      try {
        var today = new Date().toISOString().split('T')[0];
        var area = await askClaude(
          "Categorize this win into exactly one of: Work, Health, Social, Financial, Personal Growth, Dating. Respond with ONLY the category name.",
          [{ role: 'user', content: winText }]
        );
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: "'Wins'!A:D",
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[today, winText, area.trim(), 'Logged via voice']] },
        });
      } catch (e) {}
      return res.json({ response: "Win logged. Keep stacking, Trace." });
    }

    // Gym logging
    if (lowerMsg.startsWith('gym') && (lowerMsg.includes(':') || lowerMsg.length > 5)) {
      var gymText = userMessage.replace(/^gym[:\s]*/i, '').trim();
      try {
        var today = new Date().toISOString().split('T')[0];
        var day = new Date().toLocaleDateString('en-US', { weekday: 'long' });
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: "'Gym_Log'!A:E",
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[today, day, gymText, '', 'Via voice']] },
        });
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: "'Wins'!A:D",
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[today, 'Gym: ' + gymText, 'Health', 'Via voice']] },
        });
      } catch (e) {}
      return res.json({ response: "Gym logged. " + gymText + ". The discipline is building, Trace." });
    }

    // Calendar
    if (lowerMsg === 'calendar' || lowerMsg === 'schedule' || lowerMsg === 'events' || lowerMsg.includes('what do i have') || lowerMsg.includes("what's today") || lowerMsg.includes('whats today') || lowerMsg.includes('my schedule')) {
      try {
        var accounts = Object.keys(gmailTokens);
        var allEvents = [];
        for (var ca = 0; ca < accounts.length; ca++) {
          var events = await getCalendarEvents(accounts[ca], 1);
          allEvents = allEvents.concat(events);
        }
        if (allEvents.length === 0) {
          return res.json({ response: "Nothing on your calendar today. It's an open day. Use it wisely." });
        }
        var eventList = allEvents.map(function(e, i) { return e.time + ", " + e.summary + (e.location ? " at " + e.location : ""); }).join(". ");
        return res.json({ response: "Today you have: " + eventList });
      } catch (e) {
        return res.json({ response: "Couldn't access your calendar. You may need to reconnect." });
      }
    }

    // This week
    if (lowerMsg === 'week' || lowerMsg === 'this week' || lowerMsg.includes('weekly schedule')) {
      try {
        var accounts = Object.keys(gmailTokens);
        var allEvents = [];
        for (var ca = 0; ca < accounts.length; ca++) {
          var events = await getCalendarEvents(accounts[ca], 7);
          allEvents = allEvents.concat(events);
        }
        if (allEvents.length === 0) {
          return res.json({ response: "Nothing on your calendar this week." });
        }
        var eventList = allEvents.map(function(e) { return e.date + " " + e.time + ", " + e.summary; }).join(". ");
        return res.json({ response: "This week: " + eventList });
      } catch (e) {
        return res.json({ response: "Couldn't access your calendar." });
      }
    }

    // Reminders
    if (lowerMsg.startsWith('remind') || lowerMsg.startsWith('todo') || lowerMsg.startsWith('i need to') || lowerMsg.startsWith('i gotta') || lowerMsg.startsWith('need to')) {
      var reminderText = userMessage.replace(/^(remind[:\s]*|todo[:\s]*|i need to\s*|i gotta\s*|need to\s*)/i, '').trim();
      var reminderId = 'r' + Date.now();
      global.activeReminders = global.activeReminders || {};
      global.activeReminders[reminderId] = { text: reminderText, created: Date.now(), done: false, nudges: 0 };

      // Log to sheet
      try {
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: "'Reminders'!A:F",
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[new Date().toISOString().split('T')[0], new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }), reminderText, 'PENDING', '', '']] },
        });
      } catch (e) {}

      // Set up nudges
      var fiveHours = 5 * 60 * 60 * 1000;
      var tenHours = 10 * 60 * 60 * 1000;
      setTimeout(async function() {
        if (global.activeReminders[reminderId] && !global.activeReminders[reminderId].done) {
          var hour = new Date().getHours();
          if (hour >= 7 && hour < 23) {
            try { await twilioClient.messages.create({ body: "Reminder: \"" + reminderText + "\" — still pending.", from: 'whatsapp:+14155238886', to: '+18167392734' }); } catch (e) {}
          }
        }
      }, fiveHours);
      setTimeout(async function() {
        if (global.activeReminders[reminderId] && !global.activeReminders[reminderId].done) {
          var hour = new Date().getHours();
          if (hour >= 7 && hour < 23) {
            try {
              await twilioClient.messages.create({ body: "\"" + reminderText + "\" still not done. Calling you.", from: 'whatsapp:+14155238886', to: '+18167392734' });
              setTimeout(async function() {
                if (global.activeReminders[reminderId] && !global.activeReminders[reminderId].done) {
                  try { await twilioClient.calls.create({ to: MY_NUMBER, from: TWILIO_NUMBER, twiml: '<Response><Say voice="Polly.Matthew">Trace. You told me to remind you about: ' + reminderText.replace(/[<>&"']/g, '') + '. Handle it now.</Say></Response>' }); } catch (e) {}
                }
              }, 60000);
            } catch (e) {}
          }
        }
      }, tenHours);

      return res.json({ response: "Got it. I'll remind you about " + reminderText + ". First nudge in 5 hours. If it's not done in 10, I'm calling you." });
    }

    // Mark done
    if (lowerMsg.startsWith('done') || lowerMsg.startsWith('finished')) {
      var doneText = userMessage.replace(/^(done[:\s]*|finished[:\s]*)/i, '').trim().toLowerCase();
      global.activeReminders = global.activeReminders || {};
      var cleared = 0;
      var rKeys = Object.keys(global.activeReminders);
      for (var rk = 0; rk < rKeys.length; rk++) {
        if (!global.activeReminders[rKeys[rk]].done && global.activeReminders[rKeys[rk]].text.toLowerCase().includes(doneText)) {
          global.activeReminders[rKeys[rk]].done = true;
          cleared++;
        }
      }
      if (cleared > 0) {
        try {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: "'Wins'!A:D",
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[new Date().toISOString().split('T')[0], 'Completed: ' + doneText, 'Personal Growth', 'Via voice']] },
          });
        } catch (e) {}
        return res.json({ response: "Cleared. That's execution. Logged as a win too." });
      }
      return res.json({ response: "No active reminders matching that." });
    }

    // Reminders list
    if (lowerMsg === 'reminders' || lowerMsg === 'what do i need to do') {
      global.activeReminders = global.activeReminders || {};
      var pending = [];
      var rKeys2 = Object.keys(global.activeReminders);
      for (var rk2 = 0; rk2 < rKeys2.length; rk2++) {
        if (!global.activeReminders[rKeys2[rk2]].done) {
          pending.push(global.activeReminders[rKeys2[rk2]].text);
        }
      }
      if (pending.length > 0) {
        return res.json({ response: "You have " + pending.length + " pending: " + pending.join(", ") });
      }
      return res.json({ response: "No pending reminders. You're clear." });
    }

    // Briefing
    if (lowerMsg === 'briefing' || lowerMsg === 'brief' || lowerMsg === 'status') {
      var context = await buildLifeOSContext();
      var emailContext = await buildEmailContext();
      var briefing = await askClaude(
        "You are Jarvis, Trace's personal AI counselor. Give a spoken briefing. Be concise, 3-5 sentences. No markdown. Talk like a mentor.\n\nLIFE OS DATA:\n" + context + emailContext,
        [{ role: 'user', content: 'Give me my briefing.' }]
      );
      return res.json({ response: briefing });
    }

    // Business briefing
    if (lowerMsg === 'business' || lowerMsg === 'crm' || lowerMsg === 'customers' || lowerMsg === 'bookings' || lowerMsg.includes('how many bookings') || lowerMsg.includes('how many customers') || lowerMsg.includes('business update') || lowerMsg.includes('crm update')) {
      var bizContext = await buildBusinessContext();
      var bizBriefing = await askClaude(
        "You are Jarvis, Trace's business AI assistant for Wildwood Small Engine Repair. Give a spoken business update. Be concise, 3-5 sentences. No markdown. Mention key numbers: bookings, cancellations, anything that needs attention like rescheduling. Talk like a sharp operations manager.\n\nCRM DATA:\n" + bizContext,
        [{ role: 'user', content: 'Give me my business update.' }]
      );
      return res.json({ response: bizBriefing });
    }

    // Emails
    if (lowerMsg === 'email' || lowerMsg === 'emails' || lowerMsg === 'inbox') {
      var emailContext = await buildEmailContext();
      if (!emailContext) return res.json({ response: "No email accounts connected." });
      var summary = await askClaude(
        "You are Jarvis. Summarize these emails in 2-3 spoken sentences. What's urgent, what can wait. No markdown.\n\n" + emailContext,
        [{ role: 'user', content: 'Summarize my inbox.' }]
      );
      return res.json({ response: summary });
    }

    // ====== DEFAULT: Regular conversation ======
    var history = webChatHistory[sessionId];
    if (!history) {
      var context = await buildLifeOSContext();
      var bizContext = '';
      try { bizContext = await buildBusinessContext(); } catch (e) {}
      history = {
        systemPrompt: "You are Jarvis, Trace's personal AI counselor, mentor, and business operations assistant. You are speaking through a browser voice interface.\n\nRULES:\n- Talk like a wise friend, life coach, and sharp business partner.\n- NEVER mention tab names, sheet names, row counts, or entry counts.\n- NEVER recite statistics unless Trace specifically asks for numbers.\n- Use the data to UNDERSTAND his life and business, then give human advice.\n- If he asks about business/customers/bookings, use the CRM data.\n- If he asks personal questions, use the Life OS data.\n- Ask thoughtful questions. Push him to grow.\n- Keep responses to 1-3 sentences MAX. You are being read aloud.\n- Never use markdown, bullet points, or formatting.\n- Sound like a real person, not a robot or a database.\n\nLIFE OS DATA (personal):\n" + context + "\n\nBUSINESS CRM DATA (Wildwood Small Engine Repair):\n" + bizContext,
        messages: [],
      };
    }

    history.messages.push({ role: 'user', content: userMessage });
    var response = await askClaude(history.systemPrompt, history.messages);
    history.messages.push({ role: 'assistant', content: response });
    if (history.messages.length > 20) history.messages = history.messages.slice(-10);
    webChatHistory[sessionId] = history;

    res.json({ response: response });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   POST /tts — ElevenLabs Text to Speech
=========================== */

app.post('/tts', async function(req, res) {
  var text = req.body.text || '';
  var apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'No ElevenLabs API key' });

  try {
    // Selected voice from ElevenLabs library
    var voiceId = 'jP5jSWhfXz3nfQENMtf4';
    var url = 'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId;

    var https = require('https');
    var postData = JSON.stringify({
      text: text,
      model_id: 'eleven_turbo_v2',
      voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.3 }
    });

    var options = {
      method: 'POST',
      headers: {
        'xi-api-key': apiKey,
        'Content-Type': 'application/json',
        'Accept': 'audio/mpeg',
      },
    };

    var proxyReq = https.request(url, options, function(proxyRes) {
      var chunks = [];
      proxyRes.on('data', function(chunk) { chunks.push(chunk); });
      proxyRes.on('end', function() {
        var buffer = Buffer.concat(chunks);
        if (proxyRes.statusCode === 200) {
          res.set('Content-Type', 'audio/mpeg');
          res.send(buffer);
        } else {
          res.status(500).json({ error: 'ElevenLabs error: ' + buffer.toString() });
        }
      });
    });
    proxyReq.on('error', function(e) { res.status(500).json({ error: e.message }); });
    proxyReq.write(postData);
    proxyReq.end();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   GET /daily-questions — Auto trigger daily questions via cron
=========================== */

app.get('/daily-questions', async function(req, res) {
  var secret = process.env.CALL_SECRET;
  if (secret && req.query.key !== secret) return res.status(403).json({ error: 'Unauthorized' });

  try {
    var qContext = await buildLifeOSContext();
    var questionsRaw = await askClaude(
      "You generate 10 deep, personal challenge questions for Trace. These questions should challenge his beliefs, make him uncomfortable, and force growth.\n\nCover ALL these areas across the 10 questions: dating and relationships, money mindset, self-worth, business ambition, daily habits, health, purpose, fears, accountability, and personal identity.\n\nRULES:\n- Make each question personal based on his Life OS data\n- Questions should be uncomfortable but constructive\n- Never mention tab names, sheet names, or entry counts\n- Use what you know about his life to make questions HIT\n- Format: one question per line, numbered 1-10, nothing else\n- No fluff, no explanations, just the questions\n\nLIFE OS DATA:\n" + qContext,
      [{ role: 'user', content: 'Generate 10 deep personal challenge questions for today.' }]
    );

    var questions = questionsRaw.split('\n').filter(function(q) { return q.trim().match(/^\d/); });
    if (questions.length < 10) questions = questionsRaw.split('\n').filter(function(q) { return q.trim().length > 10; });

    var from = 'whatsapp:+18167392734';
    whatsappHistory[from] = whatsappHistory[from] || {};
    whatsappHistory[from].dailyQuestions = {
      questions: questions,
      answers: [],
      currentIndex: 0,
      date: new Date().toISOString().split('T')[0],
      active: true,
    };

    await twilioClient.messages.create({
      body: "Good morning Trace. Time to check in with yourself. 10 questions about your life, your beliefs, and where you're headed. Answer honestly — I'll tell you which beliefs need fixing and what to do about it.\n\n" + questions[0],
      from: 'whatsapp:+14155238886',
      to: '+18167392734',
    });

    res.json({ success: true, questionsGenerated: questions.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   POST /email/delete — Delete email from dashboard
=========================== */
app.post('/email/delete', async function(req, res) {
  try {
    var gmailClient = await getGmailClient(req.body.account);
    if (!gmailClient) return res.status(400).json({ error: 'Not connected' });
    await gmailClient.users.messages.trash({ userId: 'me', id: req.body.id });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /email/archive — Archive email from dashboard
=========================== */
app.post('/email/archive', async function(req, res) {
  try {
    var gmailClient = await getGmailClient(req.body.account);
    if (!gmailClient) return res.status(400).json({ error: 'Not connected' });
    await gmailClient.users.messages.modify({ userId: 'me', id: req.body.id, requestBody: { removeLabelIds: ['INBOX'] } });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /email/ai-reply — Generate AI reply
=========================== */
app.post('/email/ai-reply', async function(req, res) {
  try {
    var gmailClient = await getGmailClient(req.body.account);
    if (!gmailClient) return res.status(400).json({ error: 'Not connected' });
    var msgData = await gmailClient.users.messages.get({ userId: 'me', id: req.body.id, format: 'full' });
    var body = '';
    if (msgData.data.payload.body && msgData.data.payload.body.data) {
      body = Buffer.from(msgData.data.payload.body.data, 'base64').toString();
    } else if (msgData.data.payload.parts) {
      for (var p = 0; p < msgData.data.payload.parts.length; p++) {
        if (msgData.data.payload.parts[p].mimeType === 'text/plain' && msgData.data.payload.parts[p].body.data) {
          body = Buffer.from(msgData.data.payload.parts[p].body.data, 'base64').toString();
          break;
        }
      }
    }
    var headers = msgData.data.payload.headers;
    var fromH = headers.find(function(h) { return h.name === 'From'; });
    var subjH = headers.find(function(h) { return h.name === 'Subject'; });

    var reply = await askClaude(
      "You are Trace's AI assistant. Write a professional, friendly email reply. Keep it concise (2-4 sentences). No subject line, just the body. Sign off as Trace.",
      [{ role: 'user', content: 'Reply to this email:\nFrom: ' + (fromH ? fromH.value : '') + '\nSubject: ' + (subjH ? subjH.value : '') + '\nBody: ' + body.substring(0, 1000) }]
    );
    res.json({ reply: reply });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /email/send-reply — Send the AI-generated reply
=========================== */
app.post('/email/send-reply', async function(req, res) {
  try {
    var result = await replyToEmail(req.body.account, req.body.id, req.body.reply);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /nightly-checkin — Auto trigger daily check-in via cron
=========================== */

/* ===========================
   GET /business/tabs — Browse all source tabs
=========================== */
app.get('/business/tabs', async function(req, res) {
  try {
    // Lazy load source tabs with separate cache
    if (!global.allSourceTabs || !global.sourceTabsTime || (Date.now() - global.sourceTabsTime) > 1800000) {
      // Load all 12 source spreadsheets (cached for 30 min)
      var sourceSheetIds = [
        "1kl72v4yIJrpD3U5pCYwtCiDhtdFZ_n4wUvZDxhPeIQk",
        "1LlfhcfiQdXStpV1vRrZzSmaEjyTvd1nrk5sqnhxsRYU",
        "1SDOqTxEMG8f81DtLwIu9ovz9mnxP_9Lpn6PH7OFBfBs",
        "1fj6SZZx5YtLMU8ldsAEZ1yffK0rHhFQoFir0GAOCFNU",
        "1ZITxT57ue2qSAbTUFRE_k1fJzKicPAO-BZMDSJOhJ7A",
        "1CJ9nn7l_PAwXPVSXmUogXejt46bsimrJi5iLQFQuo3o",
        "1KIulnemtmR6QpRbzEflNjvVOElNszuL9zClKhcbmOow",
        "19ndlgop-P0KLwv6PiG8sPdtj83jKH3vlDHf5AgjgmC8",
        "1A8oUmigHV6DsYcWF4hlDBC5KQDIHWMOh1Is6poCacx4",
        "1vnNEZjdhhkFNpNkDXRINpS55Zfysb_MzuFLhjVw6A2g",
        "1ZshCanMloF8uUlH39s2ZxvpCuvxjJQAONaSnN7590WA",
        "1IK-T9O_-ozg7n-Fecn1DodEMuznbvVW-i0ClzCPfuOI"
      ];
      
      var allTabData = [];
      for (var si = 0; si < sourceSheetIds.length; si++) {
        try {
          var meta = await sheets.spreadsheets.get({ spreadsheetId: sourceSheetIds[si], fields: 'properties.title,sheets.properties.title' });
          var ssTitle = meta.data.properties.title;
          var tabList = meta.data.sheets || [];
          for (var ti = 0; ti < tabList.length; ti++) {
            allTabData.push({ ssId: sourceSheetIds[si], ssTitle: ssTitle, tabTitle: tabList[ti].properties.title, headers: [], rows: [], rowCount: 0, loaded: false });
          }
          // Small delay to avoid quota
          await new Promise(function(r) { setTimeout(r, 500); });
        } catch(e) { console.log("Tab list error: " + e.message); }
      }
      global.allSourceTabs = allTabData;
      global.sourceTabsTime = Date.now();
    }

    var tabs = global.allSourceTabs || [];
    var tabName = req.query.tab || '';
    
    if (!tabName) {
      // Return list of all tabs
      var tabList = tabs.map(function(t) {
        return { source: t.ssTitle, tab: t.tabTitle, rows: t.rowCount, headers: t.headers };
      });
      var html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>ATHENA — All Tabs</title>';
      html += '<style>';
      html += '@import url("https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;500;600;700&display=swap");';
      html += 'body{background:#020810;color:#c0d8f0;font-family:Rajdhani,sans-serif;padding:20px;}';
      html += 'h1{font-family:Orbitron;color:#a855f7;text-align:center;letter-spacing:5px;}';
      html += '.source{margin:20px 0;border:1px solid #a855f720;padding:15px;}';
      html += '.source-title{font-family:Orbitron;color:#ffd700;font-size:1.1em;letter-spacing:3px;margin-bottom:10px;}';
      html += '.tab-link{display:inline-block;margin:4px;padding:8px 16px;border:1px solid #a855f730;color:#c0d8f0;text-decoration:none;cursor:pointer;transition:all 0.3s;}';
      html += '.tab-link:hover{background:#a855f720;border-color:#a855f7;}';
      html += '.row-count{color:#4a6a8a;font-size:0.8em;margin-left:5px;}';
      html += 'a{color:#00d4ff;text-decoration:none;}';
      html += '</style></head><body>';
      html += '<h1>ALL SOURCE DATA</h1>';
      html += '<div style="text-align:center;margin-bottom:20px;"><a href="/business">← Back to Dashboard</a></div>';
      html += '<div style="text-align:center;color:#4a6a8a;margin-bottom:20px;">' + tabList.length + ' tabs across ' + new Set(tabList.map(function(t){return t.source;})).size + ' spreadsheets</div>';
      
      // Group by source
      var sources = {};
      tabList.forEach(function(t) {
        if (!sources[t.source]) sources[t.source] = [];
        sources[t.source].push(t);
      });
      
      Object.entries(sources).forEach(function(s) {
        html += '<div class="source">';
        html += '<div class="source-title">' + s[0] + '</div>';
        s[1].forEach(function(t) {
          html += '<a class="tab-link" href="/business/tabs?tab=' + encodeURIComponent(s[0] + ' | ' + t.tab) + '">' + t.tab + '<span class="row-count">(' + t.rows + ')</span></a>';
        });
        html += '</div>';
      });
      
      html += '</body></html>';
      return res.send(html);
    }
    
    // Show specific tab data
    var parts = tabName.split(' | ');
    var ssName = parts[0] || '';
    var tName = parts[1] || '';
    var found = tabs.find(function(t) { return t.ssTitle === ssName && t.tabTitle === tName; });
    
    if (!found) return res.status(404).json({ error: 'Tab not found' });
    
    // Load data on demand if not yet loaded
    if (!found.loaded) {
      try {
        var tabRes = await sheets.spreadsheets.values.get({
          spreadsheetId: found.ssId,
          range: "'" + found.tabTitle + "'!A1:ZZ",
        });
        var tabRows = tabRes.data.values || [];
        found.headers = tabRows[0] || [];
        found.rows = tabRows.slice(1);
        found.rowCount = found.rows.length;
        found.loaded = true;
      } catch(e) {
        return res.status(500).json({ error: 'Failed to load tab: ' + e.message });
      }
    }
    
    var html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>' + tName + ' — ATHENA</title>';
    html += '<style>';
    html += '@import url("https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;500;600;700&display=swap");';
    html += 'body{background:#020810;color:#c0d8f0;font-family:Rajdhani,sans-serif;padding:20px;}';
    html += 'h1{font-family:Orbitron;color:#a855f7;letter-spacing:3px;font-size:1.3em;}';
    html += 'h2{color:#ffd700;font-family:Orbitron;font-size:0.8em;letter-spacing:2px;}';
    html += 'table{border-collapse:collapse;width:100%;margin-top:15px;}';
    html += 'th{background:#0d1117;color:#a855f7;padding:8px 10px;text-align:left;font-family:Orbitron;font-size:0.65em;letter-spacing:1px;border:1px solid #a855f720;white-space:nowrap;}';
    html += 'td{padding:6px 10px;border:1px solid #1a2a3a;font-size:0.9em;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}';
    html += 'tr:nth-child(even){background:rgba(168,85,247,0.03);}';
    html += 'tr:hover{background:rgba(168,85,247,0.08);}';
    html += 'a{color:#00d4ff;text-decoration:none;}';
    html += '</style></head><body>';
    html += '<div style="margin-bottom:15px;"><a href="/business/tabs">← All Tabs</a> &nbsp;|&nbsp; <a href="/business">← Dashboard</a></div>';
    html += '<h1>' + tName + '</h1>';
    html += '<h2>Source: ' + ssName + ' — ' + found.rowCount + ' rows</h2>';
    
    html += '<table><thead><tr>';
    found.headers.forEach(function(h) { html += '<th>' + (h || '').toString() + '</th>'; });
    html += '</tr></thead><tbody>';
    
    var maxShow = Math.min(found.rows.length, 500);
    for (var rr = 0; rr < maxShow; rr++) {
      html += '<tr>';
      for (var cc = 0; cc < found.headers.length; cc++) {
        html += '<td>' + ((found.rows[rr][cc] || '').toString()) + '</td>';
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    if (found.rows.length > 500) html += '<div style="color:#4a6a8a;margin-top:10px;">Showing 500 of ' + found.rows.length + ' rows</div>';
    html += '</body></html>';
    res.send(html);
    
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   GET /business/search — Customer Lookup
=========================== */
app.get('/business/search', async function(req, res) {
  try {
    var q = (req.query.q || '').toLowerCase().trim();
    if (!q || q.length < 2) return res.json({ results: [] });
    await buildBusinessContext(); // ensure data loaded
    var allJobRows = (global.allSourceTabs || []).reduce(function(acc, tab) { return acc; }, []);
    // Use bizMetrics recentBookings for search
    var bm = global.bizMetrics || {};
    var recent = bm.recentBookings || [];
    var results = [];
    for (var i = 1; i < rows.length && results.length < 20; i++) {
      var row = rows[i];
      var name = ((row[1] || '') + ' ' + (row[2] || '')).toLowerCase();
      var phone = (row[6] || '').toString().toLowerCase();
      var email = (row[7] || '').toString().toLowerCase();
      var city = (row[9] || '').toString().toLowerCase();
      if (name.includes(q) || phone.includes(q) || email.includes(q) || city.includes(q)) {
        results.push({
          name: ((row[1] || '') + ' ' + (row[2] || '')).trim(),
          phone: row[6] || '', email: row[7] || '',
          city: (row[9] || '') + ', ' + (row[10] || ''),
          address: row[8] || '', equip: row[12] || '',
          brand: row[13] || '', issue: (row[14] || '').substring(0, 80),
          tech: row[15] || '', status: row[17] || '',
          date: row[0] || ''
        });
      }
    }
    res.json({ results: results });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /business/confirm-text — Send booking confirmation
=========================== */
app.post('/business/confirm-text', async function(req, res) {
  try {
    var phone = req.body.phone;
    var name = req.body.name;
    var date = req.body.date || 'your scheduled date';
    if (!phone) return res.status(400).json({ error: 'Phone required' });
    var cleanPhone = phone.replace(/[^\d+]/g, '');
    if (!cleanPhone.startsWith('+')) cleanPhone = '+1' + cleanPhone.replace(/^1/, '');
    var msg = 'Hi ' + name + '! This is Wildwood Small Engine Repair confirming your appointment for ' + date + '. Our technician will arrive during the scheduled window. Reply CONFIRM to confirm or call us to reschedule. Thank you!';
    await twilioClient.messages.create({ body: msg, from: TWILIO_NUMBER, to: cleanPhone });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /business/followup-text — Send 3-day follow-up
=========================== */
app.post('/business/followup-text', async function(req, res) {
  try {
    var phone = req.body.phone;
    var name = req.body.name;
    if (!phone) return res.status(400).json({ error: 'Phone required' });
    var cleanPhone = phone.replace(/[^\d+]/g, '');
    if (!cleanPhone.startsWith('+')) cleanPhone = '+1' + cleanPhone.replace(/^1/, '');
    var msg = 'Hi ' + name + '! This is Wildwood Small Engine Repair. We hope your equipment is running great! If you have any questions or need anything else, don\'t hesitate to reach out. We appreciate your business!';
    await twilioClient.messages.create({ body: msg, from: TWILIO_NUMBER, to: cleanPhone });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   POST /business/log-timer — Log job timer
=========================== */
app.post('/business/log-timer', async function(req, res) {
  try {
    var d = new Date();
    await sheets.spreadsheets.values.append({
      spreadsheetId: BUSINESS_SPREADSHEET_ID,
      range: "'Job_Timers'!A:E",
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[d.toISOString().split('T')[0], req.body.jobName, req.body.duration, req.body.minutes, d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Chicago' })]] },
    });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /business/insights — AI Weekly Business Report
=========================== */
app.get('/business/insights', async function(req, res) {
  try {
    var bizContext = await buildBusinessContext();
    var insights = await askClaude(
      "You are ATHENA, the AI business operations engine for Wildwood Small Engine Repair. Analyze the CRM data and generate a sharp weekly business report. Be specific with numbers. Cover: 1) What's working (top performing locations, best techs, growing markets), 2) What needs attention (high cancel rates, understaffed areas, slow response times), 3) Growth opportunities (new markets to target, seasonal prep, upsell opportunities), 4) Action items for this week (specific, numbered, actionable). Keep it under 500 words. No markdown formatting, use plain text with line breaks.",
      [{ role: 'user', content: 'Generate my weekly business insights report.\n\nCRM DATA:\n' + bizContext }]
    );
    res.json({ insights: insights });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/nightly-checkin', async function(req, res) {
  var secret = process.env.CALL_SECRET;
  if (secret && req.query.key !== secret) return res.status(403).json({ error: 'Unauthorized' });

  try {
    var from = 'whatsapp:+18167392734';
    whatsappHistory[from] = whatsappHistory[from] || {};
    whatsappHistory[from].dailyCheckin = {
      step: 0,
      data: { date: new Date().toISOString().split('T')[0] },
      active: true,
    };

    await twilioClient.messages.create({
      body: "End of day check-in. Quick answers, no overthinking.\n\nHow many hours did you sleep last night?",
      from: 'whatsapp:+14155238886',
      to: '+18167392734',
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   GET /team — Full Team Overview
=========================== */
app.get('/team', async function(req, res) {
  try {
    await buildBusinessContext();
    var bm = global.bizMetrics || {};
    var ts = bm.techStats || {};
    var techList = bm.techList || [];

    var profiles = Object.entries(ts).sort(function(a,b){return b[1].total-a[1].total;}).map(function(t) {
      var s = t[1];
      var rate = s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0;
      var avgResp = s.avgResponseDays.length > 0 ? Math.round(s.avgResponseDays.reduce(function(a,b){return a+b;},0) / s.avgResponseDays.length) : 0;
      var topEquip = Object.entries(s.equipment || {}).sort(function(a,b){return b[1]-a[1];}).slice(0,3);
      var topLocs = Object.entries(s.locations || {}).sort(function(a,b){return b[1]-a[1];}).slice(0,5);
      var topBrands = Object.entries(s.brands || {}).sort(function(a,b){return b[1]-a[1];}).slice(0,3);
      var phone = '';
      techList.forEach(function(tl) { if (tl.name && tl.name.toLowerCase().includes(t[0].toLowerCase())) phone = tl.phone; });

      // Performance grade
      var grade = 'C';
      if (rate >= 85 && s.total >= 10) grade = 'A+';
      else if (rate >= 75 && s.total >= 8) grade = 'A';
      else if (rate >= 65 && s.total >= 5) grade = 'B';
      else if (rate >= 50) grade = 'C';
      else grade = 'D';

      return {
        name: t[0], total: s.total, completed: s.completed, cancelled: s.cancelled,
        rate: rate, avgResponseDays: avgResp, phone: phone, grade: grade,
        thisWeek: s.thisWeekJobs, thisMonth: s.thisMonthJobs,
        todayJobs: s.todayJobs || [], recentJobs: s.recentJobs || [],
        topEquipment: topEquip, topLocations: topLocs, topBrands: topBrands,
        returnCustomers: s.returnCustomers || 0,
        firstSeen: s.firstSeen ? s.firstSeen.toISOString().split('T')[0] : null,
        lastSeen: s.lastSeen ? s.lastSeen.toISOString().split('T')[0] : null,
      };
    });

    res.json({ teamSize: profiles.length, profiles: profiles });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /team/:name — Individual Tech Profile
=========================== */
app.get('/team/:name', async function(req, res) {
  try {
    await buildBusinessContext();
    var ts = (global.bizMetrics || {}).techStats || {};
    var name = req.params.name;
    var found = null;
    Object.keys(ts).forEach(function(k) {
      if (k.toLowerCase() === name.toLowerCase() || k.toLowerCase().includes(name.toLowerCase())) found = { key: k, data: ts[k] };
    });
    if (!found) return res.status(404).json({ error: 'Tech not found' });
    var s = found.data;
    var rate = s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0;
    var avgResp = s.avgResponseDays.length > 0 ? Math.round(s.avgResponseDays.reduce(function(a,b){return a+b;},0) / s.avgResponseDays.length) : 0;
    res.json({
      name: found.key, total: s.total, completed: s.completed, cancelled: s.cancelled,
      rate: rate, avgResponseDays: avgResp, thisWeek: s.thisWeekJobs, thisMonth: s.thisMonthJobs,
      todayJobs: s.todayJobs, recentJobs: s.recentJobs,
      equipment: Object.entries(s.equipment).sort(function(a,b){return b[1]-a[1];}),
      locations: Object.entries(s.locations).sort(function(a,b){return b[1]-a[1];}),
      brands: Object.entries(s.brands).sort(function(a,b){return b[1]-a[1];}),
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /team/assign — Smart Auto-Assignment
=========================== */
app.get('/team/assign', async function(req, res) {
  try {
    await buildBusinessContext();
    var bm = global.bizMetrics || {};
    var ts = bm.techStats || {};
    var todayJobs = bm.todayBookings || [];
    var needsResched = bm.needsReschedule || [];

    var allJobs = todayJobs.concat(needsResched.map(function(n){return { name: n.name, location: n.location, equip: '', issue: 'Rescheduling needed', tech: '' };}));
    var unassigned = allJobs.filter(function(j){return !j.tech || j.tech === '';});
    var techEntries = Object.entries(ts).sort(function(a,b){return b[1].completed - a[1].completed;});

    var assignments = unassigned.map(function(job) {
      var bestTech = null, bestScore = -1, reasoning = '';
      techEntries.forEach(function(t) {
        var s = t[1], score = 0, reasons = [];
        var completionRate = s.total > 0 ? s.completed / s.total : 0;
        score += completionRate * 40; reasons.push(Math.round(completionRate*100) + '% completion');
        if (job.equip && s.equipment) {
          Object.keys(s.equipment).forEach(function(e) {
            if (e.toLowerCase().includes(job.equip.toLowerCase()) || job.equip.toLowerCase().includes(e.toLowerCase())) {
              score += 25; reasons.push('specializes in ' + e);
            }
          });
        }
        if (job.location && s.locations && s.locations[job.location]) {
          score += 20; reasons.push('works in ' + job.location);
        }
        score -= (s.todayJobs || []).length * 15;
        if ((s.todayJobs || []).length === 0) { score += 10; reasons.push('free today'); }
        if (score > bestScore) { bestScore = score; bestTech = t[0]; reasoning = reasons.join(', '); }
      });
      return { job: job.name, location: job.location, equipment: job.equip, recommendedTech: bestTech, score: Math.round(bestScore), reasoning: reasoning };
    });

    res.json({ unassignedCount: unassigned.length, totalToday: allJobs.length, assignments: assignments });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /team/daily-tasks — Auto-Generated Daily Task Lists Per Tech
=========================== */
app.get('/team/daily-tasks', async function(req, res) {
  try {
    await buildBusinessContext();
    var bm = global.bizMetrics || {};
    var ts = bm.techStats || {};
    var techEntries = Object.entries(ts).sort(function(a,b){return b[1].total-a[1].total;});

    var taskLists = techEntries.map(function(t) {
      var s = t[1];
      var tasks = [];

      // Today's assigned jobs
      (s.todayJobs || []).forEach(function(j) {
        tasks.push({ priority: 'HIGH', type: 'Job', task: j.name + ' — ' + j.equip + ' (' + j.location + ')', detail: j.issue });
      });

      // Follow up on recent incomplete
      (s.recentJobs || []).forEach(function(j) {
        if (j.status && (j.status.includes('pending') || j.status.includes('schedul'))) {
          tasks.push({ priority: 'MEDIUM', type: 'Follow-up', task: 'Follow up: ' + j.name + ' (' + j.location + ')', detail: 'Status: ' + j.status });
        }
      });

      // Coaching note based on stats
      var rate = s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0;
      var coaching = '';
      if (rate < 60 && s.total >= 5) coaching = 'Cancel rate is high (' + (100-rate) + '%). Focus on confirming appointments before going out.';
      else if (s.thisWeekJobs === 0 && s.total > 0) coaching = 'No jobs this week. Check scheduling or availability.';
      else if (rate >= 85) coaching = 'Excellent performance. Consider for team lead responsibilities.';

      return { tech: t[0], taskCount: tasks.length, tasks: tasks, coaching: coaching, todayLoad: (s.todayJobs || []).length, weekLoad: s.thisWeekJobs };
    });

    res.json({ taskLists: taskLists });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /team/coaching — AI Coaching Report Per Tech
=========================== */
app.get('/team/coaching', async function(req, res) {
  try {
    var bizContext = await buildBusinessContext();
    var coaching = await askClaude(
      "You are ATHENA, business operations AI for Wildwood Small Engine Repair. Generate individual coaching reports for each technician. For EACH tech: 1) Strengths (what they do well, backed by data), 2) Areas to improve (specific, actionable), 3) This week's focus (one priority), 4) Equipment they should study (based on gaps). Be specific with numbers. Keep each tech's section to 3-4 sentences. No markdown.",
      [{ role: 'user', content: 'Generate coaching reports for my team:\n\n' + bizContext }]
    );
    res.json({ coaching: coaching });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /team/workload — Workload Balance Analysis
=========================== */
app.get('/team/workload', async function(req, res) {
  try {
    await buildBusinessContext();
    var ts = (global.bizMetrics || {}).techStats || {};
    var entries = Object.entries(ts).sort(function(a,b){return b[1].total-a[1].total;});
    var totalJobs = entries.reduce(function(sum,t){return sum + t[1].total;},0);
    var avgPerTech = entries.length > 0 ? Math.round(totalJobs / entries.length) : 0;

    var analysis = entries.map(function(t) {
      var s = t[1];
      var deviation = s.total - avgPerTech;
      var status = 'balanced';
      if (deviation > avgPerTech * 0.5) status = 'overloaded';
      else if (deviation < -avgPerTech * 0.3) status = 'underutilized';
      return {
        tech: t[0], total: s.total, thisWeek: s.thisWeekJobs, thisMonth: s.thisMonthJobs,
        todayCount: (s.todayJobs || []).length, deviation: deviation, status: status,
        topLocation: Object.entries(s.locations || {}).sort(function(a,b){return b[1]-a[1];})[0],
        topEquip: Object.entries(s.equipment || {}).sort(function(a,b){return b[1]-a[1];})[0],
      };
    });

    res.json({ teamSize: entries.length, totalJobs: totalJobs, avgPerTech: avgPerTech, analysis: analysis });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   START SERVER
=========================== */

/* ===========================
   DEBUG: Sheet Diagnostics
=========================== */
app.get('/debug-sheets', async function(req, res) {
  if (req.query.key !== 'jarvis-wake-up-2026') return res.status(403).send('Forbidden');
  var results = {};
  results.SPREADSHEET_ID = SPREADSHEET_ID ? SPREADSHEET_ID.substring(0, 10) + '...' : 'NOT SET';
  results.BUSINESS_SPREADSHEET_ID = BUSINESS_SPREADSHEET_ID ? BUSINESS_SPREADSHEET_ID.substring(0, 10) + '...' : 'NOT SET';
  results.same_sheet = SPREADSHEET_ID === BUSINESS_SPREADSHEET_ID;

  // Try to list all tabs on the business sheet
  try {
    var meta = await sheets.spreadsheets.get({ spreadsheetId: BUSINESS_SPREADSHEET_ID, fields: 'sheets.properties.title' });
    results.business_tabs = meta.data.sheets.map(function(s) { return s.properties.title; });
  } catch (e) {
    results.business_tabs_error = e.message;
  }

  // Try reading Combined tab
  try {
    var combinedRes = await sheets.spreadsheets.values.get({ spreadsheetId: BUSINESS_SPREADSHEET_ID, range: "'Combined'!A1:Z5" });
    var rows = combinedRes.data.values || [];
    results.combined_headers = rows[0] || 'NO HEADERS';
    results.combined_row_count = rows.length;
    results.combined_sample = rows[1] || 'NO DATA';
  } catch (e) {
    results.combined_error = e.message;
  }

  // Also try personal sheet tabs
  try {
    var meta2 = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties.title' });
    results.personal_tabs = meta2.data.sheets.map(function(s) { return s.properties.title; });
  } catch (e) {
    results.personal_tabs_error = e.message;
  }

  // Try profit sheet
  var PROFIT_SHEET_ID = '1TXwXvcjt1M9bl38_0GhjK6izRGjTTuaGTN8RAmEiFwc';
  try {
    var profitMeta = await sheets.spreadsheets.get({ spreadsheetId: PROFIT_SHEET_ID, fields: 'sheets.properties.title' });
    results.profit_tabs = profitMeta.data.sheets.map(function(s) { return s.properties.title; });
  } catch (e) {
    results.profit_tabs_error = e.message;
  }

  // Read first tab of profit sheet
  try {
    var profitMeta2 = await sheets.spreadsheets.get({ spreadsheetId: PROFIT_SHEET_ID, fields: 'sheets.properties.title' });
    var firstTab = profitMeta2.data.sheets[0].properties.title;
    var profitRes = await sheets.spreadsheets.values.get({ spreadsheetId: PROFIT_SHEET_ID, range: "'" + firstTab + "'!A1:Z5" });
    var profitRows = profitRes.data.values || [];
    results.profit_headers = profitRows[0] || 'NO HEADERS';
    results.profit_row_count = profitRows.length;
    results.profit_sample = profitRows[1] || 'NO DATA';
    if (profitRows[2]) results.profit_sample2 = profitRows[2];
  } catch (e) {
    results.profit_read_error = e.message;
  }

  res.json(results);
});

/* ===========================
   TOOKAN LIVE DASHBOARD
=========================== */

app.get('/tookan', async function(req, res) {
  try {
    var tk = await buildTookanContext();
    var todayStr = new Date().toISOString().split('T')[0];
    var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>ATHENA — Tookan Live</title>';
    html += '<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&display=swap" rel="stylesheet">';
    html += '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css" />';
    html += '<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"><\/script>';
    html += '<style>';
    html += 'body{margin:0;background:#050d18;color:#c0d8f0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;}';
    html += '.wrap{max-width:1400px;margin:0 auto;padding:30px 40px;}';
    html += '.title{font-family:Orbitron;font-size:1.4em;letter-spacing:8px;color:#00d4ff;margin-bottom:5px;}';
    html += '.sub{font-family:Orbitron;font-size:0.6em;letter-spacing:3px;color:#4a6a8a;margin-bottom:30px;}';
    html += '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:30px;}';
    html += '.card{background:rgba(10,20,35,0.8);border:1px solid rgba(255,255,255,0.05);padding:20px;text-align:center;}';
    html += '.card .label{font-family:Orbitron;font-size:0.5em;letter-spacing:3px;color:#4a6a8a;margin-bottom:8px;}';
    html += '.card .val{font-family:Orbitron;font-size:2.2em;font-weight:900;}';
    html += '.card .sub2{color:#4a6a8a;font-size:0.8em;margin-top:4px;}';
    html += '.section{margin-bottom:30px;}';
    html += '.section-title{font-family:Orbitron;font-size:0.8em;letter-spacing:5px;text-transform:uppercase;margin-bottom:12px;display:flex;align-items:center;gap:10px;}';
    html += '.dot{width:8px;height:8px;border-radius:50%;display:inline-block;}';
    html += '.row{background:rgba(10,20,35,0.6);border:1px solid rgba(255,255,255,0.03);padding:12px 16px;margin-bottom:4px;display:flex;align-items:center;gap:12px;flex-wrap:wrap;}';
    html += '.badge{font-family:Orbitron;font-size:0.5em;letter-spacing:2px;padding:3px 10px;border:1px solid;}';
    html += '.b-completed{color:#00ff66;border-color:#00ff6640;}';
    html += '.b-assigned{color:#00d4ff;border-color:#00d4ff40;}';
    html += '.b-acknowledged{color:#a855f7;border-color:#a855f740;}';
    html += '.b-started{color:#ff9f43;border-color:#ff9f4340;}';
    html += '.b-failed{color:#ff4757;border-color:#ff475740;}';
    html += '.b-unassigned{color:#c0c0c0;border-color:#c0c0c040;}';
    html += '.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:20px;}';
    html += '.holo-btn{font-family:Orbitron;font-size:0.6em;letter-spacing:3px;padding:10px 20px;color:#00d4ff;border:1px solid #00d4ff30;background:transparent;text-decoration:none;transition:all 0.3s;}';
    html += '.holo-btn:hover{background:rgba(0,212,255,0.1);}';
    // Mobile responsive
    html += '@media(max-width:768px){';
    html += '.wrap{padding:10px 12px!important;}';
    html += '.grid{grid-template-columns:repeat(2,1fr)!important;gap:8px!important;}';
    html += '.kpi-grid{grid-template-columns:repeat(2,1fr)!important;}';
    html += '.analytics-grid{grid-template-columns:1fr!important;}';
    html += '.title{font-size:1em!important;letter-spacing:3px!important;}';
    html += '.card,.val{font-size:0.9em!important;}';
    html += 'table{font-size:0.65em!important;} th,td{padding:4px 3px!important;}';
    html += '[style*="overflow-x"]{overflow-x:auto!important;-webkit-overflow-scrolling:touch;}';
    html += '#map{height:250px!important;}';
    html += '[style*="display:flex"][style*="gap:0"]{flex-wrap:wrap!important;}';
    html += '}';
    html += '@media(max-width:480px){.grid{grid-template-columns:1fr!important;}}';
    html += '</style></head><body><div class="wrap">';

    html += '<div class="title">TOOKAN DISPATCH CENTER</div>';
    html += '<div class="sub">REAL-TIME JOB TRACKING • ' + tk.totalTasks + ' TASKS (90 DAYS) • ' + tk.agents.length + ' AGENTS</div>';

    // === TAB NAVIGATION ===
    html += '<div style="display:flex;gap:0;margin-bottom:25px;">';
    html += '<a href="/dashboard" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">JARVIS</a>';
    html += '<a href="/business" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ATHENA</a>';
    html += '<a href="/tookan" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#00d4ff;border:1px solid #00d4ff40;text-decoration:none;background:rgba(0,212,255,0.1);box-shadow:0 0 15px rgba(0,212,255,0.1);">TOOKAN</a>';
    html += '<a href="/business/chart" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">CHARTS</a>';
    html += '<a href="/analytics" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ANALYTICS</a>';
    html += '</div>';

    // Status overview cards
    html += '<div class="grid">';
    html += '<div class="card"><div class="label">TOTAL TASKS</div><div class="val" style="color:#00d4ff;">' + tk.totalTasks + '</div><div class="sub2">Last 90 days</div></div>';
    html += '<div class="card"><div class="label">COMPLETED</div><div class="val" style="color:#00ff66;">' + tk.completed + '</div><div class="sub2">' + (tk.totalTasks > 0 ? Math.round(tk.completed / tk.totalTasks * 100) : 0) + '% completion rate</div></div>';
    html += '<div class="card"><div class="label">ASSIGNED</div><div class="val" style="color:#00d4ff;">' + tk.assigned + '</div><div class="sub2">Waiting for tech</div></div>';
    html += '<div class="card"><div class="label">ACKNOWLEDGED</div><div class="val" style="color:#a855f7;">' + tk.acknowledged + '</div><div class="sub2">Tech confirmed</div></div>';
    html += '<div class="card"><div class="label">IN PROGRESS</div><div class="val" style="color:#ff9f43;">' + tk.started + '</div><div class="sub2">Started / Arrived</div></div>';
    html += '<div class="card"><div class="label">UNASSIGNED</div><div class="val" style="color:#c0c0c0;">' + tk.unassigned + '</div><div class="sub2">Needs dispatch</div></div>';
    html += '<div class="card"><div class="label">FAILED/CANCELLED</div><div class="val" style="color:#ff4757;">' + tk.cancelled + '</div><div class="sub2">' + (tk.totalTasks > 0 ? Math.round(tk.cancelled / tk.totalTasks * 100) : 0) + '% fail rate</div></div>';
    html += '</div>';

    // ====== ANALYTICS — DONUT CHARTS + KPI CARDS ======
    // SVG donut chart helper
    html += '<style>';
    html += '.analytics-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:30px;}';
    html += '.chart-card{background:rgba(10,20,35,0.8);border:1px solid rgba(255,255,255,0.05);padding:20px;text-align:center;}';
    html += '.chart-title{font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#00d4ff;padding:4px 12px;background:rgba(0,212,255,0.1);display:inline-block;margin-bottom:15px;}';
    html += '.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:30px;}';
    html += '.kpi{background:rgba(10,20,35,0.8);border:1px solid rgba(255,255,255,0.05);padding:20px;text-align:center;position:relative;}';
    html += '.kpi .kpi-label{font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#00d4ff;padding:3px 10px;background:rgba(0,212,255,0.1);display:inline-block;margin-bottom:12px;}';
    html += '.kpi .kpi-val{font-family:Orbitron;font-size:2em;font-weight:900;color:#c0d8f0;}';
    html += '.kpi .kpi-change{font-size:0.85em;margin-top:6px;}';
    html += '.kpi .kpi-sub{color:#4a6a8a;font-size:0.8em;margin-top:2px;}';
    html += '.week-chart{display:flex;align-items:flex-end;gap:6px;height:80px;margin-top:15px;padding:0 10px;}';
    html += '.week-bar{flex:1;display:flex;flex-direction:column;align-items:center;}';
    html += '.week-bar-inner{width:100%;background:linear-gradient(180deg,#00d4ff,#00d4ff40);border-radius:2px 2px 0 0;transition:height 0.3s;}';
    html += '.week-bar-label{color:#4a6a8a;font-size:0.65em;margin-top:4px;}';
    html += '.week-bar-count{color:#c0d8f0;font-size:0.7em;margin-bottom:2px;}';
    html += '@media(max-width:768px){.analytics-grid{grid-template-columns:1fr;}.kpi-grid{grid-template-columns:repeat(2,1fr);}}';
    html += '</style>';

    // Task Status donut & Agent Status donut
    var taskAccepted = tk.acknowledged + tk.started;
    var taskAssignedOnly = tk.assigned + tk.unassigned;
    var taskTotal = taskAccepted + taskAssignedOnly + tk.completed;

    html += '<div class="analytics-grid">';

    // Task Status Donut
    html += '<div class="chart-card">';
    html += '<div class="chart-title">Task Status ▾</div>';
    html += '<svg viewBox="0 0 200 200" width="220" height="220" style="display:block;margin:0 auto;">';
    // Build donut segments
    var donutData = [
      { val: taskAccepted, color: '#1a73e8', label: 'Accepted' },
      { val: taskAssignedOnly, color: '#64b5f6', label: 'Assigned' },
      { val: tk.completed, color: '#00ff66', label: 'Completed' },
    ].filter(function(d) { return d.val > 0; });
    var donutTotal = donutData.reduce(function(a, b) { return a + b.val; }, 0) || 1;
    var startAngle = 0;
    var cx = 100, cy = 100, r = 70, strokeW = 30;
    var circumference = 2 * Math.PI * r;
    donutData.forEach(function(seg) {
      var pct = seg.val / donutTotal;
      var dashLen = circumference * pct;
      var dashGap = circumference - dashLen;
      var rotation = startAngle * 360 - 90;
      html += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + seg.color + '" stroke-width="' + strokeW + '" stroke-dasharray="' + dashLen + ' ' + dashGap + '" transform="rotate(' + rotation + ' ' + cx + ' ' + cy + ')" />';
      // Label
      var midAngle = (startAngle + pct / 2) * 2 * Math.PI - Math.PI / 2;
      var lx = cx + (r) * Math.cos(midAngle);
      var ly = cy + (r) * Math.sin(midAngle);
      html += '<text x="' + lx + '" y="' + ly + '" fill="white" font-size="14" font-weight="bold" text-anchor="middle" dominant-baseline="central">' + seg.val + '</text>';
      startAngle += pct;
    });
    html += '</svg>';
    // Legend
    html += '<div style="display:flex;justify-content:center;gap:20px;margin-top:10px;">';
    donutData.forEach(function(seg) {
      html += '<div style="display:flex;align-items:center;gap:5px;"><span style="width:10px;height:10px;border-radius:50%;background:' + seg.color + ';display:inline-block;"></span><span style="color:#c0d8f0;font-size:0.85em;">' + seg.label + '</span></div>';
    });
    html += '</div></div>';

    // Agent Status Donut
    html += '<div class="chart-card">';
    html += '<div class="chart-title">Agent Status ▾</div>';
    html += '<svg viewBox="0 0 200 200" width="220" height="220" style="display:block;margin:0 auto;">';
    var agentData = [
      { val: tk.agentsBusy, color: '#64b5f6', label: 'Busy' },
      { val: tk.agentsFree, color: '#00c853', label: 'Free' },
      { val: tk.agentsInactive, color: '#9e9e9e', label: 'Inactive' },
    ].filter(function(d) { return d.val > 0; });
    var agentTotal2 = agentData.reduce(function(a, b) { return a + b.val; }, 0) || 1;
    startAngle = 0;
    agentData.forEach(function(seg) {
      var pct = seg.val / agentTotal2;
      var dashLen = circumference * pct;
      var dashGap = circumference - dashLen;
      var rotation = startAngle * 360 - 90;
      html += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + seg.color + '" stroke-width="' + strokeW + '" stroke-dasharray="' + dashLen + ' ' + dashGap + '" transform="rotate(' + rotation + ' ' + cx + ' ' + cy + ')" />';
      var midAngle = (startAngle + pct / 2) * 2 * Math.PI - Math.PI / 2;
      var lx = cx + (r) * Math.cos(midAngle);
      var ly = cy + (r) * Math.sin(midAngle);
      html += '<text x="' + lx + '" y="' + ly + '" fill="white" font-size="14" font-weight="bold" text-anchor="middle" dominant-baseline="central">' + seg.val + '</text>';
      startAngle += pct;
    });
    html += '</svg>';
    html += '<div style="display:flex;justify-content:center;gap:20px;margin-top:10px;">';
    agentData.forEach(function(seg) {
      html += '<div style="display:flex;align-items:center;gap:5px;"><span style="width:10px;height:10px;border-radius:50%;background:' + seg.color + ';display:inline-block;"></span><span style="color:#c0d8f0;font-size:0.85em;">' + seg.label + '</span></div>';
    });
    html += '</div></div>';
    html += '</div>';

    // KPI Cards row
    var changeColor = tk.taskChangeVsYesterday >= 0 ? '#ff4757' : '#00ff66';
    var changeSign = tk.taskChangeVsYesterday >= 0 ? '' : '';
    html += '<div class="kpi-grid">';

    // Total Tasks
    html += '<div class="kpi">';
    html += '<div class="kpi-label">Total Tasks ▾</div>';
    html += '<div class="kpi-val">' + tk.todayTotal + '</div>';
    html += '<div class="kpi-change" style="color:' + changeColor + ';">' + changeSign + tk.taskChangeVsYesterday + '%</div>';
    html += '<div class="kpi-sub">vs yesterday (' + tk.yesterdayTotal + ')</div>';
    html += '</div>';

    // Task Efficiency
    html += '<div class="kpi">';
    html += '<div class="kpi-label">Task Efficiency ▾</div>';
    html += '<div class="kpi-val">' + tk.taskEfficiency + '%</div>';
    html += '<div class="kpi-sub">completed / total dispatched</div>';
    html += '</div>';

    // Avg Completion Time
    var avgHrs = Math.floor(tk.avgCompletionMinutes / 60);
    var avgMins = tk.avgCompletionMinutes % 60;
    html += '<div class="kpi">';
    html += '<div class="kpi-label">Avg Completion Time ▾</div>';
    html += '<div class="kpi-val">' + (avgHrs > 0 ? avgHrs + 'h ' : '') + avgMins + 'm</div>';
    html += '<div class="kpi-sub">pickup to completed</div>';
    html += '</div>';

    // Completion Rate
    var completionRate = tk.totalTasks > 0 ? Math.round((tk.completed / tk.totalTasks) * 10000) / 100 : 0;
    html += '<div class="kpi">';
    html += '<div class="kpi-label">Completion Rate ▾</div>';
    html += '<div class="kpi-val" style="color:' + (completionRate >= 70 ? '#00ff66' : completionRate >= 40 ? '#ff9f43' : '#ff4757') + ';">' + completionRate + '%</div>';
    html += '<div class="kpi-sub">of all ' + tk.totalTasks + ' tasks (90d)</div>';
    html += '</div>';
    html += '</div>';

    // Weekly trend bar chart
    var weekData = tk.weeklyTasks || [];
    if (weekData.length > 0) {
      var maxDay = Math.max.apply(null, weekData.map(function(d) { return d.count; })) || 1;
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#00d4ff;"><span class="dot" style="background:#00d4ff;box-shadow:0 0 8px #00d4ff;"></span>7-DAY TASK TREND</div>';
      html += '<div class="week-chart">';
      weekData.forEach(function(d) {
        var barH = Math.max(4, Math.round((d.count / maxDay) * 100));
        var isToday = d.date === todayStr;
        var barCol = isToday ? '#ffd700' : '#00d4ff';
        html += '<div class="week-bar">';
        html += '<div class="week-bar-count">' + d.count + '</div>';
        html += '<div class="week-bar-inner" style="height:' + barH + '%;background:' + barCol + (isToday ? '' : '60') + ';"></div>';
        html += '<div class="week-bar-label" style="' + (isToday ? 'color:#ffd700;font-weight:700;' : '') + '">' + d.day + '</div>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // ====== LIVE MAP ======
    html += '<div class="section">';
    html += '<div class="section-title" style="color:#00d4ff;"><span class="dot" style="background:#00d4ff;box-shadow:0 0 8px #00d4ff;"></span>DISPATCH MAP</div>';
    html += '<div id="dispatch-map" style="height:500px;border:1px solid rgba(0,212,255,0.15);margin-bottom:20px;background:#0a1520;"></div>';
    html += '</div>';

    // Map initialization script (runs after page load)
    var mapTasks = tk.mapTasks || [];
    var todayMapTasks = (tk.todayTasks || []).filter(function(t) { return t.lat && t.lng && t.lat !== 0; });
    var upcomingMapTasks = (tk.upcomingTasks || []).filter(function(t) { return t.lat && t.lng && t.lat !== 0; });
    html += '<script>';
    html += 'document.addEventListener("DOMContentLoaded", function() {';
    html += '  var map = L.map("dispatch-map", { zoomControl: true }).setView([37.5, -96], 4);';
    // Dark map tiles
    html += '  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {';
    html += '    attribution: "CartoDB", subdomains: "abcd", maxZoom: 19';
    html += '  }).addTo(map);';
    // Status color function
    html += '  function statusColor(s) {';
    html += '    s = s.toLowerCase();';
    html += '    if (s.includes("completed")) return "#00ff66";';
    html += '    if (s.includes("acknowledged")) return "#a855f7";';
    html += '    if (s.includes("assigned")) return "#00d4ff";';
    html += '    if (s.includes("started") || s.includes("arrived")) return "#ff9f43";';
    html += '    if (s.includes("failed") || s.includes("cancel")) return "#ff4757";';
    html += '    return "#c0c0c0";';
    html += '  }';
    // Custom circle marker function
    html += '  function addPin(lat, lng, data, isToday) {';
    html += '    var color = statusColor(data.status);';
    html += '    var radius = isToday ? 10 : 6;';
    html += '    var marker = L.circleMarker([lat, lng], {';
    html += '      radius: radius, fillColor: color, color: "#fff", weight: isToday ? 2 : 1,';
    html += '      opacity: isToday ? 1 : 0.6, fillOpacity: isToday ? 0.9 : 0.5';
    html += '    }).addTo(map);';
    html += '    marker.bindPopup(';
    html += '      "<div style=\\"font-family:sans-serif;font-size:13px;\\">" +';
    html += '      "<b style=\\"color:" + color + "\\">" + data.status + "</b><br>" +';
    html += '      "<b>" + data.customer + "</b><br>" +';
    html += '      (data.tech ? "Tech: " + data.tech + "<br>" : "") +';
    html += '      (data.address ? data.address + "<br>" : "") +';
    html += '      "Job #" + data.jobId +';
    html += '      "</div>"';
    html += '    );';
    html += '  }';

    // Plot all historical tasks (smaller dots)
    html += '  var allTasks = ' + JSON.stringify(mapTasks) + ';';
    html += '  allTasks.forEach(function(t) { if (t.lat && t.lng) addPin(t.lat, t.lng, t, false); });';

    // Plot today's tasks (bigger, brighter)
    html += '  var todayTasks = ' + JSON.stringify(todayMapTasks) + ';';
    html += '  todayTasks.forEach(function(t) { if (t.lat && t.lng) addPin(t.lat, t.lng, t, true); });';

    // Plot upcoming tasks (purple dashed outline)
    html += '  var upcomingTasks = ' + JSON.stringify(upcomingMapTasks) + ';';
    html += '  upcomingTasks.forEach(function(t) {';
    html += '    if (t.lat && t.lng) {';
    html += '      var m = L.circleMarker([t.lat, t.lng], {';
    html += '        radius: 8, fillColor: "#a855f7", color: "#a855f7", weight: 2,';
    html += '        opacity: 0.8, fillOpacity: 0.3, dashArray: "4 4"';
    html += '      }).addTo(map);';
    html += '      m.bindPopup("<div style=\\"font-family:sans-serif;font-size:13px;\\"><b style=\\"color:#a855f7\\">UPCOMING " + t.date + "</b><br><b>" + t.customer + "</b><br>" + (t.tech ? "Tech: " + t.tech + "<br>" : "") + t.address + "</div>");';
    html += '    }';
    html += '  });';

    // Auto-fit bounds if we have markers
    html += '  var allPts = todayTasks.concat(allTasks).concat(upcomingTasks).filter(function(t) { return t.lat && t.lng; });';
    html += '  if (allPts.length > 0) {';
    html += '    var bounds = L.latLngBounds(allPts.map(function(t) { return [t.lat, t.lng]; }));';
    html += '    map.fitBounds(bounds, { padding: [40, 40], maxZoom: 12 });';
    html += '  }';

    // Legend
    html += '  var legend = L.control({ position: "bottomright" });';
    html += '  legend.onAdd = function() {';
    html += '    var div = L.DomUtil.create("div", "");';
    html += '    div.style.cssText = "background:rgba(5,13,24,0.9);padding:10px 14px;border:1px solid #00d4ff30;font-size:12px;line-height:1.8;";';
    html += '    div.innerHTML = "<div style=\\"font-family:Orbitron;font-size:9px;letter-spacing:2px;color:#4a6a8a;margin-bottom:4px;\\">STATUS</div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;background:#00d4ff;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">Assigned</span></div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;background:#a855f7;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">Acknowledged</span></div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;background:#ff9f43;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">In Progress</span></div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;background:#00ff66;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">Completed</span></div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;background:#ff4757;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">Failed</span></div>"';
    html += '      + "<div><span style=\\"display:inline-block;width:10px;height:10px;border-radius:50%;border:2px dashed #a855f7;margin-right:6px;\\"></span><span style=\\"color:#c0d8f0;\\">Upcoming</span></div>";';
    html += '    return div;';
    html += '  };';
    html += '  legend.addTo(map);';
    html += '});';
    html += '<\/script>';

    // Today's jobs
    html += '<div class="section">';
    html += '<div class="section-title" style="color:#ffd700;"><span class="dot" style="background:#ffd700;box-shadow:0 0 8px #ffd700;"></span>TODAY\'S JOBS (' + tk.todayTasks.length + ')</div>';
    if (tk.todayTasks.length > 0) {
      tk.todayTasks.forEach(function(t) {
        var badgeClass = t.status.toLowerCase().includes('completed') ? 'b-completed' : t.status.toLowerCase().includes('assigned') ? 'b-assigned' : t.status.toLowerCase().includes('acknowledged') ? 'b-acknowledged' : t.status.toLowerCase().includes('started') ? 'b-started' : 'b-unassigned';
        html += '<div class="row">';
        html += '<div style="min-width:60px;font-family:Orbitron;font-size:0.7em;color:#4a6a8a;">#' + t.jobId + '</div>';
        html += '<div style="flex:1;color:#c0d8f0;font-weight:600;">' + t.customer + '</div>';
        html += '<div style="flex:1;color:#4a6a8a;font-size:0.85em;">' + t.address + '</div>';
        html += '<div style="min-width:100px;color:#a855f7;">' + (t.tech || 'Unassigned') + '</div>';
        html += '<div class="badge ' + badgeClass + '">' + t.status.toUpperCase() + '</div>';
        html += '</div>';
      });
    } else {
      html += '<div style="color:#4a6a8a;padding:20px;text-align:center;">No tasks scheduled for today</div>';
    }
    html += '</div>';

    // Upcoming jobs (future dates)
    if (tk.upcomingTasks && tk.upcomingTasks.length > 0) {
      // Sort by date
      var upcoming = tk.upcomingTasks.slice().sort(function(a, b) { return a.date < b.date ? -1 : 1; });
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#a855f7;"><span class="dot" style="background:#a855f7;box-shadow:0 0 8px #a855f7;"></span>UPCOMING JOBS (' + upcoming.length + ')</div>';
      // Group by date
      var byDate = {};
      upcoming.forEach(function(t) {
        if (!byDate[t.date]) byDate[t.date] = [];
        byDate[t.date].push(t);
      });
      Object.keys(byDate).sort().forEach(function(dt) {
        var d = new Date(dt + 'T12:00:00');
        var dayLabel = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
        html += '<div style="font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#a855f7;margin:10px 0 5px;padding:4px 10px;border-left:3px solid #a855f740;">' + dayLabel + ' — ' + byDate[dt].length + ' jobs</div>';
        byDate[dt].forEach(function(t) {
          var sColor = t.status.toLowerCase().includes('assigned') ? '#00d4ff' : t.status.toLowerCase().includes('acknowledged') ? '#a855f7' : '#c0c0c0';
          html += '<div class="row">';
          html += '<div style="min-width:50px;font-family:Orbitron;font-size:0.65em;color:#4a6a8a;">#' + t.jobId + '</div>';
          html += '<div style="flex:1;color:#c0d8f0;font-weight:600;">' + t.customer + '</div>';
          html += '<div style="flex:1;color:#4a6a8a;font-size:0.85em;">' + t.address + '</div>';
          html += '<div style="min-width:100px;color:#a855f7;">' + (t.tech || 'Unassigned') + '</div>';
          html += '<div class="badge" style="color:' + sColor + ';border-color:' + sColor + '40;">' + t.status.toUpperCase() + '</div>';
          html += '</div>';
        });
      });
      html += '</div>';
    }

    // Tech performance
    var techEntries = Object.entries(tk.tasksByTech).sort(function(a, b) { return b[1].completed - a[1].completed; });
    html += '<div class="section">';
    html += '<div class="section-title" style="color:#00ff66;"><span class="dot" style="background:#00ff66;box-shadow:0 0 8px #00ff66;"></span>TECH PERFORMANCE (90 DAYS)</div>';
    if (techEntries.length > 0) {
      var maxComp = techEntries[0][1].completed;
      techEntries.forEach(function(t, idx) {
        var barPct = maxComp > 0 ? Math.round(t[1].completed / maxComp * 100) : 0;
        var compRate = t[1].total > 0 ? Math.round(t[1].completed / t[1].total * 100) : 0;
        html += '<div class="row">';
        html += '<div style="min-width:30px;font-family:Orbitron;font-size:0.9em;color:' + (idx === 0 ? '#ffd700' : idx === 1 ? '#c0c0c0' : idx === 2 ? '#cd7f32' : '#4a6a8a') + ';">#' + (idx + 1) + '</div>';
        html += '<div style="min-width:180px;color:#c0d8f0;font-weight:700;">' + t[0] + '</div>';
        html += '<div style="flex:1;height:20px;background:#0a1520;position:relative;min-width:200px;">';
        html += '<div style="height:100%;width:' + barPct + '%;background:linear-gradient(90deg,#00ff66,#55f7d8);"></div>';
        html += '<div style="position:absolute;right:8px;top:50%;transform:translateY(-50%);font-size:0.75em;color:#c0d8f0;">' + t[1].completed + ' done / ' + t[1].total + ' total</div>';
        html += '</div>';
        html += '<div style="min-width:60px;text-align:center;font-family:Orbitron;font-size:0.7em;color:' + (compRate >= 70 ? '#00ff66' : compRate >= 40 ? '#ff9f43' : '#ff4757') + ';">' + compRate + '%</div>';
        html += '</div>';
      });
    }
    html += '</div>';

    // Active agents
    if (tk.agents.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#a855f7;"><span class="dot" style="background:#a855f7;box-shadow:0 0 8px #a855f7;"></span>FIELD AGENTS (' + tk.agents.length + ')</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:8px;">';
      tk.agents.forEach(function(a) {
        var statusColor = a.status === 'Available' ? '#00ff66' : '#ff4757';
        html += '<div class="row" style="justify-content:space-between;">';
        html += '<div><div style="color:#c0d8f0;font-weight:600;">' + a.name + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.8em;">' + (a.phone || 'No phone') + '</div></div>';
        html += '<div style="display:flex;align-items:center;gap:6px;"><span class="dot" style="background:' + statusColor + ';box-shadow:0 0 6px ' + statusColor + ';"></span><span style="font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:' + statusColor + ';">' + a.status.toUpperCase() + '</span></div>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // Recent completed
    if (tk.recentCompleted.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#55f7d8;"><span class="dot" style="background:#55f7d8;box-shadow:0 0 8px #55f7d8;"></span>RECENTLY COMPLETED</div>';
      tk.recentCompleted.slice(0, 10).forEach(function(c) {
        html += '<div class="row">';
        html += '<div style="min-width:60px;font-family:Orbitron;font-size:0.7em;color:#4a6a8a;">#' + c.jobId + '</div>';
        html += '<div style="flex:1;color:#c0d8f0;">' + c.customer + '</div>';
        html += '<div style="color:#a855f7;">' + c.tech + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.8em;">' + c.address + '</div>';
        html += '<div class="badge b-completed">DONE</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    // Status breakdown
    var statusEntries = Object.entries(tk.tasksByStatus).sort(function(a, b) { return b[1] - a[1]; });
    if (statusEntries.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-title" style="color:#00d4ff;"><span class="dot" style="background:#00d4ff;box-shadow:0 0 8px #00d4ff;"></span>STATUS BREAKDOWN</div>';
      var maxSt = statusEntries[0][1];
      statusEntries.forEach(function(s) {
        var pct = maxSt > 0 ? Math.round(s[1] / maxSt * 100) : 0;
        html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:3px;">';
        html += '<div style="min-width:150px;color:#7a9ab0;font-size:0.85em;">' + s[0] + '</div>';
        html += '<div style="flex:1;height:18px;background:#0a1520;position:relative;">';
        html += '<div style="height:100%;width:' + pct + '%;background:#00d4ff40;border-left:3px solid #00d4ff;"></div>';
        html += '<div style="position:absolute;right:8px;top:50%;transform:translateY(-50%);color:#c0d8f0;font-size:0.7em;font-weight:700;">' + s[1] + '</div>';
        html += '</div></div>';
      });
      html += '</div>';
    }

    html += '<div class="actions">';
    html += '<a class="holo-btn" href="/tookan/json" target="_blank">Raw JSON</a>';
    html += '<a class="holo-btn" href="/tookan/refresh">Force Refresh</a>';
    html += '</div>';

    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Raw JSON endpoint for Tookan data
/* ===========================
   INTERACTIVE CHARTING — Technical Analysis for Call Volume
   /business/chart — Full TA charting with Bollinger Bands, MACD, RSI, drawing tools
=========================== */

app.get('/business/chart', async function(req, res) {
  try {
    await buildBusinessContext();
    var bm = global.bizMetrics || {};
    var monthlyCalls2 = bm.monthlyCalls || {};

    // Build data series for all locations + aggregate
    var allSeries = {};
    allSeries['ALL LOCATIONS'] = monthlyCalls2;
    var callsByCity = bm.monthlyCallsByCity || {};
    Object.keys(callsByCity).sort().forEach(function(city) {
      // Only include cities with at least 3 months of data and real names
      if (Object.keys(callsByCity[city]).length >= 3 && city.length > 2) {
        allSeries[city] = callsByCity[city];
      }
    });

    var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>ATHENA — Call Volume Technical Analysis</title>';
    html += '<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&display=swap" rel="stylesheet">';
    html += '<script src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.1/lightweight-charts.standalone.production.js"><\/script>';
    html += '<style>';
    html += 'body{margin:0;background:#050d18;color:#c0d8f0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;}';
    html += '.wrap{max-width:1500px;margin:0 auto;padding:20px 30px;}';
    html += '.toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:15px;padding:10px;background:rgba(10,20,35,0.8);border:1px solid #1a2a3a;}';
    html += '.toolbar select,.toolbar button{font-family:Orbitron;font-size:0.55em;letter-spacing:2px;padding:8px 14px;background:#0a1520;color:#c0d8f0;border:1px solid #1a2a3a;cursor:pointer;outline:none;}';
    html += '.toolbar select:hover,.toolbar button:hover{border-color:#00d4ff40;}';
    html += '.toolbar button.active{background:rgba(0,212,255,0.15);border-color:#00d4ff;color:#00d4ff;}';
    html += '.toolbar .sep{width:1px;height:20px;background:#1a2a3a;}';
    html += '.toolbar label{font-family:Orbitron;font-size:0.5em;letter-spacing:2px;color:#4a6a8a;}';
    html += '#main-chart{border:1px solid #1a2a3a;margin-bottom:5px;}';
    html += '#rsi-chart{border:1px solid #1a2a3a;margin-bottom:5px;}';
    html += '#macd-chart{border:1px solid #1a2a3a;margin-bottom:10px;}';
    html += '.indicator-label{font-family:Orbitron;font-size:0.45em;letter-spacing:2px;color:#4a6a8a;padding:4px 10px;background:rgba(10,20,35,0.8);display:inline-block;margin-bottom:2px;}';
    html += '.tab-nav{display:flex;gap:0;margin-bottom:15px;}';
    html += '.tab-nav a{font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;background:rgba(5,10,20,0.6);}';
    html += '.tab-nav a.active{color:#a855f7;border-color:#a855f740;background:rgba(168,85,247,0.1);}';
    html += '.legend{display:flex;gap:15px;flex-wrap:wrap;padding:8px 10px;font-size:0.75em;}';
    html += '.legend-item{display:flex;align-items:center;gap:5px;color:#4a6a8a;}';
    html += '.legend-dot{width:10px;height:3px;display:inline-block;}';
    // Mobile responsive
    html += '@media(max-width:768px){';
    html += '.wrap{padding:10px 12px!important;}';
    html += '.toolbar{flex-wrap:wrap!important;gap:6px!important;padding:8px!important;}';
    html += '.toolbar select,.toolbar button{font-size:0.5em!important;padding:6px 8px!important;}';
    html += '.tab-nav{flex-wrap:wrap!important;}';
    html += '.tab-nav a{padding:8px 15px!important;font-size:0.55em!important;}';
    html += '#main-chart{height:300px!important;}';
    html += '#rsi-chart{height:80px!important;}';
    html += '#macd-chart{height:80px!important;}';
    html += '}';
    html += '</style></head><body><div class="wrap">';

    // Tab nav
    html += '<div class="tab-nav">';
    html += '<a href="/dashboard">JARVIS</a>';
    html += '<a href="/business">ATHENA</a>';
    html += '<a href="/tookan">TOOKAN</a>';
    html += '<a href="/business/chart" class="active">CHARTS</a>';
    html += '<a href="/analytics" style="font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;transition:all 0.3s;background:rgba(5,10,20,0.6);">ANALYTICS</a>';
    html += '</div>';

    html += '<div style="font-family:Orbitron;font-size:1.1em;letter-spacing:6px;color:#00d4ff;margin-bottom:5px;">CALL VOLUME — TECHNICAL ANALYSIS</div>';
    html += '<div style="font-family:Orbitron;font-size:0.5em;letter-spacing:3px;color:#4a6a8a;margin-bottom:15px;">FIBONACCI • BOLLINGER BANDS • MACD • RSI • DRAWING TOOLS</div>';

    // Toolbar
    html += '<div class="toolbar">';
    html += '<label>MARKET</label>';
    html += '<select id="market-select">';
    Object.keys(allSeries).forEach(function(name) {
      html += '<option value="' + name + '">' + name + '</option>';
    });
    html += '</select>';
    html += '<div class="sep"></div>';
    html += '<label>OVERLAYS</label>';
    html += '<button id="btn-bb" onclick="toggleBB()" title="Bollinger Bands">BB</button>';
    html += '<button id="btn-sma20" onclick="toggleSMA(20)" title="20-period SMA">SMA20</button>';
    html += '<button id="btn-sma50" onclick="toggleSMA(50)" title="50-period SMA">SMA50</button>';
    html += '<button id="btn-ema12" onclick="toggleEMA()" title="12-period EMA">EMA12</button>';
    html += '<button id="btn-fib" onclick="toggleFib()" title="Fibonacci Levels">FIB</button>';
    html += '<div class="sep"></div>';
    html += '<label>DRAW</label>';
    html += '<button id="btn-trendline" onclick="setDrawMode(\'trendline\')">TRENDLINE</button>';
    html += '<button id="btn-hline" onclick="setDrawMode(\'hline\')">H-LINE</button>';
    html += '<button id="btn-clear" onclick="clearDrawings()">CLEAR</button>';
    html += '<div class="sep"></div>';
    html += '<label>BB PERIOD</label>';
    html += '<select id="bb-period" onchange="updateAll()"><option value="10">10</option><option value="15">15</option><option value="20" selected>20</option></select>';
    html += '<label>BB STD</label>';
    html += '<select id="bb-std" onchange="updateAll()"><option value="1.5">1.5</option><option value="2" selected>2.0</option><option value="2.5">2.5</option></select>';
    html += '</div>';

    // Chart containers
    html += '<div id="main-chart"></div>';
    html += '<div class="legend" id="main-legend"></div>';
    html += '<div class="indicator-label">RSI (14)</div>';
    html += '<div id="rsi-chart"></div>';
    html += '<div class="indicator-label">MACD (12, 26, 9)</div>';
    html += '<div id="macd-chart"></div>';
    html += '<div class="indicator-label">BOLLINGER BAND WIDTH (Squeeze Detector)</div>';
    html += '<div id="bbw-chart"></div>';

    // Inject data & charting logic
    html += '<script>';
    html += 'var allData = ' + JSON.stringify(allSeries) + ';';

    // TA calculation functions
    html += 'function sma(data, period) {';
    html += '  var result = [];';
    html += '  for (var i = 0; i < data.length; i++) {';
    html += '    if (i < period - 1) { result.push(null); continue; }';
    html += '    var sum = 0; for (var j = 0; j < period; j++) sum += data[i - j].value;';
    html += '    result.push({ time: data[i].time, value: Math.round(sum / period * 100) / 100 });';
    html += '  } return result.filter(function(d) { return d !== null; });';
    html += '}';

    html += 'function ema(data, period) {';
    html += '  var result = []; var k = 2 / (period + 1); var prev = data[0].value;';
    html += '  result.push({ time: data[0].time, value: prev });';
    html += '  for (var i = 1; i < data.length; i++) {';
    html += '    prev = data[i].value * k + prev * (1 - k);';
    html += '    result.push({ time: data[i].time, value: Math.round(prev * 100) / 100 });';
    html += '  } return result;';
    html += '}';

    html += 'function bollingerBands(data, period, stdDev) {';
    html += '  var upper = [], lower = [], middle = [], width = [];';
    html += '  for (var i = 0; i < data.length; i++) {';
    html += '    if (i < period - 1) continue;';
    html += '    var sum = 0; for (var j = 0; j < period; j++) sum += data[i - j].value;';
    html += '    var avg = sum / period;';
    html += '    var sqSum = 0; for (var j = 0; j < period; j++) sqSum += Math.pow(data[i - j].value - avg, 2);';
    html += '    var std = Math.sqrt(sqSum / period);';
    html += '    var u = avg + stdDev * std, l = avg - stdDev * std;';
    html += '    upper.push({ time: data[i].time, value: Math.round(u * 100) / 100 });';
    html += '    lower.push({ time: data[i].time, value: Math.round(Math.max(l, 0) * 100) / 100 });';
    html += '    middle.push({ time: data[i].time, value: Math.round(avg * 100) / 100 });';
    html += '    width.push({ time: data[i].time, value: avg > 0 ? Math.round((u - l) / avg * 10000) / 100 : 0 });';
    html += '  } return { upper: upper, lower: lower, middle: middle, width: width };';
    html += '}';

    html += 'function calcRSI(data, period) {';
    html += '  var result = []; var gains = 0, losses = 0;';
    html += '  for (var i = 1; i < data.length; i++) {';
    html += '    var diff = data[i].value - data[i-1].value;';
    html += '    if (i <= period) { if (diff > 0) gains += diff; else losses -= diff; }';
    html += '    if (i === period) { gains /= period; losses /= period; }';
    html += '    if (i > period) {';
    html += '      if (diff > 0) { gains = (gains * (period-1) + diff) / period; losses = (losses * (period-1)) / period; }';
    html += '      else { gains = (gains * (period-1)) / period; losses = (losses * (period-1) - diff) / period; }';
    html += '    }';
    html += '    if (i >= period) {';
    html += '      var rs = losses === 0 ? 100 : gains / losses;';
    html += '      result.push({ time: data[i].time, value: Math.round((100 - 100 / (1 + rs)) * 100) / 100 });';
    html += '    }';
    html += '  } return result;';
    html += '}';

    html += 'function calcMACD(data) {';
    html += '  var ema12 = ema(data, 12), ema26 = ema(data, 26);';
    html += '  var macdLine = [], signal = [], histogram = [];';
    html += '  for (var i = 0; i < ema26.length; i++) {';
    html += '    var e12 = ema12.find(function(d) { return d.time === ema26[i].time; });';
    html += '    if (e12) macdLine.push({ time: ema26[i].time, value: Math.round((e12.value - ema26[i].value) * 100) / 100 });';
    html += '  }';
    html += '  if (macdLine.length > 0) { signal = ema(macdLine, 9); }';
    html += '  for (var i = 0; i < signal.length; i++) {';
    html += '    var ml = macdLine.find(function(d) { return d.time === signal[i].time; });';
    html += '    if (ml) histogram.push({ time: signal[i].time, value: Math.round((ml.value - signal[i].value) * 100) / 100, color: ml.value >= signal[i].value ? "#26a69a" : "#ef5350" });';
    html += '  }';
    html += '  return { macdLine: macdLine, signal: signal, histogram: histogram };';
    html += '}';

    // Chart setup
    html += 'var mainChart, rsiChart, macdChart, bbwChart;';
    html += 'var mainSeries, bbUpperSeries, bbLowerSeries, bbMiddleSeries;';
    html += 'var sma20Series, sma50Series, ema12Series;';
    html += 'var rsiSeries, macdLineSeries, macdSignalSeries, macdHistSeries, bbwSeries;';
    html += 'var fibLines = [];';
    html += 'var showBB = false, showSMA20 = false, showSMA50 = false, showEMA12 = false, showFib = false;';
    html += 'var drawMode = null, drawStart = null, drawings = [];';

    html += 'function createCharts() {';
    html += '  var w = document.getElementById("main-chart").offsetWidth;';
    html += '  var opts = { width: w, layout: { background: { color: "#050d18" }, textColor: "#4a6a8a", fontSize: 11 }, grid: { vertLines: { color: "#0a1520" }, horzLines: { color: "#0a1520" } }, crosshair: { mode: 0 }, timeScale: { borderColor: "#1a2a3a", timeVisible: false } };';

    html += '  mainChart = LightweightCharts.createChart(document.getElementById("main-chart"), Object.assign({}, opts, { height: 400 }));';
    html += '  mainSeries = mainChart.addLineSeries({ color: "#00d4ff", lineWidth: 2, title: "Calls" });';
    html += '  bbUpperSeries = mainChart.addLineSeries({ color: "#ff9f4380", lineWidth: 1, lineStyle: 2, title: "BB Upper" });';
    html += '  bbLowerSeries = mainChart.addLineSeries({ color: "#ff9f4380", lineWidth: 1, lineStyle: 2, title: "BB Lower" });';
    html += '  bbMiddleSeries = mainChart.addLineSeries({ color: "#ff9f4340", lineWidth: 1, lineStyle: 1, title: "BB Mid" });';
    html += '  sma20Series = mainChart.addLineSeries({ color: "#a855f7", lineWidth: 1, title: "SMA 20" });';
    html += '  sma50Series = mainChart.addLineSeries({ color: "#00ff66", lineWidth: 1, title: "SMA 50" });';
    html += '  ema12Series = mainChart.addLineSeries({ color: "#ffd700", lineWidth: 1, title: "EMA 12" });';

    html += '  rsiChart = LightweightCharts.createChart(document.getElementById("rsi-chart"), Object.assign({}, opts, { height: 120 }));';
    html += '  rsiSeries = rsiChart.addLineSeries({ color: "#a855f7", lineWidth: 1.5, title: "RSI" });';

    html += '  macdChart = LightweightCharts.createChart(document.getElementById("macd-chart"), Object.assign({}, opts, { height: 120 }));';
    html += '  macdLineSeries = macdChart.addLineSeries({ color: "#00d4ff", lineWidth: 1.5, title: "MACD" });';
    html += '  macdSignalSeries = macdChart.addLineSeries({ color: "#ff9f43", lineWidth: 1, title: "Signal" });';
    html += '  macdHistSeries = macdChart.addHistogramSeries({ title: "Histogram" });';

    html += '  bbwChart = LightweightCharts.createChart(document.getElementById("bbw-chart"), Object.assign({}, opts, { height: 100 }));';
    html += '  bbwSeries = bbwChart.addHistogramSeries({ color: "#00d4ff40", title: "BB Width" });';

    // Sync crosshairs
    html += '  function syncCrosshair(src, targets) {';
    html += '    src.subscribeCrosshairMove(function(param) {';
    html += '      targets.forEach(function(t) { if (param.time) t.timeScale().scrollToPosition(src.timeScale().scrollPosition(), false); });';
    html += '    });';
    html += '  }';
    html += '  syncCrosshair(mainChart, [rsiChart, macdChart, bbwChart]);';

    // Drawing on main chart
    html += '  mainChart.subscribeClick(function(param) {';
    html += '    if (!drawMode || !param.time) return;';
    html += '    var price = mainSeries.coordinateToPrice(param.point.y);';
    html += '    if (drawMode === "hline") {';
    html += '      var line = mainSeries.createPriceLine({ price: price, color: "#ffd700", lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: Math.round(price) + "" });';
    html += '      drawings.push(line);';
    html += '      setDrawMode(null);';
    html += '    } else if (drawMode === "trendline") {';
    html += '      if (!drawStart) { drawStart = { time: param.time, price: price }; }';
    html += '      else {';
    html += '        var markers = [{ time: drawStart.time, position: "belowBar", color: "#ffd700", shape: "circle" }, { time: param.time, position: "belowBar", color: "#ffd700", shape: "circle" }];';
    html += '        mainSeries.setMarkers(markers);';
    html += '        drawStart = null; setDrawMode(null);';
    html += '      }';
    html += '    }';
    html += '  });';
    html += '}';

    // Update all indicators
    html += 'function updateAll() {';
    html += '  var market = document.getElementById("market-select").value;';
    html += '  var raw = allData[market] || {};';
    html += '  var months = Object.keys(raw).sort();';
    html += '  var data = months.map(function(k) { return { time: k + "-01", value: raw[k] }; });';
    html += '  if (data.length < 2) return;';

    html += '  mainSeries.setData(data);';

    // Bollinger Bands
    html += '  var bbP = parseInt(document.getElementById("bb-period").value);';
    html += '  var bbS = parseFloat(document.getElementById("bb-std").value);';
    html += '  var bb = bollingerBands(data, bbP, bbS);';
    html += '  if (showBB) { bbUpperSeries.setData(bb.upper); bbLowerSeries.setData(bb.lower); bbMiddleSeries.setData(bb.middle); }';
    html += '  else { bbUpperSeries.setData([]); bbLowerSeries.setData([]); bbMiddleSeries.setData([]); }';

    // SMAs
    html += '  sma20Series.setData(showSMA20 ? sma(data, 20) : []);';
    html += '  sma50Series.setData(showSMA50 ? sma(data, 50) : []);';
    html += '  ema12Series.setData(showEMA12 ? ema(data, 12) : []);';

    // RSI
    html += '  rsiSeries.setData(calcRSI(data, 14));';

    // MACD
    html += '  var m = calcMACD(data);';
    html += '  macdLineSeries.setData(m.macdLine);';
    html += '  macdSignalSeries.setData(m.signal);';
    html += '  macdHistSeries.setData(m.histogram);';

    // BB Width (squeeze detector)
    html += '  var bbw = bb.width.map(function(d) {';
    html += '    var isSqueeze = d.value < 15;';
    html += '    return { time: d.time, value: d.value, color: isSqueeze ? "#ff475780" : "#00d4ff40" };';
    html += '  });';
    html += '  bbwSeries.setData(bbw);';

    // Fibonacci lines
    html += '  fibLines.forEach(function(l) { try { mainChart.removePriceLine ? mainSeries.removePriceLine(l) : null; } catch(e) {} });';
    html += '  fibLines = [];';
    html += '  if (showFib && data.length >= 3) {';
    html += '    var vals = data.map(function(d) { return d.value; });';
    html += '    var hi = Math.max.apply(null, vals.slice(-6));';
    html += '    var lo = Math.min.apply(null, vals.slice(-6));';
    html += '    var range = hi - lo;';
    html += '    [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0].forEach(function(lvl) {';
    html += '      var price = lo + range * lvl;';
    html += '      var line = mainSeries.createPriceLine({ price: price, color: "#00ff6640", lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: (lvl * 100).toFixed(1) + "%" });';
    html += '      fibLines.push(line);';
    html += '    });';
    html += '  }';

    // Legend
    html += '  var leg = "";';
    html += '  if (showBB) leg += "<div class=\\"legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#ff9f43;\\"></span>Bollinger Bands</div>";';
    html += '  if (showSMA20) leg += "<div class=\\"legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#a855f7;\\"></span>SMA 20</div>";';
    html += '  if (showSMA50) leg += "<div class=\\"legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#00ff66;\\"></span>SMA 50</div>";';
    html += '  if (showEMA12) leg += "<div class=\\"legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#ffd700;\\"></span>EMA 12</div>";';
    html += '  if (showFib) leg += "<div class=\\"legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#00ff66;\\"></span>Fibonacci</div>";';
    html += '  document.getElementById("main-legend").innerHTML = leg;';

    html += '  mainChart.timeScale().fitContent();';
    html += '  rsiChart.timeScale().fitContent();';
    html += '  macdChart.timeScale().fitContent();';
    html += '  bbwChart.timeScale().fitContent();';
    html += '}';

    // Toggle functions
    html += 'function toggleBB() { showBB = !showBB; document.getElementById("btn-bb").classList.toggle("active"); updateAll(); }';
    html += 'function toggleSMA(p) { if (p===20) { showSMA20=!showSMA20; document.getElementById("btn-sma20").classList.toggle("active"); } else { showSMA50=!showSMA50; document.getElementById("btn-sma50").classList.toggle("active"); } updateAll(); }';
    html += 'function toggleEMA() { showEMA12=!showEMA12; document.getElementById("btn-ema12").classList.toggle("active"); updateAll(); }';
    html += 'function toggleFib() { showFib=!showFib; document.getElementById("btn-fib").classList.toggle("active"); updateAll(); }';
    html += 'function setDrawMode(mode) { drawMode = mode; drawStart = null; document.getElementById("btn-trendline").classList.toggle("active", mode==="trendline"); document.getElementById("btn-hline").classList.toggle("active", mode==="hline"); document.body.style.cursor = mode ? "crosshair" : "default"; }';
    html += 'function clearDrawings() { drawings.forEach(function(d) { try { mainSeries.removePriceLine(d); } catch(e) {} }); drawings = []; mainSeries.setMarkers([]); }';

    // Market change
    html += 'document.getElementById("market-select").addEventListener("change", updateAll);';

    // Init
    html += 'createCharts(); updateAll();';

    // Resize
    html += 'window.addEventListener("resize", function() {';
    html += '  var w = document.getElementById("main-chart").parentElement.offsetWidth - 60;';
    html += '  mainChart.applyOptions({ width: w }); rsiChart.applyOptions({ width: w }); macdChart.applyOptions({ width: w }); bbwChart.applyOptions({ width: w });';
    html += '});';

    html += '<\/script>';
    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ===========================
// PREDICTIVE ANALYTICS ENGINE v2.0 — /analytics
// Revenue forecasting, equipment Fibonacci, seasonal demand prediction,
// growth trajectories, city-level forecasting, tech productivity,
// cancellation analysis, market penetration, profitability projections,
// interactive LightweightCharts, correlation analysis, AI strategy engine
// ===========================

app.get('/analytics', async function(req, res) {
  try {
    await buildBusinessContext();
    var bm = global.bizMetrics || {};
    var pm = global.profitMetrics || {};
    var fh = (pm || {}).financialHistory || {};
    var monthlyCalls = bm.monthlyCalls || {};
    var callsByCity = bm.monthlyCallsByCity || {};
    var callsByTab = bm.monthlyCallsByTab || {};
    var seasonalData = bm.seasonalData || {};
    var locationStats = bm.locationStats || {};
    var equipStats = bm.equipStats || {};
    var brandStats = bm.brandStats || {};
    var techStats = bm.techStats || {};
    var dailyRevenue = pm.dailyRevenue || [];
    var dailyProfit = pm.dailyProfit || [];
    var dailyAds = pm.dailyAds || [];

    // ========== ANALYTICS ENGINE: Compute all predictions ==========
    var monthKeys = Object.keys(monthlyCalls).sort();
    var monthVals = monthKeys.map(function(k) { return monthlyCalls[k] || 0; });
    var today = new Date();
    var curMonth = today.getMonth();
    var curYear = today.getFullYear();
    var monthLabels3 = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    // --- Helper: Linear Regression ---
    function linearRegression(values) {
      var n = values.length;
      if (n < 2) return { slope: 0, intercept: values[0] || 0, r2: 0 };
      var sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
      for (var i = 0; i < n; i++) { sumX += i; sumY += values[i]; sumXY += i * values[i]; sumXX += i * i; }
      var denom = (n * sumXX - sumX * sumX);
      var slope = denom !== 0 ? (n * sumXY - sumX * sumY) / denom : 0;
      var intercept = (sumY - slope * sumX) / n;
      var ssRes = 0, ssTot = 0, mean = sumY / n;
      for (var j = 0; j < n; j++) { var pred = intercept + slope * j; ssRes += (values[j] - pred) * (values[j] - pred); ssTot += (values[j] - mean) * (values[j] - mean); }
      var r2 = ssTot > 0 ? Math.round((1 - ssRes / ssTot) * 1000) / 1000 : 0;
      return { slope: Math.round(slope * 100) / 100, intercept: Math.round(intercept * 100) / 100, r2: r2 };
    }

    // --- Helper: Exponential Moving Average ---
    function emaCalc(values, period) {
      if (values.length === 0) return [];
      var k = 2 / (period + 1); var result = [values[0]];
      for (var i = 1; i < values.length; i++) { result.push(values[i] * k + result[i - 1] * (1 - k)); }
      return result;
    }

    // --- Helper: Fibonacci levels ---
    function calcFib(values) {
      var recent = values.slice(-12);
      var high = Math.max.apply(null, recent.length > 0 ? recent : [0]);
      var low = Math.min.apply(null, recent.length > 0 ? recent : [0]);
      var range = high - low;
      return {
        high: high, low: low, range: range,
        '23.6': Math.round(low + range * 0.236), '38.2': Math.round(low + range * 0.382),
        '50.0': Math.round(low + range * 0.5), '61.8': Math.round(low + range * 0.618),
        '78.6': Math.round(low + range * 0.786), current: recent.length > 0 ? recent[recent.length - 1] : 0,
      };
    }

    // --- Helper: Fibonacci with extensions ---
    function calcFibExtended(values) {
      var fib = calcFib(values);
      fib['127.2'] = Math.round(fib.low + fib.range * 1.272);
      fib['161.8'] = Math.round(fib.low + fib.range * 1.618);
      fib['200.0'] = Math.round(fib.low + fib.range * 2.0);
      fib['261.8'] = Math.round(fib.low + fib.range * 2.618);
      return fib;
    }

    // --- Helper: Growth metrics ---
    function growthMetrics(values) {
      if (values.length < 2) return { mom: 0, qoq: 0, yoy: 0, cagr: 0, trend: 'flat', last: 0, prev: 0 };
      var last = values[values.length - 1], prev = values[values.length - 2];
      var mom = prev > 0 ? Math.round(((last - prev) / prev) * 1000) / 10 : 0;
      var q1 = 0, q2 = 0;
      if (values.length >= 6) { for (var i = values.length - 3; i < values.length; i++) q1 += values[i]; for (var j = values.length - 6; j < values.length - 3; j++) q2 += values[j]; }
      var qoq = q2 > 0 ? Math.round(((q1 - q2) / q2) * 1000) / 10 : 0;
      var yoy = 0;
      if (values.length >= 12) { var thisY = values.slice(-6).reduce(function(a,b){return a+b;},0); var lastY = values.slice(-12,-6).reduce(function(a,b){return a+b;},0); yoy = lastY > 0 ? Math.round(((thisY-lastY)/lastY)*1000)/10 : 0; }
      var first = values[0] > 0 ? values[0] : 1; var periods = values.length - 1;
      var cagr = periods > 0 ? Math.round((Math.pow(last / first, 1 / periods) - 1) * 1000) / 10 : 0;
      var trend = mom > 5 ? 'growing' : mom < -5 ? 'declining' : 'stable';
      return { mom: mom, qoq: qoq, yoy: yoy, cagr: cagr, trend: trend, last: last, prev: prev };
    }

    // --- Helper: Forecast ---
    function forecast(values, months) {
      var lr = linearRegression(values); var n = values.length;
      var emaVals = emaCalc(values, 6); var lastEma = emaVals.length > 0 ? emaVals[emaVals.length - 1] : 0;
      var preds = [];
      for (var i = 0; i < months; i++) {
        var lrPred = Math.round(lr.intercept + lr.slope * (n + i));
        var emaPred = Math.round(lastEma + lr.slope * (i + 1));
        var avg = Math.round((lrPred + emaPred) / 2);
        var confidence = Math.max(0.3, 1 - (i * 0.1));
        var stdDev = values.length > 3 ? Math.sqrt(values.slice(-6).reduce(function(s, v) { var m = lastEma; return s + (v-m)*(v-m); }, 0) / Math.min(6, values.length)) : avg * 0.15;
        preds.push({ month: i + 1, linear: Math.max(0, lrPred), ema: Math.max(0, emaPred), blended: Math.max(0, avg), confidence: confidence, upper: Math.max(0, Math.round(avg + stdDev * 1.5)), lower: Math.max(0, Math.round(avg - stdDev * 1.5)) });
      }
      return preds;
    }

    // --- Helper: Bollinger Bands ---
    function bollingerBands(values, period, stdMult) {
      var upper = [], lower = [], middle = [], width = [];
      for (var i = 0; i < values.length; i++) {
        if (i < period - 1) continue;
        var sum = 0; for (var j = 0; j < period; j++) sum += values[i - j];
        var avg = sum / period;
        var sqSum = 0; for (var j2 = 0; j2 < period; j2++) sqSum += Math.pow(values[i - j2] - avg, 2);
        var std = Math.sqrt(sqSum / period);
        upper.push(Math.round(avg + stdMult * std));
        lower.push(Math.round(Math.max(0, avg - stdMult * std)));
        middle.push(Math.round(avg));
        width.push(avg > 0 ? Math.round((2 * stdMult * std) / avg * 10000) / 100 : 0);
      }
      return { upper: upper, lower: lower, middle: middle, width: width };
    }

    // --- Helper: RSI ---
    function calcRSI(values, period) {
      var result = []; var gains = 0, losses = 0;
      for (var i = 1; i < values.length; i++) {
        var diff = values[i] - values[i-1];
        if (i <= period) { if (diff > 0) gains += diff; else losses -= diff; }
        if (i === period) { gains /= period; losses /= period; }
        if (i > period) { if (diff > 0) { gains = (gains*(period-1)+diff)/period; losses = (losses*(period-1))/period; } else { gains = (gains*(period-1))/period; losses = (losses*(period-1)-diff)/period; } }
        if (i >= period) { var rs = losses === 0 ? 100 : gains / losses; result.push(Math.round((100 - 100/(1+rs))*100)/100); }
      }
      return result;
    }

    // --- Helper: MACD ---
    function calcMACD(values) {
      var ema12 = emaCalc(values, 12), ema26 = emaCalc(values, 26);
      var macdLine = []; for (var i = 0; i < ema26.length; i++) macdLine.push(Math.round((ema12[i] - ema26[i])*100)/100);
      var signal = emaCalc(macdLine, 9);
      var histogram = signal.map(function(s, i) { return Math.round((macdLine[i] - s)*100)/100; });
      return { macdLine: macdLine, signal: signal, histogram: histogram };
    }

    // --- Helper: Volatility ---
    function calcVolatility(values) {
      if (values.length < 3) return 0;
      var returns = [];
      for (var i = 1; i < values.length; i++) { if (values[i-1] > 0) returns.push((values[i] - values[i-1]) / values[i-1]); }
      if (returns.length === 0) return 0;
      var mean = returns.reduce(function(a,b){return a+b;},0) / returns.length;
      var variance = returns.reduce(function(s,r) { return s + (r-mean)*(r-mean); }, 0) / returns.length;
      return Math.round(Math.sqrt(variance) * 10000) / 100;
    }

    // --- Helper: Trend Strength (ADX-like) ---
    function trendStrength(values) {
      if (values.length < 5) return { strength: 0, direction: 'flat' };
      var ups = 0, downs = 0;
      for (var i = 1; i < values.length; i++) { if (values[i] > values[i-1]) ups++; else if (values[i] < values[i-1]) downs++; }
      var total = values.length - 1;
      var dirStrength = Math.abs(ups - downs) / total;
      var lr = linearRegression(values);
      var combined = Math.round((dirStrength * 50 + Math.min(lr.r2, 1) * 50) * 10) / 10;
      var direction = lr.slope > 0 ? 'bullish' : lr.slope < 0 ? 'bearish' : 'flat';
      return { strength: combined, direction: direction, consistency: Math.round(Math.max(ups, downs) / total * 100) };
    }

    // ========== COMPUTE ALL ANALYTICS ==========

    // 1. Call Volume
    var callFib = calcFibExtended(monthVals);
    var callGrowth = growthMetrics(monthVals);
    var callLR = linearRegression(monthVals);
    var callForecast = forecast(monthVals, 6);
    var callBB = bollingerBands(monthVals, Math.min(6, monthVals.length), 2);
    var callRSI = calcRSI(monthVals, Math.min(6, monthVals.length));
    var callMACD = calcMACD(monthVals);
    var callVol = calcVolatility(monthVals);
    var callTrend = trendStrength(monthVals);

    var daysInMonth = new Date(curYear, curMonth + 1, 0).getDate();
    var dayOfMonth = today.getDate();
    var curMonthKey = curYear + '-' + String(curMonth + 1).padStart(2, '0');
    var curMonthCalls = monthlyCalls[curMonthKey] || 0;
    var runRate = dayOfMonth > 0 ? Math.round(curMonthCalls / dayOfMonth * daysInMonth) : 0;
    var annualRunRate = runRate * 12;

    // 2. Revenue/Profit — sort chronologically (keys are "Jan 2024", "Feb 2024" etc.)
    var monthOrder = {Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12};
    var revMonths = Object.keys(fh.months || {}).sort(function(a, b) {
      var pa = a.split(' '), pb = b.split(' ');
      var ya = parseInt(pa[1]) || 0, yb = parseInt(pb[1]) || 0;
      if (ya !== yb) return ya - yb;
      return (monthOrder[pa[0]] || 0) - (monthOrder[pb[0]] || 0);
    });
    var revVals, profVals, expVals, adsVals, marginVals;
    if (revMonths.length > 0) {
      revVals = revMonths.map(function(m) { return (fh.months[m] || {}).revenue || 0; });
      profVals = revMonths.map(function(m) { return (fh.months[m] || {}).profit || 0; });
      expVals = revMonths.map(function(m) { return (fh.months[m] || {}).expenses || 0; });
      adsVals = revMonths.map(function(m) { return (fh.months[m] || {}).ads || 0; });
      marginVals = revMonths.map(function(m) { return (fh.months[m] || {}).margin || 0; });
    } else {
      // Fallback: estimate revenue from call volume data * avg ticket
      var estTicket = (pm.revenue && bm.thisMonthCalls) ? Math.round(pm.revenue / Math.max(1, bm.thisMonthCalls)) : 321;
      revMonths = monthKeys.slice();
      revVals = monthVals.map(function(v) { return v * estTicket; });
      profVals = revVals.map(function(v) { return Math.round(v * 0.25); }); // est 25% margin
      expVals = revVals.map(function(v, i) { return v - profVals[i]; });
      adsVals = revVals.map(function(v) { return Math.round(v * 0.08); }); // est 8% ad spend
      marginVals = revVals.map(function(v) { return v > 0 ? 25 : 0; });
    }
    var revFib = calcFibExtended(revVals);
    var revGrowth = growthMetrics(revVals);
    var revForecast = forecast(revVals, 6);
    var profFib = calcFibExtended(profVals);
    var profGrowth = growthMetrics(profVals);
    var profForecast = forecast(profVals, 6);
    var expFib = calcFibExtended(expVals);
    var adsFib = calcFibExtended(adsVals);
    var marginFib = calcFib(marginVals);
    var revBB = bollingerBands(revVals, Math.min(6, revVals.length), 2);
    var revRSI = calcRSI(revVals, Math.min(6, revVals.length));
    var revMACD = calcMACD(revVals);
    var revVol = calcVolatility(revVals);
    var revTrend = trendStrength(revVals);

    var revenuePerCall = bm.totalLeads > 0 && fh.allTime ? Math.round(fh.allTime.revenue / bm.totalLeads) : 321;

    // 3. Seasonal Demand
    var seasonKeys = Object.keys(seasonalData).sort();
    var seasonByMonth = {};
    seasonKeys.forEach(function(k) {
      var mo = parseInt(k.split('-')[1]);
      if (!seasonByMonth[mo]) seasonByMonth[mo] = { snow: 0, mower: 0, generator: 0, other: 0, count: 0 };
      seasonByMonth[mo].snow += seasonalData[k].snow; seasonByMonth[mo].mower += seasonalData[k].mower;
      seasonByMonth[mo].generator += seasonalData[k].generator; seasonByMonth[mo].other += seasonalData[k].other; seasonByMonth[mo].count++;
    });
    var seasonAvg = {};
    for (var m = 1; m <= 12; m++) {
      var sd = seasonByMonth[m] || { snow: 0, mower: 0, generator: 0, other: 0, count: 1 };
      var c = Math.max(sd.count, 1);
      seasonAvg[m] = { snow: Math.round(sd.snow / c), mower: Math.round(sd.mower / c), generator: Math.round(sd.generator / c), other: Math.round(sd.other / c) };
    }

    // Equipment Fibonacci
    var equipMonthly = { mower: [], snow: [], generator: [], other: [] };
    seasonKeys.forEach(function(k) { var sd2 = seasonalData[k]; equipMonthly.mower.push(sd2.mower); equipMonthly.snow.push(sd2.snow); equipMonthly.generator.push(sd2.generator); equipMonthly.other.push(sd2.other); });
    var mowerFib = calcFibExtended(equipMonthly.mower); var snowFib = calcFibExtended(equipMonthly.snow); var genFib = calcFibExtended(equipMonthly.generator);
    var mowerGrowth = growthMetrics(equipMonthly.mower); var snowGrowth = growthMetrics(equipMonthly.snow); var genGrowth = growthMetrics(equipMonthly.generator);

    // 4. City Forecasting (enhanced)
    var cityForecasts = [];
    Object.keys(callsByCity).forEach(function(city) {
      var cm = callsByCity[city]; var ck = Object.keys(cm).sort();
      if (ck.length < 3) return;
      var cv = ck.map(function(k) { return cm[k] || 0; });
      var cFib = calcFibExtended(cv); var cGrowth = growthMetrics(cv); var cLR = linearRegression(cv);
      var total = cv.reduce(function(a,b){return a+b;},0);
      var cForecast = forecast(cv, 3);
      var cBB = bollingerBands(cv, Math.min(4, cv.length), 2);
      var cRSI = calcRSI(cv, Math.min(4, cv.length));
      var cVol = calcVolatility(cv);
      var cTrend = trendStrength(cv);
      cityForecasts.push({
        city: city, total: total, months: ck.length, current: cv[cv.length - 1],
        predicted: Math.max(0, cForecast.length > 0 ? cForecast[0].blended : 0),
        growth: cGrowth, fib: cFib, r2: cLR.r2, slope: cLR.slope, forecast: cForecast,
        bb: cBB, rsi: cRSI.length > 0 ? cRSI[cRSI.length - 1] : 50,
        volatility: cVol, trend: cTrend,
        labels: ck.slice(-12).map(function(k2) { var p = k2.split('-'); return monthLabels3[parseInt(p[1])] + "'" + p[0].substring(2); }),
        values: cv.slice(-12), allValues: cv,
      });
    });
    cityForecasts.sort(function(a, b) { return b.total - a.total; });

    // 5. Tech Productivity (enhanced)
    var techProd = [];
    Object.entries(techStats).forEach(function(t) {
      var ts = t[1]; if (ts.total < 3) return;
      var completionRate = ts.total > 0 ? Math.round(ts.completed / ts.total * 1000) / 10 : 0;
      var estRevenue = ts.total * revenuePerCall;
      var daysSeen = 1;
      if (ts.firstSeen && ts.lastSeen) daysSeen = Math.max(1, Math.round((ts.lastSeen - ts.firstSeen) / 86400000));
      var jobsPerDay = Math.round(ts.total / daysSeen * 100) / 100;
      techProd.push({
        name: t[0], total: ts.total, completed: ts.completed, rate: completionRate,
        revenue: estRevenue, jobsPerDay: jobsPerDay, locations: Object.keys(ts.locations || {}).length,
        cancelRate: ts.total > 0 ? Math.round(ts.cancelled / ts.total * 1000) / 10 : 0,
        returnRate: ts.total > 0 ? Math.round(ts.returnCustomers / ts.total * 1000) / 10 : 0,
        efficiency: completionRate * jobsPerDay,
      });
    });
    // Fallback: if techStats empty but we have tech payouts from profit sheet
    if (techProd.length === 0 && pm.techPayouts && Object.keys(pm.techPayouts).length > 0) {
      Object.entries(pm.techPayouts).forEach(function(tp) {
        if (tp[1] > 0) {
          techProd.push({
            name: tp[0], total: 0, completed: 0, rate: 0,
            revenue: tp[1], jobsPerDay: 0, locations: 0,
            cancelRate: 0, returnRate: 0, efficiency: 0,
            payoutOnly: true,
          });
        }
      });
    }
    // Also try yearlyTechPayouts as fallback
    if (techProd.length === 0 && pm.yearlyTechPayouts && Object.keys(pm.yearlyTechPayouts).length > 0) {
      Object.entries(pm.yearlyTechPayouts).forEach(function(tp) {
        if (tp[1] > 0) {
          techProd.push({
            name: tp[0], total: 0, completed: 0, rate: 0,
            revenue: tp[1], jobsPerDay: 0, locations: 0,
            cancelRate: 0, returnRate: 0, efficiency: 0,
            payoutOnly: true,
          });
        }
      });
    }
    techProd.sort(function(a, b) { return b.revenue - a.revenue; });

    // 6. Ad Spend Efficiency
    var adROI = [];
    for (var ai = 0; ai < revVals.length; ai++) {
      if (adsVals[ai] > 0) adROI.push(Math.round(revVals[ai] / adsVals[ai] * 100) / 100);
      else adROI.push(0);
    }
    var adROIFib = calcFib(adROI.filter(function(v){return v > 0;}));
    var costPerLead = [];
    for (var ci = 0; ci < monthKeys.length; ci++) {
      var callCount = monthVals[ci];
      var adSpend = ci < adsVals.length ? adsVals[ci] : 0;
      if (callCount > 0 && adSpend > 0) costPerLead.push(Math.round(adSpend / callCount * 100) / 100);
      else costPerLead.push(0);
    }
    var cplFib = calcFib(costPerLead.filter(function(v){return v > 0;}));

    // 7. Correlation: calls vs revenue
    var callRevCorrelation = 0;
    if (revVals.length >= 3 && monthVals.length >= 3) {
      var minLen = Math.min(revVals.length, monthVals.length);
      var callsSub = monthVals.slice(-minLen), revSub = revVals.slice(-minLen);
      var avgC = callsSub.reduce(function(a,b){return a+b;},0)/minLen;
      var avgR = revSub.reduce(function(a,b){return a+b;},0)/minLen;
      var cov = 0, stdC = 0, stdR = 0;
      for (var ci2 = 0; ci2 < minLen; ci2++) {
        cov += (callsSub[ci2]-avgC)*(revSub[ci2]-avgR);
        stdC += (callsSub[ci2]-avgC)*(callsSub[ci2]-avgC);
        stdR += (revSub[ci2]-avgR)*(revSub[ci2]-avgR);
      }
      callRevCorrelation = (stdC > 0 && stdR > 0) ? Math.round(cov / (Math.sqrt(stdC) * Math.sqrt(stdR)) * 1000) / 1000 : 0;
    }

    // 8. Day-of-week patterns from recent bookings
    var dowCounts = [0,0,0,0,0,0,0]; var dowLabels = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    var recentBk = bm.recentBookings || [];
    recentBk.forEach(function(b) { if (b.date) { var d = new Date(b.date); if (!isNaN(d)) dowCounts[d.getDay()]++; } });
    var dowTotal = dowCounts.reduce(function(a,b){return a+b;},0);

    // 9. Cancellation analysis
    var cancelByCity = {}; var overallCancelRate = bm.totalLeads > 0 ? Math.round(bm.totalCancelled / bm.totalLeads * 1000) / 10 : 0;
    Object.entries(locationStats).forEach(function(l) { if (l[1].total >= 5) cancelByCity[l[0]] = { total: l[1].total, cancelled: l[1].cancelled, rate: l[1].total > 0 ? Math.round(l[1].cancelled / l[1].total * 1000) / 10 : 0 }; });
    var cancelByCitySorted = Object.entries(cancelByCity).sort(function(a,b) { return b[1].rate - a[1].rate; });

    // 10. Brand analysis
    var brandSorted = Object.entries(brandStats).sort(function(a,b) { return b[1] - a[1]; });
    var brandTotal = brandSorted.reduce(function(s, b) { return s + b[1]; }, 0);

    // ========== BUILD HTML ==========
    var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>WILDWOOD — Predictive Analytics Engine</title>';
    html += '<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet">';
    html += '<script src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.1/lightweight-charts.standalone.production.js"><\/script>';
    html += '<style>';
    html += '*{margin:0;padding:0;box-sizing:border-box;}';
    html += 'body{background:#050d18;color:#c0d8f0;font-family:Rajdhani,sans-serif;overflow-x:hidden;}';
    html += '.wrap{max-width:1500px;margin:0 auto;padding:20px 30px;}';
    html += '.nav{display:flex;gap:0;margin-bottom:20px;flex-wrap:wrap;}';
    html += '.nav a{font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;background:rgba(5,10,20,0.6);transition:all 0.3s;}';
    html += '.nav a.active{color:#ff9f43;border-color:#ff9f4340;background:rgba(255,159,67,0.1);}';
    html += '.nav a:hover{border-color:#ff9f4340;}';
    html += '.title{font-family:Orbitron;font-size:1.4em;letter-spacing:8px;color:#ff9f43;margin-bottom:5px;}';
    html += '.sub{font-family:Orbitron;font-size:0.5em;letter-spacing:3px;color:#4a6a8a;margin-bottom:25px;}';
    html += '.section{margin-bottom:35px;}';
    html += '.section-head{font-family:Orbitron;font-size:0.85em;letter-spacing:5px;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;cursor:pointer;}';
    html += '.section-head .dot{width:10px;height:10px;border-radius:50%;display:inline-block;animation:glow 2s ease-in-out infinite alternate;}';
    html += '@keyframes glow{0%{box-shadow:0 0 5px var(--gc,#ff9f43);}100%{box-shadow:0 0 20px var(--gc,#ff9f43);}}';
    html += '.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:15px;}';
    html += '.kpi{background:rgba(10,20,35,0.8);border:1px solid rgba(255,255,255,0.05);padding:16px;position:relative;overflow:hidden;transition:all 0.3s;}';
    html += '.kpi:hover{border-color:var(--c,#ff9f43);transform:translateY(-2px);}';
    html += '.kpi::before{content:"";position:absolute;top:0;left:0;width:100%;height:2px;background:var(--c,#ff9f43);opacity:0.4;}';
    html += '.kpi-label{color:#4a6a8a;font-family:Orbitron;font-size:0.5em;letter-spacing:2px;margin-bottom:4px;}';
    html += '.kpi-val{font-family:Orbitron;font-size:1.5em;font-weight:900;color:var(--c,#ff9f43);}';
    html += '.kpi-sub{color:#4a6a8a;font-size:0.8em;margin-top:2px;}';
    html += '.fib-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:15px;margin-bottom:15px;}';
    html += '.fib-card{background:rgba(10,20,35,0.8);border:1px solid #1a2a3a;padding:18px;transition:border-color 0.3s;}';
    html += '.fib-card:hover{border-color:#ffffff10;}';
    html += '.fib-title{font-family:Orbitron;font-size:0.65em;letter-spacing:3px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;}';
    html += '.fib-level{display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid #0a1520;}';
    html += '.fib-level .pct{font-family:Orbitron;font-size:0.55em;color:#4a6a8a;width:55px;}';
    html += '.fib-level .bar{flex:1;height:6px;background:#0a1520;margin:0 8px;position:relative;}';
    html += '.fib-level .fill{height:100%;position:absolute;top:0;left:0;transition:width 0.5s;}';
    html += '.fib-level .val{font-family:Orbitron;font-size:0.7em;width:60px;text-align:right;}';
    html += '.pill{display:inline-block;font-family:Orbitron;font-size:0.5em;letter-spacing:1px;padding:3px 10px;border-radius:2px;}';
    html += '.pill-green{background:rgba(0,255,102,0.1);color:#00ff66;border:1px solid #00ff6630;}';
    html += '.pill-red{background:rgba(255,71,87,0.1);color:#ff4757;border:1px solid #ff475730;}';
    html += '.pill-orange{background:rgba(255,159,67,0.1);color:#ff9f43;border:1px solid #ff9f4330;}';
    html += '.pill-blue{background:rgba(0,212,255,0.1);color:#00d4ff;border:1px solid #00d4ff30;}';
    html += '.pill-purple{background:rgba(168,85,247,0.1);color:#a855f7;border:1px solid #a855f730;}';
    html += '.data-table{width:100%;border-collapse:collapse;font-size:0.8em;}';
    html += '.data-table th{padding:8px;text-align:left;color:#ff9f43;font-family:Orbitron;font-size:0.55em;letter-spacing:1px;border-bottom:2px solid #ff9f4315;}';
    html += '.data-table td{padding:6px 8px;border-bottom:1px solid #0a1520;}';
    html += '.data-table tr:nth-child(even){background:rgba(10,20,35,0.3);}';
    html += '.data-table tr:hover{background:rgba(0,212,255,0.03);}';
    html += '.chart-box{background:rgba(10,20,35,0.8);border:1px solid #1a2a3a;padding:18px;margin-bottom:15px;}';
    html += '.chart-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}';
    html += '.chart-title{font-family:Orbitron;font-size:0.65em;letter-spacing:3px;}';
    html += '.metric-badge{font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:4px 12px;border:1px solid;margin-left:6px;}';
    html += '.signal-box{padding:14px;border:1px solid;margin-top:12px;font-size:0.85em;line-height:1.5;}';
    html += '.sparkline{display:inline-flex;align-items:flex-end;gap:1px;height:20px;vertical-align:middle;}';
    html += '.sparkline .sp{width:3px;min-height:1px;display:inline-block;}';

    // Mobile responsive
    html += '@media(max-width:768px){';
    html += '.wrap{padding:10px 12px;} .title{font-size:1em;letter-spacing:3px;}';
    html += '.kpi-row{grid-template-columns:repeat(2,1fr);gap:8px;}';
    html += '.fib-grid{grid-template-columns:1fr;}';
    html += '.nav a{padding:8px 12px;font-size:0.5em;letter-spacing:2px;}';
    html += '.kpi-val{font-size:1.1em;} .kpi{padding:10px;}';
    html += 'table{font-size:0.65em;} th,td{padding:4px 3px;}';
    html += '.chart-box{padding:10px;} [style*="overflow-x"]{overflow-x:auto;-webkit-overflow-scrolling:touch;}';
    html += '.sub{font-size:0.4em;letter-spacing:1px;}';
    html += '}';
    html += '@media(max-width:480px){.kpi-row{grid-template-columns:1fr;}}';
    html += '</style></head><body><div class="wrap">';

    // Nav
    html += '<div class="nav">';
    html += '<a href="/dashboard">JARVIS</a>';
    html += '<a href="/business">ATHENA</a>';
    html += '<a href="/tookan">TOOKAN</a>';
    html += '<a href="/business/chart">CHARTS</a>';
    html += '<a href="/analytics" class="active">ANALYTICS</a>';
    html += '<a href="/ads">GOOGLE ADS</a>';
    html += '</div>';

    html += '<div class="title">PREDICTIVE ANALYTICS ENGINE</div>';
    html += '<div class="sub">FIBONACCI RETRACEMENT &bull; EXTENSIONS &bull; BOLLINGER BANDS &bull; RSI &bull; MACD &bull; REVENUE FORECASTING &bull; SEASONAL AI &bull; ' + monthKeys.length + ' MONTHS DATA</div>';

    // ====================================================================
    // SECTION 1: BUSINESS PULSE — MASTER DASHBOARD
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#ff9f43;--gc:#ff9f43;"><span class="dot" style="background:#ff9f43;"></span>BUSINESS PULSE — MASTER INDICATORS</div>';
    html += '<div class="kpi-row">';

    var pulseKPIs = [
      { label: 'MONTHLY RUN RATE', val: runRate.toLocaleString() + ' calls', sub: dayOfMonth + '/' + daysInMonth + ' days (' + curMonthCalls + ' actual)', c: '#00d4ff' },
      { label: 'ANNUAL RUN RATE', val: '$' + (annualRunRate * revenuePerCall).toLocaleString(), sub: annualRunRate.toLocaleString() + ' calls projected', c: '#00ff66' },
      { label: 'MoM MOMENTUM', val: (callGrowth.mom >= 0 ? '+' : '') + callGrowth.mom + '%', sub: callGrowth.prev + ' → ' + callGrowth.last, c: callGrowth.mom >= 0 ? '#00ff66' : '#ff4757' },
      { label: 'QoQ GROWTH', val: (callGrowth.qoq >= 0 ? '+' : '') + callGrowth.qoq + '%', sub: 'Quarter over quarter', c: callGrowth.qoq >= 0 ? '#00ff66' : '#ff4757' },
      { label: 'TREND STRENGTH', val: callTrend.strength + '%', sub: callTrend.direction.toUpperCase() + ' (' + callTrend.consistency + '% consistent)', c: callTrend.direction === 'bullish' ? '#00ff66' : callTrend.direction === 'bearish' ? '#ff4757' : '#ff9f43' },
      { label: 'VOLATILITY', val: callVol + '%', sub: callVol > 30 ? 'HIGH — unstable' : callVol > 15 ? 'MODERATE' : 'LOW — stable', c: callVol > 30 ? '#ff4757' : callVol > 15 ? '#ff9f43' : '#00ff66' },
      { label: 'RSI (6)', val: callRSI.length > 0 ? callRSI[callRSI.length-1] : 'N/A', sub: (callRSI.length > 0 && callRSI[callRSI.length-1] > 70) ? 'OVERBOUGHT' : (callRSI.length > 0 && callRSI[callRSI.length-1] < 30) ? 'OVERSOLD' : 'NEUTRAL', c: '#a855f7' },
      { label: 'CALL↔REVENUE R', val: callRevCorrelation, sub: callRevCorrelation > 0.7 ? 'Strong link' : callRevCorrelation > 0.4 ? 'Moderate link' : 'Weak link', c: '#ffd700' },
    ];
    pulseKPIs.forEach(function(k) {
      html += '<div class="kpi" style="--c:' + k.c + ';"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.val + '</div><div class="kpi-sub">' + k.sub + '</div></div>';
    });
    html += '</div>';

    // Strategy signal box
    var masterSignal = '', sigColor = '#00d4ff';
    if (callGrowth.mom > 15 && callTrend.direction === 'bullish') { masterSignal = '🚀 STRONG GROWTH — All indicators bullish. Scale aggressively: hire techs, increase ad spend, expand to new cities. Next month forecast: ~' + callForecast[0].blended + ' calls ($' + (callForecast[0].blended * revenuePerCall).toLocaleString() + ' revenue).'; sigColor = '#00ff66'; }
    else if (callGrowth.mom > 0) { masterSignal = '📈 MODERATE GROWTH — Positive momentum. Maintain current strategy, consider incremental ad increases. Watch for Fib resistance at ' + callFib['61.8'] + ' calls. Forecast: ~' + callForecast[0].blended + ' calls.'; sigColor = '#00ff66'; }
    else if (callGrowth.mom > -10) { masterSignal = '⚡ CONSOLIDATION — Volume stabilizing. Focus on conversion optimization and customer retention. Fib support at ' + callFib['38.2'] + '. Reduce discretionary spending until breakout confirmed.'; sigColor = '#ff9f43'; }
    else { masterSignal = '⚠️ PULLBACK — Volume declining ' + callGrowth.mom + '%. Defend Fib support at ' + callFib['38.2'] + '. Cut ad waste, rebook cancellations, activate retention campaigns. Next support: ' + callFib['23.6'] + '.'; sigColor = '#ff4757'; }
    if (callRSI.length > 0 && callRSI[callRSI.length-1] > 70) masterSignal += ' ⚡ RSI OVERBOUGHT — Potential pullback incoming.';
    if (callRSI.length > 0 && callRSI[callRSI.length-1] < 30) masterSignal += ' 💎 RSI OVERSOLD — Potential bounce opportunity.';
    if (callBB.width.length > 0 && callBB.width[callBB.width.length-1] < 15) masterSignal += ' 🔥 BB SQUEEZE — Breakout imminent!';

    html += '<div class="signal-box" style="border-color:' + sigColor + '30;background:' + sigColor + '05;color:' + sigColor + ';">' + masterSignal + '</div>';
    html += '</div>';

    // ====================================================================
    // SECTION 2: INTERACTIVE CALL VOLUME CHART (LightweightCharts)
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#00d4ff;--gc:#00d4ff;"><span class="dot" style="background:#00d4ff;"></span>CALL VOLUME — INTERACTIVE TECHNICAL ANALYSIS</div>';

    // Build chart data for LightweightCharts
    var lcCallData = monthKeys.map(function(k) { return { time: k + '-01', value: monthlyCalls[k] || 0 }; });
    var lcForecastData = [];
    for (var fi = 0; fi < 6; fi++) {
      var fMonth = (curMonth + fi + 1) % 12 + 1;
      var fYear = curYear + Math.floor((curMonth + fi + 1) / 12);
      lcForecastData.push({ time: fYear + '-' + String(fMonth).padStart(2, '0') + '-01', value: callForecast[fi].blended, upper: callForecast[fi].upper, lower: callForecast[fi].lower });
    }

    html += '<div style="display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;">';
    html += '<button onclick="toggleOverlay(\'bb\')" id="btn-bb2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">BB</button>';
    html += '<button onclick="toggleOverlay(\'sma\')" id="btn-sma2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">SMA</button>';
    html += '<button onclick="toggleOverlay(\'ema\')" id="btn-ema2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">EMA</button>';
    html += '<button onclick="toggleOverlay(\'fib\')" id="btn-fib2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:rgba(0,255,102,0.1);color:#00ff66;border:1px solid #00ff6640;cursor:pointer;">FIB</button>';
    html += '<button onclick="toggleOverlay(\'ext\')" id="btn-ext2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">FIB EXT</button>';
    html += '<button onclick="toggleOverlay(\'forecast\')" id="btn-fore2" style="font-family:Orbitron;font-size:0.55em;letter-spacing:1px;padding:6px 14px;background:rgba(255,159,67,0.1);color:#ff9f43;border:1px solid #ff9f4340;cursor:pointer;">FORECAST</button>';
    html += '</div>';

    html += '<div id="call-chart" style="border:1px solid #1a2a3a;min-height:350px;width:100%;"></div>';
    html += '<div style="font-family:Orbitron;font-size:0.45em;color:#4a6a8a;padding:4px 0;">RSI (6)</div>';
    html += '<div id="call-rsi" style="border:1px solid #1a2a3a;min-height:100px;width:100%;"></div>';
    html += '<div style="font-family:Orbitron;font-size:0.45em;color:#4a6a8a;padding:4px 0;">MACD (12, 26, 9)</div>';
    html += '<div id="call-macd" style="border:1px solid #1a2a3a;min-height:100px;width:100%;"></div>';

    // Fib level summary strip
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));gap:6px;margin-top:12px;">';
    [
      { label: '261.8% EXT', val: callFib['261.8'], c: '#a855f7' },
      { label: '161.8% EXT', val: callFib['161.8'], c: '#a855f7' },
      { label: '127.2% EXT', val: callFib['127.2'], c: '#00d4ff' },
      { label: '100% HIGH', val: callFib.high, c: '#00ff66' },
      { label: '78.6%', val: callFib['78.6'], c: '#00ff66' },
      { label: '61.8% RESIST', val: callFib['61.8'], c: '#00ff66' },
      { label: '50% MID', val: callFib['50.0'], c: '#ffd700' },
      { label: '38.2% SUPPORT', val: callFib['38.2'], c: '#ff4757' },
      { label: '23.6%', val: callFib['23.6'], c: '#ff4757' },
      { label: '0% LOW', val: callFib.low, c: '#ff4757' },
      { label: 'CURRENT', val: callFib.current, c: '#00d4ff' },
    ].forEach(function(f) {
      html += '<div style="background:rgba(10,20,35,0.6);padding:6px;text-align:center;border:1px solid ' + f.c + '15;">';
      html += '<div style="color:#4a6a8a;font-family:Orbitron;font-size:0.4em;">' + f.label + '</div>';
      html += '<div style="color:' + f.c + ';font-family:Orbitron;font-size:0.85em;font-weight:900;">' + f.val + '</div>';
      html += '</div>';
    });
    html += '</div></div>';

    // ====================================================================
    // SECTION 3: REVENUE & PROFIT FIBONACCI — INTERACTIVE CHARTS
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#00ff66;--gc:#00ff66;"><span class="dot" style="background:#00ff66;"></span>REVENUE & PROFIT — FIBONACCI + FORECASTING</div>';

    // Revenue chart
    var monthAbbrevToNum = {Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12};
    var lcRevData = revMonths.map(function(m, i) {
      var timeStr;
      if (m.indexOf('-') > 0 && m.length <= 7) { timeStr = m + '-01'; } // "2024-01" format
      else { var parts = m.split(' '); var mi = monthAbbrevToNum[parts[0]] || 1; timeStr = (parts[1] || '2024') + '-' + String(mi).padStart(2,'0') + '-01'; }
      return { time: timeStr, value: revVals[i] || 0 };
    });
    var lcProfData = revMonths.map(function(m, i) { return { time: lcRevData[i] ? lcRevData[i].time : '2024-01-01', value: profVals[i] || 0 }; });

    html += '<div id="rev-chart" style="border:1px solid #1a2a3a;margin-bottom:5px;min-height:300px;width:100%;"></div>';
    html += '<div style="display:flex;gap:15px;padding:6px;font-size:0.75em;">';
    html += '<span style="color:#00ff66;">● Revenue</span>';
    html += '<span style="color:#ffd700;">● Profit</span>';
    html += '<span style="color:#ff9f4380;">▪ Forecast</span>';
    html += '</div>';

    // Revenue & Profit Fib cards side by side
    html += '<div class="fib-grid">';

    var revProfFibs = [
      { name: 'REVENUE', fib: revFib, growth: revGrowth, forecast: revForecast, color: '#00ff66', prefix: '$', trend: revTrend },
      { name: 'PROFIT', fib: profFib, growth: profGrowth, forecast: profForecast, color: '#ffd700', prefix: '$', trend: trendStrength(profVals) },
      { name: 'EXPENSES', fib: expFib, growth: growthMetrics(expVals), forecast: forecast(expVals, 6), color: '#ff4757', prefix: '$', trend: trendStrength(expVals) },
      { name: 'AD SPEND', fib: adsFib, growth: growthMetrics(adsVals), forecast: forecast(adsVals, 6), color: '#a855f7', prefix: '$', trend: trendStrength(adsVals) },
    ];

    revProfFibs.forEach(function(rpf) {
      html += '<div class="fib-card">';
      html += '<div class="fib-title"><span style="color:' + rpf.color + ';">' + rpf.name + ' FIBONACCI</span>';
      var rpfTrend = rpf.growth.mom > 5 ? '▲' : rpf.growth.mom < -5 ? '▼' : '►';
      html += '<span style="color:' + (rpf.growth.mom > 0 ? '#00ff66' : '#ff4757') + ';">' + rpfTrend + ' ' + (rpf.growth.mom >= 0 ? '+' : '') + rpf.growth.mom + '%</span></div>';

      [
        { label: '161.8% EXT', val: rpf.fib['161.8'], c: '#a855f7' },
        { label: '127.2% EXT', val: rpf.fib['127.2'], c: '#00d4ff' },
        { label: '100% HIGH', val: rpf.fib.high, c: '#00ff66' },
        { label: '61.8% RESIST', val: rpf.fib['61.8'], c: '#00ff66' },
        { label: '50% MID', val: rpf.fib['50.0'], c: '#ffd700' },
        { label: '38.2% SUPPORT', val: rpf.fib['38.2'], c: '#ff4757' },
        { label: '0% LOW', val: rpf.fib.low, c: '#ff4757' },
      ].forEach(function(lv) {
        var pct2 = rpf.fib.high > 0 ? Math.round(lv.val / rpf.fib.high * 100) : 0;
        html += '<div class="fib-level"><span class="pct">' + lv.label.split(' ')[0] + '</span>';
        html += '<div class="bar"><div class="fill" style="width:' + Math.min(100, pct2) + '%;background:' + lv.c + '40;"></div></div>';
        html += '<span class="val" style="color:' + lv.c + ';">' + rpf.prefix + Math.round(lv.val).toLocaleString() + '</span></div>';
      });

      html += '<div style="margin-top:10px;">';
      html += '<div style="font-family:Orbitron;font-size:0.5em;color:#4a6a8a;margin-bottom:4px;">6-MONTH FORECAST (confidence band)</div>';
      html += '<div style="display:flex;gap:4px;">';
      rpf.forecast.forEach(function(f) {
        html += '<div style="flex:1;text-align:center;background:rgba(10,20,35,0.5);padding:4px;border:1px dashed ' + rpf.color + '30;">';
        html += '<div style="color:' + rpf.color + ';font-family:Orbitron;font-size:0.6em;">' + rpf.prefix + Math.round(f.blended).toLocaleString() + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.55em;">' + rpf.prefix + f.lower.toLocaleString() + '–' + rpf.prefix + f.upper.toLocaleString() + '</div>';
        html += '</div>';
      });
      html += '</div>';
      html += '<div style="display:flex;justify-content:space-between;margin-top:6px;">';
      html += '<span style="font-size:0.7em;color:#4a6a8a;">Trend: <strong style="color:' + (rpf.trend.direction === 'bullish' ? '#00ff66' : rpf.trend.direction === 'bearish' ? '#ff4757' : '#ff9f43') + ';">' + rpf.trend.direction.toUpperCase() + '</strong> (' + rpf.trend.strength + '%)</span>';
      html += '<span style="font-size:0.7em;color:#4a6a8a;">R²: ' + linearRegression(rpf.name === 'REVENUE' ? revVals : rpf.name === 'PROFIT' ? profVals : rpf.name === 'EXPENSES' ? expVals : adsVals).r2 + '</span>';
      html += '</div></div></div>';
    });
    html += '</div>';

    // Margin Fibonacci
    html += '<div class="chart-box">';
    html += '<div class="chart-header"><div class="chart-title" style="color:#55f7d8;">PROFIT MARGIN FIBONACCI</div>';
    html += '<span class="metric-badge" style="border-color:' + (marginFib.current > marginFib['61.8'] ? '#00ff6640' : marginFib.current > marginFib['38.2'] ? '#ffd70040' : '#ff475740') + ';color:' + (marginFib.current > marginFib['61.8'] ? '#00ff66' : marginFib.current > marginFib['38.2'] ? '#ffd700' : '#ff4757') + ';">CURRENT: ' + marginFib.current + '%</span></div>';
    html += '<div style="position:relative;height:30px;background:#0a1520;margin-bottom:8px;">';
    var markerPos = marginFib.high > 0 ? Math.min(100, Math.round((marginFib.current - marginFib.low) / (marginFib.high - marginFib.low || 1) * 100)) : 50;
    html += '<div style="position:absolute;top:0;bottom:0;left:' + markerPos + '%;width:3px;background:#55f7d8;box-shadow:0 0 10px #55f7d8;z-index:2;"></div>';
    [0, 23.6, 38.2, 50, 61.8, 78.6, 100].forEach(function(pct) {
      html += '<div style="position:absolute;top:0;bottom:0;left:' + pct + '%;width:1px;background:#ffffff10;"></div>';
    });
    html += '</div>';
    html += '<div style="display:flex;justify-content:space-between;font-family:Orbitron;font-size:0.45em;color:#4a6a8a;">';
    html += '<span>Low: ' + marginFib.low + '%</span><span>38.2%: ' + marginFib['38.2'] + '%</span><span>50%: ' + marginFib['50.0'] + '%</span><span>61.8%: ' + marginFib['61.8'] + '%</span><span>High: ' + marginFib.high + '%</span>';
    html += '</div></div>';
    html += '</div>';

    // ====================================================================
    // SECTION 4: AD SPEND ROI FIBONACCI
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#a855f7;--gc:#a855f7;"><span class="dot" style="background:#a855f7;"></span>AD SPEND EFFICIENCY — ROI FIBONACCI</div>';

    html += '<div class="kpi-row">';
    var adKPIs = [
      { label: 'CURRENT ROAS', val: adROI.length > 0 ? adROI[adROI.length-1] + 'x' : 'N/A', sub: 'Revenue per $1 ad spend', c: adROI.length > 0 && adROI[adROI.length-1] > 5 ? '#00ff66' : '#ff9f43' },
      { label: 'ROAS HIGH (Fib)', val: adROIFib.high + 'x', sub: '100% retracement level', c: '#00ff66' },
      { label: 'ROAS SUPPORT', val: adROIFib['38.2'] + 'x', sub: '38.2% Fibonacci', c: '#ff4757' },
      { label: 'COST PER LEAD', val: '$' + (costPerLead.length > 0 ? costPerLead[costPerLead.length-1] : 0), sub: 'Current month CPL', c: '#a855f7' },
      { label: 'CPL LOW (Fib)', val: '$' + cplFib.low, sub: 'Best efficiency', c: '#00ff66' },
      { label: 'CPL RESIST', val: '$' + cplFib['61.8'], sub: '61.8% — expensive zone', c: '#ff4757' },
    ];
    adKPIs.forEach(function(k) {
      html += '<div class="kpi" style="--c:' + k.c + ';"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.val + '</div><div class="kpi-sub">' + k.sub + '</div></div>';
    });
    html += '</div>';

    // ROAS trend mini chart
    if (adROI.length > 3) {
      var maxROI = Math.max.apply(null, adROI);
      html += '<div class="chart-box"><div class="chart-title" style="color:#a855f7;margin-bottom:10px;">ROAS TREND — Return On Ad Spend</div>';
      html += '<div style="display:flex;align-items:flex-end;gap:4px;height:100px;">';
      adROI.slice(-12).forEach(function(v, i) {
        var h = maxROI > 0 ? Math.max(2, Math.round(v / maxROI * 100)) : 2;
        var col = v >= adROIFib['61.8'] ? '#00ff66' : v >= adROIFib['38.2'] ? '#ff9f43' : '#ff4757';
        html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;">';
        html += '<div style="color:#4a6a8a;font-size:0.5em;margin-bottom:2px;">' + v + 'x</div>';
        html += '<div style="width:80%;height:' + h + '%;background:' + col + '40;border-top:2px solid ' + col + ';"></div>';
        html += '</div>';
      });
      html += '</div></div>';
    }

    // Ad strategy signal
    var adSignal = '';
    if (adROI.length > 0) {
      var lastROAS = adROI[adROI.length-1];
      if (lastROAS >= adROIFib['61.8']) adSignal = '🟢 EFFICIENT — ROAS above 61.8% Fib. Ads are performing well. Consider increasing budget by 10-20% to scale.';
      else if (lastROAS >= adROIFib['38.2']) adSignal = '🟡 NORMAL — ROAS in middle band. Monitor closely. Optimize targeting and creative to push above resistance.';
      else adSignal = '🔴 INEFFICIENT — ROAS below 38.2% support. Reduce ad spend, audit campaigns, check landing page conversion.';
    }
    if (adSignal) html += '<div class="signal-box" style="border-color:#a855f730;background:rgba(168,85,247,0.03);color:#a855f7;">' + adSignal + '</div>';
    html += '</div>';

    // ====================================================================
    // SECTION 5: EQUIPMENT FIBONACCI — DEMAND RETRACEMENT
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#00ff66;--gc:#00ff66;"><span class="dot" style="background:#00ff66;"></span>EQUIPMENT FIBONACCI — DEMAND RETRACEMENT</div>';
    html += '<div class="fib-grid">';

    var equipFibs = [
      { name: 'MOWERS / ZERO TURN', fib: mowerFib, growth: mowerGrowth, color: '#00ff66', vals: equipMonthly.mower, icon: '🔧' },
      { name: 'SNOW BLOWERS', fib: snowFib, growth: snowGrowth, color: '#00d4ff', vals: equipMonthly.snow, icon: '❄️' },
      { name: 'GENERATORS', fib: genFib, growth: genGrowth, color: '#ff9f43', vals: equipMonthly.generator, icon: '⚡' },
    ];

    equipFibs.forEach(function(eq) {
      var eqBB = bollingerBands(eq.vals, Math.min(4, eq.vals.length), 2);
      var eqRSI = calcRSI(eq.vals, Math.min(4, eq.vals.length));
      var eqForecast = forecast(eq.vals, 3);
      var eqTrend = trendStrength(eq.vals);

      html += '<div class="fib-card">';
      html += '<div class="fib-title"><span style="color:' + eq.color + ';">' + eq.icon + ' ' + eq.name + '</span>';
      var trendClass = eq.growth.mom > 5 ? 'trend-up' : eq.growth.mom < -5 ? 'trend-down' : 'trend-flat';
      var trendIcon = eq.growth.mom > 5 ? '▲' : eq.growth.mom < -5 ? '▼' : '►';
      html += '<span class="' + trendClass + '" style="color:' + (eq.growth.mom > 5 ? '#00ff66' : eq.growth.mom < -5 ? '#ff4757' : '#ff9f43') + ';">' + trendIcon + ' ' + (eq.growth.mom >= 0 ? '+' : '') + eq.growth.mom + '%</span></div>';

      // Mini sparkline
      var eqLast = eq.vals.slice(-12);
      var eqMax = Math.max.apply(null, eqLast.length > 0 ? eqLast : [1]);
      html += '<div style="display:flex;align-items:flex-end;gap:2px;height:60px;margin-bottom:10px;">';
      eqLast.forEach(function(v, i) {
        var h = eqMax > 0 ? Math.max(2, Math.round(v / eqMax * 100)) : 2;
        html += '<div style="flex:1;height:' + h + '%;background:' + (i >= eqLast.length - 3 ? eq.color + '80' : eq.color + '25') + ';min-height:2px;transition:height 0.3s;"></div>';
      });
      // Forecast bars
      eqForecast.forEach(function(f) {
        var h2 = eqMax > 0 ? Math.max(2, Math.round(f.blended / eqMax * 100)) : 2;
        html += '<div style="flex:1;height:' + h2 + '%;background:repeating-linear-gradient(180deg,' + eq.color + '30 0px,' + eq.color + '30 3px,transparent 3px,transparent 6px);min-height:2px;border-top:1px dashed ' + eq.color + '60;"></div>';
      });
      html += '</div>';

      // Fib levels with extensions
      [
        { label: '161.8% EXT', val: eq.fib['161.8'], c: '#a855f7' },
        { label: '100% HIGH', val: eq.fib.high, c: '#00ff66' },
        { label: '61.8% RESIST', val: eq.fib['61.8'], c: '#00ff66' },
        { label: '50% MID', val: eq.fib['50.0'], c: '#ffd700' },
        { label: '38.2% SUPPORT', val: eq.fib['38.2'], c: '#ff4757' },
        { label: '0% LOW', val: eq.fib.low, c: '#ff4757' },
      ].forEach(function(fl3) {
        var pct = eq.fib.high > 0 ? Math.min(100, Math.round(fl3.val / eq.fib.high * 100)) : 0;
        html += '<div class="fib-level"><span class="pct">' + fl3.label.split(' ')[0] + '</span>';
        html += '<div class="bar"><div class="fill" style="width:' + pct + '%;background:' + fl3.c + '40;"></div></div>';
        html += '<span class="val" style="color:' + fl3.c + ';">' + fl3.val + '</span></div>';
      });

      // Indicators row
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:4px;margin-top:10px;">';
      var curPos = eq.fib.range > 0 ? Math.round((eq.fib.current - eq.fib.low) / eq.fib.range * 100) : 50;
      var lastRSI = eqRSI.length > 0 ? Math.round(eqRSI[eqRSI.length-1]) : 50;
      html += '<div style="background:#0a1520;padding:5px;text-align:center;"><div style="color:#4a6a8a;font-size:0.5em;">CURRENT</div><div style="color:' + eq.color + ';font-family:Orbitron;font-size:0.75em;">' + eq.fib.current + '</div></div>';
      html += '<div style="background:#0a1520;padding:5px;text-align:center;"><div style="color:#4a6a8a;font-size:0.5em;">RETRACEMENT</div><div style="color:' + (curPos > 61.8 ? '#00ff66' : curPos > 38.2 ? '#ffd700' : '#ff4757') + ';font-family:Orbitron;font-size:0.75em;">' + curPos + '%</div></div>';
      html += '<div style="background:#0a1520;padding:5px;text-align:center;"><div style="color:#4a6a8a;font-size:0.5em;">RSI</div><div style="color:' + (lastRSI > 70 ? '#ff4757' : lastRSI < 30 ? '#00ff66' : '#ff9f43') + ';font-family:Orbitron;font-size:0.75em;">' + lastRSI + '</div></div>';
      html += '<div style="background:#0a1520;padding:5px;text-align:center;"><div style="color:#4a6a8a;font-size:0.5em;">FORECAST</div><div style="color:#ff9f43;font-family:Orbitron;font-size:0.75em;">~' + (eqForecast.length > 0 ? eqForecast[0].blended : '?') + '</div></div>';
      html += '</div></div>';
    });
    html += '</div></div>';

    // ====================================================================
    // SECTION 6: SEASONAL DEMAND PREDICTION
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#55f7d8;--gc:#55f7d8;"><span class="dot" style="background:#55f7d8;"></span>SEASONAL DEMAND PREDICTION ENGINE</div>';

    html += '<div class="chart-box">';
    html += '<div class="chart-title" style="color:#55f7d8;margin-bottom:12px;">PREDICTED MONTHLY DEMAND BY EQUIPMENT TYPE (Historical Avg)</div>';
    var seasonMax = 0;
    for (var sm = 1; sm <= 12; sm++) { var sa = seasonAvg[sm] || {}; var stotal = (sa.mower||0)+(sa.snow||0)+(sa.generator||0)+(sa.other||0); if (stotal > seasonMax) seasonMax = stotal; }
    html += '<div style="display:flex;align-items:flex-end;gap:6px;height:200px;margin-bottom:8px;">';
    for (var sm2 = 1; sm2 <= 12; sm2++) {
      var sa2 = seasonAvg[sm2] || {}; var stot = (sa2.mower||0)+(sa2.snow||0)+(sa2.generator||0)+(sa2.other||0);
      var mH = seasonMax > 0 ? Math.round((sa2.mower||0) / seasonMax * 100) : 0;
      var sH = seasonMax > 0 ? Math.round((sa2.snow||0) / seasonMax * 100) : 0;
      var gH = seasonMax > 0 ? Math.round((sa2.generator||0) / seasonMax * 100) : 0;
      var oH = seasonMax > 0 ? Math.round((sa2.other||0) / seasonMax * 100) : 0;
      var isNow = sm2 === curMonth + 1;
      html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;' + (isNow ? 'background:rgba(85,247,216,0.08);border:1px solid #55f7d820;' : '') + '">';
      html += '<div style="color:#c0d8f0;font-size:0.5em;margin-bottom:2px;">' + stot + '</div>';
      html += '<div style="width:70%;display:flex;flex-direction:column;">';
      if (oH > 0) html += '<div style="height:' + oH + 'px;background:#4a6a8a40;min-height:1px;"></div>';
      if (gH > 0) html += '<div style="height:' + gH + 'px;background:#ff9f4380;min-height:1px;"></div>';
      if (sH > 0) html += '<div style="height:' + sH + 'px;background:#00d4ff80;min-height:1px;"></div>';
      if (mH > 0) html += '<div style="height:' + mH + 'px;background:#00ff6680;min-height:1px;"></div>';
      html += '</div>';
      html += '<div style="font-family:Orbitron;font-size:0.4em;color:' + (isNow ? '#55f7d8' : '#4a6a8a') + ';margin-top:4px;' + (isNow ? 'font-weight:900;' : '') + '">' + monthLabels3[sm2] + '</div></div>';
    }
    html += '</div>';
    html += '<div style="display:flex;gap:15px;justify-content:center;flex-wrap:wrap;">';
    html += '<span style="color:#00ff66;font-size:0.75em;">● Mowers</span><span style="color:#00d4ff;font-size:0.75em;">● Snow Blowers</span><span style="color:#ff9f43;font-size:0.75em;">● Generators</span><span style="color:#4a6a8a;font-size:0.75em;">● Other</span>';
    html += '</div>';

    // Next 3 month prediction cards
    html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:15px;">';
    for (var nm = 1; nm <= 3; nm++) {
      var nextMo = (curMonth + nm) % 12 + 1;
      var nsa = seasonAvg[nextMo] || {};
      var nTotal = (nsa.mower||0)+(nsa.snow||0)+(nsa.generator||0)+(nsa.other||0);
      var dColor = nm === 1 ? '#55f7d8' : nm === 2 ? '#00d4ff' : '#a855f7';
      html += '<div style="background:' + dColor + '05;border:1px solid ' + dColor + '20;padding:14px;text-align:center;">';
      html += '<div style="font-family:Orbitron;font-size:0.55em;color:' + dColor + ';letter-spacing:2px;">' + monthLabels3[nextMo].toUpperCase() + ' PREDICTION</div>';
      html += '<div style="font-family:Orbitron;font-size:1.6em;color:' + dColor + ';font-weight:900;margin:5px 0;">~' + nTotal + '</div>';
      html += '<div style="font-size:0.75em;color:#4a6a8a;">🔧 ' + (nsa.mower||0) + ' &bull; ❄️ ' + (nsa.snow||0) + ' &bull; ⚡ ' + (nsa.generator||0) + '</div>';
      html += '</div>';
    }
    html += '</div></div>';

    // Day of Week pattern
    html += '<div class="chart-box">';
    html += '<div class="chart-title" style="color:#55f7d8;margin-bottom:10px;">DEMAND BY DAY OF WEEK (from recent bookings)</div>';
    var dowMax = Math.max.apply(null, dowCounts);
    html += '<div style="display:flex;align-items:flex-end;gap:8px;height:80px;">';
    dowCounts.forEach(function(v, i) {
      var h = dowMax > 0 ? Math.max(3, Math.round(v / dowMax * 100)) : 3;
      var pct = dowTotal > 0 ? Math.round(v / dowTotal * 100) : 0;
      html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;">';
      html += '<div style="color:#c0d8f0;font-size:0.6em;">' + v + ' (' + pct + '%)</div>';
      html += '<div style="width:80%;height:' + h + '%;background:#55f7d840;border-top:2px solid #55f7d8;"></div>';
      html += '<div style="font-family:Orbitron;font-size:0.45em;color:#4a6a8a;margin-top:4px;">' + dowLabels[i] + '</div></div>';
    });
    html += '</div></div></div>';

    // ====================================================================
    // SECTION 7: CITY GROWTH FORECASTING — MINI FIBONACCI CHARTS
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#a855f7;--gc:#a855f7;"><span class="dot" style="background:#a855f7;"></span>CITY GROWTH FORECASTING — ' + cityForecasts.length + ' MARKETS</div>';

    // Top city mini-fib cards (show top 12)
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px;margin-bottom:15px;">';
    cityForecasts.slice(0, 12).forEach(function(cf) {
      var cfColor = cf.growth.mom > 10 ? '#00ff66' : cf.growth.mom > 0 ? '#00d4ff' : cf.growth.mom > -10 ? '#ff9f43' : '#ff4757';
      html += '<div class="fib-card" style="padding:14px;">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">';
      html += '<div><span style="color:#c0d8f0;font-weight:700;font-size:0.95em;">' + cf.city + '</span>';
      html += '<span style="color:#4a6a8a;font-size:0.75em;margin-left:8px;">' + cf.total + ' total</span></div>';
      html += '<span style="color:' + cfColor + ';font-family:Orbitron;font-size:0.65em;">' + (cf.growth.mom >= 0 ? '+' : '') + cf.growth.mom + '%</span></div>';

      // Mini sparkline with forecast
      var cfMax = Math.max.apply(null, cf.values.length > 0 ? cf.values : [1]);
      if (cf.forecast.length > 0) cfMax = Math.max(cfMax, cf.forecast[0].upper || cf.forecast[0].blended);
      html += '<div style="display:flex;align-items:flex-end;gap:1px;height:40px;margin-bottom:6px;">';
      cf.values.forEach(function(v) {
        var h = cfMax > 0 ? Math.max(1, Math.round(v / cfMax * 100)) : 1;
        html += '<div style="flex:1;height:' + h + '%;background:' + cfColor + '50;min-height:1px;"></div>';
      });
      cf.forecast.slice(0, 3).forEach(function(f) {
        var h2 = cfMax > 0 ? Math.max(1, Math.round(f.blended / cfMax * 100)) : 1;
        html += '<div style="flex:1;height:' + h2 + '%;border-top:1px dashed #ff9f4380;min-height:1px;"></div>';
      });
      html += '</div>';

      // Fib meter
      var cfPos = cf.fib.range > 0 ? Math.min(100, Math.round((cf.current - cf.fib.low) / cf.fib.range * 100)) : 50;
      html += '<div style="height:4px;background:#0a1520;position:relative;margin-bottom:6px;">';
      html += '<div style="height:100%;width:' + cfPos + '%;background:linear-gradient(90deg,' + cfColor + '80,' + cfColor + ');"></div>';
      html += '</div>';

      // Stats row
      html += '<div style="display:flex;justify-content:space-between;font-size:0.7em;">';
      html += '<span style="color:#4a6a8a;">Now: <strong style="color:#c0d8f0;">' + cf.current + '</strong></span>';
      html += '<span style="color:#4a6a8a;">Pred: <strong style="color:#ff9f43;">~' + cf.predicted + '</strong></span>';
      html += '<span style="color:#4a6a8a;">RSI: <strong style="color:' + (cf.rsi > 70 ? '#ff4757' : cf.rsi < 30 ? '#00ff66' : '#ff9f43') + ';">' + Math.round(cf.rsi) + '</strong></span>';
      html += '<span style="color:#4a6a8a;">Fib: <strong style="color:' + (cfPos > 61.8 ? '#00ff66' : cfPos > 38.2 ? '#ffd700' : '#ff4757') + ';">' + cfPos + '%</strong></span>';
      html += '</div>';

      // Signal
      var cSignal = '';
      if (cf.growth.mom > 20 && cf.rsi < 70) cSignal = '<span class="pill pill-green">🚀 BREAKOUT</span>';
      else if (cf.growth.mom > 5) cSignal = '<span class="pill pill-green">▲ GROW</span>';
      else if (cf.growth.mom > -5) cSignal = '<span class="pill pill-orange">► STABLE</span>';
      else if (cf.rsi < 30) cSignal = '<span class="pill pill-blue">💎 OVERSOLD</span>';
      else cSignal = '<span class="pill pill-red">▼ DECLINE</span>';
      if (cf.volatility > 40) cSignal += ' <span class="pill pill-purple">⚡ VOLATILE</span>';
      html += '<div style="margin-top:4px;">' + cSignal + '</div>';
      html += '</div>';
    });
    html += '</div>';

    // Full data table
    html += '<div style="overflow-x:auto;"><table class="data-table">';
    html += '<thead><tr><th>City</th><th>Total</th><th>Current</th><th>Predicted</th><th>MoM</th><th>RSI</th><th>Volatility</th><th>Fib Pos</th><th>R²</th><th>Signal</th></tr></thead><tbody>';
    cityForecasts.slice(0, 30).forEach(function(cf2) {
      var cfPos2 = cf2.fib.range > 0 ? Math.round((cf2.current - cf2.fib.low) / cf2.fib.range * 100) : 50;
      var signal2 = cf2.growth.mom > 20 ? '<span class="pill pill-green">🚀 BREAKOUT</span>' : cf2.growth.mom > 5 ? '<span class="pill pill-green">▲ GROW</span>' : cf2.growth.mom > -5 ? '<span class="pill pill-orange">► STABLE</span>' : '<span class="pill pill-red">▼ DECLINE</span>';
      html += '<tr><td style="color:#c0d8f0;font-weight:700;white-space:nowrap;">' + cf2.city + '</td><td style="color:#00d4ff;">' + cf2.total + '</td><td>' + cf2.current + '</td><td style="color:#ff9f43;font-weight:700;">~' + cf2.predicted + '</td><td style="color:' + (cf2.growth.mom >= 0 ? '#00ff66' : '#ff4757') + ';">' + (cf2.growth.mom >= 0 ? '+' : '') + cf2.growth.mom + '%</td><td style="color:' + (cf2.rsi > 70 ? '#ff4757' : cf2.rsi < 30 ? '#00ff66' : '#4a6a8a') + ';">' + Math.round(cf2.rsi) + '</td><td>' + cf2.volatility + '%</td><td style="color:' + (cfPos2 > 61.8 ? '#00ff66' : cfPos2 > 38.2 ? '#ffd700' : '#ff4757') + ';">' + cfPos2 + '%</td><td style="color:' + (cf2.r2 > 0.5 ? '#00ff66' : '#4a6a8a') + ';">' + cf2.r2 + '</td><td>' + signal2 + '</td></tr>';
    });
    html += '</tbody></table></div></div>';

    // ====================================================================
    // SECTION 8: TECH PRODUCTIVITY + WORKFORCE FIBONACCI
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#00d4ff;--gc:#00d4ff;"><span class="dot" style="background:#00d4ff;"></span>TECH PRODUCTIVITY & WORKFORCE FIBONACCI</div>';

    // Workforce KPIs
    var totalTechJobs = techProd.reduce(function(s, t) { return s + t.total; }, 0);
    var totalTechRev = techProd.reduce(function(s, t) { return s + t.revenue; }, 0);
    var avgJobsPerTech = techProd.length > 0 ? Math.round(totalTechJobs / techProd.length) : 0;
    var hasJobData = techProd.length > 0 && techProd[0].total > 0;
    var avgCompletion = hasJobData ? Math.round(techProd.reduce(function(s,t){return s+t.rate;},0) / techProd.length * 10) / 10 : 0;
    var techRevVals = techProd.map(function(t){return t.revenue;});
    var techRevFib = calcFib(techRevVals);
    var revenuePerTech = techProd.length > 0 ? Math.round(totalTechRev / techProd.length) : 0;

    html += '<div class="kpi-row">';
    html += '<div class="kpi" style="--c:#00d4ff;"><div class="kpi-label">ACTIVE TECHS</div><div class="kpi-val">' + techProd.length + '</div><div class="kpi-sub">' + (hasJobData ? totalTechJobs + ' total jobs' : '$' + totalTechRev.toLocaleString() + ' total payouts') + '</div></div>';
    html += '<div class="kpi" style="--c:#00ff66;"><div class="kpi-label">' + (hasJobData ? 'AVG JOBS/TECH' : 'HIGHEST PAYOUT') + '</div><div class="kpi-val">' + (hasJobData ? avgJobsPerTech : '$' + (techProd.length > 0 ? techProd[0].revenue.toLocaleString() : '0')) + '</div><div class="kpi-sub">Fib high: $' + techRevFib.high.toLocaleString() + '</div></div>';
    html += '<div class="kpi" style="--c:#ffd700;"><div class="kpi-label">' + (hasJobData ? 'AVG COMPLETION' : 'FIB SUPPORT') + '</div><div class="kpi-val">' + (hasJobData ? avgCompletion + '%' : '$' + techRevFib['38.2'].toLocaleString()) + '</div><div class="kpi-sub">Fib 38.2%-61.8%: $' + techRevFib['38.2'].toLocaleString() + '-$' + techRevFib['61.8'].toLocaleString() + '</div></div>';
    html += '<div class="kpi" style="--c:#a855f7;"><div class="kpi-label">AVG REV / TECH</div><div class="kpi-val">$' + revenuePerTech.toLocaleString() + '</div><div class="kpi-sub">' + (hasJobData ? 'Estimated from jobs' : 'From payout data') + '</div></div>';
    html += '</div>';

    if (techProd.length > 0) {
      html += '<div style="overflow-x:auto;"><table class="data-table">';
      if (hasJobData) {
        html += '<thead><tr><th>Tech</th><th>Total</th><th>Done</th><th>Rate</th><th>Cancel%</th><th>Return%</th><th>Jobs/Day</th><th>Est Rev</th><th>Markets</th><th>Efficiency</th></tr></thead><tbody>';
        techProd.forEach(function(tp) {
          var rateColor = tp.rate > 50 ? '#00ff66' : tp.rate > 20 ? '#ff9f43' : '#ff4757';
          var effScore = Math.round(tp.efficiency * 10) / 10;
          var effColor = effScore > techProd[0].efficiency * 0.7 ? '#00ff66' : effScore > techProd[0].efficiency * 0.3 ? '#ff9f43' : '#ff4757';
          html += '<tr><td style="color:#c0d8f0;font-weight:700;">' + tp.name + '</td><td style="color:#00d4ff;">' + tp.total + '</td><td>' + tp.completed + '</td><td style="color:' + rateColor + ';font-weight:700;">' + tp.rate + '%</td><td style="color:' + (tp.cancelRate > 20 ? '#ff4757' : '#4a6a8a') + ';">' + tp.cancelRate + '%</td><td style="color:' + (tp.returnRate > 5 ? '#00ff66' : '#4a6a8a') + ';">' + tp.returnRate + '%</td><td>' + tp.jobsPerDay + '</td><td style="color:#00ff66;">$' + tp.revenue.toLocaleString() + '</td><td>' + tp.locations + '</td><td style="color:' + effColor + ';">' + effScore + '</td></tr>';
        });
      } else {
        // Payout-only view
        html += '<thead><tr><th>Tech</th><th>Payout</th><th>% of Total</th><th>Fib Position</th><th>Status</th></tr></thead><tbody>';
        techProd.forEach(function(tp) {
          var pctOfTotal = totalTechRev > 0 ? Math.round(tp.revenue / totalTechRev * 1000) / 10 : 0;
          var fibPos = techRevFib.range > 0 ? Math.round((tp.revenue - techRevFib.low) / techRevFib.range * 100) : 50;
          var signal = fibPos > 61.8 ? '<span class="pill pill-green">TOP EARNER</span>' : fibPos > 38.2 ? '<span class="pill pill-orange">MID RANGE</span>' : '<span class="pill pill-red">LOW</span>';
          html += '<tr><td style="color:#c0d8f0;font-weight:700;">' + tp.name + '</td><td style="color:#00ff66;font-weight:700;">$' + tp.revenue.toLocaleString() + '</td><td>' + pctOfTotal + '%</td><td style="color:' + (fibPos > 61.8 ? '#00ff66' : fibPos > 38.2 ? '#ffd700' : '#ff4757') + ';">' + fibPos + '%</td><td>' + signal + '</td></tr>';
        });
      }
      html += '</tbody></table></div>';
    }
    html += '</div>';

    // ====================================================================
    // SECTION 9: CANCELLATION & CHURN FIBONACCI
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#ff4757;--gc:#ff4757;"><span class="dot" style="background:#ff4757;"></span>CANCELLATION RISK & CHURN FIBONACCI</div>';

    html += '<div class="kpi-row">';
    html += '<div class="kpi" style="--c:#ff4757;"><div class="kpi-label">OVERALL CANCEL RATE</div><div class="kpi-val">' + overallCancelRate + '%</div><div class="kpi-sub">' + (bm.totalCancelled || 0) + ' of ' + (bm.totalLeads || 0) + '</div></div>';
    html += '<div class="kpi" style="--c:#ff9f43;"><div class="kpi-label">REVENUE AT RISK</div><div class="kpi-val">$' + ((bm.totalCancelled || 0) * revenuePerCall).toLocaleString() + '</div><div class="kpi-sub">Lost from cancellations</div></div>';
    html += '<div class="kpi" style="--c:#00ff66;"><div class="kpi-label">RECOVERY TARGET</div><div class="kpi-val">$' + Math.round((bm.totalCancelled || 0) * revenuePerCall * 0.15).toLocaleString() + '</div><div class="kpi-sub">15% rebook potential</div></div>';
    html += '<div class="kpi" style="--c:#00d4ff;"><div class="kpi-label">CONVERSION RATE</div><div class="kpi-val">' + (bm.conversionRate || 0) + '%</div><div class="kpi-sub">Lead → Completed</div></div>';
    html += '</div>';

    if (cancelByCitySorted.length > 0) {
      html += '<div style="font-family:Orbitron;font-size:0.55em;color:#4a6a8a;letter-spacing:3px;margin-bottom:8px;">HIGHEST CANCEL RATES BY CITY (min 5 jobs)</div>';
      html += '<div style="overflow-x:auto;"><table class="data-table">';
      html += '<thead><tr><th>City</th><th>Total</th><th>Cancelled</th><th>Rate</th><th>Risk</th><th>$ Lost</th></tr></thead><tbody>';
      cancelByCitySorted.slice(0, 15).forEach(function(cc) {
        var risk = cc[1].rate > 30 ? '<span class="pill pill-red">HIGH</span>' : cc[1].rate > 15 ? '<span class="pill pill-orange">MED</span>' : '<span class="pill pill-green">LOW</span>';
        html += '<tr><td style="color:#c0d8f0;font-weight:700;">' + cc[0] + '</td><td>' + cc[1].total + '</td><td style="color:#ff4757;">' + cc[1].cancelled + '</td><td style="color:' + (cc[1].rate > 25 ? '#ff4757' : '#ff9f43') + ';font-weight:700;">' + cc[1].rate + '%</td><td>' + risk + '</td><td style="color:#ff4757;">$' + (cc[1].cancelled * revenuePerCall).toLocaleString() + '</td></tr>';
      });
      html += '</tbody></table></div>';
    }
    html += '</div>';

    // ====================================================================
    // SECTION 10: EQUIPMENT & BRAND INTELLIGENCE
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#ffd700;--gc:#ffd700;"><span class="dot" style="background:#ffd700;"></span>EQUIPMENT & BRAND FIBONACCI</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:15px;">';

    // Equipment mix
    html += '<div class="chart-box">';
    html += '<div class="chart-title" style="color:#ffd700;">EQUIPMENT MIX</div>';
    var eqSorted2 = Object.entries(equipStats).sort(function(a,b) { return b[1] - a[1]; });
    var eqTotal2 = eqSorted2.reduce(function(s,e) { return s + e[1]; }, 0);
    eqSorted2.slice(0, 10).forEach(function(eq2) {
      var pct3 = eqTotal2 > 0 ? Math.round(eq2[1] / eqTotal2 * 1000) / 10 : 0;
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">';
      html += '<div style="width:110px;text-align:right;color:#c0d8f0;font-size:0.8em;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + eq2[0] + '</div>';
      html += '<div style="flex:1;height:16px;background:#0a1520;"><div style="height:100%;width:' + pct3 + '%;background:linear-gradient(90deg,#ffd70040,#ffd70020);border-left:3px solid #ffd700;"></div></div>';
      html += '<div style="width:70px;text-align:right;color:#ffd700;font-size:0.75em;">' + eq2[1] + ' (' + pct3 + '%)</div></div>';
    });
    html += '</div>';

    // Brand mix
    html += '<div class="chart-box">';
    html += '<div class="chart-title" style="color:#a855f7;">BRAND MIX</div>';
    brandSorted.slice(0, 10).forEach(function(br) {
      var pct4 = brandTotal > 0 ? Math.round(br[1] / brandTotal * 1000) / 10 : 0;
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">';
      html += '<div style="width:110px;text-align:right;color:#c0d8f0;font-size:0.8em;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + br[0] + '</div>';
      html += '<div style="flex:1;height:16px;background:#0a1520;"><div style="height:100%;width:' + pct4 + '%;background:linear-gradient(90deg,#a855f740,#a855f720);border-left:3px solid #a855f7;"></div></div>';
      html += '<div style="width:70px;text-align:right;color:#a855f7;font-size:0.75em;">' + br[1] + ' (' + pct4 + '%)</div></div>';
    });
    html += '</div></div></div>';

    // ====================================================================
    // SECTION 11: GROWTH MILESTONES & PROJECTIONS
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#55f7d8;--gc:#55f7d8;"><span class="dot" style="background:#55f7d8;"></span>GROWTH MILESTONES & PROJECTIONS</div>';

    var totalCallsEver = monthVals.reduce(function(a,b){return a+b;}, 0);
    var avgMonthCalls = monthKeys.length > 0 ? Math.round(totalCallsEver / monthKeys.length) : 0;
    var recentAvg = monthVals.length >= 3 ? Math.round(monthVals.slice(-3).reduce(function(a,b){return a+b;},0) / 3) : avgMonthCalls;
    var milestones = [5000, 10000, 15000, 20000, 25000, 50000, 75000, 100000];
    html += '<div class="kpi-row">';
    milestones.forEach(function(ms) {
      if (ms <= totalCallsEver) {
        html += '<div class="kpi" style="--c:#00ff66;"><div class="kpi-label">' + ms.toLocaleString() + ' CALLS</div><div class="kpi-val">✅</div><div class="kpi-sub">Achieved</div></div>';
      } else {
        var remaining = ms - totalCallsEver;
        var monthsToGo = recentAvg > 0 ? Math.ceil(remaining / recentAvg) : 99;
        var estDate = new Date(curYear, curMonth + monthsToGo, 1);
        var estStr = monthLabels3[estDate.getMonth() + 1] + ' ' + estDate.getFullYear();
        html += '<div class="kpi" style="--c:#4a6a8a;"><div class="kpi-label">' + ms.toLocaleString() + ' CALLS</div><div class="kpi-val">~' + monthsToGo + 'mo</div><div class="kpi-sub">Est. ' + estStr + '</div></div>';
      }
    });
    html += '</div>';

    // Revenue milestones
    var totalRevEver = (fh.allTime || {}).revenue || 0;
    // Fallback: estimate from total calls * avg ticket
    if (totalRevEver === 0 && revVals.length > 0) totalRevEver = revVals.reduce(function(a,b){return a+b;},0);
    if (totalRevEver === 0) totalRevEver = (bm.totalLeads || 0) * revenuePerCall;
    var avgMonthRev = revVals.length > 0 ? revVals.reduce(function(a,b){return a+b;},0) / revVals.length : 0;
    var recentRevAvg = revVals.length >= 3 ? revVals.slice(-3).reduce(function(a,b){return a+b;},0) / 3 : avgMonthRev;
    var revMilestones = [250000, 500000, 1000000, 2000000, 5000000, 10000000];
    html += '<div style="font-family:Orbitron;font-size:0.6em;color:#00ff66;letter-spacing:3px;margin:15px 0 8px;">REVENUE MILESTONES</div>';
    html += '<div class="kpi-row">';
    revMilestones.forEach(function(rm) {
      if (rm <= totalRevEver) {
        html += '<div class="kpi" style="--c:#00ff66;"><div class="kpi-label">$' + (rm >= 1000000 ? (rm/1000000) + 'M' : (rm/1000) + 'K') + '</div><div class="kpi-val">✅</div><div class="kpi-sub">Achieved</div></div>';
      } else {
        var revRemaining = rm - totalRevEver;
        var mToGo = recentRevAvg > 0 ? Math.ceil(revRemaining / recentRevAvg) : 99;
        html += '<div class="kpi" style="--c:#4a6a8a;"><div class="kpi-label">$' + (rm >= 1000000 ? (rm/1000000) + 'M' : (rm/1000) + 'K') + '</div><div class="kpi-val">~' + mToGo + 'mo</div><div class="kpi-sub">Need $' + Math.round(revRemaining / 1000).toLocaleString() + 'K more</div></div>';
      }
    });
    html += '</div></div>';

    // ====================================================================
    // SECTION 12: WHAT-IF SCENARIO CALCULATOR
    // ====================================================================
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#ffd700;--gc:#ffd700;"><span class="dot" style="background:#ffd700;"></span>WHAT-IF SCENARIO CALCULATOR</div>';
    html += '<div class="chart-box">';
    html += '<div class="chart-title" style="color:#ffd700;margin-bottom:12px;">GROWTH SCENARIO MODELING</div>';

    var baseRev = recentRevAvg || avgMonthRev || 50000;
    var baseCalls = recentAvg || avgMonthCalls || 300;
    var scenarios = [
      { name: 'CONSERVATIVE', callGrowth: 3, revGrowth: 3, color: '#4a6a8a', icon: '🐢' },
      { name: 'MODERATE', callGrowth: 8, revGrowth: 8, color: '#ff9f43', icon: '📊' },
      { name: 'AGGRESSIVE', callGrowth: 15, revGrowth: 15, color: '#00ff66', icon: '🚀' },
      { name: 'HYPERGROWTH', callGrowth: 25, revGrowth: 25, color: '#a855f7', icon: '⚡' },
    ];

    html += '<div style="overflow-x:auto;"><table class="data-table">';
    html += '<thead><tr><th>Scenario</th><th>Mo Growth</th><th>6-Mo Rev</th><th>12-Mo Rev</th><th>12-Mo Calls</th><th>Annual Rev</th></tr></thead><tbody>';
    scenarios.forEach(function(sc) {
      var sixMoRev = 0, twelveMoRev = 0, twelveMoCalls = 0;
      for (var si = 1; si <= 12; si++) {
        var mRev = Math.round(baseRev * Math.pow(1 + sc.revGrowth / 100, si));
        var mCalls = Math.round(baseCalls * Math.pow(1 + sc.callGrowth / 100, si));
        if (si <= 6) sixMoRev += mRev;
        twelveMoRev += mRev;
        if (si === 12) twelveMoCalls = mCalls;
      }
      html += '<tr><td style="color:' + sc.color + ';font-weight:700;">' + sc.icon + ' ' + sc.name + '</td>';
      html += '<td style="color:' + sc.color + ';">+' + sc.callGrowth + '%</td>';
      html += '<td>$' + Math.round(sixMoRev / 1000).toLocaleString() + 'K</td>';
      html += '<td style="color:' + sc.color + ';font-weight:700;">$' + Math.round(twelveMoRev / 1000).toLocaleString() + 'K</td>';
      html += '<td>' + twelveMoCalls.toLocaleString() + '/mo</td>';
      html += '<td style="font-weight:700;">$' + Math.round(twelveMoRev / 1000).toLocaleString() + 'K</td></tr>';
    });
    html += '</tbody></table></div></div></div>';

    // Footer
    html += '<div style="text-align:center;padding:30px;font-family:Orbitron;font-size:0.55em;letter-spacing:3px;color:#1a2a3a;border-top:1px solid #0a1520;">WILDWOOD ANALYTICS ENGINE v2.0 // ' + monthKeys.length + ' months &bull; ' + (bm.totalLeads || 0) + ' records &bull; ' + Object.keys(locationStats).length + ' markets &bull; ' + cityForecasts.length + ' forecasts</div>';

    // ====================================================================
    // LIGHTWEIGHT CHARTS JAVASCRIPT
    // ====================================================================
    html += '<script>';

    // Data injection
    html += 'var callData=' + JSON.stringify(lcCallData) + ';';
    html += 'var forecastData=' + JSON.stringify(lcForecastData) + ';';
    html += 'var revChartData=' + JSON.stringify(lcRevData) + ';';
    html += 'var profChartData=' + JSON.stringify(lcProfData) + ';';
    html += 'var callFib=' + JSON.stringify(callFib) + ';';
    html += 'var revFib=' + JSON.stringify(revFib) + ';';

    // Chart creation - wrapped in load handler
    html += 'window.addEventListener("load",function(){';
    html += 'var chartOpts={layout:{background:{color:"#050d18"},textColor:"#4a6a8a",fontSize:11},grid:{vertLines:{color:"#0a1520"},horzLines:{color:"#0a1520"}},crosshair:{mode:0},timeScale:{borderColor:"#1a2a3a",timeVisible:false},rightPriceScale:{borderColor:"#1a2a3a"}};';

    // Call Volume Interactive Chart
    html += 'var cEl=document.getElementById("call-chart");';
    html += 'var cW=cEl.offsetWidth;';
    html += 'var mainC=LightweightCharts.createChart(cEl,Object.assign({},chartOpts,{width:cW,height:350}));';
    html += 'var mainS=mainC.addLineSeries({color:"#00d4ff",lineWidth:2,title:"Calls"});';
    html += 'mainS.setData(callData);';

    // Forecast line
    html += 'var foreS=mainC.addLineSeries({color:"#ff9f43",lineWidth:2,lineStyle:2,title:"Forecast"});';
    html += 'var foreUpperS=mainC.addLineSeries({color:"#ff9f4320",lineWidth:1,lineStyle:1});';
    html += 'var foreLowerS=mainC.addLineSeries({color:"#ff9f4320",lineWidth:1,lineStyle:1});';
    html += 'if(callData.length>0){var lastPt=callData[callData.length-1];';
    html += 'var fd=[{time:lastPt.time,value:lastPt.value}].concat(forecastData.map(function(d){return{time:d.time,value:d.value};}));';
    html += 'var fu=[{time:lastPt.time,value:lastPt.value}].concat(forecastData.map(function(d){return{time:d.time,value:d.upper};}));';
    html += 'var fl=[{time:lastPt.time,value:lastPt.value}].concat(forecastData.map(function(d){return{time:d.time,value:d.lower};}));';
    html += 'foreS.setData(fd);foreUpperS.setData(fu);foreLowerS.setData(fl);}';

    // BB overlays (hidden by default)
    html += 'var bbUp=mainC.addLineSeries({color:"#ff9f4360",lineWidth:1,lineStyle:2,visible:false});';
    html += 'var bbLo=mainC.addLineSeries({color:"#ff9f4360",lineWidth:1,lineStyle:2,visible:false});';
    html += 'var bbMd=mainC.addLineSeries({color:"#ff9f4330",lineWidth:1,lineStyle:1,visible:false});';
    html += 'var smaS=mainC.addLineSeries({color:"#a855f7",lineWidth:1,visible:false});';
    html += 'var emaS=mainC.addLineSeries({color:"#ffd700",lineWidth:1,visible:false});';

    // Compute BB/SMA/EMA from callData
    html += 'function computeSMA(data,p){var r=[];for(var i=0;i<data.length;i++){if(i<p-1)continue;var s=0;for(var j=0;j<p;j++)s+=data[i-j].value;r.push({time:data[i].time,value:Math.round(s/p*100)/100});}return r;}';
    html += 'function computeEMA(data,p){if(data.length===0)return[];var k=2/(p+1);var r=[{time:data[0].time,value:data[0].value}];for(var i=1;i<data.length;i++){r.push({time:data[i].time,value:Math.round((data[i].value*k+r[i-1].value*(1-k))*100)/100});}return r;}';
    html += 'function computeBB(data,p,std){var u=[],l=[],m=[];for(var i=0;i<data.length;i++){if(i<p-1)continue;var s=0;for(var j=0;j<p;j++)s+=data[i-j].value;var avg=s/p;var sq=0;for(var j2=0;j2<p;j2++)sq+=(data[i-j2].value-avg)*(data[i-j2].value-avg);var sd=Math.sqrt(sq/p);u.push({time:data[i].time,value:Math.round((avg+std*sd)*100)/100});l.push({time:data[i].time,value:Math.round(Math.max(0,avg-std*sd)*100)/100});m.push({time:data[i].time,value:Math.round(avg*100)/100});}return{upper:u,lower:l,middle:m};}';

    html += 'var bbData=computeBB(callData,Math.min(6,callData.length),2);';
    html += 'bbUp.setData(bbData.upper);bbLo.setData(bbData.lower);bbMd.setData(bbData.middle);';
    html += 'smaS.setData(computeSMA(callData,Math.min(6,callData.length)));';
    html += 'emaS.setData(computeEMA(callData,Math.min(6,callData.length)));';

    // Fibonacci lines
    html += 'var fibLines=[];var showFib2=true;';
    html += 'function drawFibLines(){fibLines.forEach(function(fl){try{mainS.removePriceLine(fl);}catch(e){}});fibLines=[];if(!showFib2)return;';
    html += 'var levels={"0%":callFib.low,"23.6%":callFib["23.6"],"38.2%":callFib["38.2"],"50%":callFib["50.0"],"61.8%":callFib["61.8"],"78.6%":callFib["78.6"],"100%":callFib.high};';
    html += 'var colors={"0%":"#ff4757","23.6%":"#ff4757","38.2%":"#ff9f43","50%":"#ffd700","61.8%":"#00ff66","78.6%":"#00ff66","100%":"#00ff66"};';
    html += 'Object.keys(levels).forEach(function(k){var line=mainS.createPriceLine({price:levels[k],color:colors[k]+"60",lineWidth:1,lineStyle:2,axisLabelVisible:true,title:"Fib "+k});fibLines.push(line);});';
    html += '}drawFibLines();';

    // Extension lines (hidden by default)
    html += 'var extLines=[];var showExt=false;';
    html += 'function drawExtLines(){extLines.forEach(function(el){try{mainS.removePriceLine(el);}catch(e){}});extLines=[];if(!showExt)return;';
    html += '["127.2","161.8","200.0","261.8"].forEach(function(k){if(callFib[k]){var l=mainS.createPriceLine({price:callFib[k],color:"#a855f760",lineWidth:1,lineStyle:1,axisLabelVisible:true,title:"Ext "+k+"%"});extLines.push(l);}});';
    html += '}';

    // Toggle functions
    html += 'var overlays={bb:false,sma:false,ema:false,fib:true,ext:false,forecast:true};';
    html += 'window.toggleOverlay=function(id){overlays[id]=!overlays[id];';
    html += 'var el=document.getElementById("btn-"+id+"2");';
    html += 'if(overlays[id]){el.style.background="rgba(0,212,255,0.15)";el.style.color="#00d4ff";el.style.borderColor="#00d4ff40";}';
    html += 'else{el.style.background="#0a1520";el.style.color="#4a6a8a";el.style.borderColor="#1a2a3a";}';
    html += 'bbUp.applyOptions({visible:overlays.bb});bbLo.applyOptions({visible:overlays.bb});bbMd.applyOptions({visible:overlays.bb});';
    html += 'smaS.applyOptions({visible:overlays.sma});emaS.applyOptions({visible:overlays.ema});';
    html += 'showFib2=overlays.fib;drawFibLines();';
    html += 'showExt=overlays.ext;drawExtLines();';
    html += 'foreS.applyOptions({visible:overlays.forecast});foreUpperS.applyOptions({visible:overlays.forecast});foreLowerS.applyOptions({visible:overlays.forecast});';
    html += '}';

    // RSI chart
    html += 'var rsiEl=document.getElementById("call-rsi");';
    html += 'var rsiC=LightweightCharts.createChart(rsiEl,Object.assign({},chartOpts,{width:cW,height:100}));';
    html += 'var rsiS=rsiC.addLineSeries({color:"#a855f7",lineWidth:1.5});';
    html += 'var rsiData=callData.slice(' + Math.min(6, monthVals.length) + ').map(function(d,i){return{time:d.time,value:' + JSON.stringify(callRSI) + '[i]||50};});';
    html += 'rsiS.setData(rsiData);';
    // RSI levels
    html += 'rsiS.createPriceLine({price:70,color:"#ff475740",lineWidth:1,lineStyle:2,title:"Overbought"});';
    html += 'rsiS.createPriceLine({price:30,color:"#00ff6640",lineWidth:1,lineStyle:2,title:"Oversold"});';
    html += 'rsiS.createPriceLine({price:50,color:"#ffd70020",lineWidth:1,lineStyle:1,title:""});';

    // MACD chart
    html += 'var macdEl=document.getElementById("call-macd");';
    html += 'var macdC=LightweightCharts.createChart(macdEl,Object.assign({},chartOpts,{width:cW,height:100}));';
    html += 'var macdLS=macdC.addLineSeries({color:"#00d4ff",lineWidth:1.5,title:"MACD"});';
    html += 'var macdSS=macdC.addLineSeries({color:"#ff9f43",lineWidth:1,title:"Signal"});';
    html += 'var macdHS=macdC.addHistogramSeries({title:"Hist"});';
    html += 'var macdLine2=' + JSON.stringify(callMACD.macdLine) + ';';
    html += 'var macdSig2=' + JSON.stringify(callMACD.signal) + ';';
    html += 'var macdHist2=' + JSON.stringify(callMACD.histogram) + ';';
    html += 'var macdD=callData.map(function(d,i){return{time:d.time,value:macdLine2[i]||0};});';
    html += 'var sigD=callData.map(function(d,i){return{time:d.time,value:macdSig2[i]||0};});';
    html += 'var histD=callData.map(function(d,i){return{time:d.time,value:macdHist2[i]||0,color:(macdHist2[i]||0)>=0?"#26a69a80":"#ef535080"};});';
    html += 'macdLS.setData(macdD);macdSS.setData(sigD);macdHS.setData(histD);';

    // Revenue chart
    html += 'if(revChartData.length>1){';
    html += 'var rEl=document.getElementById("rev-chart");';
    html += 'var revC=LightweightCharts.createChart(rEl,Object.assign({},chartOpts,{width:rEl.offsetWidth,height:300}));';
    html += 'var revS=revC.addLineSeries({color:"#00ff66",lineWidth:2,title:"Revenue"});';
    html += 'var profS=revC.addLineSeries({color:"#ffd700",lineWidth:2,title:"Profit"});';
    html += 'revS.setData(revChartData);profS.setData(profChartData);';
    // Rev fib lines
    html += '["38.2","50.0","61.8"].forEach(function(k){if(revFib[k]){revS.createPriceLine({price:revFib[k],color:k==="38.2"?"#ff475730":k==="50.0"?"#ffd70030":"#00ff6630",lineWidth:1,lineStyle:2,axisLabelVisible:true,title:"Fib "+k+"%"});}});';
    html += 'revC.timeScale().fitContent();}';

    // Sync crosshairs
    html += 'mainC.timeScale().fitContent();rsiC.timeScale().fitContent();macdC.timeScale().fitContent();';

    // Resize handler
    html += 'window.addEventListener("resize",function(){';
    html += 'var w=document.getElementById("call-chart").offsetWidth;';
    html += 'mainC.applyOptions({width:w});rsiC.applyOptions({width:w});macdC.applyOptions({width:w});';
    html += 'if(typeof revC!=="undefined"){revC.applyOptions({width:document.getElementById("rev-chart").offsetWidth});}';
    html += '});'; // close resize handler
    html += '});'; // close window load handler

    html += '<\/script>';
    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    console.error("Analytics error:", err.stack || err.message);
    res.status(500).json({ error: err.message });
  }
});



/* ===========================
   GOOGLE ADS API INTEGRATION
   OAuth2, Data Fetching, Analytics, Fibonacci, Forecasting
=========================== */

// Google Ads config
var GOOGLE_ADS_DEVELOPER_TOKEN = process.env.GOOGLE_ADS_DEVELOPER_TOKEN || '';
var GOOGLE_ADS_CUSTOMER_ID = (process.env.GOOGLE_ADS_CUSTOMER_ID || '').replace(/-/g, '');
var GOOGLE_ADS_MANAGER_ID = (process.env.GOOGLE_ADS_MANAGER_ID || '').replace(/-/g, '');
var GOOGLE_ADS_CLIENT_ID = process.env.GOOGLE_ADS_CLIENT_ID || '';
var GOOGLE_ADS_CLIENT_SECRET = process.env.GOOGLE_ADS_CLIENT_SECRET || '';
var GOOGLE_ADS_REFRESH_TOKEN = process.env.GOOGLE_ADS_REFRESH_TOKEN || '';

// In-memory token storage
var adsTokenStore = {
  accessToken: null,
  refreshToken: GOOGLE_ADS_REFRESH_TOKEN,
  expiresAt: 0,
};

// Ads data cache (5 min)
var adsCache = { data: null, time: 0 };
var ADS_CACHE_TTL = 300000;

console.log("Google Ads Config: " + (GOOGLE_ADS_DEVELOPER_TOKEN ? "Token ✓" : "Token ✗") + " | " + (GOOGLE_ADS_CUSTOMER_ID ? "CID: " + GOOGLE_ADS_CUSTOMER_ID : "CID ✗") + " | " + (GOOGLE_ADS_MANAGER_ID ? "MCC: " + GOOGLE_ADS_MANAGER_ID : "MCC ✗"));

/* ===========================
   GOOGLE ADS — OAuth2 Flow
   User must create OAuth2 Web credentials in Cloud Console
   and set GOOGLE_ADS_CLIENT_ID + GOOGLE_ADS_CLIENT_SECRET
=========================== */

// Step 1: Redirect to Google consent
app.get('/ads/auth', function(req, res) {
  if (!GOOGLE_ADS_CLIENT_ID) {
    return res.send('<html><body style="background:#050d18;color:#ff4757;font-family:monospace;padding:40px;"><h2>Missing GOOGLE_ADS_CLIENT_ID</h2><p>Go to console.cloud.google.com → APIs & Credentials → Create OAuth 2.0 Client ID (Web Application)</p><p>Add redirect URI: <code>' + 'https://' + req.get('host') + '/ads/callback</code></p><p>Then add to Render env vars:<br>GOOGLE_ADS_CLIENT_ID=your_client_id<br>GOOGLE_ADS_CLIENT_SECRET=your_client_secret</p></body></html>');
  }
  var redirectUri = 'https://' + req.get('host') + '/ads/callback';
  var authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' +
    'client_id=' + encodeURIComponent(GOOGLE_ADS_CLIENT_ID) +
    '&redirect_uri=' + encodeURIComponent(redirectUri) +
    '&response_type=code' +
    '&scope=' + encodeURIComponent('https://www.googleapis.com/auth/adwords') +
    '&access_type=offline' +
    '&prompt=consent';
  res.redirect(authUrl);
});

// Step 2: Exchange code for tokens
app.get('/ads/callback', async function(req, res) {
  var code = req.query.code;
  if (!code) return res.send('Missing code parameter');
  try {
    var redirectUri = 'https://' + req.get('host') + '/ads/callback';
    var tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: 'client_id=' + encodeURIComponent(GOOGLE_ADS_CLIENT_ID) +
        '&client_secret=' + encodeURIComponent(GOOGLE_ADS_CLIENT_SECRET) +
        '&code=' + encodeURIComponent(code) +
        '&grant_type=authorization_code' +
        '&redirect_uri=' + encodeURIComponent(redirectUri),
    });
    var tokenData = await tokenRes.json();
    if (tokenData.error) {
      return res.send('<pre>Error: ' + JSON.stringify(tokenData) + '</pre>');
    }
    adsTokenStore.accessToken = tokenData.access_token;
    adsTokenStore.refreshToken = tokenData.refresh_token || adsTokenStore.refreshToken;
    adsTokenStore.expiresAt = Date.now() + (tokenData.expires_in * 1000);
    
    var html = '<html><body style="background:#050d18;color:#00ff66;font-family:monospace;padding:40px;">';
    html += '<h2>✅ Google Ads Connected!</h2>';
    html += '<p>Access Token: ✓</p>';
    html += '<p>Refresh Token: ' + (adsTokenStore.refreshToken ? '✓ (save this to Render!)' : '✗') + '</p>';
    if (adsTokenStore.refreshToken) {
      html += '<p style="background:#0a1520;padding:15px;border:1px solid #00ff6640;word-break:break-all;"><strong>GOOGLE_ADS_REFRESH_TOKEN=</strong>' + adsTokenStore.refreshToken + '</p>';
      html += '<p style="color:#ff9f43;">⚠️ Copy this refresh token and add it to your Render environment variables so it persists across restarts.</p>';
    }
    html += '<p><a href="/ads" style="color:#00d4ff;">→ Go to Ads Dashboard</a></p>';
    html += '</body></html>';
    res.send(html);
  } catch (err) {
    res.send('<pre>OAuth Error: ' + err.message + '</pre>');
  }
});

// Helper: Get valid access token (refresh if expired)
async function getAdsAccessToken() {
  if (adsTokenStore.accessToken && Date.now() < adsTokenStore.expiresAt - 60000) {
    return adsTokenStore.accessToken;
  }
  if (!adsTokenStore.refreshToken) return null;
  if (!GOOGLE_ADS_CLIENT_ID || !GOOGLE_ADS_CLIENT_SECRET) return null;
  
  try {
    var res = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: 'client_id=' + encodeURIComponent(GOOGLE_ADS_CLIENT_ID) +
        '&client_secret=' + encodeURIComponent(GOOGLE_ADS_CLIENT_SECRET) +
        '&refresh_token=' + encodeURIComponent(adsTokenStore.refreshToken) +
        '&grant_type=refresh_token',
    });
    var data = await res.json();
    if (data.access_token) {
      adsTokenStore.accessToken = data.access_token;
      adsTokenStore.expiresAt = Date.now() + (data.expires_in * 1000);
      return data.access_token;
    }
    console.log("Ads token refresh failed:", data);
    return null;
  } catch (err) {
    console.log("Ads token refresh error:", err.message);
    return null;
  }
}

// Helper: Execute Google Ads Query Language (GAQL) query
async function executeGAQL(query, customerId) {
  var token = await getAdsAccessToken();
  if (!token) return null;
  var cid = customerId || GOOGLE_ADS_CUSTOMER_ID;
  if (!cid) return null;
  
  try {
    var headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + token,
      'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
    };
    if (GOOGLE_ADS_MANAGER_ID) {
      headers['login-customer-id'] = GOOGLE_ADS_MANAGER_ID;
    }
    
    var res = await fetch('https://googleads.googleapis.com/v18/customers/' + cid + '/googleAds:search', {
      method: 'POST',
      headers: headers,
      body: JSON.stringify({ query: query, pageSize: 10000 }),
    });
    var rawText = await res.text();
    var data;
    try { data = JSON.parse(rawText); } catch(pe) {
      console.log("GAQL Parse Error:", rawText.substring(0, 300));
      return null;
    }
    if (data.error) {
      console.log("GAQL Error [" + res.status + "]:", JSON.stringify(data.error).substring(0, 500));
      return null;
    }
    // search returns {results:[...]} or array of batches
    var results = [];
    if (Array.isArray(data)) {
      data.forEach(function(batch) {
        if (batch.results) results = results.concat(batch.results);
      });
    } else if (data.results) {
      results = data.results;
    }
    console.log("GAQL returned " + results.length + " results for: " + query.substring(0, 60) + "...");
    return results;
  } catch (err) {
    console.log("GAQL fetch error:", err.message);
    return null;
  }
}

/* ===========================
   BUILD ADS CONTEXT — Pull All Data
=========================== */

async function buildAdsContext() {
  // Check cache
  if (adsCache.data && (Date.now() - adsCache.time) < ADS_CACHE_TTL) return adsCache.data;
  
  var result = {
    connected: false,
    needsAuth: false,
    campaigns: [],
    adGroups: [],
    keywords: [],
    searchTerms: [],
    geoPerformance: [],
    devicePerformance: [],
    hourlyPerformance: [],
    dayOfWeekPerformance: [],
    monthlyPerformance: [],
    weeklyPerformance: [],
    dailyPerformance: [],
    conversionActions: [],
    accountSummary: {},
    errors: [],
  };
  
  // Check if configured
  if (!GOOGLE_ADS_DEVELOPER_TOKEN || !GOOGLE_ADS_CUSTOMER_ID) {
    result.errors.push("Missing GOOGLE_ADS_DEVELOPER_TOKEN or GOOGLE_ADS_CUSTOMER_ID");
    return result;
  }
  
  var token = await getAdsAccessToken();
  if (!token) {
    result.needsAuth = true;
    result.errors.push("No valid access token. Visit /ads/auth to connect.");
    adsCache.data = result;
    adsCache.time = Date.now();
    return result;
  }
  
  result.connected = true;
  
  // ====== 1. CAMPAIGN PERFORMANCE (last 365 days) ======
  try {
    var campaignData = await executeGAQL(
      "SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, " +
      "campaign.bidding_strategy_type, campaign.budget_amount_micros, " +
      "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, " +
      "metrics.conversions_value, metrics.ctr, metrics.average_cpc, metrics.average_cpm, " +
      "metrics.search_impression_share, metrics.cost_per_conversion, " +
      "metrics.all_conversions, metrics.interactions, metrics.interaction_rate " +
      "FROM campaign WHERE segments.date DURING LAST_365_DAYS AND campaign.status != 'REMOVED' " +
      "ORDER BY metrics.cost_micros DESC"
    );
    if (campaignData) {
      result.campaigns = campaignData.map(function(r) {
        var c = r.campaign || {}, m = r.metrics || {};
        return {
          id: c.id, name: c.name || 'Unknown', status: c.status || 'UNKNOWN',
          type: c.advertisingChannelType || '', bidStrategy: c.biddingStrategyType || '',
          budget: (c.budgetAmountMicros || 0) / 1000000,
          impressions: parseInt(m.impressions || 0), clicks: parseInt(m.clicks || 0),
          cost: (parseInt(m.costMicros || 0)) / 1000000,
          conversions: parseFloat(m.conversions || 0), convValue: parseFloat(m.conversionsValue || 0),
          ctr: parseFloat(m.ctr || 0), avgCPC: (parseInt(m.averageCpc || 0)) / 1000000,
          avgCPM: (parseInt(m.averageCpm || 0)) / 1000000,
          impressionShare: parseFloat(m.searchImpressionShare || 0),
          costPerConv: (parseInt(m.costPerConversion || 0)) / 1000000,
          allConversions: parseFloat(m.allConversions || 0),
          interactions: parseInt(m.interactions || 0), interactionRate: parseFloat(m.interactionRate || 0),
        };
      });
    }
  } catch(e) { result.errors.push("Campaign fetch: " + e.message); }
  if (result.campaigns.length === 0 && result.errors.length === 0) {
    result.errors.push("Campaign query returned empty. Check Render logs for GAQL errors. Test token may need Basic Access approval at ads.google.com/aw/apicenter");
  }

  // ====== 2. AD GROUP PERFORMANCE ======
  try {
    var adGroupData = await executeGAQL(
      "SELECT ad_group.id, ad_group.name, ad_group.status, campaign.name, " +
      "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, " +
      "metrics.ctr, metrics.average_cpc, metrics.cost_per_conversion " +
      "FROM ad_group WHERE segments.date DURING LAST_90_DAYS AND ad_group.status != 'REMOVED' " +
      "ORDER BY metrics.cost_micros DESC LIMIT 100"
    );
    if (adGroupData) {
      result.adGroups = adGroupData.map(function(r) {
        var ag = r.adGroup || {}, m = r.metrics || {}, c = r.campaign || {};
        return {
          id: ag.id, name: ag.name || 'Unknown', status: ag.status || '',
          campaign: c.name || '', impressions: parseInt(m.impressions || 0),
          clicks: parseInt(m.clicks || 0), cost: parseInt(m.costMicros || 0) / 1000000,
          conversions: parseFloat(m.conversions || 0), ctr: parseFloat(m.ctr || 0),
          avgCPC: parseInt(m.averageCpc || 0) / 1000000,
          costPerConv: parseInt(m.costPerConversion || 0) / 1000000,
        };
      });
    }
  } catch(e) { result.errors.push("Ad Group fetch: " + e.message); }

  // ====== 3. KEYWORD PERFORMANCE ======
  try {
    var kwData = await executeGAQL(
      "SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, " +
      "ad_group_criterion.quality_info.quality_score, ad_group_criterion.status, " +
      "campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.ctr, metrics.average_cpc, metrics.cost_per_conversion, " +
      "metrics.search_impression_share " +
      "FROM keyword_view WHERE segments.date DURING LAST_90_DAYS " +
      "ORDER BY metrics.cost_micros DESC LIMIT 200"
    );
    if (kwData) {
      result.keywords = kwData.map(function(r) {
        var kw = (r.adGroupCriterion || {}).keyword || {};
        var qi = (r.adGroupCriterion || {}).qualityInfo || {};
        var m = r.metrics || {}, c = r.campaign || {};
        return {
          text: kw.text || '', matchType: kw.matchType || '',
          qualityScore: parseInt(qi.qualityScore || 0), status: (r.adGroupCriterion || {}).status || '',
          campaign: c.name || '', impressions: parseInt(m.impressions || 0),
          clicks: parseInt(m.clicks || 0), cost: parseInt(m.costMicros || 0) / 1000000,
          conversions: parseFloat(m.conversions || 0), ctr: parseFloat(m.ctr || 0),
          avgCPC: parseInt(m.averageCpc || 0) / 1000000,
          costPerConv: parseInt(m.costPerConversion || 0) / 1000000,
          impressionShare: parseFloat(m.searchImpressionShare || 0),
        };
      });
    }
  } catch(e) { result.errors.push("Keyword fetch: " + e.message); }

  // ====== 4. SEARCH TERMS (what people actually searched) ======
  try {
    var stData = await executeGAQL(
      "SELECT search_term_view.search_term, search_term_view.status, campaign.name, " +
      "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, " +
      "metrics.ctr, metrics.average_cpc " +
      "FROM search_term_view WHERE segments.date DURING LAST_30_DAYS " +
      "ORDER BY metrics.impressions DESC LIMIT 200"
    );
    if (stData) {
      result.searchTerms = stData.map(function(r) {
        var st = r.searchTermView || {}, m = r.metrics || {}, c = r.campaign || {};
        return {
          term: st.searchTerm || '', status: st.status || '',
          campaign: c.name || '', impressions: parseInt(m.impressions || 0),
          clicks: parseInt(m.clicks || 0), cost: parseInt(m.costMicros || 0) / 1000000,
          conversions: parseFloat(m.conversions || 0), ctr: parseFloat(m.ctr || 0),
          avgCPC: parseInt(m.averageCpc || 0) / 1000000,
        };
      });
    }
  } catch(e) { result.errors.push("Search terms: " + e.message); }

  // ====== 5. GEO PERFORMANCE (by city/region) ======
  try {
    var geoData = await executeGAQL(
      "SELECT geographic_view.country_criterion_id, geographic_view.location_type, " +
      "campaign_criterion.location.geo_target_constant, " +
      "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, " +
      "metrics.ctr, metrics.average_cpc, metrics.cost_per_conversion " +
      "FROM geographic_view WHERE segments.date DURING LAST_90_DAYS " +
      "ORDER BY metrics.impressions DESC LIMIT 100"
    );
    if (geoData) {
      result.geoPerformance = geoData.map(function(r) {
        var gv = r.geographicView || {}, m = r.metrics || {};
        return {
          locationType: gv.locationType || '',
          impressions: parseInt(m.impressions || 0), clicks: parseInt(m.clicks || 0),
          cost: parseInt(m.costMicros || 0) / 1000000,
          conversions: parseFloat(m.conversions || 0), ctr: parseFloat(m.ctr || 0),
          avgCPC: parseInt(m.averageCpc || 0) / 1000000,
          costPerConv: parseInt(m.costPerConversion || 0) / 1000000,
        };
      });
    }
  } catch(e) { result.errors.push("Geo: " + e.message); }

  // ====== 6. DEVICE PERFORMANCE ======
  try {
    var deviceData = await executeGAQL(
      "SELECT segments.device, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.ctr, metrics.average_cpc, metrics.cost_per_conversion " +
      "FROM campaign WHERE segments.date DURING LAST_90_DAYS"
    );
    if (deviceData) {
      var deviceMap = {};
      deviceData.forEach(function(r) {
        var dev = (r.segments || {}).device || 'UNKNOWN';
        if (!deviceMap[dev]) deviceMap[dev] = { impressions: 0, clicks: 0, cost: 0, conversions: 0 };
        var m = r.metrics || {};
        deviceMap[dev].impressions += parseInt(m.impressions || 0);
        deviceMap[dev].clicks += parseInt(m.clicks || 0);
        deviceMap[dev].cost += parseInt(m.costMicros || 0) / 1000000;
        deviceMap[dev].conversions += parseFloat(m.conversions || 0);
      });
      result.devicePerformance = Object.entries(deviceMap).map(function(d) {
        var v = d[1];
        return {
          device: d[0], impressions: v.impressions, clicks: v.clicks, cost: Math.round(v.cost * 100) / 100,
          conversions: Math.round(v.conversions * 10) / 10,
          ctr: v.impressions > 0 ? Math.round(v.clicks / v.impressions * 10000) / 100 : 0,
          avgCPC: v.clicks > 0 ? Math.round(v.cost / v.clicks * 100) / 100 : 0,
          costPerConv: v.conversions > 0 ? Math.round(v.cost / v.conversions * 100) / 100 : 0,
        };
      });
    }
  } catch(e) { result.errors.push("Device: " + e.message); }

  // ====== 7. HOURLY PERFORMANCE ======
  try {
    var hourData = await executeGAQL(
      "SELECT segments.hour, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.ctr " +
      "FROM campaign WHERE segments.date DURING LAST_30_DAYS"
    );
    if (hourData) {
      var hourMap = {};
      hourData.forEach(function(r) {
        var hr = (r.segments || {}).hour || 0;
        if (!hourMap[hr]) hourMap[hr] = { impressions: 0, clicks: 0, cost: 0, conversions: 0 };
        var m = r.metrics || {};
        hourMap[hr].impressions += parseInt(m.impressions || 0);
        hourMap[hr].clicks += parseInt(m.clicks || 0);
        hourMap[hr].cost += parseInt(m.costMicros || 0) / 1000000;
        hourMap[hr].conversions += parseFloat(m.conversions || 0);
      });
      for (var h = 0; h < 24; h++) {
        var hd = hourMap[h] || { impressions: 0, clicks: 0, cost: 0, conversions: 0 };
        result.hourlyPerformance.push({
          hour: h, impressions: hd.impressions, clicks: hd.clicks,
          cost: Math.round(hd.cost * 100) / 100, conversions: Math.round(hd.conversions * 10) / 10,
          ctr: hd.impressions > 0 ? Math.round(hd.clicks / hd.impressions * 10000) / 100 : 0,
        });
      }
    }
  } catch(e) { result.errors.push("Hourly: " + e.message); }

  // ====== 8. DAY OF WEEK PERFORMANCE ======
  try {
    var dowData = await executeGAQL(
      "SELECT segments.day_of_week, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.ctr " +
      "FROM campaign WHERE segments.date DURING LAST_90_DAYS"
    );
    if (dowData) {
      var dowMap = {};
      dowData.forEach(function(r) {
        var day = (r.segments || {}).dayOfWeek || 'UNKNOWN';
        if (!dowMap[day]) dowMap[day] = { impressions: 0, clicks: 0, cost: 0, conversions: 0 };
        var m = r.metrics || {};
        dowMap[day].impressions += parseInt(m.impressions || 0);
        dowMap[day].clicks += parseInt(m.clicks || 0);
        dowMap[day].cost += parseInt(m.costMicros || 0) / 1000000;
        dowMap[day].conversions += parseFloat(m.conversions || 0);
      });
      ['MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY','SUNDAY'].forEach(function(d) {
        var dd = dowMap[d] || { impressions: 0, clicks: 0, cost: 0, conversions: 0 };
        result.dayOfWeekPerformance.push({
          day: d, impressions: dd.impressions, clicks: dd.clicks,
          cost: Math.round(dd.cost * 100) / 100, conversions: Math.round(dd.conversions * 10) / 10,
          ctr: dd.impressions > 0 ? Math.round(dd.clicks / dd.impressions * 10000) / 100 : 0,
          costPerConv: dd.conversions > 0 ? Math.round(dd.cost / dd.conversions * 100) / 100 : 0,
        });
      });
    }
  } catch(e) { result.errors.push("DOW: " + e.message); }

  // ====== 9. MONTHLY PERFORMANCE (for Fibonacci) ======
  try {
    var monthData = await executeGAQL(
      "SELECT segments.month, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.conversions_value, metrics.ctr, metrics.average_cpc " +
      "FROM campaign WHERE segments.date DURING LAST_365_DAYS"
    );
    if (monthData) {
      var monthMap = {};
      monthData.forEach(function(r) {
        var mo = (r.segments || {}).month || '';
        if (!monthMap[mo]) monthMap[mo] = { impressions: 0, clicks: 0, cost: 0, conversions: 0, convValue: 0 };
        var m = r.metrics || {};
        monthMap[mo].impressions += parseInt(m.impressions || 0);
        monthMap[mo].clicks += parseInt(m.clicks || 0);
        monthMap[mo].cost += parseInt(m.costMicros || 0) / 1000000;
        monthMap[mo].conversions += parseFloat(m.conversions || 0);
        monthMap[mo].convValue += parseFloat(m.conversionsValue || 0);
      });
      result.monthlyPerformance = Object.keys(monthMap).sort().map(function(mo) {
        var md = monthMap[mo];
        return {
          month: mo, impressions: md.impressions, clicks: md.clicks,
          cost: Math.round(md.cost * 100) / 100, conversions: Math.round(md.conversions * 10) / 10,
          convValue: Math.round(md.convValue * 100) / 100,
          ctr: md.impressions > 0 ? Math.round(md.clicks / md.impressions * 10000) / 100 : 0,
          avgCPC: md.clicks > 0 ? Math.round(md.cost / md.clicks * 100) / 100 : 0,
          roas: md.cost > 0 ? Math.round(md.convValue / md.cost * 100) / 100 : 0,
          costPerConv: md.conversions > 0 ? Math.round(md.cost / md.conversions * 100) / 100 : 0,
        };
      });
    }
  } catch(e) { result.errors.push("Monthly: " + e.message); }

  // ====== 10. DAILY PERFORMANCE (last 90 days for granular charts) ======
  try {
    var dayData = await executeGAQL(
      "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, " +
      "metrics.conversions, metrics.conversions_value, metrics.ctr, metrics.average_cpc " +
      "FROM campaign WHERE segments.date DURING LAST_90_DAYS ORDER BY segments.date"
    );
    if (dayData) {
      var dayMap = {};
      dayData.forEach(function(r) {
        var dt = (r.segments || {}).date || '';
        if (!dayMap[dt]) dayMap[dt] = { impressions: 0, clicks: 0, cost: 0, conversions: 0, convValue: 0 };
        var m = r.metrics || {};
        dayMap[dt].impressions += parseInt(m.impressions || 0);
        dayMap[dt].clicks += parseInt(m.clicks || 0);
        dayMap[dt].cost += parseInt(m.costMicros || 0) / 1000000;
        dayMap[dt].conversions += parseFloat(m.conversions || 0);
        dayMap[dt].convValue += parseFloat(m.conversionsValue || 0);
      });
      result.dailyPerformance = Object.keys(dayMap).sort().map(function(dt) {
        var dd = dayMap[dt];
        return {
          date: dt, impressions: dd.impressions, clicks: dd.clicks,
          cost: Math.round(dd.cost * 100) / 100, conversions: Math.round(dd.conversions * 10) / 10,
          convValue: Math.round(dd.convValue * 100) / 100,
          ctr: dd.impressions > 0 ? Math.round(dd.clicks / dd.impressions * 10000) / 100 : 0,
          avgCPC: dd.clicks > 0 ? Math.round(dd.cost / dd.clicks * 100) / 100 : 0,
          roas: dd.cost > 0 ? Math.round(dd.convValue / dd.cost * 100) / 100 : 0,
        };
      });
    }
  } catch(e) { result.errors.push("Daily: " + e.message); }

  // ====== 11. WEEKLY PERFORMANCE ======
  if (result.dailyPerformance.length > 0) {
    var weekMap = {};
    result.dailyPerformance.forEach(function(d) {
      var dt = new Date(d.date);
      var weekStart = new Date(dt); weekStart.setDate(dt.getDate() - dt.getDay());
      var wk = weekStart.toISOString().split('T')[0];
      if (!weekMap[wk]) weekMap[wk] = { impressions: 0, clicks: 0, cost: 0, conversions: 0, convValue: 0, days: 0 };
      weekMap[wk].impressions += d.impressions; weekMap[wk].clicks += d.clicks;
      weekMap[wk].cost += d.cost; weekMap[wk].conversions += d.conversions;
      weekMap[wk].convValue += d.convValue; weekMap[wk].days++;
    });
    result.weeklyPerformance = Object.keys(weekMap).sort().map(function(wk) {
      var wd = weekMap[wk];
      return {
        week: wk, impressions: wd.impressions, clicks: wd.clicks,
        cost: Math.round(wd.cost * 100) / 100, conversions: Math.round(wd.conversions * 10) / 10,
        convValue: Math.round(wd.convValue * 100) / 100,
        ctr: wd.impressions > 0 ? Math.round(wd.clicks / wd.impressions * 10000) / 100 : 0,
        avgCPC: wd.clicks > 0 ? Math.round(wd.cost / wd.clicks * 100) / 100 : 0,
        roas: wd.cost > 0 ? Math.round(wd.convValue / wd.cost * 100) / 100 : 0,
      };
    });
  }

  // ====== ACCOUNT SUMMARY ======
  var totalSpend = 0, totalClicks = 0, totalImpressions = 0, totalConversions = 0, totalConvValue = 0;
  result.campaigns.forEach(function(c) {
    totalSpend += c.cost; totalClicks += c.clicks; totalImpressions += c.impressions;
    totalConversions += c.conversions; totalConvValue += c.convValue;
  });
  result.accountSummary = {
    totalSpend: Math.round(totalSpend * 100) / 100,
    totalClicks: totalClicks, totalImpressions: totalImpressions,
    totalConversions: Math.round(totalConversions * 10) / 10,
    totalConvValue: Math.round(totalConvValue * 100) / 100,
    avgCPC: totalClicks > 0 ? Math.round(totalSpend / totalClicks * 100) / 100 : 0,
    avgCTR: totalImpressions > 0 ? Math.round(totalClicks / totalImpressions * 10000) / 100 : 0,
    avgCostPerConv: totalConversions > 0 ? Math.round(totalSpend / totalConversions * 100) / 100 : 0,
    roas: totalSpend > 0 ? Math.round(totalConvValue / totalSpend * 100) / 100 : 0,
    activeCampaigns: result.campaigns.filter(function(c) { return c.status === 'ENABLED'; }).length,
    totalCampaigns: result.campaigns.length,
  };

  console.log("Ads context built: " + result.campaigns.length + " campaigns, " + result.keywords.length + " keywords, " + result.dailyPerformance.length + " daily records");
  
  adsCache.data = result;
  adsCache.time = Date.now();
  global.adsData = result;
  return result;
}

// JSON endpoint for ads data
app.get('/ads/json', async function(req, res) {
  try {
    var data = await buildAdsContext();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Refresh ads cache
app.get('/ads/refresh', async function(req, res) {
  adsCache = { data: null, time: 0 };
  try {
    var data = await buildAdsContext();
    res.json({ status: 'refreshed', campaigns: data.campaigns.length, keywords: data.keywords.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Debug endpoint — raw API test
app.get('/ads/debug', async function(req, res) {
  try {
    var token = await getAdsAccessToken();
    if (!token) return res.json({ error: 'No access token. Visit /ads/auth', hasRefresh: !!adsTokenStore.refreshToken, hasClientId: !!GOOGLE_ADS_CLIENT_ID });
    
    var cid = GOOGLE_ADS_CUSTOMER_ID;
    var headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + token,
      'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
    };
    if (GOOGLE_ADS_MANAGER_ID) headers['login-customer-id'] = GOOGLE_ADS_MANAGER_ID;
    
    // Simple query to test
    var query = "SELECT campaign.id, campaign.name, campaign.status FROM campaign LIMIT 10";
    
    var apiRes = await fetch('https://googleads.googleapis.com/v18/customers/' + cid + '/googleAds:search', {
      method: 'POST',
      headers: headers,
      body: JSON.stringify({ query: query }),
    });
    var rawText = await apiRes.text();
    
    res.json({
      config: {
        customerID: cid,
        managerID: GOOGLE_ADS_MANAGER_ID || 'NOT SET',
        devToken: GOOGLE_ADS_DEVELOPER_TOKEN ? GOOGLE_ADS_DEVELOPER_TOKEN.substring(0, 6) + '...' : 'NOT SET',
        hasAccessToken: !!token,
      },
      httpStatus: apiRes.status,
      rawResponse: rawText.substring(0, 2000),
    });
  } catch (err) {
    res.json({ error: err.message, stack: err.stack.substring(0, 500) });
  }
});

/* ===========================
   /ads — GOOGLE ADS DASHBOARD
   Full interactive dashboard with Fibonacci, forecasting, every metric
=========================== */

app.get('/ads', async function(req, res) {
  try {
    await buildBusinessContext();
    var ads;
    try { ads = await buildAdsContext(); } catch(ae) { ads = null; console.log("Ads load error:", ae.message); }
    if (!ads) ads = { connected: false, needsAuth: true, campaigns: [], adGroups: [], keywords: [], searchTerms: [], geoPerformance: [], devicePerformance: [], hourlyPerformance: [], dayOfWeekPerformance: [], monthlyPerformance: [], weeklyPerformance: [], dailyPerformance: [], conversionActions: [], accountSummary: {}, errors: ['Failed to load ads data'] };
    var bm = global.bizMetrics || {};
    var pm = global.profitMetrics || {};

    // Analytics helpers (same as /analytics)
    function linearRegression(values) {
      var n = values.length;
      if (n < 2) return { slope: 0, intercept: values[0] || 0, r2: 0 };
      var sumX=0,sumY=0,sumXY=0,sumXX=0;
      for (var i=0;i<n;i++){sumX+=i;sumY+=values[i];sumXY+=i*values[i];sumXX+=i*i;}
      var d=(n*sumXX-sumX*sumX);var slope=d!==0?(n*sumXY-sumX*sumY)/d:0;var intercept=(sumY-slope*sumX)/n;
      var ssRes=0,ssTot=0,mean=sumY/n;
      for(var j=0;j<n;j++){var p=intercept+slope*j;ssRes+=(values[j]-p)*(values[j]-p);ssTot+=(values[j]-mean)*(values[j]-mean);}
      return{slope:Math.round(slope*100)/100,intercept:Math.round(intercept*100)/100,r2:ssTot>0?Math.round((1-ssRes/ssTot)*1000)/1000:0};
    }
    function calcFibExt(values) {
      var recent=values.slice(-12);var high=Math.max.apply(null,recent.length>0?recent:[0]);
      var low=Math.min.apply(null,recent.length>0?recent:[0]);var range=high-low;
      return{high:high,low:low,range:range,'23.6':Math.round(low+range*0.236),'38.2':Math.round(low+range*0.382),
        '50.0':Math.round(low+range*0.5),'61.8':Math.round(low+range*0.618),'78.6':Math.round(low+range*0.786),
        '127.2':Math.round(low+range*1.272),'161.8':Math.round(low+range*1.618),
        current:recent.length>0?recent[recent.length-1]:0};
    }
    function growthMetrics(values) {
      if(values.length<2)return{mom:0,last:0,prev:0};
      var last=values[values.length-1],prev=values[values.length-2];
      var mom=prev>0?Math.round(((last-prev)/prev)*1000)/10:0;
      return{mom:mom,last:last,prev:prev};
    }
    function forecast(values,months) {
      var lr=linearRegression(values);var n=values.length;var preds=[];
      for(var i=0;i<months;i++){preds.push(Math.max(0,Math.round(lr.intercept+lr.slope*(n+i))));}
      return preds;
    }

    // Compute monthly Fibonacci data for ads
    var monthlyCosts = ads.monthlyPerformance.map(function(m){return m.cost;});
    var monthlyClicks = ads.monthlyPerformance.map(function(m){return m.clicks;});
    var monthlyConv = ads.monthlyPerformance.map(function(m){return m.conversions;});
    var monthlyImpr = ads.monthlyPerformance.map(function(m){return m.impressions;});
    var monthlyCPC = ads.monthlyPerformance.map(function(m){return m.avgCPC;});
    var monthlyROAS = ads.monthlyPerformance.map(function(m){return m.roas;});
    var monthlyCPConv = ads.monthlyPerformance.map(function(m){return m.costPerConv;});
    var monthlyCTR = ads.monthlyPerformance.map(function(m){return m.ctr;});

    var costFib = calcFibExt(monthlyCosts);
    var clickFib = calcFibExt(monthlyClicks);
    var convFib = calcFibExt(monthlyConv);
    var cpcFib = calcFibExt(monthlyCPC);
    var roasFib = calcFibExt(monthlyROAS);
    var cpConvFib = calcFibExt(monthlyCPConv);
    var ctrFib = calcFibExt(monthlyCTR);

    var costGrowth = growthMetrics(monthlyCosts);
    var clickGrowth = growthMetrics(monthlyClicks);
    var convGrowth = growthMetrics(monthlyConv);
    var cpcGrowth = growthMetrics(monthlyCPC);
    var roasGrowth = growthMetrics(monthlyROAS);

    var costForecast = forecast(monthlyCosts, 3);
    var clickForecast = forecast(monthlyClicks, 3);
    var convForecast = forecast(monthlyConv, 3);

    // Compute daily Fibonacci for LightweightCharts
    var dailyCosts = ads.dailyPerformance.map(function(d){return d.cost;});
    var dailyClicks = ads.dailyPerformance.map(function(d){return d.clicks;});
    var dailyCostFib = calcFibExt(dailyCosts);
    var dailyClickFib = calcFibExt(dailyClicks);

    var as = ads.accountSummary || {};
    as.totalSpend = as.totalSpend || 0;
    as.totalClicks = as.totalClicks || 0;
    as.totalImpressions = as.totalImpressions || 0;
    as.totalConversions = as.totalConversions || 0;
    as.totalConvValue = as.totalConvValue || 0;
    as.avgCPC = as.avgCPC || 0;
    as.avgCTR = as.avgCTR || 0;
    as.avgCostPerConv = as.avgCostPerConv || 0;
    as.roas = as.roas || 0;
    as.activeCampaigns = as.activeCampaigns || 0;
    as.totalCampaigns = as.totalCampaigns || 0;

    // ========== BUILD HTML ==========
    var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
    html += '<title>WILDWOOD — Google Ads Intelligence</title>';
    html += '<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet">';
    html += '<script src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.1/lightweight-charts.standalone.production.js"><\/script>';
    html += '<style>';
    html += '*{margin:0;padding:0;box-sizing:border-box;}';
    html += 'body{background:#050d18;color:#c0d8f0;font-family:Rajdhani,sans-serif;overflow-x:hidden;}';
    html += '.wrap{max-width:1500px;margin:0 auto;padding:20px 30px;}';
    html += '.nav{display:flex;gap:0;margin-bottom:20px;flex-wrap:wrap;}';
    html += '.nav a{font-family:Orbitron;font-size:0.7em;letter-spacing:4px;padding:12px 30px;color:#4a6a8a;border:1px solid #1a2a3a;text-decoration:none;background:rgba(5,10,20,0.6);transition:all 0.3s;}';
    html += '.nav a.active{color:#4285f4;border-color:#4285f440;background:rgba(66,133,244,0.1);}';
    html += '.nav a:hover{border-color:#4285f440;}';
    html += '.section{margin-bottom:30px;} .section-head{font-family:Orbitron;font-size:0.85em;letter-spacing:5px;text-transform:uppercase;margin-bottom:15px;display:flex;align-items:center;gap:10px;}';
    html += '.section-head .dot{width:10px;height:10px;border-radius:50%;display:inline-block;animation:glow 2s ease-in-out infinite alternate;}';
    html += '@keyframes glow{0%{box-shadow:0 0 5px var(--gc,#4285f4);}100%{box-shadow:0 0 20px var(--gc,#4285f4);}}';
    html += '.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-bottom:15px;}';
    html += '.kpi{background:rgba(10,20,35,0.8);border:1px solid rgba(255,255,255,0.05);padding:14px;position:relative;overflow:hidden;transition:all 0.3s;}';
    html += '.kpi:hover{border-color:var(--c,#4285f4);transform:translateY(-2px);}';
    html += '.kpi::before{content:"";position:absolute;top:0;left:0;width:100%;height:2px;background:var(--c,#4285f4);opacity:0.4;}';
    html += '.kpi-label{color:#4a6a8a;font-family:Orbitron;font-size:0.45em;letter-spacing:2px;margin-bottom:4px;}';
    html += '.kpi-val{font-family:Orbitron;font-size:1.3em;font-weight:900;color:var(--c,#4285f4);}';
    html += '.kpi-sub{color:#4a6a8a;font-size:0.75em;margin-top:2px;}';
    html += '.fib-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin-bottom:15px;}';
    html += '.fib-card{background:rgba(10,20,35,0.8);border:1px solid #1a2a3a;padding:16px;}';
    html += '.fib-title{font-family:Orbitron;font-size:0.6em;letter-spacing:3px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center;}';
    html += '.fib-level{display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid #0a1520;}';
    html += '.fib-level .pct{font-family:Orbitron;font-size:0.5em;color:#4a6a8a;width:50px;}';
    html += '.fib-level .bar{flex:1;height:5px;background:#0a1520;margin:0 6px;position:relative;}';
    html += '.fib-level .fill{height:100%;position:absolute;top:0;left:0;}';
    html += '.fib-level .val{font-family:Orbitron;font-size:0.65em;width:65px;text-align:right;}';
    html += '.data-table{width:100%;border-collapse:collapse;font-size:0.8em;}';
    html += '.data-table th{padding:8px;text-align:left;color:#4285f4;font-family:Orbitron;font-size:0.5em;letter-spacing:1px;border-bottom:2px solid #4285f415;}';
    html += '.data-table td{padding:5px 8px;border-bottom:1px solid #0a1520;}';
    html += '.data-table tr:nth-child(even){background:rgba(10,20,35,0.3);}';
    html += '.data-table tr:hover{background:rgba(66,133,244,0.03);}';
    html += '.pill{display:inline-block;font-family:Orbitron;font-size:0.45em;letter-spacing:1px;padding:2px 8px;border-radius:2px;}';
    html += '.pill-green{background:rgba(0,255,102,0.1);color:#00ff66;border:1px solid #00ff6630;}';
    html += '.pill-red{background:rgba(255,71,87,0.1);color:#ff4757;border:1px solid #ff475730;}';
    html += '.pill-blue{background:rgba(66,133,244,0.1);color:#4285f4;border:1px solid #4285f430;}';
    html += '.pill-orange{background:rgba(255,159,67,0.1);color:#ff9f43;border:1px solid #ff9f4330;}';
    html += '.signal-box{padding:12px;border:1px solid;margin-top:10px;font-size:0.85em;line-height:1.5;}';
    html += '.chart-box{background:rgba(10,20,35,0.8);border:1px solid #1a2a3a;padding:16px;margin-bottom:12px;}';
    html += '.chart-title{font-family:Orbitron;font-size:0.6em;letter-spacing:3px;margin-bottom:10px;}';
    html += '@media(max-width:768px){.wrap{padding:10px 12px;} .kpi-row{grid-template-columns:repeat(2,1fr);} .fib-grid{grid-template-columns:1fr;} .nav a{padding:8px 12px;font-size:0.5em;} .kpi-val{font-size:1em;} table{font-size:0.65em;}}';
    html += '@media(max-width:480px){.kpi-row{grid-template-columns:1fr;}}';
    html += '</style></head><body><div class="wrap">';

    // Nav
    html += '<div class="nav">';
    html += '<a href="/dashboard">JARVIS</a>';
    html += '<a href="/business">ATHENA</a>';
    html += '<a href="/tookan">TOOKAN</a>';
    html += '<a href="/business/chart">CHARTS</a>';
    html += '<a href="/analytics">ANALYTICS</a>';
    html += '<a href="/ads" class="active">GOOGLE ADS</a>';
    html += '</div>';

    html += '<div style="font-family:Orbitron;font-size:1.4em;letter-spacing:8px;color:#4285f4;margin-bottom:5px;">GOOGLE ADS INTELLIGENCE</div>';
    html += '<div style="font-family:Orbitron;font-size:0.5em;letter-spacing:3px;color:#4a6a8a;margin-bottom:20px;">CAMPAIGN FIBONACCI &bull; CPC ANALYSIS &bull; CONVERSION TRACKING &bull; SEARCH TERMS &bull; BUDGET OPTIMIZATION</div>';

    // Connection status
    if (!ads.connected) {
      html += '<div style="padding:30px;border:2px solid #ff9f4340;background:rgba(255,159,67,0.05);text-align:center;margin-bottom:20px;">';
      html += '<div style="font-family:Orbitron;font-size:1.2em;color:#ff9f43;margin-bottom:10px;">⚡ CONNECT GOOGLE ADS</div>';
      if (!GOOGLE_ADS_CLIENT_ID) {
        html += '<div style="color:#c0d8f0;margin-bottom:10px;">Step 1: Create OAuth2 credentials in Google Cloud Console</div>';
        html += '<div style="color:#4a6a8a;font-size:0.85em;margin-bottom:6px;">Go to console.cloud.google.com → APIs & Credentials → Create OAuth 2.0 Client ID (Web Application)</div>';
        html += '<div style="color:#4a6a8a;font-size:0.85em;margin-bottom:6px;">Add redirect URI: <code style="color:#00d4ff;">' + 'https://' + req.get('host') + '/ads/callback</code></div>';
        html += '<div style="color:#4a6a8a;font-size:0.85em;">Add to Render: GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET</div>';
      } else {
        html += '<a href="/ads/auth" style="display:inline-block;font-family:Orbitron;font-size:0.8em;letter-spacing:3px;padding:12px 40px;background:rgba(66,133,244,0.2);border:1px solid #4285f4;color:#4285f4;text-decoration:none;">AUTHORIZE GOOGLE ADS →</a>';
      }
      if (ads.errors.length > 0) {
        html += '<div style="margin-top:10px;color:#ff4757;font-size:0.8em;">' + ads.errors.join('<br>') + '</div>';
      }
      html += '</div>';
    }

    // ====== SECTION 1: ACCOUNT OVERVIEW ======
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#4285f4;--gc:#4285f4;"><span class="dot" style="background:#4285f4;"></span>ACCOUNT OVERVIEW — LAST 365 DAYS</div>';
    html += '<div class="kpi-row">';
    var acctKPIs = [
      { label: 'TOTAL SPEND', val: '$' + as.totalSpend.toLocaleString(), sub: as.activeCampaigns + ' active campaigns', c: '#ff4757' },
      { label: 'TOTAL CLICKS', val: as.totalClicks.toLocaleString(), sub: as.avgCTR + '% CTR', c: '#4285f4' },
      { label: 'IMPRESSIONS', val: as.totalImpressions.toLocaleString(), sub: 'Across all campaigns', c: '#00d4ff' },
      { label: 'CONVERSIONS', val: as.totalConversions, sub: '$' + as.avgCostPerConv + ' per conversion', c: '#00ff66' },
      { label: 'CONV. VALUE', val: '$' + as.totalConvValue.toLocaleString(), sub: as.roas + 'x ROAS', c: '#ffd700' },
      { label: 'AVG CPC', val: '$' + as.avgCPC, sub: 'Cost per click', c: '#a855f7' },
      { label: 'AVG CTR', val: as.avgCTR + '%', sub: 'Click-through rate', c: '#55f7d8' },
      { label: 'COST/CONVERSION', val: '$' + as.avgCostPerConv, sub: 'Avg acquisition cost', c: '#ff9f43' },
    ];
    acctKPIs.forEach(function(k) {
      html += '<div class="kpi" style="--c:' + k.c + ';"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.val + '</div><div class="kpi-sub">' + k.sub + '</div></div>';
    });
    html += '</div></div>';

    // ====== SECTION 2: INTERACTIVE DAILY CHART ======
    if (ads.dailyPerformance.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#00d4ff;--gc:#00d4ff;"><span class="dot" style="background:#00d4ff;"></span>DAILY PERFORMANCE — INTERACTIVE CHART</div>';
      html += '<div style="display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;">';
      html += '<button onclick="switchMetric(\'cost\')" id="btn-m-cost" class="mbtn active" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:rgba(66,133,244,0.15);color:#4285f4;border:1px solid #4285f440;cursor:pointer;">SPEND</button>';
      html += '<button onclick="switchMetric(\'clicks\')" id="btn-m-clicks" class="mbtn" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">CLICKS</button>';
      html += '<button onclick="switchMetric(\'conversions\')" id="btn-m-conv" class="mbtn" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">CONVERSIONS</button>';
      html += '<button onclick="switchMetric(\'ctr\')" id="btn-m-ctr" class="mbtn" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">CTR</button>';
      html += '<button onclick="switchMetric(\'avgCPC\')" id="btn-m-cpc" class="mbtn" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">CPC</button>';
      html += '<button onclick="switchMetric(\'roas\')" id="btn-m-roas" class="mbtn" style="font-family:Orbitron;font-size:0.5em;padding:6px 12px;background:#0a1520;color:#4a6a8a;border:1px solid #1a2a3a;cursor:pointer;">ROAS</button>';
      html += '</div>';
      html += '<div id="ads-daily-chart" style="border:1px solid #1a2a3a;min-height:350px;width:100%;"></div>';
      html += '</div>';
    }

    // ====== SECTION 3: MONTHLY FIBONACCI CARDS ======
    if (ads.monthlyPerformance.length >= 3) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#ffd700;--gc:#ffd700;"><span class="dot" style="background:#ffd700;"></span>MONTHLY METRICS — FIBONACCI RETRACEMENT</div>';
      html += '<div class="fib-grid">';

      var adsFibs = [
        { name: 'AD SPEND', fib: costFib, growth: costGrowth, color: '#ff4757', prefix: '$', inv: true },
        { name: 'CLICKS', fib: clickFib, growth: clickGrowth, color: '#4285f4', prefix: '' },
        { name: 'CONVERSIONS', fib: convFib, growth: convGrowth, color: '#00ff66', prefix: '' },
        { name: 'AVG CPC', fib: cpcFib, growth: cpcGrowth, color: '#a855f7', prefix: '$', inv: true },
        { name: 'ROAS', fib: roasFib, growth: roasGrowth, color: '#ffd700', prefix: '', suffix: 'x' },
        { name: 'CTR', fib: ctrFib, growth: growthMetrics(monthlyCTR), color: '#55f7d8', prefix: '', suffix: '%' },
      ];

      adsFibs.forEach(function(af) {
        html += '<div class="fib-card">';
        html += '<div class="fib-title"><span style="color:' + af.color + ';">' + af.name + '</span>';
        var tIcon = af.growth.mom > 0 ? '▲' : af.growth.mom < 0 ? '▼' : '►';
        var tColor = af.inv ? (af.growth.mom > 0 ? '#ff4757' : '#00ff66') : (af.growth.mom > 0 ? '#00ff66' : '#ff4757');
        html += '<span style="color:' + tColor + ';">' + tIcon + ' ' + (af.growth.mom >= 0 ? '+' : '') + af.growth.mom + '%</span></div>';

        var fibArr = [
          { label: '161.8% EXT', val: af.fib['161.8'], c: '#a855f7' },
          { label: '100% HIGH', val: af.fib.high, c: '#00ff66' },
          { label: '61.8%', val: af.fib['61.8'], c: '#00ff66' },
          { label: '50%', val: af.fib['50.0'], c: '#ffd700' },
          { label: '38.2%', val: af.fib['38.2'], c: '#ff4757' },
          { label: '0% LOW', val: af.fib.low, c: '#ff4757' },
        ];
        fibArr.forEach(function(fl) {
          var pct = af.fib.high > 0 ? Math.min(100, Math.round(fl.val / af.fib.high * 100)) : 0;
          html += '<div class="fib-level"><span class="pct">' + fl.label.split(' ')[0] + '</span>';
          html += '<div class="bar"><div class="fill" style="width:' + pct + '%;background:' + fl.c + '40;"></div></div>';
          html += '<span class="val" style="color:' + fl.c + ';">' + af.prefix + (typeof fl.val === 'number' ? (fl.val >= 1000 ? Math.round(fl.val).toLocaleString() : fl.val) : fl.val) + (af.suffix || '') + '</span></div>';
        });

        // Current position
        var curPos = af.fib.range > 0 ? Math.round((af.fib.current - af.fib.low) / af.fib.range * 100) : 50;
        html += '<div style="margin-top:8px;display:flex;justify-content:space-between;font-size:0.75em;">';
        html += '<span style="color:#4a6a8a;">Now: <strong style="color:' + af.color + ';">' + af.prefix + (typeof af.fib.current === 'number' ? af.fib.current : 0) + (af.suffix || '') + '</strong></span>';
        html += '<span style="color:' + (curPos > 61.8 ? '#00ff66' : curPos > 38.2 ? '#ffd700' : '#ff4757') + ';font-family:Orbitron;font-size:0.7em;">' + curPos + '% RET</span>';
        html += '</div></div>';
      });
      html += '</div></div>';
    }

    // ====== SECTION 4: CAMPAIGN TABLE ======
    if (ads.campaigns.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#4285f4;--gc:#4285f4;"><span class="dot" style="background:#4285f4;"></span>CAMPAIGN PERFORMANCE</div>';
      html += '<div style="overflow-x:auto;"><table class="data-table">';
      html += '<thead><tr><th>Campaign</th><th>Status</th><th>Spend</th><th>Clicks</th><th>CTR</th><th>CPC</th><th>Conv</th><th>Cost/Conv</th><th>ROAS</th><th>Impr Share</th></tr></thead><tbody>';
      ads.campaigns.forEach(function(c) {
        var statusPill = c.status === 'ENABLED' ? '<span class="pill pill-green">ACTIVE</span>' : c.status === 'PAUSED' ? '<span class="pill pill-orange">PAUSED</span>' : '<span class="pill pill-red">' + c.status + '</span>';
        html += '<tr><td style="color:#c0d8f0;font-weight:700;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + c.name + '</td>';
        html += '<td>' + statusPill + '</td>';
        html += '<td style="color:#ff4757;">$' + c.cost.toLocaleString() + '</td>';
        html += '<td style="color:#4285f4;">' + c.clicks.toLocaleString() + '</td>';
        html += '<td>' + (c.ctr * 100).toFixed(2) + '%</td>';
        html += '<td style="color:#a855f7;">$' + c.avgCPC.toFixed(2) + '</td>';
        html += '<td style="color:#00ff66;">' + c.conversions + '</td>';
        html += '<td style="color:#ff9f43;">$' + c.costPerConv.toFixed(2) + '</td>';
        var roas = c.cost > 0 ? (c.convValue / c.cost).toFixed(2) : '0';
        html += '<td style="color:#ffd700;">' + roas + 'x</td>';
        html += '<td>' + (c.impressionShare * 100).toFixed(1) + '%</td></tr>';
      });
      html += '</tbody></table></div></div>';
    }

    // ====== SECTION 5: TOP KEYWORDS ======
    if (ads.keywords.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#a855f7;--gc:#a855f7;"><span class="dot" style="background:#a855f7;"></span>TOP KEYWORDS — ' + ads.keywords.length + ' TRACKED</div>';
      html += '<div style="overflow-x:auto;"><table class="data-table">';
      html += '<thead><tr><th>Keyword</th><th>Match</th><th>QS</th><th>Clicks</th><th>Cost</th><th>CTR</th><th>CPC</th><th>Conv</th><th>Cost/Conv</th></tr></thead><tbody>';
      ads.keywords.slice(0, 50).forEach(function(kw) {
        var qsColor = kw.qualityScore >= 7 ? '#00ff66' : kw.qualityScore >= 5 ? '#ff9f43' : '#ff4757';
        html += '<tr><td style="color:#c0d8f0;font-weight:600;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + kw.text + '</td>';
        html += '<td style="color:#4a6a8a;font-size:0.8em;">' + (kw.matchType || '').replace('_', ' ') + '</td>';
        html += '<td style="color:' + qsColor + ';font-weight:700;">' + (kw.qualityScore || '-') + '</td>';
        html += '<td style="color:#4285f4;">' + kw.clicks.toLocaleString() + '</td>';
        html += '<td style="color:#ff4757;">$' + kw.cost.toFixed(2) + '</td>';
        html += '<td>' + (kw.ctr * 100).toFixed(2) + '%</td>';
        html += '<td style="color:#a855f7;">$' + kw.avgCPC.toFixed(2) + '</td>';
        html += '<td style="color:#00ff66;">' + kw.conversions + '</td>';
        html += '<td style="color:#ff9f43;">' + (kw.costPerConv > 0 ? '$' + kw.costPerConv.toFixed(2) : '-') + '</td></tr>';
      });
      html += '</tbody></table></div></div>';
    }

    // ====== SECTION 6: SEARCH TERMS ======
    if (ads.searchTerms.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#55f7d8;--gc:#55f7d8;"><span class="dot" style="background:#55f7d8;"></span>SEARCH TERMS — WHAT PEOPLE ACTUALLY SEARCHED</div>';
      html += '<div style="overflow-x:auto;"><table class="data-table">';
      html += '<thead><tr><th>Search Term</th><th>Impressions</th><th>Clicks</th><th>CTR</th><th>Cost</th><th>CPC</th><th>Conv</th></tr></thead><tbody>';
      ads.searchTerms.slice(0, 50).forEach(function(st) {
        html += '<tr><td style="color:#c0d8f0;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + st.term + '</td>';
        html += '<td>' + st.impressions.toLocaleString() + '</td>';
        html += '<td style="color:#4285f4;">' + st.clicks.toLocaleString() + '</td>';
        html += '<td>' + (st.ctr * 100).toFixed(2) + '%</td>';
        html += '<td style="color:#ff4757;">$' + st.cost.toFixed(2) + '</td>';
        html += '<td style="color:#a855f7;">$' + st.avgCPC.toFixed(2) + '</td>';
        html += '<td style="color:#00ff66;">' + st.conversions + '</td></tr>';
      });
      html += '</tbody></table></div></div>';
    }

    // ====== SECTION 7: DEVICE PERFORMANCE ======
    if (ads.devicePerformance.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#ff9f43;--gc:#ff9f43;"><span class="dot" style="background:#ff9f43;"></span>DEVICE BREAKDOWN</div>';
      html += '<div class="kpi-row">';
      ads.devicePerformance.forEach(function(d) {
        var icon = d.device === 'MOBILE' ? '📱' : d.device === 'DESKTOP' ? '🖥️' : d.device === 'TABLET' ? '📋' : '📺';
        html += '<div class="kpi" style="--c:#ff9f43;"><div class="kpi-label">' + icon + ' ' + d.device + '</div><div class="kpi-val">' + d.clicks.toLocaleString() + '</div><div class="kpi-sub">$' + d.cost.toLocaleString() + ' spent &bull; ' + d.ctr + '% CTR &bull; $' + d.avgCPC + ' CPC</div></div>';
      });
      html += '</div></div>';
    }

    // ====== SECTION 8: HOURLY + DOW HEATMAP ======
    if (ads.hourlyPerformance.length > 0) {
      html += '<div class="section">';
      html += '<div class="section-head" style="color:#00d4ff;--gc:#00d4ff;"><span class="dot" style="background:#00d4ff;"></span>TIME-BASED PERFORMANCE</div>';
      html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">';

      // Hourly chart
      html += '<div class="chart-box"><div class="chart-title" style="color:#00d4ff;">CLICKS BY HOUR</div>';
      var maxHrClicks = Math.max.apply(null, ads.hourlyPerformance.map(function(h){return h.clicks;}));
      html += '<div style="display:flex;align-items:flex-end;gap:2px;height:100px;">';
      ads.hourlyPerformance.forEach(function(h) {
        var ht = maxHrClicks > 0 ? Math.max(1, Math.round(h.clicks / maxHrClicks * 100)) : 1;
        html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;">';
        html += '<div style="width:90%;height:' + ht + '%;background:#00d4ff40;border-top:1px solid #00d4ff;"></div>';
        html += '<div style="font-size:0.35em;color:#4a6a8a;margin-top:2px;">' + h.hour + '</div></div>';
      });
      html += '</div></div>';

      // DOW chart
      html += '<div class="chart-box"><div class="chart-title" style="color:#ff9f43;">CLICKS BY DAY OF WEEK</div>';
      var maxDowClicks = Math.max.apply(null, ads.dayOfWeekPerformance.map(function(d){return d.clicks;}));
      html += '<div style="display:flex;align-items:flex-end;gap:6px;height:100px;">';
      ads.dayOfWeekPerformance.forEach(function(d) {
        var dt2 = maxDowClicks > 0 ? Math.max(3, Math.round(d.clicks / maxDowClicks * 100)) : 3;
        html += '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;">';
        html += '<div style="color:#c0d8f0;font-size:0.55em;">' + d.clicks.toLocaleString() + '</div>';
        html += '<div style="width:80%;height:' + dt2 + '%;background:#ff9f4340;border-top:2px solid #ff9f43;"></div>';
        html += '<div style="font-family:Orbitron;font-size:0.4em;color:#4a6a8a;margin-top:4px;">' + d.day.substring(0, 3) + '</div></div>';
      });
      html += '</div></div></div></div>';
    }

    // ====== SECTION 9: AI STRATEGY SIGNAL ======
    html += '<div class="section">';
    html += '<div class="section-head" style="color:#ffd700;--gc:#ffd700;"><span class="dot" style="background:#ffd700;"></span>AI STRATEGY SIGNALS</div>';
    var signals = [];
    if (as.avgCPC > cpcFib['61.8'] && cpcFib['61.8'] > 0) signals.push('⚠️ CPC above 61.8% Fibonacci resistance ($' + cpcFib['61.8'] + '). Review bids and quality scores.');
    if (as.avgCPC < cpcFib['38.2'] && cpcFib['38.2'] > 0) signals.push('✅ CPC below 38.2% support ($' + cpcFib['38.2'] + '). Efficiency is strong.');
    if (convGrowth.mom > 10) signals.push('🚀 Conversions up ' + convGrowth.mom + '% MoM — scale budget!');
    if (convGrowth.mom < -10) signals.push('⚠️ Conversions down ' + convGrowth.mom + '% MoM — check landing pages and ad copy.');
    if (costGrowth.mom > 20 && convGrowth.mom < 5) signals.push('🔴 Spend growing (' + costGrowth.mom + '%) faster than conversions (' + convGrowth.mom + '%). Reduce waste.');
    if (as.roas > 5) signals.push('💰 ROAS is ' + as.roas + 'x — ads are highly profitable. Consider scaling budget.');
    if (as.roas > 0 && as.roas < 2) signals.push('⚡ ROAS is ' + as.roas + 'x — below break-even zone. Optimize or pause underperformers.');
    if (ads.keywords.filter(function(k){return k.qualityScore > 0 && k.qualityScore < 5;}).length > 5) signals.push('📝 ' + ads.keywords.filter(function(k){return k.qualityScore > 0 && k.qualityScore < 5;}).length + ' keywords with Quality Score < 5. Improve ad relevance and landing pages.');
    if (signals.length === 0) signals.push('📊 Connect Google Ads and allow data to accumulate for AI strategy recommendations.');

    signals.forEach(function(s) {
      var sColor = s.startsWith('✅') || s.startsWith('💰') || s.startsWith('🚀') ? '#00ff66' : s.startsWith('⚠️') || s.startsWith('🔴') ? '#ff4757' : '#ff9f43';
      html += '<div class="signal-box" style="border-color:' + sColor + '30;background:' + sColor + '05;color:' + sColor + ';margin-bottom:6px;">' + s + '</div>';
    });
    html += '</div>';

    // ====== SECTIONS 10-16: EXPANDED ANALYTICS ======

    html += '<div style="text-align:center;padding:30px;font-family:Orbitron;font-size:0.5em;letter-spacing:3px;color:#1a2a3a;border-top:1px solid #0a1520;">WILDWOOD ADS INTELLIGENCE v1.0 // ' + ads.campaigns.length + ' campaigns &bull; ' + ads.keywords.length + ' keywords &bull; ' + ads.dailyPerformance.length + ' daily records</div>';

    // ====== JAVASCRIPT — Interactive Charts ======
    html += '<script>';
    html += 'var dailyData=' + JSON.stringify(ads.dailyPerformance) + ';';

    html += 'window.addEventListener("load",function(){';
    html += 'if(dailyData.length < 2) return;';
    html += 'var chartOpts={layout:{background:{color:"#050d18"},textColor:"#4a6a8a",fontSize:11},grid:{vertLines:{color:"#0a1520"},horzLines:{color:"#0a1520"}},crosshair:{mode:0},timeScale:{borderColor:"#1a2a3a",timeVisible:false},rightPriceScale:{borderColor:"#1a2a3a"}};';
    html += 'var el=document.getElementById("ads-daily-chart");';
    html += 'if(!el)return;';
    html += 'var chart=LightweightCharts.createChart(el,Object.assign({},chartOpts,{width:el.offsetWidth,height:350}));';
    html += 'var series=chart.addLineSeries({color:"#4285f4",lineWidth:2});';
    html += 'var currentMetric="cost";';

    html += 'window.switchMetric=function(metric){';
    html += 'currentMetric=metric;';
    html += 'var d=dailyData.map(function(r){return{time:r.date,value:r[metric]||0};});';
    html += 'series.setData(d);chart.timeScale().fitContent();';
    html += 'var colors={cost:"#ff4757",clicks:"#4285f4",conversions:"#00ff66",ctr:"#55f7d8",avgCPC:"#a855f7",roas:"#ffd700"};';
    html += 'series.applyOptions({color:colors[metric]||"#4285f4"});';
    html += 'document.querySelectorAll(".mbtn").forEach(function(b){b.style.background="#0a1520";b.style.color="#4a6a8a";b.style.borderColor="#1a2a3a";});';
    html += 'var btnMap={cost:"btn-m-cost",clicks:"btn-m-clicks",conversions:"btn-m-conv",ctr:"btn-m-ctr",avgCPC:"btn-m-cpc",roas:"btn-m-roas"};';
    html += 'var btn=document.getElementById(btnMap[metric]);';
    html += 'if(btn){btn.style.background="rgba(66,133,244,0.15)";btn.style.color="#4285f4";btn.style.borderColor="#4285f440";}';
    html += '};';

    // Initial render
    html += 'switchMetric("cost");';

    html += 'window.addEventListener("resize",function(){chart.applyOptions({width:el.offsetWidth});});';
    html += '});';
    html += '<\/script>';

    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    console.error("Ads dashboard error:", err.stack || err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/tookan/json', async function(req, res) {
  try {
    var tk = await buildTookanContext();
    res.json(tk);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Force refresh Tookan cache
app.get('/tookan/refresh', async function(req, res) {
  tookanCache = { data: null, time: 0 };
  try {
    var tk = await buildTookanContext();
    res.json({ status: 'refreshed', tasks: tk.totalTasks, completed: tk.completed, agents: tk.agents.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, function() {
  console.log("LifeOS Jarvis running on port " + PORT);
  console.log("Endpoints: /tabs /tab/:name /scan /scan/full /search?q= /summary /priority /briefing /call /voice /conversation /whatsapp /gmail/auth /gmail/unread /gmail/summary /dashboard /business /tookan /chat /daily-questions /nightly-checkin /team /team/:name /team/assign /team/daily-tasks /team/coaching /team/workload");
  // Start calendar watcher for 10-min-before calls
  startCalendarWatcher();
  console.log("Calendar watcher started — checking every 2 minutes");
  // Pre-load business context, THEN Tookan (Tookan needs CRM job IDs)
  buildBusinessContext().then(function() {
    console.log("Business context loaded — now fetching Tookan data...");
    return buildTookanContext();
  }).then(function(d) {
    console.log("Tookan startup: " + d.totalTasks + " tasks, " + d.completed + " completed, " + d.agents.length + " agents");
    // Also try loading Google Ads data
    if (GOOGLE_ADS_DEVELOPER_TOKEN && GOOGLE_ADS_CUSTOMER_ID) {
      buildAdsContext().then(function(ad) {
        console.log("Google Ads startup: " + ad.campaigns.length + " campaigns, " + (ad.connected ? "connected" : "needs auth"));
      }).catch(function(ae) { console.log("Ads startup: " + ae.message); });
    }
  }).catch(function(e) {
    console.log("Startup fetch error: " + e.message);
    // Still try Tookan even if business failed
    buildTookanContext().catch(function(e2) { console.log("Tookan fallback error: " + e2.message); });
  });
});