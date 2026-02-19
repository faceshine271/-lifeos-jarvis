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
    // Fetch key data
    var tabs = await getAllTabNames();
    var context = await buildLifeOSContext();

    // Parse numbers from context
    var screenTimeMatch = context.match(/Daily average[:\s]*([\d.]+)/i);
    var debtMatch = context.match(/\~?\$([,\d]+)\s*total debt/i);

    var screenTime = screenTimeMatch ? screenTimeMatch[1] : '?';
    var debtAmount = debtMatch ? debtMatch[1].replace(/,/g, '') : '0';

    // Get email count
    var emailAccounts = Object.keys(gmailTokens);
    var totalUnread = 0;
    for (var ea = 0; ea < emailAccounts.length; ea++) {
      var emails = await getUnreadEmails(emailAccounts[ea], 50);
      totalUnread += emails.length;
    }

    // Get today's calendar events
    var todayEvents = [];
    for (var ca = 0; ca < emailAccounts.length; ca++) {
      var events = await getCalendarEvents(emailAccounts[ca], 1);
      todayEvents = todayEvents.concat(events);
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
    html += '<title>J.A.R.V.I.S. — LifeOS Command Center</title>';
    html += '<style>';

    // Base
    html += '* { margin: 0; padding: 0; box-sizing: border-box; }';
    html += '@import url("https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;500;600;700&display=swap");';
    html += 'body { background: #020810; color: #c0d8f0; font-family: "Rajdhani", sans-serif; min-height: 100vh; overflow-x: hidden; }';

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

    html += '</style></head><body>';

    // ====== BOOT SEQUENCE SPLASH ======
    html += '<div id="boot-screen" style="position:fixed;top:0;left:0;width:100%;height:100%;background:#020810;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;overflow:hidden;">';
    
    // Hex grid background for boot
    html += '<div style="position:absolute;top:0;left:0;width:100%;height:100%;background-image:linear-gradient(rgba(0,212,255,0.02) 1px,transparent 1px),linear-gradient(90deg,rgba(0,212,255,0.02) 1px,transparent 1px);background-size:40px 40px;"></div>';
    
    // Center ring animation
    html += '<div id="boot-ring" style="width:120px;height:120px;border:2px solid #00d4ff20;border-radius:50%;position:relative;animation:bootRingSpin 2s linear infinite;opacity:0;">';
    html += '<div style="position:absolute;top:-2px;left:50%;width:8px;height:8px;background:#00d4ff;border-radius:50;margin-left:-4px;box-shadow:0 0 20px #00d4ff;"></div>';
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
    html += '<div class="boot-line" style="opacity:0;color:#00ff66;">ALL SYSTEMS OPERATIONAL</div>';
    html += '</div>';
    
    // JARVIS title reveal
    html += '<div id="boot-title" style="position:absolute;opacity:0;font-family:Orbitron;font-size:4em;font-weight:900;letter-spacing:20px;color:#00d4ff;text-shadow:0 0 60px rgba(0,212,255,0.6),0 0 120px rgba(0,212,255,0.2);">J.A.R.V.I.S.</div>';
    
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
    html += '    fetch("https://api.elevenlabs.io/v1/text-to-speech/jP5jSWhfXz3nfQENMtf4",{method:"POST",headers:{"xi-api-key":"sk_2106002b395df58e01d77515940ca9ca6baa0cb4d856dd1b","Content-Type":"application/json","Accept":"audio/mpeg"},body:JSON.stringify({text:greetText,model_id:"eleven_turbo_v2",voice_settings:{stability:0.5,similarity_boost:0.75,style:0.3}})})';
    html += '    .then(function(r){if(!r.ok)throw new Error("ElevenLabs failed");return r.blob();})';
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
        html += '<div style="color:#ff6348;font-size:0.9em;font-weight:600;">' + ed.from.substring(0, 40) + '</div>';
        html += '<div style="color:#c0d8f0;font-size:0.95em;margin:4px 0;">' + ed.subject.substring(0, 60) + '</div>';
        html += '<div style="color:#4a6a8a;font-size:0.8em;">' + ed.snippet.substring(0, 100) + '...</div>';
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
    html += '  fetch("https://api.elevenlabs.io/v1/text-to-speech/jP5jSWhfXz3nfQENMtf4",{method:"POST",headers:{"xi-api-key":"sk_2106002b395df58e01d77515940ca9ca6baa0cb4d856dd1b","Content-Type":"application/json","Accept":"audio/mpeg"},body:JSON.stringify({text:text,model_id:"eleven_turbo_v2",voice_settings:{stability:0.5,similarity_boost:0.75,style:0.3}})})';
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

    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   POST /chat — Browser voice chat
=========================== */

var webChatHistory = {};
var webChatHistory = {};

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
      history = {
        systemPrompt: "You are Jarvis, Trace's personal AI counselor and mentor. You are speaking through a browser voice interface.\n\nRULES:\n- Talk like a wise friend and life coach. Be real and direct.\n- NEVER mention tab names, sheet names, row counts, or entry counts.\n- NEVER recite statistics unless Trace specifically asks for numbers.\n- Use the data to UNDERSTAND his life, then give human advice.\n- Ask thoughtful questions. Push him to grow.\n- Keep responses to 1-3 sentences MAX. You are being read aloud.\n- Never use markdown, bullet points, or formatting.\n- Sound like a real person, not a robot or a database.\n\nLIFE OS DATA (use to inform advice, don't recite):\n" + context,
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
   START SERVER
=========================== */

app.listen(PORT, function() {
  console.log("LifeOS Jarvis running on port " + PORT);
  console.log("Endpoints: /tabs /tab/:name /scan /scan/full /search?q= /summary /priority /briefing /call /voice /conversation /whatsapp /gmail/auth /gmail/unread /gmail/summary /dashboard /chat /daily-questions /nightly-checkin");
  // Start calendar watcher for 10-min-before calls
  startCalendarWatcher();
  console.log("Calendar watcher started — checking every 2 minutes");
});