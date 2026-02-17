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
   Build Life OS context for Claude
=========================== */

async function buildLifeOSContext() {
  var tabs = await getAllTabNames();
  var context = "LIFE OS SYSTEM: " + tabs.length + " tabs\n";
  context += "Tabs: " + tabs.join(', ') + "\n\n";

  // Debt snapshot
  try {
    var res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Ultimate_Debt_Tracker_Advanced'!A1:N30",
    });
    var rows = res.data.values || [];
    if (rows.length > 1) {
      var headers = rows[0];
      var nameCol = headers.indexOf('Account Name');
      var balCol = headers.indexOf('Current_Balance');
      var typeCol = headers.indexOf('Account Type');
      var statusCol = headers.indexOf('Status');
      var totalActive = 0;
      var totalBalance = 0;
      var debtLines = [];
      rows.slice(1).forEach(function(r) {
        var status = statusCol >= 0 ? (r[statusCol] || '') : 'active';
        if (status.toLowerCase() === 'active') {
          totalActive++;
          var bal = parseFloat((r[balCol] || '0').replace(/[$,]/g, ''));
          if (!isNaN(bal)) totalBalance += bal;
          debtLines.push("  " + (r[nameCol] || '?') + " (" + (r[typeCol] || '?') + "): $" + (r[balCol] || '0'));
        }
      });
      context += "FINANCES: " + totalActive + " active accounts, ~$" + Math.round(totalBalance).toLocaleString() + " total\n";
      context += debtLines.slice(0, 10).join('\n') + '\n\n';
    }
  } catch (e) {}

  // Screen time
  try {
    var res2 = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Dashboard'!A1:F20",
    });
    var rows2 = res2.data.values || [];
    if (rows2.length > 1) {
      context += "SCREEN TIME: " + (rows2[1][0] || '?') + " hours daily\n";
      context += "Top app: " + (rows2[1][1] || '?') + "\n\n";
    }
  } catch (e) {}

  // Gratitude
  try {
    var res3 = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Gratitude_Memory'!A1:B6",
    });
    var rows3 = res3.data.values || [];
    if (rows3.length > 1) {
      var gratCount = await getTabRowCount('Gratitude_Memory');
      context += "GRATITUDE: " + gratCount + " total entries\n";
      rows3.slice(1, 6).forEach(function(r) {
        context += "  - " + (r[0] || '?') + " (" + (r[1] || '') + ")\n";
      });
      context += '\n';
    }
  } catch (e) {}

  // Tasks
  var taskTabs = ['Tasks', 'Daily_Log', 'Focus_Log', 'Jira_Log'];
  for (var i = 0; i < taskTabs.length; i++) {
    try {
      var taskRes = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: "'" + taskTabs[i] + "'!A2:B5",
      });
      var taskRows = taskRes.data.values;
      if (taskRows && taskRows.length > 0 && taskRows[0][0]) {
        context += "TOP TASKS (from " + taskTabs[i] + "):\n";
        taskRows.forEach(function(r) {
          context += "  - " + r[0] + (r[1] ? ' (' + r[1] + ')' : '') + "\n";
        });
        context += '\n';
        break;
      }
    } catch (e) {}
  }

  // Identity
  try {
    var res4 = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Trace_Identity_Profile'!A1:A20",
    });
    var rows4 = res4.data.values || [];
    if (rows4.length > 1) {
      context += "IDENTITY PROFILE:\n";
      rows4.slice(0, 15).forEach(function(r) {
        context += "  " + (r[0] || '') + "\n";
      });
      context += '\n';
    }
  } catch (e) {}

  // Business
  try {
    var bizCount = await getTabRowCount('Business_Idea_Ledger');
    context += "BUSINESS: " + bizCount + " ideas tracked\n\n";
  } catch (e) {}

  console.log("Context built: " + context.length + " chars");
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
    var systemPrompt = "You are Jarvis, Trace's personal Life OS AI agent. You are on a phone call with Trace.\n\nRULES:\n- Keep responses SHORT (3-5 sentences max). This is a phone call.\n- Be direct, confident, and motivational.\n- Speak naturally like a real assistant.\n- Reference actual numbers and data.\n- If there are unread emails, mention the most urgent ones.\n- After briefing, ask what Trace wants to dig into.\n- Never use markdown, bullet points, or formatting.\n\nLIFE OS DATA:\n" + fullContext;

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
    if (lowerMsg === 'briefing' || lowerMsg === 'brief' || lowerMsg === 'status') {
      var context = await buildLifeOSContext();
      var briefing = await askClaude(
        "You are Jarvis, Trace's Life OS agent on WhatsApp. Be concise. Use short paragraphs. No markdown formatting.\n\nLIFE OS DATA:\n" + context,
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

    // Regular conversation with Claude
    if (!history.systemPrompt) {
      var context2 = await buildLifeOSContext();
      history.systemPrompt = "You are Jarvis, Trace's personal Life OS AI agent on WhatsApp.\n\nRULES:\n- Keep responses SHORT (2-4 sentences). This is WhatsApp, not email.\n- Be direct, confident, and motivational.\n- Reference actual data.\n- No markdown, no bullet points, no formatting.\n\nLIFE OS DATA:\n" + context2;
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
      "You are Jarvis, Trace's Life OS agent. Be concise and direct. No markdown. Reference real data. If there are emails, prioritize them and tell Trace which to handle first.\n\nLIFE OS DATA:\n" + fullContext,
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
    var screenTimeMatch = context.match(/Screen time[:\s]*([\d.]+)/i);
    var gratitudeMatch = context.match(/([\d,]+)\s*gratitude/i);
    var businessMatch = context.match(/([\d,]+)\s*business idea/i);
    var debtMatch = context.match(/\$([.\d]+)\s*total balance/i);

    var screenTime = screenTimeMatch ? screenTimeMatch[1] : '?';
    var gratitudeCount = gratitudeMatch ? gratitudeMatch[1] : '?';
    var businessCount = businessMatch ? businessMatch[1] : '?';
    var debtAmount = debtMatch ? debtMatch[1] : '0';

    // Get email count
    var emailAccounts = Object.keys(gmailTokens);
    var totalUnread = 0;
    for (var ea = 0; ea < emailAccounts.length; ea++) {
      var emails = await getUnreadEmails(emailAccounts[ea], 50);
      totalUnread += emails.length;
    }

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
    html += '<div class="header">';
    html += '<div class="hex-container"><div class="hex-ring"><div class="hex-center">' + tabs.length + '</div></div></div>';
    html += '<div class="jarvis-title">J.A.R.V.I.S.</div>';
    html += '<div style="font-family:Rajdhani;font-size:1.1em;letter-spacing:8px;color:#3a5a7a;margin-top:5px;text-transform:uppercase;">LifeOS Command Center</div>';
    html += '<div class="status-bar">';
    html += '<div class="status-item"><div class="status-dot green"></div>SYSTEMS ONLINE</div>';
    html += '<div class="status-item"><div class="status-dot blue"></div>AI ACTIVE</div>';
    html += '<div class="status-item"><div class="status-dot green"></div>' + emailAccounts.length + ' EMAIL LINKED</div>';
    html += '<div class="status-item"><div class="status-dot blue"></div>VOICE READY</div>';
    html += '</div>';
    html += '</div>';

    // Stats Grid
    html += '<div class="grid">';

    var screenPct = Math.min(100, Math.round((parseFloat(screenTime) / 24) * 100));
    html += '<div class="card"><div class="label">Active Systems</div><div class="value">' + tabs.length + '</div><div class="sub">Tracking all life domains</div><div class="bar"><div class="bar-fill" style="width:100%"></div></div></div>';

    html += '<div class="card"><div class="label">Screen Time</div><div class="value">' + screenTime + 'h</div><div class="sub">' + (parseFloat(screenTime) > 10 ? 'WARNING — Exceeds optimal threshold' : 'Within optimal range') + '</div><div class="bar"><div class="bar-fill" style="width:' + screenPct + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Gratitude Index</div><div class="value">' + gratitudeCount + '</div><div class="sub">Lifetime entries — exceptional consistency</div><div class="bar"><div class="bar-fill" style="width:95%"></div></div></div>';

    html += '<div class="card"><div class="label">Business Intel</div><div class="value">' + businessCount + '</div><div class="sub">Ideas tracked and evaluated</div><div class="bar"><div class="bar-fill" style="width:76%"></div></div></div>';

    html += '<div class="card"><div class="label">Financial Status</div><div class="value">$' + debtAmount + '</div><div class="sub">Total debt — ' + (parseFloat(debtAmount) === 0 ? 'CLEAR' : 'Active balance') + '</div><div class="bar"><div class="bar-fill" style="width:' + (parseFloat(debtAmount) === 0 ? 100 : 30) + '%"></div></div></div>';

    html += '<div class="card"><div class="label">Inbox Status</div><div class="value">' + totalUnread + '</div><div class="sub">Unread across ' + emailAccounts.length + ' account(s)</div><div class="bar"><div class="bar-fill" style="width:' + Math.min(100, totalUnread * 3) + '%"></div></div></div>';

    html += '</div>';

    // Actions
    html += '<div class="actions">';
    html += '<a class="holo-btn green" href="/call?key=' + (process.env.CALL_SECRET || '') + '">Initiate Call</a>';
    html += '<a class="holo-btn" href="/briefing" target="_blank">Full Briefing</a>';
    html += '<a class="holo-btn" href="/gmail/summary" target="_blank">Email Intel</a>';
    html += '<a class="holo-btn" href="/scan" target="_blank">System Scan</a>';
    html += '<a class="holo-btn" href="/gmail/auth" target="_blank">Link Account</a>';
    html += '</div>';

    // All Systems
    html += '<div class="systems">';
    html += '<div class="systems-title">Active Subsystems (' + tabs.length + ')</div>';
    html += '<div class="systems-grid">';
    for (var t = 0; t < tabs.length; t++) {
      html += '<div class="sys-chip">' + tabs[t].replace(/_/g, ' ') + '</div>';
    }
    html += '</div></div>';

    // Live clock
    html += '<div class="clock" id="clock"></div>';
    html += '<script>function updateClock(){var d=new Date();var h=String(d.getHours()).padStart(2,"0");var m=String(d.getMinutes()).padStart(2,"0");var s=String(d.getSeconds()).padStart(2,"0");document.getElementById("clock").textContent=h+":"+m+":"+s+" // "+d.toLocaleDateString("en-US",{weekday:"long",year:"numeric",month:"long",day:"numeric"}).toUpperCase();}setInterval(updateClock,1000);updateClock();<\/script>';

    // Footer
    html += '<div class="footer">J.A.R.V.I.S. v2.0 // Built by Trace // Claude AI + Google Sheets + Twilio + Gmail</div>';

    html += '</div></body></html>';
    res.send(html);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   START SERVER
=========================== */

app.listen(PORT, function() {
  console.log("LifeOS Jarvis running on port " + PORT);
  console.log("Endpoints: /tabs /tab/:name /scan /scan/full /search?q= /summary /priority /briefing /call /voice /conversation /whatsapp /gmail/auth /gmail/unread /gmail/summary /dashboard");
});