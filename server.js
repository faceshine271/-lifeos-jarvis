require('dotenv').config();

const express = require('express');
const bodyParser = require('body-parser');
const { google } = require('googleapis');
const twilio = require('twilio');
const path = require('path');

const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;

console.log("🚀 Starting LifeOS Jarvis...");

/* ===========================
   GOOGLE SHEETS AUTH
=========================== */

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

if (!SPREADSHEET_ID) {
  console.error("❌ Missing SPREADSHEET_ID in .env");
  process.exit(1);
}

const auth = new google.auth.GoogleAuth({
  keyFile: path.join(__dirname, 'google-credentials.json'),
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth });
console.log("✅ Google Auth Ready");

/* ===========================
   TWILIO CLIENT
=========================== */

const twilioClient = twilio(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN
);

const TWILIO_NUMBER = '+18884310969';
const MY_NUMBER = '+18167392734';
console.log("✅ Twilio Ready");

/* ===========================
   CLAUDE API KEY
=========================== */

const CLAUDE_API_KEY = process.env.CLAUDE_API_KEY;
if (!CLAUDE_API_KEY) {
  console.error("⚠️ Missing CLAUDE_API_KEY — voice AI conversation won't work");
}
console.log("✅ Claude API Ready");

/* ===========================
   In-memory conversation history per call
=========================== */

const callHistory = {};

/* ===========================
   HELPERS
=========================== */

async function getAllTabNames() {
  const res = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
    fields: 'sheets.properties.title',
  });
  return res.data.sheets.map(s => s.properties.title);
}

async function getTabData(tabName) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `'${tabName}'!A1:ZZ`,
    });
    const rows = res.data.values || [];
    if (rows.length === 0) return { tab: tabName, headers: [], rowCount: 0, rows: [] };
    return { tab: tabName, headers: rows[0], rowCount: rows.length - 1, rows: rows.slice(1) };
  } catch (err) {
    return { tab: tabName, error: err.message, headers: [], rowCount: 0, rows: [] };
  }
}

/* ===========================
   Build Life OS context for Claude
=========================== */

async function buildLifeOSContext() {
  const tabs = await getAllTabNames();
  let totalRows = 0;
  const tabSummaries = [];

  for (const tab of tabs) {
    const data = await getTabData(tab);
    totalRows += data.rowCount;
    tabSummaries.push(`- ${tab}: ${data.rowCount} rows, columns: [${data.headers.join(', ')}]`);
  }

  let context = `LIFE OS OVERVIEW:\n`;
  context += `Total tabs: ${tabs.length}\n`;
  context += `Total data points: ${totalRows}\n\n`;
  context += `TAB DETAILS:\n${tabSummaries.join('\n')}\n\n`;

  // Pull key data snapshots
  // Debt
  try {
    const debt = await getTabData('Ultimate_Debt_Tracker_Advanced');
    if (debt.rows.length > 0) {
      const statusCol = debt.headers.indexOf('Status');
      const nameCol = debt.headers.indexOf('Account Name');
      const balCol = debt.headers.indexOf('Current_Balance');
      const typeCol = debt.headers.indexOf('Account Type');

      const activeDebts = debt.rows.filter(r => statusCol === -1 || (r[statusCol] || '').toLowerCase() === 'active');

      let totalBal = 0;
      const debtLines = activeDebts.slice(0, 15).map(r => {
        const bal = parseFloat((r[balCol] || '0').replace(/[$,]/g, ''));
        if (!isNaN(bal)) totalBal += bal;
        return `  ${r[nameCol] || 'Unknown'} (${r[typeCol] || '?'}): $${r[balCol] || '0'}`;
      });

      context += `\nACTIVE DEBTS (${activeDebts.length} accounts, ~$${Math.round(totalBal).toLocaleString()} total):\n${debtLines.join('\n')}\n`;
    }
  } catch (e) {}

  // Screen time
  try {
    const dash = await getTabData('Dashboard');
    if (dash.rows.length > 0) {
      context += `\nSCREEN TIME:\n`;
      context += `  Daily screen time: ${dash.rows[0][0] || '?'} hours\n`;
      context += `  Top app: ${dash.rows[0][1] || '?'}\n`;

      const topApps = dash.rows.slice(1, 8).map(r => `  ${r[0] || '?'}: ${r[4] || '?'} hrs/day`);
      if (topApps.length > 0) context += `  Top apps:\n${topApps.join('\n')}\n`;
    }
  } catch (e) {}

  // Recent gratitude
  try {
    const grat = await getTabData('Gratitude_Memory');
    if (grat.rows.length > 0) {
      const recent = grat.rows.slice(-5).reverse();
      const gratLines = recent.map(r => `  - ${r[0] || '?'} (${r[1] || ''})`);
      context += `\nRECENT GRATITUDE ENTRIES:\n${gratLines.join('\n')}\n`;
    }
  } catch (e) {}

  // Priority tasks
  const taskTabs = ['Tasks', 'Daily_Log', 'Focus_Log', 'Jira_Log'];
  for (const tabName of taskTabs) {
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `'${tabName}'!A2:B5`,
      });
      const rows = response.data.values;
      if (rows && rows.length > 0 && rows[0][0]) {
        const taskLines = rows.map(r => `  - ${r[0]}${r[1] ? ' (' + r[1] + ')' : ''}`);
        context += `\nTOP TASKS (from ${tabName}):\n${taskLines.join('\n')}\n`;
        break;
      }
    } catch (e) {}
  }

  return context;
}

/* ===========================
   Call Claude API
=========================== */

async function askClaude(systemPrompt, messages) {
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': CLAUDE_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      system: systemPrompt,
      messages: messages,
    }),
  });

  const data = await response.json();

  if (data.content && data.content.length > 0) {
    return data.content[0].text;
  }

  return "I'm having trouble processing that right now.";
}

/* ===========================
   POST /voice — Initial greeting when call connects
=========================== */

app.post('/voice', async (req, res) => {
  const twiml = new twilio.twiml.VoiceResponse();
  const callSid = req.body.CallSid || 'unknown';

  // Initialize conversation history for this call
  callHistory[callSid] = [];

  try {
    const lifeContext = await buildLifeOSContext();

    // Store the context for this call
    callHistory[callSid].context = lifeContext;

    // Build opening briefing with Claude
    const systemPrompt = `You are Jarvis, Trace's personal Life OS AI agent. You are calling Trace on the phone to give him a briefing.

RULES:
- Keep responses SHORT (2-4 sentences max). This is a phone call, not an essay.
- Be direct, confident, and motivational.
- Speak naturally like a real assistant, not a robot.
- Reference actual data from the Life OS context below.
- After the briefing, ask Trace what he wants to know or do.

LIFE OS DATA:
${lifeContext}`;

    const greeting = await askClaude(systemPrompt, [
      { role: 'user', content: 'Give Trace a quick opening briefing covering his system overview, finances, and top priorities. End by asking what he wants to dig into.' }
    ]);

    callHistory[callSid].systemPrompt = systemPrompt;
    callHistory[callSid].messages = [
      { role: 'assistant', content: greeting }
    ];

    // Speak the greeting, then listen
    const gather = twiml.gather({
      input: 'speech',
      action: '/conversation',
      method: 'POST',
      speechTimeout: 3,
      language: 'en-US',
    });

    gather.say({ voice: 'Polly.Matthew' }, greeting);

    // If no input, prompt again
    twiml.say({ voice: 'Polly.Matthew' }, "I didn't catch that. What would you like to know?");
    twiml.redirect('/voice-listen');

  } catch (err) {
    console.error("❌ Voice Error:", err.message);
    twiml.say("There was an error starting your briefing. Please try again.");
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   POST /voice-listen — Re-prompt for input
=========================== */

app.post('/voice-listen', async (req, res) => {
  const twiml = new twilio.twiml.VoiceResponse();

  const gather = twiml.gather({
    input: 'speech',
    action: '/conversation',
    method: 'POST',
    speechTimeout: 3,
    language: 'en-US',
  });

  gather.say({ voice: 'Polly.Matthew' }, "I'm listening. What do you want to know?");

  twiml.say({ voice: 'Polly.Matthew' }, "I still didn't hear anything. Call back when you're ready. Goodbye.");

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   POST /conversation — Handle back-and-forth
=========================== */

app.post('/conversation', async (req, res) => {
  const twiml = new twilio.twiml.VoiceResponse();
  const callSid = req.body.CallSid || 'unknown';
  const userSpeech = req.body.SpeechResult || '';

  console.log(`🎤 Trace said: "${userSpeech}"`);

  // Check for goodbye
  const goodbyeWords = ['goodbye', 'bye', 'hang up', 'end call', 'that\'s all', 'nothing', 'i\'m good'];
  if (goodbyeWords.some(w => userSpeech.toLowerCase().includes(w))) {
    twiml.say({ voice: 'Polly.Matthew' }, "Copy that, Trace. Go execute. Jarvis out.");
    twiml.hangup();
    delete callHistory[callSid];
    res.type('text/xml');
    return res.send(twiml.toString());
  }

  try {
    // Get or rebuild conversation state
    const history = callHistory[callSid] || {};
    const systemPrompt = history.systemPrompt || `You are Jarvis, Trace's personal Life OS AI agent on a phone call.
Keep responses SHORT (2-4 sentences). Be direct and useful.`;

    const messages = history.messages || [];

    // Add user message
    messages.push({ role: 'user', content: userSpeech });

    // If user asks about specific tab data, try to fetch it
    let extraContext = '';
    const lowerSpeech = userSpeech.toLowerCase();

    // Dynamic data fetch based on what user asks about
    const dataKeywords = {
      'debt': 'Ultimate_Debt_Tracker_Advanced',
      'finance': 'Ultimate_Debt_Tracker_Advanced',
      'money': 'Ultimate_Debt_Tracker_Advanced',
      'loan': 'Ultimate_Debt_Tracker_Advanced',
      'screen time': 'Dashboard',
      'productivity': 'Dashboard',
      'app': 'Dashboard',
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

    for (const [keyword, tabName] of Object.entries(dataKeywords)) {
      if (lowerSpeech.includes(keyword)) {
        try {
          const tabData = await getTabData(tabName);
          if (tabData.rows.length > 0) {
            const sample = tabData.rows.slice(-10).map(r => r.join(' | ')).join('\n');
            extraContext += `\n\nDETAILED DATA FROM ${tabName} (last 10 rows):\nHeaders: ${tabData.headers.join(', ')}\n${sample}`;
          }
        } catch (e) {}
        break;
      }
    }

    if (extraContext) {
      messages[messages.length - 1].content += `\n\n[SYSTEM: Here is fresh data for this question]${extraContext}`;
    }

    // Ask Claude
    const response = await askClaude(systemPrompt, messages);

    console.log(`🤖 Jarvis: "${response}"`);

    // Store in history
    messages.push({ role: 'assistant', content: response });
    callHistory[callSid] = { ...history, messages };

    // Speak response and listen again
    const gather = twiml.gather({
      input: 'speech',
      action: '/conversation',
      method: 'POST',
      speechTimeout: 3,
      language: 'en-US',
    });

    gather.say({ voice: 'Polly.Matthew' }, response);

    // If no response, prompt
    twiml.say({ voice: 'Polly.Matthew' }, "Anything else, Trace?");
    twiml.redirect('/voice-listen');

  } catch (err) {
    console.error("❌ Conversation Error:", err.message);
    twiml.say({ voice: 'Polly.Matthew' }, "I had trouble processing that. Can you repeat?");
    twiml.redirect('/voice-listen');
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

/* ===========================
   GET /call — Trigger Jarvis to call you
=========================== */

app.get('/call', async (req, res) => {
  try {
    console.log("📞 Initiating call to Trace...");

    const baseUrl = req.query.url || process.env.BASE_URL;

    if (!baseUrl) {
      return res.status(400).json({
        error: "Need a public URL for Twilio to reach your server.",
        steps: [
          "1. Install ngrok: npm install -g ngrok",
          "2. Run: ngrok http 3000",
          "3. Copy the https URL",
          "4. Visit: localhost:3000/call?url=YOUR_NGROK_URL",
        ],
      });
    }

    const call = await twilioClient.calls.create({
      to: MY_NUMBER,
      from: TWILIO_NUMBER,
      url: `${baseUrl}/voice`,
    });

    console.log(`✅ Call initiated: ${call.sid}`);
    res.json({
      message: "📞 Calling you now, Trace.",
      callSid: call.sid,
    });

  } catch (err) {
    console.error("❌ Call Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

/* ===========================
   GET /tabs
=========================== */

app.get('/tabs', async (req, res) => {
  try {
    const tabs = await getAllTabNames();
    res.json({ tabCount: tabs.length, tabs });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /tab/:name
=========================== */

app.get('/tab/:name', async (req, res) => {
  try {
    const data = await getTabData(req.params.name);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /scan
=========================== */

app.get('/scan', async (req, res) => {
  try {
    const tabs = await getAllTabNames();
    const results = [];
    for (const tab of tabs) {
      const data = await getTabData(tab);
      results.push({ tab: data.tab, headers: data.headers, rowCount: data.rowCount, error: data.error || null });
    }
    const totalRows = results.reduce((sum, t) => sum + t.rowCount, 0);
    res.json({ totalTabs: tabs.length, totalRows, tabs: results });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /scan/full
=========================== */

app.get('/scan/full', async (req, res) => {
  try {
    const tabs = await getAllTabNames();
    const results = [];
    for (const tab of tabs) { results.push(await getTabData(tab)); }
    const totalRows = results.reduce((sum, t) => sum + t.rowCount, 0);
    res.json({ totalTabs: tabs.length, totalRows, tabs: results });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /search?q=keyword
=========================== */

app.get('/search', async (req, res) => {
  try {
    const query = (req.query.q || '').toLowerCase().trim();
    if (!query) return res.status(400).json({ error: "Provide ?q=search_term" });
    const tabs = await getAllTabNames();
    const matches = [];
    for (const tab of tabs) {
      const data = await getTabData(tab);
      if (data.error) continue;
      data.rows.forEach((row, i) => {
        if (row.join(' ').toLowerCase().includes(query)) {
          const obj = {};
          data.headers.forEach((h, j) => { obj[h] = row[j] || ''; });
          matches.push({ tab, row: i + 2, data: obj });
        }
      });
    }
    res.json({ query, matchCount: matches.length, matches });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /summary
=========================== */

app.get('/summary', async (req, res) => {
  try {
    const tabs = await getAllTabNames();
    const summary = { totalTabs: tabs.length, categories: {} };
    for (const tab of tabs) {
      const data = await getTabData(tab);
      let cat = 'other';
      const l = tab.toLowerCase();
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
      summary.categories[cat].push({ tab, headers: data.headers, rowCount: data.rowCount });
    }
    res.json(summary);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /briefing — Text preview
=========================== */

app.get('/briefing', async (req, res) => {
  try {
    const context = await buildLifeOSContext();
    const response = await askClaude(
      `You are Jarvis, Trace's Life OS agent. Give a concise briefing based on this data:\n\n${context}`,
      [{ role: 'user', content: 'Give me my full Life OS briefing.' }]
    );
    res.json({ briefing: response });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   GET /priority
=========================== */

app.get('/priority', async (req, res) => {
  try {
    const taskTabs = ['Tasks', 'Daily_Log', 'Focus_Log', 'Jira_Log'];
    for (const tabName of taskTabs) {
      try {
        const r = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `'${tabName}'!A2:B10` });
        if (r.data.values && r.data.values.length > 0) return res.json({ source: tabName, task: r.data.values[0][0], detail: r.data.values[0][1] || '' });
      } catch (e) {}
    }
    res.json({ message: "No tasks found." });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* ===========================
   START SERVER
=========================== */

app.listen(PORT, () => {
  console.log(`🔥 LifeOS Jarvis running on port ${PORT}`);
  console.log('');
  console.log('📡 Available endpoints:');
  console.log(`   GET  /tabs         → List all tab names`);
  console.log(`   GET  /tab/:name    → Get full data from one tab`);
  console.log(`   GET  /scan         → Overview of every tab`);
  console.log(`   GET  /scan/full    → Full data dump`);
  console.log(`   GET  /search?q=    → Search across all tabs`);
  console.log(`   GET  /summary      → Categorized summary`);
  console.log(`   GET  /priority     → Top priority task`);
  console.log(`   GET  /briefing     → Preview AI briefing`);
  console.log(`   GET  /call         → 📞 Jarvis calls you (needs ngrok URL)`);
  console.log(`   POST /voice        → Twilio voice webhook`);
  console.log(`   POST /conversation → Twilio conversation webhook`);
  console.log('');
  console.log('🧠 To make Jarvis call you:');
  console.log('   1. Run: ngrok http 3000');
  console.log('   2. Visit: localhost:3000/call?url=YOUR_NGROK_URL');
  console.log('');
});