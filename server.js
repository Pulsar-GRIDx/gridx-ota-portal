const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const mqtt = require('mqtt');
const mysql = require('mysql2/promise');
const { hydroHashHex } = require('./hydroHash');

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Configuration ───
const PORT = process.env.OTA_PORT || 4500;
const JWT_SECRET = process.env.OTA_JWT_SECRET || 'gridx-ota-secret-2026';
const FIRMWARE_DIR = process.env.FIRMWARE_DIR || '/home/gridxadmin/gridx-combined-backend/backend/hardware/files/Data';
const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://127.0.0.1:1883';
const MQTT_USER = process.env.MQTT_USER || 'gridx-backend';
const MQTT_PASS = process.env.MQTT_PASS || 'gridx-mqtt-2026';

// ─── Users (file-based for simplicity) ───
const USERS_FILE = path.join(__dirname, 'users.json');

function loadUsers() {
  if (!fs.existsSync(USERS_FILE)) {
    const hash = bcrypt.hashSync('GridX@OTA2026', 10);
    const users = [{ id: 1, username: 'admin', password: hash, role: 'admin' }];
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
    return users;
  }
  return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
}

// ─── MySQL pool ───
let dbPool;
async function getDb() {
  if (!dbPool) {
    dbPool = mysql.createPool({
      host: process.env.RDS_HOSTNAME || '127.0.0.1',
      user: process.env.RDS_USERNAME || 'gridX-sql-admin',
      password: process.env.RDS_PASSWORD || 'Refit+icepacks@wordpress89',
      port: process.env.RDS_PORT || 3306,
      database: process.env.RDS_DB_NAME || 'gridx',
      waitForConnections: true,
      connectionLimit: 5,
    });
  }
  return dbPool;
}

// ═══════════════════════════════════════════════════════════
// OTA Progress Tracking (in-memory)
// ═══════════════════════════════════════════════════════════
// { drn: { status: 'idle'|'updating'|'complete'|'error', progress: 0-100, version: '', startedAt: Date, updatedAt: Date, detail: '' } }
const otaState = {};
const sseClients = new Set();

function setOtaStatus(drn, status, progress, detail) {
  if (!otaState[drn]) {
    otaState[drn] = { status: 'idle', progress: 0, version: '', startedAt: null, updatedAt: null, detail: '' };
  }
  otaState[drn].status = status;
  otaState[drn].progress = progress;
  otaState[drn].updatedAt = new Date();
  if (detail !== undefined) otaState[drn].detail = detail;
  if (status === 'updating' && !otaState[drn].startedAt) {
    otaState[drn].startedAt = new Date();
  }
  if (status === 'complete' || status === 'error' || status === 'idle') {
    otaState[drn].startedAt = null;
  }

  // Broadcast to all SSE clients
  broadcastSSE({ type: 'ota_progress', drn, ...otaState[drn] });
}

function broadcastSSE(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    client.write(msg);
  }
}

// ─── MQTT client with OTA subscriptions ───
let mqttClient;
function getMqtt() {
  if (!mqttClient || !mqttClient.connected) {
    mqttClient = mqtt.connect(MQTT_BROKER, {
      clientId: `gridx-ota-portal-${Date.now()}`,
      username: MQTT_USER,
      password: MQTT_PASS,
    });

    mqttClient.on('connect', () => {
      console.log('[MQTT] Connected');
      // Subscribe to OTA request/status topics to track progress
      mqttClient.subscribe(['gx/+/ota/req', 'gx/+/health'], { qos: 0 }, (err) => {
        if (err) console.error('[MQTT] Subscribe error:', err.message);
        else console.log('[MQTT] Subscribed to OTA progress & health topics');
      });
    });

    mqttClient.on('message', (topic, message) => {
      try { handleMqttMessage(topic, message); }
      catch (err) { console.error('[MQTT] Message error:', err.message); }
    });

    mqttClient.on('error', (err) => console.error('[MQTT] Error:', err.message));
  }
  return mqttClient;
}

function handleMqttMessage(topic, buf) {
  const parts = topic.split('/');
  if (parts.length < 3 || parts[0] !== 'gx') return;
  const drn = parts[1];
  const type = parts[2];

  // Handle OTA request messages: gx/{drn}/ota/req
  if (type === 'ota' && parts[3] === 'req') {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    if (msg.action === 'chunk') {
      const offset = msg.offset || 0;
      const chunkSize = msg.size || 1024;
      // Get firmware size to calculate progress
      const fwInfo = getFirmwareInfo();
      if (fwInfo) {
        const progress = Math.min(100, Math.round(((offset + chunkSize) / fwInfo.size) * 100));
        setOtaStatus(drn, 'updating', progress, `Downloading: ${offset + chunkSize} / ${fwInfo.size} bytes`);
      }
    } else if (msg.action === 'complete') {
      const fwInfo = getFirmwareInfo();
      setOtaStatus(drn, 'complete', 100, `Updated to v${fwInfo ? fwInfo.version : '?'}`);
      console.log(`[OTA] ${drn}: Update complete`);
    } else if (msg.action === 'error') {
      setOtaStatus(drn, 'error', 0, msg.detail || 'Update failed');
      console.error(`[OTA] ${drn}: Error - ${msg.detail || 'unknown'}`);
    } else if (msg.action === 'check') {
      // Meter is checking for updates - not an active download
      console.log(`[OTA] ${drn}: Check request`);
    }
  }

  // Handle health reports: gx/{drn}/health
  if (type === 'health') {
    try {
      const data = JSON.parse(buf.toString());
      if (data.firmware) {
        // Store latest firmware version for this meter
        meterFirmwareVersions[drn] = {
          version: data.firmware,
          lastSeen: new Date(),
          uptime: data.uptime || 0,
        };
        broadcastSSE({ type: 'meter_firmware', drn, version: data.firmware });
      }
    } catch {}
  }
}

// In-memory cache of meter firmware versions from health reports
const meterFirmwareVersions = {};

function getFirmwareInfo() {
  const infoPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
  try {
    return JSON.parse(fs.readFileSync(infoPath, 'utf8'));
  } catch { return null; }
}

// ─── Multer for firmware upload ───
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, FIRMWARE_DIR),
  filename: (req, file, cb) => cb(null, 'firmware.bin'),
});
const upload = multer({
  storage,
  limits: { fileSize: 4 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.originalname.endsWith('.bin')) cb(null, true);
    else cb(new Error('Only .bin files accepted'));
  },
});

// ─── Auth middleware ───
function auth(req, res, next) {
  const header = req.headers.authorization;
  if (!header || !header.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No token provided' });
  }
  try {
    const decoded = jwt.verify(header.split(' ')[1], JWT_SECRET);
    req.user = decoded;
    next();
  } catch {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

// ═══════════════════════════════════════════════════════════
// API Routes
// ═══════════════════════════════════════════════════════════

// ─── Login ───
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const users = loadUsers();
  const user = users.find(u => u.username === username);
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '24h' });
  res.json({ token, user: { username: user.username, role: user.role } });
});

// ─── SSE endpoint for real-time OTA progress ───
app.get('/api/ota/events', auth, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  // Send current state immediately
  res.write(`data: ${JSON.stringify({ type: 'init', otaState, meterFirmwareVersions })}\n\n`);

  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// ─── OTA status for all meters ───
app.get('/api/ota/status', auth, (req, res) => {
  res.json({ otaState, meterFirmwareVersions });
});

// ─── Firmware info ───
app.get('/api/firmware/info', auth, (req, res) => {
  const infoPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
  if (!fs.existsSync(infoPath)) {
    return res.json({ available: false });
  }
  try {
    const info = JSON.parse(fs.readFileSync(infoPath, 'utf8'));
    const stat = fs.statSync(path.join(FIRMWARE_DIR, 'firmware.bin'));
    res.json({ available: true, ...info, uploaded_at: stat.mtime });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Firmware versions ───
app.get('/api/firmware/versions', auth, (req, res) => {
  try {
    const files = fs.readdirSync(FIRMWARE_DIR)
      .filter(f => f.startsWith('firmware_') && f.endsWith('.bin'))
      .map(f => {
        const stat = fs.statSync(path.join(FIRMWARE_DIR, f));
        const ver = f.replace('firmware_', '').replace('.bin', '').replace(/_/g, '.');
        return { filename: f, version: ver, size: stat.size, date: stat.mtime };
      })
      .sort((a, b) => new Date(b.date) - new Date(a.date));
    res.json({ versions: files });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Upload firmware ───
app.post('/api/firmware/upload', auth, upload.single('firmware'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file provided' });
  const { version } = req.body;
  if (!version) return res.status(400).json({ error: 'Missing version' });

  try {
    const fwPath = path.join(FIRMWARE_DIR, 'firmware.bin');
    const fwData = fs.readFileSync(fwPath);
    const hash = hydroHashHex(fwData, 'metering');
    const info = {
      version,
      url: 'https://tech.gridx-meters.com/files/firmware.bin',
      size: fwData.length,
      hash,
    };
    fs.writeFileSync(path.join(FIRMWARE_DIR, 'fw_latest.json'), JSON.stringify(info, null, 2));
    const backup = `firmware_${version.replace(/\./g, '_')}.bin`;
    fs.copyFileSync(fwPath, path.join(FIRMWARE_DIR, backup));

    // Reset all OTA states when new firmware is uploaded
    for (const drn in otaState) {
      otaState[drn].status = 'idle';
      otaState[drn].progress = 0;
      otaState[drn].detail = '';
    }

    console.log(`[OTA] Uploaded v${version} (${fwData.length} bytes) hash=${hash}`);
    res.json({ success: true, message: `Firmware v${version} uploaded`, firmware: info });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── List meters (with firmware version from health reports) ───
app.get('/api/meters', auth, async (req, res) => {
  try {
    const db = await getDb();
    // Get meters with their last reported firmware version from MeterHealthReport
    const [rows] = await db.query(
      `SELECT m.DRN as drn, CONCAT(m.Name, ' ', m.Surname) as customer,
              m.City as city, m.Region as region, m.SIMNumber as sim,
              h.firmware as firmware_version, h.created_at as last_health_report
       FROM MeterProfileReal m
       LEFT JOIN (
         SELECT DRN, firmware, created_at,
                ROW_NUMBER() OVER (PARTITION BY DRN ORDER BY created_at DESC) as rn
         FROM MeterHealthReport
         WHERE firmware IS NOT NULL AND firmware != ''
       ) h ON m.DRN = h.DRN AND h.rn = 1
       ORDER BY m.DRN`
    );

    // Merge with in-memory firmware versions (more recent than DB)
    const meters = rows.map(m => {
      const memVersion = meterFirmwareVersions[m.drn];
      if (memVersion && (!m.last_health_report || memVersion.lastSeen > new Date(m.last_health_report))) {
        m.firmware_version = memVersion.version;
      }
      // Add OTA status
      m.ota_status = otaState[m.drn] || { status: 'idle', progress: 0, detail: '' };
      return m;
    });

    res.json({ meters });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Push OTA to single meter ───
app.post('/api/ota/push', auth, (req, res) => {
  const { drn } = req.body;
  if (!drn) return res.status(400).json({ error: 'Missing drn' });

  try {
    const infoPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
    const info = JSON.parse(fs.readFileSync(infoPath, 'utf8'));
    const client = getMqtt();
    const cmd = {
      type: 'ota_mqtt',
      action: 'start',
      version: info.version,
      size: info.size,
      hash: info.hash,
      chunk_size: 1024,
    };
    client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });

    // Set initial OTA state
    setOtaStatus(drn, 'updating', 0, `Pushed v${info.version} - waiting for meter response`);
    if (!otaState[drn]) otaState[drn] = {};
    otaState[drn].version = info.version;

    console.log(`[OTA] Pushed v${info.version} to ${drn}`);
    res.json({ success: true, message: `OTA pushed to ${drn}`, command: cmd });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Push OTA to ALL meters ───
app.post('/api/ota/push-all', auth, async (req, res) => {
  try {
    const infoPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
    const info = JSON.parse(fs.readFileSync(infoPath, 'utf8'));
    const db = await getDb();
    const [rows] = await db.query('SELECT DRN as drn FROM MeterProfileReal');

    const client = getMqtt();
    const cmd = {
      type: 'ota_mqtt',
      action: 'start',
      version: info.version,
      size: info.size,
      hash: info.hash,
      chunk_size: 1024,
    };
    const cmdStr = JSON.stringify(cmd);

    let pushed = 0;
    const results = [];
    for (const row of rows) {
      client.publish(`gx/${row.drn}/cmd`, cmdStr, { qos: 1 });
      setOtaStatus(row.drn, 'updating', 0, `Pushed v${info.version} - waiting for meter response`);
      if (!otaState[row.drn]) otaState[row.drn] = {};
      otaState[row.drn].version = info.version;
      results.push(row.drn);
      pushed++;
    }

    console.log(`[OTA] Pushed v${info.version} to ALL ${pushed} meters`);
    res.json({
      success: true,
      message: `OTA pushed to ${pushed} meter(s)`,
      meters: results,
      firmware: { version: info.version, hash: info.hash },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════
// Serve frontend
// ═══════════════════════════════════════════════════════════
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ═══════════════════════════════════════════════════════════
// Start
// ═══════════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`[OTA Portal] Running on port ${PORT}`);
  getMqtt();
});
