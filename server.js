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

// ─── Create OTA tracking table on startup ───
async function ensureOTATable() {
  try {
    const db = await getDb();
    await db.query(`
      CREATE TABLE IF NOT EXISTS MeterOTAStatus (
        DRN VARCHAR(50) PRIMARY KEY,
        firmware_version VARCHAR(20) NOT NULL,
        status ENUM('idle','updating','complete','error') DEFAULT 'idle',
        progress INT DEFAULT 0,
        detail VARCHAR(255) DEFAULT '',
        pushed_at DATETIME NULL,
        updated_at DATETIME NULL,
        completed_at DATETIME NULL
      )
    `);
    console.log('[DB] MeterOTAStatus table ready');

    // Load persisted OTA state into memory
    const [rows] = await db.query('SELECT * FROM MeterOTAStatus');
    for (const row of rows) {
      otaState[row.DRN] = {
        status: row.status,
        progress: row.progress,
        version: row.firmware_version,
        detail: row.detail || '',
        startedAt: row.pushed_at,
        updatedAt: row.updated_at,
        completedAt: row.completed_at,
      };
    }
    console.log(`[DB] Loaded OTA state for ${rows.length} meter(s)`);
  } catch (err) {
    console.error('[DB] Table creation error:', err.message);
  }
}

// ═══════════════════════════════════════════════════════════
// OTA Progress Tracking (in-memory + persisted to MySQL)
// ═══════════════════════════════════════════════════════════
const otaState = {};
const sseClients = new Set();

async function setOtaStatus(drn, status, progress, detail) {
  if (!otaState[drn]) {
    otaState[drn] = { status: 'idle', progress: 0, version: '', startedAt: null, updatedAt: null, completedAt: null, detail: '' };
  }
  otaState[drn].status = status;
  otaState[drn].progress = progress;
  otaState[drn].updatedAt = new Date();
  if (detail !== undefined) otaState[drn].detail = detail;
  if (status === 'updating' && !otaState[drn].startedAt) {
    otaState[drn].startedAt = new Date();
  }
  if (status === 'complete') {
    otaState[drn].completedAt = new Date();
  }
  if (status === 'idle') {
    otaState[drn].startedAt = null;
  }

  // Persist to database
  try {
    const db = await getDb();
    await db.query(
      `INSERT INTO MeterOTAStatus (DRN, firmware_version, status, progress, detail, pushed_at, updated_at, completed_at)
       VALUES (?, ?, ?, ?, ?, ?, NOW(), ?)
       ON DUPLICATE KEY UPDATE
         firmware_version = VALUES(firmware_version),
         status = VALUES(status),
         progress = VALUES(progress),
         detail = VALUES(detail),
         pushed_at = COALESCE(VALUES(pushed_at), pushed_at),
         updated_at = NOW(),
         completed_at = COALESCE(VALUES(completed_at), completed_at)`,
      [
        drn,
        otaState[drn].version || '',
        status,
        progress,
        detail || '',
        otaState[drn].startedAt,
        status === 'complete' ? new Date() : null,
      ]
    );
  } catch (err) {
    console.error(`[DB] OTA status persist error for ${drn}:`, err.message);
  }

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
      mqttClient.subscribe(['gx/+/ota/req', 'gx/+/health', 'gx/+/nextion/req', 'gx/+/ack', 'gx/+/power', 'gx/+/energy', 'gx/+/net_energy'], { qos: 0 }, (err) => {
        if (err) console.error('[MQTT] Subscribe error:', err.message);
        else console.log('[MQTT] Subscribed to OTA, health, nextion, ack & telemetry topics');
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

  if (type === 'ota' && parts[3] === 'req') {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    if (msg.action === 'chunk') {
      const offset = msg.offset || 0;
      const chunkSize = msg.size || 1024;
      const fwInfo = getFirmwareInfo();
      if (fwInfo) {
        const progress = Math.min(100, Math.round(((offset + chunkSize) / fwInfo.size) * 100));
        setOtaStatus(drn, 'updating', progress, `Downloading: ${offset + chunkSize} / ${fwInfo.size} bytes`);
      }
    } else if (msg.action === 'complete') {
      const fwInfo = getFirmwareInfo();
      if (fwInfo && otaState[drn]) {
        otaState[drn].version = fwInfo.version;
      }
      setOtaStatus(drn, 'complete', 100, `Updated to v${fwInfo ? fwInfo.version : '?'}`);
      console.log(`[OTA] ${drn}: Update complete`);
    } else if (msg.action === 'error') {
      setOtaStatus(drn, 'error', 0, msg.detail || 'Update failed');
      console.error(`[OTA] ${drn}: Error - ${msg.detail || 'unknown'}`);
    } else if (msg.action === 'check') {
      console.log(`[OTA] ${drn}: Check request`);
    }
  }

  // Handle Nextion TFT OTA requests: gx/{drn}/nextion/req
  if (type === 'nextion' && parts[3] === 'req') {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    if (msg.action === 'chunk') {
      const offset = msg.offset || 0;
      const requestedSize = msg.size || 4096;
      const tftPath = path.join(FIRMWARE_DIR, 'nextion.tft');
      const tft = loadNextionTft(tftPath);
      if (!tft) {
        console.error(`[Nextion] No TFT file for chunk request from ${drn}`);
        return;
      }
      const remaining = tft.size - offset;
      if (remaining <= 0) {
        console.log(`[Nextion] ${drn}: offset ${offset} beyond TFT size ${tft.size}`);
        return;
      }
      const chunkSize = Math.min(requestedSize, remaining);

      // Binary response: [4B offset BE][4B length BE][data]
      const header = Buffer.alloc(8);
      header.writeUInt32BE(offset, 0);
      header.writeUInt32BE(chunkSize, 4);
      const chunkData = tft.data.slice(offset, offset + chunkSize);
      const response = Buffer.concat([header, chunkData]);

      const dataTopic = `gx/${drn}/nextion/data`;
      mqttClient.publish(dataTopic, response, { qos: 1 }, (err) => {
        if (err) {
          console.error(`[Nextion] Publish failed for ${drn} offset ${offset}:`, err.message);
          return;
        }
        const progress = Math.min(100, Math.round(((offset + chunkSize) / tft.size) * 100));
        setNextionOtaStatus(drn, 'updating', progress, `Downloading: ${offset + chunkSize} / ${tft.size} bytes`);
        if (progress % 10 === 0 || offset === 0 || offset + chunkSize >= tft.size) {
          console.log(`[Nextion] ${drn}: chunk offset=${offset} size=${chunkSize} progress=${progress}%`);
        }
      });

    } else if (msg.action === 'complete') {
      setNextionOtaStatus(drn, 'complete', 100, 'Nextion display updated');
      console.log(`[Nextion] ${drn}: TFT update complete`);
    } else if (msg.action === 'error') {
      setNextionOtaStatus(drn, 'error', 0, msg.detail || 'Nextion update failed');
      console.error(`[Nextion] ${drn}: Error - ${msg.detail || 'unknown'}`);
    } else if (msg.action === 'flashing') {
      const progress = msg.progress || 0;
      setNextionOtaStatus(drn, 'updating', progress, `Flashing to display: ${progress}%`);
    }
  }

  if (type === 'health') {
    try {
      const data = JSON.parse(buf.toString());
      if (data.firmware) {
        meterFirmwareVersions[drn] = {
          version: data.firmware,
          lastSeen: new Date(),
          uptime: data.uptime || 0,
        };
        broadcastSSE({ type: 'meter_firmware', drn, version: data.firmware });
      }
    } catch {}
  }

  if (type === 'ack') {
    try {
      const data = JSON.parse(buf.toString());
      const ackType = data.type || 'unknown';
      const key = `${drn}_${ackType}`;
      if (data.msg && !data.message) data.message = data.msg;
      cmdResponses[key] = { drn, ...data, receivedAt: new Date() };
      broadcastSSE({ type: 'cmd_ack', drn, ack: data });
      console.log(`[CMD] Ack from ${drn}: ${ackType}`);
    } catch {}
  }

  if (type === 'power' && buf.length >= 37 && buf[0] === 0x01) {
    try {
      const current = buf.readFloatLE(1);
      const voltage = buf.readFloatLE(5);
      const active_power = buf.readFloatLE(9);
      const power_factor = buf.readFloatLE(29);
      const direction = active_power < 0 ? 'EXPORT' : (active_power > 5 ? 'IMPORT' : 'IDLE');
      const data = { type: 'power_report', voltage: voltage, current: current, power: active_power, power_factor: power_factor, direction };
      const key = `${drn}_power_report`;
      cmdResponses[key] = { drn, ...data, receivedAt: new Date() };
      broadcastSSE({ type: 'cmd_ack', drn, ack: data });
    } catch {}
  }

  if (type === 'energy' && buf.length >= 23 && buf[0] === 0x02) {
    try {
      const active_energy = buf.readFloatLE(1);
      const data = { type: 'energy_report', import_kwh: active_energy / 1000, export_kwh: 0, net_kwh: active_energy / 1000 };
      const key = `${drn}_energy_report`;
      cmdResponses[key] = { drn, ...data, receivedAt: new Date() };
      broadcastSSE({ type: 'cmd_ack', drn, ack: data });
    } catch {}
  }

  if (type === 'net_energy' && buf.length >= 17 && buf[0] === 0x07) {
    try {
      const import_e = buf.readFloatLE(1);
      const export_e = buf.readFloatLE(5);
      const net_e = buf.readFloatLE(9);
      const data = { type: 'net_energy_report', import_kwh: import_e / 1000, export_kwh: export_e / 1000, net_kwh: net_e / 1000 };
      const key = `${drn}_net_energy_report`;
      cmdResponses[key] = { drn, ...data, receivedAt: new Date() };
      broadcastSSE({ type: 'cmd_ack', drn, ack: data });
    } catch {}
  }
}

const meterFirmwareVersions = {};
const cmdResponses = {};

function getFirmwareInfo() {
  const infoPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
  try {
    return JSON.parse(fs.readFileSync(infoPath, 'utf8'));
  } catch { return null; }
}

function getNextionInfo() {
  const infoPath = path.join(FIRMWARE_DIR, 'nextion_latest.json');
  try {
    return JSON.parse(fs.readFileSync(infoPath, 'utf8'));
  } catch { return null; }
}

// ─── Nextion OTA state (separate from firmware OTA) ───
const nextionOtaState = {};

async function setNextionOtaStatus(drn, status, progress, detail) {
  if (!nextionOtaState[drn]) {
    nextionOtaState[drn] = { status: 'idle', progress: 0, startedAt: null, updatedAt: null, completedAt: null, detail: '' };
  }
  nextionOtaState[drn].status = status;
  nextionOtaState[drn].progress = progress;
  nextionOtaState[drn].updatedAt = new Date();
  if (detail !== undefined) nextionOtaState[drn].detail = detail;
  if (status === 'updating' && !nextionOtaState[drn].startedAt) {
    nextionOtaState[drn].startedAt = new Date();
  }
  if (status === 'complete') {
    nextionOtaState[drn].completedAt = new Date();
  }
  if (status === 'idle') {
    nextionOtaState[drn].startedAt = null;
  }
  broadcastSSE({ type: 'nextion_progress', drn, ...nextionOtaState[drn] });
}

// ─── Nextion TFT file cache ───
let nextionCache = null;

function loadNextionTft(tftPath) {
  try {
    const stat = fs.statSync(tftPath);
    if (nextionCache && nextionCache.path === tftPath && nextionCache.mtime === stat.mtimeMs) {
      return nextionCache;
    }
    const data = fs.readFileSync(tftPath);
    nextionCache = { path: tftPath, data, size: data.length, mtime: stat.mtimeMs };
    console.log(`[Nextion] TFT loaded: ${tftPath} (${data.length} bytes)`);
    return nextionCache;
  } catch (err) {
    console.error(`[Nextion] Failed to load TFT: ${err.message}`);
    return null;
  }
}

// ─── GRIDx Firmware Signature Verification ───
const GRIDX_SIG_MARKER = 'GRIDX_FW_SIG:';
const GRIDX_MFR_CODE = 260;

function verifyGRIDxSignature(fwData) {
  // Scan the binary for the GRIDx signature marker
  const sigStr = GRIDX_SIG_MARKER;
  const sigBuf = Buffer.from(sigStr, 'ascii');

  let sigOffset = -1;
  for (let i = 0; i < fwData.length - sigBuf.length; i++) {
    if (fwData.compare(sigBuf, 0, sigBuf.length, i, i + sigBuf.length) === 0) {
      sigOffset = i;
      break;
    }
  }

  if (sigOffset === -1) {
    return { valid: false, error: 'No GRIDx firmware signature found in binary' };
  }

  // Extract the full signature string (until null byte or max 128 chars)
  let endOffset = sigOffset;
  while (endOffset < fwData.length && fwData[endOffset] !== 0 && (endOffset - sigOffset) < 128) {
    endOffset++;
  }
  const sigString = fwData.toString('ascii', sigOffset, endOffset);

  // Parse: "GRIDX_FW_SIG:MFR=260:VER=0.60.6"
  const mfrMatch = sigString.match(/MFR=(\d+)/);
  const verMatch = sigString.match(/VER=([\d.]+)/);

  if (!mfrMatch) {
    return { valid: false, error: 'GRIDx signature found but missing manufacturer code' };
  }

  const mfrCode = parseInt(mfrMatch[1]);
  if (mfrCode !== GRIDX_MFR_CODE) {
    return { valid: false, error: `Invalid manufacturer code: ${mfrCode} (expected ${GRIDX_MFR_CODE})` };
  }

  const embeddedVersion = verMatch ? verMatch[1] : 'unknown';
  console.log(`[OTA] GRIDx signature verified: MFR=${mfrCode}, VER=${embeddedVersion}`);
  return { valid: true, mfr: mfrCode, version: embeddedVersion };
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

// ─── Multer for Nextion TFT upload ───
const tftStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, FIRMWARE_DIR),
  filename: (req, file, cb) => cb(null, 'nextion.tft'),
});
const tftUpload = multer({
  storage: tftStorage,
  limits: { fileSize: 16 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const name = file.originalname.toLowerCase();
    if (name.endsWith('.tft') || name.endsWith('.hmi')) cb(null, true);
    else cb(new Error('Only .tft and .HMI files accepted'));
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

app.get('/api/ota/events', auth, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write(`data: ${JSON.stringify({ type: 'init', otaState, meterFirmwareVersions, nextionOtaState })}\n\n`);
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/ota/status', auth, (req, res) => {
  res.json({ otaState, meterFirmwareVersions, nextionOtaState });
});

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

app.post('/api/firmware/upload', auth, upload.single('firmware'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file provided' });
  const { version } = req.body;
  if (!version) return res.status(400).json({ error: 'Missing version' });

  try {
    const fwPath = path.join(FIRMWARE_DIR, 'firmware.bin');
    const fwData = fs.readFileSync(fwPath);

    // Verify GRIDx firmware signature
    const sigCheck = verifyGRIDxSignature(fwData);
    if (!sigCheck.valid) {
      // Delete the rejected file
      fs.unlinkSync(fwPath);
      console.error(`[OTA] Firmware REJECTED: ${sigCheck.error}`);
      return res.status(400).json({ error: `Firmware rejected: ${sigCheck.error}` });
    }

    const hash = hydroHashHex(fwData, 'metering');
    const info = {
      version: version || sigCheck.version,
      url: 'https://tech.gridx-meters.com/files/firmware.bin',
      size: fwData.length,
      hash,
      mfr: sigCheck.mfr,
      embedded_version: sigCheck.version,
    };
    fs.writeFileSync(path.join(FIRMWARE_DIR, 'fw_latest.json'), JSON.stringify(info, null, 2));
    const backup = `firmware_${(version || sigCheck.version).replace(/\./g, '_')}.bin`;
    fs.copyFileSync(fwPath, path.join(FIRMWARE_DIR, backup));

    // Reset in-memory OTA states (but keep DB records for history)
    for (const drn in otaState) {
      if (otaState[drn].status === 'complete' && otaState[drn].version === version) continue;
      otaState[drn].status = 'idle';
      otaState[drn].progress = 0;
      otaState[drn].detail = '';
    }

    console.log(`[OTA] Uploaded v${version} (${fwData.length} bytes) hash=${hash} MFR=${sigCheck.mfr}`);
    res.json({ success: true, message: `Firmware v${version} uploaded (GRIDx verified)`, firmware: info });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Download current firmware binary ───
app.get('/api/firmware/download', auth, (req, res) => {
  const fwPath = path.join(FIRMWARE_DIR, 'firmware.bin');
  if (!fs.existsSync(fwPath)) return res.status(404).json({ error: 'No firmware file available' });
  try {
    const jsonPath = path.join(FIRMWARE_DIR, 'fw_latest.json');
    let filename = 'firmware.bin';
    if (fs.existsSync(jsonPath)) {
      const info = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
      filename = `GRIDx_firmware_v${info.version}.bin`;
    }
    res.download(fwPath, filename);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Download a specific firmware version ───
app.get('/api/firmware/download/:version', auth, (req, res) => {
  const ver = req.params.version;
  const filename = `firmware_${ver.replace(/\./g, '_')}.bin`;
  const fwPath = path.join(FIRMWARE_DIR, filename);
  if (!fs.existsSync(fwPath)) return res.status(404).json({ error: `Firmware v${ver} not found` });
  res.download(fwPath, `GRIDx_firmware_v${ver}.bin`);
});

// ─── List meters (with firmware version from OTA status + health reports) ───
app.get('/api/meters', auth, async (req, res) => {
  try {
    const db = await getDb();
    const [rows] = await db.query(
      `SELECT m.DRN as drn, CONCAT(m.Name, ' ', m.Surname) as customer,
              m.City as city, m.Region as region, m.SIMNumber as sim,
              o.firmware_version, o.status as ota_db_status, o.progress as ota_db_progress,
              o.detail as ota_db_detail, o.pushed_at, o.updated_at as ota_updated_at,
              o.completed_at
       FROM MeterProfileReal m
       LEFT JOIN MeterOTAStatus o ON m.DRN = o.DRN
       ORDER BY m.DRN`
    );

    const meters = rows.map(m => {
      // Use in-memory state if available (more current), else fall back to DB
      const memState = otaState[m.drn];
      const memFw = meterFirmwareVersions[m.drn];

      let firmware_version = m.firmware_version || null;
      let ota_status = { status: 'idle', progress: 0, detail: '' };
      let completed_at = m.completed_at || null;
      let pushed_at = m.pushed_at || null;

      if (memState) {
        ota_status = {
          status: memState.status,
          progress: memState.progress,
          detail: memState.detail,
        };
        if (memState.version) firmware_version = memState.version;
        if (memState.completedAt) completed_at = memState.completedAt;
        if (memState.startedAt) pushed_at = memState.startedAt;
      } else if (m.ota_db_status) {
        ota_status = {
          status: m.ota_db_status,
          progress: m.ota_db_progress || 0,
          detail: m.ota_db_detail || '',
        };
      }

      // Health report firmware version overrides if more recent
      if (memFw) {
        firmware_version = memFw.version;
      }

      return {
        drn: m.drn,
        customer: m.customer,
        city: m.city,
        region: m.region,
        sim: m.sim,
        firmware_version,
        ota_status,
        completed_at,
        pushed_at,
      };
    });

    res.json({ meters });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

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

    if (!otaState[drn]) {
      otaState[drn] = { status: 'idle', progress: 0, version: '', startedAt: null, updatedAt: null, completedAt: null, detail: '' };
    }
    otaState[drn].version = info.version;
    setOtaStatus(drn, 'updating', 0, `Pushed v${info.version} - waiting for meter response`);

    console.log(`[OTA] Pushed v${info.version} to ${drn}`);
    res.json({ success: true, message: `OTA pushed to ${drn}`, command: cmd });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════
// Nextion TFT API Routes
// ═══════════════════════════════════════════════════════════

app.get('/api/nextion/info', auth, (req, res) => {
  const infoPath = path.join(FIRMWARE_DIR, 'nextion_latest.json');
  if (!fs.existsSync(infoPath)) {
    return res.json({ available: false });
  }
  try {
    const info = JSON.parse(fs.readFileSync(infoPath, 'utf8'));
    const stat = fs.statSync(path.join(FIRMWARE_DIR, 'nextion.tft'));
    res.json({ available: true, ...info, uploaded_at: stat.mtime });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/nextion/upload', auth, tftUpload.single('tft'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file provided' });
  const { version } = req.body;
  if (!version) return res.status(400).json({ error: 'Missing version' });

  try {
    const tftPath = path.join(FIRMWARE_DIR, 'nextion.tft');
    const tftData = fs.readFileSync(tftPath);
    const info = {
      version,
      size: tftData.length,
      chunk_size: 4096,
    };
    fs.writeFileSync(path.join(FIRMWARE_DIR, 'nextion_latest.json'), JSON.stringify(info, null, 2));
    const backup = `nextion_${version.replace(/\./g, '_')}.tft`;
    fs.copyFileSync(tftPath, path.join(FIRMWARE_DIR, backup));

    // Reset nextion OTA states
    for (const drn in nextionOtaState) {
      nextionOtaState[drn].status = 'idle';
      nextionOtaState[drn].progress = 0;
      nextionOtaState[drn].detail = '';
    }

    console.log(`[Nextion] Uploaded v${version} (${tftData.length} bytes)`);
    res.json({ success: true, message: `Nextion TFT v${version} uploaded`, nextion: info });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/nextion/versions', auth, (req, res) => {
  try {
    const files = fs.readdirSync(FIRMWARE_DIR)
      .filter(f => f.startsWith('nextion_') && f.endsWith('.tft'))
      .map(f => {
        const stat = fs.statSync(path.join(FIRMWARE_DIR, f));
        const ver = f.replace('nextion_', '').replace('.tft', '').replace(/_/g, '.');
        return { filename: f, version: ver, size: stat.size, date: stat.mtime };
      })
      .sort((a, b) => new Date(b.date) - new Date(a.date));
    res.json({ versions: files });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/nextion/status', auth, (req, res) => {
  res.json({ nextionOtaState });
});

app.post('/api/nextion/push', auth, (req, res) => {
  const { drn } = req.body;
  if (!drn) return res.status(400).json({ error: 'Missing drn' });

  try {
    const info = getNextionInfo();
    if (!info) return res.status(400).json({ error: 'No Nextion TFT uploaded' });

    const client = getMqtt();
    const cmd = {
      type: 'nextion_ota',
      action: 'start',
      version: info.version,
      size: info.size,
      chunk_size: info.chunk_size || 4096,
    };
    client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });

    setNextionOtaStatus(drn, 'updating', 0, `Pushed Nextion v${info.version} - waiting for meter`);
    console.log(`[Nextion] Pushed v${info.version} to ${drn}`);
    res.json({ success: true, message: `Nextion TFT pushed to ${drn}`, command: cmd });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/nextion/push-all', auth, async (req, res) => {
  try {
    const info = getNextionInfo();
    if (!info) return res.status(400).json({ error: 'No Nextion TFT uploaded' });

    const db = await getDb();
    const [rows] = await db.query('SELECT DRN as drn FROM MeterProfileReal');
    const client = getMqtt();
    const cmd = {
      type: 'nextion_ota',
      action: 'start',
      version: info.version,
      size: info.size,
      chunk_size: info.chunk_size || 4096,
    };
    const cmdStr = JSON.stringify(cmd);

    let pushed = 0;
    const results = [];
    for (const row of rows) {
      client.publish(`gx/${row.drn}/cmd`, cmdStr, { qos: 1 });
      setNextionOtaStatus(row.drn, 'updating', 0, `Pushed Nextion v${info.version} - waiting for meter`);
      results.push(row.drn);
      pushed++;
    }

    console.log(`[Nextion] Pushed v${info.version} to ALL ${pushed} meters`);
    res.json({
      success: true,
      message: `Nextion TFT pushed to ${pushed} meter(s)`,
      meters: results,
      nextion: { version: info.version, size: info.size },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

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
      if (!otaState[row.drn]) {
        otaState[row.drn] = { status: 'idle', progress: 0, version: '', startedAt: null, updatedAt: null, completedAt: null, detail: '' };
      }
      otaState[row.drn].version = info.version;
      setOtaStatus(row.drn, 'updating', 0, `Pushed v${info.version} - waiting for meter response`);
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
// Meter Management Commands
// ═══════════════════════════════════════════════════════════

app.post('/api/cmd/send', auth, (req, res) => {
  const { drn, command } = req.body;
  if (!drn || !command) return res.status(400).json({ error: 'Missing drn or command' });
  const client = getMqtt();
  const topic = `gx/${drn}/cmd`;
  client.publish(topic, JSON.stringify(command), { qos: 1 });
  res.json({ ok: true, topic });
});

app.post('/api/cmd/response/:drn', auth, (req, res) => {
  const { drn } = req.params;
  const filtered = {};
  for (const key of Object.keys(cmdResponses)) {
    if (key.startsWith(`${drn}_`)) filtered[key] = cmdResponses[key];
  }
  res.json({ drn, responses: filtered });
});

app.get('/api/cmd/acks/:drn', auth, (req, res) => {
  const { drn } = req.params;
  const filtered = {};
  for (const key of Object.keys(cmdResponses)) {
    if (key.startsWith(`${drn}_`)) filtered[key] = cmdResponses[key];
  }
  res.json({ drn, acks: filtered });
});

app.post('/api/cmd/relay-control', auth, (req, res) => {
  const { drn, relay, action } = req.body;
  if (!drn || !relay || !action) return res.status(400).json({ error: 'Missing drn, relay, or action' });
  const validRelays = ['mains', 'geyser'];
  const validActions = ['on', 'off', 'enable', 'disable'];
  if (!validRelays.includes(relay)) return res.status(400).json({ error: 'relay must be mains or geyser' });
  if (!validActions.includes(action)) return res.status(400).json({ error: 'action must be on, off, enable, or disable' });

  const map = {
    'mains_on': { ms: 1 }, 'mains_off': { ms: 0 },
    'mains_enable': { mc: 1 }, 'mains_disable': { mc: 0 },
    'geyser_on': { gs: 1 }, 'geyser_off': { gs: 0 },
    'geyser_enable': { gc: 1 }, 'geyser_disable': { gc: 0 },
  };
  const cmd = map[`${relay}_${action}`];
  const client = getMqtt();
  const topic = `gx/${drn}/cmd`;
  client.publish(topic, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic, command: cmd });
});

app.post('/api/cmd/geyser-mode', auth, (req, res) => {
  const { drn, mode } = req.body;
  if (!drn || !mode) return res.status(400).json({ error: 'Missing drn or mode' });
  const validModes = ['manual', 'timer', 'schedule'];
  if (!validModes.includes(mode)) return res.status(400).json({ error: 'mode must be manual, timer, or schedule' });
  const cmd = { type: 'geyser_mode', mode };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/geyser-timer', auth, (req, res) => {
  const { drn, action, hours, minutes } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['start', 'stop'].includes(action)) return res.status(400).json({ error: 'action must be start or stop' });
  const cmd = { type: 'geyser_timer', action };
  if (action === 'start') {
    cmd.hours = hours || 0;
    cmd.minutes = minutes || 0;
  }
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/geyser-schedule', auth, (req, res) => {
  const { drn, schedules } = req.body;
  if (!drn || !schedules) return res.status(400).json({ error: 'Missing drn or schedules' });
  const cmd = { type: 'geyser_schedule_sync', schedules };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/token', auth, (req, res) => {
  const { drn, token } = req.body;
  if (!drn || !token) return res.status(400).json({ error: 'Missing drn or token' });
  if (!/^\d{20}$/.test(token)) return res.status(400).json({ error: 'Token must be 20 digits' });
  const cmd = { type: 'token', token };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/calibrate', auth, (req, res) => {
  const { drn, action, aigain, avgain, save } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  const validActions = ['auto', 'verify', 'exercise', 'voltage', 'current', 'read_gains', 'write_gains', 'reset_defaults'];
  if (!validActions.includes(action)) return res.status(400).json({ error: `action must be one of: ${validActions.join(', ')}` });
  const cmd = { type: 'calibrate', action };
  if (action === 'write_gains') {
    if (aigain === undefined || avgain === undefined) return res.status(400).json({ error: 'write_gains requires aigain and avgain' });
    cmd.aigain = aigain;
    cmd.avgain = avgain;
    cmd.save = !!save;
  }
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/net-metering-mode', auth, (req, res) => {
  const { drn, mode, feed_in_rate } = req.body;
  if (!drn || !mode) return res.status(400).json({ error: 'Missing drn or mode' });
  const validModes = ['gross', 'net_billing', 'feed_in', 'tou'];
  if (!validModes.includes(mode)) return res.status(400).json({ error: 'mode must be gross, net_billing, feed_in, or tou' });
  const cmd = { type: 'net_metering_mode', mode };
  if (feed_in_rate !== undefined) cmd.feed_in_rate = feed_in_rate;
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/billing-mode', auth, (req, res) => {
  const { drn, mode } = req.body;
  if (!drn || !mode) return res.status(400).json({ error: 'Missing drn or mode' });
  if (!['prepaid', 'postpaid'].includes(mode)) return res.status(400).json({ error: 'mode must be prepaid or postpaid' });
  const cmd = { type: 'billing_mode', mode };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/device', auth, (req, res) => {
  const { drn, action } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  const validActions = ['restart', 'sleep', 'wake', 'reset_ble', 'relay_log_request', 'energy_usage_request'];
  if (!validActions.includes(action)) return res.status(400).json({ error: `action must be one of: ${validActions.join(', ')}` });
  const cmd = { type: action };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/auth-numbers', auth, (req, res) => {
  const { drn, action, number } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['add', 'remove', 'list'].includes(action)) return res.status(400).json({ error: 'action must be add, remove, or list' });
  if (action !== 'list' && !number) return res.status(400).json({ error: 'number required for add/remove' });
  const cmd = { type: `auth_number_${action}` };
  if (number) cmd.number = number;
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/commissioning', auth, (req, res) => {
  const { drn, action } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['enter', 'exit', 'status'].includes(action)) return res.status(400).json({ error: 'action must be enter, exit, or status' });
  const cmd = { type: 'commissioning_mode', action };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/energy-test', auth, (req, res) => {
  const { drn, action, duration_s } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['start', 'status'].includes(action)) return res.status(400).json({ error: 'action must be start or status' });
  const cmd = { type: 'energy_test', action };
  if (duration_s !== undefined) cmd.duration_s = duration_s;
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/tou-mode', auth, (req, res) => {
  const { drn, mode } = req.body;
  if (!drn || mode === undefined) return res.status(400).json({ error: 'Missing drn or mode' });
  if (![0, 1, 2].includes(mode)) return res.status(400).json({ error: 'mode must be 0, 1, or 2' });
  const cmd = { type: 'tou_mode', mode };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

// ═══════════════════════════════════════════════════════════
// Net Metering Page Commands
// ═══════════════════════════════════════════════════════════

app.post('/api/cmd/polarity', auth, (req, res) => {
  const { drn, action, mode } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['check', 'fix', 'set_mode'].includes(action)) return res.status(400).json({ error: 'action must be check, fix, or set_mode' });
  const cmd = { type: 'polarity', action };
  if (action === 'set_mode' && mode !== undefined) cmd.mode = mode;
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/energy-reset', auth, (req, res) => {
  const { drn, target } = req.body;
  if (!drn || !target) return res.status(400).json({ error: 'Missing drn or target' });
  if (!['import', 'export', 'all'].includes(target)) return res.status(400).json({ error: 'target must be import, export, or all' });
  const cmd = { type: 'energy_reset', target };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/solar-config', auth, (req, res) => {
  const { drn, enabled } = req.body;
  if (!drn || enabled === undefined) return res.status(400).json({ error: 'Missing drn or enabled' });
  const cmd = { type: 'solar_config', enabled: !!enabled };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

// ═══════════════════════════════════════════════════════════
// Commissioning Page Commands
// ═══════════════════════════════════════════════════════════

app.post('/api/cmd/measurement-test', auth, (req, res) => {
  const { drn, action, ref_voltage, ref_current, ref_power } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['start', 'stop', 'auto_calibration'].includes(action)) return res.status(400).json({ error: 'action must be start, stop, or auto_calibration' });
  const cmd = { type: 'measurement_test', action };
  if (ref_voltage !== undefined) cmd.ref_voltage = ref_voltage;
  if (ref_current !== undefined) cmd.ref_current = ref_current;
  if (ref_power !== undefined) cmd.ref_power = ref_power;
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/load-test', auth, (req, res) => {
  const { drn, action } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['on', 'off', 'range_test'].includes(action)) return res.status(400).json({ error: 'action must be on, off, or range_test' });
  const cmd = { type: 'load_test', action };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/register-meter', auth, (req, res) => {
  const { drn, region, sub_region, area, erf, owner_name, owner_surname, phone, email, gps, street } = req.body;
  if (!drn) return res.status(400).json({ error: 'Missing drn' });
  const cmd = { type: 'register_meter', region, sub_region, area, erf, owner_name, owner_surname, phone, email, gps, street };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/full-system-test', auth, (req, res) => {
  const { drn, action } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['start', 'stop', 'status', 'send_report', 'clear'].includes(action)) return res.status(400).json({ error: 'action must be start, stop, status, send_report, or clear' });
  const cmd = { type: 'full_system_test', action };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

app.post('/api/cmd/net-metering-test', auth, (req, res) => {
  const { drn, action } = req.body;
  if (!drn || !action) return res.status(400).json({ error: 'Missing drn or action' });
  if (!['start', 'fix_polarity'].includes(action)) return res.status(400).json({ error: 'action must be start or fix_polarity' });
  const cmd = { type: 'net_metering_test', action };
  const client = getMqtt();
  client.publish(`gx/${drn}/cmd`, JSON.stringify(cmd), { qos: 1 });
  res.json({ ok: true, topic: `gx/${drn}/cmd`, command: cmd });
});

// ═══════════════════════════════════════════════════════════
// Multer error handler — return JSON errors instead of HTML
// ═══════════════════════════════════════════════════════════
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    return res.status(400).json({ error: `Upload error: ${err.message}` });
  }
  if (err && err.message) {
    return res.status(400).json({ error: err.message });
  }
  next(err);
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
app.listen(PORT, async () => {
  console.log(`[OTA Portal] Running on port ${PORT}`);
  await ensureOTATable();
  getMqtt();
});
