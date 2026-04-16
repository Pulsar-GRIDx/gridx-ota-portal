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
    // Create default admin user
    const hash = bcrypt.hashSync('GridX@OTA2026', 10);
    const users = [{ id: 1, username: 'admin', password: hash, role: 'admin' }];
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
    return users;
  }
  return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
}

// ─── MySQL pool (for reading meter DRNs) ───
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

// ─── MQTT client ───
let mqttClient;
function getMqtt() {
  if (!mqttClient || !mqttClient.connected) {
    mqttClient = mqtt.connect(MQTT_BROKER, {
      clientId: `gridx-ota-portal-${Date.now()}`,
      username: MQTT_USER,
      password: MQTT_PASS,
    });
    mqttClient.on('connect', () => console.log('[MQTT] Connected'));
    mqttClient.on('error', (err) => console.error('[MQTT] Error:', err.message));
  }
  return mqttClient;
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
    console.log(`[OTA] Uploaded v${version} (${fwData.length} bytes) hash=${hash}`);
    res.json({ success: true, message: `Firmware v${version} uploaded`, firmware: info });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── List meters ───
app.get('/api/meters', auth, async (req, res) => {
  try {
    const db = await getDb();
    const [rows] = await db.query(
      `SELECT DRN as drn, CONCAT(Name, ' ', Surname) as customer, City as city,
              Region as region, SIMNumber as sim
       FROM MeterProfileReal ORDER BY DRN`
    );
    res.json({ meters: rows });
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
  getMqtt(); // Connect MQTT eagerly
});
