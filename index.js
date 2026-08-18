const express = require('express');
const { MongoClient } = require('mongodb');
const { default: makeWASocket, DisconnectReason, useMultiFileAuthState, makeCacheableSignalKeyStore, BufferJSON } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const qrcode = require('qrcode');
const P = require('pino');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const MONGO_URL = 'mongodb+srv://propertyncn113_db_user:200939nimuthu@cluster0.zngzztb.mongodb.net/?appName=Cluster0';
const DB_NAME = 'nova-md';
const SESSION_BASE_PATH = path.join(__dirname, 'sessions');

let activeSockets = {};
let sessionTimers = {};
let connectionTimers = {}; // New timer for tracking connection time
let mongoClient = null;
let mongooseConnection = null;
let restartCount = 0;

// Ensure session directory exists
if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

// Mongoose Session Schema
const SessionSchema = new mongoose.Schema({
    number: { type: String, unique: true, required: true },
    lid: { type: String },
    creds: { type: Object, required: true },
    config: { type: Object },
    updatedAt: { type: Date, default: Date.now }
});
SessionSchema.index({ lid: 1 });
const Session = mongoose.model('Session', SessionSchema);

const SessionKeyStoreSchema = new mongoose.Schema({
    number: { type: String, required: true },
    category: { type: String, required: true },
    keyId: { type: String, required: true },
    value: { type: String }
});
SessionKeyStoreSchema.index({ number: 1, category: 1, keyId: 1 }, { unique: true });
const SessionKeyStore = mongoose.model('SessionKeyStore', SessionKeyStoreSchema);

// Connect to MongoDB with Mongoose
async function connectMongoDB() {
    try {
        await mongoose.connect(MONGO_URL, {
            maxPoolSize: 10,
            minPoolSize: 2,
            socketTimeoutMS: 45000,
            connectTimeoutMS: 30000
        });
        mongooseConnection = mongoose.connection;
        console.log('MongoDB connected successfully via Mongoose');
    } catch (error) {
        console.error('MongoDB connection failed:', error);
        throw error;
    }
}

// Get MongoDB client (legacy for compatibility)
async function getMongoClient() {
    if (!mongoClient) {
        mongoClient = new MongoClient(MONGO_URL, {
            maxPoolSize: 10,
            minPoolSize: 2,
            socketTimeoutMS: 60000,
            connectTimeoutMS: 30000,
        });
        await mongoClient.connect();
        console.log('MongoDB client connected');
    }
    return mongoClient;
}

function generateSessionId() {
    return crypto.randomBytes(8).toString('hex');
}

function fixCredsBuffers(obj) {
    if (!obj || typeof obj !== 'object') return obj;
    if (Buffer.isBuffer(obj)) return obj;
    if (obj._bsontype === 'Binary' || obj.buffer) {
        return Buffer.from(obj.buffer || obj);
    }
    if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
        return Buffer.from(obj.data);
    }
    if (obj.type === 'Buffer' && typeof obj.data === 'string') {
        return Buffer.from(obj.data, 'base64');
    }

    for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
            const val = obj[key];
            if (val && typeof val === 'object') {
                if (Buffer.isBuffer(val)) {
                    continue;
                }
                if (val._bsontype === 'Binary' || (val.buffer && (val.buffer instanceof ArrayBuffer || ArrayBuffer.isView(val.buffer)))) {
                    obj[key] = Buffer.from(val.buffer || val);
                } else if (val.type === 'Buffer' && Array.isArray(val.data)) {
                    obj[key] = Buffer.from(val.data);
                } else if (val.type === 'Buffer' && typeof val.data === 'string') {
                    obj[key] = Buffer.from(val.data, 'base64');
                } else if (val.type === 'Buffer' && val.data && typeof val.data === 'object') {
                    obj[key] = Buffer.from(Object.values(val.data));
                } else {
                    fixCredsBuffers(val);
                }
            } else if (typeof val === 'string' && (key === 'private' || key === 'public')) {
                try {
                    if (/^[A-Za-z0-9+/=]+$/.test(val) && val.length > 20) {
                        obj[key] = Buffer.from(val, 'base64');
                    }
                } catch (e) {}
            }
        }
    }
    return obj;
}

// Session save function - Modified to check 10-second rule
async function saveSession(number, creds, lid = null, force = false) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        if (!sanitizedNumber) return;

        // force = true නම් හෝ connection එක තත්පර 10කට වඩා පැරණි නම් save කරන්න
        if (!force) {
            const timerKey = `save_${sanitizedNumber}`;
            if (connectionTimers[timerKey]) {
                const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                if (elapsed < 10000) {
                    console.log(`[Session Manager] Waiting for 10 seconds before saving session for ${sanitizedNumber} (${elapsed}ms elapsed)`);
                    // තවමත් save නොකරන්න
                    return;
                }
            } else {
                console.log(`[Session Manager] Connection timer not started for ${sanitizedNumber}`);
                return;
            }
        }

        const credsString = typeof creds === 'string' ? creds : JSON.stringify(creds, BufferJSON.replacer);
        const credsObj = JSON.parse(credsString);

        const updateFields = { number: sanitizedNumber, creds: credsObj, updatedAt: new Date() };
        if (lid) {
            updateFields.lid = lid.replace(/[^0-9]/g, '');
        }

        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { $set: updateFields },
            { upsert: true, returnDocument: 'after' }
        );
        console.log(`[Session Manager] Saved session for ${sanitizedNumber} to MongoDB successfully.`);

        const sessionPath = path.join(SESSION_BASE_PATH, `Bot_${sanitizedNumber}`);
        if (!fs.existsSync(sessionPath)) {
            fs.mkdirSync(sessionPath, { recursive: true });
        }
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));

    } catch (error) {
        console.error(`[Session Manager] Error saving session for ${number}:`, error.message);
    }
}

// Session restore function
async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const session = await Session.findOne({
            $or: [ { number: sanitizedNumber }, { lid: sanitizedNumber } ]
        });
        if (!session) {
            return null;
        }
        if (!session.creds || !session.creds.me || !session.creds.me.id) {
            console.warn(`[Session Manager] Incomplete credentials in DB for ${sanitizedNumber}, skipping restore`);
            return null;
        }
        let credsJsonStr = typeof session.creds === 'string' ? session.creds : JSON.stringify(session.creds);
        let creds = JSON.parse(credsJsonStr, BufferJSON.reviver);
        creds = fixCredsBuffers(creds);

        const sessionPath = path.join(SESSION_BASE_PATH, `Bot_${sanitizedNumber}`);
        if (!fs.existsSync(sessionPath)) {
            fs.mkdirSync(sessionPath, { recursive: true });
        }
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, BufferJSON.replacer, 2));
        
        return creds;
    } catch (error) {
        console.error(`[Session Manager] Error restoring session:`, error.message);
        return null;
    }
}

// Delete session function
async function deleteSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.deleteOne({
            $or: [ { number: sanitizedNumber }, { lid: sanitizedNumber } ]
        });
        await SessionKeyStore.deleteMany({ number: sanitizedNumber });
        const sessionPath = path.join(SESSION_BASE_PATH, `Bot_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            fs.rmSync(sessionPath, { recursive: true, force: true });
        }
        console.log(`[Session Manager] Deleted session for ${sanitizedNumber}`);
    } catch (error) {
        console.error(`[Session Manager] Error deleting session:`, error.message);
    }
}

async function useMongoDBAuthState(collection, sessionId) {
    const mainSessionId = sessionId;
    
    const writeData = async (data, id) => {
        try {
            const cleanData = JSON.parse(JSON.stringify(data, (key, value) => 
                Buffer.isBuffer(value) ? { type: 'Buffer', data: Array.from(value) } : value
            ));
            
            // Fix registration status
            if (cleanData.creds && cleanData.creds.me && cleanData.creds.me.id && cleanData.creds.registered === false) {
                cleanData.creds.registered = true;
                console.log(`[${mainSessionId}] Fixed registration status`);
            }
            
            // Save to sessions collection (legacy)
            await collection.updateOne(
                { sessionId: mainSessionId },
                { 
                    $set: { 
                        sessionId: mainSessionId, 
                        data: cleanData,
                        updatedAt: new Date()
                    } 
                },
                { upsert: true }
            );
            
            // Also save using Mongoose Session model - only if forced or 10 seconds passed
            if (cleanData.creds && cleanData.creds.me && cleanData.creds.me.id) {
                const phoneNumber = cleanData.creds.me.id.split(':')[0].replace(/[^0-9]/g, '');
                // Check if 10 seconds passed before saving
                const timerKey = `save_${phoneNumber}`;
                if (connectionTimers[timerKey]) {
                    const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                    if (elapsed >= 10000) {
                        await saveSession(phoneNumber, cleanData.creds, null, true);
                    } else {
                        console.log(`[${mainSessionId}] Not saving yet - only ${elapsed}ms elapsed`);
                    }
                }
            }
            
            console.log(`[${mainSessionId}] Data saved to MongoDB`);
        } catch (error) {
            console.error(`[${mainSessionId}] Write Error:`, error.message);
        }
    };

    const readData = async (id) => {
        try {
            const doc = await collection.findOne({ sessionId: mainSessionId });
            
            if (doc && doc.data) {
                const parsed = JSON.parse(JSON.stringify(doc.data), (key, value) => {
                    if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
                        return Buffer.from(value.data);
                    }
                    return value;
                });
                return parsed;
            }
            
            return null;
        } catch (error) {
            console.error(`[${mainSessionId}] Read Error:`, error.message);
            return null;
        }
    };

    const removeData = async (id) => {
        try {
            if (id === `${sessionId}_creds`) {
                await collection.deleteOne({ sessionId: mainSessionId });
                console.log(`[${mainSessionId}] Session removed`);
            }
        } catch (error) {
            console.error(`[${mainSessionId}] Remove Error:`, error.message);
        }
    };

    let sessionData = await readData(`${sessionId}_creds`);
    
    if (!sessionData || !sessionData.creds) {
        const { initAuthCreds } = await import('@whiskeysockets/baileys');
        sessionData = {
            creds: initAuthCreds(),
            keys: {},
            preKeys: {},
            senderKeys: {},
            appStateSyncKeys: {},
            updatedAt: new Date()
        };
        console.log(`[${mainSessionId}] New session initialized`);
    }

    // Fix registration status
    if (sessionData.creds && sessionData.creds.me && sessionData.creds.me.id) {
        sessionData.creds.registered = true;
    }

    return {
        state: {
            creds: sessionData.creds || {},
            keys: {
                get: async (type, ids) => {
                    let data = {};
                    for (let id of ids) {
                        let keyName = id;
                        let value = null;
                        
                        if (type === 'pre-key') {
                            value = sessionData.preKeys?.[keyName];
                        } else if (type === 'sender-key') {
                            value = sessionData.senderKeys?.[keyName];
                        } else if (type === 'app-state-sync-key') {
                            value = sessionData.appStateSyncKeys?.[keyName];
                        } else {
                            value = sessionData.keys?.[keyName];
                        }
                        
                        if (value) data[id] = value;
                    }
                    return data;
                },
                set: async (data) => {
                    try {
                        const cleanData = JSON.parse(JSON.stringify(data, (key, value) => 
                            Buffer.isBuffer(value) ? { type: 'Buffer', data: Array.from(value) } : value
                        ));
                        
                        for (let category of Object.keys(cleanData)) {
                            for (let id of Object.keys(cleanData[category])) {
                                let value = cleanData[category][id];
                                let keyName = id;
                                
                                if (category === 'pre-key') {
                                    if (!sessionData.preKeys) sessionData.preKeys = {};
                                    sessionData.preKeys[keyName] = value;
                                } else if (category === 'sender-key') {
                                    if (!sessionData.senderKeys) sessionData.senderKeys = {};
                                    sessionData.senderKeys[keyName] = value;
                                } else if (category === 'app-state-sync-key') {
                                    if (!sessionData.appStateSyncKeys) sessionData.appStateSyncKeys = {};
                                    sessionData.appStateSyncKeys[keyName] = value;
                                } else {
                                    if (!sessionData.keys) sessionData.keys = {};
                                    sessionData.keys[keyName] = value;
                                }
                            }
                        }
                        
                        sessionData.updatedAt = new Date();
                        
                        await writeData(sessionData, `${sessionId}_creds`);
                    } catch (error) {
                        console.error(`[${mainSessionId}] Set Error:`, error.message);
                    }
                }
            }
        },
        saveCreds: async () => {
            try {
                if (sessionData.creds) {
                    // Fix registration status
                    if (sessionData.creds.me && sessionData.creds.me.id) {
                        sessionData.creds.registered = true;
                    }
                    await writeData(sessionData, `${sessionId}_creds`);
                    console.log(`[${mainSessionId}] Credentials saved`);
                }
            } catch (error) {
                console.error(`[${mainSessionId}] SaveCreds Error:`, error.message);
            }
        }
    };
}

async function sendSuccessMessage(sock, jid) {
    try {
        // Wait 2 seconds just to ensure the socket is fully ready to transmit
        setTimeout(async () => {
            const messageText = "success full conntect plase wait a 30 second and bot conntect use check .menu";
            await sock.sendMessage(jid, { text: messageText });
            console.log(`[Session] Success message sent to ${jid}`);
        }, 2000);
    } catch (err) {
        console.error(`[Session] Failed to send success message:`, err.message);
    }
}

function createWhatsAppSocket(auth, sessionId) {
    return makeWASocket({
        auth: auth,
        printQRInTerminal: false,
        logger: P({ level: 'silent' }),
        browser: ['Mac OS', 'Safari', ''],
        patchMessageBeforeSending: (message) => {
            const requiresPatch = !!(
                message.buttonsMessage ||
                message.templateMessage ||
                message.listMessage ||
                message.productMessage
            );
            if (requiresPatch) {
                message = {
                    viewOnceMessage: {
                        message: {
                            messageContextInfo: {
                                deviceListMetadataVersion: 2,
                                deviceListMetadata: {},
                            },
                            ...message,
                        },
                    },
                };
            }
            return message;
        },
        syncFullHistory: false,
        markOnlineOnConnect: true,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000,
    });
}

async function checkExistingUserSession(usersCollection, phoneNumber) {
    try {
        const user = await usersCollection.findOne({ phoneNumber: phoneNumber });
        if (user && user.sessionId) {
            return {
                exists: true,
                sessionId: user.sessionId,
                user: user
            };
        }
        return { exists: false };
    } catch (error) {
        console.error(`Error checking existing user session:`, error.message);
        return { exists: false };
    }
}

async function disconnectExistingSession(usersCollection, phoneNumber, currentSessionId) {
    try {
        const user = await usersCollection.findOne({ phoneNumber: phoneNumber });
        if (user && user.sessionId && user.sessionId !== currentSessionId) {
            const socketKey = `${phoneNumber}_${user.sessionId}`;
            if (activeSockets[socketKey]) {
                try {
                    await activeSockets[socketKey].end(undefined);
                    console.log(`Closed existing session: ${user.sessionId}`);
                } catch (err) {
                    console.error(`Error closing session ${user.sessionId}:`, err.message);
                }
                delete activeSockets[socketKey];
                if (sessionTimers[socketKey]) {
                    clearTimeout(sessionTimers[socketKey]);
                    delete sessionTimers[socketKey];
                }
                // Cleanup connection timers
                const timerKey = `save_${phoneNumber}`;
                if (connectionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                    delete connectionTimers[timerKey];
                }
            }
            
            const client = await getMongoClient();
            const db = client.db(DB_NAME);
            const sessionsCollection = db.collection('sessions');
            await sessionsCollection.deleteMany({ sessionId: user.sessionId });
            
            await usersCollection.deleteOne({ phoneNumber: phoneNumber });
            
            await deleteSession(user.sessionId);
            
            console.log(`[${phoneNumber}] Old session removed: ${user.sessionId}`);
        }
    } catch (error) {
        console.error(`Error disconnecting existing session:`, error.message);
    }
}

async function saveUserToDatabase(collection, phoneNumber, sessionId) {
    try {
        const existing = await checkExistingUserSession(collection, phoneNumber);
        if (existing.exists) {
            console.log(`[${phoneNumber}] User already has a session, disconnecting old one...`);
            await disconnectExistingSession(collection, phoneNumber, sessionId);
        }

        const userData = {
            phoneNumber: phoneNumber,
            sessionId: sessionId,
            connectedAt: new Date(),
            updatedAt: new Date()
        };

        await collection.updateOne(
            { phoneNumber: phoneNumber },
            { 
                $set: userData
            },
            { upsert: true }
        );
        console.log(`[${phoneNumber}] User data saved with session: ${sessionId}`);
    } catch (error) {
        console.error(`Error saving user to database:`, error.message);
    }
}

async function removeUserFromDatabase(collection, phoneNumber) {
    try {
        await collection.deleteOne({ phoneNumber: phoneNumber });
        console.log(`[${phoneNumber}] User removed from database`);
    } catch (error) {
        console.error(`Error removing user:`, error.message);
    }
}

async function reconnectSocket(sessionId, collection, usersCollection) {
    try {
        console.log(`[${sessionId}] Attempting to reconnect after 515 status...`);
        
        for (const key of Object.keys(activeSockets)) {
            if (key.includes(sessionId)) {
                try {
                    await activeSockets[key].end();
                } catch (e) {}
                delete activeSockets[key];
                if (sessionTimers[key]) {
                    clearTimeout(sessionTimers[key]);
                    delete sessionTimers[key];
                }
                // Cleanup connection timers
                const phoneNumber = key.split('_')[0];
                const timerKey = `save_${phoneNumber}`;
                if (connectionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                    delete connectionTimers[timerKey];
                }
            }
        }

        const { state, saveCreds } = await useMongoDBAuthState(collection, sessionId);
        const sock = createWhatsAppSocket(state, sessionId);
        
        sock.ev.on('creds.update', saveCreds);
        
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            
            if (connection === 'open') {
                const connectedNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                console.log(`[${connectedNumber}] Reconnected successfully! Session: ${sessionId}`);
                
                const existing = await checkExistingUserSession(usersCollection, connectedNumber);
                if (existing.exists) {
                    console.log(`[${connectedNumber}] Found existing session, disconnecting...`);
                    await disconnectExistingSession(usersCollection, connectedNumber, sessionId);
                }
                
                // Start connection timer
                const timerKey = `save_${connectedNumber}`;
                connectionTimers[timerKey] = {
                    startTime: Date.now(),
                    socket: sock,
                    phoneNumber: connectedNumber,
                    sessionId: sessionId
                };
                
                // Setup 10-second timer
                if (sessionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                }
                
                sessionTimers[timerKey] = setTimeout(async () => {
                    console.log(`[${connectedNumber}] 10 seconds passed, saving session and closing connection...`);
                    
                    try {
                        await saveSession(connectedNumber, state.creds, null, true);
                        await saveUserToDatabase(usersCollection, connectedNumber, sessionId);
                        
                        const socketKey = `${connectedNumber}_${sessionId}`;
                        if (activeSockets[socketKey]) {
                            try {
                                await activeSockets[socketKey].end(undefined);
                                console.log(`[${connectedNumber}] Connection closed after 10 seconds`);
                            } catch (err) {
                                console.error(`[${connectedNumber}] Error closing connection:`, err.message);
                            }
                            delete activeSockets[socketKey];
                        }
                        
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                        
                    } catch (error) {
                        console.error(`[${connectedNumber}] Error in 10-second timer:`, error.message);
                    }
                }, 10000);
                
                await sendSuccessMessage(sock, `${connectedNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {
                    // Silent fail
                }
                
                console.log(`[${connectedNumber}] Session will be saved and closed after 10 seconds`);
            }
            
            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${phoneNumber}_${sessionId}`;
                const timerKey = `save_${phoneNumber}`;
                
                // If disconnected before 10 seconds, don't save session
                if (connectionTimers[timerKey]) {
                    const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                    if (elapsed < 10000) {
                        console.log(`[${phoneNumber}] Disconnected before 10 seconds (${elapsed}ms), session will NOT be saved`);
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                    }
                }
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await collection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersCollection, phoneNumber);
                    await deleteSession(phoneNumber);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 5 seconds...`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                    setTimeout(() => {
                        reconnectSocket(sessionId, collection, usersCollection);
                    }, 5000);
                } else {
                    console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error during reconnect:`, error.message);
            if (error.message && error.message.includes('restart required')) {
                setTimeout(() => {
                    reconnectSocket(sessionId, collection, usersCollection);
                }, 5000);
            }
        });

        activeSockets[sessionId] = sock;
        console.log(`[${sessionId}] Reconnection attempt completed`);

    } catch (error) {
        console.error(`[${sessionId}] Reconnection failed:`, error.message);
        setTimeout(() => {
            reconnectSocket(sessionId, collection, usersCollection);
        }, 10000);
    }
}

app.get('/qr', async (req, res) => {
    const sessionId = generateSessionId();
    console.log(`[${sessionId}] New QR request received`);

    try {
        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const collection = db.collection('sessions');
        const usersCollection = db.collection('users');

        const { state, saveCreds } = await useMongoDBAuthState(collection, sessionId);
        const sock = createWhatsAppSocket(state, sessionId);

        sock.ev.on('creds.update', saveCreds);

        let qrSent = false;
        let qrTimeout = setTimeout(() => {
            if (!res.headersSent && !qrSent) {
                console.log(`[${sessionId}] QR generation timeout`);
                res.status(408).json({ error: 'QR generation timeout', sessionId });
            }
        }, 30000);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr && !qrSent) {
                qrSent = true;
                clearTimeout(qrTimeout);
                console.log(`[${sessionId}] QR code generated`);
                try {
                    const qrBase64 = await qrcode.toDataURL(qr);
                    if (!res.headersSent) {
                        res.json({ 
                            status: 'success', 
                            sessionId, 
                            qr_base64: qrBase64
                        });
                    }
                } catch (err) {
                    console.error(`[${sessionId}] Failed to generate QR:`, err.message);
                    if (!res.headersSent) res.status(500).json({ error: 'Failed to generate QR' });
                }
            }

            if (connection === 'open') {
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                console.log(`[${phoneNumber}] Connected successfully! Session: ${sessionId}`);
                
                // Start connection timer
                const timerKey = `save_${phoneNumber}`;
                connectionTimers[timerKey] = {
                    startTime: Date.now(),
                    socket: sock,
                    phoneNumber: phoneNumber,
                    sessionId: sessionId
                };
                
                // Setup 10-second timer
                if (sessionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                }
                
                sessionTimers[timerKey] = setTimeout(async () => {
                    console.log(`[${phoneNumber}] 10 seconds passed, saving session and closing connection...`);
                    
                    try {
                        await saveSession(phoneNumber, state.creds, null, true);
                        await saveUserToDatabase(usersCollection, phoneNumber, sessionId);
                        
                        const socketKey = `${phoneNumber}_${sessionId}`;
                        if (activeSockets[socketKey]) {
                            try {
                                await activeSockets[socketKey].end(undefined);
                                console.log(`[${phoneNumber}] Connection closed after 10 seconds`);
                            } catch (err) {
                                console.error(`[${phoneNumber}] Error closing connection:`, err.message);
                            }
                            delete activeSockets[socketKey];
                        }
                        
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                        
                    } catch (error) {
                        console.error(`[${phoneNumber}] Error in 10-second timer:`, error.message);
                    }
                }, 10000);
                
                await sendSuccessMessage(sock, `${phoneNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {
                    // Silent fail
                }
                
                console.log(`[${phoneNumber}] Session will be saved and closed after 10 seconds`);
            }

            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${phoneNumber}_${sessionId}`;
                const timerKey = `save_${phoneNumber}`;
                
                // If disconnected before 10 seconds, don't save session
                if (connectionTimers[timerKey]) {
                    const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                    if (elapsed < 10000) {
                        console.log(`[${phoneNumber}] Disconnected before 10 seconds (${elapsed}ms), session will NOT be saved`);
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                    }
                }
                
                console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await collection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersCollection, phoneNumber);
                    await deleteSession(phoneNumber);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 3 seconds...`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                    setTimeout(() => {
                        reconnectSocket(sessionId, collection, usersCollection);
                    }, 3000);
                } else {
                    console.log(`[${sessionId}] Connection closed normally`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error:`, error.message);
        });

        activeSockets[sessionId] = sock;
        console.log(`[${sessionId}] Socket initialized successfully`);

    } catch (error) {
        console.error(`[${sessionId}] Error in /qr:`, error.message);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
});

app.get('/pair', async (req, res) => {
    const phoneNumber = req.query.number;
    if (!phoneNumber) {
        return res.status(400).json({ error: 'Phone number is required! Example: /pair?number=94718860945' });
    }

    const sessionId = generateSessionId();
    console.log(`[${sessionId}] New pairing request for number: ${phoneNumber}`);

    try {
        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const collection = db.collection('sessions');
        const usersCollection = db.collection('users');

        const existing = await checkExistingUserSession(usersCollection, phoneNumber);
        if (existing.exists) {
            console.log(`[${phoneNumber}] User already has a session, disconnecting old one...`);
            await disconnectExistingSession(usersCollection, phoneNumber, null);
        }

        const { state, saveCreds } = await useMongoDBAuthState(collection, sessionId);
        const sock = createWhatsAppSocket(state, sessionId);

        sock.ev.on('creds.update', saveCreds);

        let pairingSent = false;
        let pairingTimeout = setTimeout(() => {
            if (!res.headersSent && !pairingSent) {
                console.log(`[${sessionId}] Pairing code timeout`);
                res.status(408).json({ error: 'Pairing code timeout', sessionId });
            }
        }, 30000);

        setTimeout(async () => {
            if (!sock.authState.creds.registered && !pairingSent) {
                pairingSent = true;
                clearTimeout(pairingTimeout);
                try {
                    const cleanNumber = phoneNumber.replace(/[^0-9]/g, '');
                    const code = await sock.requestPairingCode(cleanNumber);
                    console.log(`[${sessionId}] Pairing code generated for ${cleanNumber}`);
                    
                    if (!res.headersSent) {
                        res.json({ 
                            status: 'success', 
                            sessionId, 
                            phone_number: cleanNumber, 
                            pairing_code: code
                        });
                    }
                } catch (err) {
                    console.error(`[${sessionId}] Failed to generate pairing code:`, err.message);
                    if (!res.headersSent) {
                        res.status(500).json({ error: 'Failed to generate pairing code', details: err.message });
                    }
                }
            }
        }, 3000);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            
            if (connection === 'open') {
                const connectedNumber = sock.user?.id ? sock.user.id.split(':')[0] : sessionId;
                console.log(`[${connectedNumber}] Connected via Pairing Code! Session: ${sessionId}`);
                
                // Start connection timer
                const timerKey = `save_${connectedNumber}`;
                connectionTimers[timerKey] = {
                    startTime: Date.now(),
                    socket: sock,
                    phoneNumber: connectedNumber,
                    sessionId: sessionId
                };
                
                // Setup 10-second timer
                if (sessionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                }
                
                sessionTimers[timerKey] = setTimeout(async () => {
                    console.log(`[${connectedNumber}] 10 seconds passed, saving session and closing connection...`);
                    
                    try {
                        await saveSession(connectedNumber, state.creds, null, true);
                        await saveUserToDatabase(usersCollection, connectedNumber, sessionId);
                        
                        const socketKey = `${connectedNumber}_${sessionId}`;
                        if (activeSockets[socketKey]) {
                            try {
                                await activeSockets[socketKey].end(undefined);
                                console.log(`[${connectedNumber}] Connection closed after 10 seconds`);
                            } catch (err) {
                                console.error(`[${connectedNumber}] Error closing connection:`, err.message);
                            }
                            delete activeSockets[socketKey];
                        }
                        
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                        
                    } catch (error) {
                        console.error(`[${connectedNumber}] Error in 10-second timer:`, error.message);
                    }
                }, 10000);
                
                await sendSuccessMessage(sock, `${connectedNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {
                    // Silent fail
                }
                
                console.log(`[${connectedNumber}] Session will be saved and closed after 10 seconds`);
            }

            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${phoneNumber}_${sessionId}`;
                const timerKey = `save_${phoneNumber}`;
                
                // If disconnected before 10 seconds, don't save session
                if (connectionTimers[timerKey]) {
                    const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                    if (elapsed < 10000) {
                        console.log(`[${phoneNumber}] Disconnected before 10 seconds (${elapsed}ms), session will NOT be saved`);
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                        delete connectionTimers[timerKey];
                    }
                }
                
                console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await collection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersCollection, phoneNumber);
                    await deleteSession(phoneNumber);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 3 seconds...`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                    setTimeout(() => {
                        reconnectSocket(sessionId, collection, usersCollection);
                    }, 3000);
                } else {
                    console.log(`[${sessionId}] Connection closed normally`);
                    delete activeSockets[socketKey];
                    if (sessionTimers[timerKey]) {
                        clearTimeout(sessionTimers[timerKey]);
                        delete sessionTimers[timerKey];
                    }
                    if (connectionTimers[timerKey]) {
                        delete connectionTimers[timerKey];
                    }
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error:`, error.message);
        });

        activeSockets[sessionId] = sock;
        console.log(`[${sessionId}] Socket initialized successfully`);

    } catch (error) {
        console.error(`[${sessionId}] Error in /pair:`, error.message);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
});

app.get('/check-user/:number', async (req, res) => {
    try {
        const phoneNumber = req.params.number;
        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const usersCollection = db.collection('users');
        
        const existing = await checkExistingUserSession(usersCollection, phoneNumber);
        
        if (existing.exists) {
            res.json({
                status: 'success',
                phoneNumber: phoneNumber,
                hasSession: true,
                sessionId: existing.sessionId
            });
        } else {
            res.json({
                status: 'success',
                phoneNumber: phoneNumber,
                hasSession: false,
                message: 'No session found for this user'
            });
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

async function cleanupAndRestart() {
    restartCount++;
    console.log(`[Restart #${restartCount}] Starting server cleanup...`);
    
    const socketKeys = Object.keys(activeSockets);
    console.log(`[Restart #${restartCount}] Closing ${socketKeys.length} active sessions...`);
    
    for (const key of socketKeys) {
        try {
            const sock = activeSockets[key];
            if (sock?.end) {
                await sock.end(undefined);
            }
            delete activeSockets[key];
            if (sessionTimers[key]) {
                clearTimeout(sessionTimers[key]);
                delete sessionTimers[key];
            }
            // Cleanup connection timers
            const phoneNumber = key.split('_')[0];
            const timerKey = `save_${phoneNumber}`;
            if (connectionTimers[timerKey]) {
                clearTimeout(sessionTimers[timerKey]);
                delete connectionTimers[timerKey];
            }
        } catch (err) {
            console.error(`Error closing session ${key}:`, err.message);
        }
    }

    if (mongoClient) {
        try {
            await mongoClient.close();
            mongoClient = null;
            console.log(`[Restart #${restartCount}] MongoDB client closed`);
        } catch (err) {
            console.error('Error closing MongoDB client:', err.message);
        }
    }

    if (mongooseConnection) {
        try {
            await mongooseConnection.close();
            mongooseConnection = null;
            console.log(`[Restart #${restartCount}] Mongoose connection closed`);
        } catch (err) {
            console.error('Error closing Mongoose connection:', err.message);
        }
    }

    activeSockets = {};
    sessionTimers = {};
    connectionTimers = {};

    console.log(`[Restart #${restartCount}] Cleanup completed. Restarting server in 2 seconds...`);
    
    setTimeout(() => {
        console.log(`[Restart #${restartCount}] Server restarting...`);
        process.exit(0);
    }, 2000);
}

// Initialize MongoDB connection
console.log('Initializing MongoDB connection...');
connectMongoDB().then(() => {
    console.log('MongoDB initialized successfully');
}).catch(err => {
    console.error('Failed to initialize MongoDB:', err);
    process.exit(1);
});

console.log('Server starting with auto-restart capability...');
console.log('Auto-restart will trigger on:');
console.log('  - Uncaught exceptions');
console.log('  - Unhandled rejections');
console.log('  - Server errors');
console.log('  - Signal termination (SIGTERM/SIGINT)');
console.log('  - High memory usage (>500MB)');
console.log('  - WhatsApp status 515 (restart required)');

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM signal. Cleaning up...');
    await cleanupAndRestart();
});

process.on('SIGINT', async () => {
    console.log('Received SIGINT signal. Cleaning up...');
    await cleanupAndRestart();
});

process.on('uncaughtException', async (err) => {
    console.error(`Uncaught Exception:`, err.message);
    console.log(`Auto-restarting due to uncaught exception...`);
    await cleanupAndRestart();
});

process.on('unhandledRejection', async (err) => {
    console.error(`Unhandled Rejection:`, err.message);
    console.log(`Auto-restarting due to unhandled rejection...`);
    await cleanupAndRestart();
});

const server = app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`Available endpoints:`);
    console.log(`  - GET  /qr`);
    console.log(`  - GET  /pair?number=947xxxxxxxx`);
    console.log(`  - GET  /check-user/:number`);
    console.log(`Auto-restart enabled on errors`);
    console.log(`Session ID auto-generated for each request`);
    console.log(`WhatsApp status 515 will trigger auto-reconnect`);
});

server.on('error', async (err) => {
    console.error(`Server error:`, err.message);
    console.log(`Auto-restarting due to server error...`);
    await cleanupAndRestart();
});

setInterval(() => {
    const used = process.memoryUsage();
    const heapUsed = used.heapUsed / 1024 / 1024;
    const heapTotal = used.heapTotal / 1024 / 1024;
    
    if (heapUsed > 500) {
        console.log(`High memory usage detected: ${heapUsed.toFixed(2)}MB / ${heapTotal.toFixed(2)}MB`);
        console.log(`Auto-restarting due to high memory usage...`);
        cleanupAndRestart();
    }
}, 60000);
