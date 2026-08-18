const express = require('express');
const { MongoClient } = require('mongodb');
const { default: makeWASocket, DisconnectReason, useMultiFileAuthState, makeCacheableSignalKeyStore, BufferJSON } = require('@crysnovax/baileys');
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
const MONGO_URL = "mongodb+srv://propertyncn113_db_user:200939nimuthu@cluster0.zngzztb.mongodb.net/nova-md?retryWrites=true&w=majority&appName=Cluster0";
if (!MONGO_URL) {
    console.error('❌ MONGO_URL environment variable is required!');
    process.exit(1);
}
const DB_NAME = 'nova-md';
const SESSION_BASE_PATH = path.join(__dirname, 'sessions');

let activeSockets = {};
let sessionTimers = {};
let connectionTimers = {};
let mongoClient = null;
let mongooseConnection = null;
let restartCount = 0;
const sessionRecoveryAttempts = {};
const BAD_MAC_RECOVERY_DELAY_MS = 5000;

// Ensure session directory exists
if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

// Mongoose Session Schema
const SessionSchema = new mongoose.Schema({
    number: { type: String, unique: true, sparse: true, required: false },
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

// ============================================================
// BAD MAC HANDLING FUNCTIONS
// ============================================================
async function handleBadMACError(sock, sessionId, phoneNumber) {
    const sanitizedNumber = phoneNumber.replace(/[^0-9]/g, '');
    
    sessionRecoveryAttempts[sanitizedNumber] = (sessionRecoveryAttempts[sanitizedNumber] || 0) + 1;
    
    console.log(`[BadMAC] Attempt ${sessionRecoveryAttempts[sanitizedNumber]} for ${sanitizedNumber}`);
    
    if (sessionRecoveryAttempts[sanitizedNumber] >= 3) {
        console.log(`[BadMAC] Too many failures for ${sanitizedNumber}, forcing session reset`);
        await forceSessionReset(phoneNumber);
        return;
    }
    
    await cleanupCorruptedSessionKeys(sanitizedNumber);
    
    setTimeout(() => {
        console.log(`[BadMAC] Reconnecting ${sanitizedNumber} after Bad MAC error`);
        const sessionId = generateSessionId();
        reconnectSocket(sessionId, null, null, sanitizedNumber);
    }, BAD_MAC_RECOVERY_DELAY_MS);
}

async function forceSessionReset(phoneNumber) {
    const sanitizedNumber = phoneNumber.replace(/[^0-9]/g, '');
    console.log(`[Reset] Force resetting session for ${sanitizedNumber}`);
    
    const socketKey = Object.keys(activeSockets).find(key => key.includes(sanitizedNumber));
    if (socketKey && activeSockets[socketKey]) {
        try {
            await activeSockets[socketKey].end();
        } catch (e) {}
        delete activeSockets[socketKey];
    }
    
    await deleteSession(sanitizedNumber);
    delete sessionRecoveryAttempts[sanitizedNumber];
    console.log(`[Reset] Session ${sanitizedNumber} has been reset. User needs to re-scan QR code.`);
}

async function cleanupCorruptedSessionKeys(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    try {
        const result = await SessionKeyStore.deleteMany({
            number: sanitizedNumber,
            $or: [
                { keyId: { $regex: /^pre-key-/i } },
                { category: 'session' }
            ]
        });
        
        if (result.deletedCount > 0) {
            console.log(`[Cleanup] Removed ${result.deletedCount} corrupted keys for ${sanitizedNumber}`);
        }
        return result.deletedCount;
    } catch (error) {
        console.error(`[Cleanup] Error cleaning keys for ${sanitizedNumber}:`, error.message);
        return 0;
    }
}

// ============================================================
// SESSION INTEGRITY VALIDATION
// ============================================================
async function validateSessionIntegrity(sessionData) {
    try {
        if (!sessionData || !sessionData.creds) {
            console.log('[Validate] No session data or creds');
            return false;
        }
        const creds = sessionData.creds;
        if (!creds.me || !creds.me.id) {
            console.log('[Validate] Missing me.id in credentials');
            return false;
        }
        if (creds.registered !== true) {
            console.log('[Validate] Session not registered');
            return false;
        }
        if (!creds.signedPreKey || !creds.signedPreKey.public) {
            console.log('[Validate] Missing signedPreKey');
            return false;
        }
        return true;
    } catch (error) {
        console.error('[Validate] Error validating session:', error.message);
        return false;
    }
}

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
        console.log('✅ MongoDB connected successfully via Mongoose');
        
        try {
            await mongooseConnection.db.collection('sessions').dropIndex('number_1');
            console.log('Dropped existing number_1 index');
        } catch (err) {
            console.log('Index drop skipped or not found:', err.message);
        }
        
        try {
            await mongooseConnection.db.collection('sessions').createIndex(
                { number: 1 }, 
                { sparse: true, unique: true }
            );
            console.log('Created sparse unique index on number field');
        } catch (err) {
            console.log('Index creation skipped:', err.message);
        }
        
    } catch (error) {
        console.error('❌ MongoDB connection failed:', error);
        throw error;
    }
}

async function getMongoClient() {
    if (!mongoClient) {
        mongoClient = new MongoClient(MONGO_URL, {
            maxPoolSize: 10,
            minPoolSize: 2,
            socketTimeoutMS: 60000,
            connectTimeoutMS: 30000,
        });
        await mongoClient.connect();
        console.log('✅ MongoDB client connected');
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

// ============================================================
// ⭐ NEW: Immediate Session Save Function
// ============================================================
async function saveSessionImmediately(phoneNumber, creds, sessionId, usersCollection) {
    try {
        const sanitizedNumber = phoneNumber.replace(/[^0-9]/g, '');
        
        // Save credentials to session collection
        await saveSession(sanitizedNumber, creds, null, true);
        
        // Save user to users collection
        await saveUserToDatabase(usersCollection, sanitizedNumber, sessionId);
        
        console.log(`[${sanitizedNumber}] ✅ Session saved immediately to database`);
        return true;
    } catch (error) {
        console.error(`[${sanitizedNumber}] ❌ Failed to save session immediately:`, error.message);
        return false;
    }
}

// ============================================================
// SESSION SAVE FUNCTION WITH DELAY
// ============================================================
async function saveSession(number, creds, lid = null, force = false) {
    try {
        const sanitizedNumber = number ? number.replace(/[^0-9]/g, '') : null;
        
        if (!sanitizedNumber && !lid) {
            console.log(`[Session Manager] No number or lid provided, skipping save`);
            return;
        }

        // Add small delay for stability
        await new Promise(resolve => setTimeout(resolve, 2000));

        if (!force && sanitizedNumber) {
            const timerKey = `save_${sanitizedNumber}`;
            if (connectionTimers[timerKey]) {
                const elapsed = Date.now() - connectionTimers[timerKey].startTime;
                if (elapsed < 10000) {
                    console.log(`[Session Manager] Waiting for 10 seconds before saving session for ${sanitizedNumber} (${elapsed}ms elapsed)`);
                    return;
                }
            } else {
                console.log(`[Session Manager] Connection timer not started for ${sanitizedNumber}`);
                return;
            }
        }

        // Clean credentials before saving
        const cleanCreds = JSON.parse(JSON.stringify(creds, (key, value) => {
            if (Buffer.isBuffer(value)) {
                return { type: 'Buffer', data: value.toString('base64') };
            }
            return value;
        }));

        const updateFields = { creds: cleanCreds, updatedAt: new Date() };
        
        if (sanitizedNumber) {
            updateFields.number = sanitizedNumber;
        }
        if (lid) {
            updateFields.lid = lid.replace(/[^0-9]/g, '');
        }

        const query = {};
        if (sanitizedNumber) {
            query.number = sanitizedNumber;
        } else if (lid) {
            query.lid = lid.replace(/[^0-9]/g, '');
        } else {
            const tempId = creds?.me?.id?.split(':')[0] || Date.now().toString();
            query.number = tempId;
            updateFields.number = tempId;
        }

        await Session.findOneAndUpdate(
            query,
            { $set: updateFields },
            { upsert: true, returnDocument: 'after' }
        );
        console.log(`[Session Manager] ✅ Saved session for ${sanitizedNumber || 'unknown'} to MongoDB successfully.`);

        if (sanitizedNumber) {
            const sessionPath = path.join(SESSION_BASE_PATH, `Bot_${sanitizedNumber}`);
            if (!fs.existsSync(sessionPath)) {
                fs.mkdirSync(sessionPath, { recursive: true });
            }
            fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));
        }

    } catch (error) {
        console.error(`[Session Manager] Error saving session for ${number || 'unknown'}:`, error.message);
    }
}

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

// ============================================================
// useMongoDBAuthState - Stable key by phone number
// ============================================================
async function useMongoDBAuthState(collection, sessionId, phoneNumber = null) {
    const stateKey = phoneNumber
        ? `creds_${phoneNumber.replace(/[^0-9]/g, '')}`
        : `${sessionId}_creds`;
    
    console.log(`[${stateKey}] Using auth state with key: ${stateKey}`);

    const writeData = async (data) => {
        try {
            const cleanData = JSON.parse(JSON.stringify(data, (key, value) =>
                Buffer.isBuffer(value) ? { type: 'Buffer', data: Array.from(value) } : value
            ));

            if (cleanData.creds?.me?.id && cleanData.creds.registered === false) {
                cleanData.creds.registered = true;
            }

            await collection.updateOne(
                { sessionId: stateKey },
                { $set: { sessionId: stateKey, data: cleanData, updatedAt: new Date() } },
                { upsert: true }
            );

            if (cleanData.creds?.me?.id) {
                const num = cleanData.creds.me.id.split(':')[0].replace(/[^0-9]/g, '');
                await saveSession(num, cleanData.creds, null, true);
            }
        } catch (error) {
            console.error(`[${stateKey}] Write Error:`, error.message);
        }
    };

    const readData = async () => {
        try {
            const doc = await collection.findOne({ sessionId: stateKey });
            if (doc?.data) {
                return JSON.parse(JSON.stringify(doc.data), (key, value) =>
                    value?.type === 'Buffer' && Array.isArray(value.data)
                        ? Buffer.from(value.data)
                        : value
                );
            }
            return null;
        } catch (error) {
            console.error(`[${stateKey}] Read Error:`, error.message);
            return null;
        }
    };

    let sessionData = await readData();

    if ((!sessionData || !sessionData.creds) && phoneNumber) {
        const restoredCreds = await restoreSession(phoneNumber);
        if (restoredCreds) {
            sessionData = { 
                creds: restoredCreds, 
                keys: {}, 
                preKeys: {}, 
                senderKeys: {}, 
                appStateSyncKeys: {} 
            };
            console.log(`[${stateKey}] ✅ Resumed existing identity for ${phoneNumber}`);
        }
    }

    if (!sessionData || !sessionData.creds) {
        const { initAuthCreds } = await import('@whiskeysockets/baileys');
        sessionData = { 
            creds: initAuthCreds(), 
            keys: {}, 
            preKeys: {}, 
            senderKeys: {}, 
            appStateSyncKeys: {} 
        };
        console.log(`[${stateKey}] 🆕 New identity created`);
    }

    if (sessionData.creds?.me?.id) {
        sessionData.creds.registered = true;
    }

    return {
        state: {
            creds: sessionData.creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    for (const id of ids) {
                        const store = type === 'pre-key' ? sessionData.preKeys
                            : type === 'sender-key' ? sessionData.senderKeys
                            : type === 'app-state-sync-key' ? sessionData.appStateSyncKeys
                            : sessionData.keys;
                        if (store?.[id]) data[id] = store[id];
                    }
                    return data;
                },
                set: async (data) => {
                    for (const category of Object.keys(data)) {
                        const store = category === 'pre-key' ? (sessionData.preKeys ??= {})
                            : category === 'sender-key' ? (sessionData.senderKeys ??= {})
                            : category === 'app-state-sync-key' ? (sessionData.appStateSyncKeys ??= {})
                            : (sessionData.keys ??= {});
                        for (const id of Object.keys(data[category])) {
                            store[id] = data[category][id];
                        }
                    }
                    sessionData.updatedAt = new Date();
                    await writeData(sessionData);
                }
            }
        },
        saveCreds: async () => {
            if (sessionData.creds?.me?.id) sessionData.creds.registered = true;
            await writeData(sessionData);
        }
    };
}

async function sendSuccessMessage(sock, jid) {
    try {
        setTimeout(async () => {
            const imageUrl = "https://cdn.phototourl.com/free/2026-08-18-a18634f6-91e2-454e-877b-92ca61570125.png";
            const messageText = 
                "✅ *WhatsApp Bot Connected Successfully!*\n\n" +
                "⏳ Please wait about 60 seconds, then type `.menu` to check if the bot is working.\n" +
                "⚠️ If the bot does not connect within 5 minutes, please unlink/log out the device from WhatsApp Linked Devices and try again.\n\n" +
                "----------------------------------------\n\n" +
                "✅ *වට්ස්ඇප් බොට් සාර්ථකව සම්බන්ධ විය!*\n\n" +
                "⏳ කරුණාකර තත්පර 60ක් පමණ රැදී සිට, බොට් වැඩ කරන්නේ දැයි බැලීමට `.menu` ලෙස යවන්න.\n" +
                "⚠️ විනාඩි 5ක් ඇතුළත බොට් සම්බන්ධ නොවූ වුවහොත්, කරුණාකර ඔබේ දුරකථනයේ WhatsApp Linked Devices වෙත ගොස් ඩිවයිස් එක ලොග් අවුට් කර (Unlink කර) නැවත උත්සාහ කරන්න.\n\n" +
                "----------------------------------------\n\n" +
                "✅ *வாட்ஸ்அப் போட் வெற்றிகரமாக இணைக்கப்பட்டது!*\n\n" +
                "⏳ தயவுசெய்து 60 விநாடிகள் காத்திருந்து, போட் வேலை செய்கிறதா என்பதைப் பார்க்க `.menu` என தட்டச்சு செய்யவும்.\n" +
                "⚠️ 5 நிமிடங்களுக்குள் போட் இணைக்கப்படவில்லை என்றால், உங்கள் சாதனத்தில் உள்ள WhatsApp Linked Devices-ல் சென்று சாதனத்தை லாக் அவுட் (Log out) செய்துவிட்டு மீண்டும் முயற்சிக்கவும்.";

            await sock.sendMessage(jid, { 
                image: { url: imageUrl }, 
                caption: messageText 
            });
            console.log(`[Session] Success image and message sent to ${jid}`);
        }, 2000);
    } catch (err) {
        console.error(`[Session] Failed to send success message:`, err.message);
    }
}

function createWhatsAppSocket(auth, sessionId) {
    const sock = makeWASocket({
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

    sock.ev.on('error', (error) => {
        if (error.message && error.message.includes('Bad MAC')) {
            console.error(`[${sessionId}] ⚠️ Bad MAC error detected`);
            const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
            handleBadMACError(sock, sessionId, phoneNumber);
        } else {
            console.error(`[${sessionId}] Socket error:`, error.message);
        }
    });

    return sock;
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

async function reconnectSocket(sessionId, collection, usersCollection, phoneNumber = null) {
    try {
        console.log(`[${sessionId}] Attempting to reconnect...`);
        
        for (const key of Object.keys(activeSockets)) {
            if (phoneNumber && key.includes(phoneNumber)) {
                try {
                    await activeSockets[key].end();
                } catch (e) {}
                delete activeSockets[key];
                if (sessionTimers[key]) {
                    clearTimeout(sessionTimers[key]);
                    delete sessionTimers[key];
                }
                const timerKey = `save_${phoneNumber}`;
                if (connectionTimers[timerKey]) {
                    clearTimeout(sessionTimers[timerKey]);
                    delete connectionTimers[timerKey];
                }
            } else if (!phoneNumber && key.includes(sessionId)) {
                try {
                    await activeSockets[key].end();
                } catch (e) {}
                delete activeSockets[key];
                if (sessionTimers[key]) {
                    clearTimeout(sessionTimers[key]);
                    delete sessionTimers[key];
                }
            }
        }

        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const sessionsCollection = collection || db.collection('sessions');
        const usersColl = usersCollection || db.collection('users');

        const { state, saveCreds } = await useMongoDBAuthState(sessionsCollection, sessionId, phoneNumber);
        const sock = createWhatsAppSocket(state, sessionId);
        
        sock.ev.on('creds.update', saveCreds);
        
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            
            if (connection === 'open') {
                const connectedNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                console.log(`[${connectedNumber}] ✅ Reconnected successfully! Session: ${sessionId}`);
                
                // ⭐ IMMEDIATELY SAVE SESSION ON RECONNECT
                await saveSessionImmediately(connectedNumber, state.creds, sessionId, usersColl);
                
                await sendSuccessMessage(sock, `${connectedNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {}
                
                console.log(`[${connectedNumber}] Session saved and connection kept alive`);
            }
            
            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const connectedNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${connectedNumber}_${sessionId}`;
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await sessionsCollection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersColl, connectedNumber);
                    await deleteSession(connectedNumber);
                    delete activeSockets[socketKey];
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 5 seconds...`);
                    delete activeSockets[socketKey];
                    setTimeout(() => {
                        reconnectSocket(sessionId, sessionsCollection, usersColl, connectedNumber);
                    }, 5000);
                } else {
                    console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                    delete activeSockets[socketKey];
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error during reconnect:`, error.message);
            if (error.message && error.message.includes('restart required')) {
                setTimeout(() => {
                    reconnectSocket(sessionId, sessionsCollection, usersColl, phoneNumber);
                }, 5000);
            }
        });

        const socketKey = phoneNumber ? `${phoneNumber}_${sessionId}` : sessionId;
        activeSockets[socketKey] = sock;
        console.log(`[${sessionId}] Reconnection attempt completed`);

    } catch (error) {
        console.error(`[${sessionId}] Reconnection failed:`, error.message);
        setTimeout(() => {
            reconnectSocket(sessionId, collection, usersCollection, phoneNumber);
        }, 10000);
    }
}

// ============================================================
// QR ENDPOINT
// ============================================================
app.get('/qr', async (req, res) => {
    const sessionId = generateSessionId();
    console.log(`[${sessionId}] New QR request received`);

    try {
        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const collection = db.collection('sessions');
        const usersCollection = db.collection('users');

        const { state, saveCreds } = await useMongoDBAuthState(collection, sessionId, null);
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
                console.log(`[${phoneNumber}] ✅ Connected successfully! Session: ${sessionId}`);
                
                // ⭐ IMMEDIATELY SAVE SESSION TO DATABASE WITH DELAY
                await saveSessionImmediately(phoneNumber, state.creds, sessionId, usersCollection);
                
                // Add delay before sending success message
                await new Promise(resolve => setTimeout(resolve, 3000));
                
                await sendSuccessMessage(sock, `${phoneNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {}
                
                console.log(`[${phoneNumber}] Session saved and connection kept alive`);
            }

            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${phoneNumber}_${sessionId}`;
                
                console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await collection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersCollection, phoneNumber);
                    await deleteSession(phoneNumber);
                    delete activeSockets[socketKey];
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 3 seconds...`);
                    delete activeSockets[socketKey];
                    setTimeout(() => {
                        reconnectSocket(sessionId, collection, usersCollection, phoneNumber);
                    }, 3000);
                } else {
                    console.log(`[${sessionId}] Connection closed normally`);
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error:`, error.message);
            if (error.message && error.message.includes('Bad MAC')) {
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                handleBadMACError(sock, sessionId, phoneNumber);
            }
        });

        const socketKey = `Unknown_${sessionId}`;
        activeSockets[socketKey] = sock;
        console.log(`[${sessionId}] Socket initialized successfully`);

    } catch (error) {
        console.error(`[${sessionId}] Error in /qr:`, error.message);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
});

// ============================================================
// PAIR ENDPOINT
// ============================================================
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

        const { state, saveCreds } = await useMongoDBAuthState(collection, sessionId, phoneNumber);
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
                console.log(`[${connectedNumber}] ✅ Connected via Pairing Code! Session: ${sessionId}`);
                
                // ⭐ IMMEDIATELY SAVE SESSION TO DATABASE WITH DELAY
                await saveSessionImmediately(connectedNumber, state.creds, sessionId, usersCollection);
                
                // Add delay before sending success message
                await new Promise(resolve => setTimeout(resolve, 3000));
                
                await sendSuccessMessage(sock, `${connectedNumber}@s.whatsapp.net`);
                
                try {
                    await sock.sendPresenceUpdate('available');
                } catch (err) {}
                
                console.log(`[${connectedNumber}] Session saved and connection kept alive`);
            }

            if (connection === 'close') {
                const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                const socketKey = `${phoneNumber}_${sessionId}`;
                
                console.log(`[${sessionId}] Connection closed with status: ${statusCode}`);
                
                if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
                    console.log(`[${sessionId}] Logged out, removing session...`);
                    await collection.deleteMany({ sessionId: { $regex: `^${sessionId}` } });
                    await removeUserFromDatabase(usersCollection, phoneNumber);
                    await deleteSession(phoneNumber);
                    delete activeSockets[socketKey];
                } else if (statusCode === DisconnectReason.restartRequired || 
                          statusCode === 515) {
                    console.log(`[${sessionId}] Status 515: Restart required, reconnecting in 3 seconds...`);
                    delete activeSockets[socketKey];
                    setTimeout(() => {
                        reconnectSocket(sessionId, collection, usersCollection, phoneNumber);
                    }, 3000);
                } else {
                    console.log(`[${sessionId}] Connection closed normally`);
                }
            }
        });

        sock.ev.on('error', (error) => {
            console.error(`[${sessionId}] Socket error:`, error.message);
            if (error.message && error.message.includes('Bad MAC')) {
                const phoneNumber = sock.user?.id ? sock.user.id.split(':')[0] : 'Unknown';
                handleBadMACError(sock, sessionId, phoneNumber);
            }
        });

        const socketKey = `${phoneNumber}_${sessionId}`;
        activeSockets[socketKey] = sock;
        console.log(`[${sessionId}] Socket initialized successfully`);

    } catch (error) {
        console.error(`[${sessionId}] Error in /pair:`, error.message);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
});

// ============================================================
// CHECK USER ENDPOINT
// ============================================================
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

// ============================================================
// CLEANUP AND RESTART FUNCTIONS
// ============================================================
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
    console.log('✅ MongoDB initialized successfully');
}).catch(err => {
    console.error('❌ Failed to initialize MongoDB:', err);
    process.exit(1);
});

console.log('🚀 Server starting with auto-restart capability...');
console.log('Auto-restart will trigger on:');
console.log('  - Uncaught exceptions');
console.log('  - Unhandled rejections');
console.log('  - Server errors');
console.log('  - Signal termination (SIGTERM/SIGINT)');
console.log('  - High memory usage (>500MB)');
console.log('  - WhatsApp status 515 (restart required)');
console.log('  - Bad MAC errors (auto-recovery)');

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
    console.log(`✅ Server is running on port ${PORT}`);
    console.log(`📡 Available endpoints:`);
    console.log(`  - GET  /qr`);
    console.log(`  - GET  /pair?number=947xxxxxxxx`);
    console.log(`  - GET  /check-user/:number`);
    console.log(`🔄 Auto-restart enabled on errors`);
    console.log(`🔑 Session ID auto-generated for each request`);
    console.log(`📱 WhatsApp status 515 will trigger auto-reconnect`);
    console.log(`⚠️  Bad MAC errors will trigger auto-recovery`);
    console.log(`💾 Sessions are saved immediately on connection with a 2-second delay for stability`);
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
        console.log(`⚠️ High memory usage detected: ${heapUsed.toFixed(2)}MB / ${heapTotal.toFixed(2)}MB`);
        console.log(`Auto-restarting due to high memory usage...`);
        cleanupAndRestart();
    }
}, 60000);
