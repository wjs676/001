const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const RECORDS_FILE = path.join(__dirname, 'records.txt');
const OFFLINE_MSGS_FILE = path.join(__dirname, 'offline_messages.json');
const MESSAGES_FILE = path.join(__dirname, 'messages.json');

// 存储结构
const rooms = new Map();
const socketRoom = new Map();
const roomIdentities = new Map();

// 读取留言
function loadMessages() {
    try {
        if (fs.existsSync(MESSAGES_FILE)) {
            const content = fs.readFileSync(MESSAGES_FILE, 'utf8');
            return JSON.parse(content);
        }
    } catch (e) {
        console.error('读取留言文件失败:', e);
    }
    return [];
}

// 保存留言
function saveMessages(messages) {
    try {
        fs.writeFileSync(MESSAGES_FILE, JSON.stringify(messages, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('保存留言文件失败:', e);
        return false;
    }
}

// 添加留言
function addMessage(content, time) {
    const messages = loadMessages();
    messages.push({ content, time, timestamp: Date.now() });
    saveMessages(messages);
    return messages;
}

// 读取离线消息
function loadOfflineMessages() {
    try {
        if (fs.existsSync(OFFLINE_MSGS_FILE)) {
            const content = fs.readFileSync(OFFLINE_MSGS_FILE, 'utf8');
            return JSON.parse(content);
        }
    } catch (e) {
        console.error('读取离线消息文件失败:', e);
    }
    return {};
}

function saveOfflineMessages(messages) {
    try {
        fs.writeFileSync(OFFLINE_MSGS_FILE, JSON.stringify(messages, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('保存离线消息文件失败:', e);
        return false;
    }
}

function addOfflineMiss(from, to, time) {
    const messages = loadOfflineMessages();
    const key = `${to}_${from}`;
    if (!messages[key]) {
        messages[key] = [];
    }
    messages[key].push({ from, to, time, timestamp: Date.now() });
    saveOfflineMessages(messages);
    return messages[key].length;
}

function getAndClearOfflineMisses(identity, partnerIdentity) {
    const messages = loadOfflineMessages();
    const key = `${identity}_${partnerIdentity}`;
    const misses = messages[key] || [];
    delete messages[key];
    saveOfflineMessages(messages);
    return misses;
}

function loadRecords() {
    try {
        if (fs.existsSync(RECORDS_FILE)) {
            const content = fs.readFileSync(RECORDS_FILE, 'utf8');
            const data = JSON.parse(content);
            return Array.isArray(data) ? data : [];
        }
    } catch (e) {
        console.error('读取记录文件失败:', e);
    }
    return [];
}

function saveRecords(records) {
    try {
        fs.writeFileSync(RECORDS_FILE, JSON.stringify(records, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('保存记录文件失败:', e);
        return false;
    }
}

function addRecord(record) {
    const records = loadRecords();
    records.push(record);
    saveRecords(records);
    return records;
}

function clearRecords() {
    saveRecords([]);
    return [];
}

// 留言板API
app.use(express.json());
app.get('/api/messages', (req, res) => {
    const messages = loadMessages();
    res.json(messages);
});

app.post('/api/messages', (req, res) => {
    const { content } = req.body;
    if (!content || content.trim() === '' || content.length > 15) {
        return res.json({ success: false, message: '留言内容无效' });
    }
    const now = new Date();
    const time = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')} ${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`;
    const updatedMessages = addMessage(content.trim(), time);
    res.json({ success: true, messages: updatedMessages });
});

io.on('connection', (socket) => {
    console.log(`✅ 用户连接: ${socket.id}`);

    socket.on('join-room', (data, callback) => {
        let roomId, identity;
        
        if (typeof data === 'string') {
            roomId = data;
            identity = null;
        } else {
            roomId = data.roomId;
            identity = data.identity;
        }
        
        const oldRoom = socketRoom.get(socket.id);
        if (oldRoom) {
            const oldRoomSet = rooms.get(oldRoom);
            if (oldRoomSet) {
                oldRoomSet.delete(socket.id);
                if (oldRoomSet.size === 0) {
                    rooms.delete(oldRoom);
                    roomIdentities.delete(oldRoom);
                } else {
                    const oldIdentities = roomIdentities.get(oldRoom);
                    if (oldIdentities) {
                        for (let [idKey, idValue] of oldIdentities.entries()) {
                            if (idValue === socket.id) {
                                oldIdentities.delete(idKey);
                                break;
                            }
                        }
                    }
                    io.to(oldRoom).emit('room-users', { roomId: oldRoom, count: oldRoomSet.size });
                }
            }
            socketRoom.delete(socket.id);
        }
        
        let roomSet = rooms.get(roomId);
        if (!roomSet) {
            roomSet = new Set();
            rooms.set(roomId, roomSet);
            roomIdentities.set(roomId, new Map());
        }
        
        const identities = roomIdentities.get(roomId);
        
        if (identity && identities && identities.has(identity)) {
            if (callback) {
                callback({ success: false, message: `身份冲突：${identity} 已经在房间中` });
            }
            return;
        }
        
        if (roomSet.size >= 2) {
            if (callback) callback({ success: false, message: '房间已满 (最多2人)' });
            return;
        }
        
        roomSet.add(socket.id);
        socketRoom.set(socket.id, roomId);
        if (identity && identities) {
            identities.set(identity, socket.id);
        }
        socket.join(roomId);
        const currentCount = roomSet.size;
        
        console.log(`用户 ${socket.id} (${identity}) 加入房间 ${roomId}，当前人数 ${currentCount}`);
        io.to(roomId).emit('room-users', { roomId, count: currentCount });
        
        if (currentCount === 2 && identity) {
            const partnerIdentity = (identity === "周周") ? "小汪" : "周周";
            const offlineMisses = getAndClearOfflineMisses(identity, partnerIdentity);
            if (offlineMisses.length > 0) {
                socket.emit('offline-misses', { count: offlineMisses.length, misses: offlineMisses });
                console.log(`📬 发送离线想念给 ${identity}，共 ${offlineMisses.length} 条`);
            }
        }
        
        if (callback) callback({ success: true, count: currentCount });
    });
    
    socket.on('get-records', (callback) => {
        const records = loadRecords();
        if (callback) callback({ success: true, records });
    });
    
    socket.on('new-record', (data, callback) => {
        const { from, to, time, timestamp } = data;
        if (!from || !to) {
            if (callback) callback({ success: false, message: '参数错误' });
            return;
        }
        const newRecord = { from, to, time, timestamp };
        const updatedRecords = addRecord(newRecord);
        
        const roomId = socketRoom.get(socket.id);
        if (roomId) {
            io.to(roomId).emit('records-updated', { records: updatedRecords });
        }
        
        if (callback) callback({ success: true, records: updatedRecords });
    });
    
    socket.on('clear-records', (callback) => {
        const emptyRecords = clearRecords();
        rooms.forEach((_, roomId) => {
            io.to(roomId).emit('records-updated', { records: emptyRecords });
        });
        if (callback) callback({ success: true, records: emptyRecords });
    });
    
    socket.on('miss-notify', ({ roomId, from, time }) => {
        const curRoom = socketRoom.get(socket.id);
        if (!curRoom || curRoom !== roomId) return;
        
        const roomSet = rooms.get(roomId);
        const isPartnerOnline = roomSet && roomSet.size === 2;
        
        if (isPartnerOnline) {
            socket.to(roomId).emit('someone-missed', { from, time, isOnline: true });
            console.log(`💕 ${from} 发送了想念（对方在线，实时通知）`);
        } else {
            const to = (from === "周周") ? "小汪" : "周周";
            const count = addOfflineMiss(from, to, time);
            console.log(`💾 ${from} 发送了想念（对方离线，已保存，累计${count}条）`);
            socket.emit('miss-saved-offline', { to, time, count });
        }
    });
    
    socket.on('disconnect', () => {
        const roomId = socketRoom.get(socket.id);
        if (roomId) {
            const roomSet = rooms.get(roomId);
            if (roomSet) {
                roomSet.delete(socket.id);
                
                const identities = roomIdentities.get(roomId);
                if (identities) {
                    for (let [identity, id] of identities.entries()) {
                        if (id === socket.id) {
                            identities.delete(identity);
                            break;
                        }
                    }
                }
                
                if (roomSet.size === 0) {
                    rooms.delete(roomId);
                    roomIdentities.delete(roomId);
                } else {
                    io.to(roomId).emit('room-users', { roomId, count: roomSet.size });
                }
            }
            socketRoom.delete(socket.id);
        }
        console.log(`❌ 用户断开: ${socket.id}`);
    });
});

app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 服务器运行在 http://0.0.0.0:${PORT}`);
    console.log(`📁 记录文件位置: ${RECORDS_FILE}`);
    console.log(`📁 离线消息文件位置: ${OFFLINE_MSGS_FILE}`);
    console.log(`📁 留言板文件位置: ${MESSAGES_FILE}`);
});