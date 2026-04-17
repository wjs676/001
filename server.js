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

// 数据存储文件路径
const RECORDS_FILE = path.join(__dirname, 'records.txt');

// 存储房间信息
const rooms = new Map();        // roomId -> Set of socket ids
const socketRoom = new Map();   // socketId -> roomId
const roomIdentities = new Map(); // roomId -> Map of identity -> socketId (用于身份冲突检测)

// 读取记录
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

// 写入记录
function saveRecords(records) {
    try {
        fs.writeFileSync(RECORDS_FILE, JSON.stringify(records, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('保存记录文件失败:', e);
        return false;
    }
}

// 添加新记录
function addRecord(record) {
    const records = loadRecords();
    records.push(record);
    saveRecords(records);
    return records;
}

// 清空记录
function clearRecords() {
    saveRecords([]);
    return [];
}

io.on('connection', (socket) => {
    console.log(`✅ 用户连接: ${socket.id}`);

    // 加入房间（增加身份校验）
    socket.on('join-room', (data, callback) => {
        // 兼容旧版调用：可能是 (roomId, callback) 或 ({roomId, identity}, callback)
        let roomId, identity, callbackFunc;
        if (typeof data === 'string') {
            roomId = data;
            identity = null;
            callbackFunc = callback;
        } else {
            roomId = data.roomId;
            identity = data.identity;
            callbackFunc = callback;
        }
        
        // 离开旧房间
        const oldRoom = socketRoom.get(socket.id);
        if (oldRoom) {
            const oldRoomSet = rooms.get(oldRoom);
            if (oldRoomSet) {
                oldRoomSet.delete(socket.id);
                if (oldRoomSet.size === 0) {
                    rooms.delete(oldRoom);
                    roomIdentities.delete(oldRoom);
                } else {
                    io.to(oldRoom).emit('room-users', { roomId: oldRoom, count: oldRoomSet.size });
                }
            }
            socketRoom.delete(socket.id);
        }

        let roomSet = rooms.get(roomId);
        if (!roomSet) {
            roomSet = new Set();
            rooms.set(roomId, roomSet);
            roomIdentities.set(roomId, new Map()); // 初始化身份映射
        }

        // 身份冲突检测（如果提供了identity）
        const identities = roomIdentities.get(roomId);
        if (identity && identities && identities.has(identity)) {
            // 同一身份已经在房间中，拒绝加入
            if (callbackFunc) callbackFunc({ success: false, message: `身份冲突：${identity} 已经在房间中，不能重复加入` });
            return;
        }

        // 限制最多2人
        if (roomSet.size >= 2) {
            if (callbackFunc) callbackFunc({ success: false, message: '房间已满 (最多2人)' });
            return;
        }

        // 加入房间
        roomSet.add(socket.id);
        socketRoom.set(socket.id, roomId);
        if (identity && identities) {
            identities.set(identity, socket.id);
        }
        socket.join(roomId);
        const currentCount = roomSet.size;

        console.log(`用户 ${socket.id} 加入房间 ${roomId}，身份: ${identity || '未知'}，当前人数 ${currentCount}`);
        io.to(roomId).emit('room-users', { roomId, count: currentCount });

        if (callbackFunc) callbackFunc({ success: true, count: currentCount });
    });

    // 获取历史记录
    socket.on('get-records', (callback) => {
        const records = loadRecords();
        if (callback) callback({ success: true, records });
    });

    // 新增记录
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

    // 清空记录
    socket.on('clear-records', (callback) => {
        const emptyRecords = clearRecords();
        rooms.forEach((_, roomId) => {
            io.to(roomId).emit('records-updated', { records: emptyRecords });
        });
        if (callback) callback({ success: true, records: emptyRecords });
    });

    // 发送想念通知
    socket.on('miss-notify', ({ roomId, from, time }) => {
        const curRoom = socketRoom.get(socket.id);
        if (!curRoom || curRoom !== roomId) return;
        socket.to(roomId).emit('someone-missed', { from, time });
        console.log(`💕 ${from} 发送了想念`);
    });

    // 断开连接
    socket.on('disconnect', () => {
        const roomId = socketRoom.get(socket.id);
        if (roomId) {
            const roomSet = rooms.get(roomId);
            if (roomSet) {
                roomSet.delete(socket.id);
                
                // 清理身份映射
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
            console.log(`用户 ${socket.id} 离开房间 ${roomId}`);
        }
        console.log(`❌ 用户断开: ${socket.id}`);
    });
});

// 提供静态文件
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 服务器运行在 http://0.0.0.0:${PORT}`);
    console.log(`📁 记录文件位置: ${RECORDS_FILE}`);
});