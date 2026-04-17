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

// 存储结构
const rooms = new Map();              // roomId -> Set of socket ids
const socketRoom = new Map();        // socketId -> roomId
const roomIdentities = new Map();    // roomId -> Map { identity: socketId }

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

io.on('connection', (socket) => {
    console.log(`✅ 用户连接: ${socket.id}`);

    // 加入房间（支持身份校验）
    socket.on('join-room', (data, callback) => {
        console.log(`收到 join-room 请求:`, data, `socketId: ${socket.id}`);
        
        let roomId, identity;
        
        // 兼容两种调用方式
        if (typeof data === 'string') {
            roomId = data;
            identity = null;
        } else {
            roomId = data.roomId;
            identity = data.identity;
        }
        
        console.log(`解析后: roomId=${roomId}, identity=${identity}`);
        
        // 1. 先离开旧房间
        const oldRoom = socketRoom.get(socket.id);
        if (oldRoom) {
            const oldRoomSet = rooms.get(oldRoom);
            if (oldRoomSet) {
                oldRoomSet.delete(socket.id);
                if (oldRoomSet.size === 0) {
                    rooms.delete(oldRoom);
                    roomIdentities.delete(oldRoom);
                } else {
                    // 清理旧房间的身份映射
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
        
        // 2. 获取或创建房间
        let roomSet = rooms.get(roomId);
        if (!roomSet) {
            roomSet = new Set();
            rooms.set(roomId, roomSet);
            roomIdentities.set(roomId, new Map());
        }
        
        const identities = roomIdentities.get(roomId);
        
        // 3. 【关键】身份冲突检测：同一身份不能重复加入
        if (identity && identities && identities.has(identity)) {
            console.log(`❌ 身份冲突: ${identity} 已经在房间中，拒绝 ${socket.id} 加入`);
            if (callback) {
                callback({ 
                    success: false, 
                    message: `身份冲突：${identity} 已经在房间中，不能重复加入` 
                });
            }
            return;
        }
        
        // 4. 房间人数限制（最多2人）
        if (roomSet.size >= 2) {
            console.log(`❌ 房间已满: ${roomId} 已有 ${roomSet.size} 人，拒绝 ${socket.id} 加入`);
            if (callback) {
                callback({ success: false, message: '房间已满 (最多2人)' });
            }
            return;
        }
        
        // 5. 加入房间
        roomSet.add(socket.id);
        socketRoom.set(socket.id, roomId);
        if (identity && identities) {
            identities.set(identity, socket.id);
        }
        socket.join(roomId);
        const currentCount = roomSet.size;
        
        console.log(`✅ 用户 ${socket.id} (${identity || '未知'}) 加入房间 ${roomId}，当前人数 ${currentCount}`);
        console.log(`当前房间身份映射:`, Array.from(identities.entries()));
        
        // 6. 广播人数变化
        io.to(roomId).emit('room-users', { roomId, count: currentCount });
        
        // 7. 回调成功
        if (callback) {
            callback({ success: true, count: currentCount });
        }
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
                
                const identities = roomIdentities.get(roomId);
                if (identities) {
                    for (let [identity, id] of identities.entries()) {
                        if (id === socket.id) {
                            identities.delete(identity);
                            console.log(`🗑️ 移除身份映射: ${identity}`);
                            break;
                        }
                    }
                }
                
                if (roomSet.size === 0) {
                    rooms.delete(roomId);
                    roomIdentities.delete(roomId);
                    console.log(`🗑️ 房间 ${roomId} 已清空`);
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

app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 服务器运行在 http://0.0.0.0:${PORT}`);
    console.log(`📁 记录文件位置: ${RECORDS_FILE}`);
});