/**
 * Easy-Chat WebSocket 压测脚本
 * 
 * 使用方法:
 *   1. 安装 k6: https://k6.io/docs/getting-started/installation/
 *   2. 运行测试: k6 run scripts/loadtest/ws_loadtest.js
 *   3. 高并发测试: k6 run scripts/loadtest/ws_loadtest.js --vus 100 --duration 60s
 *   4. 输出到 InfluxDB/Grafana: k6 run scripts/loadtest/ws_loadtest.js --out influxdb=http://localhost:8086/k6
 */

import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend, Rate, Gauge } from 'k6/metrics';

// ============================================
// 自定义指标
// ============================================
const wsConnected = new Counter('ws_connection_success');
const wsDisconnected = new Counter('ws_connection_closed');
const wsError = new Counter('ws_error');
const messagesSent = new Counter('ws_messages_sent');
const messagesReceived = new Counter('ws_messages_received');
const messageLatency = new Trend('ws_message_latency');       // 端到端消息延迟
const serverPushLatency = new Trend('ws_server_push_latency'); // 服务端推送延迟
const pingLatency = new Trend('ws_ping_latency');              // Ping/Pong 延迟
const connectionDuration = new Trend('ws_connection_duration');

// ============================================
// 配置
// ============================================
const WS_URL = __ENV.WS_URL || 'ws://127.0.0.1:10090/ws';
const TEST_RECV_ID = __ENV.RECV_ID || '1';  // 接收者 ID (给自己发消息测试)
const MESSAGE_INTERVAL_MIN = parseFloat(__ENV.MSG_INTERVAL_MIN || '0.5');
const MESSAGE_INTERVAL_MAX = parseFloat(__ENV.MSG_INTERVAL_MAX || '2.0');
const PING_INTERVAL = parseFloat(__ENV.PING_INTERVAL || '1'); // Ping 间隔秒数

// ============================================
// 测试场景配置
// ============================================
export const options = {
  // 默认场景: 渐增负载
  scenarios: {
    ramping_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 5 },    // 预热: 10 秒升到 5 用户
        { duration: '20s', target: 5 },    // 保持 5 用户 20 秒
        { duration: '10s', target: 20 },   // 升到 20 用户
        { duration: '30s', target: 20 },   // 保持 20 用户 30 秒
        { duration: '10s', target: 0 },    // 下降
      ],
    },
  },
  
  // 阈值
  thresholds: {
    'ws_connection_success': ['count>0'],
    'ws_message_latency': ['p(95)<280', 'p(99)<500'],  // 目标: p99 < 280ms
    'ws_server_push_latency': ['p(95)<100', 'p(99)<280'],
    'ws_ping_latency': ['p(95)<100', 'p(99)<200'],
    'ws_messages_sent': ['count>10'],
  },
};

// 高并发压测配置
export const highLoadOptions = {
  scenarios: {
    stress_test: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 100 },  // 30 秒升到 100 用户
        { duration: '60s', target: 100 },  // 保持 60 秒
        { duration: '30s', target: 0 },    // 下降
      ],
    },
  },
};

// 稳定性测试配置
export const soakOptions = {
  scenarios: {
    soak_test: {
      executor: 'constant-vus',
      vus: 50,
      duration: '10m',  // 持续 10 分钟
    },
  },
};

// ============================================
// 工具函数
// ============================================

/**
 * 生成唯一消息 ID
 */
function generateMsgId() {
  return `${Date.now()}-${__VU}-${__ITER}-${Math.random().toString(36).substr(2, 6)}`;
}

/**
 * 构造私聊消息
 * @param {string} recvId - 接收者 ID
 * @param {string} content - 消息内容
 * @param {number} mType - 消息类型 (0: 文本)
 */
function buildChatMessage(recvId, content, mType = 0) {
  const msgId = generateMsgId();
  const timestamp = Date.now();
  return {
    id: msgId,
    timestamp: timestamp,
    frameType: 0,
    method: 'conversation.chat',
    data: {
      recvId: recvId,
      chatType: 1,  // 1: 私聊, 2: 群聊
      msg: {
        mType: mType,
        content: content,
      },
    },
  };
}

/**
 * 构造心跳消息
 */
function buildPingMessage() {
  return JSON.stringify({
    id: generateMsgId(),
    frameType: 0,
    method: 'ping',
    data: {},
  });
}

// ============================================
// 主测试函数
// ============================================
export default function () {
  const url = WS_URL;
  let socket = null;
  let connectStartTime = 0;

  // 用于跟踪已发送消息的 ID 和时间戳
  const pendingMessages = new Map();
  
  // 用于跟踪 ping 的 ID 和时间
  const pendingPings = new Map();

  try {
    // 建立 WebSocket 连接
    connectStartTime = Date.now();
    
    socket = ws.connect(url, {}, function (socket) {
      const connectDuration = Date.now() - connectStartTime;
      connectionDuration.add(connectDuration);
      wsConnected.add(1);

      console.log(`[VU ${__VU}:${__ITER}] Connected in ${connectDuration}ms`);

      // 设置连接超时 (60 秒)
      socket.setTimeout(() => {
        console.log(`[VU ${__VU}:${__ITER}] Connection timeout, closing`);
        socket.close();
      }, 60000);

      // 处理收到的消息
      socket.on('message', (data) => {
        messagesReceived.add(1);
        
        // 尝试解析消息计算延迟
        try {
          const msg = JSON.parse(data);
          
          // 1. 测量 Ping 响应延迟
          if (msg.method === 'pong' && msg.id && pendingPings.has(msg.id)) {
            const pingSentTime = pendingPings.get(msg.id);
            const latency = Date.now() - pingSentTime;
            pingLatency.add(latency);
            pendingPings.delete(msg.id);
          }
          
          // 2. 计算服务端推送延迟 (从服务器 timestamp 到收到)
          if (msg.timestamp) {
            const serverPushLat = Date.now() - msg.timestamp;
            // 只记录合理的推送延迟 (避免负数和超大值)
            if (serverPushLat >= 0 && serverPushLat < 10000) {
              serverPushLatency.add(serverPushLat);
            }
          }
          
          // 3. 计算端到端延迟 (发送请求到收到响应)
          // 如果消息包含对应的请求 ID，计算延迟
          if (msg.id && pendingMessages.has(msg.id)) {
            const sentTime = pendingMessages.get(msg.id);
            const e2eLatency = Date.now() - sentTime;
            if (e2eLatency >= 0 && e2eLatency < 10000) {
              messageLatency.add(e2eLatency);
            }
            pendingMessages.delete(msg.id);
            console.log(`[VU ${__VU}:${__ITER}] E2E latency: ${e2eLatency}ms`);
          }
          
          // 4. 测量自己发送消息后收到推送的延迟 (自发自收测试)
          // 服务器推送消息时，检查是否是发给自己的
          if (msg.method === 'conversation.push' && msg.data && msg.data.sendId === TEST_RECV_ID) {
            // 这是发给自己的消息，可以测量推送延迟
            if (msg.timestamp) {
              const pushLat = Date.now() - msg.timestamp;
              if (pushLat >= 0 && pushLat < 10000) {
                serverPushLatency.add(pushLat);
                console.log(`[VU ${__VU}:${__ITER}] Server push latency: ${pushLat}ms`);
              }
            }
          }
        } catch (e) {
          // 非 JSON 消息忽略
        }
      });

      // 处理错误
      socket.on('error', (e) => {
        wsError.add(1);
        console.error(`[VU ${__VU}:${__ITER}] Error: ${e}`);
      });

      // 处理关闭
      socket.on('close', () => {
        wsDisconnected.add(1);
        console.log(`[VU ${__VU}:${__ITER}] Connection closed`);
      });

      // ==========================================
      // 模拟用户行为
      // ==========================================
      
      // 1. 发送心跳并测量延迟
      const pingId = generateMsgId();
      const pingSendTime = Date.now();
      pendingPings.set(pingId, pingSendTime);
      socket.send(JSON.stringify({
        id: pingId,
        frameType: 0,
        method: 'ping',
        data: {},
      }));
      sleep(0.2); // 等待 pong 响应
      
      // 2. 给自己发消息测试 (测试消息推送延迟)
      // 由于消息会异步推送，需要自己收到推送才能测量延迟
      const recvId = TEST_RECV_ID;
      const content = `Load test message from VU${__VU} iter${__ITER} ts${Date.now()}`;
      const chatMsg = buildChatMessage(recvId, content);
      
      const sendStart = Date.now();
      pendingMessages.set(chatMsg.id, sendStart);
      
      socket.send(JSON.stringify(chatMsg));
      messagesSent.add(1);
      
      console.log(`[VU ${__VU}:${__ITER}] Sent message: ${content}`);

      // 3. 等待接收推送（给自己发消息会收到推送）
      sleep(0.5);
      
      // 4. 随机间隔后再发一条
      sleep(Math.random() * (MESSAGE_INTERVAL_MAX - MESSAGE_INTERVAL_MIN) + MESSAGE_INTERVAL_MIN);
      
      const content2 = `Load test message 2 from VU${__VU} ts${Date.now()}`;
      const chatMsg2 = buildChatMessage(recvId, content2);
      const sendStart2 = Date.now();
      pendingMessages.set(chatMsg2.id, sendStart2);
      
      socket.send(JSON.stringify(chatMsg2));
      messagesSent.add(1);

      // 5. 再发一次 ping 测试延迟
      const pingId2 = generateMsgId();
      pendingPings.set(pingId2, Date.now());
      socket.send(JSON.stringify({
        id: pingId2,
        frameType: 0,
        method: 'ping',
        data: {},
      }));

      // 6. 模拟用户在线时间
      sleep(Math.random() * 2 + 1);  // 1-3 秒后断开
      
      // 7. 关闭连接
      socket.close();
    });

    // 检查连接状态
    const connectionSuccess = check(socket, {
      'WebSocket connected': (r) => r && r.status === 101,
    });

    if (!connectionSuccess) {
      wsError.add(1);
      console.error(`[VU ${__VU}:${__ITER}] Connection check failed`);
    }

  } catch (e) {
    wsError.add(1);
    console.error(`[VU ${__VU}:${__ITER}] Exception: ${e}`);
  }
}

// ============================================
// 测试前准备
// ============================================
export function setup() {
  console.log('='.repeat(50));
  console.log('Easy-Chat WebSocket Load Test');
  console.log('='.repeat(50));
  console.log(`Target URL: ${WS_URL}`);
  console.log(`Receiver ID: ${TEST_RECV_ID}`);
  console.log(`Start Time: ${new Date().toISOString()}`);
  console.log('='.repeat(50));
  
  return {
    testStartTime: Date.now(),
    wsUrl: WS_URL,
  };
}

// ============================================
// 测试后清理/报告
// ============================================
export function teardown(data) {
  const duration = (Date.now() - data.testStartTime) / 1000;
  
  console.log('='.repeat(50));
  console.log('Load Test Completed!');
  console.log('='.repeat(50));
  console.log(`Duration: ${duration.toFixed(2)}s`);
  console.log('');
  console.log('Metrics collected:');
  console.log('  - ws_connection_success: 成功连接数');
  console.log('  - ws_connection_closed: 关闭连接数');
  console.log('  - ws_error: 错误数');
  console.log('  - ws_messages_sent: 发送消息数');
  console.log('  - ws_messages_received: 接收消息数');
  console.log('  - ws_message_latency: 消息延迟 (p95, p99)');
  console.log('  - ws_server_push_latency: 服务端推送延迟');
  console.log('  - ws_ping_latency: Ping/Pong 延迟');
  console.log('  - ws_connection_duration: 连接持续时间');
  console.log('');
  console.log('Run with Grafana for visualization:');
  console.log('  k6 run ws_loadtest.js --out influxdb=http://localhost:8086/k6');
  console.log('='.repeat(50));
}
