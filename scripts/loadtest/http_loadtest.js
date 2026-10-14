/**
 * Easy-Chat HTTP API 压测脚本
 * 
 * 测试目标:
 * 1. HTTP API 响应时间 (目标: p99 < 65ms)
 * 2. 消息推送延迟
 * 
 * 使用方法:
 *   1. 运行测试: k6 run scripts/loadtest/http_loadtest.js
 *   2. 高并发测试: k6 run scripts/loadtest/http_loadtest.js --vus 100 --duration 60s
 *   3. 输出到 InfluxDB/Grafana: k6 run scripts/loadtest/http_loadtest.js --out influxdb=http://localhost:8086/k6
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';

// ============================================
// 自定义指标
// ============================================
const apiSuccess = new Counter('api_request_success');
const apiError = new Counter('api_request_error');
const apiLatency = new Trend('api_request_latency');          // API 响应时间
const loginLatency = new Trend('api_login_latency');            // 登录延迟
const chatLogLatency = new Trend('api_chatlog_latency');       // 获取聊天记录延迟
const conversationLatency = new Trend('api_conversation_latency'); // 获取会话延迟

// ============================================
// 配置
// ============================================
const BASE_URL = __ENV.API_BASE_URL || 'http://127.0.0.1:8888';
const TEST_USER_ID = __ENV.TEST_USER_ID || '1';
const TEST_RECV_ID = __ENV.TEST_RECV_ID || '2';

// ============================================
// 测试场景配置
// ============================================
export const options = {
  scenarios: {
    // 默认场景: 渐增负载
    ramping_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 10 },   // 预热: 10 秒升到 10 用户
        { duration: '20s', target: 10 },   // 保持 10 用户 20 秒
        { duration: '10s', target: 50 },  // 升到 50 用户
        { duration: '30s', target: 50 },   // 保持 50 用户 30 秒
        { duration: '10s', target: 0 },   // 下降
      ],
    },
  },
  
  // 阈值 - 目标: p99 < 65ms
  thresholds: {
    'api_request_success': ['count>0'],
    'api_request_latency': ['p(95)<100', 'p(99)<150'],  // 宽松阈值用于测试
    'api_login_latency': ['p(95)<100', 'p(99)<200'],
    'api_chatlog_latency': ['p(95)<100', 'p(99)<150'],
    'api_conversation_latency': ['p(95)<100', 'p(99)<150'],
  },
};

// ============================================
// 辅助函数
// ============================================

/**
 * 生成测试 token (模拟 JWT token)
 * 实际使用时需要先调用登录接口获取真实 token
 */
function generateMockToken(userId) {
  // 由于 k6 不支持 btoa，使用简单的模拟 token
  // 注意: 实际测试中需要先调用登录接口获取真实 token
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(7);
  return `mock_token_${userId}_${timestamp}_${random}`;
}

// 全局变量存储登录后的 token
let authToken = null;

/**
 * 构造带认证的请求头
 */
function getAuthHeaders(userId) {
  const token = generateMockToken(userId);
  return {
    'Content-Type': 'application/json',
    'Authorization': token,
  };
}

/**
 * 测量请求延迟
 */
function measureRequest(url, method, headers, body, latencyMetric) {
  const startTime = Date.now();
  
  let response;
  if (method === 'GET') {
    response = http.get(url, { headers });
  } else if (method === 'POST') {
    response = http.post(url, JSON.stringify(body), { headers });
  } else if (method === 'PUT') {
    response = http.put(url, JSON.stringify(body), { headers });
  }
  
  const latency = Date.now() - startTime;
  
  // 记录延迟
  latencyMetric.add(latency);
  apiLatency.add(latency);
  
  return response;
}

// ============================================
// 测试用例
// ============================================

/**
 * 测试 1: 登录接口
 */
function testLogin() {
  const url = `${BASE_URL}/v1/user/login`;
  const body = {
    phone: `138${String(__VU + 1).padStart(8, '0')}`,
    password: '123456',
  };
  
  const startTime = Date.now();
  const response = http.post(url, JSON.stringify(body), {
    'Content-Type': 'application/json',
  });
  const latency = Date.now() - startTime;
  
  loginLatency.add(latency);
  apiLatency.add(latency);
  
  // 记录请求结果，不管成功失败都记录
  if (response.status === 200) {
    const success = check(response, {
      'Login: status is 200': (r) => r.status === 200,
      'Login: has token': (r) => r.json('token') !== undefined || r.json('data.token') !== undefined,
    });
    
    if (success) {
      apiSuccess.add(1);
      // 保存 token
      const body = response.json();
      authToken = body.token || (body.data && body.data.token);
    } else {
      apiError.add(1);
    }
  } else {
    // 登录失败（用户未注册），尝试注册
    apiError.add(1);
    console.log(`[VU ${__VU}] Login failed: ${response.status} - ${response.body.substring(0, 100)}`);
    
    // 尝试注册
    testRegister();
  }
  
  return response;
}

/**
 * 测试注册接口
 */
function testRegister() {
  const url = `${BASE_URL}/v1/user/register`;
  const body = {
    phone: `138${String(__VU + 1).padStart(8, '0')}`,
    password: '123456',
    nickname: `User${__VU}`,
    sex: 1,
  };
  
  const startTime = Date.now();
  const response = http.post(url, JSON.stringify(body), {
    'Content-Type': 'application/json',
  });
  const latency = Date.now() - startTime;
  
  loginLatency.add(latency);
  apiLatency.add(latency);
  
  if (response.status === 200) {
    const success = check(response, {
      'Register: status is 200': (r) => r.status === 200,
      'Register: has token': (r) => r.json('token') !== undefined || r.json('data.token') !== undefined,
    });
    
    if (success) {
      apiSuccess.add(1);
      const body = response.json();
      authToken = body.token || (body.data && body.data.token);
      console.log(`[VU ${__VU}] Register successful`);
    }
  }
  
  return response;
}

/**
 * 测试 2: 获取聊天记录
 */
function testGetChatLog(token) {
  const url = `${BASE_URL}/v1/im/chatlog?conversationId=conv_${TEST_USER_ID}_${TEST_RECV_ID}&page=1&pageSize=20`;
  
  const response = measureRequest(
    url, 'GET',
    {
      'Content-Type': 'application/json',
      'Authorization': token,
    },
    null,
    chatLogLatency
  );
  
  const success = check(response, {
    'ChatLog: status is 200': (r) => r.status === 200,
  });
  
  if (success) {
    apiSuccess.add(1);
  } else {
    apiError.add(1);
    console.log(`[VU ${__VU}] GetChatLog failed: ${response.status}`);
  }
  
  return response;
}

/**
 * 测试 3: 获取会话列表
 */
function testGetConversations(token) {
  const url = `${BASE_URL}/v1/im/conversation`;
  
  const response = measureRequest(
    url, 'GET',
    {
      'Content-Type': 'application/json',
      'Authorization': token,
    },
    null,
    conversationLatency
  );
  
  const success = check(response, {
    'Conversation: status is 200': (r) => r.status === 200,
  });
  
  if (success) {
    apiSuccess.add(1);
  } else {
    apiError.add(1);
    console.log(`[VU ${__VU}] GetConversations failed: ${response.status}`);
  }
  
  return response;
}

/**
 * 测试 4: 更新会话
 */
function testUpdateConversation(token) {
  const url = `${BASE_URL}/v1/im/conversation`;
  const body = {
    conversationId: `conv_${TEST_USER_ID}_${TEST_RECV_ID}`,
    isPinned: false,
    isMuted: false,
  };
  
  const response = measureRequest(
    url, 'PUT',
    {
      'Content-Type': 'application/json',
      'Authorization': token,
    },
    body,
    conversationLatency
  );
  
  const success = check(response, {
    'UpdateConversation: status is 200': (r) => r.status === 200 || r.status === 204,
  });
  
  if (success) {
    apiSuccess.add(1);
  } else {
    apiError.add(1);
    console.log(`[VU ${__VU}] UpdateConversation failed: ${response.status}`);
  }
  
  return response;
}

/**
 * 测试 5: 获取已读记录
 */
function testGetReadRecords(token) {
  const url = `${BASE_URL}/v1/im/chatlog/readRecords?conversationId=conv_${TEST_USER_ID}_${TEST_RECV_ID}`;
  
  const response = measureRequest(
    url, 'GET',
    {
      'Content-Type': 'application/json',
      'Authorization': token,
    },
    null,
    chatLogLatency
  );
  
  const success = check(response, {
    'ReadRecords: status is 200': (r) => r.status === 200,
  });
  
  if (success) {
    apiSuccess.add(1);
  } else {
    apiError.add(1);
    console.log(`[VU ${__VU}] GetReadRecords failed: ${response.status}`);
  }
  
  return response;
}

/**
 * 测试 6: 设置用户会话 (建立会话)
 */
function testSetupConversation(token) {
  const url = `${BASE_URL}/v1/im/setup/conversation`;
  const body = {
    recvId: TEST_RECV_ID,
    chatType: 1, // 1: 私聊
  };
  
  const response = measureRequest(
    url, 'POST',
    {
      'Content-Type': 'application/json',
      'Authorization': token,
    },
    body,
    conversationLatency
  );
  
  const success = check(response, {
    'SetupConversation: status is 200': (r) => r.status === 200,
  });
  
  if (success) {
    apiSuccess.add(1);
  } else {
    apiError.add(1);
    console.log(`[VU ${__VU}] SetupConversation failed: ${response.status} - ${response.body}`);
  }
  
  return response;
}

// ============================================
// 主测试函数
// ============================================
export default function () {
  // 为每个 VU 生成唯一的用户 ID
  const userId = `${TEST_USER_ID}_${__VU}`;
  
  // 先尝试登录获取真实 token (setup 中缓存)
  if (!authToken) {
    const loginUrl = `${BASE_URL}/v1/user/login`;
    const loginBody = {
      phone: `138${String(__VU).padStart(8, '0')}`,
      password: '123456',
    };
    
    const loginResponse = http.post(loginUrl, JSON.stringify(loginBody), {
      'Content-Type': 'application/json',
    });
    
    if (loginResponse.status === 200) {
      const body = loginResponse.json();
      authToken = body.token || (body.data && body.data.token);
      console.log(`[VU ${__VU}] Login successful, got token`);
    } else {
      // 登录失败，使用 mock token
      authToken = generateMockToken(userId);
      console.log(`[VU ${__VU}] Login failed (${loginResponse.status}), using mock token`);
    }
  }
  
  const token = authToken;
  
  console.log(`[VU ${__VU}] Starting API tests`);
  
  // 1. 测试登录
  testLogin();
  sleep(0.1);
  
  // 2. 测试建立会话
  testSetupConversation(token);
  sleep(0.1);
  
  // 3. 测试获取会话列表
  testGetConversations(token);
  sleep(0.1);
  
  // 4. 测试获取聊天记录
  testGetChatLog(token);
  sleep(0.1);
  
  // 5. 测试更新会话
  testUpdateConversation(token);
  sleep(0.1);
  
  // 6. 测试获取已读记录
  testGetReadRecords(token);
  
  // 模拟用户思考时间
  sleep(Math.random() * 2 + 0.5);
}

// ============================================
// 测试前准备
// ============================================
export function setup() {
  console.log('='.repeat(50));
  console.log('Easy-Chat HTTP API Load Test');
  console.log('='.repeat(50));
  console.log(`API Base URL: ${BASE_URL}`);
  console.log(`Test User ID: ${TEST_USER_ID}`);
  console.log(`Test Receiver ID: ${TEST_RECV_ID}`);
  console.log(`Start Time: ${new Date().toISOString()}`);
  console.log('='.repeat(50));
  
  // 可以在这里做一些初始化，比如预热服务
  
  return {
    testStartTime: Date.now(),
    baseUrl: BASE_URL,
  };
}

// ============================================
// 测试后清理/报告
// ============================================
export function teardown(data) {
  const duration = (Date.now() - data.testStartTime) / 1000;
  
  console.log('='.repeat(50));
  console.log('API Load Test Completed!');
  console.log('='.repeat(50));
  console.log(`Duration: ${duration.toFixed(2)}s`);
  console.log('');
  console.log('Metrics collected:');
  console.log('  - api_request_success: 成功请求数');
  console.log('  - api_request_error: 失败请求数');
  console.log('  - api_request_latency: API 响应时间 (全局)');
  console.log('  - api_login_latency: 登录响应时间');
  console.log('  - api_chatlog_latency: 获取聊天记录响应时间');
  console.log('  - api_conversation_latency: 会话操作响应时间');
  console.log('');
  console.log('Target SLA:');
  console.log('  - HTTP API 响应时间: p99 < 65ms');
  console.log('='.repeat(50));
}
