import ws from 'k6/ws';
import { check, sleep } from 'k6';

export const options = {
  vus: 3,
  duration: '10s',
};

export default function () {
  const url = 'ws://127.0.0.1:10090/ws';
  
  ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      console.log('Connected!');
      socket.send(JSON.stringify({
        id: 'test-1',
        frameType: 0,
        method: 'conversation.chat',
        data: {
          recvId: '1',
          chatType: 1,
          msg: { mType: 0, content: 'hello' }
        }
      }));
    });

    socket.on('message', (data) => {
      console.log('Received:', data);
    });

    socket.on('error', (e) => {
      console.error('Error:', e);
    });

    socket.on('close', () => {
      console.log('Closed');
    });

    sleep(2);
    socket.close();
  });
}
