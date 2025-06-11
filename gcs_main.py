# gcs_main.py

# --- Python Standard Libraries ---
import threading
import asyncio

# --- Third-party Libraries ---
import rospy
import uvicorn
import fastapi
import socketio

# --- ROS Message Types ---
from sensor_msgs.msg import NavSatFix
from std_msgs.msg import String  # 用作简单的指令消息


class GCSGateway:
    """
    一个集成的ROS-to-Web网关，在一个Python进程中运行。
    """

    def __init__(self):
        # 1. 初始化共享数据和线程锁
        self.shared_state = {"gps": {"lat": 0.0, "lon": 0.0}}
        self.lock = threading.Lock()

        # 2. 初始化Web服务器和Socket.IO
        self.app = fastapi.FastAPI()
        self.sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
        self.sio_app = socketio.ASGIApp(self.sio, self.app)

        # 3. 初始化ROS节点
        rospy.init_node('gcs_gateway_node', anonymous=True)

        # 4. 绑定所有事件和回调
        self._bind_sio_events()
        self._setup_ros_communications()

        # 5. 启动后台任务
        self._start_background_tasks()

    def _bind_sio_events(self):
        """绑定所有Socket.IO事件处理器"""
        @self.sio.on('connect')
        def connect(sid, environ):
            print(f"[Socket.IO] Client connected: {sid}")

        @self.sio.on('disconnect')
        def disconnect(sid):
            print(f"[Socket.IO] Client disconnected: {sid}")

        @self.sio.on('rtl_command')
        async def handle_rtl_command(sid, data):
            """处理从前端收到的返航指令"""
            print(f"[Socket.IO] Received 'rtl_command' from {sid} with data: {data}")

            # 创建一个ROS消息并发布
            # 这里的指令内容可以根据你的无人机端逻辑来定义
            command_str = f"RTL triggered by {sid}"
            self.command_publisher.publish(String(data=command_str))

            # 向前端发回一个确认消息
            await self.sio.emit('command_response', {'status': 'OK', 'command': 'RTL'}, to=sid)

    def _setup_ros_communications(self):
        """设置所有ROS订阅者和发布者"""
        # 订阅GPS话题
        rospy.Subscriber('/uav/gps/fix', NavSatFix, self._gps_callback)
        # 创建指令发布者
        self.command_publisher = rospy.Publisher('/gcs/command', String, queue_size=10)

    def _gps_callback(self, message: NavSatFix):
        """ROS回调：当收到GPS消息时更新共享状态"""
        with self.lock:
            self.shared_state["gps"]["lat"] = message.latitude
            self.shared_state["gps"]["lon"] = message.longitude
        # 打印日志以供调试
        # print(f"[ROS] Received GPS: Lat={message.latitude}, Lon={message.longitude}")

    def _start_background_tasks(self):
        """启动所有后台线程"""
        # 启动一个线程用于将数据推送到Web端
        pusher_thread = threading.Thread(target=self._telemetry_pusher)
        pusher_thread.daemon = True
        pusher_thread.start()

    def _telemetry_pusher(self):
        """后台线程任务：定期推送遥测数据"""
        # 获取当前线程的事件循环，如果不存在则创建一个
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        while not rospy.is_shutdown():
            with self.lock:
                # 创建共享状态的副本以发送
                state_copy = self.shared_state.copy()

            # 发射'telemetry_update'事件，并附上数据
            future = asyncio.run_coroutine_threadsafe(
                self.sio.emit('telemetry_update', state_copy),
                loop
            )
            # 等待任务在主事件循环中完成
            future.result()

            rospy.sleep(0.1)  # 推送频率为10Hz

    def run(self):
        """启动整个网关应用"""
        # 启动ROS事件循环(rospy.spin)在独立的线程中
        ros_thread = threading.Thread(target=lambda: rospy.spin())
        ros_thread.daemon = True
        ros_thread.start()

        # 启动Uvicorn Web服务器
        uvicorn.run(self.app, host="0.0.0.0", port=8000)


# 主程序入口
if __name__ == '__main__':
    gateway = GCSGateway()
    gateway.run()