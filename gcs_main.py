# gcs_main.py

# --- Python Standard Libraries ---
import threading
import asyncio
import time

# --- Third-party Libraries ---
import uvicorn
import fastapi
from fastapi.middleware.cors import CORSMiddleware
import socketio

# --- ROS ---
try:
    import rospy
    from sensor_msgs.msg import NavSatFix
    from std_msgs.msg import String
except ImportError:
    print("!!! 错误：无法导入rospy。请确保ROS环境已正确激活！")
    exit()


class GCSGateway:
    """
    一个集成的ROS-to-Web网关，在一个Python进程中运行。
    """

    def __init__(self):
        # 1. 初始化共享数据和线程锁
        self.shared_state = {"gps": {"lat": 0.0, "lon": 0.0}}
        self.lock = threading.Lock()

        # 2. 初始化Web服务器和Socket.IO
        self.fastapi_app = fastapi.FastAPI()
        self.fastapi_app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # 允许所有来源的请求
            allow_credentials=True,
            allow_methods=["*"],   # 允许所有HTTP方法 (GET, POST, etc.)
            allow_headers=["*"],   # 允许所有HTTP请求头
        )
        self.sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
        self.main_app = socketio.ASGIApp(
            self.sio,
            other_asgi_app=self.fastapi_app,  # 将FastAPI应用作为后备
            socketio_path='/sio'  # 明确我们期望的路径
        )
        # --- 添加一个变量来存储主事件循环，并设置一个启动事件 ---
        self.main_loop = None

        @self.fastapi_app.on_event("startup")
        async def app_startup():
            # 在FastAPI启动时，获取当前正在运行的事件循环
            self.main_loop = asyncio.get_running_loop()
            print("--- ✅ 主事件循环已捕获 ---")

        # 3. 初始化ROS节点
        print("[INIT] 准备初始化ROS节点 (rospy.init_node)...")
        try:
            rospy.init_node('gcs_gateway_node', anonymous=True)
            print("[INIT] ROS节点初始化成功！")
        except Exception as e:
            print(f"!!! 初始化ROS节点失败: {e}")
            exit()

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

    # --- 使用最终的、正确的、非阻塞的跨线程推送方式 ---
    def _telemetry_pusher(self):
        while not rospy.is_shutdown():
            # 等待主循环被捕获
            if self.main_loop is None:
                rospy.sleep(0.5)
                continue

            with self.lock:
                state_copy = self.shared_state.copy()

            # 将sio.emit作为一个协程，安全地提交给主线程的事件循环去执行
            print(f"--- 📤 [{time.time()}] [PUSHER] 准备提交emit任务, 数据: {state_copy} ---")
            asyncio.run_coroutine_threadsafe(
                self.sio.emit('telemetry_update', state_copy),
                self.main_loop
            )
            rospy.sleep(0.1)  # 控制推送频率

    def run(self):
        """启动整个网关应用"""
        # 启动ROS事件循环(rospy.spin)在独立的线程中
        ros_thread = threading.Thread(target=lambda: rospy.spin())
        ros_thread.daemon = True
        ros_thread.start()

        # 启动Uvicorn Web服务器
        uvicorn.run(self.main_app, host="0.0.0.0", port=8000)


# 主程序入口
if __name__ == '__main__':
    gateway = GCSGateway()
    gateway.run()