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
        pass  # Phase 4 中实现

    def _setup_ros_communications(self):
        """设置所有ROS订阅者和发布者"""
        pass  # Phase 3 和 4 中实现

    def _start_background_tasks(self):
        """启动所有后台线程"""
        pass  # Phase 3 中实现

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