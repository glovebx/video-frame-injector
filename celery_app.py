from celery import Celery
from kombu import Queue

# 使用 Redis 作为 broker 和 backend
# 生产环境建议用 RabbitMQ 做 broker，Redis 做 backend
celery_app = Celery(
    "video_processor",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=["tasks"]  # 导入任务模块
)

# 队列配置：不同优先级
celery_app.conf.task_queues = (
    Queue("detect", routing_key="detect"),
    Queue("inject", routing_key="inject"),
)

celery_app.conf.task_routes = {
    "tasks.detect_scenes": {"queue": "detect", "routing_key": "detect"},
    "tasks.inject_frames": {"queue": "inject", "routing_key": "inject"},
}

# 任务执行设置
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
    # 任务结果过期时间
    result_expires=3600 * 24 * 7,  # 7天
    # 防止任务丢失
    task_track_started=True,
    # 最大重试次数
    task_default_retry=3,
    # Worker 并发（视频处理是 CPU 密集型，建议设为 CPU 核心数）
    worker_concurrency=2,
    # 每个 worker 同时处理的任务数（视频处理占内存，设为1避免 OOM）
    worker_prefetch_multiplier=1,
)