#!/usr/bin/env python3
"""
Video Processor Service Manager
统一管理 Redis、Celery Workers、API 服务的启动/停止/状态查看
"""
import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set


# 颜色输出
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def color(text: str, color_code: str) -> str:
    return f"{color_code}{text}{Colors.END}"


def ok(text: str) -> str:
    return color(f"✓ {text}", Colors.GREEN)


def err(text: str) -> str:
    return color(f"✗ {text}", Colors.RED)


def warn(text: str) -> str:
    return color(f"⚠ {text}", Colors.YELLOW)


def info(text: str) -> str:
    return color(f"ℹ {text}", Colors.BLUE)


# PID 文件目录
PID_DIR = Path("./pids")
PID_DIR.mkdir(exist_ok=True)


@dataclass
class ServiceConfig:
    name: str
    pid_file: Path
    log_file: Path
    start_cmd: List[str]
    stop_signal: int = signal.SIGTERM
    description: str = ""


# 服务配置
SERVICES: Dict[str, ServiceConfig] = {
    "redis": ServiceConfig(
        name="Redis",
        pid_file=PID_DIR / "redis.pid",
        log_file=Path("./logs/redis.log"),
        start_cmd=["redis-server", "--daemonize", "yes", "--pidfile", str(PID_DIR / "redis.pid")],
        description="Redis 消息队列 (Broker & Backend)",
    ),
    "celery-detect": ServiceConfig(
        name="Celery Detect Worker",
        pid_file=PID_DIR / "celery-detect.pid",
        log_file=Path("./logs/celery-detect.log"),
        start_cmd=[
            "celery", "-A", "celery_app", "worker",
            "-Q", "detect",
            "-n", "detect_worker@%h",
            "-l", "info",
            "--concurrency=2",
            "--pidfile", str(PID_DIR / "celery-detect.pid"),
            "-f", str(Path("./logs/celery-detect.log")),
        ],
        description="场景检测任务 Worker (CPU 密集型)",
    ),
    "celery-inject": ServiceConfig(
        name="Celery Inject Worker",
        pid_file=PID_DIR / "celery-inject.pid",
        log_file=Path("./logs/celery-inject.log"),
        start_cmd=[
            "celery", "-A", "celery_app", "worker",
            "-Q", "inject",
            "-n", "inject_worker@%h",
            "-l", "info",
            "--concurrency=1",
            "--pidfile", str(PID_DIR / "celery-inject.pid"),
            "-f", str(Path("./logs/celery-inject.log")),
        ],
        description="帧注入任务 Worker (内存敏感型)",
    ),
    "celery-beat": ServiceConfig(
        name="Celery Beat Scheduler",
        pid_file=PID_DIR / "celery-beat.pid",
        log_file=Path("./logs/celery-beat.log"),
        start_cmd=[
            "celery", "-A", "celery_app", "beat",
            "-l", "info",
            "--pidfile", str(PID_DIR / "celery-beat.pid"),
            "-f", str(Path("./logs/celery-beat.log")),
            "--scheduler", "celery.beat.PersistentScheduler",
        ],
        description="定时任务调度器 (清理旧任务等)",
    ),
    "flower": ServiceConfig(
        name="Flower Monitor",
        pid_file=PID_DIR / "flower.pid",
        log_file=Path("./logs/flower.log"),
        start_cmd=[
            "celery", "-A", "celery_app", "flower",
            "--port=5555",
            "--pidfile", str(PID_DIR / "flower.pid"),
            "-f", str(Path("./logs/flower.log")),
        ],
        description="Celery 监控面板 (http://localhost:5555)",
    ),
    "api": ServiceConfig(
        name="FastAPI Server",
        pid_file=PID_DIR / "api.pid",
        log_file=Path("./logs/api.log"),
        start_cmd=[
            "uvicorn", "api:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--workers", "2",
        ],
        description="FastAPI HTTP 服务 (端口 8000)",
    ),
}


class ServiceManager:
    def __init__(self):
        self.logs_dir = Path("./logs")
        self.logs_dir.mkdir(exist_ok=True)
    
    def is_running(self, service_name: str) -> bool:
        """检查服务是否运行中"""
        config = SERVICES[service_name]
        
        # 方法1: 检查 PID 文件
        if config.pid_file.exists():
            try:
                with open(config.pid_file, "r") as f:
                    pid = int(f.read().strip())
                # 检查进程是否存在
                os.kill(pid, 0)
                return True
            except (ValueError, OSError, ProcessLookupError):
                # PID 文件存在但进程已死，清理
                config.pid_file.unlink(missing_ok=True)
        
        # 方法2: 对 Redis 特殊处理（它自己管理 PID）
        if service_name == "redis":
            try:
                result = subprocess.run(
                    ["redis-cli", "ping"],
                    capture_output=True,
                    timeout=2,
                )
                return result.returncode == 0 and b"PONG" in result.stdout
            except (subprocess.TimeoutExpired, FileNotFoundError):
                return False
        
        # 方法3: 对 API 特殊处理（检查端口）
        if service_name == "api":
            try:
                import socket
                with socket.create_connection(("127.0.0.1", 8000), timeout=1):
                    return True
            except (socket.timeout, ConnectionRefusedError):
                return False
        
        # 方法4: 对 Flower 检查端口
        if service_name == "flower":
            try:
                import socket
                with socket.create_connection(("127.0.0.1", 5555), timeout=1):
                    return True
            except (socket.timeout, ConnectionRefusedError):
                return False
        
        return False
    
    def start(self, service_name: str, foreground: bool = False) -> bool:
        """启动单个服务"""
        config = SERVICES[service_name]
        
        if self.is_running(service_name):
            print(ok(f"{config.name} 已在运行 (PID: {self._get_pid(service_name)})"))
            return True
        
        print(info(f"正在启动 {config.name}..."))
        
        try:
            if foreground:
                # 前台运行（调试用）
                print(color(f"  命令: {' '.join(config.start_cmd)}", Colors.CYAN))
                process = subprocess.Popen(
                    config.start_cmd,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                )
                print(ok(f"{config.name} 已前台启动 (PID: {process.pid})"))
                return True
            else:
                # 后台运行
                log_file = open(config.log_file, "a")
                
                # 对 Redis 特殊处理：它已经 daemonize
                if service_name == "redis":
                    result = subprocess.run(
                        config.start_cmd,
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        print(err(f"Redis 启动失败: {result.stderr}"))
                        return False
                    # 等待 Redis 就绪
                    time.sleep(0.5)
                else:
                    process = subprocess.Popen(
                        config.start_cmd,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,  # 脱离终端会话
                    )
                    # 写入 PID 文件
                    with open(config.pid_file, "w") as f:
                        f.write(str(process.pid))
                    time.sleep(1)  # 等待启动
                
                if self.is_running(service_name):
                    pid = self._get_pid(service_name) or "unknown"
                    print(ok(f"{config.name} 已启动 (PID: {pid})"))
                    print(info(f"  日志: {config.log_file}"))
                    if config.description:
                        print(info(f"  说明: {config.description}"))
                    return True
                else:
                    print(err(f"{config.name} 启动失败，请检查日志"))
                    return False
                    
        except FileNotFoundError as e:
            print(err(f"启动失败: 找不到命令 {e.filename}"))
            print(warn(f"  请确保已安装: {self._get_install_hint(service_name)}"))
            return False
        except Exception as e:
            print(err(f"启动失败: {e}"))
            return False
    
    def stop(self, service_name: str, force: bool = False) -> bool:
        """停止单个服务"""
        config = SERVICES[service_name]
        
        if not self.is_running(service_name):
            print(warn(f"{config.name} 未运行"))
            # 清理残留 PID 文件
            config.pid_file.unlink(missing_ok=True)
            return True
        
        pid = self._get_pid(service_name)
        print(info(f"正在停止 {config.name} (PID: {pid})..."))
        
        try:
            if pid:
                sig = signal.SIGKILL if force else config.stop_signal
                os.kill(pid, sig)
                
                # 等待进程退出
                for _ in range(30):  # 最多等 3 秒
                    if not self.is_running(service_name):
                        break
                    time.sleep(0.1)
                
                if self.is_running(service_name) and not force:
                    print(warn(f"{config.name} 未响应，尝试强制停止..."))
                    return self.stop(service_name, force=True)
            else:
                # 找不到 PID，尝试 pkill
                subprocess.run(["pkill", "-f", " ".join(config.start_cmd[:3])], check=False)
            
            # 清理 PID 文件
            config.pid_file.unlink(missing_ok=True)
            
            if not self.is_running(service_name):
                print(ok(f"{config.name} 已停止"))
                return True
            else:
                print(err(f"{config.name} 停止失败"))
                return False
                
        except ProcessLookupError:
            # 进程已不存在
            config.pid_file.unlink(missing_ok=True)
            print(ok(f"{config.name} 已停止"))
            return True
        except Exception as e:
            print(err(f"停止失败: {e}"))
            return False
    
    def restart(self, service_name: str) -> bool:
        """重启服务"""
        print(color(f"\n{BOLD}重启 {SERVICES[service_name].name}{Colors.END}", Colors.YELLOW))
        self.stop(service_name)
        time.sleep(1)
        return self.start(service_name)
    
    def status(self, service_name: str) -> None:
        """查看单个服务状态"""
        config = SERVICES[service_name]
        running = self.is_running(service_name)
        pid = self._get_pid(service_name)
        
        status_text = ok("运行中") if running else err("已停止")
        pid_text = f" (PID: {pid})" if pid and running else ""
        
        print(f"  {config.name:20} {status_text}{pid_text}")
        if config.description:
            print(f"    {color(config.description, Colors.CYAN)}")
        
        # 显示日志最后几行
        if running and config.log_file.exists():
            try:
                with open(config.log_file, "r") as f:
                    lines = f.readlines()
                    if lines:
                        print(f"    日志: ...{lines[-1].strip()[-80:]}")
            except:
                pass
    
    def _get_pid(self, service_name: str) -> Optional[int]:
        """获取服务 PID"""
        config = SERVICES[service_name]
        
        if config.pid_file.exists():
            try:
                with open(config.pid_file, "r") as f:
                    return int(f.read().strip())
            except:
                pass
        
        # 备用：通过 ps 查找
        try:
            result = subprocess.run(
                ["pgrep", "-f", " ".join(config.start_cmd[:3])],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return int(result.stdout.strip().split("\n")[0])
        except:
            pass
        
        return None
    
    def _get_install_hint(self, service_name: str) -> str:
        """获取安装提示"""
        hints = {
            "redis": "brew install redis / apt-get install redis-server",
            "celery-detect": "pip install celery[redis]",
            "celery-inject": "pip install celery[redis]",
            "celery-beat": "pip install celery[redis]",
            "flower": "pip install flower",
            "api": "pip install uvicorn fastapi",
        }
        return hints.get(service_name, "pip install -r requirements.txt")
    
    def start_all(self, exclude: Optional[Set[str]] = None) -> None:
        """启动所有服务"""
        exclude = exclude or set()
        services = [s for s in SERVICES.keys() if s not in exclude]
        
        print(color(f"\n{Colors.BOLD}启动所有服务...{Colors.END}", Colors.YELLOW))
        
        success = []
        failed = []
        
        for name in services:
            if self.start(name):
                success.append(name)
            else:
                failed.append(name)
            time.sleep(0.5)
        
        print(f"\n{ok(f'成功: {len(success)}')}")
        if failed:
            print(f"{err(f'失败: {len(failed)}')} {failed}")
        
        self._print_access_info()
    
    def stop_all(self, include: Optional[Set[str]] = None) -> None:
        """停止所有服务"""
        include = include or set(SERVICES.keys())
        # 按依赖反向停止：api -> workers -> redis
        order = ["api", "flower", "celery-beat", "celery-inject", "celery-detect", "redis"]
        
        print(color(f"\n{Colors.BOLD}停止所有服务...{Colors.END}", Colors.YELLOW))
        
        for name in order:
            if name in include:
                self.stop(name)
                time.sleep(0.3)
    
    def status_all(self) -> None:
        """查看所有服务状态"""
        print(color(f"\n{Colors.BOLD}服务状态{Colors.END}", Colors.YELLOW))
        print("-" * 50)
        
        running = 0
        for name in SERVICES.keys():
            self.status(name)
            if self.is_running(name):
                running += 1
        
        print("-" * 50)
        total = len(SERVICES)
        print(f"总计: {ok(f'{running} 运行')} / {total} 服务")
        
        if running > 0:
            self._print_access_info()
    
    def _print_access_info(self) -> None:
        """打印访问信息"""
        print(f"\n{Colors.BOLD}访问地址:{Colors.END}")
        if self.is_running("api"):
            print(f"  API 服务:    {color('http://localhost:8000', Colors.GREEN)}")
            print(f"  API 文档:    {color('http://localhost:8000/docs', Colors.GREEN)}")
        if self.is_running("flower"):
            print(f"  监控面板:    {color('http://localhost:5555', Colors.GREEN)}")
        if self.is_running("redis"):
            print(f"  Redis:       {color('redis://localhost:6379', Colors.GREEN)}")


def create_parser() -> argparse.ArgumentParser:
    """创建命令行解析器"""
    parser = argparse.ArgumentParser(
        prog="manager",
        description="Video Processor 服务管理器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python manager.py start              # 启动所有服务
  python manager.py start redis api    # 只启动 Redis 和 API
  python manager.py stop               # 停止所有服务
  python manager.py restart api          # 重启 API
  python manager.py status             # 查看所有状态
  python manager.py logs celery-detect # 查看检测 Worker 日志
  python manager.py run api            # 前台运行 API（调试用）
        """,
    )
    
    parser.add_argument(
        "action",
        choices=["start", "stop", "restart", "status", "logs", "run"],
        help="操作命令",
    )
    
    parser.add_argument(
        "services",
        nargs="*",
        choices=list(SERVICES.keys()) + ["all", "workers"],
        default=["all"],
        help="要操作的服务名称 (默认: all)",
    )
    
    parser.add_argument(
        "-f", "--foreground",
        action="store_true",
        help="前台运行（仅对 run 命令有效）",
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="详细输出",
    )
    
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    
    manager = ServiceManager()
    
    # 解析服务列表
    requested = set()
    for s in args.services:
        if s == "all":
            requested = set(SERVICES.keys())
            break
        elif s == "workers":
            requested.update(["celery-detect", "celery-inject", "celery-beat"])
        else:
            requested.add(s)
    
    # 执行操作
    if args.action == "start":
        if "all" in args.services or len(args.services) == 0 or "all" in requested:
            manager.start_all(exclude=requested - set(SERVICES.keys()))
        else:
            for name in requested:
                manager.start(name)
                time.sleep(0.3)
    
    elif args.action == "stop":
        if "all" in args.services or len(args.services) == 0 or "all" in requested:
            manager.stop_all(include=requested)
        else:
            for name in requested:
                manager.stop(name)
                time.sleep(0.2)
    
    elif args.action == "restart":
        for name in requested:
            manager.restart(name)
            time.sleep(0.5)
    
    elif args.action == "status":
        if len(requested) == 1:
            name = list(requested)[0]
            manager.status(name)
        else:
            manager.status_all()
    
    elif args.action == "logs":
        for name in requested:
            config = SERVICES[name]
            print(color(f"\n{config.name} 日志 ({config.log_file}):", Colors.YELLOW))
            print("-" * 50)
            if config.log_file.exists():
                try:
                    with open(config.log_file, "r") as f:
                        lines = f.readlines()
                        # 显示最后 50 行
                        for line in lines[-50:]:
                            print(line.rstrip())
                except Exception as e:
                    print(err(f"读取日志失败: {e}"))
            else:
                print(warn("日志文件不存在"))
    
    elif args.action == "run":
        # 前台运行单个服务（调试用）
        for name in requested:
            manager.start(name, foreground=True)
            # 前台运行会阻塞，所以只运行第一个
            break


if __name__ == "__main__":
    main()