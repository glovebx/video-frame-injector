import os
import shutil
import traceback
from pathlib import Path
from typing import Optional

from celery import states
from celery.exceptions import Ignore, SoftTimeLimitExceeded

from celery_app import celery_app
from scene_detector import SceneDetector
from frame_injector import FrameInjector


BASE_DIR = Path("./output")
BASE_DIR.mkdir(exist_ok=True)


def _update_job_meta(job_id: str, **kwargs):
    """更新 job 元数据到文件（用于前端轮询）"""
    meta_file = BASE_DIR / job_id / "job_meta.json"
    meta = {}
    if meta_file.exists():
        import json
        with open(meta_file, "r") as f:
            meta = json.load(f)
    
    meta.update(kwargs)
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_file, "w") as f:
        import json
        json.dump(meta, f, indent=2)


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    soft_time_limit=3600 * 2,      # 2小时软限制
    time_limit=3600 * 4,           # 4小时硬限制
    queue="detect",
)
def detect_scenes_task(
    self,
    video_path: str,
    job_id: str,
    threshold: float = 0.35,
    output_width: int = 1280,
):
    """
    异步场景检测任务
    
    状态流转: PENDING -> STARTED -> PROGRESS... -> SUCCESS/FAILED
    """
    job_dir = BASE_DIR / job_id
    video_file = Path(video_path)
    
    try:
        # 更新状态：开始处理
        self.update_state(
            state=states.STARTED,
            meta={
                "step": "initializing",
                "progress": 0,
                "message": "Initializing scene detector...",
            }
        )
        _update_job_meta(
            job_id,
            status="processing",
            step="detect_scenes",
            progress=0,
            message="Initializing...",
        )
        
        # 初始化检测器
        detector = SceneDetector(
            output_width=output_width
        )
        
        # 更新状态：分析视频信息
        self.update_state(
            state="PROGRESS",
            meta={
                "step": "analyzing",
                "progress": 5,
                "message": "Analyzing video metadata...",
            }
        )
        _update_job_meta(job_id, progress=5, message="Analyzing video...")
        
        # 执行检测（这个函数内部会打印进度，我们重定向到 Celery）
        scenes, json_path = detector.detect(str(video_file), str(job_dir))
        
        # 打包图片
        self.update_state(
            state="PROGRESS",
            meta={
                "step": "packaging",
                "progress": 90,
                "message": "Packaging scene images...",
            }
        )
        
        scenes_dir = job_dir / "scenes"
        zip_path = job_dir / "scenes.zip"
        
        import zipfile
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for img_file in scenes_dir.glob("*.jpg"):
                zf.write(img_file, img_file.name)
        
        # 完成
        result = {
            "job_id": job_id,
            "scenes_count": len(scenes),
            "scenes_json": str(job_dir / "scenes.json"),
            "scenes_zip": str(zip_path),
            "video_path": str(video_file),
        }
        
        _update_job_meta(
            job_id,
            status="completed",
            step="detect_scenes",
            progress=100,
            message=f"Detected {len(scenes)} scenes",
            result=result,
        )
        
        return result
        
    except SoftTimeLimitExceeded:
        _update_job_meta(
            job_id,
            status="failed",
            error="Task timed out (2 hours)",
        )
        self.update_state(
            state=states.FAILURE,
            meta={"error": "Processing timed out"},
        )
        raise Ignore()
        
    except Exception as exc:
        error_msg = str(exc)
        traceback_str = traceback.format_exc()
        
        _update_job_meta(
            job_id,
            status="failed",
            error=error_msg,
            traceback=traceback_str,
        )
        
        # 重试逻辑
        if self.request.retries < self.max_retries:
            self.retry(exc=exc, countdown=60 * (self.request.retries + 1))
        
        self.update_state(
            state=states.FAILURE,
            meta={"error": error_msg, "traceback": traceback_str},
        )
        raise Ignore()


@celery_app.task(
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=3600 * 3,      # 3小时（视频合并可能很慢）
    time_limit=3600 * 6,           # 6小时
    queue="inject",
)
def inject_frames_task(
    self,
    job_id: str,
    original_video: str,
    scenes_json: str,
    modified_images_dir: str,
    output_filename: Optional[str] = None,
):
    """
    异步帧注入任务
    """
    job_dir = BASE_DIR / job_id
    
    try:
        self.update_state(
            state=states.STARTED,
            meta={
                "step": "preparing",
                "progress": 0,
                "message": "Preparing frame injection...",
            }
        )
        _update_job_meta(
            job_id,
            status="processing",
            step="inject_frames",
            progress=0,
            message="Preparing injection...",
        )
        
        # 验证输入
        if not Path(modified_images_dir).exists():
            raise FileNotFoundError(f"Modified images not found: {modified_images_dir}")
        
        result_dir = job_dir / "results"
        result_dir.mkdir(exist_ok=True)
        
        if not output_filename:
            output_filename = f"final_{Path(original_video).name}"
        output_path = result_dir / output_filename
        
        # 执行注入
        injector = FrameInjector(crf=18, preset="medium")
        
        self.update_state(
            state="PROGRESS",
            meta={
                "step": "injecting",
                "progress": 10,
                "message": "Merging frames into video...",
            }
        )
        _update_job_meta(job_id, progress=10, message="Merging frames...")
        
        injector.inject(
            original_video,
            scenes_json,
            modified_images_dir,
            str(output_path),
        )
        
        # 验证输出
        if not output_path.exists():
            raise RuntimeError("Output video was not created")
        
        file_size = output_path.stat().st_size
        
        result = {
            "job_id": job_id,
            "output_video": str(output_path),
            "filename": output_filename,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
        }
        
        _update_job_meta(
            job_id,
            status="completed",
            step="inject_frames",
            progress=100,
            message="Video generated successfully",
            result=result,
        )
        
        return result
        
    except SoftTimeLimitExceeded:
        _update_job_meta(
            job_id,
            status="failed",
            error="Injection timed out (3 hours)",
        )
        raise Ignore()
        
    except Exception as exc:
        error_msg = str(exc)
        traceback_str = traceback.format_exc()
        
        _update_job_meta(
            job_id,
            status="failed",
            error=error_msg,
            traceback=traceback_str,
        )
        
        if self.request.retries < self.max_retries:
            self.retry(exc=exc, countdown=30)
        
        self.update_state(
            state=states.FAILURE,
            meta={"error": error_msg},
        )
        raise Ignore()


@celery_app.task
def cleanup_old_jobs(max_age_hours: int = 168):
    """定时任务：清理旧 job 文件"""
    import time
    from datetime import datetime, timedelta
    
    cutoff = time.time() - (max_age_hours * 3600)
    cleaned = 0
    
    for job_dir in BASE_DIR.iterdir():
        if not job_dir.is_dir():
            continue
        
        # 检查最后修改时间
        mtime = job_dir.stat().st_mtime
        if mtime < cutoff:
            shutil.rmtree(job_dir)
            cleaned += 1
    
    return {"cleaned_jobs": cleaned}