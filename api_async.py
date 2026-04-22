import os
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from celery.result import AsyncResult
from celery import states

from celery_app import celery_app
from tasks import detect_scenes_task, inject_frames_task, BASE_DIR


app = FastAPI(title="Video Scene Processor (Async)")

# 启动 Celery Worker 的命令：
# celery -A celery_app worker -Q detect,inject -l info --concurrency=2


@app.post("/api/v1/detect-scenes")
async def detect_scenes(
    video: UploadFile = File(...),
    threshold: float = Form(0.35),
    output_width: int = Form(1280),
    webhook_url: Optional[str] = Form(None),  # 完成后回调
):
    """
    接口 1 (异步): 提交场景检测任务
    
    立即返回 job_id，通过轮询 /jobs/{id}/status 或 webhook 获取结果
    """
    job_id = str(uuid.uuid4())[:8]
    job_dir = BASE_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存上传的视频
    video_path = job_dir / video.filename
    with open(video_path, "wb") as f:
        content = await video.read()
        f.write(content)
    
    # 提交异步任务
    task = detect_scenes_task.delay(
        video_path=str(video_path),
        job_id=job_id,
        threshold=threshold,
        output_width=output_width,
    )
    
    return {
        "job_id": job_id,
        "task_id": task.id,
        "status": "queued",
        "message": "Scene detection task queued. Poll /api/v1/jobs/{job_id}/status for progress.",
        "poll_url": f"/api/v1/jobs/{job_id}/status",
    }


@app.post("/api/v1/inject-frames")
async def inject_frames(
    job_id: str = Form(...),
    modified_images: Optional[UploadFile] = File(None),
    modified_dir: Optional[str] = Form(None),
    output_filename: Optional[str] = Form(None),
):
    """
    接口 2 (异步): 提交帧注入任务
    
    需要 job_id 来自第一步的检测任务
    """
    job_dir = BASE_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")
    
    # 查找原始视频
    original_videos = list(job_dir.glob("*.mp4")) + list(job_dir.glob("*.mov")) + list(job_dir.glob("*.avi"))
    if not original_videos:
        raise HTTPException(status_code=404, detail="Original video not found")
    
    original_video = original_videos[0]
    scenes_json = job_dir / "scenes.json"
    
    if not scenes_json.exists():
        raise HTTPException(status_code=400, detail="Scenes JSON not found. Run detect-scenes first.")
    
    # 确定修改后的图片目录
    if modified_images:
        mod_dir = job_dir / "modified_uploaded"
        mod_dir.mkdir(exist_ok=True)
        
        import zipfile
        with zipfile.ZipFile(modified_images.file, "r") as zf:
            zf.extractall(mod_dir)
            
    elif modified_dir and Path(modified_dir).exists():
        mod_dir = Path(modified_dir)
    else:
        # 默认尝试 job_dir/modified
        mod_dir = job_dir / "modified"
        if not mod_dir.exists():
            raise HTTPException(
                status_code=400,
                detail="Modified images not found. Upload zip or specify directory."
            )
    
    # 提交异步任务
    task = inject_frames_task.delay(
        job_id=job_id,
        original_video=str(original_video),
        scenes_json=str(scenes_json),
        modified_images_dir=str(mod_dir),
        output_filename=output_filename,
    )
    
    return {
        "job_id": job_id,
        "task_id": task.id,
        "status": "queued",
        "message": "Frame injection task queued.",
        "poll_url": f"/api/v1/jobs/{job_id}/status",
    }


@app.get("/api/v1/jobs/{job_id}/status")
async def job_status(job_id: str):
    """
    查询任务状态（轮询接口）
    
    返回实时进度和结果
    """
    job_dir = BASE_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")
    
    meta_file = job_dir / "job_meta.json"
    meta = {}
    if meta_file.exists():
        import json
        with open(meta_file, "r") as f:
            meta = json.load(f)
    
    # 也检查 Celery 任务状态（如果知道 task_id）
    # 这里简化，主要依靠 meta 文件
    
    status = meta.get("status", "unknown")
    step = meta.get("step")
    progress = meta.get("progress", 0)
    result = meta.get("result", {})
    error = meta.get("error")
    
    response = {
        "job_id": job_id,
        "status": status,
        "step": step,
        "progress": progress,
    }
    
    if error:
        response["error"] = error
    
    if status == "completed" and result:
        response["result"] = result
        # 添加下载链接
        if "scenes_zip" in result:
            response["download"] = {
                "scenes_json": f"/api/v1/jobs/{job_id}/scenes.json",
                "scenes_zip": f"/api/v1/jobs/{job_id}/scenes.zip",
            }
        if "output_video" in result:
            response["download"] = {
                "output_video": f"/api/v1/jobs/{job_id}/results/{result['filename']}",
            }
    
    return response


@app.get("/api/v1/tasks/{task_id}")
async def task_status(task_id: str):
    """
    直接查询 Celery 任务状态（需要 task_id）
    """
    task_result = AsyncResult(task_id, app=celery_app)
    
    response = {
        "task_id": task_id,
        "state": task_result.state,
    }
    
    if task_result.state == states.PENDING:
        response["message"] = "Task is waiting to be processed"
        
    elif task_result.state == states.STARTED:
        info = task_result.info or {}
        response.update({
            "step": info.get("step"),
            "progress": info.get("progress"),
            "message": info.get("message"),
        })
        
    elif task_result.state == "PROGRESS":
        info = task_result.info or {}
        response.update({
            "step": info.get("step"),
            "progress": info.get("progress"),
            "message": info.get("message"),
        })
        
    elif task_result.state == states.SUCCESS:
        response["result"] = task_result.result
        
    elif task_result.state == states.FAILURE:
        response["error"] = str(task_result.info)
    
    return response


@app.get("/api/v1/jobs/{job_id}/{path:path}")
async def download_file(job_id: str, path: str):
    """下载 job 中的文件"""
    file_path = BASE_DIR / job_id / path
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        file_path,
        filename=file_path.name,
        media_type="application/octet-stream"
    )


@app.delete("/api/v1/jobs/{job_id}")
async def delete_job(job_id: str):
    """删除 job 及其所有文件"""
    import shutil
    
    job_dir = BASE_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")
    
    shutil.rmtree(job_dir)
    return {"message": f"Job {job_id} deleted"}


@app.get("/api/v1/health")
async def health_check():
    """健康检查"""
    # 检查 Redis 连接
    try:
        celery_app.backend.client.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"
    
    return {
        "status": "ok",
        "redis": redis_status,
        "storage_path": str(BASE_DIR),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)