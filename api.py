import os
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from scene_detector import SceneDetector
from frame_injector import FrameInjector

MAX_VIDEO_SIZE = 20 * 1024 * 1024  # 20 MB

# 方法2：使用配置模块
from config import settings

app = FastAPI(title="Meta Video Scene Processor")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有请求头
)

# 存储目录
BASE_DIR = Path("./output")
BASE_DIR.mkdir(exist_ok=True)


class ProcessResponse(BaseModel):
    job_id: str
    scenes_json: str
    images_archive: Optional[str] = None
    message: str


class InjectRequest(BaseModel):
    job_id: str
    # 或者提供路径
    original_video: Optional[str] = None
    scenes_json: Optional[str] = None
    modified_images_dir: Optional[str] = None


@app.post("/videoinjector/api/v1/detect-scenes", response_model=ProcessResponse)
async def detect_scenes(
    background_tasks: BackgroundTasks,
    video: UploadFile = File(...),
    output_width: int = Form(1280)
):
    """
    接口 1: 接收视频，检测场景切换帧，导出图片和 JSON
    
    - 上传视频文件
    - 返回 job_id，用于后续注入
    - 图片和 JSON 打包在 job 目录中
    """

    hasSizeChecked = False
    # 检查文件大小（通过 Content-Length 或先读取一部分）
    # 方法1：使用 Content-Length 头（可靠）
    if video.size:
        hasSizeChecked = True
        if video.size > MAX_VIDEO_SIZE:
            error_response = ProcessResponse(
                job_id="",
                scenes_json="",
                images_archive=None,
                message=f"Video file size bytes exceeds limit of {MAX_VIDEO_SIZE} bytes (20MB)"
            )
            return error_response
    
    job_id = str(uuid.uuid4())[:8]
    job_dir = BASE_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存上传的视频
    video_path = job_dir / video.filename
    with open(video_path, "wb") as f:
        content = await video.read()
        if not hasSizeChecked and len(content) > MAX_VIDEO_SIZE:
            error_response = ProcessResponse(
                job_id="",
                scenes_json="",
                images_archive=None,
                message=f"Video file size bytes exceeds limit of {MAX_VIDEO_SIZE} bytes (20MB)"
            )
            return error_response
        
        f.write(content)
    
    # 异步处理（大视频可能耗时较长）
    detector = SceneDetector(
        output_width=output_width
    )
    
    # 同步处理（生产环境应改为 Celery/Redis 队列）
    scenes, json_path = detector.detect(str(video_path), str(job_dir))
    
    # 打包图片为 zip 方便下载
    scenes_dir = job_dir / "scenes"
    zip_path = job_dir / "scenes.zip"
    
    import zipfile
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for img_file in scenes_dir.glob("*.jpg"):
            zf.write(img_file, img_file.name)
    
    # 清理原视频（可选，保留用于后续注入）
    # os.remove(video_path)
    
    return ProcessResponse(
        job_id=job_id,
        scenes_json=f"/api/v1/jobs/{job_id}/scenes.json",
        images_archive=f"/api/v1/jobs/{job_id}/scenes.zip",
        message=f"Detected {len(scenes)} scenes. Images ready for modification."
    )


@app.post("/videoinjector/api/v1/inject-frames")
async def inject_frames(
    job_id: str = Form(...),
    modified_images: Optional[UploadFile] = File(None),  # 上传 zip 或单文件
    # 或者从指定目录读取（如果美化程序直接写到磁盘）
    modified_dir: Optional[str] = Form(None)
):
    """
    接口 2: 将美化后的图片注入原视频，生成新视频
    
    两种方式提供图片：
    1. 上传 zip 文件（包含所有修改后的图片，文件名与 JSON 一致）
    2. 指定本地目录路径（如果美化程序在同一服务器运行）
    """
    job_dir = BASE_DIR / job_id
    if not job_dir.exists():
        return JSONResponse({"error": "Job not found"}, status_code=404)
    
    original_video = list(job_dir.glob("*.mp4"))
    if not original_video:
        return JSONResponse({"error": "Original video not found"}, status_code=404)
    
    original_video = original_video[0]
    scenes_json = job_dir / "scenes.json"
    
    # 确定修改后的图片目录
    if modified_images:
        # 解压上传的 zip
        mod_dir = job_dir / "modified"
        mod_dir.mkdir(exist_ok=True)
        
        # 读取上传的 ZIP 文件内容到内存
        content = await modified_images.read()          # 异步读取全部字节
        from io import BytesIO
        zip_bytes = BytesIO(content)                    # 包装为 BytesIO
                
        import zipfile
        with zipfile.ZipFile(zip_bytes, "r") as zf:
            zf.extractall(mod_dir)
    elif modified_dir and Path(modified_dir).exists():
        mod_dir = Path(mod_dir)
    else:
        # 默认尝试 job_dir/modified
        mod_dir = job_dir / "modified"
        if not mod_dir.exists():
            return JSONResponse(
                {"error": "Modified images not found. Upload zip or specify directory."},
                status_code=400
            )
    
    # 输出路径
    result_dir = job_dir / "results"
    result_dir.mkdir(exist_ok=True)
    output_video = result_dir / f"final_{original_video.name}"
    
    # 执行注入
    injector = FrameInjector(crf=18, preset="slow")  # slow = 更好压缩
    injector.inject(
        str(original_video),
        str(scenes_json),
        str(mod_dir),
        str(output_video)
    )
    
    return {
        "job_id": job_id,
        "output_video": f"/api/v1/jobs/{job_id}/results/{output_video.name}",
        "message": "Video generated successfully"
    }


@app.get("/videoinjector/api/v1/jobs/{job_id}/{path:path}")
async def download_file(job_id: str, path: str):
    """下载 job 中的文件"""
    file_path = BASE_DIR / job_id / path
    if not file_path.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    
    return FileResponse(
        file_path,
        filename=file_path.name,
        media_type="application/octet-stream"
    )


@app.get("/videoinjector/api/v1/jobs/{job_id}/status")
async def job_status(job_id: str):
    """查询 job 状态"""
    job_dir = BASE_DIR / job_id
    if not job_dir.exists():
        return {"status": "not_found"}
    
    files = {
        "scenes_json": (job_dir / "scenes.json").exists(),
        "scenes_zip": (job_dir / "scenes.zip").exists(),
        "modified_dir": (job_dir / "modified").exists(),
        "result_video": list((job_dir / "results").glob("*.mp4")) if (job_dir / "results").exists() else []
    }
    
    return {
        "job_id": job_id,
        "status": "completed" if files["result_video"] else "processing",
        "files": files
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8078)