import hashlib
import json
from pathlib import Path


def compute_file_hash(filepath: str, algorithm="blake2b") -> str:
    """计算文件哈希，用于缓存/去重"""
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def validate_scenes_json(data: dict) -> bool:
    """验证场景 JSON 格式"""
    required = ["source_video", "fps", "scenes"]
    if not all(k in data for k in required):
        return False
    
    for scene in data["scenes"]:
        if not all(k in scene for k in ["frame_number", "timestamp", "filename"]):
            return False
    
    return True


def merge_json_updates(base_json: str, updates: dict, output_path: str):
    """合并 JSON 更新（如外部程序添加了图片处理参数）"""
    with open(base_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    data.update(updates)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)