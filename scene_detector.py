import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import cv2
import numpy as np


@dataclass
class SceneFrame:
    frame_number: int
    timestamp: float
    filename: str
    change_score: float  # 与上一场景帧的差异度 (0-1, 越大变化越大)


class SceneDetector:
    def __init__(
        self,
        change_threshold: float = 0.08,   # 帧差异阈值（0-1，越小越敏感，推荐 0.05~0.15）
        min_scene_duration: float = 0.5,   # 最短场景时长（秒）
        output_width: int = 1280,
        jpeg_quality: int = 90,
        verbose: bool = True,
    ):
        self.change_threshold = change_threshold
        self.min_scene_duration = min_scene_duration
        self.output_width = output_width
        self.jpeg_quality = jpeg_quality
        self.verbose = verbose

    def detect(self, video_path: str, output_dir: str) -> Tuple[List[SceneFrame], str]:
        video_path = Path(video_path)
        output_dir = Path(output_dir)
        scenes_dir = output_dir / "scenes"
        scenes_dir.mkdir(parents=True, exist_ok=True)

        info = self._get_video_info(str(video_path))
        fps = info["fps"]
        duration = info["duration"]

        if self.verbose:
            print(f"Video: {video_path.name}")
            print(f"Duration: {duration:.2f}s, FPS: {fps:.2f}")

        scenes = self._frame_difference_detection(str(video_path), fps)

        if not scenes:
            scenes = [SceneFrame(0, 0.0, "scene_0000.jpg", 1.0)]

        exported = self._export_frames(str(video_path), scenes, str(scenes_dir))

        json_data = {
            "source_video": video_path.name,
            "duration": duration,
            "fps": fps,
            "change_threshold": self.change_threshold,
            "scenes": [
                {
                    "frame_number": s.frame_number,
                    "timestamp": round(s.timestamp, 3),
                    "filename": s.filename,
                    "change_score": round(s.change_score, 4),
                }
                for s in exported
            ],
        }

        json_path = output_dir / "scenes.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

        if self.verbose:
            print(f"Detected {len(exported)} scenes")
            print(f"JSON saved: {json_path}")
            print(f"Images saved: {scenes_dir}")

        return exported, str(json_path)

    def _get_video_info(self, video_path: str) -> dict:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=avg_frame_rate",
            "-show_entries", "format=duration",
            "-of", "json", video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)

        fps_str = data["streams"][0].get("avg_frame_rate", "30/1")
        if "/" in fps_str:
            num, den = map(int, fps_str.split("/"))
            fps = num / den if den != 0 else 30.0
        else:
            fps = float(fps_str)

        duration = float(data["format"].get("duration", 0))
        return {"fps": fps, "duration": duration}

    def _frame_difference_detection(self, video_path: str, fps: float) -> List[SceneFrame]:
        """
        使用帧差法（Mean Absolute Difference）检测场景变化。
        对文字切换、渐变等非常敏感。
        """
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {video_path}")

        scenes = []
        prev_gray = None
        frame_num = 0
        last_scene_frame = -1

        # 可选：跳帧采样（提高速度，但不影响检测准确性）
        sample_interval = 1  # 1 = 每帧，2 = 隔一帧

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            timestamp = frame_num / fps

            if frame_num % sample_interval == 0:
                # 转为灰度图
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                change_score = 0.0
                is_new_scene = False

                if prev_gray is not None:
                    # 计算平均绝对差 (Mean Absolute Difference)
                    diff = cv2.absdiff(prev_gray, gray)
                    mae = np.mean(diff) / 255.0  # 归一化到 [0,1]
                    change_score = mae

                    # 如果变化超过阈值，则视为新场景
                    if change_score >= self.change_threshold:
                        # 检查最小时间间隔
                        if last_scene_frame == -1 or (frame_num - last_scene_frame) / fps >= self.min_scene_duration:
                            is_new_scene = True
                else:
                    # 第一帧
                    is_new_scene = True
                    change_score = 1.0  # 无前一帧，视为最大变化

                if is_new_scene:
                    scenes.append(
                        SceneFrame(
                            frame_number=frame_num,
                            timestamp=timestamp,
                            filename=f"scene_{len(scenes):04d}.jpg",
                            change_score=change_score,
                        )
                    )
                    prev_gray = gray
                    last_scene_frame = frame_num

                    if self.verbose and len(scenes) > 1:
                        print(f"  Scene {len(scenes)-1} -> {len(scenes)} at {timestamp:.3f}s (change={change_score:.4f})")

            frame_num += 1

            if self.verbose and frame_num % 500 == 0:
                print(f"  Processed {frame_num} frames...")

        cap.release()
        return scenes

    def _export_frames(self, video_path: str, scenes: List[SceneFrame], output_dir: str) -> List[SceneFrame]:
        cap = cv2.VideoCapture(video_path)
        exported = []

        for scene in scenes:
            cap.set(cv2.CAP_PROP_POS_FRAMES, scene.frame_number)
            ret, frame = cap.read()
            if not ret:
                if self.verbose:
                    print(f"Warning: Could not read frame {scene.frame_number}")
                continue

            h, w = frame.shape[:2]
            if w > self.output_width:
                ratio = self.output_width / w
                new_size = (self.output_width, int(h * ratio))
                frame = cv2.resize(frame, new_size, interpolation=cv2.INTER_LANCZOS4)

            out_path = os.path.join(output_dir, scene.filename)
            cv2.imwrite(out_path, frame, [cv2.IMWRITE_JPEG_QUALITY, self.jpeg_quality])
            exported.append(scene)
            if self.verbose:
                print(f"  Exported: {scene.filename} @ {scene.timestamp:.3f}s")

        cap.release()
        return exported


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python scene_detector.py <video.mp4> [output_dir] [change_threshold]")
        print("  change_threshold: 0.05~0.15 (sensitive), 0.2~0.3 (strict)")
        sys.exit(1)

    video = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "./output"
    threshold = float(sys.argv[3]) if len(sys.argv) > 3 else 0.08

    detector = SceneDetector(change_threshold=threshold, verbose=True)
    detector.detect(video, out_dir)