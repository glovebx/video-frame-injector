import json
import os
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict


class FrameInjector:
    def __init__(
        self,
        crf: int = 18,
        preset: str = "medium",
        audio_codec: str = "aac",
        video_codec: str = "libx264",
        keep_audio: bool = True,
        verbose: bool = True,
    ):
        self.crf = crf
        self.preset = preset
        self.audio_codec = audio_codec
        self.video_codec = video_codec
        self.keep_audio = keep_audio
        self.verbose = verbose

    def inject(
        self,
        original_video: str,
        scenes_json: str,
        modified_images_dir: str,
        output_path: str,
    ) -> str:
        original_video = Path(original_video)
        scenes_json = Path(scenes_json)
        modified_images_dir = Path(modified_images_dir)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 读取场景信息并按时间排序
        with open(scenes_json, "r", encoding="utf-8") as f:
            data = json.load(f)
        scenes = sorted(data["scenes"], key=lambda x: x["timestamp"])

        # 获取视频信息
        info = self._get_video_info(str(original_video))
        width, height = info["width"], info["height"]
        fps = info["fps"]
        total_duration = info["duration"]

        # 构建区间列表: (start, end, image_filename)
        intervals = []
        for i, scene in enumerate(scenes):
            start = scene["timestamp"]
            end = scenes[i + 1]["timestamp"] if i + 1 < len(scenes) else total_duration
            if end - start > 0.001:  # 有效区间
                intervals.append((start, end, scene["filename"]))

        if self.verbose:
            print(f"Creating {len(intervals)} segments from scenes")

        temp_dir = tempfile.mkdtemp(prefix="frame_inject_")
        try:
            segments = []
            for idx, (start, end, img_name) in enumerate(intervals):
                img_path = os.path.join(modified_images_dir, img_name)
                if not os.path.exists(img_path):
                    if self.verbose:
                        print(f"Warning: {img_name} not found, skipping {start:.3f}-{end:.3f}")
                    continue

                seg_path = os.path.join(temp_dir, f"segment_{idx:04d}.mp4")
                print(seg_path)

                self._create_image_segment(
                    img_path, str(original_video), start, end,
                    seg_path, width, height, fps
                )
                segments.append(seg_path)

            if not segments:
                raise RuntimeError("No valid image segments created")

            self._concat_segments(segments, str(output_path), temp_dir)
            return str(output_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _get_video_info(self, video_path: str) -> Dict:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,avg_frame_rate",
            "-show_entries", "format=duration",
            "-of", "json", video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        stream = data["streams"][0]
        fps_str = stream.get("avg_frame_rate", "30/1")
        if "/" in fps_str:
            num, den = map(int, fps_str.split("/"))
            fps = num / den if den != 0 else 30.0
        else:
            fps = float(fps_str)
        return {
            "width": stream["width"],
            "height": stream["height"],
            "fps": fps,
            "duration": float(data["format"]["duration"])
        }

    def _create_image_segment(
        self,
        img_path: str,
        video_path: str,
        start: float,
        end: float,
        output: str,
        width: int,
        height: int,
        fps: float,
    ):
        duration = end - start
        # 图片缩放并填充至目标分辨率
        scale_filter = (
            f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black"
        )

        # 输入0: 图片循环; 输入1: 原视频（提取音频）
        cmd = [
            "ffmpeg", "-y",
            "-loop", "1",
            "-i", img_path,
            "-ss", str(start),
            "-t", str(duration),
            "-i", video_path,
            "-filter_complex",
            f"[0:v]{scale_filter}[v];"
            f"[1:a]aresample=48000[a]",
            "-map", "[v]",
            "-map", "[a]",
            "-c:v", self.video_codec,
            "-crf", str(self.crf),
            "-preset", self.preset,
            "-t", str(duration),
            "-r", str(fps),
            "-pix_fmt", "yuv420p",
            "-c:a", self.audio_codec,
            "-b:a", "192k",
            "-shortest",
            output
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)

    def _concat_segments(self, segments: List[str], output_path: str, temp_dir: str):
        concat_list = os.path.join(temp_dir, "concat_list.txt")
        with open(concat_list, "w", encoding="utf-8") as f:
            for seg in segments:
                f.write(f"file '{seg}'\n")

        # 直接重新编码，避免任何时间戳或关键帧问题
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0", "-i", concat_list,
            "-c:v", self.video_codec,
            "-crf", str(self.crf),
            "-preset", self.preset,
            "-c:a", self.audio_codec,
            "-b:a", "192k",
            "-vsync", "cfr",
            "-start_at_zero",
            "-force_key_frames", "expr:eq(n,0)",
            "-movflags", "+faststart",
            output_path
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)

    # def _concat_segments(self, segments: List[str], output_path: str, temp_dir: str):
    #     concat_list = os.path.join(temp_dir, "concat_list.txt")
    #     with open(concat_list, "w", encoding="utf-8") as f:
    #         for seg in segments:
    #             f.write(f"file '{seg}'\n")

    #     # 优先尝试直接复制流（速度最快）
    #     cmd = [
    #         "ffmpeg", "-y",
    #         "-f", "concat",
    #         "-safe", "0",
    #         "-i", concat_list,
    #         "-c", "copy",
    #         "-movflags", "+faststart",
    #         output_path
    #     ]
    #     result = subprocess.run(cmd, capture_output=True, text=True)
    #     if result.returncode != 0:
    #         if self.verbose:
    #             print("Direct concat failed, re-encoding...")
    #         cmd = [
    #             "ffmpeg", "-y",
    #             "-f", "concat",
    #             "-safe", "0",
    #             "-i", concat_list,
    #             "-c:v", self.video_codec,
    #             "-crf", str(self.crf),
    #             "-preset", self.preset,
    #             "-c:a", self.audio_codec,
    #             "-b:a", "192k",
    #             "-movflags", "+faststart",
    #             output_path
    #         ]
    #         subprocess.run(cmd, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 5:
        print("Usage: python frame_injector.py <video.mp4> <scenes.json> <modified_dir> <output.mp4>")
        sys.exit(1)

    injector = FrameInjector(verbose=True)
    injector.inject(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])