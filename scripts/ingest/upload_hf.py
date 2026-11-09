#!/usr/bin/env python3
"""
Hugging Face Dataset Backup Uploader for ApexStream
Uses official huggingface_hub to commit and upload video files
to the target Hugging Face dataset repository preserving file hierarchy:
  apexstream/movies/{Title}.{Year}/{Filename}
  apexstream/series/{Title}.{Year}/Season {S}/{Filename}
"""
import os
import sys
import json
import time

def ensure_huggingface_hub():
    try:
        import huggingface_hub
        return
    except ImportError:
        print("[HuggingFace] huggingface_hub not found in Python environment. Installing via pip...", flush=True)
        import subprocess
        cmd = [sys.executable, "-m", "pip", "install", "--break-system-packages", "--ignore-installed", "--no-cache-dir", "huggingface_hub"]
        subprocess.check_call(cmd)
        print("[HuggingFace] huggingface_hub installed successfully.", flush=True)

def main():
    file_path = os.environ.get("FILE_PATH")
    hf_token = os.environ.get("HF_TOKEN")
    hf_repo = os.environ.get("HF_REPO")
    hf_target_path = os.environ.get("HF_TARGET_PATH")
    job_id = os.environ.get("JOB_ID")

    if not hf_token or not hf_repo or not hf_target_path:
        print("[HuggingFace] HF_TOKEN, HF_REPO, or HF_TARGET_PATH not set. Skipping Hugging Face backup.", flush=True)
        sys.exit(0)

    if not file_path or not os.path.exists(file_path):
        print(f"[HuggingFace] Error: Local file does not exist: {file_path}", file=sys.stderr, flush=True)
        sys.exit(1)

    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    clean_path = hf_target_path.lstrip("/").replace("\\", "/")
    filename = os.path.basename(file_path)

    print(f"[HuggingFace] Initiating backup for {clean_path} ({file_size_mb:.1f} MB) to repo: {hf_repo}...", flush=True)

    ensure_huggingface_hub()
    from huggingface_hub import HfApi, create_repo, upload_file

    # Ensure target repository exists on Hugging Face (datasets)
    try:
        create_repo(
            repo_id=hf_repo,
            repo_type="dataset",
            token=hf_token,
            exist_ok=True,
            private=True
        )
    except Exception as e:
        # Ignore if repo already exists or user has upload/write access without repo-creation permissions
        print(f"[HuggingFace] Repo precheck info: {e}", flush=True)

    max_attempts = 3
    last_err = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[HuggingFace] Upload Attempt {attempt}/{max_attempts} via commit endpoint...", flush=True)
            commit_info = upload_file(
                path_or_fileobj=file_path,
                path_in_repo=clean_path,
                repo_id=hf_repo,
                repo_type="dataset",
                token=hf_token,
                commit_message=f"Add {filename}",
                commit_description=f"Path: {clean_path}"
            )

            resolve_url = f"https://huggingface.co/datasets/{hf_repo}/resolve/main/{clean_path}"
            commit_url = getattr(commit_info, "commit_url", None)

            print(f"[HuggingFace] Backup successful! Permanent resolve URL: {resolve_url}", flush=True)
            if commit_url:
                print(f"[HuggingFace] Commit URL: {commit_url}", flush=True)

            result = {
                "hf_url": resolve_url,
                "hf_repo": hf_repo,
                "hf_path": clean_path,
                "size_bytes": file_size,
                "commit_url": commit_url
            }

            if job_id:
                result_filename = f"hf-result-{job_id}.json"
                with open(result_filename, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2)
                print(f"[HuggingFace] Backup result saved to {result_filename}", flush=True)

            print(f"[HuggingFace-Result] {json.dumps(result)}", flush=True)
            sys.exit(0)

        except Exception as e:
            last_err = e
            print(f"[HuggingFace] Attempt {attempt} failed: {e}", file=sys.stderr, flush=True)
            if attempt < max_attempts:
                time.sleep(5)

    print(f"[HuggingFace] All backup attempts failed: {last_err}", file=sys.stderr, flush=True)
    sys.exit(1)

if __name__ == "__main__":
    main()

