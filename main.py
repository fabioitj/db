import base64
import io
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from PIL import Image

PT_URL = "https://raw.githubusercontent.com/blawar/titledb/master/PT.pt.json"
US_URL = "https://raw.githubusercontent.com/blawar/titledb/master/US.en.json"

OUT_TXT = "titledb_icons.txt"
SPLIT = "|split|"

# Performance tuning
MAX_WORKERS = 32
JSON_TIMEOUT = 60
IMG_TIMEOUT = 20

# Size reduction knobs (these matter a lot)
TARGET_SIZE = (128, 128)     # try (48,48) or (32,32) for even smaller
FORMAT = "WEBP"            # WEBP usually smallest. Use "JPEG" if you prefer.
WEBP_QUALITY = 50          # lower = smaller (try 35)
JPEG_QUALITY = 50          # if FORMAT="JPEG"

# Optional disk cache so you don't redownload/recompress on reruns
ICON_CACHE_DIR = "icon_cache_b64"  # set None to disable


def safe_mkdir(path: str | None):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def fetch_json(url: str) -> dict:
    r = requests.get(url, timeout=JSON_TIMEOUT)
    r.raise_for_status()
    return r.json()


def build_map(raw: dict) -> dict:
    # id -> {name, iconUrl}
    out = {}
    for item in raw.values():
        tid = item.get("id")
        if not tid:
            continue
        out[tid] = {"name": item.get("name"), "iconUrl": item.get("iconUrl")}
    return out


def cache_key_from_url(url: str) -> str:
    # stable filename-ish without extra deps
    import hashlib
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def try_read_cache(icon_url: str) -> str | None:
    if not ICON_CACHE_DIR:
        return None
    safe_mkdir(ICON_CACHE_DIR)
    fp = os.path.join(ICON_CACHE_DIR, cache_key_from_url(icon_url) + ".txt")
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def write_cache(icon_url: str, b64: str) -> None:
    if not ICON_CACHE_DIR:
        return
    safe_mkdir(ICON_CACHE_DIR)
    fp = os.path.join(ICON_CACHE_DIR, cache_key_from_url(icon_url) + ".txt")
    try:
        with open(fp, "w", encoding="utf-8") as f:
            f.write(b64)
    except Exception:
        pass


def image_bytes_to_small_base64(img_bytes: bytes) -> str | None:
    try:
        with Image.open(io.BytesIO(img_bytes)) as im:
            im = im.convert("RGBA")
            im.thumbnail(TARGET_SIZE, Image.Resampling.LANCZOS)

            out = io.BytesIO()
            if FORMAT.upper() == "WEBP":
                # method=6 usually better compression (slower), you can remove it for speed
                im.save(out, format="WEBP", quality=WEBP_QUALITY, method=6)
            elif FORMAT.upper() == "JPEG":
                # JPEG doesn't support alpha; flatten onto black
                bg = Image.new("RGB", im.size, (0, 0, 0))
                bg.paste(im, mask=im.split()[-1])
                bg.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True, progressive=True)
            else:
                # fallback
                im.save(out, format="PNG", optimize=True)

            data = out.getvalue()
            return base64.b64encode(data).decode("utf-8")
    except Exception as e:
        print(f"[img] convert failed: {e}")
        return None


def download_and_process_icon(icon_url: str) -> str | None:
    if not icon_url:
        return None

    cached = try_read_cache(icon_url)
    if cached:
        return cached

    try:
        with requests.Session() as s:
            r = s.get(icon_url, timeout=IMG_TIMEOUT)
            r.raise_for_status()
            b64 = image_bytes_to_small_base64(r.content)
            if b64:
                write_cache(icon_url, b64)
            return b64
    except Exception as e:
        print(f"[icon] download failed: {icon_url} | {e}")
        return None


def main():
    print("Downloading PT JSON...")
    pt_raw = fetch_json(PT_URL)
    print("Downloading US JSON...")
    us_raw = fetch_json(US_URL)

    pt_by_id = build_map(pt_raw)
    us_by_id = build_map(us_raw)

    all_ids = sorted(set(pt_by_id.keys()) | set(us_by_id.keys()))
    print(f"Total IDs: {len(all_ids)}")

    # Choose icon URL per id (prefer PT, fallback US)
    id_to_icon_url = {}
    unique_icon_urls = set()
    for tid in all_ids:
        icon_url = (pt_by_id.get(tid, {}) or {}).get("iconUrl") or (us_by_id.get(tid, {}) or {}).get("iconUrl")
        if icon_url:
            id_to_icon_url[tid] = icon_url
            unique_icon_urls.add(icon_url)

    unique_icon_urls = sorted(unique_icon_urls)
    print(f"Unique icons: {len(unique_icon_urls)}")

    # Multithread icon processing: iconUrl -> base64
    icon_b64_by_url: dict[str, str | None] = {}
    lock = threading.Lock()

    def worker(url: str):
        b64 = download_and_process_icon(url)
        with lock:
            icon_b64_by_url[url] = b64

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, url) for url in unique_icon_urls]
        done = 0
        for _ in as_completed(futures):
            done += 1
            if done % 200 == 0 or done == len(futures):
                print(f"Icons processed: {done}/{len(futures)}")

    # Write TXT line by line (streaming = low RAM)
    # Format: id|split|pt|split|en|split|iconBase64
    with open(OUT_TXT, "w", encoding="utf-8") as f:
        for tid in all_ids:
            pt_name = (pt_by_id.get(tid) or {}).get("name") or ""
            en_name = (us_by_id.get(tid) or {}).get("name") or ""
            icon_url = id_to_icon_url.get(tid)
            icon_b64 = icon_b64_by_url.get(icon_url) if icon_url else None
            icon_b64 = icon_b64 or ""

            # IMPORTANT: ensure no newlines inside names (just in case)
            pt_name = pt_name.replace("\n", " ").replace("\r", " ")
            en_name = en_name.replace("\n", " ").replace("\r", " ")

            f.write(f"{tid}{SPLIT}{pt_name}{SPLIT}{en_name}{SPLIT}{icon_b64}\n")

    print(f"Saved: {OUT_TXT}")
    if ICON_CACHE_DIR:
        print(f"Icon cache folder: {ICON_CACHE_DIR}/")


if __name__ == "__main__":
    main()