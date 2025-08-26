import os, re, time, json, pickle, tempfile, requests, io, base64
from datetime import datetime
from flask import Flask, request, jsonify

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as SA_Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ============================ ENV ============================
TELEGRAM_BOT_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_TOKEN        = os.environ.get("WEBHOOK_TOKEN", "")

SPREADSHEET_ID       = os.environ.get("SPREADSHEET_ID", "")

SHEET_NAME_OLD       = os.environ.get("SHEET_NAME_OLD", "Лист1")
TRIGGER_TEXT_OLD     = os.environ.get("TRIGGER_TEXT_OLD", "1")

SHEET_NAME_NEW       = os.environ.get("SHEET_NAME_NEW", "Лист2")
TRIGGER_TEXT_NEW     = os.environ.get("TRIGGER_TEXT_NEW", "2")

SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "").strip()
CLIENT_SECRET_FILE   = os.environ.get("CLIENT_SECRET_FILE", "").strip()

TOKEN_FILE_OLD       = os.environ.get("TOKEN_FILE_OLD", "/opt/render/project/src/token_old.pickle")
TOKEN_FILE_NEW       = os.environ.get("TOKEN_FILE_NEW", "/opt/render/project/src/token_new.pickle")

YOUTUBE_TOKEN_B64_OLD = os.environ.get("YOUTUBE_TOKEN_B64_OLD", "")
YOUTUBE_TOKEN_B64_NEW = os.environ.get("YOUTUBE_TOKEN_B64_NEW", "")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# статические настройки
COL_VIDEO = "A"
COL_TITLE = "B"
COL_DESC  = "C"
DELETE_FIRST_ROW_AFTER_SUCCESS = True

YOUTUBE_CATEGORY_ID = "22"
YOUTUBE_DEFAULT_VISIBILITY = "public"
YOUTUBE_MADE_FOR_KIDS = False
YOUTUBE_DEFAULT_TAGS = ["Shorts"]

# ============================ UTILS ============================
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} - {msg}", flush=True)

def ensure_env():
    miss = []
    for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "WEBHOOK_TOKEN": WEBHOOK_TOKEN,
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "SERVICE_ACCOUNT_FILE": SERVICE_ACCOUNT_FILE,
        "CLIENT_SECRET_FILE": CLIENT_SECRET_FILE,
    }.items():
        if not v: miss.append(k)
    if miss:
        for k in miss:
            log(f"❌ Отсутствует переменная окружения: {k}")
        raise SystemExit(1)

def maybe_restore_token(path, token_b64):
    if token_b64 and not os.path.exists(path):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except Exception:
            pass
        with open(path, "wb") as f:
            f.write(base64.b64decode(token_b64))
        log(f"Создан token.pickle по пути: {path}")

# ============================ GOOGLE AUTH ============================
SCOPES_YT = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
SCOPES_SA = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _sa_credentials():
    if SERVICE_ACCOUNT_FILE.startswith("{"):
        return SA_Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_FILE), scopes=SCOPES_SA)
    return SA_Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES_SA)

def _oauth_flow():
    if CLIENT_SECRET_FILE.startswith("{"):
        return InstalledAppFlow.from_client_config(json.loads(CLIENT_SECRET_FILE), SCOPES_YT)
    return InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES_YT)

def youtube_service(token_file: str):
    if not os.path.exists(token_file):
        raise RuntimeError("Отсутствует token.pickle. Задайте YOUTUBE_TOKEN_B64 или загрузите файл вручную.")

    with open(token_file, "rb") as f:
        creds = pickle.load(f)

    if not getattr(creds, "valid", False):
        if getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
            with open(token_file, "wb") as f:
                pickle.dump(creds, f)
        else:
            raise RuntimeError("Невозможно обновить OAuth токен. Выполните локально авторизацию и обновите YOUTUBE_TOKEN_B64.")
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def sheets_service():
    return build("sheets", "v4", credentials=_sa_credentials(), cache_discovery=False)

def drive_service():
    return build("drive", "v3", credentials=_sa_credentials(), cache_discovery=False)

# ============================ SHEETS ============================
def _normalize_sheet_id(x: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", x)
    return m.group(1) if m else x

SPREADSHEET_ID = _normalize_sheet_id(SPREADSHEET_ID)

def get_first_row_with_sheet(sh, sheet_name):
    rng = f"{sheet_name}!{COL_VIDEO}1:{COL_DESC}1"
    res = sh.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
    vals = res.get("values", [])
    if not vals or not vals[0]: return None
    row = vals[0]
    v = row[0].strip() if len(row) > 0 else ""
    t = row[1].strip() if len(row) > 1 else ""
    d = row[2].strip() if len(row) > 2 else ""
    if not v: return None
    return {"video": v, "title": t, "desc": d}

def get_sheet_id(sh, sheet_name) -> int:
    meta = sh.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    raise ValueError(f"Sheet '{sheet_name}' not found")

def delete_first_row_with_sheet(sh, sheet_name):
    sid = get_sheet_id(sh, sheet_name)
    body = {"requests": [{"deleteDimension": {"range": {
        "sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1
    }}}]}
    sh.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()

# ============================ VIDEO ============================
DRIVE_ID_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([A-Za-z0-9_-]+)")

def _save_stream_to_tmp(resp) -> str:
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk: tmp.write(chunk)
    tmp.flush(); tmp.close()
    return tmp.name

def _ensure_valid_video(path: str):
    size = os.path.getsize(path)
    if size < 200 * 1024:
        raise ValueError(f"Скачан слишком маленький файл ({size} байт)")
    with open(path, "rb") as f:
        head = f.read(65536)
    if head.lstrip().lower().startswith(b"<!doctype html") or b"<html" in head.lower():
        raise ValueError("Получен HTML вместо видео")
    return path

def gdrive_download_via_api(file_id: str) -> str:
    svc = drive_service()
    req = svc.files().get_media(fileId=file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    fh = io.FileIO(tmp.name, "wb")
    downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.close()
    return _ensure_valid_video(tmp.name)

def gdrive_download_public(file_id: str) -> str:
    URL = "https://drive.google.com/uc?export=download"
    with requests.Session() as s:
        r = s.get(URL, params={"id": file_id}, stream=True, timeout=180)
        if "text/html" in (r.headers.get("Content-Type") or ""):
            token = None
            m = re.search(r"confirm=([0-9A-Za-z_]+)", r.text)
            if m: token = m.group(1)
            else:
                for k, v in r.cookies.items():
                    if k.startswith("download_warning"): token = v; break
            if token: r = s.get(URL, params={"id": file_id, "confirm": token}, stream=True, timeout=180)
        path = _save_stream_to_tmp(r)
    return _ensure_valid_video(path)

def url_to_tempfile(url: str) -> str:
    r = requests.get(url, stream=True, timeout=300)
    path = _save_stream_to_tmp(r)
    return _ensure_valid_video(path)

def resolve_video_source(src: str):
    src = src.strip().strip('"').strip("'")
    src = os.path.expanduser(os.path.expandvars(src))
    if os.path.isfile(src): return src, False
    m = DRIVE_ID_RX.search(src)
    if m:
        file_id = m.group(1)
        try: return gdrive_download_via_api(file_id), True
        except: return gdrive_download_public(file_id), True
    if src.startswith("http://") or src.startswith("https://"):
        return url_to_tempfile(src), True
    raise FileNotFoundError(f"Источник видео не найден: {src}")

# ============================ UPLOAD ============================
class UploadLimitExceeded(Exception): pass

def _is_upload_limit_error(err: Exception) -> bool:
    try:
        if isinstance(err, HttpError):
            content = (err.content or b"").decode("utf-8", "ignore")
            if "uploadLimitExceeded" in content: return True
    except: pass
    return False

def upload_video(yt, file_path: str, title: str, description: str) -> str:
    snippet = {
        "title": (title or os.path.basename(file_path))[:100],
        "description": description or "",
        "categoryId": YOUTUBE_CATEGORY_ID,
    }
    if YOUTUBE_DEFAULT_TAGS:
        snippet["tags"] = YOUTUBE_DEFAULT_TAGS
    status = {"privacyStatus": YOUTUBE_DEFAULT_VISIBILITY,
              "selfDeclaredMadeForKids": bool(YOUTUBE_MADE_FOR_KIDS)}
    media = MediaFileUpload(file_path, chunksize=8*1024*1024, resumable=True, mimetype="video/*")
    request = yt.videos().insert(part="snippet,status", body={"snippet": snippet, "status": status}, media_body=media)
    response = None
    while response is None:
        try: _, response = request.next_chunk()
        except Exception as e:
            if _is_upload_limit_error(e): raise UploadLimitExceeded("Лимит YouTube")
            log(f"Повторная попытка загрузки: {e}"); time.sleep(3)
            continue
    return response.get("id")

# ============================ TELEGRAM ============================
def tg_send(chat_id: int, text: str):
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=30)
    except Exception as e:
        log(f"Ошибка Telegram: {e}")

# ============================ CORE ============================
def process_once(sheet_name, token_file, yt_label):
    try:
        sh = sheets_service()
        row = get_first_row_with_sheet(sh, sheet_name)
    except Exception as e:
        return {"status": "SHEETS_ACCESS_ERROR", "error": str(e)}

    if not row:
        try: delete_first_row_with_sheet(sh, sheet_name)
        except Exception as e: return {"status": "ROW_DELETE_ERROR", "error": str(e)}
        return {"status": "EMPTY_SHEET"}

    src, title, desc = row["video"], row["title"], row["desc"]
    try: local_path, is_temp = resolve_video_source(src)
    except Exception as e: return {"status": "DOWNLOAD_ERROR", "error": str(e)}

    try:
        yt = youtube_service(token_file)
        vid = upload_video(yt, local_path, title, desc)
    except UploadLimitExceeded as e: return {"status": "UPLOAD_LIMIT", "error": str(e)}
    except Exception as e: return {"status": "YOUTUBE_AUTH_ERROR", "error": str(e)}
    finally:
        if 'is_temp' in locals() and is_temp and os.path.exists(local_path):
            try: os.remove(local_path)
            except: pass

    if DELETE_FIRST_ROW_AFTER_SUCCESS:
        try: delete_first_row_with_sheet(sh, sheet_name)
        except Exception as e: return {"status": "ROW_DELETE_ERROR", "video_id": vid, "error": str(e)}

    return {"status": "OK", "video_id": vid, "yt_label": yt_label}

# ============================ FLASK ============================
app = Flask(__name__)

@app.route("/", methods=["GET"])
def root(): return "ok", 200

@app.route("/webhook/<token>", methods=["POST"])
def webhook(token):
    if token != WEBHOOK_TOKEN: return "not found", 404
    upd = request.get_json(silent=True) or {}
    msg = upd.get("message") or upd.get("channel_post") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id:
        log("❌ Ошибка: отсутствует chat_id"); return jsonify({"ok": True}), 200
    if not text:
        tg_send(chat_id, "❌ Ошибка: пустая строка"); return jsonify({"ok": True}), 200

    if text == TRIGGER_TEXT_OLD:
        tg_send(chat_id, "Старт публикации (старый YouTube)…")
        rep = process_once(SHEET_NAME_OLD, TOKEN_FILE_OLD, "старого youtube")
    elif text == TRIGGER_TEXT_NEW:
        tg_send(chat_id, "Старт публикации (новый YouTube)…")
        rep = process_once(SHEET_NAME_NEW, TOKEN_FILE_NEW, "нового youtube")
    else:
        tg_send(chat_id, "Код ничего не активирует"); return jsonify({"ok": True}), 200

    status = rep.get("status")
    if status == "OK":
        vid, yt_label = rep["video_id"], rep["yt_label"]
        tg_send(chat_id, f"✅ Создано видео для {yt_label}, ID: {vid}")
        log(f"Создано видео для {yt_label}, ID: {vid}")
    elif status == "UPLOAD_LIMIT":
        tg_send(chat_id, "⚠️ Лимит отправки видео на YouTube")
    elif status == "EMPTY_SHEET":
        tg_send(chat_id, "❌ Ошибка: нет данных в таблице")
    elif status == "SHEETS_ACCESS_ERROR":
        tg_send(chat_id, f"❌ Ошибка доступа к таблице: {rep.get('error')}")
    elif status == "DOWNLOAD_ERROR":
        tg_send(chat_id, f"❌ Ошибка загрузки видео: {rep.get('error')}")
    elif status == "YOUTUBE_AUTH_ERROR":
        tg_send(chat_id, f"❌ Ошибка авторизации YouTube: {rep.get('error')}")
    elif status == "ROW_DELETE_ERROR":
        vid = rep.get("video_id", "")
        tg_send(chat_id, f"❌ Ошибка удаления строки: {rep.get('error')} (Видео загружено: {vid})")
    else:
        tg_send(chat_id, f"❌ Неизвестный статус: {status}")

    return jsonify({"ok": True}), 200

if __name__ == "__main__":
    ensure_env()
    maybe_restore_token(TOKEN_FILE_OLD, YOUTUBE_TOKEN_B64_OLD)
    maybe_restore_token(TOKEN_FILE_NEW, YOUTUBE_TOKEN_B64_NEW)
    log("Сценарий запущен")
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
