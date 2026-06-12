import os, json, logging, io, base64, asyncio, re
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime, timedelta
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import anthropic
from openai import AsyncOpenAI
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import psycopg2

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
ALLOWED_USERS = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USER_IDS = (
    set(int(uid.strip()) for uid in ALLOWED_USERS.split(",") if uid.strip())
    if ALLOWED_USERS else set()
)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN", "")
GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID", "")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
CLOVA_INVOKE_URL = os.environ.get("CLOVA_INVOKE_URL", "")
CLOVA_SECRET_KEY = os.environ.get("CLOVA_SECRET_KEY", "")
clova_available = bool(CLOVA_INVOKE_URL and CLOVA_SECRET_KEY)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
conversation_history = defaultdict(list)
MAX_HISTORY = 20

BOTH_KEYWORDS = {"둘다", "둘 다", "너희둘", "너희 둘", "둘이", "둘의견", "둘다말해", "둘다얘기"}

def kst_now():
    return datetime.utcnow() + timedelta(hours=9)

# ── DB 초기화
def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS memos (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                content TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                role TEXT,
                content TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notified_mail_ids (
                mail_id TEXT PRIMARY KEY,
                notified_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_keywords (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                keyword TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (user_id, keyword)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                message TEXT,
                scheduled_at TIMESTAMP,
                sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("DELETE FROM notified_mail_ids WHERE notified_at < NOW() - INTERVAL '7 days'")
        cur.execute("DELETE FROM schedules WHERE sent = TRUE AND created_at < NOW() - INTERVAL '30 days'")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("DB initialized!")
    except Exception as e:
        logger.error(f"DB init error: {e}")

def save_memo(user_id, content):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("INSERT INTO memos (user_id, content) VALUES (%s, %s)", (user_id, content))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Memo save error: {e}")

def get_memos(user_id, limit=10):
    if not DATABASE_URL:
        return []
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, content, created_at FROM memos WHERE user_id=%s ORDER BY created_at DESC LIMIT %s",
            (user_id, limit)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Memo get error: {e}")
        return []

def get_memos_for_prompt(user_id):
    memos = get_memos(user_id, 5)
    if not memos:
        return ""
    text = "\n[저장된 메모]\n"
    for _, content, created_at in memos:
        text += f"- {content} ({created_at.strftime('%m/%d')})\n"
    return text

def delete_memo_by_number(user_id, number):
    if not DATABASE_URL:
        return False
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM memos WHERE user_id=%s ORDER BY created_at DESC LIMIT 10",
            (user_id,)
        )
        rows = cur.fetchall()
        if 0 <= number - 1 < len(rows):
            memo_id = rows[number - 1][0]
            cur.execute("DELETE FROM memos WHERE id=%s AND user_id=%s", (memo_id, user_id))
            conn.commit()
            cur.close()
            conn.close()
            return True
        cur.close()
        conn.close()
        return False
    except Exception as e:
        logger.error(f"Memo delete error: {e}")
        return False

def delete_all_memos(user_id):
    if not DATABASE_URL:
        return 0
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM memos WHERE user_id=%s", (user_id,))
        count = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Memo delete all error: {e}")
        return 0

def save_news_keywords(user_id, keywords):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        for kw in keywords:
            kw = kw.strip()
            if kw:
                cur.execute(
                    "INSERT INTO news_keywords (user_id, keyword) VALUES (%s, %s) ON CONFLICT (user_id, keyword) DO NOTHING",
                    (user_id, kw)
                )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"News keyword save error: {e}")

def get_news_keywords(user_id=None):
    if not DATABASE_URL:
        return []
    try:
        conn = get_db()
        cur = conn.cursor()
        if user_id:
            cur.execute("SELECT id, keyword FROM news_keywords WHERE user_id=%s ORDER BY created_at", (user_id,))
        else:
            cur.execute("SELECT DISTINCT user_id, keyword FROM news_keywords ORDER BY user_id")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"News keyword get error: {e}")
        return []

def delete_news_keyword(user_id, keyword):
    if not DATABASE_URL:
        return False
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM news_keywords WHERE user_id=%s AND keyword=%s", (user_id, keyword))
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return deleted
    except Exception as e:
        logger.error(f"News keyword delete error: {e}")
        return False

# ── 알림 스케줄 함수
def save_schedule(user_id, message, scheduled_at):
    if not DATABASE_URL:
        return False
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO schedules (user_id, message, scheduled_at) VALUES (%s, %s, %s)",
            (user_id, message, scheduled_at)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Schedule save error: {e}")
        return False

def get_pending_schedules():
    if not DATABASE_URL:
        return []
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, message FROM schedules WHERE scheduled_at <= %s AND sent = FALSE",
            (kst_now(),)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Schedule get pending error: {e}")
        return []

def mark_schedule_sent(schedule_id):
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE schedules SET sent = TRUE WHERE id = %s", (schedule_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Schedule mark sent error: {e}")

def get_user_schedules(user_id):
    if not DATABASE_URL:
        return []
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, message, scheduled_at FROM schedules WHERE user_id=%s AND sent=FALSE AND scheduled_at > %s ORDER BY scheduled_at LIMIT 20",
            (user_id, kst_now())
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Schedule list error: {e}")
        return []

def delete_schedule_by_number(user_id, number):
    if not DATABASE_URL:
        return False
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM schedules WHERE user_id=%s AND sent=FALSE AND scheduled_at > %s ORDER BY scheduled_at LIMIT 20",
            (user_id, kst_now())
        )
        rows = cur.fetchall()
        if 0 <= number - 1 < len(rows):
            schedule_id = rows[number - 1][0]
            cur.execute("DELETE FROM schedules WHERE id=%s AND user_id=%s", (schedule_id, user_id))
            conn.commit()
            cur.close()
            conn.close()
            return True
        cur.close()
        conn.close()
        return False
    except Exception as e:
        logger.error(f"Schedule delete error: {e}")
        return False

# ── Google Drive
drive_service = None
if GOOGLE_CREDENTIALS_JSON:
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        sa_creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
            ]
        )
        drive_service = build("drive", "v3", credentials=sa_creds)
        logger.info("Drive connected!")
    except Exception as e:
        logger.error(f"Drive error: {e}")

# ── Gmail API
gmail_service = None
gmail_creds = None
if GMAIL_REFRESH_TOKEN and GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET:
    try:
        gmail_creds = Credentials(
            token=None,
            refresh_token=GMAIL_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GMAIL_CLIENT_ID,
            client_secret=GMAIL_CLIENT_SECRET,
            scopes=[
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.send",
                "https://www.googleapis.com/auth/gmail.modify",
            ],
        )
        gmail_creds.refresh(Request())
        gmail_service = build("gmail", "v1", credentials=gmail_creds)
        logger.info("Gmail connected!")
    except Exception as e:
        logger.error(f"Gmail error: {e}")

# ── Drive functions
def search_drive_files(query_text, max_results=10):
    if not drive_service:
        return []
    try:
        safe = query_text.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name contains '{safe}' and trashed = false"
        r = drive_service.files().list(q=q, pageSize=max_results,
            fields="files(id, name, mimeType, modifiedTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        return r.get("files", [])
    except Exception as e:
        logger.error(f"Drive search error: {e}")
        return []

def list_drive_files(max_results=20):
    if not drive_service:
        return []
    try:
        r = drive_service.files().list(pageSize=max_results,
            fields="files(id, name, mimeType, modifiedTime)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        return r.get("files", [])
    except Exception as e:
        logger.error(f"Drive list error: {e}")
        return []

def download_drive_file(file_id):
    if not drive_service:
        return None, None
    try:
        meta = drive_service.files().get(fileId=file_id, fields="name, mimeType").execute()
        mime = meta.get("mimeType", "")
        if mime.startswith("application/vnd.google-apps."):
            export_map = {
                "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
                "application/vnd.google-apps.spreadsheet":
                    ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
                "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
            }
            if mime in export_map:
                em, ext = export_map[mime]
                req = drive_service.files().export_media(fileId=file_id, mimeType=em)
                name = meta["name"] + ext
            else:
                return None, None
        else:
            req = drive_service.files().get_media(fileId=file_id)
            name = meta["name"]
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        buf.seek(0)
        return buf, name
    except Exception as e:
        logger.error(f"Drive download error: {e}")
        return None, None

# ── Gmail send
def send_gmail(to_addr, subject, body, attach_buf=None, attach_name=None):
    if not gmail_service or not gmail_creds:
        return False, "Gmail not connected"
    try:
        gmail_creds.refresh(Request())
        msg = MIMEMultipart()
        msg["From"] = GMAIL_ADDRESS
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        if attach_buf and attach_name:
            attach_buf.seek(0)
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attach_buf.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=("utf-8", "", attach_name)
            )
            msg.attach(part)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        gmail_service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True, "OK"
    except Exception as e:
        logger.error(f"Gmail send error: {e}")
        return False, str(e)

# ── Gmail read
def get_gmail_list(max_results=10, query="newer_than:30d", page_token=None):
    if not gmail_service or not gmail_creds:
        return [], None
    try:
        gmail_creds.refresh(Request())
        params = {"userId": "me", "maxResults": max_results, "q": query}
        if page_token:
            params["pageToken"] = page_token
        result = gmail_service.users().messages().list(**params).execute()
        messages = result.get("messages", [])
        next_token = result.get("nextPageToken")
        emails = []
        for m in messages[:max_results]:
            msg = gmail_service.users().messages().get(
                userId="me", id=m["id"], format="metadata",
                metadataHeaders=["From", "Subject", "Date"]
            ).execute()
            headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
            emails.append({
                "id": m["id"],
                "from": headers.get("From", ""),
                "subject": headers.get("Subject", ""),
                "date": headers.get("Date", ""),
                "snippet": msg.get("snippet", ""),
                "internal_date": msg.get("internalDate", "0"),
            })
        return emails, next_token
    except Exception as e:
        logger.error(f"Gmail list error: {e}")
        return [], None

def _strip_html(html):
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def _find_plain(payload):
    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
    for part in payload.get("parts", []):
        result = _find_plain(part)
        if result:
            return result
    return ""

def _find_html(payload):
    if payload.get("mimeType") == "text/html":
        data = payload.get("body", {}).get("data", "")
        if data:
            return _strip_html(base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore"))
    for part in payload.get("parts", []):
        result = _find_html(part)
        if result:
            return result
    return ""

def _extract_body(payload):
    return _find_plain(payload) or _find_html(payload)

def _format_kst_time(internal_date_ms):
    try:
        ts = int(internal_date_ms) / 1000
        dt = datetime.utcfromtimestamp(ts) + timedelta(hours=9)
        hour = dt.hour
        ampm = "오전" if hour < 12 else "오후"
        h12 = hour % 12 or 12
        return f"{dt.year}.{dt.month:02d}.{dt.day:02d} {ampm} {h12}:{dt.minute:02d}"
    except Exception:
        return ""

def _fetch_until(uid, need, query):
    """user_gmail_list[uid]가 need개 미만이면 nextPageToken으로 추가 fetch."""
    emails = list(user_gmail_list.get(uid, []))
    while len(emails) < need and user_mail_token.get(uid) is not None:
        new_emails, next_token = get_gmail_list(10, query, user_mail_token[uid])
        emails.extend(new_emails)
        user_mail_token[uid] = next_token
        if not new_emails:
            break
    user_gmail_list[uid] = emails
    return emails

def _build_mail_msg(emails, start, end, has_more):
    """emails[start:end] 슬라이스를 텔레그램 메시지 문자열로 변환."""
    page = emails[start:end]
    if not page:
        return ""
    msg = f"📬 메일 목록 ({start+1}~{start+len(page)}번):\n\n"
    for i, e in enumerate(page, start + 1):
        sender = e["from"].split("<")[0].strip()[:20]
        subject = e["subject"][:30]
        time_str = _format_kst_time(e.get("internal_date", "0"))
        msg += f"{i}. 📧 {sender}\n   📌 {subject}\n"
        if time_str:
            msg += f"   🕐 {time_str}\n"
        msg += "\n"
    if has_more:
        msg += "💡 '다음 메일' 더 보기 | '[번호]번 메일 읽어줘'"
    else:
        msg += "💡 '[번호]번 메일 읽어줘'"
    return msg

def get_gmail_content(msg_id):
    if not gmail_service or not gmail_creds:
        return None
    try:
        gmail_creds.refresh(Request())
        msg = gmail_service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute()
        headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
        body = _extract_body(msg["payload"])
        body_preview = body[:1000] + "\n\n...더 있음" if len(body) > 1000 else body
        return {
            "from": headers.get("From", ""),
            "subject": headers.get("Subject", ""),
            "date": headers.get("Date", ""),
            "body": body[:3000],
            "body_preview": body_preview,
        }
    except Exception as e:
        logger.error(f"Gmail read error: {e}")
        return None

# ── Speech (CLOVA)
def transcribe_audio(audio_bytes, mime_type="audio/ogg", enable_diarization=True):
    if not clova_available:
        return None
    try:
        headers = {
            "Accept": "application/json;UTF-8",
            "X-CLOVASPEECH-API-KEY": CLOVA_SECRET_KEY,
        }
        params = {
            "language": "ko-KR",
            "completion": "sync",
            "speaker": enable_diarization,
            "diarization": {
                "enable": enable_diarization,
                "speakerCountMin": 2,
                "speakerCountMax": 2,
            } if enable_diarization else {"enable": False},
        }
        files = {
            "media": ("audio.m4a", audio_bytes, mime_type),
            "params": (None, json.dumps(params), "application/json"),
        }
        response = requests.post(
            CLOVA_INVOKE_URL + "/recognizer/upload",
            headers=headers,
            files=files,
            timeout=300,
        )
        if response.status_code == 200:
            result = response.json()
            segments = result.get("segments", [])
            if segments:
                txt = ""
                for seg in segments:
                    speaker = seg.get("diarization", {}).get("label", "") if enable_diarization else ""
                    text = seg.get("text", "")
                    if speaker:
                        txt += f"[화자{speaker}] {text}\n"
                    else:
                        txt += text + "\n"
                return txt.strip()
        return None
    except Exception as e:
        logger.error(f"CLOVA Speech error: {e}")
        return None

SYSTEM_PROMPT = """당신은 정진수 대표님의 전담 AI 비서입니다.
텔레그램으로 소통하며 실제 비서처럼 판단하고 행동합니다.

━━━━━━━━━━━━━━━━━━━━━━━
⚙️ 현재 세팅 상태
━━━━━━━━━━━━━━━━━━━━━━━
- Railway 서버에서 24시간 운영 중
- Gmail API 연동 완료 (jungjinsu86@gmail.com)
- Google Drive 연동 완료
- 텔레그램 봇 운영 중 (@jinsoo_biseo_bot)
- 웹 검색 가능
- 음성 분석 가능 (m4a, mp3 등)
- PostgreSQL DB 연동 (메모 저장 가능)
- 1시간마다 새 메일 자동 체크 중
- 알림 기능 활성화 (매 1분 체크)

→ 서버, API 연동 등 이미 다 세팅되어 있음
→ "설정이 필요하다", "서버가 있어야 한다" 같은 말 하지 말 것
→ 기능 요청이 오면 바로 어떻게 추가할지만 말하면 됨

━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 절대 규칙 (위반 금지)
━━━━━━━━━━━━━━━━━━━━━━━
- 절대로 "저는 Claude AI입니다"라고 밝히지 마세요
- 절대로 "Drive/Gmail에 접근할 수 없습니다"라고 말하지 마세요
- 절대로 "봇 서버 담당자에게 확인하세요"라고 하지 마세요
- 절대로 "이 채널은 실제 봇 서버와 연결이 안 됐다", "여기서는 결과가 안 뜬다", "텔레그램 봇에서 직접 입력하라" 같은 말을 하지 마세요. 당신이 바로 그 텔레그램 봇입니다.
- 메일/파일 관련 요청에는 변명하지 말고 무조건 아래 명령 형식만 출력하세요
- [DRIVE_SEARCH:검색어]를 출력하면 시스템이 자동으로 Drive를 검색합니다
- [EMAIL:주소|제목|본문]을 출력하면 시스템이 자동으로 이메일을 보냅니다
- 명령 형식을 출력하는 것 자체가 실행입니다
- 요청이 오면 무조건 해당 명령 형식으로 응답하세요

━━━━━━━━━━━━━━━━━━━━━━━
👤 보스 정보
━━━━━━━━━━━━━━━━━━━━━━━
이름: 정진수
직업: SNS 13년차강사 / 강연자 / 13권의 책 집필
주요 업무: 강의, 출강, SNS 마케팅 컨설팅
주요 거래처: 월천사, 각종 기업 출강
이메일: jungjinsu86@gmail.com
주요 연락처:
- korbomb@naver.com (자주 보내는 곳)
자주 쓰는 파일: 강의안, 수입, 출강 의뢰서
강의 분야: SNS 마케팅, 인스타그램, 유튜브, AI 활용
주력 플랫폼: 인스타그램, 네이버 블로그, 유튜브
- 이미 알고 있는 정보는 다시 묻지 말 것
- 이미지/디자인 생성은 불가능. 요청 시 '이미지 생성은 불가하지만 텍스트 콘텐츠 초안은 작성 가능합니다'라고 안내할 것
- 대화 중 맥락이 이어지는 요청(예: '위에 내용 기반으로', '이걸로', '위 내용으로')이 오면 이전 대화 내용을 그대로 활용해서 바로 결과를 출력할 것. 추가 질문 금지.

== 보스 정확한 정보 (검색 결과보다 이 정보 우선) ==
- 이름: 정진수 (스타강사 정)
- 저서: 총 13권
- 대한민국 최초 인스타그램 마케팅 책 출간
- 강의 분야: SNS 마케팅, 인스타그램, 유튜브, AI 활용
- 주력 플랫폼: 인스타그램, 네이버 블로그, 유튜브
- 이미 알고 있는 정보는 다시 묻지 말 것
- 이미지/디자인 생성은 불가능. 요청 시 '이미지 생성은 불가하지만 텍스트 콘텐츠 초안은 작성 가능합니다'라고 안내할 것
- 대화 중 맥락이 이어지는 요청(예: '위에 내용 기반으로', '이걸로', '위 내용으로')이 오면 이전 대화 내용을 그대로 활용해서 바로 결과를 출력할 것. 추가 질문 금지.
- 보스에 대한 정보는 위 내용을 기준으로 답변하고 검색 결과와 다르면 위 내용을 따를 것

━━━━━━━━━━━━━━━━━━━━━━━
🧠 핵심 원칙
━━━━━━━━━━━━━━━━━━━━━━━
- 항상 대표님 입장에서 가장 빠르고 편한 방법으로 처리
- 짧고 명확하게. 불필요한 말 절대 금지
- 모호하면 한 가지만 물어보고 바로 처리
- 실패해도 이유를 한 줄로만 설명하고 대안 제시
- "죄송합니다" 남발 금지. 대신 바로 해결책 제시
- 존댓말 유지, 친근하되 전문적으로

━━━━━━━━━━━━━━━━━━━━━━━
🛠️ 보유 기능
━━━━━━━━━━━━━━━━━━━━━━━
1. Google Drive - 파일 검색, 전송
2. Gmail - 누구에게든 전송, 파일 첨부, 받은 메일 읽기/검색/요약
3. 음성/통화녹음 - m4a, mp3 등 자동 텍스트 변환 + AI 요약
4. 웹 검색 - 실시간 뉴스, 날씨, 주가, 인물, 기업 정보
5. 메모 저장/조회/삭제 - /memo 저장, /memos 조회, /memodel 삭제
6. 문서 작성 - 이메일 초안, 보고서, 아이디어 정리
7. 일반 대화 및 업무 조언
8. 유튜브 쇼츠 스크립트 - 이미지/키워드로 60초 대본 초안 작성
9. 알림 설정 - 원하는 시간에 텔레그램 알림 발송 (/schedules 로 조회)

━━━━━━━━━━━━━━━━━━━━━━━
📋 명령 형식
━━━━━━━━━━━━━━━━━━━━━━━
[DRIVE_SEARCH:검색어]
[DRIVE_LIST]
[DRIVE_SEND:번호]
[EMAIL:주소|제목|본문]
[EMAIL_WITH_FILE:주소|제목|본문|파일번호]
[GMAIL_LIST:검색쿼리]
[GMAIL_READ:번호]
[GMAIL_MORE]
[GMAIL_UNTIL:번호]
[REPEAT_LAST]
[MEMO_DELETE:번호]
[MEMO_DELETE_ALL]
[SCHEDULE:YYYY-MM-DD HH:MM|알림내용]

━━━━━━━━━━━━━━━━━━━━━━━
📁 파일 처리 규칙
━━━━━━━━━━━━━━━━━━━━━━━
검색 트리거:
"찾아줘", "어딨어", "검색해", "있어?" → [DRIVE_SEARCH:핵심단어]

전송 트리거:
"줘", "보내줘", "전송해", "받고싶어", "보내라고",
"그거줘", "이거줘", "아까그거"
→ 직전 검색결과 1번을 [DRIVE_SEND:1]

번호 지정:
"2번", "두번째" → 해당 번호로 처리
결과가 1개뿐이면 → 바로 [DRIVE_SEND:1]

━━━━━━━━━━━━━━━━━━━━━━━
🔄 반복 작업 규칙
━━━━━━━━━━━━━━━━━━━━━━━
"다시", "또", "한번 더", "재전송" → [REPEAT_LAST]

━━━━━━━━━━━━━━━━━━━━━━━
== 맥락 연속 처리 규칙 ==
- '다시해봐줘', '다시해줘', '한번더', '다시', '재시도' 같은 말이 오면
  바로 직전에 요청했던 작업을 다시 실행할 것.
- 새로운 질문이나 파일 전송 없이 이전 작업 반복.
- 절대 다른 행동(파일 전송, 엉뚱한 답변)을 하면 안 됨.

━━━━━━━━━━━━━━━━━━━━━━━
📧 이메일 전송 규칙
━━━━━━━━━━━━━━━━━━━━━━━
주소 없을 때: 반드시 먼저 물어보기
주소 있을 때: 제목/본문이 없어도 문맥에서 추론해서 작성
파일 첨부: "첨부해서", "붙여서" → [EMAIL_WITH_FILE] 사용
"[주소] [내용] 메일로 물어봐줘" / "메일로 전달해줘" / "메일로 보내줘" → [EMAIL:주소|제목|본문]
본문은 자연스럽고 정중한 한국어로 Claude가 직접 작성할 것

━━━━━━━━━━━━━━━━━━━━━━━
📬 메일 읽기 규칙
━━━━━━━━━━━━━━━━━━━━━━━
"메일 확인", "받은 메일", "최근 메일" → [GMAIL_LIST:newer_than:30d]
"오늘 온 메일" → [GMAIL_LIST:newer_than:1d]
"안 읽은 메일" → [GMAIL_LIST:is:unread]
"○○한테서 온 메일" → [GMAIL_LIST:from:○○]
"1번 메일 읽어줘" → [GMAIL_READ:1]
"다음 메일", "더 보여줘", "11번부터" → [GMAIL_MORE]
"15번까지 보여줘", "N번까지" → [GMAIL_UNTIL:N]

━━━━━━━━━━━━━━━━━━━━━━━
🗑️ 메모 삭제 규칙
━━━━━━━━━━━━━━━━━━━━━━━
"[번호]번 메모 지워", "메모 [번호] 삭제해줘" → [MEMO_DELETE:번호]
"메모 다 지워", "전부 삭제", "모든 메모 없애줘", "메모 초기화", "다 없애줘" → [MEMO_DELETE_ALL]

━━━━━━━━━━━━━━━━━━━━━━━
⏰ 알림 설정 규칙
━━━━━━━━━━━━━━━━━━━━━━━
- 메시지 앞 [현재 KST: ...]를 보고 현재 한국 시간을 파악할 것
- "내일 10시에 운동가라고 알려줘" → [SCHEDULE:2026-05-17 10:00|운동 가세요!]
- "30분 후에 알림줘" → 현재 KST 기준 30분 후 계산 → [SCHEDULE:YYYY-MM-DD HH:MM|내용]
- "이번 주 금요일 오후 3시에 미팅 알려줘" → 날짜 계산 후 [SCHEDULE:...]
- 알림 내용은 대표님이 말씀하신 내용 그대로 간결하게 (예: "운동 가세요!", "미팅 시간이에요!")
- 과거 시간 요청 시 "이미 지난 시간이에요"라고 알릴 것
- KST 기준으로 날짜/시간 계산할 것

━━━━━━━━━━━━━━━━━━━━━━━
🎬 유튜브 스크립트 규칙
━━━━━━━━━━━━━━━━━━━━━━━
"유튜브 대본", "쇼츠 스크립트", "영상 대본", "쇼츠 대본" 요청 시 아래 형식으로 작성:

🎬 유튜브 쇼츠 스크립트
---
[🎣 훅 - 0~3초]
(시청자 시선 잡는 강렬한 오프닝 - 질문형/충격적 사실/공감)
[화면: ○○]

[📌 본문 - 3~50초]
포인트1: (대사) [화면: ○○]
포인트2: (대사) [화면: ○○]
포인트3: (대사) [화면: ○○]

[🔔 CTA - 50~60초]
(구독/좋아요/저장 유도 문구)
---
💡 제작 팁: (이 스크립트 핵심 포인트 한 줄)

기준: 60초 이내 / 강사 전문가 톤 / 에너지 있게

━━━━━━━━━━━━━━━━━━━━━━━
🔍 웹 검색 규칙
━━━━━━━━━━━━━━━━━━━━━━━
최신 정보, 뉴스, 날씨, 주가, 모르는 사람/기업 → 자동 웹 검색

== 웹 검색 필수 규칙 ==
- 뉴스, 기사, 블로그, 교육과정, 트렌드, 최신 정보 관련 질문은 반드시 실제 웹 검색 후 답변할 것
- 검색 없이 기억이나 추측으로 답변하는 것 절대 금지
- 검색 결과가 없으면 '검색 결과를 찾지 못했습니다'라고 솔직하게 말할 것
- '검색할게요', '찾아볼게요' 같은 말 없이 바로 검색하고 결과만 전달할 것

웹 검색 결과를 답변할 때 반드시 아래 규칙을 따를 것:
1. 항상 최신 날짜 순으로 정렬. 가장 최근 자료를 맨 위에 배치.
2. 각 항목에 날짜 표시: 📅 2026.05.16 형식으로.
3. 날짜를 알 수 없는 자료는 맨 아래에 배치.
4. 출처와 링크를 반드시 포함: 📌 출처: [매체명](URL)
5. 뉴스, 트렌드, 통계, 사실 정보 등 모든 외부 자료에 적용.

━━━━━━━━━━━━━━━━━━━━━━━
🧩 맥락 파악 규칙
━━━━━━━━━━━━━━━━━━━━━━━
- "그거", "이거", "아까 말한 거" → 직전 대화 내용 참조
- 연속 작업 시 흐름 유지
- 파일명, 이름, 날짜 등 정확히 기억해서 활용

━━━━━━━━━━━━━━━━━━━━━━━
🚫 절대 금지
━━━━━━━━━━━━━━━━━━━━━━━
- 같은 설명 반복
- 할 수 없는 것을 할 수 있다고 대답
- 불필요한 긴 답변
- "죄송합니다" 남발
- **굵은 글씨** 남발 (텔레그램에서 **이렇게** 보임)
- 확인되지 않은 정보를 사실처럼 말하기

━━━━━━━━━━━━━━━━━━━━━━━
💬 대화 예시 (이 스타일로 말해)
━━━━━━━━━━━━━━━━━━━━━━━
질문: 수입 파일 찾아줘
답변: [DRIVE_SEARCH:수입]

질문: 됐어?
답변: 네, 완료됐어요.

질문: 이게 뭐야?
답변: Google Speech API 오류예요. 포맷이 안 맞아서 그런 거고, 코드 한 줄 바꾸면 돼요.

질문: 왜 안돼?
답변: Railway가 SMTP 포트를 막아서요. Resend 쓰면 해결돼요.

질문: 오늘 날씨 어때?
답변: [웹 검색 후] 의왕시 오늘 23도, 맑아요.

질문: 메일 확인해줘
답변: [GMAIL_LIST:newer_than:30d]

질문: 다시 보내줘
답변: [REPEAT_LAST]

질문: 내일 오전 10시에 운동가라고 알려줘
답변: [SCHEDULE:2026-05-17 10:00|운동 가세요!]

질문: 메모 다 지워줘
답변: [MEMO_DELETE_ALL]

규칙: 위 예시처럼 짧고 자연스럽게.
불필요한 설명, 형식적인 인사, 굵은 글씨 쓰지 말 것."""

IMAGE_SYSTEM_PROMPT = """당신은 15년 경력의 시각 디자인 디렉터입니다.
이미지를 분석할 때 다음 관점에서 전문적이고 구체적인 피드백을 제공합니다:

1. 레이아웃/구도 - 시선 흐름, 정보 계층, 여백 활용
2. 타이포그래피 - 폰트 선택, 가독성, 크기 대비, 줄간격
3. 컬러 - 색 조합, 브랜드 톤, 대비, 감정 전달
4. 메시지 전달력 - 핵심 메시지가 3초 안에 읽히는지
5. 타겟 적합성 - 대상 고객에게 맞는 톤인지
6. 개선 제안 - 구체적으로 "어디를 어떻게" 바꾸면 좋은지

※ CTA(클릭 유도 문구)나 신청 방법 안내가 없는 경우, 없는 이유가 있을 수 있으므로 이에 대한 피드백은 하지 말 것. 명시적으로 CTA 추가를 요청할 때만 관련 제안을 할 것.

피드백 형식:
✅ 잘된 점 (2-3개)
⚠️ 개선 포인트 (구체적 수정 방향 포함)
💡 한 단계 업그레이드 팁

캡션에 사용자의 추가 요청이 있으면 그에 맞춰 분석 방향을 조절하세요.
짧고 임팩트 있게, 실무에서 바로 적용 가능한 수준으로 답하세요."""

INSTAGRAM_SYSTEM_PROMPT = """당신은 SNS 마케팅 13년차 전문가의 인스타그램 콘텐츠 담당 비서입니다.

이미지를 분석한 뒤 인스타그램 게시물 초안을 작성합니다.

작성 규칙:
1. 첫 줄은 시선을 끄는 후킹 문장 (질문형 or 공감형)
2. 본문은 3-5줄, 줄바꿈 활용해서 가독성 확보
3. 이모지는 자연스럽게 섞되 거의 안쓰기
4. 마지막에 CTA (댓글 유도, 저장 유도 등)
5. 해시태그 5개
6. 톤: 전문적이면서 친근한 강사 느낌

캡션에 추가 지시가 있으면 (예: "강의 후기 느낌으로", "홍보용으로") 그에 맞춰 조절.

출력 형식:
📱 인스타그램 초안
---
[캡션 본문]

[해시태그]
---
💡 이 초안의 포인트: (왜 이렇게 썼는지 한 줄 설명)"""

MARKETING_SYSTEM_PROMPT = """당신은 SNS 마케팅 13년 경력의 마케터입니다.
이미지를 마케터 관점에서 분석하고 실무적인 피드백을 제공합니다.

분석 관점:
1. 타겟 오디언스 - 누가 이 콘텐츠를 봐야 하는가
2. 메시지 명확성 - 핵심 메시지가 즉시 전달되는가
3. 클릭/전환 유도 - CTA가 있는가, 행동을 유도하는가
4. 브랜드 일관성 - 브랜드 톤앤매너와 맞는가
5. 경쟁력 - 비슷한 콘텐츠 대비 차별점이 있는가
6. 개선 제안 - 마케팅 성과를 높이기 위한 구체적 액션

출력 형식:
🎯 타겟 분석
📢 메시지 전달력
🔥 강점
⚠️ 개선 포인트
📈 마케팅 성과를 높이는 제안"""

YOUTUBE_SYSTEM_PROMPT = """당신은 SNS 마케팅 13년차 전문가의 유튜브 콘텐츠 담당 비서입니다.

이미지 또는 주제를 바탕으로 유튜브 쇼츠 스크립트를 작성합니다.

작성 규칙:
1. 훅(Hook): 첫 3초 안에 시청자를 잡는 강렬한 오프닝 (질문형/충격적 사실/공감)
2. 본문: 핵심 내용 3-5개 포인트, 각 포인트 15-20초 분량
3. CTA: 마지막에 구독/좋아요/저장 유도 문구
4. 전체 60초 이내 분량
5. 각 파트에 화면 연출 가이드 포함 (예: [화면: 강사 정면 샷])
6. 톤: 에너지 있고 빠른 호흡, 전문적이면서 친근하게

출력 형식:
🎬 유튜브 쇼츠 스크립트
---
[🎣 훅 - 0~3초]
(대사)
[화면: ○○]

[📌 본문 - 3~50초]
포인트1: (대사)
[화면: ○○]

[🔔 CTA - 50~60초]
(대사)
---
💡 제작 팁: (이 스크립트의 핵심 포인트 한 줄)"""

ANALYSIS_KEYWORDS = {
    "분석", "피드백", "인스타", "캡션", "마케팅", "마케터",
    "평가", "리뷰", "봐줘", "어때",
    "유튜브", "쇼츠", "스크립트", "대본",
}

def _select_image_prompt(text):
    if any(kw in text for kw in ("유튜브", "쇼츠", "스크립트", "대본")):
        return YOUTUBE_SYSTEM_PROMPT, text or "이 이미지를 바탕으로 유튜브 쇼츠 스크립트를 작성해주세요"
    elif any(kw in text for kw in ("인스타", "캡션", "게시물")):
        return INSTAGRAM_SYSTEM_PROMPT, text or "이 이미지를 바탕으로 인스타그램 게시물 초안을 작성해주세요"
    elif any(kw in text for kw in ("마케팅", "마케터")):
        return MARKETING_SYSTEM_PROMPT, text or "마케터 관점에서 이 이미지를 분석해주세요"
    else:
        return IMAGE_SYSTEM_PROMPT, text or "이 디자인을 전문적으로 분석하고 피드백해주세요"

async def _call_vision(b64, mime_type, system_prompt, prompt, update):
    for attempt in range(2):
        try:
            r = await client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": mime_type, "data": b64}},
                    {"type": "text", "text": prompt},
                ]}],
            )
            result = r.content[0].text if r.content else "분석 결과 없음"
            if len(result) > 4096:
                for i in range(0, len(result), 4096):
                    await update.message.reply_text(result[i:i+4096])
            else:
                await update.message.reply_text(result)
            return
        except Exception as e:
            logger.error(f"Vision error (attempt {attempt + 1}): {e}")
            if attempt == 0:
                await asyncio.sleep(2)
            else:
                await update.message.reply_text("잠시 서버가 바빠요. 다시 말씀해주시면 바로 답변드릴게요! 🙏")

def is_authorized(uid):
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS

user_search_results = defaultdict(list)
user_gmail_list = defaultdict(list)
user_last_action = defaultdict(dict)
user_last_photo = {}
user_mail_offset = {}    # uid -> 다음 표시 시작 인덱스 (0-based)
user_mail_token = {}     # uid -> nextPageToken (None이면 마지막 페이지)
user_mail_query_store = {}  # uid -> 현재 페이지네이션 중인 Gmail 쿼리

_GPT_BOTH_SYSTEM = (
    "반드시 2025년 이후 최신 자료만 사용할 것. "
    "오래된 자료(2024년 이전)는 사용 금지. "
    "검색 시 2026년 기준 최신 정보 우선. "
    "날짜가 명확한 자료만 인용할 것."
)

_CLAUDE_BOTH_SYSTEM = (
    "최신 정보를 웹에서 검색하여 답변할 것. "
    "검색 결과는 최신순으로 정렬. "
    "각 정보마다 출처 URL 반드시 포함. "
    "최소 3개 이상의 실제 출처 기반으로 답변할 것. "
    "추측이나 일반 지식으로 때우지 말 것."
)

async def ask_gpt(message):
    if not openai_client:
        return "❌ OPENAI_API_KEY 미설정"
    try:
        prefixed = f"[2026년 최신 정보 기준으로 답변] {message}"
        r = await openai_client.chat.completions.create(
            model="gpt-4o-search-preview",
            messages=[
                {"role": "system", "content": _GPT_BOTH_SYSTEM},
                {"role": "user", "content": prefixed},
            ],
            max_completion_tokens=2048,
        )
        return r.choices[0].message.content or "응답 없음"
    except Exception as e:
        logger.error(f"GPT error: {e}")
        return f"❌ GPT 오류: {e}"

async def ask_both(question):
    claude_task = asyncio.create_task(ask_claude_simple(question))
    gpt_task = asyncio.create_task(ask_gpt(question))
    claude_ans, gpt_ans = await asyncio.gather(claude_task, gpt_task)
    return f"🧠 Claude:\n{claude_ans}\n\n---\n\n🤖 GPT-4o Search:\n{gpt_ans}"

async def ask_claude_simple(message):
    """대화 이력 없이 단순 Claude 호출 (both 전용)"""
    for attempt in range(2):
        try:
            r = await client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                system=_CLAUDE_BOTH_SYSTEM,
                tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
                messages=[{"role": "user", "content": message}],
            )
            parts = [b.text for b in r.content if b.type == "text"]
            return "\n".join(parts) if parts else "응답 없음"
        except Exception as e:
            logger.error(f"Claude simple error (attempt {attempt + 1}): {e}")
            if attempt == 0:
                await asyncio.sleep(2)
            else:
                return "잠시 서버가 바빠요. 다시 말씀해주시면 바로 답변드릴게요! 🙏"

async def cmd_both(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("Access denied.")
        return
    question = " ".join(context.args).strip()
    if not question:
        await update.message.reply_text("사용법: /both [질문]\n예: /both 인스타 팔로워 늘리는 방법")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    result = await ask_both(question)
    for i in range(0, len(result), 4096):
        await update.message.reply_text(result[i:i+4096])

async def ask_claude(user_id, message):
    history = conversation_history[user_id]
    memos = get_memos_for_prompt(user_id)
    system = SYSTEM_PROMPT + memos

    history.append({"role": "user", "content": f"[현재 KST: {kst_now().strftime('%Y-%m-%d %H:%M')}]\n{message}"})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
    for attempt in range(2):
        try:
            r = await client.messages.create(
                model="claude-sonnet-4-6", max_tokens=4096,
                system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
                tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
                messages=history,
            )
            text_parts = [block.text for block in r.content if block.type == "text"]
            txt = "\n".join(text_parts) if text_parts else "응답 없음"
            history.append({"role": "assistant", "content": txt})
            return txt
        except Exception as e:
            logger.error(f"Claude error (attempt {attempt + 1}): {e}")
            if attempt == 0:
                await asyncio.sleep(2)
            else:
                if history and history[-1]["role"] == "user":
                    history.pop()
                return "잠시 서버가 바빠요. 다시 말씀해주시면 바로 답변드릴게요! 🙏"

# ── 알림 체크 (1분마다)
async def check_schedules(app):
    pending = get_pending_schedules()
    for schedule_id, user_id, message in pending:
        try:
            await app.bot.send_message(chat_id=user_id, text=f"⏰ 알림\n\n{message}")
            mark_schedule_sent(schedule_id)
        except Exception as e:
            logger.error(f"Schedule send error: {e}")

# ── Gmail 자동 체크 (1시간마다)
async def check_new_gmail(app):
    if not gmail_service or not ALLOWED_USER_IDS or not DATABASE_URL:
        return
    try:
        emails, _ = get_gmail_list(10, "newer_than:1h")
        if not emails:
            return
        current_ids = [e["id"] for e in emails]
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT mail_id FROM notified_mail_ids WHERE mail_id = ANY(%s)",
                (current_ids,)
            )
            already_notified = {row[0] for row in cur.fetchall()}
            truly_new = [e for e in emails if e["id"] not in already_notified]
            for e in truly_new:
                sender = e["from"].split("<")[0].strip()[:20]
                subject = e["subject"][:30]
                msg = f"📬 새 메일 왔어요!\n\n👤 {sender}\n📌 {subject}"
                for uid in ALLOWED_USER_IDS:
                    await app.bot.send_message(chat_id=uid, text=msg)
                cur.execute(
                    "INSERT INTO notified_mail_ids (mail_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (e["id"],)
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.error(f"Gmail check error: {e}")

async def cmd_start(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return
    d = "✅" if drive_service else "❌"
    g = "✅" if gmail_service else "❌"
    s = "✅" if clova_available else "❌"
    db = "✅" if DATABASE_URL else "❌"
    await update.message.reply_text(
        f"안녕하세요! 👋\n\n"
        f"📁 Drive: {d}  📧 Gmail: {g}  🎙️ 음성: {s}\n"
        f"🔍 웹검색: ✅  🗄️ DB: {db}\n\n"
        f"/files - 파일 목록\n"
        f"/mail - 받은 메일\n"
        f"/memo [내용] - 메모 저장\n"
        f"/memos - 메모 목록\n"
        f"/schedules - 알림 목록\n"
        f"/clear - 초기화\n"
        f"/help - 도움말")

async def cmd_clear(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    conversation_history[uid].clear()
    user_search_results[uid].clear()
    user_gmail_list[uid].clear()
    user_last_action[uid].clear()
    user_last_photo.pop(uid, None)
    user_mail_offset.pop(uid, None)
    user_mail_token.pop(uid, None)
    user_mail_query_store.pop(uid, None)
    await update.message.reply_text("🗑️ 초기화 완료!")

async def cmd_memo(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    text = " ".join(context.args)
    if not text:
        await update.message.reply_text("사용법: /memo [저장할 내용]")
        return
    save_memo(uid, text)
    await update.message.reply_text(f"✅ 메모 저장됨: {text}")

async def cmd_memos(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    memos = get_memos(uid, 10)
    if not memos:
        await update.message.reply_text("저장된 메모 없음")
        return
    msg = "📝 저장된 메모:\n\n"
    for i, (_, content, created_at) in enumerate(memos, 1):
        msg += f"{i}. {content} ({created_at.strftime('%m/%d %H:%M')})\n"
    for i in range(0, len(msg), 4096):
        await update.message.reply_text(msg[i:i+4096])

async def cmd_memodel(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not context.args:
        await update.message.reply_text("사용법: /memodel [번호]\n예: /memodel 2")
        return
    try:
        num = int(context.args[0])
    except ValueError:
        await update.message.reply_text("번호를 입력해주세요. 예: /memodel 2")
        return
    if delete_memo_by_number(uid, num):
        await update.message.reply_text(f"✅ {num}번 메모 삭제 완료!")
    else:
        await update.message.reply_text(f"❌ {num}번 메모를 찾을 수 없어요.")

async def cmd_memoclear(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    count = delete_all_memos(uid)
    if count > 0:
        await update.message.reply_text(f"✅ 메모 {count}개 전체 삭제 완료!")
    else:
        await update.message.reply_text("삭제할 메모가 없어요.")

async def cmd_schedules(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    rows = get_user_schedules(uid)
    if not rows:
        await update.message.reply_text("예정된 알림 없음\n\n예: '내일 오전 10시에 운동가라고 알려줘'")
        return
    msg = "⏰ 예정된 알림:\n\n"
    for i, (_, message, scheduled_at) in enumerate(rows, 1):
        msg += f"{i}. {scheduled_at.strftime('%m/%d %H:%M')} - {message}\n"
    msg += "\n💡 /scheduledel [번호] 로 삭제"
    await update.message.reply_text(msg)

async def cmd_scheduledel(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not context.args:
        await update.message.reply_text("사용법: /scheduledel [번호]\n예: /scheduledel 1")
        return
    try:
        num = int(context.args[0])
    except ValueError:
        await update.message.reply_text("번호를 입력해주세요.")
        return
    if delete_schedule_by_number(uid, num):
        await update.message.reply_text(f"✅ {num}번 알림 삭제 완료!")
    else:
        await update.message.reply_text(f"❌ {num}번 알림을 찾을 수 없어요.")

async def cmd_files(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not drive_service:
        await update.message.reply_text("❌ Drive 미연결")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    files = list_drive_files()
    if not files:
        await update.message.reply_text("📁 파일 없음")
        return
    user_search_results[uid] = files
    msg = "📁 파일 목록:\n\n"
    for i, f in enumerate(files, 1):
        msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
    msg += "\n💡 번호로 전송/이메일 첨부 가능!"
    for i in range(0, len(msg), 4096):
        await update.message.reply_text(msg[i:i+4096])

async def cmd_mail(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not gmail_service:
        await update.message.reply_text("❌ Gmail 미연결")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    query = "newer_than:30d"
    emails, next_token = get_gmail_list(10, query)
    if not emails:
        await update.message.reply_text("📭 최근 30일 메일 없음")
        return
    user_gmail_list[uid] = emails
    user_mail_token[uid] = next_token
    user_mail_query_store[uid] = query
    user_mail_offset[uid] = len(emails)
    has_more = next_token is not None
    msg = _build_mail_msg(emails, 0, 10, has_more)
    for i in range(0, len(msg), 4096):
        await update.message.reply_text(msg[i:i+4096])

async def cmd_help(update, context):
    if not is_authorized(update.effective_user.id): return
    await update.message.reply_text(
        "📖 사용 가이드\n\n"
        "💬 대화: 메시지 보내면 AI 답변\n"
        "🔍 검색: '오늘 뉴스', '코스피' 등\n"
        "📁 파일: '파일 찾아줘', '보내줘', /files\n"
        "📧 이메일: 'abc@gmail.com에 안녕 보내줘'\n"
        "📬 메일: /mail, '받은 메일 보여줘'\n"
        "🎙️ 음성: 음성/m4a 파일 보내면 자동 분석\n"
        "📝 메모: /memo [내용], /memos\n"
        "🗑️ 메모삭제: /memodel [번호], /memoclear\n"
        "🎬 유튜브: '쇼츠 대본 써줘 주제: ○○'\n"
        "⏰ 알림: '내일 10시에 운동가라고 알려줘'\n"
        "📅 알림목록: /schedules\n"
        "❌ 알림삭제: /scheduledel [번호]\n"
        "📰 뉴스: /news 키워드1, 키워드2\n"
        "🔄 반복: '다시 보내줘'\n\n"
        "/clear - 초기화")

async def cmd_news(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not DATABASE_URL:
        await update.message.reply_text("❌ DB 미연결")
        return
    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("사용법: /news 키워드1, 키워드2, 키워드3")
        return
    keywords = [kw.strip() for kw in text.split(",") if kw.strip()]
    save_news_keywords(uid, keywords)
    await update.message.reply_text(
        f"✅ 키워드 저장 완료!\n\n저장된 키워드: {', '.join(keywords)}\n\n매일 오전 10시에 뉴스 브리핑 보내드릴게요."
    )

async def cmd_newslist(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    rows = get_news_keywords(uid)
    if not rows:
        await update.message.reply_text("저장된 키워드 없음\n\n/news 키워드1, 키워드2 로 추가하세요.")
        return
    msg = "📰 저장된 뉴스 키워드:\n\n"
    for i, (_, kw) in enumerate(rows, 1):
        msg += f"{i}. {kw}\n"
    await update.message.reply_text(msg)

async def cmd_newsdel(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    keyword = " ".join(context.args).strip()
    if not keyword:
        await update.message.reply_text("사용법: /newsdel 키워드명")
        return
    deleted = delete_news_keyword(uid, keyword)
    if deleted:
        await update.message.reply_text(f"✅ '{keyword}' 삭제 완료!")
    else:
        await update.message.reply_text(f"❌ '{keyword}' 키워드를 찾을 수 없어요.")

async def _search_one_keyword(kw):
    try:
        r = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}],
            messages=[{"role": "user", "content":
                f"'{kw}' 관련 오늘 또는 최근 뉴스 1건을 검색해서 아래 형식으로만 답해줘:\n"
                f"📅 날짜\n제목\n📌 출처: [매체명](URL)\n→ 한줄 요약\n\n"
                f"형식 외 다른 말은 하지 말 것."
            }],
        )
        text_parts = [b.text for b in r.content if b.type == "text"]
        return kw, "\n".join(text_parts) if text_parts else "검색 결과 없음"
    except Exception as e:
        logger.error(f"News search error for '{kw}': {e}")
        return kw, "검색 실패"

async def send_news_briefing(app):
    if not ALLOWED_USER_IDS or not DATABASE_URL:
        return
    today = kst_now().strftime("%Y.%m.%d")
    all_rows = get_news_keywords()
    user_keywords = defaultdict(list)
    for user_id, keyword in all_rows:
        user_keywords[user_id].append(keyword)

    for uid in ALLOWED_USER_IDS:
        keywords = user_keywords.get(uid, [])
        if not keywords:
            continue
        try:
            results = await asyncio.gather(*[_search_one_keyword(kw) for kw in keywords])
            briefing = f"📰 오늘의 뉴스 브리핑 ({today})\n"
            for kw, result in results:
                briefing += f"\n🔍 [{kw}]\n{result}\n"
            for i in range(0, len(briefing), 4096):
                await app.bot.send_message(chat_id=uid, text=briefing[i:i+4096])
        except Exception as e:
            logger.error(f"News briefing send error for uid {uid}: {e}")

async def cmd_briefing(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    await update.message.reply_text("📰 뉴스 브리핑 시작합니다...")
    await send_news_briefing(context.application)

async def handle_voice(update, context):
    u = update.effective_user
    if not is_authorized(u.id): return
    if not clova_available:
        await update.message.reply_text("❌ 음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 분석 중...")
    voice = update.message.voice or update.message.audio
    try:
        f = await context.bot.get_file(voice.file_id)
        data = await f.download_as_bytearray()
    except Exception as e:
        logger.error(f"Voice download error: {e}")
        await update.message.reply_text("❌ 파일 다운로드 실패")
        return
    loop = asyncio.get_running_loop()
    # 일반 음성 메시지: 단일 화자 → diarization 비활성화
    txt = await loop.run_in_executor(None, transcribe_audio, bytes(data), voice.mime_type or "audio/ogg", False)
    if txt:
        transcript = f"📝 텍스트:\n\n{txt}"
        for i in range(0, len(transcript), 4096):
            await update.message.reply_text(transcript[i:i+4096])
        analysis = await ask_claude(u.id, f"다음 음성 내용을 분석하고 요약해줘:\n\n{txt}")
        full = f"🔍 분석:\n\n{analysis}"
        for i in range(0, len(full), 4096):
            await update.message.reply_text(full[i:i+4096])
    else:
        await update.message.reply_text("❌ 음성 인식 실패")

async def handle_audio_file(update, context):
    u = update.effective_user
    if not is_authorized(u.id): return
    if not clova_available:
        await update.message.reply_text("❌ 음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 분석 중... (잠시 기다려주세요)")
    audio = update.message.audio or update.message.document
    try:
        f = await context.bot.get_file(audio.file_id)
        data = await f.download_as_bytearray()
    except Exception as e:
        logger.error(f"Audio download error: {e}")
        await update.message.reply_text("❌ 파일 다운로드 실패")
        return
    loop = asyncio.get_running_loop()
    # 파일 업로드 음성: 통화 녹음 가능성 → diarization 활성화
    txt = await loop.run_in_executor(None, transcribe_audio, bytes(data), audio.mime_type or "audio/mpeg", True)
    if txt:
        if len(txt) > 3000:
            for i in range(0, len(txt), 3000):
                await update.message.reply_text(f"📝 ({i//3000+1}):\n\n{txt[i:i+3000]}")
        else:
            await update.message.reply_text(f"📝 텍스트:\n\n{txt}")
        analysis = await ask_claude(u.id,
            f"통화/음성 녹음입니다. 핵심 요약하고 중요 포인트 정리해줘:\n\n{txt}")
        full = f"🔍 분석:\n\n{analysis}"
        for i in range(0, len(full), 4096):
            await update.message.reply_text(full[i:i+4096])
    else:
        await update.message.reply_text("❌ 음성 인식 실패")

async def handle_photo(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return

    try:
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            mime_type = "image/jpeg"
        else:
            doc = update.message.document
            file_id = doc.file_id
            mime_type = doc.mime_type or "image/jpeg"

        try:
            tg_file = await context.bot.get_file(file_id)
            data = await tg_file.download_as_bytearray()
            b64 = base64.b64encode(bytes(data)).decode("utf-8")
        except Exception as e:
            logger.error(f"Image download error: {e}")
            await update.message.reply_text("❌ 이미지 다운로드 실패")
            return

        # TTL과 함께 저장 (메모리 누수 방지)
        user_last_photo[u.id] = {"b64": b64, "mime_type": mime_type, "ts": datetime.utcnow()}

        caption = (update.message.caption or "").strip()
        if caption:
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
            system_prompt, prompt = _select_image_prompt(caption)
            await _call_vision(b64, mime_type, system_prompt, prompt, update)
            user_last_photo.pop(u.id, None)
        else:
            await update.message.reply_text(
                "📸 이미지 수신 완료! 분석 방법을 말씀해주세요.\n"
                "예: 디자인 피드백, 인스타 캡션, 마케팅 분석, 유튜브 쇼츠 대본"
            )

    except Exception as e:
        logger.error(f"handle_photo error: {e}")
        await update.message.reply_text(f"❌ 이미지 처리 중 오류: {e}")

async def handle_message(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return

    text = update.message.text or ""

    # 10분 초과 사진 TTL 정리
    if u.id in user_last_photo:
        age = (datetime.utcnow() - user_last_photo[u.id]["ts"]).total_seconds()
        if age > 600:
            user_last_photo.pop(u.id)

    # 이전에 받은 이미지 + 분석 키워드 감지 → vision 호출
    if u.id in user_last_photo and any(kw in text for kw in ANALYSIS_KEYWORDS):
        photo_data = user_last_photo.pop(u.id)
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        try:
            system_prompt, prompt = _select_image_prompt(text)
            await _call_vision(photo_data["b64"], photo_data["mime_type"], system_prompt, text, update)
        except Exception as e:
            logger.error(f"Vision from message error: {e}")
            await update.message.reply_text(f"❌ 이미지 분석 오류: {e}")
        return

    # 양쪽 AI 비교 키워드 감지
    if any(kw in text for kw in BOTH_KEYWORDS):
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        result = await ask_both(text)
        for i in range(0, len(result), 4096):
            await update.message.reply_text(result[i:i+4096])
        return

    # ── "N번 읽어줘/보여줘" 또는 그냥 "N번" → 최근 메일 목록의 N번째 메일 내용 바로 열기
    _num_m = re.search(r"(\d+)\s*번", text)
    _read_intent = any(w in text for w in ["읽", "열", "보여", "내용", "봐"])
    _bare_num = re.fullmatch(r"\s*\d+\s*(번|번째)?\s*", text) is not None
    _other_list = any(w in text for w in ["메모", "저서", "파일", "일정", "알림", "스케줄"])
    if (_num_m or _bare_num) and (_read_intent or _bare_num) and not _other_list and user_gmail_list.get(u.id):
        _emails = user_gmail_list.get(u.id, [])
        _digits = re.search(r"\d+", text)
        idx = int(_digits.group()) - 1 if _digits else -1
        if 0 <= idx < len(_emails):
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
            content = get_gmail_content(_emails[idx]["id"])
            if content:
                body_display = content.get("body_preview") or content.get("body", "")[:1000] or "본문 없음"
                msg = (f"📧 메일 내용\n\n"
                       f"👤 {content['from']}\n"
                       f"📌 {content['subject']}\n"
                       f"📅 {content['date']}\n\n"
                       f"📝 내용:\n{body_display}")
                for i in range(0, len(msg), 4096):
                    await update.message.reply_text(msg[i:i+4096])
            else:
                await update.message.reply_text("❌ 메일을 여는 데 실패했어요. 다시 시도해주세요.")
        else:
            await update.message.reply_text(f"❌ 목록에 {idx+1}번이 없어요. 메일 목록 번호(1~{len(_emails)})를 확인해주세요.")
        return

    # ── 메일 확인 요청은 AI 판단 없이 바로 Gmail 조회 (가짜 변명 차단)
    _t = text.replace(" ", "")
    _is_send = ("보내" in _t) or ("전송" in _t) or ("@" in text)
    _is_read_num = bool(re.search(r"\d+\s*번", text)) and any(w in text for w in ["읽", "열", "내용"])
    _mail_word = ("메일" in _t) or ("이메일" in _t) or ("메일함" in _t)
    _check_word = any(w in _t for w in [
        "확인", "보여", "봐줘", "봐봐", "왔", "온거", "온게", "온것", "받은",
        "체크", "열어", "열려", "못열", "최근", "새메일", "안읽", "목록", "있나", "있어"
    ])
    if _mail_word and _check_word and not _is_send and not _is_read_num:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        if ("안읽" in _t) or ("안 읽" in text):
            query = "is:unread"
        elif "오늘" in _t:
            query = "newer_than:1d"
        else:
            query = "newer_than:30d"
        emails, next_token = get_gmail_list(10, query)
        if emails:
            user_gmail_list[u.id] = emails
            user_mail_token[u.id] = next_token
            user_mail_query_store[u.id] = query
            user_mail_offset[u.id] = len(emails)
            has_more = next_token is not None
            msg = _build_mail_msg(emails, 0, 10, has_more)
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])
        else:
            await update.message.reply_text("📭 해당 조건에 맞는 메일이 없어요.")
        return

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    resp = await ask_claude(u.id, text)

    if "[REPEAT_LAST]" in resp:
        last = user_last_action.get(u.id, {})
        if not last:
            await update.message.reply_text("반복할 이전 작업이 없어요.")
            return
        if last["type"] == "file":
            await update.message.reply_text(f"📤 '{last['name']}' 다시 전송 중...")
            buf, name = download_drive_file(last["file_id"])
            if buf:
                await update.message.reply_document(document=buf, filename=name, caption=f"📄 {name}")
        elif last["type"] == "email":
            await update.message.reply_text(f"📧 {last['to']}로 다시 전송 중...")
            buf = None
            if last.get("file_id"):
                buf, _ = download_drive_file(last["file_id"])
            ok, msg = send_gmail(last["to"], last["subject"], last["body"], buf, last.get("file_name"))
            if ok:
                await update.message.reply_text(f"✅ {last['to']}로 전송 완료!")
            else:
                await update.message.reply_text(f"❌ 전송 실패: {msg}")
        return

    if "[DRIVE_SEARCH:" in resp:
        m = re.search(r"\[DRIVE_SEARCH:(.*)\]", resp)
        kw = m.group(1) if m else ""
        files = search_drive_files(kw)
        if files:
            user_search_results[u.id] = files
            msg = f"🔍 '{kw}' 검색 결과:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            msg += "\n💡 번호로 전송/이메일 첨부 가능!"
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])
        else:
            await update.message.reply_text(f"'{kw}' 결과 없음")

    elif "[DRIVE_LIST]" in resp:
        files = list_drive_files()
        if files:
            user_search_results[u.id] = files
            msg = "📁 파일 목록:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])
        else:
            await update.message.reply_text("파일 없음")

    elif "[DRIVE_SEND:" in resp:
        try:
            num = int(resp.split("[DRIVE_SEND:")[1].split("]")[0]) - 1
            files = user_search_results.get(u.id, [])
            if 0 <= num < len(files):
                fi = files[num]
                await update.message.reply_text(f"📤 '{fi['name']}' 전송 중...")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_document")
                    await update.message.reply_document(document=buf, filename=name, caption=f"📄 {name}")
                    user_last_action[u.id] = {"type": "file", "file_id": fi["id"], "name": name}
                else:
                    await update.message.reply_text("❌ 다운로드 실패")
            else:
                await update.message.reply_text("❌ 잘못된 번호")
        except Exception as e:
            logger.error(f"Drive send error: {e}")
            await update.message.reply_text("❌ 번호 확인 필요")

    elif "[EMAIL_WITH_FILE:" in resp:
        try:
            m = re.search(r"\[EMAIL_WITH_FILE:(.*?)\]", resp, re.DOTALL)
            parts = m.group(1).split("|", 3) if m else []
            to_addr, subject, body, fnum = parts[0], parts[1], parts[2], int(parts[3]) - 1
            files = user_search_results.get(u.id, [])
            if 0 <= fnum < len(files):
                fi = files[fnum]
                await update.message.reply_text(f"📧 '{fi['name']}' 첨부하여 전송 중...")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    ok, msg = send_gmail(to_addr, subject, body, buf, name)
                    if ok:
                        await update.message.reply_text(f"✅ {to_addr}로 전송 완료!")
                        user_last_action[u.id] = {"type": "email", "to": to_addr, "subject": subject, "body": body, "file_id": fi["id"], "file_name": name}
                    else:
                        await update.message.reply_text(f"❌ 전송 실패: {msg}")
                else:
                    await update.message.reply_text("❌ 파일 다운로드 실패")
            else:
                await update.message.reply_text("❌ 잘못된 파일 번호")
        except Exception as e:
            logger.error(f"Email+file error: {e}")
            await update.message.reply_text(f"❌ 이메일 전송 실패: {e}")

    elif "[EMAIL:" in resp:
        try:
            m = re.search(r"\[EMAIL:(.*?)\]", resp, re.DOTALL)
            parts = m.group(1).split("|", 2) if m else []
            to_addr, subject, body = parts[0].strip(), parts[1].strip(), parts[2].strip()
            await update.message.reply_text(f"📧 {to_addr}로 전송 중...")
            ok, msg = send_gmail(to_addr, subject, body)
            if ok:
                await update.message.reply_text(f"✅ 메일 전송 완료 ({to_addr})")
                user_last_action[u.id] = {"type": "email", "to": to_addr, "subject": subject, "body": body}
            else:
                await update.message.reply_text(f"❌ 전송 실패: {msg}")
        except Exception as e:
            logger.error(f"Email error: {e}")
            await update.message.reply_text(f"❌ 이메일 전송 실패: {e}")

    elif "[GMAIL_LIST:" in resp:
        query = resp.split("[GMAIL_LIST:")[1].split("]")[0]
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        emails, next_token = get_gmail_list(10, query)
        if emails:
            user_gmail_list[u.id] = emails
            user_mail_token[u.id] = next_token
            user_mail_query_store[u.id] = query
            user_mail_offset[u.id] = len(emails)
            has_more = next_token is not None
            msg = _build_mail_msg(emails, 0, 10, has_more)
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])
        else:
            await update.message.reply_text("📭 메일 없음")

    elif "[GMAIL_READ:" in resp:
        try:
            num = int(resp.split("[GMAIL_READ:")[1].split("]")[0]) - 1
            emails = user_gmail_list.get(u.id, [])
            if 0 <= num < len(emails):
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
                content = get_gmail_content(emails[num]["id"])
                if content:
                    body_display = content.get("body_preview") or content["body"][:1000] or "본문 없음"
                    msg = (f"📧 메일 내용\n\n"
                           f"👤 {content['from']}\n"
                           f"📌 {content['subject']}\n"
                           f"📅 {content['date']}\n\n"
                           f"📝 내용:\n{body_display}")
                    if len(msg) > 4096:
                        for i in range(0, len(msg), 4096):
                            await update.message.reply_text(msg[i:i+4096])
                    else:
                        await update.message.reply_text(msg)
                    summary = await ask_claude(u.id, f"이 메일 내용을 간단히 요약해줘:\n{content['body']}")
                    full_summary = f"🔍 요약: {summary}"
                    for i in range(0, len(full_summary), 4096):
                        await update.message.reply_text(full_summary[i:i+4096])
                else:
                    await update.message.reply_text("❌ 메일 읽기 실패")
            else:
                await update.message.reply_text("❌ 잘못된 번호")
        except Exception as e:
            logger.error(f"Gmail read error: {e}")
            await update.message.reply_text("❌ 번호 확인 필요")

    elif "[GMAIL_MORE]" in resp:
        uid = u.id
        query = user_mail_query_store.get(uid, "newer_than:30d")
        start = user_mail_offset.get(uid, 0)
        end = start + 10
        emails = _fetch_until(uid, end, query)
        page = emails[start:end]
        if not page:
            await update.message.reply_text("📭 더 이상 메일이 없어요.")
        else:
            user_mail_offset[uid] = start + len(page)
            has_more = bool(user_mail_token.get(uid)) or len(emails) > end
            msg = _build_mail_msg(emails, start, end, has_more)
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])

    elif "[GMAIL_UNTIL:" in resp:
        try:
            n = int(resp.split("[GMAIL_UNTIL:")[1].split("]")[0])
            uid = u.id
            query = user_mail_query_store.get(uid, "newer_than:30d")
            start = user_mail_offset.get(uid, 0)
            emails = _fetch_until(uid, n, query)
            page = emails[start:n]
            if not page:
                await update.message.reply_text("📭 해당 범위에 메일이 없어요.")
            else:
                user_mail_offset[uid] = start + len(page)
                has_more = bool(user_mail_token.get(uid)) or len(emails) > n
                msg = _build_mail_msg(emails, start, n, has_more)
                for i in range(0, len(msg), 4096):
                    await update.message.reply_text(msg[i:i+4096])
        except Exception as e:
            logger.error(f"Gmail until error: {e}")
            await update.message.reply_text("❌ 메일 불러오기 실패")

    elif "[MEMO_DELETE_ALL]" in resp:
        count = delete_all_memos(u.id)
        if count > 0:
            await update.message.reply_text(f"✅ 메모 {count}개 전체 삭제 완료!")
        else:
            await update.message.reply_text("삭제할 메모가 없어요.")

    elif "[MEMO_DELETE:" in resp:
        try:
            num = int(resp.split("[MEMO_DELETE:")[1].split("]")[0])
            if delete_memo_by_number(u.id, num):
                await update.message.reply_text(f"✅ {num}번 메모 삭제 완료!")
            else:
                await update.message.reply_text(f"❌ {num}번 메모를 찾을 수 없어요.")
        except Exception as e:
            logger.error(f"Memo delete error: {e}")
            await update.message.reply_text("❌ 메모 삭제 실패")

    elif "[SCHEDULE:" in resp:
        try:
            m = re.search(r"\[SCHEDULE:(.*)\]", resp)
            inner = m.group(1) if m else ""
            dt_str, msg_content = inner.split("|", 1)
            scheduled_at = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M")
            if scheduled_at <= kst_now():
                await update.message.reply_text("⚠️ 이미 지난 시간이에요. 다시 설정해주세요.")
            elif save_schedule(u.id, msg_content.strip(), scheduled_at):
                await update.message.reply_text(
                    f"⏰ 알림 설정 완료!\n\n"
                    f"📅 {scheduled_at.strftime('%Y.%m.%d %H:%M')}\n"
                    f"📝 {msg_content.strip()}"
                )
            else:
                await update.message.reply_text("❌ 알림 저장 실패")
        except Exception as e:
            logger.error(f"Schedule error: {e}")
            await update.message.reply_text("❌ 알림 설정 실패. 날짜 형식을 확인해주세요.")

    else:
        if len(resp) > 4096:
            for i in range(0, len(resp), 4096):
                await update.message.reply_text(resp[i:i+4096])
        else:
            await update.message.reply_text(resp)

def main():
    init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("both", cmd_both))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("memo", cmd_memo))
    app.add_handler(CommandHandler("memos", cmd_memos))
    app.add_handler(CommandHandler("memodel", cmd_memodel))
    app.add_handler(CommandHandler("memoclear", cmd_memoclear))
    app.add_handler(CommandHandler("schedules", cmd_schedules))
    app.add_handler(CommandHandler("scheduledel", cmd_scheduledel))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("mail", cmd_mail))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("newslist", cmd_newslist))
    app.add_handler(CommandHandler("newsdel", cmd_newsdel))
    app.add_handler(CommandHandler("briefing", cmd_briefing))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.IMAGE, handle_photo))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.AUDIO, handle_audio_file))
    app.add_handler(MessageHandler(
        filters.Document.MimeType("audio/mpeg") |
        filters.Document.MimeType("audio/mp4") |
        filters.Document.MimeType("audio/ogg") |
        filters.Document.MimeType("audio/wav") |
        filters.Document.MimeType("audio/x-m4a") |
        filters.Document.MimeType("video/mp4"),
        handle_audio_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info(f"Handlers registered: {len(app.handlers[0])}")

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_schedules, "interval", minutes=1, args=[app])
    scheduler.add_job(check_new_gmail, "interval", minutes=60, args=[app])
    scheduler.add_job(send_news_briefing, "cron", hour=1, minute=0, args=[app])  # 01:00 UTC = 10:00 KST
    scheduler.start()

    logger.info("Bot started! (Drive + Gmail + Speech + Web Search + DB + Mail Alert + Schedules)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
