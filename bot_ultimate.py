import os, json, logging, io, base64, tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from collections import defaultdict

import psycopg2
from psycopg2.extras import Json

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import anthropic
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ━━━━━━━━━━━━━━━━━━━━━━━
# 환경변수
# ━━━━━━━━━━━━━━━━━━━━━━━
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

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
MAX_HISTORY = 20

# ━━━━━━━━━━━━━━━━━━━━━━━
# PostgreSQL
# ━━━━━━━━━━━━━━━━━━━━━━━
def get_db():
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

def init_db():
    conn = get_db()
    if not conn:
        logger.warning("DATABASE_URL not set — in-memory mode")
        return False
    try:
        cur = conn.cursor()
        # 기존 테이블 스키마 문제 자동 수정
        cur.execute("""
            DROP TABLE IF EXISTS conversation_history;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversation_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_conv_user ON conversation_history(user_id);
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_state (
                user_id BIGINT PRIMARY KEY,
                search_results JSONB DEFAULT '[]',
                gmail_list JSONB DEFAULT '[]',
                last_action JSONB DEFAULT '{}'
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS memos (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_memo_user ON memos(user_id);
        """)
        conn.close()
        logger.info("DB tables initialized!")
        return True
    except Exception as e:
        logger.error(f"DB init error: {e}")
        conn.close()
        return False

# 메모리 fallback
_mem_history = defaultdict(list)
_mem_search = defaultdict(list)
_mem_gmail = defaultdict(list)
_mem_action = defaultdict(dict)
db_available = False

# ━━━━━━━━━━━━━━━━━━━━━━━
# DB 헬퍼
# ━━━━━━━━━━━━━━━━━━━━━━━
def db_get_history(user_id):
    if not db_available:
        return list(_mem_history[user_id])
    conn = get_db()
    if not conn:
        return list(_mem_history[user_id])
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT role, content FROM conversation_history WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            (user_id, MAX_HISTORY)
        )
        rows = cur.fetchall()
        conn.close()
        rows.reverse()
        return [{"role": r[0], "content": r[1]} for r in rows]
    except Exception as e:
        logger.error(f"db_get_history: {e}")
        conn.close()
        return []

def db_add_message(user_id, role, content):
    if not db_available:
        _mem_history[user_id].append({"role": role, "content": content})
        if len(_mem_history[user_id]) > MAX_HISTORY:
            _mem_history[user_id] = _mem_history[user_id][-MAX_HISTORY:]
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO conversation_history (user_id, role, content) VALUES (%s, %s, %s)",
            (user_id, role, content)
        )
        conn.close()
    except Exception as e:
        logger.error(f"db_add_message: {e}")
        conn.close()

def db_clear_history(user_id):
    if not db_available:
        _mem_history[user_id].clear()
        _mem_search[user_id].clear()
        _mem_gmail[user_id].clear()
        _mem_action[user_id].clear()
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM conversation_history WHERE user_id=%s", (user_id,))
        cur.execute("""
            INSERT INTO user_state (user_id) VALUES (%s)
            ON CONFLICT (user_id) DO UPDATE
            SET search_results='[]', gmail_list='[]', last_action='{}'
        """, (user_id,))
        conn.close()
    except Exception as e:
        logger.error(f"db_clear_history: {e}")
        conn.close()

def db_get_state(user_id, field):
    if not db_available:
        if field == "search_results":
            return list(_mem_search[user_id])
        if field == "gmail_list":
            return list(_mem_gmail[user_id])
        if field == "last_action":
            return dict(_mem_action[user_id])
        return None
    conn = get_db()
    if not conn:
        return [] if field != "last_action" else {}
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {field} FROM user_state WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return row[0] if row[0] is not None else ([] if field != "last_action" else {})
        return [] if field != "last_action" else {}
    except Exception as e:
        logger.error(f"db_get_state {field}: {e}")
        conn.close()
        return [] if field != "last_action" else {}

def db_set_state(user_id, field, value):
    if not db_available:
        if field == "search_results":
            _mem_search[user_id] = value
        elif field == "gmail_list":
            _mem_gmail[user_id] = value
        elif field == "last_action":
            _mem_action[user_id] = value
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO user_state (user_id, {field}) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET {field}=%s
        """, (user_id, Json(value), Json(value)))
        conn.close()
    except Exception as e:
        logger.error(f"db_set_state {field}: {e}")
        conn.close()

# ━━━━━━━━━━━━━━━━━━━━━━━
# Google Drive + Speech
# ━━━━━━━━━━━━━━━━━━━━━━━
drive_service = None
speech_service = None

if GOOGLE_CREDENTIALS_JSON:
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        sa_creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/cloud-platform",
            ]
        )
        drive_service = build("drive", "v3", credentials=sa_creds)
        speech_service = build("speech", "v1", credentials=sa_creds)
        logger.info("Drive + Speech connected!")
    except Exception as e:
        logger.error(f"Drive/Speech error: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━
# Gmail
# ━━━━━━━━━━━━━━━━━━━━━━━
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

# ━━━━━━━━━━━━━━━━━━━━━━━
# Drive 함수
# ━━━━━━━━━━━━━━━━━━━━━━━
def search_drive_files(query_text, max_results=10):
    if not drive_service:
        return []
    try:
        q = f"name contains '{query_text}' and trashed = false"
        r = drive_service.files().list(
            q=q, pageSize=max_results,
            fields="files(id, name, mimeType, modifiedTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        return r.get("files", [])
    except Exception as e:
        logger.error(f"Drive search error: {e}")
        return []

def list_drive_files(max_results=20):
    if not drive_service:
        return []
    try:
        r = drive_service.files().list(
            pageSize=max_results,
            fields="files(id, name, mimeType, modifiedTime)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
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
                "application/vnd.google-apps.spreadsheet": (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
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

# ━━━━━━━━━━━━━━━━━━━━━━━
# Gmail 함수
# ━━━━━━━━━━━━━━━━━━━━━━━
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
            part.add_header("Content-Disposition", "attachment", filename=("utf-8", "", attach_name))
            msg.attach(part)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        gmail_service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True, "OK"
    except Exception as e:
        logger.error(f"Gmail send error: {e}")
        return False, str(e)

def get_gmail_list(max_results=10, query="is:unread"):
    if not gmail_service:
        return []
    try:
        result = gmail_service.users().messages().list(
            userId="me", maxResults=max_results, q=query
        ).execute()
        messages = result.get("messages", [])
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
            })
        return emails
    except Exception as e:
        logger.error(f"Gmail list error: {e}")
        return []

def get_gmail_content(msg_id):
    if not gmail_service:
        return None
    try:
        msg = gmail_service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute()
        headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
        body = ""
        if "parts" in msg["payload"]:
            for part in msg["payload"]["parts"]:
                if part["mimeType"] == "text/plain":
                    data = part["body"].get("data", "")
                    body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
                    break
        else:
            data = msg["payload"]["body"].get("data", "")
            if data:
                body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        return {
            "from": headers.get("From", ""),
            "subject": headers.get("Subject", ""),
            "date": headers.get("Date", ""),
            "body": body[:3000],
        }
    except Exception as e:
        logger.error(f"Gmail read error: {e}")
        return None

# ━━━━━━━━━━━━━━━━━━━━━━━
# 음성 인식
# ━━━━━━━━━━━━━━━━━━━━━━━
def transcribe_audio(audio_bytes, mime_type="audio/ogg"):
    if not speech_service:
        return None
    try:
        from pydub import AudioSegment
        ext_map = {
            "audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/mp3": ".mp3",
            "audio/mp4": ".m4a", "audio/x-m4a": ".m4a", "audio/m4a": ".m4a",
            "audio/wav": ".wav", "audio/x-wav": ".wav", "video/mp4": ".mp4",
        }
        ext = ext_map.get(mime_type, ".mp3")
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
            f.write(audio_bytes)
            tmp_in = f.name
        tmp_out = tmp_in + ".wav"
        audio = AudioSegment.from_file(tmp_in)
        audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)
        audio.export(tmp_out, format="wav")
        with open(tmp_out, "rb") as f:
            wav_bytes = f.read()
        os.unlink(tmp_in)
        os.unlink(tmp_out)
        body = {
            "config": {
                "encoding": "LINEAR16",
                "sampleRateHertz": 16000,
                "languageCode": "ko-KR",
                "alternativeLanguageCodes": ["en-US"],
                "enableAutomaticPunctuation": True,
                "model": "latest_long",
                "useEnhanced": True,
            },
            "audio": {"content": base64.b64encode(wav_bytes).decode("utf-8")},
        }
        resp = speech_service.speech().recognize(body=body).execute()
        results = resp.get("results", [])
        if results:
            return " ".join(
                r["alternatives"][0]["transcript"]
                for r in results if r.get("alternatives")
            )
        return "음성 인식 결과 없음"
    except Exception as e:
        logger.error(f"Speech error: {e}")
        return None

# ━━━━━━━━━━━━━━━━━━━━━━━
# 시스템 프롬프트
# ━━━━━━━━━━━━━━━━━━━━━━━
SYSTEM_PROMPT = """당신은 정진수 대표님의 전담 AI 비서입니다.
텔레그램으로 소통하며 실제 비서처럼 판단하고 행동합니다.

핵심 원칙:
- 항상 대표님 입장에서 가장 빠르고 편한 방법으로 처리
- 호칭은 보스 라고 부르기
- 짧고 명확하게. 불필요한 말 절대 금지
- 모호하면 한 가지만 물어보고 바로 처리
- 실패해도 이유를 한 줄로만 설명하고 대안 제시
- 죄송합니다 남발 금지. 대신 바로 해결책 제시
- 존댓말 유지, 친근하되 전문적으로

보유 기능:
1. Google Drive - 파일 검색, 전송
2. Gmail - 전송, 파일 첨부, 받은 메일 읽기/검색/요약
3. 음성/통화녹음 - m4a, mp3 등 자동 텍스트 변환 + AI 요약
4. 웹 검색 - 실시간 뉴스, 날씨, 주가, 인물, 기업 정보
5. 문서 작성 - 이메일 초안, 보고서, 아이디어 정리
6. 메모 저장 - 중요한 내용 저장 및 조회
7. 일반 대화 및 업무 조언

명령 형식:
[DRIVE_SEARCH:검색어]
[DRIVE_LIST]
[DRIVE_SEND:번호]
[EMAIL:주소|제목|본문]
[EMAIL_WITH_FILE:주소|제목|본문|파일번호]
[GMAIL_LIST:검색쿼리]
[GMAIL_READ:번호]
[MEMO_SAVE:내용]
[MEMO_LIST]
[REPEAT_LAST]

파일 처리:
검색 트리거: 찾아줘, 어딨어, 검색해, 있어? -> [DRIVE_SEARCH:핵심단어]
전송 트리거: 줘, 보내줘, 전송해 -> [DRIVE_SEND:1]
[중요] 당신은 Railway 배포 커스텀 텔레그램 봇입니다.
Drive API 이미 연결됨. 검색됐으면 전송도 무조건 가능.
"권한없다", "연동안됐다" 절대 금지.

검색 트리거: 찾아줘, 어딨어, 검색해, 있어? -> [DRIVE_SEARCH:핵심단어]

전송/다운로드 트리거:
"줘", "보내줘", "받아줘", "다운해줘", "다운받아줘", "보여줘",
"전송해", "진행해", "저장해줘", "이거줘", "그거줘", "1번", "2번" 등
숫자만 단독 입력해도 검색 결과 있으면 즉시 [DRIVE_SEND:번호]

여러 개: "1,2번", "둘다" -> [DRIVE_SEND:1] 후 [DRIVE_SEND:2]

결과 1개면 확인 없이 바로 [DRIVE_SEND:1]

이메일:
주소 없을 때: 반드시 먼저 물어보기
파일 첨부: 파일도 같이, 첨부해서 -> [EMAIL_WITH_FILE] 사용

메일 읽기:
메일 확인, 받은 메일 -> [GMAIL_LIST:is:unread]
오늘 온 메일 -> [GMAIL_LIST:newer_than:1d]
목록 표시 후 1번 읽어줘 -> [GMAIL_READ:1]

메모:
메모해줘, 기억해줘, 저장해줘 -> [MEMO_SAVE:내용]
메모 뭐 있어 -> [MEMO_LIST]

웹 검색:
자동 검색: 오늘/최신 뉴스, 날씨, 주가, 환율, 모르는 사람/기업
결과: 핵심 3줄 요약 + 출처 1개

반복:
다시, 또, 한번 더 -> [REPEAT_LAST]"""


def is_authorized(uid):
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS


# ━━━━━━━━━━━━━━━━━━━━━━━
# Claude 호출
# ━━━━━━━━━━━━━━━━━━━━━━━
async def ask_claude(user_id, message):
    history = db_get_history(user_id)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    user_content = f"[{timestamp}]\n{message}"
    db_add_message(user_id, "user", user_content)
    history.append({"role": "user", "content": user_content})

    try:
        r = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
            messages=history,
        )
        text_parts = [block.text for block in r.content if block.type == "text"]
        txt = "\n".join(text_parts) if text_parts else "응답 없음"
        db_add_message(user_id, "assistant", txt)
        return txt
    except anthropic.APIError as e:
        logger.error(f"Claude error: {e}")
        return "AI 오류. 잠시 후 다시 시도하세요."


# ━━━━━━━━━━━━━━━━━━━━━━━
# 커맨드 핸들러
# ━━━━━━━━━━━━━━━━━━━━━━━
async def cmd_start(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return
    d = "✅" if drive_service else "❌"
    g = "✅" if gmail_service else "❌"
    s = "✅" if speech_service else "❌"
    db = "✅" if db_available else "메모리"
    await update.message.reply_text(
        f"보스, 안녕하세요! 👋\n\n"
        f"📁 Drive: {d} 📧 Gmail: {g} 🎙️ 음성: {s}\n"
        f"🔍 웹검색: ✅ 🗄️ DB: {db}\n\n"
        f"/files - 파일 목록\n"
        f"/mail - 받은 메일\n"
        f"/memo - 저장된 메모\n"
        f"/clear - 대화 초기화\n"
        f"/help - 도움말"
    )

async def cmd_clear(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid):
        return
    db_clear_history(uid)
    await update.message.reply_text("대화 기록 초기화 완료!")

async def cmd_files(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid):
        return
    if not drive_service:
        await update.message.reply_text("Drive 미연결")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    files = list_drive_files()
    if not files:
        await update.message.reply_text("파일 없음")
        return
    db_set_state(uid, "search_results", files)
    msg = "📁 최근 파일 목록:\n\n"
    for i, f in enumerate(files, 1):
        msg += f"{i}. {f['name']} ({f.get('modifiedTime','')[:10]})\n"
    msg += "\n번호로 전송/이메일 첨부 가능!"
    await update.message.reply_text(msg)

async def cmd_mail(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid):
        return
    if not gmail_service:
        await update.message.reply_text("Gmail 미연결")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    emails = get_gmail_list(10, "is:unread")
    if not emails:
        await update.message.reply_text("읽지 않은 메일 없음")
        return
    db_set_state(uid, "gmail_list", emails)
    msg = "📬 읽지 않은 메일:\n\n"
    for i, e in enumerate(emails, 1):
        sender = e["from"].split("<")[0].strip()[:20]
        subject = e["subject"][:30]
        msg += f"{i}. {sender}\n   {subject}\n\n"
    msg += "1번 메일 읽어줘 라고 하세요!"
    await update.message.reply_text(msg)

async def cmd_memo(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid):
        return
    if not db_available:
        await update.message.reply_text("DB 미연결")
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT content, created_at FROM memos WHERE user_id=%s ORDER BY id DESC LIMIT 10",
            (uid,)
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            await update.message.reply_text("저장된 메모 없음")
            return
        msg = "📝 저장된 메모:\n\n"
        for r in rows:
            dt = r[1].strftime("%m/%d %H:%M") if r[1] else ""
            msg += f"[{dt}] {r[0]}\n\n"
        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"cmd_memo error: {e}")
        await update.message.reply_text("메모 조회 실패")

async def cmd_help(update, context):
    if not is_authorized(update.effective_user.id):
        return
    await update.message.reply_text(
        "📖 사용 가이드\n\n"
        "파일: '파일 찾아줘 [이름]' / '보내줘' / /files\n\n"
        "이메일: 'abc@gmail.com에 안녕 보내줘'\n\n"
        "메일 읽기: /mail 또는 '받은 메일 보여줘'\n\n"
        "메모: '메모해줘 [내용]' / /memo\n\n"
        "음성: 음성메시지/m4a/mp3 보내면 자동 분석\n\n"
        "검색: '오늘 뉴스', '코스피' 등\n\n"
        "/clear - 대화 초기화"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━
# 음성 핸들러
# ━━━━━━━━━━━━━━━━━━━━━━━
async def handle_voice(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        return
    if not speech_service:
        await update.message.reply_text("음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 음성 분석 중...")
    voice = update.message.voice or update.message.audio
    f = await context.bot.get_file(voice.file_id)
    data = await f.download_as_bytearray()
    txt = transcribe_audio(bytes(data), voice.mime_type or "audio/ogg")
    if txt:
        await update.message.reply_text(f"📝 텍스트:\n\n{txt}")
        analysis = await ask_claude(u.id, f"다음 음성 내용을 분석하고 요약해줘:\n\n{txt}")
        if len(analysis) > 4096:
            for i in range(0, len(analysis), 4096):
                await update.message.reply_text(analysis[i:i+4096])
        else:
            await update.message.reply_text(f"분석:\n\n{analysis}")
    else:
        await update.message.reply_text("음성 인식 실패")
        db_add_message(u.id, "assistant", "음성 파일 분석을 시도했으나 음성 인식에 실패했습니다.")

async def handle_audio_file(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        return
    if not speech_service:
        await update.message.reply_text("음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 오디오 분석 중...")
    audio = update.message.audio or update.message.document
    f = await context.bot.get_file(audio.file_id)
    data = await f.download_as_bytearray()
    mime = audio.mime_type or "audio/mpeg"
    txt = transcribe_audio(bytes(data), mime)
    if txt:
        if len(txt) > 3000:
            for i in range(0, len(txt), 3000):
                await update.message.reply_text(f"📝 ({i//3000+1}):\n\n{txt[i:i+3000]}")
        else:
            await update.message.reply_text(f"📝 텍스트:\n\n{txt}")
        analysis = await ask_claude(u.id, f"통화/음성 녹음입니다. 핵심 요약하고 중요 포인트 정리해줘:\n\n{txt}")
        if len(analysis) > 4096:
            for i in range(0, len(analysis), 4096):
                await update.message.reply_text(analysis[i:i+4096])
        else:
            await update.message.reply_text(f"분석:\n\n{analysis}")
    else:
        await update.message.reply_text("음성 인식 실패")

# ━━━━━━━━━━━━━━━━━━━━━━━
# 메인 메시지 핸들러
# ━━━━━━━━━━━━━━━━━━━━━━━
async def handle_message(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    resp = await ask_claude(u.id, update.message.text)

    if "[REPEAT_LAST]" in resp:
        last = db_get_state(u.id, "last_action")
        if not last:
            await update.message.reply_text("반복할 이전 작업이 없습니다.")
            return
        if last.get("type") == "file":
            await update.message.reply_text(f"'{last['name']}' 다시 전송 중...")
            buf, name = download_drive_file(last["file_id"])
            if buf:
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_document")
                await update.message.reply_document(document=buf, filename=name, caption=name)
        elif last.get("type") == "email":
            await update.message.reply_text(f"{last['to']}로 다시 전송 중...")
            buf, fname = None, None
            if last.get("file_id"):
                buf, fname = download_drive_file(last["file_id"])
            ok, msg = send_gmail(last["to"], last["subject"], last["body"], buf, fname)
            if ok:
                await update.message.reply_text(f"✅ {last['to']}로 전송 완료!")
            else:
                await update.message.reply_text(f"전송 실패: {msg}")
        return

    if "[DRIVE_SEARCH:" in resp:
        kw = resp.split("[DRIVE_SEARCH:")[1].split("]")[0]
        files = search_drive_files(kw)
        if files:
            db_set_state(u.id, "search_results", files)
            msg = f"🔍 '{kw}' 검색 결과:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            msg += "\n번호로 전송/이메일 첨부 가능!"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text(f"'{kw}' 결과 없음")

    elif "[DRIVE_LIST]" in resp:
        files = list_drive_files()
        if files:
            db_set_state(u.id, "search_results", files)
            msg = "📁 파일 목록:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("파일 없음")

    elif "[DRIVE_SEND:" in resp:
        try:
            num = int(resp.split("[DRIVE_SEND:")[1].split("]")[0]) - 1
            files = db_get_state(u.id, "search_results")
            if 0 <= num < len(files):
                fi = files[num]
                await update.message.reply_text(f"'{fi['name']}' 전송 중...")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_document")
                    await update.message.reply_document(document=buf, filename=name, caption=name)
                    db_set_state(u.id, "last_action", {"type": "file", "file_id": fi["id"], "name": name})
                else:
                    await update.message.reply_text("다운로드 실패")
            else:
                await update.message.reply_text("잘못된 번호")
        except Exception:
            await update.message.reply_text("번호 확인 필요")

    elif "[EMAIL_WITH_FILE:" in resp:
        try:
            parts = resp.split("[EMAIL_WITH_FILE:")[1].split("]")[0].split("|")
            to_addr, subject, body, fnum = parts[0], parts[1], parts[2], int(parts[3]) - 1
            files = db_get_state(u.id, "search_results")
            if 0 <= fnum < len(files):
                fi = files[fnum]
                await update.message.reply_text(f"'{fi['name']}' 첨부하여 전송 중...")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    ok, msg = send_gmail(to_addr, subject, body, buf, name)
                    if ok:
                        await update.message.reply_text(f"✅ {to_addr}로 전송 완료!")
                        db_set_state(u.id, "last_action", {
                            "type": "email", "to": to_addr, "subject": subject,
                            "body": body, "file_id": fi["id"], "file_name": name
                        })
                    else:
                        await update.message.reply_text(f"전송 실패: {msg}")
                else:
                    await update.message.reply_text("파일 다운로드 실패")
            else:
                await update.message.reply_text("잘못된 파일 번호")
        except Exception as e:
            logger.error(f"Email+file error: {e}")
            await update.message.reply_text("이메일 전송 실패")

    elif "[EMAIL:" in resp:
        try:
            parts = resp.split("[EMAIL:")[1].split("]")[0].split("|")
            to_addr, subject, body = parts[0], parts[1], parts[2]
            await update.message.reply_text(f"{to_addr}로 전송 중...")
            ok, msg = send_gmail(to_addr, subject, body)
            if ok:
                await update.message.reply_text(f"✅ {to_addr}로 전송 완료!")
                db_set_state(u.id, "last_action", {"type": "email", "to": to_addr, "subject": subject, "body": body})
            else:
                await update.message.reply_text(f"전송 실패: {msg}")
        except Exception as e:
            logger.error(f"Email error: {e}")
            await update.message.reply_text("이메일 전송 실패")

    elif "[GMAIL_LIST:" in resp:
        query = resp.split("[GMAIL_LIST:")[1].split("]")[0]
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        emails = get_gmail_list(10, query)
        if emails:
            db_set_state(u.id, "gmail_list", emails)
            msg = "📬 메일 목록:\n\n"
            for i, e in enumerate(emails, 1):
                sender = e["from"].split("<")[0].strip()[:20]
                subject = e["subject"][:30]
                msg += f"{i}. {sender}\n   {subject}\n\n"
            msg += "1번 메일 읽어줘 라고 하세요!"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("메일 없음")

    elif "[GMAIL_READ:" in resp:
        try:
            num = int(resp.split("[GMAIL_READ:")[1].split("]")[0]) - 1
            emails = db_get_state(u.id, "gmail_list")
            if 0 <= num < len(emails):
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
                content = get_gmail_content(emails[num]["id"])
                if content:
                    msg = (f"📧 메일 내용\n\n"
                           f"보낸이: {content['from']}\n"
                           f"제목: {content['subject']}\n"
                           f"날짜: {content['date']}\n\n"
                           f"내용:\n{content['body']}")
                    if len(msg) > 4096:
                        for i in range(0, len(msg), 4096):
                            await update.message.reply_text(msg[i:i+4096])
                    else:
                        await update.message.reply_text(msg)
                    summary = await ask_claude(u.id, f"이 메일 내용을 간단히 요약해줘:\n{content['body']}")
                    await update.message.reply_text(f"요약: {summary}")
                else:
                    await update.message.reply_text("메일 읽기 실패")
            else:
                await update.message.reply_text("잘못된 번호")
        except Exception:
            await update.message.reply_text("번호 확인 필요")

    elif "[MEMO_SAVE:" in resp:
        content = resp.split("[MEMO_SAVE:")[1].split("]")[0]
        if db_available:
            conn = get_db()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO memos (user_id, content) VALUES (%s, %s)", (u.id, content))
                    conn.close()
                    await update.message.reply_text(f"📝 메모 저장 완료!\n\n{content}")
                except Exception as e:
                    logger.error(f"Memo save error: {e}")
                    await update.message.reply_text("메모 저장 실패")
        else:
            await update.message.reply_text("DB 미연결")

    elif "[MEMO_LIST]" in resp:
        await cmd_memo(update, context)

    else:
        if len(resp) > 4096:
            for i in range(0, len(resp), 4096):
                await update.message.reply_text(resp[i:i+4096])
        else:
            await update.message.reply_text(resp)


# ━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━
def main():
    global db_available
    db_available = init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("mail", cmd_mail))
    app.add_handler(CommandHandler("memo", cmd_memo))
    app.add_handler(CommandHandler("help", cmd_help))

    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.AUDIO, handle_audio_file))
    app.add_handler(MessageHandler(
        filters.Document.MimeType("audio/mpeg") |
        filters.Document.MimeType("audio/mp4") |
        filters.Document.MimeType("audio/ogg") |
        filters.Document.MimeType("audio/wav") |
        filters.Document.MimeType("audio/x-m4a") |
        filters.Document.MimeType("video/mp4"),
        handle_audio_file
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info(f"Bot started! DB: {'PostgreSQL' if db_available else 'in-memory'}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
