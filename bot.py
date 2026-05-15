import os, json, logging, io, base64, tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import anthropic
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
conversation_history = defaultdict(list)
MAX_HISTORY = 20

# ── Google Drive + Speech
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

# ── Gmail API
gmail_service = None
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
        q = f"name contains '{query_text}' and trashed = false"
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
    if not gmail_service:
        return False, "Gmail not connected"
    try:
        msg = MIMEMultipart()
        msg["From"] = GMAIL_ADDRESS
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        if attach_buf and attach_name:
            attach_buf.seek(0)
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attach_buf.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={attach_name}")
            msg.attach(part)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        gmail_service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True, "OK"
    except Exception as e:
        logger.error(f"Gmail send error: {e}")
        return False, str(e)

# ── Gmail read
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

# ── Speech (m4a 포함 자동 변환)
def transcribe_audio(audio_bytes, mime_type="audio/ogg"):
    if not speech_service:
        return None
    try:
        from pydub import AudioSegment

        # m4a, mp4 → mp3 변환
        if mime_type in ("audio/mp4", "audio/x-m4a", "audio/m4a", "video/mp4"):
            with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as f:
                f.write(audio_bytes)
                tmp_in = f.name
            tmp_out = tmp_in.replace(".m4a", ".mp3")
            AudioSegment.from_file(tmp_in, format="m4a").export(tmp_out, format="mp3")
            with open(tmp_out, "rb") as f:
                audio_bytes = f.read()
            mime_type = "audio/mpeg"
            os.unlink(tmp_in)
            os.unlink(tmp_out)

        enc_map = {
            "audio/ogg": "OGG_OPUS", "audio/mpeg": "MP3", "audio/mp3": "MP3",
            "audio/wav": "LINEAR16", "audio/x-wav": "LINEAR16",
            "audio/mp4": "MP3", "audio/x-m4a": "MP3"
        }
        encoding = enc_map.get(mime_type, "OGG_OPUS")
        body = {
            "config": {
                "encoding": encoding,
                "languageCode": "ko-KR",
                "alternativeLanguageCodes": ["en-US"],
                "enableAutomaticPunctuation": True,
                "model": "latest_long"
            },
            "audio": {"content": base64.b64encode(audio_bytes).decode("utf-8")},
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

SYSTEM_PROMPT = """당신은 텔레그램에서 동작하는 정진수님의 개인 비서입니다.
한국어로 친절하고 간결하게 답변합니다.

기능:
1. Google Drive 파일 검색/전송
2. Gmail 이메일 전송 (누구한테든 가능, 파일 첨부 포함)
3. Gmail 이메일 읽기/목록/검색
4. 음성/통화녹음 분석 (m4a, mp3, ogg 등)
5. 웹 검색 (실시간 정보)
6. 일반 대화 및 업무 지원

명령 형식:
- 파일 검색: [DRIVE_SEARCH:검색어]
- 파일 목록: [DRIVE_LIST]
- 파일 전송: [DRIVE_SEND:번호]
- 이메일 전송: [EMAIL:받는주소|제목|본문]
- 파일 첨부 이메일: [EMAIL_WITH_FILE:받는주소|제목|본문|파일번호]
- 메일 목록: [GMAIL_LIST:검색어]
- 메일 읽기: [GMAIL_READ:메일번호]

예시:
- "수입 파일 찾아줘" -> [DRIVE_SEARCH:수입]
- "1번 보내줘" -> [DRIVE_SEND:1]
- "korbomb@naver.com에 안녕이라고 보내줘" -> [EMAIL:korbomb@naver.com|안녕|안녕하세요!]
- "1번 파일 korbomb@naver.com으로 보내줘" -> [EMAIL_WITH_FILE:korbomb@naver.com|파일 전송|파일 보내드립니다.|1]
- "받은 메일 보여줘" -> [GMAIL_LIST:is:unread]
- "오늘 받은 메일" -> [GMAIL_LIST:newer_than:1d]
- "김과장 메일 찾아줘" -> [GMAIL_LIST:from:김과장]
- "1번 메일 읽어줘" -> [GMAIL_READ:1]

중요 규칙:
- "보내줘", "보내라고", "전송해" 등 전송 요청 시 최근 검색결과 1번을 [DRIVE_SEND:1]로 처리
- 검색 결과가 1개면 바로 [DRIVE_SEND:1]
- 사용자 표현이 자연스러워도 의도 파악해서 명령 형식으로 변환
- 간결하게 답변
- 최신 정보는 웹 검색 활용
- 이메일 주소 모르면 반드시 물어보기"""

def is_authorized(uid):
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS

user_search_results = defaultdict(list)
user_gmail_list = defaultdict(list)

async def ask_claude(user_id, message):
    history = conversation_history[user_id]
    history.append({"role": "user", "content": f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}]\n{message}"})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
    try:
        r = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=4096,
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
            messages=history,
        )
        text_parts = [block.text for block in r.content if block.type == "text"]
        txt = "\n".join(text_parts) if text_parts else "응답 없음"
        history.append({"role": "assistant", "content": r.content})
        return txt
    except anthropic.APIError as e:
        logger.error(f"Claude error: {e}")
        history.pop()
        return "⚠️ AI 오류. 잠시 후 다시 시도하세요."

async def cmd_start(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return
    d = "✅" if drive_service else "❌"
    g = "✅" if gmail_service else "❌"
    s = "✅" if speech_service else "❌"
    await update.message.reply_text(
        f"안녕하세요, {u.first_name}님! 👋\n\n"
        f"📁 Drive: {d}  📧 Gmail: {g}  🎙️ 음성: {s}  🔍 웹검색: ✅\n\n"
        f"/files - 파일 목록\n"
        f"/mail - 받은 메일 목록\n"
        f"/clear - 초기화\n"
        f"/help - 도움말")

async def cmd_clear(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    conversation_history[uid].clear()
    user_search_results[uid].clear()
    user_gmail_list[uid].clear()
    await update.message.reply_text("🗑️ 초기화 완료!")

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
    await update.message.reply_text(msg)

async def cmd_mail(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    if not gmail_service:
        await update.message.reply_text("❌ Gmail 미연결")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    emails = get_gmail_list(10, "is:unread")
    if not emails:
        await update.message.reply_text("📭 읽지 않은 메일 없음")
        return
    user_gmail_list[uid] = emails
    msg = "📬 읽지 않은 메일:\n\n"
    for i, e in enumerate(emails, 1):
        sender = e["from"].split("<")[0].strip()[:20]
        subject = e["subject"][:30]
        msg += f"{i}. 👤 {sender}\n   📌 {subject}\n\n"
    msg += "💡 '1번 메일 읽어줘'라고 하세요!"
    await update.message.reply_text(msg)

async def cmd_help(update, context):
    if not is_authorized(update.effective_user.id): return
    await update.message.reply_text(
        "📖 사용 가이드\n\n"
        "💬 대화: 메시지 보내면 AI 답변\n\n"
        "🔍 검색: '오늘 뉴스', '코스피' 등 실시간 검색\n\n"
        "📁 파일:\n- '파일 찾아줘 [이름]'\n- '보내줘'\n- /files\n\n"
        "📧 이메일:\n- 'korbomb@naver.com에 안녕 보내줘'\n- '1번 파일 메일로 보내줘'\n\n"
        "📬 메일 읽기:\n- /mail 또는 '받은 메일 보여줘'\n- '1번 메일 읽어줘'\n\n"
        "🎙️ 음성: 음성메시지/m4a/mp3 보내면 자동 분석\n\n"
        "/clear - 초기화")

async def handle_voice(update, context):
    u = update.effective_user
    if not is_authorized(u.id): return
    if not speech_service:
        await update.message.reply_text("❌ 음성 분석 미연결")
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
            await update.message.reply_text(f"🔍 분석:\n\n{analysis}")
    else:
        await update.message.reply_text("❌ 음성 인식 실패")

async def handle_audio_file(update, context):
    u = update.effective_user
    if not is_authorized(u.id): return
    if not speech_service:
        await update.message.reply_text("❌ 음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 오디오 분석 중... (잠시 기다려주세요)")
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
        analysis = await ask_claude(u.id,
            f"통화/음성 녹음입니다. 핵심 요약하고 중요 포인트 정리해줘:\n\n{txt}")
        if len(analysis) > 4096:
            for i in range(0, len(analysis), 4096):
                await update.message.reply_text(analysis[i:i+4096])
        else:
            await update.message.reply_text(f"🔍 분석:\n\n{analysis}")
    else:
        await update.message.reply_text("❌ 음성 인식 실패")

async def handle_message(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("Access denied.")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    resp = await ask_claude(u.id, update.message.text)

    if "[DRIVE_SEARCH:" in resp:
        kw = resp.split("[DRIVE_SEARCH:")[1].split("]")[0]
        files = search_drive_files(kw)
        if files:
            user_search_results[u.id] = files
            msg = f"🔍 '{kw}' 검색 결과:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            msg += "\n💡 번호로 전송/이메일 첨부 가능!"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text(f"😅 '{kw}' 결과 없음")

    elif "[DRIVE_LIST]" in resp:
        files = list_drive_files()
        if files:
            user_search_results[u.id] = files
            msg = "📁 파일 목록:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("📁 파일 없음")

    elif "[DRIVE_SEND:" in resp:
        try:
            num = int(resp.split("[DRIVE_SEND:")[1].split("]")[0]) - 1
            files = user_search_results.get(u.id, [])
            if 0 <= num < len(files):
                fi = files[num]
                await update.message.reply_text(f"📤 '{fi['name']}' 전송 중...")
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_document")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    await update.message.reply_document(document=buf, filename=name, caption=f"📄 {name}")
                else:
                    await update.message.reply_text("❌ 다운로드 실패")
            else:
                await update.message.reply_text("❌ 잘못된 번호")
        except:
            await update.message.reply_text("❌ 번호 확인 필요")

    elif "[EMAIL_WITH_FILE:" in resp:
        try:
            parts = resp.split("[EMAIL_WITH_FILE:")[1].split("]")[0].split("|")
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
                    else:
                        await update.message.reply_text(f"❌ 전송 실패: {msg}")
                else:
                    await update.message.reply_text("❌ 파일 다운로드 실패")
            else:
                await update.message.reply_text("❌ 잘못된 파일 번호")
        except Exception as e:
            logger.error(f"Email+file error: {e}")
            await update.message.reply_text("❌ 이메일 전송 실패")

    elif "[EMAIL:" in resp:
        try:
            parts = resp.split("[EMAIL:")[1].split("]")[0].split("|")
            to_addr, subject, body = parts[0], parts[1], parts[2]
            await update.message.reply_text(f"📧 {to_addr}로 전송 중...")
            ok, msg = send_gmail(to_addr, subject, body)
            if ok:
                await update.message.reply_text(f"✅ {to_addr}로 전송 완료!")
            else:
                await update.message.reply_text(f"❌ 전송 실패: {msg}")
        except Exception as e:
            logger.error(f"Email error: {e}")
            await update.message.reply_text("❌ 이메일 전송 실패")

    elif "[GMAIL_LIST:" in resp:
        query = resp.split("[GMAIL_LIST:")[1].split("]")[0]
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        emails = get_gmail_list(10, query)
        if emails:
            user_gmail_list[u.id] = emails
            msg = "📬 메일 목록:\n\n"
            for i, e in enumerate(emails, 1):
                sender = e["from"].split("<")[0].strip()[:20]
                subject = e["subject"][:30]
                msg += f"{i}. 👤 {sender}\n   📌 {subject}\n\n"
            msg += "💡 '1번 메일 읽어줘'라고 하세요!"
            await update.message.reply_text(msg)
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
                    msg = (f"📧 메일 내용\n\n"
                           f"👤 {content['from']}\n"
                           f"📌 {content['subject']}\n"
                           f"📅 {content['date']}\n\n"
                           f"📝 내용:\n{content['body']}")
                    if len(msg) > 4096:
                        for i in range(0, len(msg), 4096):
                            await update.message.reply_text(msg[i:i+4096])
                    else:
                        await update.message.reply_text(msg)
                    summary = await ask_claude(u.id, f"이 메일 내용을 간단히 요약해줘:\n{content['body']}")
                    await update.message.reply_text(f"🔍 요약: {summary}")
                else:
                    await update.message.reply_text("❌ 메일 읽기 실패")
            else:
                await update.message.reply_text("❌ 잘못된 번호")
        except:
            await update.message.reply_text("❌ 번호 확인 필요")

    else:
        if len(resp) > 4096:
            for i in range(0, len(resp), 4096):
                await update.message.reply_text(resp[i:i+4096])
        else:
            await update.message.reply_text(resp)

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("mail", cmd_mail))
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
        handle_audio_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started! (Drive + Gmail + Speech + Web Search)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
