import os, json, logging, io, base64, smtplib
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
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
conversation_history = defaultdict(list)
MAX_HISTORY = 20

drive_service = None
speech_service = None
if GOOGLE_CREDENTIALS_JSON:
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/cloud-platform",
            ]
        )
        drive_service = build("drive", "v3", credentials=creds)
        speech_service = build("speech", "v1", credentials=creds)
        logger.info("Google Drive + Speech connected!")
    except Exception as e:
        logger.error(f"Google connection failed: {e}")

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

def send_email(to_addr, subject, body, attach_buf=None, attach_name=None):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        return False, "Email not configured"
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
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            s.sendmail(GMAIL_ADDRESS, to_addr, msg.as_string())
        return True, "OK"
    except Exception as e:
        logger.error(f"Email error: {e}")
        return False, str(e)

def transcribe_audio(audio_bytes, mime_type="audio/ogg"):
    if not speech_service:
        return None
    try:
        enc_map = {"audio/ogg": "OGG_OPUS", "audio/mpeg": "MP3", "audio/mp3": "MP3",
                   "audio/wav": "LINEAR16", "audio/x-wav": "LINEAR16",
                   "audio/mp4": "MP3", "audio/x-m4a": "MP3"}
        encoding = enc_map.get(mime_type, "OGG_OPUS")
        body = {
            "config": {"encoding": encoding, "languageCode": "ko-KR",
                       "alternativeLanguageCodes": ["en-US"],
                       "enableAutomaticPunctuation": True, "model": "latest_long"},
            "audio": {"content": base64.b64encode(audio_bytes).decode("utf-8")},
        }
        resp = speech_service.speech().recognize(body=body).execute()
        results = resp.get("results", [])
        if results:
            return " ".join(r["alternatives"][0]["transcript"] for r in results if r.get("alternatives"))
        return "No speech detected."
    except Exception as e:
        logger.error(f"Speech error: {e}")
        return None

SYSTEM_PROMPT = """당신은 텔레그램에서 동작하는 정진수님의 개인 비서입니다.
한국어로 친절하고 간결하게 답변합니다.

기능:
1. Google Drive 파일 검색/전송
2. 이메일 전송 (파일 첨부 가능)
3. 음성/통화녹음 분석
4. 웹 검색 (실시간 정보 검색)
5. 일반 대화 및 업무 지원

Drive 명령 형식:
- 파일 검색: [DRIVE_SEARCH:검색어]
- 파일 목록: [DRIVE_LIST]
- 파일 전송: [DRIVE_SEND:번호]
- 이메일: [EMAIL:받는주소|제목|본문]
- 파일 첨부 이메일: [EMAIL_WITH_FILE:받는주소|제목|본문|파일번호]

예시:
- "출강 의뢰서 찾아줘" -> [DRIVE_SEARCH:출강 의뢰서]
- "1번 보내줘" -> [DRIVE_SEND:1]
- "1번 파일 abc@gmail.com으로 보내줘" -> [EMAIL_WITH_FILE:abc@gmail.com|파일 전송|첨부 파일 보내드립니다.|1]
- "abc@gmail.com에 회의 안내 메일 보내줘" -> [EMAIL:abc@gmail.com|회의 안내|안녕하세요, 회의 안내드립니다.]

웹 검색은 자동으로 수행됩니다. 최신 뉴스, 날씨, 주가 등 실시간 정보가 필요하면 검색 도구를 사용하세요.

규칙:
- 간결하게 답변
- 모르면 솔직히 답변
- 이메일 주소 모르면 물어보기
- 최신 정보가 필요하면 웹 검색 활용"""

def is_authorized(uid):
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS

user_search_results = defaultdict(list)

async def ask_claude(user_id, message):
    history = conversation_history[user_id]
    history.append({"role": "user", "content": f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}]\n{message}"})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
    try:
        r = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
            messages=history,
        )
        # Extract text from response blocks
        text_parts = []
        for block in r.content:
            if block.type == "text":
                text_parts.append(block.text)
        txt = "\n".join(text_parts) if text_parts else "응답을 생성하지 못했습니다."
        history.append({"role": "assistant", "content": r.content})
        return txt
    except anthropic.APIError as e:
        logger.error(f"Claude error: {e}")
        history.pop()
        return "⚠️ AI 오류. 잠시 후 다시 시도하세요."

async def cmd_start(update, context):
    u = update.effective_user
    if not is_authorized(u.id):
        await update.message.reply_text("⛔ 권한 없음")
        return
    d = "✅" if drive_service else "❌"
    e = "✅" if GMAIL_ADDRESS else "❌"
    s = "✅" if speech_service else "❌"
    await update.message.reply_text(
        f"안녕하세요, {u.first_name}님! 👋\n\n"
        f"📁 Drive: {d}  📧 이메일: {e}  🎙️ 음성: {s}  🔍 웹검색: ✅\n\n"
        f"/files - 파일 목록\n/clear - 초기화\n/help - 도움말")

async def cmd_clear(update, context):
    uid = update.effective_user.id
    if not is_authorized(uid): return
    conversation_history[uid].clear()
    user_search_results[uid].clear()
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

async def cmd_help(update, context):
    if not is_authorized(update.effective_user.id): return
    await update.message.reply_text(
        "📖 사용 가이드\n\n"
        "💬 대화: 메시지 보내면 AI 답변\n\n"
        "🔍 검색: '오늘 뉴스', '코스피 지수' 등 물어보면 실시간 검색\n\n"
        "📁 파일: '파일 찾아줘', '1번 보내줘', /files\n\n"
        "📧 이메일: '1번 파일 abc@gmail.com으로 보내줘'\n\n"
        "🎙️ 음성: 음성메시지 또는 녹음파일 보내면 자동 분석\n\n"
        "/clear - 초기화")

async def handle_voice(update, context):
    u = update.effective_user
    if not is_authorized(u.id): return
    if not speech_service:
        await update.message.reply_text("❌ 음성 분석 미연결")
        return
    await update.message.reply_text("🎙️ 음성 분석 중...")
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    voice = update.message.voice or update.message.audio
    f = await context.bot.get_file(voice.file_id)
    data = await f.download_as_bytearray()
    mime = voice.mime_type if voice.mime_type else "audio/ogg"
    txt = transcribe_audio(bytes(data), mime)
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
    await update.message.reply_text("🎙️ 오디오 분석 중... (시간이 걸릴 수 있습니다)")
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    audio = update.message.audio or update.message.document
    f = await context.bot.get_file(audio.file_id)
    data = await f.download_as_bytearray()
    mime = audio.mime_type if audio.mime_type else "audio/mpeg"
    txt = transcribe_audio(bytes(data), mime)
    if txt:
        if len(txt) > 3000:
            for i in range(0, len(txt), 3000):
                await update.message.reply_text(f"📝 텍스트 ({i//3000+1}):\n\n{txt[i:i+3000]}")
        else:
            await update.message.reply_text(f"📝 텍스트:\n\n{txt}")
        analysis = await ask_claude(u.id,
            f"통화/음성 녹음 텍스트입니다. 핵심 내용 요약하고 중요 포인트 정리해줘:\n\n{txt}")
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
        await update.message.reply_text("⛔ 권한 없음")
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
                await update.message.reply_text(f"📤 '{fi['name']}' 다운로드 중...")
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
                await update.message.reply_text(f"📧 '{fi['name']}' 첨부하여 이메일 전송 중...")
                buf, name = download_drive_file(fi["id"])
                if buf:
                    ok, msg = send_email(to_addr, subject, body, buf, name)
                    if ok:
                        await update.message.reply_text(f"✅ {to_addr}로 이메일 전송 완료!")
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
            ok, msg = send_email(to_addr, subject, body)
            if ok:
                await update.message.reply_text(f"✅ {to_addr}로 이메일 전송 완료!")
            else:
                await update.message.reply_text(f"❌ 전송 실패: {msg}")
        except Exception as e:
            logger.error(f"Email error: {e}")
            await update.message.reply_text("❌ 이메일 전송 실패")

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
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.AUDIO, handle_audio_file))
    app.add_handler(MessageHandler(
        filters.Document.MimeType("audio/mpeg") | filters.Document.MimeType("audio/mp4") |
        filters.Document.MimeType("audio/ogg") | filters.Document.MimeType("audio/wav") |
        filters.Document.MimeType("audio/x-m4a"), handle_audio_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started! (Drive + Email + Speech + Web Search)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
