import os
import json
import logging
import io
from datetime import datetime
from collections import defaultdict

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import anthropic

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ──────────────────────────────────────────────
# 환경변수
# ──────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
ALLOWED_USERS = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USER_IDS = (
    set(int(uid.strip()) for uid in ALLOWED_USERS.split(",") if uid.strip())
    if ALLOWED_USERS else set()
)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS", "")

# ──────────────────────────────────────────────
# 로깅
# ──────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Claude 클라이언트
# ──────────────────────────────────────────────
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
conversation_history = defaultdict(list)
MAX_HISTORY = 20

# ──────────────────────────────────────────────
# Google Drive 클라이언트
# ──────────────────────────────────────────────
drive_service = None
if GOOGLE_CREDENTIALS_JSON:
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        drive_service = build("drive", "v3", credentials=creds)
        logger.info("Google Drive 연결 성공!")
    except Exception as e:
        logger.error(f"Google Drive 연결 실패: {e}")

# ──────────────────────────────────────────────
# Google Drive 함수들
# ──────────────────────────────────────────────
def search_drive_files(query_text, max_results=10):
    if not drive_service:
        return []
    try:
        query = f"name contains '{query_text}' and trashed = false"
        results = drive_service.files().list(
            q=query, pageSize=max_results,
            fields="files(id, name, mimeType, size, modifiedTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        return results.get("files", [])
    except Exception as e:
        logger.error(f"Drive 검색 오류: {e}")
        return []

def list_drive_files(max_results=20):
    if not drive_service:
        return []
    try:
        results = drive_service.files().list(
            pageSize=max_results,
            fields="files(id, name, mimeType, size, modifiedTime)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        return results.get("files", [])
    except Exception as e:
        logger.error(f"Drive 목록 오류: {e}")
        return []

def download_drive_file(file_id):
    if not drive_service:
        return None, None
    try:
        file_meta = drive_service.files().get(
            fileId=file_id, fields="name, mimeType"
        ).execute()
        mime = file_meta.get("mimeType", "")
        if mime.startswith("application/vnd.google-apps."):
            export_map = {
                "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
                "application/vnd.google-apps.spreadsheet": (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
                "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
            }
            if mime in export_map:
                export_mime, ext = export_map[mime]
                request = drive_service.files().export_media(fileId=file_id, mimeType=export_mime)
                name = file_meta["name"] + ext
            else:
                return None, None
        else:
            request = drive_service.files().get_media(fileId=file_id)
            name = file_meta["name"]
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer, name
    except Exception as e:
        logger.error(f"Drive 다운로드 오류: {e}")
        return None, None

# ──────────────────────────────────────────────
# 시스템 프롬프트
# ──────────────────────────────────────────────
SYSTEM_PROMPT = """당신은 텔레그램 메신저에서 동작하는 정진수님의 개인 비서입니다.
사용자의 요청에 친절하고 간결하게 답변합니다.
한국어로 대화하며, 필요한 경우 영어도 사용합니다.

당신은 Google Drive 파일 기능을 가지고 있습니다.
사용자가 파일을 찾거나, 보내달라고 하거나, 드라이브 관련 요청을 하면
반드시 아래 형식으로 응답하세요:

파일 검색: [DRIVE_SEARCH:검색어]
파일 목록: [DRIVE_LIST]
파일 전송: [DRIVE_SEND:파일번호]

예시:
- "출강 의뢰서 찾아줘" -> [DRIVE_SEARCH:출강 의뢰서]
- "드라이브에 뭐 있어?" -> [DRIVE_LIST]
- "1번 파일 보내줘" -> [DRIVE_SEND:1]

일반 대화는 평소처럼 자연스럽게 답변하세요.
규칙:
- 간결하되 필요한 정보는 빠짐없이 전달
- 모르는 것은 모른다고 솔직히 답변
- 요청이 불명확하면 되물어보기"""

def is_authorized(user_id):
    if not ALLOWED_USER_IDS:
        return True
    return user_id in ALLOWED_USER_IDS

user_search_results = defaultdict(list)

async def ask_claude(user_id, message):
    history = conversation_history[user_id]
    timestamped = f"[현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}]\n{message}"
    history.append({"role": "user", "content": timestamped})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=[{"type": "text", "text": SYSTEM_PROMPT,
                     "cache_control": {"type": "ephemeral"}}],
            messages=history,
        )
        assistant_message = response.content[0].text
        history.append({"role": "assistant", "content": assistant_message})
        return assistant_message
    except anthropic.APIError as e:
        logger.error(f"Claude API error: {e}")
        history.pop()
        return "⚠️ AI 응답 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요."

# ──────────────────────────────────────────────
# 텔레그램 핸들러
# ──────────────────────────────────────────────
async def cmd_start(update, context):
    user = update.effective_user
    if not is_authorized(user.id):
        await update.message.reply_text("⛔ 사용 권한이 없습니다.")
        return
    drive_status = "✅ 연결됨" if drive_service else "❌ 미연결"
    await update.message.reply_text(
        f"안녕하세요, {user.first_name}님! 👋\n"
        f"저는 AI 비서입니다.\n\n"
        f"📁 Google Drive: {drive_status}\n\n"
        f"/clear - 대화 기록 초기화\n"
        f"/files - 드라이브 파일 목록\n"
        f"/help - 도움말"
    )

async def cmd_clear(update, context):
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        return
    conversation_history[user_id].clear()
    user_search_results[user_id].clear()
    await update.message.reply_text("🗑️ 대화 기록이 초기화되었습니다.")

async def cmd_files(update, context):
    if not is_authorized(update.effective_user.id):
        return
    if not drive_service:
        await update.message.reply_text("❌ Google Drive가 연결되지 않았습니다.")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    files = list_drive_files()
    if not files:
        await update.message.reply_text("📁 공유 폴더에 파일이 없습니다.")
        return
    user_search_results[update.effective_user.id] = files
    msg = "📁 드라이브 파일 목록:\n\n"
    for i, f in enumerate(files, 1):
        msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime', '')[:10]})\n"
    msg += "\n💡 파일을 받으려면 '1번 보내줘'라고 하세요."
    await update.message.reply_text(msg)

async def cmd_help(update, context):
    if not is_authorized(update.effective_user.id):
        return
    await update.message.reply_text(
        "📖 사용 가이드\n\n"
        "💬 일반 대화: 메시지를 보내면 AI가 답변\n\n"
        "📁 파일 기능:\n"
        "- '파일 찾아줘 [이름]' → 드라이브 검색\n"
        "- '드라이브 파일 보여줘' → 전체 목록\n"
        "- '1번 보내줘' → 파일 전송\n"
        "- /files → 파일 목록\n\n"
        "/clear - 대화 초기화\n"
        "/help - 도움말"
    )

async def handle_message(update, context):
    user = update.effective_user
    if not is_authorized(user.id):
        await update.message.reply_text("⛔ 사용 권한이 없습니다.")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    response = await ask_claude(user.id, update.message.text)

    if "[DRIVE_SEARCH:" in response:
        keyword = response.split("[DRIVE_SEARCH:")[1].split("]")[0]
        files = search_drive_files(keyword)
        if files:
            user_search_results[user.id] = files
            msg = f"🔍 '{keyword}' 검색 결과:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime', '')[:10]})\n"
            msg += "\n💡 번호를 말해주세요. (예: '1번 보내줘')"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text(f"😅 '{keyword}'로 검색했지만 파일을 찾지 못했습니다.")
    elif "[DRIVE_LIST]" in response:
        files = list_drive_files()
        if files:
            user_search_results[user.id] = files
            msg = "📁 파일 목록:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime', '')[:10]})\n"
            msg += "\n💡 번호를 말해주세요."
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("📁 파일이 없습니다.")
    elif "[DRIVE_SEND:" in response:
        try:
            num = int(response.split("[DRIVE_SEND:")[1].split("]")[0]) - 1
            files = user_search_results.get(user.id, [])
            if 0 <= num < len(files):
                file_info = files[num]
                await update.message.reply_text(f"📤 '{file_info['name']}' 다운로드 중...")
                await context.bot.send_chat_action(
                    chat_id=update.effective_chat.id, action="upload_document")
                buffer, filename = download_drive_file(file_info["id"])
                if buffer:
                    await update.message.reply_document(
                        document=buffer, filename=filename, caption=f"📄 {filename}")
                else:
                    await update.message.reply_text("❌ 다운로드 실패.")
            else:
                await update.message.reply_text("❌ 잘못된 번호입니다.")
        except (ValueError, IndexError):
            await update.message.reply_text("❌ 파일 번호를 확인해주세요.")
    else:
        if len(response) > 4096:
            for i in range(0, len(response), 4096):
                await update.message.reply_text(response[i:i+4096])
        else:
            await update.message.reply_text(response)

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("🤖 비서 봇 시작! (Google Drive 연동)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
