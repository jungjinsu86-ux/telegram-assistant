import os
import logging
from anthropic import Anthropic
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
ALLOWED_USERS = set(
    int(uid.strip())
    for uid in os.environ.get("ALLOWED_USERS", "").split(",")
    if uid.strip()
)

anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """당신은 친절하고 유능한 한국어 AI 비서입니다.
사용자의 질문에 정확하고 도움이 되는 답변을 한국어로 제공하세요.
필요한 경우 마크다운 형식을 사용해 읽기 쉽게 답변을 구성하세요.
모르는 것은 모른다고 솔직하게 말하고, 불확실한 정보는 그렇다고 명시하세요."""

# user_id -> list of {"role": ..., "content": ...}
conversation_histories: dict[int, list[dict]] = {}

MAX_HISTORY = 20  # 최대 유지할 메시지 쌍 수


def is_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS


def get_history(user_id: int) -> list[dict]:
    return conversation_histories.setdefault(user_id, [])


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_allowed(user.id):
        await update.message.reply_text("죄송합니다. 이 봇을 사용할 권한이 없습니다.")
        return

    await update.message.reply_text(
        f"안녕하세요, {user.first_name}님! 👋\n\n"
        "저는 Claude 기반 AI 비서입니다. 무엇이든 물어보세요!\n\n"
        "📌 사용 가능한 명령어:\n"
        "/start - 봇 시작\n"
        "/clear - 대화 히스토리 초기화\n"
        "/help  - 도움말 보기"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(update.effective_user.id):
        await update.message.reply_text("죄송합니다. 이 봇을 사용할 권한이 없습니다.")
        return

    await update.message.reply_text(
        "🤖 *Claude AI 비서 도움말*\n\n"
        "자유롭게 메시지를 보내면 AI가 답변해드립니다.\n\n"
        "📌 *명령어*\n"
        "/start - 봇 시작 및 인사\n"
        "/clear - 대화 히스토리 초기화 (새 주제 시작 시 권장)\n"
        "/help  - 이 도움말 보기\n\n"
        "💡 *팁*\n"
        f"• 대화 맥락을 최대 {MAX_HISTORY}개 메시지까지 기억합니다.\n"
        "• /clear 로 초기화하면 이전 대화와 무관하게 새로 시작합니다.",
        parse_mode="Markdown",
    )


async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_allowed(user_id):
        await update.message.reply_text("죄송합니다. 이 봇을 사용할 권한이 없습니다.")
        return

    conversation_histories.pop(user_id, None)
    await update.message.reply_text("✅ 대화 히스토리가 초기화되었습니다. 새로운 대화를 시작하세요!")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_allowed(user_id):
        await update.message.reply_text("죄송합니다. 이 봇을 사용할 권한이 없습니다.")
        return

    user_text = update.message.text.strip()
    if not user_text:
        return

    history = get_history(user_id)
    history.append({"role": "user", "content": user_text})

    # 히스토리 길이 제한 (user+assistant 쌍 기준)
    if len(history) > MAX_HISTORY * 2:
        history[:] = history[-(MAX_HISTORY * 2):]

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id, action="typing"
    )

    try:
        response = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=history,
        )
        assistant_text = response.content[0].text
    except Exception as e:
        logger.error("Anthropic API error: %s", e)
        history.pop()  # 실패한 user 메시지 롤백
        await update.message.reply_text(
            "⚠️ 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
        )
        return

    history.append({"role": "assistant", "content": assistant_text})

    # Telegram 메시지 최대 길이(4096자) 초과 시 분할 전송
    for i in range(0, len(assistant_text), 4096):
        await update.message.reply_text(assistant_text[i : i + 4096])


def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("clear", clear_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("봇을 시작합니다...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
