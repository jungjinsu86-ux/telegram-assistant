import os, json, logging, io, base64
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import anthropic
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

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
conversation_history = defaultdict(list)
MAX_HISTORY = 20

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
        cur.execute("DELETE FROM notified_mail_ids WHERE notified_at < NOW() - INTERVAL '7 days'")
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
        cur.execute("SELECT content, created_at FROM memos WHERE user_id=%s ORDER BY created_at DESC LIMIT %s", (user_id, limit))
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
    for content, created_at in memos:
        text += f"- {content} ({created_at.strftime('%m/%d')})\n"
    return text

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

# ── Speech (CLOVA)
def transcribe_audio(audio_bytes, mime_type="audio/ogg"):
    if not clova_available:
        return None
    try:
        headers = {
            "Accept": "application/json;UTF-8",
            "X-CLOVASPEECH-API-KEY": CLOVA_SECRET_KEY,
        }
        files = {
            "media": ("audio.m4a", audio_bytes, mime_type),
            "params": (None, json.dumps({
                "language": "ko-KR",
                "completion": "sync",
                "speaker": True,
                "diarization": {
                    "enable": True,
                    "speakerCountMin": 2,
                    "speakerCountMax": 2,
                },
            }), "application/json"),
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
                    speaker = seg.get("diarization", {}).get("label", "")
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

→ 서버, API 연동 등 이미 다 세팅되어 있음
→ "설정이 필요하다", "서버가 있어야 한다" 같은 말 하지 말 것
→ 기능 요청이 오면 바로 어떻게 추가할지만 말하면 됨

━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 절대 규칙 (위반 금지)
━━━━━━━━━━━━━━━━━━━━━━━
- 절대로 "저는 Claude AI입니다"라고 밝히지 마세요
- 절대로 "Drive/Gmail에 접근할 수 없습니다"라고 말하지 마세요
- 절대로 "봇 서버 담당자에게 확인하세요"라고 하지 마세요
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
5. 메모 저장/조회 - /memo 로 저장, /memos 로 조회
6. 문서 작성 - 이메일 초안, 보고서, 아이디어 정리
7. 일반 대화 및 업무 조언

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
[REPEAT_LAST]

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
📧 이메일 전송 규칙
━━━━━━━━━━━━━━━━━━━━━━━
주소 없을 때: 반드시 먼저 물어보기
주소 있을 때: 제목/본문이 없어도 문맥에서 추론해서 작성
파일 첨부: "첨부해서", "붙여서" → [EMAIL_WITH_FILE] 사용

━━━━━━━━━━━━━━━━━━━━━━━
📬 메일 읽기 규칙
━━━━━━━━━━━━━━━━━━━━━━━
"메일 확인", "받은 메일" → [GMAIL_LIST:is:unread]
"오늘 온 메일" → [GMAIL_LIST:newer_than:1d]
"○○한테서 온 메일" → [GMAIL_LIST:from:○○]
"1번 메일 읽어줘" → [GMAIL_READ:1]

━━━━━━━━━━━━━━━━━━━━━━━
🔍 웹 검색 규칙
━━━━━━━━━━━━━━━━━━━━━━━
최신 정보, 뉴스, 날씨, 주가, 모르는 사람/기업 → 자동 웹 검색
핵심 3줄 요약, 출처 1개만 표시

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
답변: [GMAIL_LIST:is:unread]

질문: 다시 보내줘
답변: [REPEAT_LAST]

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

INSTAGRAM_KEYWORDS = {"인스타", "캡션", "게시물", "피드"}
ANALYSIS_KEYWORDS = {"분석", "피드백", "인스타", "캡션", "마케팅", "마케터", "평가", "리뷰", "봐줘", "어때"}

def _select_image_prompt(text):
    if any(kw in text for kw in ("인스타", "캡션", "게시물")):
        return INSTAGRAM_SYSTEM_PROMPT, text or "이 이미지를 바탕으로 인스타그램 게시물 초안을 작성해주세요"
    elif any(kw in text for kw in ("마케팅", "마케터")):
        return MARKETING_SYSTEM_PROMPT, text or "마케터 관점에서 이 이미지를 분석해주세요"
    else:
        return IMAGE_SYSTEM_PROMPT, text or "이 디자인을 전문적으로 분석하고 피드백해주세요"

async def _call_vision(b64, mime_type, system_prompt, prompt, update):
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

def is_authorized(uid):
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS

user_search_results = defaultdict(list)
user_gmail_list = defaultdict(list)
user_last_action = defaultdict(dict)
user_last_photo = {}

async def ask_claude(user_id, message):
    history = conversation_history[user_id]
    memos = get_memos_for_prompt(user_id)
    system = SYSTEM_PROMPT + memos

    history.append({"role": "user", "content": f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}]\n{message}"})
    if len(history) > MAX_HISTORY:
        history[:] = history[-MAX_HISTORY:]
    try:
        r = await client.messages.create(
            model="claude-sonnet-4-6", max_tokens=4096,
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
            messages=history,
        )
        text_parts = [block.text for block in r.content if block.type == "text"]
        txt = "\n".join(text_parts) if text_parts else "응답 없음"
        # 텍스트만 저장 (토큰 절약)
        history.append({"role": "assistant", "content": txt})
        return txt
    except anthropic.APIError as e:
        logger.error(f"Claude error: {e}")
        history.pop()
        return "⚠️ AI 오류. 잠시 후 다시 시도하세요."

# ── Gmail 자동 체크 (1시간마다)
async def check_new_gmail(app):
    if not gmail_service or not ALLOWED_USER_IDS or not DATABASE_URL:
        return
    try:
        emails = get_gmail_list(10, "is:unread newer_than:1h")
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
    for i, (content, created_at) in enumerate(memos, 1):
        msg += f"{i}. {content} ({created_at.strftime('%m/%d %H:%M')})\n"
    await update.message.reply_text(msg)

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
        "💬 대화: 메시지 보내면 AI 답변\n"
        "🔍 검색: '오늘 뉴스', '코스피' 등\n"
        "📁 파일: '파일 찾아줘', '보내줘', /files\n"
        "📧 이메일: 'abc@gmail.com에 안녕 보내줘'\n"
        "📬 메일: /mail, '받은 메일 보여줘'\n"
        "🎙️ 음성: 음성/m4a 파일 보내면 자동 분석\n"
        "📝 메모: /memo [내용], /memos\n"
        "🔄 반복: '다시 보내줘'\n\n"
        "/clear - 초기화")

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
    txt = transcribe_audio(bytes(data), voice.mime_type or "audio/ogg")
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
    txt = transcribe_audio(bytes(data), audio.mime_type or "audio/mpeg")
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
    print("=== PHOTO HANDLER TRIGGERED ===", flush=True)
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

        user_last_photo[u.id] = {"b64": b64, "mime_type": mime_type}

        caption = (update.message.caption or "").strip()
        if caption:
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
            system_prompt, prompt = _select_image_prompt(caption)
            await _call_vision(b64, mime_type, system_prompt, prompt, update)
            user_last_photo.pop(u.id, None)
        else:
            await update.message.reply_text(
                "📸 이미지 수신 완료! 분석 방법을 말씀해주세요.\n"
                "예: 디자인 피드백, 인스타 캡션, 마케팅 분석"
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
            await update.message.reply_text(f"'{kw}' 결과 없음")

    elif "[DRIVE_LIST]" in resp:
        files = list_drive_files()
        if files:
            user_search_results[u.id] = files
            msg = "📁 파일 목록:\n\n"
            for i, f in enumerate(files, 1):
                msg += f"{i}. 📄 {f['name']} ({f.get('modifiedTime','')[:10]})\n"
            await update.message.reply_text(msg)
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
                        user_last_action[u.id] = {"type": "email", "to": to_addr, "subject": subject, "body": body, "file_id": fi["id"], "file_name": name}
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
                user_last_action[u.id] = {"type": "email", "to": to_addr, "subject": subject, "body": body}
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

    else:
        if len(resp) > 4096:
            for i in range(0, len(resp), 4096):
                await update.message.reply_text(resp[i:i+4096])
        else:
            await update.message.reply_text(resp)

def main():
    init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("memo", cmd_memo))
    app.add_handler(CommandHandler("memos", cmd_memos))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("mail", cmd_mail))
    app.add_handler(CommandHandler("help", cmd_help))
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
    print("=== HANDLERS REGISTERED ===", flush=True)
    print(f"Total handlers: {len(app.handlers[0])}", flush=True)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_new_gmail, "interval", minutes=60, args=[app])
    scheduler.start()

    logger.info("Bot started! (Drive + Gmail + Speech + Web Search + DB + Mail Alert)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
