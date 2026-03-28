from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import re
import secrets
import socket
import time
import uuid
import urllib.parse
import urllib.request
from enum import Enum
from math import asin, cos, radians, sin, sqrt
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, Request, Response, status
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, EmailStr, Field

import jwt

# ─────────────────────────────────────────────────────
# Env + DB
# ─────────────────────────────────────────────────────

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
JWT_SECRET = os.getenv("JWT_SECRET", "CHANGE_ME_TEMPORARY_SECRET")

logger = logging.getLogger("incontro")
logging.basicConfig(level=logging.INFO)

APP_NAME = "Incontro Studio Hub"
app = FastAPI(title=APP_NAME, version="0.2.0")


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert psycopg2 row to dict, coercing UUID objects to str."""
    d: Dict[str, Any] = {}
    for k, v in dict(row).items():
        if hasattr(v, "hex") and hasattr(v, "int"):  # uuid.UUID
            d[k] = str(v)
        elif isinstance(v, list):
            d[k] = [str(x) if (hasattr(x, "hex") and hasattr(x, "int")) else x for x in v]
        else:
            d[k] = v
    return d


def get_db():
    parsed = urllib.parse.urlparse(DATABASE_URL)
    try:
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=urllib.parse.unquote(parsed.password or ""),
            connect_timeout=10,
        )
    except psycopg2.OperationalError as e:
        msg = str(e)
        if "translate host name" in msg or "Name or service not known" in msg:
            raise psycopg2.OperationalError(
                f"Impossibile risolvere '{parsed.hostname}'. "
                "Il DB Supabase è solo IPv6. Usa il Session Pooler: "
                "Settings -> Database -> Connection string -> Session pooler. "
                f"Originale: {msg}"
            ) from e
        raise
    conn.autocommit = True
    return conn


# ─────────────────────────────────────────────────────
# Error handling
# ─────────────────────────────────────────────────────


class AppError(Exception):
    def __init__(self, *, message: str, status_code: int = 400, code: str = "app_error"):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code


@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_error", "message": "Internal server error"}},
    )


# ─────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────

INSTITUTIONAL_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.(edu|it)$")
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")


class LearningStyle(str, Enum):
    rumoroso = "Rumoroso"
    silenzioso = "Silenzioso"


class UserInDB(BaseModel):
    id: str
    email: str
    password_salt: str
    password_hash: str
    is_verified: bool = False
    study_subjects: List[str] = Field(default_factory=list)
    learning_style: Optional[LearningStyle] = None
    created_at: float = Field(default_factory=lambda: time.time())
    lat: Optional[float] = None
    lon: Optional[float] = None
    is_studying: bool = False
    study_location_name: Optional[str] = None
    study_started_at: Optional[float] = None
    username: Optional[str] = None
    avatar_base64: Optional[str] = None
    followers: List[str] = Field(default_factory=list)
    following: List[str] = Field(default_factory=list)
    stories: List[Dict] = Field(default_factory=list)
    has_active_story_cached: Optional[bool] = None


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class AuthResponse(BaseModel):
    token: str
    user: Dict[str, Any]


class UserPublic(BaseModel):
    id: str
    email: str
    is_verified: bool
    study_subjects: List[str]
    learning_style: Optional[LearningStyle]
    created_at: float
    username: Optional[str] = None
    avatar_base64: Optional[str] = None
    followers_count: int = 0
    following_count: int = 0
    has_active_story: bool = False


# ─────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────


def _db_to_user(row: Dict[str, Any], stories: Optional[List[Dict]] = None) -> UserInDB:
    """Convert a psycopg2 RealDictRow (or plain dict) to UserInDB."""
    learning_style = row.get("learning_style")
    if learning_style:
        try:
            learning_style = LearningStyle(learning_style)
        except ValueError:
            learning_style = None

    return UserInDB(
        id=row["id"],
        email=row["email"],
        password_salt=row["password_salt"],
        password_hash=row["password_hash"],
        is_verified=row.get("is_verified", False),
        study_subjects=row.get("study_subjects") or [],
        learning_style=learning_style,
        created_at=float(row.get("created_at") or time.time()),
        lat=row.get("lat"),
        lon=row.get("lon"),
        is_studying=row.get("is_studying", False),
        study_location_name=row.get("study_location_name"),
        study_started_at=row.get("study_started_at"),
        username=row.get("username"),
        avatar_base64=row.get("avatar_base64"),
        followers=row.get("followers") or [],
        following=row.get("following") or [],
        stories=stories or [],
        has_active_story_cached=row.get("has_active_story"),
    )


def _get_user_by_id(user_id: str, with_stories: bool = False) -> UserInDB:
    """Fetch a user from DB by ID. Raises AppError 401 if not found."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    except Exception as e:
        cur.close()
        conn.close()
        raise AppError(
            message="Non autorizzato.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
        ) from e

    if not row:
        cur.close()
        conn.close()
        raise AppError(
            message="Non autorizzato.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
        )

    stories: List[Dict] = []
    if with_stories:
        cutoff = time.time() - 86400
        try:
            cur.execute(
                "SELECT * FROM stories WHERE user_id = %s AND created_at >= %s",
                (user_id, cutoff),
            )
            stories = [dict(r) for r in cur.fetchall()]
        except Exception:
            pass

    cur.close()
    conn.close()
    return _db_to_user(_row_to_dict(row), stories)


def _make_user_public(
    user: UserInDB,
    has_active_story_override: Optional[bool] = None,
) -> UserPublic:
    now = time.time()
    if has_active_story_override is not None:
        has_active_story = has_active_story_override
    elif user.stories:
        has_active_story = any(
            s.get("created_at", 0) > now - 86400 for s in user.stories
        )
    else:
        has_active_story = user.has_active_story_cached or False

    return UserPublic(
        id=user.id,
        email=user.email,
        is_verified=user.is_verified,
        study_subjects=user.study_subjects,
        learning_style=user.learning_style,
        created_at=user.created_at,
        username=user.username,
        avatar_base64=user.avatar_base64,
        followers_count=len(user.followers),
        following_count=len(user.following),
        has_active_story=has_active_story,
    )


# ─────────────────────────────────────────────────────
# Passwords & JWT
# ─────────────────────────────────────────────────────

JWT_ISSUER = "incontro-studio-hub"
JWT_EXPIRE_HOURS = 24


def normalize_email(email: str) -> str:
    return email.strip().lower()


def validate_institutional_email(email: str) -> None:
    if not INSTITUTIONAL_EMAIL_RE.match(email.strip()):
        raise AppError(
            message="Email non istituzionale. Usa un indirizzo con dominio .edu o .it.",
            status_code=status.HTTP_400_BAD_REQUEST,
            code="invalid_email",
        )


def hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200_000)
    return dk.hex()


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    return secrets.compare_digest(hash_password(password, salt), expected_hash)


def create_access_token(user_id: str) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {
        "sub": user_id,
        "iss": JWT_ISSUER,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_access_token(token: str) -> str:
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            issuer=JWT_ISSUER,
            options={"require": ["exp", "iat", "sub", "iss"]},
        )
    except jwt.ExpiredSignatureError:
        raise AppError(
            message="Sessione scaduta.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="token_expired",
        )
    except jwt.InvalidTokenError:
        raise AppError(
            message="Non autorizzato.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_token",
        )
    user_id = payload.get("sub")
    if not user_id:
        raise AppError(
            message="Non autorizzato.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
        )
    return str(user_id)


async def auth_bearer(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> UserInDB:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise AppError(
            message="Non autorizzato.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
        )
    token = authorization.split(" ", 1)[1].strip()
    user_id = decode_access_token(token)
    return await asyncio.to_thread(_get_user_by_id, user_id, True)


# ─────────────────────────────────────────────────────
# Username generation
# ─────────────────────────────────────────────────────


def _generate_unique_username(base_email: str) -> str:
    base = re.sub(r"[^a-z0-9]", "", base_email.split("@")[0].lower())
    if not base:
        base = "user"
    if len(base) < 3:
        base = base + "user"
    candidate = base[:20]
    counter = 1
    conn = get_db()
    cur = conn.cursor()
    try:
        while True:
            cur.execute("SELECT id FROM users WHERE username = %s", (candidate,))
            if not cur.fetchone():
                return candidate
            suffix = str(counter)
            candidate = base[: 20 - len(suffix)] + suffix
            counter += 1
    finally:
        cur.close()
        conn.close()


# ─────────────────────────────────────────────────────
# Seed bot profiles
# ─────────────────────────────────────────────────────


def _seed_bot_profiles() -> None:
    bots = [
        ("marco@unina.it",      ["Matematica", "Fisica"],       LearningStyle.silenzioso, False, "marco_unina"),
        ("sofia@unibo.it",      ["Informatica", "Algoritmi"],   LearningStyle.rumoroso,   False, "sofia_unibo"),
        ("luca@polimi.it",      ["Ingegneria", "Matematica"],   LearningStyle.silenzioso, True,  "luca_polimi"),
        ("giulia@uniroma1.it",  ["Medicina", "Biologia"],       LearningStyle.silenzioso, False, "giulia_uniroma"),
        ("alessio@unina.it",    ["Fisica", "Chimica"],          LearningStyle.rumoroso,   False, "alessio_unina"),
        ("chiara@unibo.it",     ["Informatica", "Matematica"],  LearningStyle.silenzioso, True,  "chiara_unibo"),
        ("davide@polimi.it",    ["Architettura", "Disegno"],    LearningStyle.rumoroso,   False, "davide_polimi"),
        ("martina@uniroma1.it", ["Legge", "Storia"],            LearningStyle.silenzioso, True,  "martina_uniroma"),
        ("andrea@unina.it",     ["Economia", "Matematica"],     LearningStyle.rumoroso,   False, "andrea_unina"),
        ("valentina@unibo.it",  ["Psicologia", "Biologia"],     LearningStyle.silenzioso, False, "valentina_unibo"),
    ]
    password = "bot123456"
    for email, subjects, style, is_studying, username in bots:
        normalized = email.strip().lower()
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT id FROM users WHERE email = %s", (normalized,))
            if cur.fetchone():
                continue

            user_id = str(uuid.uuid4())
            salt = secrets.token_hex(16)
            pwd_hash = hash_password(password, salt)
            cur.execute(
                """
                INSERT INTO users (
                    id, email, password_salt, password_hash, is_verified,
                    study_subjects, learning_style, is_studying, study_location_name,
                    study_started_at, username, created_at, followers, following
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s::text[], %s, %s, %s,
                    %s, %s, %s, %s::text[], %s::text[]
                )
                ON CONFLICT (email) DO NOTHING
                """,
                (
                    user_id, normalized, salt, pwd_hash, True,
                    subjects, style.value, is_studying,
                    "Biblioteca Nazionale" if is_studying else None,
                    time.time() if is_studying else None,
                    username, time.time(), [], [],
                ),
            )
            logger.info("Seeded bot: %s", username)
        except Exception as e:
            logger.debug("Bot %s insert failed: %s", username, e)
        finally:
            cur.close()
            conn.close()


# ─────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────


def _check_db_connection() -> bool:
    """Test DB connectivity. Returns True if OK, logs instructions if not."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(
            "DATABASE NON RAGGIUNGIBILE: %s\n"
            "  Il DB Supabase usa solo IPv6 per connessioni dirette.\n"
            "  Soluzione: abilita il Session Pooler in Supabase:\n"
            "  https://supabase.com/dashboard/project/baijwbcjqghvctyjojuk\n"
            "  Settings -> Database -> Connection string -> Session pooler\n"
            "  Poi aggiorna DATABASE_URL nel file .env con l'URL del pooler.",
            e,
        )
        return False


@app.on_event("startup")
async def startup_event() -> None:
    logger.info("Starting %s…", APP_NAME)
    db_ok = await asyncio.to_thread(_check_db_connection)
    if not db_ok:
        logger.warning(
            "Server avviato ma DB non raggiungibile. "
            "Aggiorna DATABASE_URL nel .env con l'URL del Session Pooler Supabase."
        )
        return
    try:
        await asyncio.to_thread(_seed_bot_profiles)
        logger.info("Bot profiles ready.")
    except Exception as e:
        logger.warning("Could not seed bots: %s", e)


# ─────────────────────────────────────────────────────
# Auth endpoints
# ─────────────────────────────────────────────────────


@app.post("/auth/register", response_model=AuthResponse)
async def register(payload: RegisterRequest) -> AuthResponse:
    validate_institutional_email(payload.email)
    normalized = normalize_email(payload.email)

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM users WHERE email = %s", (normalized,))
        if cur.fetchone():
            raise AppError(
                message="Email già registrata.",
                status_code=status.HTTP_409_CONFLICT,
                code="email_exists",
            )
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    user_id = str(uuid.uuid4())
    salt = secrets.token_hex(16)
    pwd_hash = hash_password(payload.password, salt)
    username = await asyncio.to_thread(_generate_unique_username, normalized)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO users (
                id, email, password_salt, password_hash, is_verified,
                study_subjects, username, created_at, followers, following
            ) VALUES (%s, %s, %s, %s, %s, %s::text[], %s, %s, %s::text[], %s::text[])
            """,
            (
                user_id, normalized, salt, pwd_hash, False,
                [], username, time.time(), [], [],
            ),
        )
    except Exception as e:
        raise AppError(message="Errore durante la registrazione.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    user = await asyncio.to_thread(_get_user_by_id, user_id)
    token = create_access_token(user_id)
    return AuthResponse(token=token, user=_make_user_public(user).model_dump())


@app.post("/auth/login", response_model=AuthResponse)
async def login(payload: LoginRequest) -> AuthResponse:
    validate_institutional_email(payload.email)
    normalized = normalize_email(payload.email)

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE email = %s", (normalized,))
        row = cur.fetchone()
    except Exception as e:
        cur.close()
        conn.close()
        raise AppError(message="Errore database.", status_code=500) from e
    cur.close()
    conn.close()

    if not row:
        raise AppError(
            message="Credenziali non valide.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="bad_credentials",
        )

    row = _row_to_dict(row)
    if not verify_password(payload.password, row["password_salt"], row["password_hash"]):
        raise AppError(
            message="Credenziali non valide.",
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="bad_credentials",
        )

    user = _db_to_user(row)
    token = create_access_token(user.id)
    return AuthResponse(token=token, user=_make_user_public(user).model_dump())


@app.get("/auth/me", response_model=UserPublic)
async def me(user: UserInDB = Depends(auth_bearer)) -> UserPublic:
    return _make_user_public(user)


# ─────────────────────────────────────────────────────
# Profile
# ─────────────────────────────────────────────────────


class ProfileUpdateRequest(BaseModel):
    study_subjects: List[str] = Field(min_length=1, max_length=30)
    learning_style: LearningStyle


@app.put("/me/profile", response_model=UserPublic)
async def update_profile(
    payload: ProfileUpdateRequest, user: UserInDB = Depends(auth_bearer)
) -> UserPublic:
    subjects = [s.strip() for s in payload.study_subjects if s.strip()]
    if not subjects:
        raise AppError(message="study_subjects non può essere vuoto.", code="invalid_profile")

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET study_subjects = %s::text[], learning_style = %s WHERE id = %s",
            (subjects, payload.learning_style.value, user.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    user.study_subjects = subjects
    user.learning_style = payload.learning_style
    return _make_user_public(user)


# ─────────────────────────────────────────────────────
# Location & study session
# ─────────────────────────────────────────────────────


class LocationRequest(BaseModel):
    lat: float
    lon: float


class StudySessionRequest(BaseModel):
    location_name: str
    lat: float
    lon: float


@app.put("/me/location")
async def update_location(
    payload: LocationRequest, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET lat = %s, lon = %s WHERE id = %s",
            (payload.lat, payload.lon, user.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()
    return {"saved": True}


@app.post("/me/study-session")
async def start_study_session(
    payload: StudySessionRequest, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE users SET
                is_studying = TRUE,
                study_location_name = %s,
                study_started_at = %s,
                lat = %s,
                lon = %s
            WHERE id = %s
            """,
            (payload.location_name, time.time(), payload.lat, payload.lon, user.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()
    return {"started": True, "location_name": payload.location_name}


@app.delete("/me/study-session")
async def stop_study_session(user: UserInDB = Depends(auth_bearer)) -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET is_studying = FALSE, study_location_name = NULL, study_started_at = NULL WHERE id = %s",
            (user.id,),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()
    return {"stopped": True}


# ─────────────────────────────────────────────────────
# Username & Avatar
# ─────────────────────────────────────────────────────


class UsernameRequest(BaseModel):
    username: str


class AvatarRequest(BaseModel):
    avatar_base64: str


@app.put("/me/username", response_model=UserPublic)
async def update_username(
    payload: UsernameRequest, user: UserInDB = Depends(auth_bearer)
) -> UserPublic:
    if not USERNAME_RE.match(payload.username):
        raise AppError(
            message="Username non valido. Usa solo lettere, numeri e underscore (3-20 caratteri).",
            status_code=400,
            code="invalid_username",
        )

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM users WHERE username = %s", (payload.username,))
        existing = cur.fetchone()
        if existing and existing["id"] != user.id:
            raise AppError(
                message="Username già in uso.",
                status_code=status.HTTP_409_CONFLICT,
                code="username_exists",
            )
        cur.execute("UPDATE users SET username = %s WHERE id = %s", (payload.username, user.id))
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    user.username = payload.username
    return _make_user_public(user)


@app.put("/me/avatar")
async def update_avatar(
    payload: AvatarRequest, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    try:
        base64.b64decode(payload.avatar_base64, validate=True)
    except Exception:
        raise AppError(message="avatar_base64 non è una stringa base64 valida.", code="invalid_base64")

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET avatar_base64 = %s WHERE id = %s",
            (payload.avatar_base64, user.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()
    return {"saved": True}


# ─────────────────────────────────────────────────────
# User search & lookup
# ─────────────────────────────────────────────────────


@app.get("/users/search", response_model=List[UserPublic])
async def search_users(q: str, user: UserInDB = Depends(auth_bearer)) -> List[UserPublic]:
    query = q.lstrip("@").lower()
    if not query:
        return []

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM users WHERE username ILIKE %s AND id != %s LIMIT 20",
            (f"{query}%", user.id),
        )
        rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return [_make_user_public(_db_to_user(_row_to_dict(row))) for row in rows]


@app.get("/users/{username}", response_model=UserPublic)
async def get_user_by_username(
    username: str, user: UserInDB = Depends(auth_bearer)
) -> UserPublic:
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    if not row:
        raise AppError(
            message="Utente non trovato.",
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
        )
    return _make_user_public(_db_to_user(_row_to_dict(row)))


# ─────────────────────────────────────────────────────
# Followers / Following
# ─────────────────────────────────────────────────────


def _get_user_by_username_sync(username: str) -> UserInDB:
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        raise AppError(
            message="Utente non trovato.",
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
        )
    return _db_to_user(_row_to_dict(row))


@app.post("/users/{username}/follow")
async def follow_user(
    username: str, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    try:
        target = await asyncio.to_thread(_get_user_by_username_sync, username)
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e

    if target.id == user.id:
        raise AppError(message="Non puoi seguire te stesso.", code="self_follow")
    if target.id in user.following:
        raise AppError(
            message="Già seguito.",
            status_code=status.HTTP_409_CONFLICT,
            code="already_following",
        )

    new_following = list(set(user.following + [target.id]))
    new_followers = list(set(target.followers + [user.id]))

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET following = %s::text[] WHERE id = %s",
            (new_following, user.id),
        )
        cur.execute(
            "UPDATE users SET followers = %s::text[] WHERE id = %s",
            (new_followers, target.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return {"following": True, "followers_count": len(new_followers)}


@app.delete("/users/{username}/follow")
async def unfollow_user(
    username: str, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    try:
        target = await asyncio.to_thread(_get_user_by_username_sync, username)
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e

    new_following = [uid for uid in user.following if uid != target.id]
    new_followers = [uid for uid in target.followers if uid != user.id]

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET following = %s::text[] WHERE id = %s",
            (new_following, user.id),
        )
        cur.execute(
            "UPDATE users SET followers = %s::text[] WHERE id = %s",
            (new_followers, target.id),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return {"following": False, "followers_count": len(new_followers)}


@app.get("/users/{username}/followers", response_model=List[UserPublic])
async def get_followers(
    username: str, user: UserInDB = Depends(auth_bearer)
) -> List[UserPublic]:
    try:
        target = await asyncio.to_thread(_get_user_by_username_sync, username)
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e

    if not target.followers:
        return []

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM users WHERE id = ANY(%s::text[])",
            (target.followers,),
        )
        rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return [_make_user_public(_db_to_user(_row_to_dict(row))) for row in rows]


@app.get("/users/{username}/following", response_model=List[UserPublic])
async def get_following(
    username: str, user: UserInDB = Depends(auth_bearer)
) -> List[UserPublic]:
    try:
        target = await asyncio.to_thread(_get_user_by_username_sync, username)
    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e

    if not target.following:
        return []

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM users WHERE id = ANY(%s::text[])",
            (target.following,),
        )
        rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return [_make_user_public(_db_to_user(_row_to_dict(row))) for row in rows]


# ─────────────────────────────────────────────────────
# Stories
# ─────────────────────────────────────────────────────


class StoryRequest(BaseModel):
    image_base64: str
    caption: Optional[str] = None


@app.post("/me/story")
async def post_story(
    payload: StoryRequest, user: UserInDB = Depends(auth_bearer)
) -> Dict[str, Any]:
    try:
        base64.b64decode(payload.image_base64, validate=True)
    except Exception:
        raise AppError(message="image_base64 non è valido.", code="invalid_base64")

    story_id = str(uuid.uuid4())
    now = time.time()
    story: Dict[str, Any] = {
        "id": story_id,
        "user_id": user.id,
        "image_base64": payload.image_base64,
        "caption": payload.caption or "",
        "created_at": now,
    }

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO stories (id, user_id, image_base64, caption, created_at) VALUES (%s, %s, %s, %s, %s)",
            (story_id, user.id, payload.image_base64, payload.caption or "", now),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return story


@app.get("/stories/feed")
async def stories_feed(user: UserInDB = Depends(auth_bearer)) -> List[Dict[str, Any]]:
    if not user.following:
        return []

    cutoff = time.time() - 86400
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM stories WHERE user_id = ANY(%s::text[]) AND created_at >= %s",
            (user.following, cutoff),
        )
        stories_rows = cur.fetchall()

        grouped: Dict[str, List[Dict]] = {}
        for s in stories_rows:
            s_dict = _row_to_dict(s)
            uid = s_dict["user_id"]
            grouped.setdefault(uid, []).append(s_dict)

        if not grouped:
            return []

        cur.execute(
            "SELECT * FROM users WHERE id = ANY(%s::text[])",
            (list(grouped.keys()),),
        )
        users_rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    result: List[Dict[str, Any]] = []
    for row in users_rows:
        uid = row["id"]
        u = _db_to_user(_row_to_dict(row))
        result.append({
            "user": _make_user_public(u).model_dump(),
            "stories": grouped.get(uid, []),
        })

    return result


# ─────────────────────────────────────────────────────
# Swipe & Matches
# ─────────────────────────────────────────────────────


class SwipeDirection(str, Enum):
    like = "like"
    dislike = "dislike"


class SwipeRequest(BaseModel):
    target_user_id: str
    direction: SwipeDirection


class SwipeResponse(BaseModel):
    saved: bool
    is_match: bool
    match_user_id: Optional[str] = None


def compute_match_score(me: UserInDB, candidate: UserInDB) -> int:
    if (
        not candidate.study_subjects
        or candidate.learning_style is None
        or me.learning_style is None
        or not me.study_subjects
    ):
        return 0
    if candidate.learning_style != me.learning_style:
        return 0
    overlap = set(map(str.lower, me.study_subjects)).intersection(
        set(map(str.lower, candidate.study_subjects))
    )
    if not overlap:
        return 0
    return 2 * len(overlap) + 3


@app.post("/matches/recommendations", response_model=List[Dict[str, Any]])
async def recommendations(
    limit: int = 10,
    user: UserInDB = Depends(auth_bearer),
) -> List[Dict[str, Any]]:
    if not user.study_subjects or user.learning_style is None:
        raise AppError(
            message="Profilo incompleto: imposta prima `study_subjects` e `learning_style`.",
            status_code=status.HTTP_400_BAD_REQUEST,
            code="profile_required",
        )

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE id != %s", (user.id,))
        all_rows = cur.fetchall()
        cur.execute(
            "SELECT to_user_id FROM swipes WHERE from_user_id = %s",
            (user.id,),
        )
        swiped_ids = {row["to_user_id"] for row in cur.fetchall()}
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    scored: List[Tuple[int, UserInDB, Dict]] = []
    now = time.time()
    for row in all_rows:
        candidate = _db_to_user(_row_to_dict(row))
        if candidate.id in swiped_ids:
            continue
        score = compute_match_score(user, candidate)
        if score > 0:
            scored.append((score, candidate, _row_to_dict(row)))

    scored.sort(key=lambda x: x[0], reverse=True)
    items = []
    for score, candidate, row in scored[: max(1, min(limit, 50))]:
        public = _make_user_public(candidate)
        is_studying_active = (
            candidate.is_studying
            and candidate.study_started_at is not None
            and (now - candidate.study_started_at < 7200)
        )
        items.append({
            "user": public.model_dump(),
            "score": score,
            "is_studying": is_studying_active,
            "study_location": candidate.study_location_name if is_studying_active else None,
            "username": candidate.username,
            "avatar_base64": candidate.avatar_base64,
            "followers_count": len(candidate.followers),
            "has_active_story": public.has_active_story,
        })
    return items


@app.post("/swipe", response_model=SwipeResponse)
async def swipe(
    payload: SwipeRequest, user: UserInDB = Depends(auth_bearer)
) -> SwipeResponse:
    if payload.target_user_id == user.id:
        raise AppError(
            message="Non puoi swipare te stesso.",
            status_code=status.HTTP_400_BAD_REQUEST,
            code="self_swipe",
        )

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE id = %s", (payload.target_user_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            raise AppError(
                message="target_user_id non valido.",
                status_code=status.HTTP_404_NOT_FOUND,
                code="target_not_found",
            )
        target = _db_to_user(_row_to_dict(row))

        cur.execute(
            """
            INSERT INTO swipes (id, from_user_id, to_user_id, direction, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (from_user_id, to_user_id)
            DO UPDATE SET direction = EXCLUDED.direction, created_at = EXCLUDED.created_at
            """,
            (str(uuid.uuid4()), user.id, target.id, payload.direction.value, time.time()),
        )

        if payload.direction != SwipeDirection.like:
            cur.close()
            conn.close()
            return SwipeResponse(saved=True, is_match=False)

        cur.execute(
            "SELECT from_user_id FROM swipes WHERE from_user_id = %s AND to_user_id = %s AND direction = 'like'",
            (target.id, user.id),
        )
        is_mutual = bool(cur.fetchone())

        is_compatible = compute_match_score(user, target) > 0
        if is_compatible and is_mutual:
            a_id, b_id = sorted([user.id, target.id])
            match_id = str(uuid.uuid4())
            try:
                cur.execute(
                    """
                    INSERT INTO matches (id, user_a_id, user_b_id, matched_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_a_id, user_b_id) DO NOTHING
                    """,
                    (match_id, a_id, b_id, time.time()),
                )
            except Exception:
                pass
            cur.close()
            conn.close()
            return SwipeResponse(saved=True, is_match=True, match_user_id=target.id)

    except AppError:
        raise
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return SwipeResponse(saved=True, is_match=False)


class MatchItem(BaseModel):
    match_user: UserPublic
    matched_at: float


@app.get("/matches/me", response_model=List[MatchItem])
async def my_matches(user: UserInDB = Depends(auth_bearer)) -> List[MatchItem]:
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM matches WHERE user_a_id = %s OR user_b_id = %s",
            (user.id, user.id),
        )
        matches_rows = cur.fetchall()

        if not matches_rows:
            return []

        other_ids = []
        match_map: Dict[str, Dict] = {}
        for m in matches_rows:
            m_dict = _row_to_dict(m)
            other_id = m_dict["user_b_id"] if m_dict["user_a_id"] == user.id else m_dict["user_a_id"]
            other_ids.append(other_id)
            match_map[other_id] = m_dict

        if not other_ids:
            return []

        cur.execute(
            "SELECT * FROM users WHERE id = ANY(%s::text[])",
            (other_ids,),
        )
        users_rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    items: List[MatchItem] = []
    for row in users_rows:
        m = match_map.get(row["id"])
        if not m:
            continue
        other = _db_to_user(_row_to_dict(row))
        items.append(
            MatchItem(
                match_user=_make_user_public(other),
                matched_at=float(m.get("matched_at") or time.time()),
            )
        )

    items.sort(key=lambda x: x.matched_at, reverse=True)
    return items


# ─────────────────────────────────────────────────────
# Messages
# ─────────────────────────────────────────────────────


class MessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=4000)


def _find_match_id(user_id: str, other_user_id: str) -> Optional[str]:
    a_id, b_id = sorted([user_id, other_user_id])
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT id FROM matches WHERE user_a_id = %s AND user_b_id = %s",
            (a_id, b_id),
        )
        row = cur.fetchone()
        return str(row["id"]) if row else None
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


@app.get("/messages/{match_user_id}")
async def get_messages(
    match_user_id: str, user: UserInDB = Depends(auth_bearer)
) -> List[Dict[str, Any]]:
    match_id = await asyncio.to_thread(_find_match_id, user.id, match_user_id)
    if not match_id:
        raise AppError(
            message="Match non trovato.",
            status_code=status.HTTP_404_NOT_FOUND,
            code="match_not_found",
        )

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM messages WHERE match_id = %s ORDER BY created_at",
            (match_id,),
        )
        rows = cur.fetchall()
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return [_row_to_dict(row) for row in rows]


@app.post("/messages/{match_user_id}")
async def send_message(
    match_user_id: str,
    payload: MessageRequest,
    user: UserInDB = Depends(auth_bearer),
) -> Dict[str, Any]:
    match_id = await asyncio.to_thread(_find_match_id, user.id, match_user_id)
    if not match_id:
        raise AppError(
            message="Match non trovato.",
            status_code=status.HTTP_404_NOT_FOUND,
            code="match_not_found",
        )

    msg: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "match_id": match_id,
        "from_user_id": user.id,
        "text": payload.text,
        "created_at": time.time(),
    }

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO messages (id, match_id, from_user_id, text, created_at) VALUES (%s, %s, %s, %s, %s)",
            (msg["id"], msg["match_id"], msg["from_user_id"], msg["text"], msg["created_at"]),
        )
    except Exception as e:
        raise AppError(message="Errore database.", status_code=500) from e
    finally:
        cur.close()
        conn.close()

    return msg


# ─────────────────────────────────────────────────────
# Maps — Nominatim (unchanged logic)
# ─────────────────────────────────────────────────────


class PlaceCategory(str, Enum):
    cafe = "cafe"
    study_room = "study_room"


class NearbyRequest(BaseModel):
    lat: float
    lon: float
    radius_m: int = Field(default=1500, ge=50, le=10_000)
    limit: int = Field(default=20, ge=1, le=50)


class NearbyPlace(BaseModel):
    category: PlaceCategory
    name: str
    address: Optional[str] = None
    lat: float
    lon: float
    distance_m: Optional[float] = None


class NearbyResponse(BaseModel):
    places: List[NearbyPlace]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lam = radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lam / 2) ** 2
    return r * 2 * asin(sqrt(a))


def meters_to_deg_delta_lat(radius_m: int) -> float:
    return radius_m / 111_320.0


def meters_to_deg_delta_lon(radius_m: int, at_lat_deg: float) -> float:
    return radius_m / (111_320.0 * max(0.01, cos(radians(at_lat_deg))))


def nominatim_search_url(
    query: str, left: float, top: float, right: float, bottom: float, limit: int
) -> str:
    params = {
        "format": "json",
        "q": query,
        "limit": str(limit),
        "bounded": "1",
        "viewbox": f"{left},{top},{right},{bottom}",
    }
    return f"https://nominatim.openstreetmap.org/search?{urllib.parse.urlencode(params)}"


def fetch_json(url: str, timeout_s: int = 8) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": f"{APP_NAME} (FastAPI demo)"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


async def nominatim_search(
    query: str, lat: float, lon: float, radius_m: int, limit: int
) -> List[Dict[str, Any]]:
    delta_lat = meters_to_deg_delta_lat(radius_m)
    delta_lon = meters_to_deg_delta_lon(radius_m, lat)
    url = nominatim_search_url(
        query,
        lon - delta_lon,
        lat + delta_lat,
        lon + delta_lon,
        lat - delta_lat,
        limit=limit,
    )
    try:
        socket.setdefaulttimeout(10)
        data = await asyncio.to_thread(fetch_json, url)
        return data if isinstance(data, list) else []
    except Exception:
        return []


async def nominatim_search_nearby(
    category: PlaceCategory, lat: float, lon: float, radius_m: int, limit: int
) -> List[Dict[str, Any]]:
    query = "library" if category == PlaceCategory.study_room else "cafe"
    return await nominatim_search(query, lat=lat, lon=lon, radius_m=radius_m, limit=limit)


def _build_nearby_places(
    cafe_raw: List[Dict[str, Any]],
    library_raw: List[Dict[str, Any]],
    center_lat: float,
    center_lon: float,
    limit: int,
) -> List[NearbyPlace]:
    unique: Dict[str, Tuple[Dict[str, Any], PlaceCategory]] = {}
    for item in cafe_raw:
        osm_id = str(item.get("place_id") or item.get("osm_id") or "")
        if osm_id:
            unique.setdefault(osm_id, (item, PlaceCategory.cafe))
    for item in library_raw:
        osm_id = str(item.get("place_id") or item.get("osm_id") or "")
        if osm_id:
            unique.setdefault(osm_id, (item, PlaceCategory.study_room))

    places: List[NearbyPlace] = []
    for raw, cat in unique.values():
        try:
            r_lat = float(raw.get("lat"))
            r_lon = float(raw.get("lon"))
        except Exception:
            continue
        dist = haversine_m(center_lat, center_lon, r_lat, r_lon)
        places.append(
            NearbyPlace(
                category=cat,
                name=str(raw.get("name") or cat.value),
                address=raw.get("display_name"),
                lat=r_lat,
                lon=r_lon,
                distance_m=dist,
            )
        )
    places.sort(key=lambda p: p.distance_m if p.distance_m is not None else float("inf"))
    return places[:limit]


@app.post("/maps/nearby", response_model=NearbyResponse)
async def maps_nearby(payload: NearbyRequest) -> NearbyResponse:
    lat, lon, radius_m = payload.lat, payload.lon, payload.radius_m
    cafe_raw = await nominatim_search_nearby(PlaceCategory.cafe, lat, lon, radius_m, payload.limit)
    library_raw = await nominatim_search_nearby(PlaceCategory.study_room, lat, lon, radius_m, payload.limit)

    unique: Dict[str, Tuple[Dict[str, Any], PlaceCategory]] = {}
    for item in cafe_raw:
        osm_id = str(item.get("place_id") or item.get("osm_id") or "")
        if osm_id:
            unique.setdefault(osm_id, (item, PlaceCategory.cafe))
    for item in library_raw:
        osm_id = str(item.get("place_id") or item.get("osm_id") or "")
        if osm_id:
            unique.setdefault(osm_id, (item, PlaceCategory.study_room))

    places: List[NearbyPlace] = []
    for raw, cat in unique.values():
        try:
            r_lat = float(raw.get("lat"))
            r_lon = float(raw.get("lon"))
        except Exception:
            continue
        dist = haversine_m(lat, lon, r_lat, r_lon)
        if dist > radius_m * 1.2:
            continue
        places.append(
            NearbyPlace(
                category=cat,
                name=str(raw.get("name") or cat.value),
                address=raw.get("display_name"),
                lat=r_lat,
                lon=r_lon,
                distance_m=dist,
            )
        )

    places.sort(key=lambda p: p.distance_m if p.distance_m is not None else float("inf"))
    return NearbyResponse(places=places[: payload.limit])


@app.get("/maps/places", response_model=NearbyResponse)
async def places_by_city(city: str, limit: int = 20) -> NearbyResponse:
    geocode_params = urllib.parse.urlencode(
        {"q": f"{city}, Italy", "format": "json", "limit": "1"}
    )
    geocode_url = f"https://nominatim.openstreetmap.org/search?{geocode_params}"
    try:
        geocode_data = await asyncio.to_thread(fetch_json, geocode_url)
    except Exception:
        return NearbyResponse(places=[])
    if not geocode_data or not isinstance(geocode_data, list):
        return NearbyResponse(places=[])

    city_lat = float(geocode_data[0]["lat"])
    city_lon = float(geocode_data[0]["lon"])

    cafe_raw = await nominatim_search_nearby(PlaceCategory.cafe, city_lat, city_lon, 3000, limit)
    await asyncio.sleep(1.1)
    library_raw = await nominatim_search_nearby(
        PlaceCategory.study_room, city_lat, city_lon, 3000, limit
    )
    return NearbyResponse(places=_build_nearby_places(cafe_raw, library_raw, city_lat, city_lon, limit))


@app.get("/maps/places/bbox", response_model=NearbyResponse)
async def places_by_bbox(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 30,
) -> NearbyResponse:
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    async def _search(q: str) -> List[Dict[str, Any]]:
        params = {
            "format": "json",
            "q": q,
            "limit": str(limit),
            "bounded": "1",
            "viewbox": f"{min_lon},{max_lat},{max_lon},{min_lat}",
        }
        url = f"https://nominatim.openstreetmap.org/search?{urllib.parse.urlencode(params)}"
        try:
            data = await asyncio.to_thread(fetch_json, url)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    cafe_raw = await _search("cafe")
    await asyncio.sleep(1.1)
    library_raw = await _search("library")
    return NearbyResponse(
        places=_build_nearby_places(cafe_raw, library_raw, center_lat, center_lon, limit)
    )


# ─────────────────────────────────────────────────────
# Geo redirects
# ─────────────────────────────────────────────────────


class GeoRedirectResponse(BaseModel):
    uri: str


def build_geo_uri(lat: float, lon: float, label: Optional[str] = None) -> str:
    if label:
        safe_label = urllib.parse.quote(label, safe="")
        return f"geo:{lat},{lon}?q={lat},{lon}({safe_label})"
    return f"geo:{lat},{lon}"


@app.get("/redirect/geo", response_model=GeoRedirectResponse)
async def redirect_geo(
    lat: float, lon: float, label: Optional[str] = None
) -> GeoRedirectResponse:
    return GeoRedirectResponse(uri=build_geo_uri(lat, lon, label))


@app.get("/redirect/maps")
async def redirect_maps(
    lat: float, lon: float, label: Optional[str] = None
) -> Response:
    return RedirectResponse(url=build_geo_uri(lat, lon, label), status_code=307)
