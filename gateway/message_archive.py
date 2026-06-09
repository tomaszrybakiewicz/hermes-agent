"""SQLite-backed archive for inbound/outbound gateway message traffic."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from hermes_constants import get_hermes_home
from hermes_state import apply_wal_with_fallback

logger = logging.getLogger(__name__)

_DB_FILENAME = "messages.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS archived_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_key TEXT UNIQUE,
    platform TEXT NOT NULL,
    direction TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    chat_name TEXT,
    chat_type TEXT,
    thread_id TEXT,
    user_id TEXT,
    user_name TEXT,
    platform_message_id TEXT,
    platform_update_id TEXT,
    reply_to_message_id TEXT,
    text TEXT,
    media_json TEXT,
    metadata_json TEXT,
    raw_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_archived_messages_platform_chat_time
    ON archived_messages(platform, chat_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived_messages_direction_time
    ON archived_messages(direction, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived_messages_thread_time
    ON archived_messages(thread_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived_messages_user_time
    ON archived_messages(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived_messages_platform_message
    ON archived_messages(platform, platform_message_id);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        try:
            return json.dumps(str(value), ensure_ascii=False)
        except Exception:
            return None


def _normalize_raw_message(raw_message: Any) -> Any:
    if raw_message is None:
        return None
    if hasattr(raw_message, "to_dict"):
        try:
            return raw_message.to_dict()
        except Exception:
            pass
    if isinstance(raw_message, (dict, list, str, int, float, bool)):
        return raw_message
    return str(raw_message)


def _message_key(*, platform: str, direction: str, chat_id: str, thread_id: Optional[str], platform_message_id: Optional[str]) -> Optional[str]:
    if not platform_message_id:
        return None
    return f"{platform}:{direction}:{chat_id}:{thread_id or ''}:{platform_message_id}"


class MessageArchive:
    """Small SQLite helper for archiving gateway messages across platforms."""

    def __init__(self, db_path: Optional[str] = None):
        path = db_path or str(get_hermes_home() / _DB_FILENAME)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        apply_wal_with_fallback(self._conn, db_label=_DB_FILENAME)
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()
        self._lock = threading.Lock()
        self.db_path = path

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def log_inbound_event(self, event: Any, *, event_kind: str = "received") -> None:
        source = getattr(event, "source", None)
        platform = self._platform_name(getattr(source, "platform", None)) or "unknown"
        record = {
            "platform": platform,
            "direction": "inbound",
            "event_kind": event_kind,
            "chat_id": str(getattr(source, "chat_id", "") or ""),
            "chat_name": getattr(source, "chat_name", None),
            "chat_type": getattr(source, "chat_type", None),
            "thread_id": self._string_or_none(getattr(source, "thread_id", None)),
            "user_id": self._string_or_none(
                getattr(source, "user_id_alt", None) or getattr(source, "user_id", None)
            ),
            "user_name": getattr(source, "user_name", None),
            "platform_message_id": self._string_or_none(getattr(event, "message_id", None)),
            "platform_update_id": self._string_or_none(getattr(event, "platform_update_id", None)),
            "reply_to_message_id": self._string_or_none(getattr(event, "reply_to_message_id", None)),
            "text": getattr(event, "text", "") or "",
            "media_json": _json_dumps(self._media_rows(event)),
            "metadata_json": None,
            "raw_json": _json_dumps(_normalize_raw_message(getattr(event, "raw_message", None))),
        }
        self._upsert(record)

    def log_outbound_message(
        self,
        *,
        platform: str,
        chat_id: str,
        text: str,
        platform_message_id: Optional[str],
        metadata: Optional[dict[str, Any]] = None,
        raw_message: Any = None,
        event_kind: str = "sent",
        reply_to_message_id: Optional[str] = None,
        media: Optional[list[dict[str, Any]]] = None,
        chat_name: Optional[str] = None,
        chat_type: Optional[str] = None,
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        thread_id: Optional[str] = None,
        platform_update_id: Optional[str] = None,
    ) -> None:
        normalized_platform = self._platform_name(platform) or "unknown"
        record = {
            "platform": normalized_platform,
            "direction": "outbound",
            "event_kind": event_kind,
            "chat_id": str(chat_id or ""),
            "chat_name": chat_name if chat_name is not None else self._chat_name_from_raw(raw_message),
            "chat_type": chat_type if chat_type is not None else self._chat_type_from_raw(raw_message),
            "thread_id": self._string_or_none(thread_id) or self._thread_id_from_metadata(metadata),
            "user_id": self._string_or_none(user_id) or self._user_id_from_raw(raw_message),
            "user_name": user_name if user_name is not None else self._user_name_from_raw(raw_message),
            "platform_message_id": self._outbound_message_id(raw_message, platform_message_id),
            "platform_update_id": self._string_or_none(platform_update_id),
            "reply_to_message_id": self._string_or_none(reply_to_message_id),
            "text": text or "",
            "media_json": _json_dumps(media),
            "metadata_json": _json_dumps(metadata),
            "raw_json": _json_dumps(_normalize_raw_message(raw_message)),
        }
        self._upsert(record)

    def _upsert(self, record: dict[str, Any]) -> None:
        chat_id = str(record.get("chat_id") or "")
        platform = self._platform_name(record.get("platform")) or "unknown"
        if not chat_id:
            return
        message_key = _message_key(
            platform=platform,
            direction=str(record.get("direction") or ""),
            chat_id=chat_id,
            thread_id=self._string_or_none(record.get("thread_id")),
            platform_message_id=self._string_or_none(record.get("platform_message_id")),
        )
        now = _utc_now_iso()
        payload = {
            "message_key": message_key,
            "platform": platform,
            "direction": record.get("direction"),
            "event_kind": record.get("event_kind"),
            "chat_id": chat_id,
            "chat_name": record.get("chat_name"),
            "chat_type": record.get("chat_type"),
            "thread_id": self._string_or_none(record.get("thread_id")),
            "user_id": self._string_or_none(record.get("user_id")),
            "user_name": record.get("user_name"),
            "platform_message_id": self._string_or_none(record.get("platform_message_id")),
            "platform_update_id": self._string_or_none(record.get("platform_update_id")),
            "reply_to_message_id": self._string_or_none(record.get("reply_to_message_id")),
            "text": record.get("text") or "",
            "media_json": record.get("media_json"),
            "metadata_json": record.get("metadata_json"),
            "raw_json": record.get("raw_json"),
            "created_at": now,
            "updated_at": now,
        }
        with self._lock:
            if message_key:
                self._conn.execute(
                    """
                    INSERT INTO archived_messages (
                        message_key, platform, direction, event_kind, chat_id, chat_name,
                        chat_type, thread_id, user_id, user_name, platform_message_id,
                        platform_update_id, reply_to_message_id, text, media_json,
                        metadata_json, raw_json, created_at, updated_at
                    ) VALUES (
                        :message_key, :platform, :direction, :event_kind, :chat_id, :chat_name,
                        :chat_type, :thread_id, :user_id, :user_name, :platform_message_id,
                        :platform_update_id, :reply_to_message_id, :text, :media_json,
                        :metadata_json, :raw_json, :created_at, :updated_at
                    )
                    ON CONFLICT(message_key) DO UPDATE SET
                        event_kind=excluded.event_kind,
                        chat_name=excluded.chat_name,
                        chat_type=excluded.chat_type,
                        thread_id=excluded.thread_id,
                        user_id=excluded.user_id,
                        user_name=excluded.user_name,
                        platform_update_id=COALESCE(excluded.platform_update_id, archived_messages.platform_update_id),
                        reply_to_message_id=COALESCE(excluded.reply_to_message_id, archived_messages.reply_to_message_id),
                        text=excluded.text,
                        media_json=COALESCE(excluded.media_json, archived_messages.media_json),
                        metadata_json=COALESCE(excluded.metadata_json, archived_messages.metadata_json),
                        raw_json=COALESCE(excluded.raw_json, archived_messages.raw_json),
                        updated_at=excluded.updated_at
                    """,
                    payload,
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO archived_messages (
                        message_key, platform, direction, event_kind, chat_id, chat_name,
                        chat_type, thread_id, user_id, user_name, platform_message_id,
                        platform_update_id, reply_to_message_id, text, media_json,
                        metadata_json, raw_json, created_at, updated_at
                    ) VALUES (
                        :message_key, :platform, :direction, :event_kind, :chat_id, :chat_name,
                        :chat_type, :thread_id, :user_id, :user_name, :platform_message_id,
                        :platform_update_id, :reply_to_message_id, :text, :media_json,
                        :metadata_json, :raw_json, :created_at, :updated_at
                    )
                    """,
                    payload,
                )
            self._conn.commit()

    @staticmethod
    def _platform_name(value: Any) -> str:
        raw = getattr(value, "value", value)
        return str(raw or "").strip().lower()

    @staticmethod
    def _string_or_none(value: Any) -> Optional[str]:
        if value is None or value == "":
            return None
        return str(value)

    @staticmethod
    def _media_rows(event: Any) -> list[dict[str, Any]]:
        urls = list(getattr(event, "media_urls", []) or [])
        types = list(getattr(event, "media_types", []) or [])
        rows: list[dict[str, Any]] = []
        for idx, url in enumerate(urls):
            rows.append({
                "url": url,
                "media_type": types[idx] if idx < len(types) else None,
            })
        return rows

    @classmethod
    def _thread_id_from_metadata(cls, metadata: Optional[dict[str, Any]]) -> Optional[str]:
        if not metadata:
            return None
        thread_id = metadata.get("thread_id") or metadata.get("message_thread_id") or metadata.get("direct_messages_topic_id")
        return cls._string_or_none(thread_id)

    @classmethod
    def _outbound_message_id(cls, raw_message: Any, platform_message_id: Optional[str]) -> Optional[str]:
        if raw_message is not None:
            mid = getattr(raw_message, "message_id", None)
            if mid is not None:
                return cls._string_or_none(mid)
        return cls._string_or_none(platform_message_id)

    @classmethod
    def _chat_name_from_raw(cls, raw_message: Any) -> Optional[str]:
        chat = getattr(raw_message, "chat", None)
        if chat is None:
            return None
        return getattr(chat, "title", None) or getattr(chat, "full_name", None)

    @classmethod
    def _chat_type_from_raw(cls, raw_message: Any) -> Optional[str]:
        chat = getattr(raw_message, "chat", None)
        if chat is None:
            return None
        value = str(getattr(chat, "type", "")).split(".")[-1].lower()
        return value or None

    @classmethod
    def _user_id_from_raw(cls, raw_message: Any) -> Optional[str]:
        user = getattr(raw_message, "from_user", None)
        if user is None:
            return None
        return cls._string_or_none(getattr(user, "id", None))

    @classmethod
    def _user_name_from_raw(cls, raw_message: Any) -> Optional[str]:
        user = getattr(raw_message, "from_user", None)
        if user is None:
            return None
        return getattr(user, "full_name", None) or getattr(user, "username", None)
