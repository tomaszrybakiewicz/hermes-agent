import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _ensure_telegram_mock():
    if "telegram" in sys.modules and hasattr(sys.modules["telegram"], "__file__"):
        return

    telegram_mod = MagicMock()
    telegram_mod.ext.ContextTypes.DEFAULT_TYPE = type(None)
    telegram_mod.constants.ParseMode.MARKDOWN_V2 = "MarkdownV2"
    telegram_mod.constants.ChatType.GROUP = "group"
    telegram_mod.constants.ChatType.SUPERGROUP = "supergroup"
    telegram_mod.constants.ChatType.CHANNEL = "channel"
    telegram_mod.constants.ChatType.PRIVATE = "private"

    class InputMediaPhoto:
        def __init__(self, media=None, caption=None):
            self.media = media
            self.caption = caption

    telegram_mod.InputMediaPhoto = InputMediaPhoto

    for name in ("telegram", "telegram.ext", "telegram.constants", "telegram.request"):
        sys.modules.setdefault(name, telegram_mod)


_ensure_telegram_mock()

from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.hooks import HookRegistry  # noqa: E402
from gateway.message_archive import MessageArchive  # noqa: E402
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, SendResult  # noqa: E402
from gateway.platforms.telegram import TelegramAdapter  # noqa: E402
from gateway.session import SessionSource  # noqa: E402


class DummyArchiveAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)

    @property
    def name(self):
        return "dummy"

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id: str, content: str, reply_to=None, metadata=None) -> SendResult:
        return SendResult(success=True, message_id="dummy-1", raw_response={"ok": True})

    async def get_chat_info(self, chat_id: str):
        return {"name": "Dummy", "type": "dm"}


class RecordingHookRegistry:
    def __init__(self):
        self.calls = []

    async def emit(self, event_type, context=None):
        self.calls.append((event_type, context or {}))


@pytest.fixture()
def adapter():
    a = TelegramAdapter(PlatformConfig(enabled=True, token="fake-token"))
    a._bot = AsyncMock()
    return a


def test_message_archive_upserts_inbound_and_outbound(tmp_path):
    db_path = tmp_path / "messages.db"
    log = MessageArchive(str(db_path))

    event = MessageEvent(
        text="hello",
        source=SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_name="Test Chat", chat_type="private", user_id="7", user_name="Alice"),
        message_id="55",
        platform_update_id=9001,
        reply_to_message_id="44",
        media_urls=["file:///tmp/pic.png"],
        media_types=["image/png"],
        raw_message={"kind": "inbound"},
    )
    log.log_inbound_event(event)
    log.log_outbound_message(
        platform="telegram",
        chat_id="123",
        text="world",
        platform_message_id="56",
        metadata={"thread_id": "99"},
        raw_message=SimpleNamespace(
            message_id=56,
            chat=SimpleNamespace(title="Test Chat", full_name="Test Chat", type="private"),
            from_user=SimpleNamespace(id=999, full_name="Hermes"),
        ),
        reply_to_message_id="55",
        media=[{"media_type": "document", "path": "/tmp/a.txt"}],
    )
    log.log_outbound_message(
        platform="telegram",
        chat_id="123",
        text="world edited",
        platform_message_id="56",
        metadata={"thread_id": "99"},
        raw_message=None,
        event_kind="edited",
        reply_to_message_id="55",
    )

    rows = list(log._conn.execute(
        "select platform, direction, event_kind, chat_id, thread_id, platform_message_id, text, media_json from archived_messages order by id"
    ))
    assert len(rows) == 2
    assert rows[0][0] == "telegram"
    assert rows[0][1] == "inbound"
    assert rows[0][2] == "received"
    assert rows[0][3] == "123"
    assert rows[0][5] == "55"
    assert rows[1][0] == "telegram"
    assert rows[1][1] == "outbound"
    assert rows[1][2] == "edited"
    assert rows[1][4] == "99"
    assert rows[1][5] == "56"
    assert rows[1][6] == "world edited"
    assert rows[1][7] is not None

    log.close()


@pytest.mark.asyncio
async def test_base_handle_message_emits_inbound_hook_before_dispatch():
    adapter = DummyArchiveAdapter()
    adapter._message_handler = AsyncMock(return_value=None)
    adapter._start_session_processing = MagicMock()
    hooks = RecordingHookRegistry()

    adapter.set_hook_registry(hooks)

    event = MessageEvent(
        text="hello",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="123",
            chat_type="private",
            user_id="7",
            user_name="Alice",
        ),
        message_id="55",
    )

    await adapter.handle_message(event)

    assert len(hooks.calls) == 1
    event_type, context = hooks.calls[0]
    assert event_type == "message:inbound"
    assert context["event_kind"] == "received"
    assert context["direction"] == "inbound"
    assert context["event"] is event
    adapter._start_session_processing.assert_called_once()


@pytest.mark.asyncio
async def test_base_send_with_retry_emits_successful_outbound_hook():
    adapter = DummyArchiveAdapter()
    hooks = RecordingHookRegistry()
    adapter.set_hook_registry(hooks)

    result = await adapter._send_with_retry(
        chat_id="123",
        content="hello world",
        reply_to="44",
        metadata={"thread_id": "99"},
    )

    assert result.success is True
    assert len(hooks.calls) == 1
    event_type, context = hooks.calls[0]
    assert event_type == "message:outbound"
    assert context["event_kind"] == "sent"
    assert context["direction"] == "outbound"
    assert context["chat_id"] == "123"
    assert context["text"] == "hello world"
    assert context["message_id"] == "dummy-1"
    assert context["reply_to_message_id"] == "44"
    assert context["metadata"]["thread_id"] == "99"


@pytest.mark.asyncio
async def test_send_document_emits_outbound_hook(adapter, tmp_path):
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello")
    adapter._bot.send_document = AsyncMock(return_value=SimpleNamespace(message_id=321))
    hooks = RecordingHookRegistry()
    adapter.set_hook_registry(hooks)

    result = await adapter.send_document(
        chat_id="123",
        file_path=str(file_path),
        caption="Quarterly report",
        metadata={"thread_id": "77"},
    )

    assert result == SendResult(success=True, message_id="321")
    assert len(hooks.calls) == 1
    event_type, context = hooks.calls[0]
    assert event_type == "message:outbound"
    assert context["text"] == "Quarterly report"
    assert context["message_id"] == "321"
    assert context["media"][0]["media_type"] == "document"
    assert context["media"][0]["path"] == str(file_path)


@pytest.mark.asyncio
async def test_local_hook_can_archive_message_events(tmp_path):
    db_path = tmp_path / "messages.db"
    hooks_dir = tmp_path / "hooks"
    hook_dir = hooks_dir / "message-archive"
    hook_dir.mkdir(parents=True)
    (hook_dir / "HOOK.yaml").write_text(
        "name: message-archive\n"
        "description: archive message events\n"
        "events: ['message:inbound', 'message:outbound']\n"
    )
    (hook_dir / "handler.py").write_text(
        "from gateway.message_archive import MessageArchive\n"
        "archive = MessageArchive(r'" + str(db_path) + "')\n"
        "def handle(event_type, context):\n"
        "    if event_type == 'message:inbound':\n"
        "        archive.log_inbound_event(context['event'], event_kind=context.get('event_kind', 'received'))\n"
        "    elif event_type == 'message:outbound':\n"
        "        archive.log_outbound_message(\n"
        "            platform=context['platform'],\n"
        "            chat_id=context['chat_id'],\n"
        "            text=context.get('text', ''),\n"
        "            platform_message_id=context.get('message_id'),\n"
        "            metadata=context.get('metadata'),\n"
        "            raw_message=context.get('raw_message'),\n"
        "            event_kind=context.get('event_kind', 'sent'),\n"
        "            reply_to_message_id=context.get('reply_to_message_id'),\n"
        "            media=context.get('media'),\n"
        "            chat_name=context.get('chat_name'),\n"
        "            chat_type=context.get('chat_type'),\n"
        "            user_id=context.get('user_id'),\n"
        "            user_name=context.get('user_name'),\n"
        "            thread_id=context.get('thread_id'),\n"
        "            platform_update_id=context.get('platform_update_id'),\n"
        "        )\n"
    )

    reg = HookRegistry()
    with patch("gateway.hooks.HOOKS_DIR", hooks_dir), patch.object(reg, "_register_builtin_hooks"):
        reg.discover_and_load()

    adapter = DummyArchiveAdapter()
    adapter.set_hook_registry(reg)
    adapter._message_handler = AsyncMock(return_value=None)
    adapter._start_session_processing = MagicMock()

    inbound_event = MessageEvent(
        text="hello",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="123",
            chat_name="Test Chat",
            chat_type="private",
            user_id="7",
            user_name="Alice",
        ),
        message_id="55",
    )

    await adapter.handle_message(inbound_event)
    await adapter._emit_outbound_message_hook(
        chat_id="123",
        text="world",
        message_id="56",
        metadata={"thread_id": "99"},
        raw_message=SimpleNamespace(
            message_id=56,
            chat=SimpleNamespace(title="Test Chat", full_name="Test Chat", type="private"),
            from_user=SimpleNamespace(id=999, full_name="Hermes"),
        ),
        event_kind="sent",
        reply_to_message_id="55",
    )

    archive = MessageArchive(str(db_path))
    rows = [
        tuple(row)
        for row in archive._conn.execute(
            "select direction, event_kind, chat_id, thread_id, platform_message_id, text from archived_messages order by id"
        )
    ]
    assert rows == [
        ("inbound", "received", "123", None, "55", "hello"),
        ("outbound", "sent", "123", "99", "56", "world"),
    ]
    archive.close()
