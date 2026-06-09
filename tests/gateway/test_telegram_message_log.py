import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

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
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, SendResult  # noqa: E402
from gateway.session import SessionSource  # noqa: E402
from gateway.message_archive import MessageArchive  # noqa: E402
from gateway.platforms.telegram import TelegramAdapter  # noqa: E402


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
    # upsert same outbound id to prove it updates instead of duplicating
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
async def test_base_handle_message_archives_inbound_before_dispatch():
    adapter = DummyArchiveAdapter()
    adapter._message_handler = AsyncMock(return_value=None)
    adapter._archive_inbound_event = MagicMock()
    adapter._start_session_processing = MagicMock()

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

    adapter._archive_inbound_event.assert_called_once_with(event)
    adapter._start_session_processing.assert_called_once()


@pytest.mark.asyncio
async def test_base_send_with_retry_archives_successful_outbound_text():
    adapter = DummyArchiveAdapter()
    adapter._archive_outbound_message = MagicMock()

    result = await adapter._send_with_retry(
        chat_id="123",
        content="hello world",
        reply_to="44",
        metadata={"thread_id": "99"},
    )

    assert result.success is True
    adapter._archive_outbound_message.assert_called_once()
    kwargs = adapter._archive_outbound_message.call_args.kwargs
    assert kwargs["chat_id"] == "123"
    assert kwargs["text"] == "hello world"
    assert kwargs["message_id"] == "dummy-1"
    assert kwargs["reply_to_message_id"] == "44"


@pytest.mark.asyncio
async def test_send_document_logs_outbound_message(adapter, tmp_path):
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello")
    adapter._bot.send_document = AsyncMock(return_value=SimpleNamespace(message_id=321))
    adapter._archive_outbound_message = MagicMock()

    result = await adapter.send_document(
        chat_id="123",
        file_path=str(file_path),
        caption="Quarterly report",
        metadata={"thread_id": "77"},
    )

    assert result == SendResult(success=True, message_id="321")
    adapter._archive_outbound_message.assert_called_once()
    kwargs = adapter._archive_outbound_message.call_args.kwargs
    assert kwargs["chat_id"] == "123"
    assert kwargs["text"] == "Quarterly report"
    assert kwargs["message_id"] == "321"
    assert kwargs["media"][0]["media_type"] == "document"
    assert kwargs["media"][0]["path"] == str(file_path)


@pytest.mark.asyncio
async def test_send_multiple_images_logs_each_sent_photo(adapter):
    adapter._bot.send_media_group = AsyncMock(return_value=[
        SimpleNamespace(message_id=11),
        SimpleNamespace(message_id=12),
    ])
    adapter._archive_outbound_message = MagicMock()

    await adapter.send_multiple_images(
        chat_id="123",
        images=[
            ("https://example.com/one.png", "One"),
            ("https://example.com/two.png", "Two"),
        ],
        metadata={"thread_id": "88"},
    )

    assert adapter._archive_outbound_message.call_count == 2
    first = adapter._archive_outbound_message.call_args_list[0].kwargs
    second = adapter._archive_outbound_message.call_args_list[1].kwargs
    assert first["message_id"] == "11"
    assert first["media"][0]["url"] == "https://example.com/one.png"
    assert second["message_id"] == "12"
    assert second["media"][0]["url"] == "https://example.com/two.png"
