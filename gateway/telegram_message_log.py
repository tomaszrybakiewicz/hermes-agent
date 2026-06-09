"""Backward-compatible alias for the generic gateway message archive."""

from gateway.message_archive import MessageArchive as TelegramMessageLog

__all__ = ["TelegramMessageLog"]
