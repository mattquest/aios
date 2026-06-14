"""Telegram connector built on the aios-connector-http SDK.

Multi-connection runtime container: one container can serve N
connections of type ``"telegram"``, each bound to one bot token.
Each connection gets its own python-telegram-bot ``Application``
because every bot token is a distinct PTB identity with its own
polling offset, identity, and update stream — there's no useful
sharing across tokens.

Lifecycle:

* :meth:`serve_connection` per connection: build the PTB
  ``Application`` from the connection's bot token, register message +
  reaction handlers, ``get_me`` for identity, then race a polling
  loop with an inbound drainer that funnels updates through
  :meth:`emit_inbound`.  On cancellation, stop polling + shut down
  the application cleanly.
* :meth:`teardown` is a no-op container-wide; per-connection
  cleanup is owned by ``serve_connection``'s ``finally``.

Tool methods take ``connection_id`` and ``chat_id`` from the call's
``focal_channel`` / payload automatically — declare them as kwargs and
the SDK threads them through.  Sandbox path resolution for outbound
attachments runs at the dispatcher level — declare ``attachments:
list[SandboxPath] | None`` and the SDK hands you host paths.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import structlog
from aios_connector_http import (
    Attachment as SDKAttachment,
)
from aios_connector_http import (
    AttachmentError,
    HttpConnector,
    SandboxPath,
    tool,
)
from telegram import (
    InputFile,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    ReactionTypeEmoji,
    Update,
)
from telegram.constants import ChatAction
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ContextTypes,
    MessageHandler,
    MessageReactionHandler,
    filters,
)

from .format import markdown_to_telegram_html
from .parse import (
    Attachment,
    InboundMessage,
    InboundReaction,
    parse_message,
    parse_reaction,
)

log = structlog.get_logger(__name__)


_ALLOWED_UPDATES: list[str] = [
    Update.MESSAGE,
    Update.EDITED_MESSAGE,
    Update.MESSAGE_REACTION,
]


@dataclass
class _TelegramConnectionState:
    """Per-connection PTB plumbing + identity caches."""

    application: Application  # type: ignore[type-arg]
    bot_id: int
    first_name: str
    username: str | None
    inbound_queue: asyncio.Queue[InboundMessage | InboundReaction]


class TelegramConnector(HttpConnector):
    connector = "telegram"
    state: dict[str, _TelegramConnectionState]

    # ── lifecycle ─────────────────────────────────────────────────────

    async def serve_connection(self, connection_id: str, secrets: dict[str, str]) -> None:
        """Build a PTB Application for this connection and run its loops.

        Each connection has its own bot token (one PTB Application per
        token); no sharing across connections.  Races polling + drainer
        in a :class:`asyncio.TaskGroup`; on cancellation, both stop and
        ``finally`` shuts the application down cleanly.
        """
        bot_token = secrets.get("bot_token")
        if not bot_token:
            raise RuntimeError(
                f"telegram connection {connection_id!r} requires a 'bot_token' entry in its secrets"
            )
        state = await self._build_state(bot_token)
        self.state[connection_id] = state
        log.info(
            "telegram.connection.ready",
            connection_id=connection_id,
            bot_id=state.bot_id,
            username=state.username,
            first_name=state.first_name,
        )
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self._run_polling(state),
                    name=f"telegram-polling-{connection_id}",
                )
                tg.create_task(
                    self._drain_queue(connection_id, state),
                    name=f"telegram-drain-{connection_id}",
                )
        finally:
            await self._shutdown_application(state.application)

    async def _build_state(self, bot_token: str) -> _TelegramConnectionState:
        application = Application.builder().token(bot_token).build()
        await application.initialize()
        try:
            me = await application.bot.get_me()
        except BaseException:
            await application.shutdown()
            raise

        bot_id = int(me.id)
        bot_username = me.username or None
        bot_display_name = me.first_name
        inbound_queue: asyncio.Queue[InboundMessage | InboundReaction] = asyncio.Queue()

        # Handlers are tiny so PTB's worker doesn't block on our queue
        # write.  Closures capture per-connection ``inbound_queue`` and
        # ``bot_id`` directly — no instance-wide aliases that drift
        # under multi-connection.
        async def on_message(update: Any, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
            message = update.message or update.edited_message
            if message is None:
                return
            parsed = parse_message(
                message,
                bot_id=bot_id,
                bot_username=bot_username,
                bot_display_name=bot_display_name,
            )
            if parsed is None:
                return
            await inbound_queue.put(parsed)

        async def on_reaction(update: Any, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
            reaction = update.message_reaction
            if reaction is None:
                return
            parsed = parse_reaction(reaction, bot_id=bot_id)
            if parsed is None:
                return
            await inbound_queue.put(parsed)

        async def on_error(_update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
            log.error("telegram.handler.error", bot_id=bot_id, error=str(context.error))

        application.add_handler(MessageHandler(filters.UpdateType.MESSAGES, on_message))
        application.add_handler(MessageReactionHandler(on_reaction))
        application.add_error_handler(on_error)

        return _TelegramConnectionState(
            application=application,
            bot_id=bot_id,
            first_name=me.first_name,
            username=me.username or None,
            inbound_queue=inbound_queue,
        )

    @staticmethod
    async def _shutdown_application(application: Application) -> None:  # type: ignore[type-arg]
        """Best-effort PTB shutdown sequence."""
        with contextlib.suppress(Exception):
            if application.updater is not None:
                await application.updater.stop()
        with contextlib.suppress(Exception):
            await application.stop()
        with contextlib.suppress(Exception):
            await application.shutdown()

    async def _run_polling(self, state: _TelegramConnectionState) -> None:
        await state.application.start()
        assert state.application.updater is not None
        # ``allowed_updates`` is opt-in — Telegram only delivers update
        # types we explicitly subscribe to.  Without this, edits and
        # reactions never reach the bot regardless of which handlers we
        # register locally.
        await state.application.updater.start_polling(allowed_updates=_ALLOWED_UPDATES)
        await asyncio.Event().wait()

    async def _drain_queue(self, connection_id: str, state: _TelegramConnectionState) -> None:
        while True:
            item = await state.inbound_queue.get()
            if isinstance(item, InboundReaction):
                await self._emit_reaction(connection_id, state, item)
            else:
                await self._emit_message(connection_id, state, item)

    async def _emit_message(
        self,
        connection_id: str,
        state: _TelegramConnectionState,
        msg: InboundMessage,
    ) -> None:
        sender_payload: dict[str, Any] = {
            "id": msg.sender_id,
            "display_name": msg.sender_name or str(msg.sender_id),
        }
        metadata = build_metadata(msg, state.bot_id)
        attachments = await self._download_attachments(state, msg.attachments)
        # Telegram's (chat_id, message_id) pair is the platform's
        # canonical message identity; feeding it as ``event_id`` lets
        # aios's ``inbound_acks`` dedupe a redelivered update after a
        # runtime restart (PTB's offset rewinds replay unread updates
        # the new container also sees). But Telegram PRESERVES
        # message_id across edits — an edit arrives as a fresh update
        # with the same id — so a naive (chat_id, message_id) event_id
        # makes every edit collide with the original and get silently
        # dropped by the dedup ledger, contradicting the documented
        # contract (prompts.py: "Edits arrive as a fresh inbound").
        # Suffix the edit timestamp so each revision is a distinct
        # inbound; non-edited messages keep the bare id, preserving the
        # restart-redelivery dedup property for the common case.
        # Ceiling: Telegram's edit_date is second-resolution, so two
        # edits of the same message within one wall-clock second still
        # collide and the second is dedup-dropped. That's the platform's
        # observability limit (no finer timestamp is exposed), not a
        # fixable gap here.
        event_id = f"telegram-{msg.chat_id}-{msg.message_id}"
        if msg.edit_date_ms is not None:
            event_id = f"{event_id}-e{msg.edit_date_ms}"
        await self.emit_inbound(
            connection_id=connection_id,
            event_id=event_id,
            chat_id=str(msg.chat_id),
            sender=sender_payload,
            content=msg.text,
            attachments=attachments,
            metadata=metadata,
            timestamp=_iso(msg.timestamp_ms),
        )

    async def _emit_reaction(
        self,
        connection_id: str,
        state: _TelegramConnectionState,
        reaction: InboundReaction,
    ) -> None:
        sender_payload: dict[str, Any] = {
            "id": reaction.sender_id,
            "display_name": reaction.sender_name or str(reaction.sender_id),
        }
        metadata: dict[str, Any] = {
            "channel": self.focal_channel(str(state.bot_id), str(reaction.chat_id)),
            "chat_type": reaction.chat_kind,
            "sender_id": reaction.sender_id,
            "timestamp_ms": reaction.timestamp_ms,
            "reaction": {
                "target_message_id": reaction.target_message_id,
                "old_emojis": list(reaction.old_emojis),
                "new_emojis": list(reaction.new_emojis),
            },
        }
        if reaction.sender_name is not None:
            metadata["sender_name"] = reaction.sender_name
        if reaction.chat_name is not None:
            metadata["chat_name"] = reaction.chat_name
        await self.emit_inbound(
            connection_id=connection_id,
            # Reactions key by (target_message_id, sender_id) since
            # the same user can re-react after clearing — pair with
            # timestamp_ms to discriminate edit waves of the same
            # target.  Stable across restarts since Telegram redelivers
            # the same MessageReactionUpdated payload via PTB's offset.
            event_id=(
                f"telegram-react-{reaction.chat_id}-{reaction.target_message_id}"
                f"-{reaction.sender_id}-{reaction.timestamp_ms}"
            ),
            chat_id=str(reaction.chat_id),
            sender=sender_payload,
            content="",
            metadata=metadata,
            timestamp=_iso(reaction.timestamp_ms),
        )

    async def _download_attachments(
        self,
        state: _TelegramConnectionState,
        attachments: tuple[Attachment, ...],
    ) -> list[tuple[str, bytes, str]] | None:
        """Download each attachment, validate size + bytes, return runtime tuples."""
        host_paths = await asyncio.gather(*(self._download_one(state, a) for a in attachments))
        out: list[tuple[str, bytes, str]] = []
        for att, host_path in zip(attachments, host_paths, strict=True):
            if host_path is None:
                continue
            # Unconditionally drop the per-attachment temp file once we've
            # taken the runtime tuple. ``_download_one`` creates it with
            # ``delete=False`` so PTB can write to it; without this
            # ``finally`` every successful Telegram inbound attachment
            # leaks one file into ``/tmp`` for the lifetime of the
            # connector container — eventual ``/tmp`` exhaustion kills
            # the next ``NamedTemporaryFile`` and tears the serve task
            # down. The runtime tuple carries the in-memory ``blob``
            # bytes; the on-disk path is single-use scratch.
            try:
                candidate = SDKAttachment(
                    host_path=str(host_path),
                    filename=att.filename,
                    content_type=att.content_type,
                )
                try:
                    candidate.as_params()  # size + existence
                except AttachmentError as err:
                    log.warning(
                        "telegram.inbound.attachment_rejected",
                        file_id=att.file_id,
                        filename=att.filename,
                        error=str(err),
                    )
                    continue
                try:
                    blob = host_path.read_bytes()
                except OSError as err:
                    log.warning(
                        "telegram.inbound.attachment_read_failed",
                        file_id=att.file_id,
                        filename=att.filename,
                        error=str(err),
                    )
                    continue
                out.append((att.filename, blob, att.content_type))
            finally:
                await asyncio.to_thread(host_path.unlink, missing_ok=True)
        return out or None

    async def _download_one(self, state: _TelegramConnectionState, att: Attachment) -> Path | None:
        try:
            file = await state.application.bot.get_file(att.file_id)
        except TelegramError as err:
            log.warning(
                "telegram.inbound.get_file_failed",
                file_id=att.file_id,
                error=str(err),
            )
            return None
        # Close the handle immediately so PTB owns the write side.
        with tempfile.NamedTemporaryFile(
            prefix="aios-telegram-", suffix=Path(att.filename).suffix, delete=False
        ) as tmp:
            target = Path(tmp.name)
        try:
            await file.download_to_drive(custom_path=target)
        except (TelegramError, OSError) as err:
            log.warning(
                "telegram.inbound.download_failed",
                file_id=att.file_id,
                target=str(target),
                error=str(err),
            )
            await asyncio.to_thread(target.unlink, missing_ok=True)
            return None
        return target

    # ── model-facing tools ────────────────────────────────────────────

    @tool(fire_and_forget=True)
    async def telegram_send(
        self,
        text: str,
        attachments: list[SandboxPath] | None = None,
        parse_mode: Literal["plain", "markdown", "html"] = "plain",
        reply_to_message_id: int | None = None,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Send a Telegram message to your focal chat, optionally with attachments.

        The message is delivered to your focal channel. You must pass
        channel_id equal to your focal channel's channel_id; to message a
        different chat, call the built-in ``switch_channel`` tool first.

        Args:
            text: Message body.  Becomes the caption when attachments are
                present.  See ``parse_mode``.
            attachments: Optional in-sandbox file paths.  Type is inferred
                from extension (``.jpg``/``.png``/``.webp`` → photo,
                ``.gif`` → animation (plays inline; ``send_photo``
                would render only the first frame), ``.mp4``/``.mov``
                → video, ``.ogg`` → voice, ``.mp3``/``.m4a``/``.wav``
                → audio, anything else → document).  Single attachment
                uses ``send_photo`` / ``send_animation`` / ``send_voice``
                / etc.; multiple attachments use ``send_media_group``
                with caption attached to the first item only (per
                Telegram API).
            parse_mode: How to interpret ``text``.
                ``"plain"`` (default) — literal characters, no
                formatting.  ``"markdown"`` — input is markdown;
                ``**bold**``, ``*italic*``, ``[label](url)``, fenced
                code, ``> quotes``, and ``||spoilers||`` are converted
                to Telegram's native styling.  ``"html"`` — input is
                raw HTML (``<b>``, ``<i>``, ``<a href="...">``,
                ``<code>``, ``<blockquote>``, ``<tg-spoiler>``); passed
                through verbatim to Telegram per its Bot API
                ``parse_mode=HTML`` semantics.
            reply_to_message_id: When set, the message is rendered as a
                native Telegram reply quoting that message (the client
                UI shows the parent inline above your text).  Pass the
                id of the message you're replying to — ``message_id``
                from a user inbound's metadata header, or ``message_id``
                from an earlier ``telegram_send`` response.  Default
                ``None`` sends as a top-level message in the chat.
        """
        state = self.state[connection_id]
        chat_id_int = _coerce_chat_id(chat_id)

        body, ptb_parse_mode = _prepare_text(text, parse_mode)
        host_paths: list[Path] = list(attachments or [])
        bot = state.application.bot

        reply_kwargs: dict[str, Any] = (
            {"reply_to_message_id": reply_to_message_id} if reply_to_message_id is not None else {}
        )

        if not host_paths:
            sent = await bot.send_message(
                chat_id=chat_id_int,
                text=body,
                parse_mode=ptb_parse_mode,
                **reply_kwargs,
            )
            return {"message_id": sent.message_id}

        if len(host_paths) == 1:
            single = await _send_single_media(
                bot,
                chat_id=chat_id_int,
                host_path=host_paths[0],
                caption=body or None,
                parse_mode=ptb_parse_mode,
                reply_to_message_id=reply_to_message_id,
            )
            return {"message_id": single.message_id}

        sent_group = await bot.send_media_group(
            chat_id=chat_id_int,
            media=_build_media_group(host_paths, caption=body or None, parse_mode=ptb_parse_mode),
            **reply_kwargs,
        )
        return {"message_ids": [m.message_id for m in sent_group]}

    @tool()
    async def telegram_typing(
        self,
        action: Literal[
            "typing", "upload_photo", "record_voice", "upload_voice", "upload_document"
        ] = "typing",
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Show a chat-action bubble (e.g. "typing…") in your focal chat.

        Telegram displays the action for up to 5 seconds or until the
        next message arrives.  Call this *before* doing slow work (a
        long tool run, an LLM hop) so users see you're alive; you don't
        need to keep re-calling it on a timer for typical replies.

        Args:
            action: Which bubble to show.  ``"typing"`` is the default
                and right for plain replies; ``"upload_photo"`` /
                ``"record_voice"`` / ``"upload_voice"`` /
                ``"upload_document"`` make sense before sending media.
        """
        state = self.state[connection_id]
        chat_id_int = _coerce_chat_id(chat_id)
        await state.application.bot.send_chat_action(chat_id=chat_id_int, action=ChatAction(action))
        return {"status": "ok"}

    @tool()
    async def telegram_edit_message(
        self,
        message_id: int,
        text: str,
        parse_mode: Literal["plain", "markdown", "html"] = "plain",
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Replace the text of a message you sent earlier in your focal chat.

        Useful for streaming-style updates ("I'm thinking…" → final
        answer) or correcting a typo without spamming a follow-up.
        Telegram only lets you edit your own messages and only within
        48 hours of sending.

        Args:
            message_id: The id of the message to edit.  Returned by
                ``telegram_send`` as ``message_id``.
            text: The new body.  See ``parse_mode``.
            parse_mode: How to interpret ``text``.  Same semantics as
                ``telegram_send``: ``"plain"`` literal text, ``"markdown"``
                runs the Markdown→Telegram-HTML converter, ``"html"``
                passes raw HTML through to Telegram.
        """
        state = self.state[connection_id]
        chat_id_int = _coerce_chat_id(chat_id)
        body, ptb_parse_mode = _prepare_text(text, parse_mode)
        edited = await state.application.bot.edit_message_text(
            chat_id=chat_id_int,
            message_id=message_id,
            text=body,
            parse_mode=ptb_parse_mode,
        )
        # ``edit_message_text`` returns ``True`` when the message is an
        # inline-bot message we don't own, otherwise the edited Message.
        if isinstance(edited, bool):
            return {"status": "ok"}
        return {"message_id": edited.message_id}

    @tool()
    async def telegram_delete_message(
        self,
        message_id: int,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Delete a message in your focal chat by id.

        You can delete your own messages within 48 hours.  In groups,
        admins can delete others' messages — but bots only get that
        permission when explicitly granted "Delete Messages."

        Args:
            message_id: The id of the message to delete.
        """
        state = self.state[connection_id]
        chat_id_int = _coerce_chat_id(chat_id)
        await state.application.bot.delete_message(chat_id=chat_id_int, message_id=message_id)
        return {"status": "ok"}

    @tool(fire_and_forget=True)
    async def telegram_react(
        self,
        message_id: int,
        emoji: str | None,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """React to a message in your focal chat with an emoji, or clear your reaction.

        Bots can set at most one reaction per message.  Pass
        ``emoji=None`` to clear the bot's existing reaction.

        Args:
            message_id: The id of the message to react to.
            emoji: A single emoji glyph (e.g. ``"👍"``, ``"❤"``, ``"🔥"``).
                Telegram restricts bot reactions to a curated allowlist;
                unsupported glyphs surface as ``Reaction_invalid`` from
                the API.  The full current allowlist is published at
                https://core.telegram.org/bots/api#reactiontypeemoji —
                check there if a glyph you'd expect to work is rejected.
                Pass ``None`` to clear the bot's existing reaction.
        """
        state = self.state[connection_id]
        chat_id_int = _coerce_chat_id(chat_id)
        reaction = [ReactionTypeEmoji(emoji=emoji)] if emoji is not None else None
        await state.application.bot.set_message_reaction(
            chat_id=chat_id_int,
            message_id=message_id,
            reaction=reaction,
        )
        return {"status": "ok"}


_PHOTO_EXTS = frozenset({".jpg", ".jpeg", ".png", ".webp"})
_ANIMATION_EXTS = frozenset({".gif"})
_VIDEO_EXTS = frozenset({".mp4", ".mov", ".webm"})
_VOICE_EXTS = frozenset({".ogg", ".oga"})
_AUDIO_EXTS = frozenset({".mp3", ".m4a", ".wav", ".flac"})


def _classify(host_path: Path) -> str:
    """Route an outbound attachment to the right PTB send method.

    ``.gif`` rides on ``send_animation`` (not ``send_photo``) — Telegram's
    Bot API treats a GIF passed to ``sendPhoto`` as a static image and
    only renders the first frame, defeating the point of an animated GIF.
    ``sendAnimation`` is Telegram's first-class animated-image surface;
    clients render it as an inline playing animation just like a video.
    """
    ext = host_path.suffix.lower()
    if ext in _PHOTO_EXTS:
        return "photo"
    if ext in _ANIMATION_EXTS:
        return "animation"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _VOICE_EXTS:
        return "voice"
    if ext in _AUDIO_EXTS:
        return "audio"
    return "document"


def _coerce_chat_id(chat_id: str) -> int:
    try:
        return int(chat_id)
    except ValueError as e:
        raise ValueError(f"telegram chat_id must be an integer; got {chat_id!r}") from e


def _prepare_text(text: str, parse_mode: str) -> tuple[str, str | None]:
    """Map a model-facing ``parse_mode`` to (body, ptb_parse_mode).

    Three modes, named so each matches the actual *input* convention
    being declared:

    - ``"plain"`` — literal text, no formatting.  Empty PTB parse_mode.
    - ``"markdown"`` — input is markdown; run through
      :func:`markdown_to_telegram_html` and tell PTB to render the
      result as HTML.  This is the new name for what used to be called
      ``"html"`` (which collided with Telegram Bot API semantics — see
      smoke finding #17).
    - ``"html"`` — input IS HTML; pass through verbatim and tell PTB
      to render as HTML.  Matches Telegram's own ``parse_mode=HTML``
      semantics, so an agent that writes ``<a href="...">link</a>``
      gets the expected rendering instead of literal text.
    """
    if parse_mode == "markdown":
        return markdown_to_telegram_html(text), "HTML"
    if parse_mode == "html":
        return text, "HTML"
    return text, None


def _iso(ts_ms: int) -> str:
    """Render a unix-ms timestamp as ISO-8601 UTC."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat()


def _read_for_upload(host_path: Path) -> InputFile:
    """Wrap attachment bytes in a PTB :class:`InputFile` for upload.

    Two problems we have to thread:

    1. python-telegram-bot's HTTPX request layer JSON-serializes the
       request body; passing a raw ``pathlib.Path`` falls into the
       "unknown object" branch and raises ``TypeError('Object of type
       PosixPath is not JSON serializable')``.
    2. Passing raw ``bytes`` *does* survive the JSON path (PTB wraps
       them in InputFile internally), but with no filename hint the
       default falls to literal ``"application.octet-stream"`` and
       Telegram interprets the attachment as
       ``application/octet-stream`` — animated GIFs render as a
       static download instead of an inline animation, photos lose
       their image affordance, etc.  Surfaced live during PR 8 smoke
       when a ``.gif`` attachment rendered as a downloadable blob
       even after routing through ``send_animation``.

    Wrapping in ``InputFile(bytes, filename=host_path.name)`` lets
    PTB's multipart writer set the correct ``Content-Type`` from the
    extension, which Telegram then uses to render the attachment in
    its native player.
    """
    return InputFile(host_path.read_bytes(), filename=host_path.name)


async def _send_single_media(
    bot: Any,
    *,
    chat_id: int,
    host_path: Path,
    caption: str | None,
    parse_mode: str | None = None,
    reply_to_message_id: int | None = None,
) -> Any:
    kind = _classify(host_path)
    sender = {
        "photo": bot.send_photo,
        "animation": bot.send_animation,
        "video": bot.send_video,
        "voice": bot.send_voice,
        "audio": bot.send_audio,
        "document": bot.send_document,
    }[kind]
    kwargs: dict[str, Any] = {"chat_id": chat_id, kind: _read_for_upload(host_path)}
    if caption is not None:
        kwargs["caption"] = caption
        if parse_mode is not None:
            kwargs["parse_mode"] = parse_mode
    if reply_to_message_id is not None:
        kwargs["reply_to_message_id"] = reply_to_message_id
    return await sender(**kwargs)


def _build_media_group(
    host_paths: list[Path], *, caption: str | None, parse_mode: str | None = None
) -> list[Any]:
    # Caption rides on the FIRST item only — Telegram's API ignores
    # captions on items 2..N of a media group.
    items: list[Any] = []
    for idx, path in enumerate(host_paths):
        kind = _classify(path)
        kwargs: dict[str, Any] = {"media": _read_for_upload(path)}
        if idx == 0 and caption is not None:
            kwargs["caption"] = caption
            if parse_mode is not None:
                kwargs["parse_mode"] = parse_mode
        if kind == "photo":
            items.append(InputMediaPhoto(**kwargs))
        elif kind == "video":
            items.append(InputMediaVideo(**kwargs))
        elif kind == "audio":
            items.append(InputMediaAudio(**kwargs))
        else:
            # Voice can't go in a media group; fall back to document.
            items.append(InputMediaDocument(**kwargs))
    return items


def build_metadata(msg: InboundMessage, bot_id: int) -> dict[str, Any]:
    """Stamp telegram-specific metadata onto an inbound aios event.

    ``channel`` is redundant with what aios stamps server-side, but we
    include it so events are self-describing when read outside aios
    (e.g. ``aios sessions events`` JSON output).  Reply-payload is
    nested so the model sees it as a structured sibling of ``content``
    rather than embedded prose.

    Mentions ride as a structured list plus a derived ``self_mentioned``
    bool, matching the shape the signal connector emits and the harness
    renders.  ``uuid`` is the stringified telegram ``user_id`` — the
    platform-stable identity the model needs for downstream addressing.
    """
    metadata: dict[str, Any] = {
        "channel": f"telegram/{bot_id}/{msg.chat_id}",
        "chat_type": msg.chat_kind,
        "sender_id": msg.sender_id,
        "message_id": msg.message_id,
        "timestamp_ms": msg.timestamp_ms,
    }
    if msg.sender_name is not None:
        metadata["sender_name"] = msg.sender_name
    if msg.chat_name is not None:
        metadata["chat_name"] = msg.chat_name
    if msg.mentions:
        metadata["mentions"] = [{"uuid": str(m.user_id), "name": m.name} for m in msg.mentions]
        metadata["self_mentioned"] = any(m.user_id == bot_id for m in msg.mentions)
    if msg.reply is not None:
        metadata["reply_to"] = {
            "message_id": msg.reply.message_id,
            "text": msg.reply.text,
        }
    if msg.edited:
        metadata["edited"] = True
    if msg.sticker_emoji is not None:
        metadata["sticker_emoji"] = msg.sticker_emoji
    return metadata
