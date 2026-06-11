"""Signal connector built on the aios-connector-http SDK.

Multi-connection runtime container: one container can serve N
connections of type ``"signal"``, each bound to a registered phone.
A single signal-cli daemon (launched without ``-a``) serves every
phone the operator has registered in ``config_dir/data/accounts.json``;
the connector class fans inbound envelopes out to per-connection
serve loops by the daemon's ``params.account`` field.

Lifecycle:

* :meth:`setup` spawns the shared :class:`SignalDaemon` and a
  dispatcher task that consumes ``daemon.listener.messages()`` and
  routes each ``(account, envelope)`` to a per-account
  :class:`asyncio.Queue`.
* :meth:`serve_connection` per connection: verify the phone is
  registered, resolve its bot UUID, load contacts + groups, then
  drain the per-account queue and forward each parsed envelope via
  :meth:`emit_inbound` with the connection_id stamped on it.
* :meth:`teardown` closes the daemon subprocess.  The dispatcher
  + per-connection drain loops live in the runner's TaskGroup and
  are cancelled by it on exit.

Tool methods take ``connection_id`` and ``chat_id`` from the call's
``focal_channel`` / payload automatically — declare them as kwargs
and the SDK threads them through.  Each method looks up its
connection's phone + bot UUID + caches via
``self.state[connection_id]``.

Operator-callable management handlers (``register``, ``verify``,
``updateProfile``) live in :mod:`aios_signal.management` and are
mixed in via :class:`SignalManagementMixin`.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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

from .addressing import decode_chat_id, encode_chat_id
from .config import Settings
from .daemon import GroupInfo, SignalDaemon
from .errors import RpcError
from .management import SignalManagementMixin
from .markdown import convert_markdown_to_signal_styles
from .mentions import build_mention_strings, encode_mentions
from .parse import InboundMessage, build_content_text, is_group_update_envelope, parse_envelope

log = structlog.get_logger(__name__)

# How long ``signal_send`` waits for the self-echo of a group send
# to arrive on the inbound stream before degrading to the
# no-timestamp result shape.  Signal-cli's ``send`` blocks until the
# network round-trip completes, so the echo typically arrives within
# tens of ms; 2.0s gives generous headroom for a slow network and
# still keeps the model's tool-call latency reasonable on failure.
_ECHO_WAIT_S: float = 2.0


@dataclass
class _SignalConnectionState:
    """Per-connection state: identity + per-phone roster caches.

    ``contact_names`` / ``groups`` are loaded once at
    :meth:`SignalConnector.serve_connection` startup; ``groups`` is
    refreshed on inbound group-update envelopes and on outbound
    mention-cache-miss in :meth:`SignalConnector._group_member_uuids`.
    """

    phone: str
    bot_uuid: str
    contact_names: dict[str, str] = field(default_factory=dict)
    groups: list[GroupInfo] = field(default_factory=list)


class SignalConnector(SignalManagementMixin, HttpConnector):
    connector = "signal"
    state: dict[str, _SignalConnectionState]

    def __init__(self, cfg: Settings) -> None:
        super().__init__()
        self._cfg = cfg
        self._daemon: SignalDaemon | None = None
        # Per-account inbound queues populated by the dispatcher task;
        # created on demand the first time the dispatcher sees that
        # phone OR ``serve_connection`` registers it (whichever comes
        # first — there's a natural race between an inbound arriving
        # and the connection's serve loop coming online).
        self._inbound_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        # Self-echo correlation for group sends: signal-cli 0.14.x's
        # ``send`` JSON-RPC returns a top-level ``timestamp`` for DM
        # sends but a bare ``null`` for groups.  The send timestamp
        # *does* arrive on the receive stream as a self-echo envelope
        # (source_uuid == bot_uuid + groupInfo + dataMessage.timestamp);
        # we register a future before issuing ``send`` and resolve it
        # from ``_handle_envelope`` when the matching echo arrives.
        # Keyed by ``(account_phone, chat_id)``, FIFO since signal-cli's
        # send blocks until the network round-trip completes so echoes
        # to the same chat arrive in send order.
        self._pending_echoes: dict[tuple[str, str], deque[asyncio.Future[int]]] = {}

    # ── lifecycle ─────────────────────────────────────────────────────

    async def setup(self, tg: asyncio.TaskGroup) -> None:
        """Open the shared signal-cli daemon + start the inbound dispatcher.

        signal-cli serves every account registered in
        ``config_dir/data/accounts.json``; we don't pass ``-a``.  Each
        connection's :meth:`serve_connection` verifies its own phone
        is registered before reading the dispatcher's per-account
        queue.

        The dispatcher task is spawned under the runner's TaskGroup so
        an unhandled exception mid-loop tears the container down (fail
        hard) instead of silently stalling inbound delivery.
        """
        self._daemon = await SignalDaemon(
            phones=[],
            config_dir=self._cfg.config_dir,
            cli_bin=self._cfg.cli_bin,
            host=self._cfg.daemon_host,
            port=self._cfg.daemon_port,
        ).__aenter__()
        tg.create_task(self._inbound_dispatcher(), name="signal-inbound-dispatcher")

    async def teardown(self) -> None:
        # The dispatcher task lives in the runner's TaskGroup and is
        # cancelled by it on exit; only the daemon subprocess is
        # ours to close here.
        if self._daemon is not None:
            await self._daemon.__aexit__(None, None, None)
            self._daemon = None

    async def serve_connection(self, connection_id: str, secrets: dict[str, str]) -> None:
        """Verify the phone, load roster, drain its inbound queue.

        ``secrets["phone"]`` is the account this connection owns.
        Missing → raise; :meth:`HttpConnector._isolated_serve_connection`
        logs the failure under ``connector.connection.serve_failed`` and
        keeps the container serving its other connections.
        """
        phone = secrets.get("phone")
        if not phone:
            raise RuntimeError(
                f"signal connection {connection_id!r} requires a 'phone' entry in its secrets"
            )
        assert self._daemon is not None, "setup() must run before serve_connection()"
        # Three independent reads against signal-cli: parallelize to cut
        # connection-bring-up latency by two RPC round-trips.  verify_phone
        # is a local file read; list_contacts / list_groups are JSON-RPCs.
        bot_uuid, contact_names, groups = await asyncio.gather(
            self._daemon.verify_phone(phone),
            self._daemon.list_contacts(account=phone),
            self._daemon.list_groups(account=phone),
        )
        state = _SignalConnectionState(
            phone=phone,
            bot_uuid=bot_uuid,
            contact_names=contact_names,
            groups=groups,
        )
        self.state[connection_id] = state
        queue = self._queue_for(phone)
        log.info(
            "signal.connection.ready",
            connection_id=connection_id,
            phone=phone,
            bot_uuid=bot_uuid,
            contacts=len(contact_names),
            groups=len(groups),
        )
        while True:
            envelope = await queue.get()
            await self._handle_envelope(connection_id, state, envelope)

    # ── inbound plumbing ──────────────────────────────────────────────

    def _queue_for(self, phone: str) -> asyncio.Queue[dict[str, Any]]:
        """Per-account inbound queue, created on first use.

        Unbounded: a bounded queue with silent-drop-on-full would lose
        real user messages under a slow ``emit_inbound`` round-trip.
        Back-pressure to signal-cli is the fail-hard alternative — the
        listener's queue ``put`` blocks, signal-cli's stdout drain
        buffer fills, and the operator notices via the resulting RPC
        timeout instead of by users missing replies.
        """
        if phone not in self._inbound_queues:
            self._inbound_queues[phone] = asyncio.Queue()
        return self._inbound_queues[phone]

    async def _inbound_dispatcher(self) -> None:
        """Fan ``daemon.listener.messages()`` out to per-account queues.

        Self-echo resolution rides at this layer rather than inside the
        per-account queue drain because signal-cli's group self-echoes
        (the ``dataMessage`` with ``groupInfo`` carrying the send
        timestamp) emit on the RECEIVING peers' account streams, not on
        the sender's own account stream.  When the bot sends to a group
        that includes other peers whose accounts signal-cli also serves
        (e.g. a co-located bot like Ultron in the QA group), the echo
        arrives on *their* stream and never reaches the bot's queue.
        Checking each envelope's ``sourceUuid`` against every known
        bot before routing lets us correlate regardless of which
        account stream the envelope landed on.
        """
        assert self._daemon is not None
        async for account, envelope in self._daemon.listener.messages():
            source_uuid = envelope.get("sourceUuid")
            if isinstance(source_uuid, str) and source_uuid:
                for state in self.state.values():
                    if state.bot_uuid == source_uuid:
                        self._maybe_resolve_self_echo(state, envelope)
                        break
            queue = self._queue_for(account.strip())
            await queue.put(envelope)

    async def _handle_envelope(
        self,
        connection_id: str,
        state: _SignalConnectionState,
        envelope: dict[str, Any],
    ) -> None:
        await self._maybe_refresh_roster(state, envelope)
        msg = parse_envelope(envelope, bot_account_uuid=state.bot_uuid)
        if msg is None:
            return
        if msg.sender_name is None:
            resolved = state.contact_names.get(msg.sender_uuid)
            if resolved:
                msg = replace(msg, sender_name=resolved)
        chat_id = encode_chat_id(msg.raw_chat_id, msg.chat_type)
        sender_payload: dict[str, Any] = {
            "uuid": msg.sender_uuid,
            "display_name": msg.sender_name or msg.sender_uuid,
        }
        attachments = await self._build_attachment_tuples(msg)
        # Signal envelope timestamps are ms since epoch.  Render as
        # ISO-8601 UTC so operators reading event logs see absolute
        # times rather than connector-source unix-ms.
        timestamp_iso = (
            datetime.fromtimestamp(msg.timestamp_ms / 1000, tz=UTC).isoformat()
            if msg.timestamp_ms
            else None
        )
        result = await self.emit_inbound(
            connection_id=connection_id,
            # Signal's (sender_uuid, timestamp_ms) pair is the platform's
            # canonical message identity; feeding it as ``event_id`` lets
            # aios's ``inbound_acks`` dedupe a redelivered envelope after
            # a runtime restart (the inbound dispatcher's queue can hold
            # up to ``maxsize`` envelopes that signal-cli would replay).
            event_id=f"signal-{msg.sender_uuid}-{msg.timestamp_ms}",
            chat_id=chat_id,
            sender=sender_payload,
            content=build_content_text(msg),
            attachments=attachments,
            metadata=build_metadata(msg, chat_id, state.bot_uuid),
            timestamp=timestamp_iso,
        )
        if result is None:
            return
        # Send a read receipt now that the message is persisted in the
        # session event log — semantically, "the agent has seen it."
        # signal-cli's automatic delivery receipts in daemon mode batch
        # unpredictably (some receipts land seconds after the envelope,
        # some get skipped until a later flush trigger), so the sender's
        # UI checkmark state lags reality.  An explicit read receipt
        # forces the 2nd checkmark immediately and gives the sender
        # confirmation tied to actual consumption rather than to
        # signal-cli's internal flush timing.  Best-effort: a failure
        # here is cosmetic only — the inbound is already in the log.
        await self._send_read_receipt(state, msg)

    # ── model-facing tools ────────────────────────────────────────────

    @tool()
    async def signal_send(
        self,
        text: str,
        attachments: list[SandboxPath] | None = None,
        quote_timestamp_ms: int | None = None,
        quote_author_uuid: str | None = None,
        edit_timestamp_ms: int | None = None,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Send a Signal message to your focal chat, optionally with attachments.

        The message is delivered to your focal channel. You must pass
        channel_id equal to your focal channel's channel_id; to message a
        different chat, call switch_channel first.

        Args:
            text: Message body.  Markdown is converted to Signal text styles.
                In group chats, ``@<uuid_prefix>`` mentions are encoded
                automatically when the prefix uniquely matches a member.
            attachments: Optional in-sandbox file paths to attach.  The SDK
                resolves each entry to a host path before this method runs.
            quote_timestamp_ms: ``timestamp_ms`` of the message you're
                quoting.  Must be set together with ``quote_author_uuid``;
                passing only one raises ``ValueError``.
            quote_author_uuid: ``sender_uuid`` of the message you're quoting.
            edit_timestamp_ms: ``sent_at_ms`` of a prior message from this
                account to rewrite.  The new ``text`` replaces the old one;
                Signal clients render an "edited" indicator.
        """
        state = self.state[connection_id]
        host_paths: list[Path] = list(attachments or [])
        member_uuids = await self._group_member_uuids(state, chat_id)
        params = _build_send_params(
            state.phone,
            chat_id,
            text,
            attachments=host_paths,
            member_uuids=member_uuids,
            quote_timestamp_ms=quote_timestamp_ms,
            quote_author_uuid=quote_author_uuid,
            edit_timestamp_ms=edit_timestamp_ms,
        )
        assert self._daemon is not None
        # Pre-register an echo future for group sends.  DMs get a
        # timestamp inline from signal-cli's send result; groups
        # return null there and we fish the timestamp out of the
        # self-echo on the inbound stream instead.  Registering
        # *before* issuing the RPC closes the race where the echo
        # could arrive before we're listening.
        chat_type, _ = decode_chat_id(chat_id)
        echo_future: asyncio.Future[int] | None = None
        if chat_type == "group":
            echo_future = asyncio.get_running_loop().create_future()
            self._pending_echoes.setdefault((state.phone, chat_id), deque()).append(echo_future)
        try:
            result = await self._daemon.rpc.call("send", params)
            ts = _extract_timestamp(result)
            if ts is None and echo_future is not None:
                try:
                    ts = await asyncio.wait_for(echo_future, timeout=_ECHO_WAIT_S)
                except (TimeoutError, asyncio.CancelledError):
                    # Echo never arrived in the deadline window — degrade
                    # to the no-timestamp shape.  The cancelled future
                    # gets pruned at next ``_maybe_resolve_self_echo``
                    # pop-from-front.
                    log.warning(
                        "signal.send.echo_timeout",
                        phone=state.phone,
                        chat_id=chat_id,
                    )
            return {"sent_at_ms": ts} if ts is not None else {"status": "ok"}
        finally:
            # Cancel any unresolved future so the drain-stale logic in
            # ``_maybe_resolve_self_echo`` prunes it before the NEXT
            # successful send's echo can match it.  Without this guard,
            # a failing rpc.call (e.g. signal-cli's libsignal
            # ``InvalidSessionException`` when a group member has no
            # established protocol session yet) would leak an orphan
            # future at the head of the deque, and the next echo would
            # resolve to the WRONG send's timestamp.  cancel() on
            # already-done futures (resolved by the echo dispatcher or
            # cancelled by wait_for's timeout path) is a no-op.
            if echo_future is not None and not echo_future.done():
                echo_future.cancel()

    @tool()
    async def signal_delete(
        self,
        target_timestamp_ms: int,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Delete-for-everyone a prior message in your focal Signal chat.

        signal-cli only allows deleting messages this account sent.

        Args:
            target_timestamp_ms: The Signal timestamp of the message to delete.
        """
        state = self.state[connection_id]
        assert self._daemon is not None
        params: dict[str, Any] = {
            "account": state.phone,
            "targetTimestamp": target_timestamp_ms,
            **_recipient_params(chat_id),
        }
        await self._daemon.rpc.call("remoteDelete", params)
        return {"status": "ok"}

    @tool()
    async def signal_create_group(
        self,
        name: str,
        member_uuids: list[str],
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Create a new Signal group on your account.

        channel_id is required by the focal protocol — pass your focal
        channel's channel_id. The new group itself has no channel yet;
        its id is returned so you can switch_channel into it later.

        Args:
            name: Group display name.
            member_uuids: ACI UUIDs of the members to add (excluding this
                account, which is added implicitly as the creator).
        """
        state = self.state[connection_id]
        assert self._daemon is not None
        result = await self._daemon.rpc.call(
            "updateGroup",
            {"account": state.phone, "name": name, "members": member_uuids},
        )
        if not isinstance(result, dict) or not result.get("groupId"):
            raise RuntimeError(f"signal-cli updateGroup did not return a groupId; got {result!r}")
        await self._refresh_roster(state)
        return {"group_id": result["groupId"]}

    @tool()
    async def signal_rename_group(
        self,
        name: str,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """Rename your focal Signal group.

        Only valid when the focal channel is a group.

        Args:
            name: The new group name.
        """
        state = self.state[connection_id]
        assert self._daemon is not None
        chat_type, raw_id = decode_chat_id(chat_id)
        if chat_type != "group":
            raise ValueError("signal_rename_group: focal channel is a DM, not a group")
        await self._daemon.rpc.call(
            "updateGroup",
            {"account": state.phone, "groupId": raw_id, "name": name},
        )
        return {"status": "ok"}

    @tool()
    async def signal_react(
        self,
        target_author_uuid: str,
        target_timestamp_ms: int,
        emoji: str,
        *,
        connection_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        """React to a message in your focal Signal chat with an emoji.

        The target message is identified by ``(target_author_uuid,
        target_timestamp_ms)``.  Every inbound Signal message in your
        conversation starts with a header line like ``[channel=... ·
        from=... · sender_uuid=<uuid> · timestamp_ms=<ms> (<iso>)]``;
        copy ``sender_uuid`` and the raw ``timestamp_ms`` integer from
        that header.

        Args:
            target_author_uuid: ``sender_uuid`` of the target message.
            target_timestamp_ms: ``timestamp_ms`` of the target message.
            emoji: The reaction emoji.
        """
        state = self.state[connection_id]
        assert self._daemon is not None
        params = _build_react_params(
            state.phone, chat_id, target_author_uuid, target_timestamp_ms, emoji
        )
        await self._daemon.rpc.call("sendReaction", params)
        return {"status": "ok"}

    # ── helpers ───────────────────────────────────────────────────────

    def _maybe_resolve_self_echo(
        self, state: _SignalConnectionState, envelope: dict[str, Any]
    ) -> None:
        """Resolve a pending ``signal_send`` echo future from a self-envelope.

        signal-cli emits the bot's own outbound group messages back on
        the receive stream as ``sourceUuid == bot_uuid`` envelopes with
        ``dataMessage.groupInfo``.  The envelope's ``timestamp`` is the
        send timestamp the model needs to edit / delete / react to the
        message later.  We match by ``(phone, chat_id)`` FIFO since
        signal-cli's send blocks until the network round-trip
        completes, so echoes to the same chat arrive in send order.

        Edits use a different envelope shape — ``editMessage.dataMessage``
        nested one level deeper, with the new edit's timestamp at the
        envelope root.  We accept both so an edit-send-then-edit-again
        flow (model wants the new timestamp for chained edits) works.

        DM echoes are silently dropped — signal-cli's DM send returns
        the timestamp inline, so the future-registration path in
        ``signal_send`` is groups-only.
        """
        if envelope.get("sourceUuid") != state.bot_uuid:
            return
        data = envelope.get("dataMessage")
        if not isinstance(data, dict):
            edit = envelope.get("editMessage")
            if isinstance(edit, dict):
                data = edit.get("dataMessage")
            if not isinstance(data, dict):
                return
        group_info = data.get("groupInfo")
        if not isinstance(group_info, dict):
            return
        raw_id = group_info.get("groupId")
        if not isinstance(raw_id, str) or not raw_id:
            return
        ts = envelope.get("timestamp")
        if not isinstance(ts, int):
            return
        chat_id = encode_chat_id(raw_id, "group")
        key = (state.phone, chat_id)
        queue = self._pending_echoes.get(key)
        if queue is None:
            return
        # Drain stale (timed-out / cancelled) waiters at the front so
        # the FIFO discipline reflects live ``signal_send`` callers.
        while queue and queue[0].done():
            queue.popleft()
        if queue:
            queue.popleft().set_result(ts)
        if not queue:
            self._pending_echoes.pop(key, None)

    async def _send_read_receipt(self, state: _SignalConnectionState, msg: InboundMessage) -> None:
        """Send a read receipt for ``msg`` to its original sender.

        Best-effort: signal-cli's daemon-mode automatic delivery
        receipts batch unpredictably, so we fire an explicit read
        receipt synchronously after ``emit_inbound`` succeeds.  A
        failure here is logged and swallowed — the inbound is already
        in the session log, so retrying or crashing would just churn.
        """
        assert self._daemon is not None
        params = {
            "account": state.phone,
            "recipient": msg.sender_uuid,
            "targetTimestamp": [msg.timestamp_ms],
            "type": "read",
        }
        try:
            await self._daemon.rpc.call("sendReceipt", params)
        except RpcError as exc:
            log.warning(
                "signal.read_receipt.send_failed",
                phone=state.phone,
                sender_uuid=msg.sender_uuid,
                target_timestamp_ms=msg.timestamp_ms,
                error=str(exc),
            )

    async def _maybe_refresh_roster(
        self, state: _SignalConnectionState, envelope: dict[str, Any]
    ) -> None:
        if is_group_update_envelope(envelope):
            await self._refresh_roster(state)

    async def _refresh_roster(self, state: _SignalConnectionState) -> None:
        assert self._daemon is not None
        state.groups = await self._daemon.list_groups(account=state.phone)

    async def _group_member_uuids(self, state: _SignalConnectionState, chat_id: str) -> list[str]:
        """Member UUIDs of the focal group, or ``[]`` for a DM.

        On cache miss, refreshes the roster once via ``listGroups`` and
        re-checks — signal-cli sometimes returns an empty group list at
        boot before the account's group state has finished loading from
        disk; without refresh-on-miss, mentions stay broken for the
        connection's lifetime.  Refreshing on miss also picks up groups
        joined after boot.
        """
        chat_type, _ = decode_chat_id(chat_id)
        if chat_type != "group":
            return []
        cached = self._lookup_group_members(state, chat_id)
        if cached is not None:
            return cached
        await self._refresh_roster(state)
        return self._lookup_group_members(state, chat_id) or []

    def _lookup_group_members(
        self, state: _SignalConnectionState, chat_id: str
    ) -> list[str] | None:
        for group in state.groups:
            if group.id == chat_id:
                return list(group.member_uuids)
        return None

    async def _build_attachment_tuples(
        self, msg: InboundMessage
    ) -> list[tuple[str, bytes, str]] | None:
        """Read each attachment's bytes for multipart upload to aios.

        signal-cli's JSON-RPC daemon mode (0.14.x) auto-downloads
        attachment bytes but omits the ``file`` field — we fall back to
        the storage-layout convention ``<config_dir>/attachments/<id>``
        for envelopes without a ``host_path``.  SDK :class:`Attachment`'s
        ``as_params`` validates size + existence; we then read the bytes
        off the event loop (multi-MiB photo+video attachments would
        block the inbound dispatcher otherwise) for the runtime-multipart
        ``emit_inbound`` shape.
        """
        validated: list[tuple[str, str, str]] = []
        for att in msg.attachments:
            host_path = att.host_path
            if host_path is None and att.id is not None:
                host_path = str(self._cfg.config_dir / "attachments" / att.id)
            if host_path is None:
                log.warning(
                    "signal.inbound.attachment_no_host_path",
                    content_type=att.content_type,
                    filename=att.filename,
                )
                continue
            filename = att.filename or "unnamed"
            try:
                SDKAttachment(
                    host_path=host_path,
                    filename=filename,
                    content_type=att.content_type,
                ).as_params()  # validate size + existence
            except AttachmentError as err:
                log.warning(
                    "signal.inbound.attachment_rejected",
                    host_path=host_path,
                    filename=filename,
                    error=str(err),
                )
                continue
            validated.append((host_path, filename, att.content_type))

        if not validated:
            return None

        blobs = await asyncio.gather(
            *(asyncio.to_thread(Path(p).read_bytes) for p, _, _ in validated),
            return_exceptions=True,
        )
        out: list[tuple[str, bytes, str]] = []
        for (host_path, filename, content_type), blob in zip(validated, blobs, strict=True):
            if isinstance(blob, BaseException):
                log.warning(
                    "signal.inbound.attachment_read_failed",
                    host_path=host_path,
                    filename=filename,
                    error=str(blob),
                )
                continue
            out.append((filename, blob, content_type))
        return out or None


def _recipient_params(chat_id: str) -> dict[str, Any]:
    """signal-cli's per-chat addressing: ``groupId`` for groups, ``recipient`` array for DMs."""
    chat_type, raw_id = decode_chat_id(chat_id)
    if chat_type == "group":
        return {"groupId": raw_id}
    return {"recipient": [raw_id]}


def _build_send_params(
    account_phone: str,
    chat_id: str,
    text: str,
    *,
    attachments: list[Path],
    member_uuids: list[str] | None = None,
    quote_timestamp_ms: int | None = None,
    quote_author_uuid: str | None = None,
    edit_timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Translate ``(account_phone, chat_id, text)`` into signal-cli ``send`` params.

    ``member_uuids`` enables outbound mention encoding for group sends:
    any ``@<uuid_prefix>`` in ``text`` that uniquely matches a member's
    UUID is rewritten as a U+FFFC placeholder + ``mentions`` array
    entry.  Mention encoding runs BEFORE markdown conversion so the
    UTF-16 offsets stay correct for both annotation lists.

    Quote: ``quote_timestamp_ms`` and ``quote_author_uuid`` must be
    set together; one-without-the-other raises ``ValueError`` (signal-cli
    rejects partial quotes, and silently dropping the model's intent
    leaves no signal that the thread didn't land).

    Edit: ``edit_timestamp_ms`` rewrites the prior message of this
    account at that timestamp.
    """
    if (quote_timestamp_ms is None) != (quote_author_uuid is None):
        raise ValueError("quote_timestamp_ms and quote_author_uuid must be set together")
    # Mention encoding inserts U+FFFC placeholders; markdown stripping
    # then removes delimiter chars (which can shift placeholder offsets
    # leftward).  Compute Signal's mention offsets against the *final*
    # stripped message so they match what the recipient sees.
    encoded, ordered_uuids = encode_mentions(text, member_uuids or [])
    stripped, styles = convert_markdown_to_signal_styles(encoded)
    mentions = build_mention_strings(stripped, ordered_uuids)
    params: dict[str, Any] = {
        "account": account_phone,
        "message": stripped,
        **_recipient_params(chat_id),
    }
    if styles:
        params["textStyles"] = styles
    if mentions:
        params["mentions"] = mentions
    if attachments:
        params["attachments"] = [str(p) for p in attachments]
    if quote_timestamp_ms is not None and quote_author_uuid is not None:
        params["quoteTimestamp"] = quote_timestamp_ms
        params["quoteAuthor"] = quote_author_uuid
    if edit_timestamp_ms is not None:
        params["editTimestamp"] = edit_timestamp_ms
    return params


def _build_react_params(
    account_phone: str,
    chat_id: str,
    target_author_uuid: str,
    target_timestamp_ms: int,
    emoji: str,
) -> dict[str, Any]:
    """Translate a react request into signal-cli ``sendReaction`` params."""
    return {
        "account": account_phone,
        "emoji": emoji,
        "targetAuthor": target_author_uuid,
        "targetTimestamp": target_timestamp_ms,
        **_recipient_params(chat_id),
    }


def build_metadata(msg: InboundMessage, chat_id: str, bot_uuid: str) -> dict[str, Any]:
    """Stamp signal-specific metadata onto an inbound aios event.

    ``channel`` is redundant with what aios stamps server-side, but we
    include it so events are self-describing when read outside aios
    (e.g. ``aios sessions events`` JSON output).  Reply / reaction
    payloads are nested so the model sees them as structured siblings
    of ``content`` rather than embedded prose.

    Mentions ride as a structured list plus a derived ``self_mentioned``
    bool, so the agent can distinguish "sender typed @<name> as text"
    from "sender's client encoded a mention targeting my UUID" without
    substring-searching ``content``.  Group sends in particular often
    @-tag the bot to summon a response, and the placeholder-substituted
    text alone hides that signal.
    """
    metadata: dict[str, Any] = {
        "channel": f"signal/{bot_uuid}/{chat_id}",
        "sender_uuid": msg.sender_uuid,
        "timestamp_ms": msg.timestamp_ms,
        "chat_type": msg.chat_type,
    }
    if msg.sender_name is not None:
        metadata["sender_name"] = msg.sender_name
    if msg.chat_name is not None:
        metadata["chat_name"] = msg.chat_name
    if msg.edited:
        # The harness's ``_format_channel_header`` already renders
        # ``edited=true`` when this flag is set; pairing it with
        # ``edit_target_timestamp_ms`` lets the model correlate the
        # edited message back to the original it's replacing.
        metadata["edited"] = True
        if msg.edit_target_timestamp_ms is not None:
            metadata["edit_target_timestamp_ms"] = msg.edit_target_timestamp_ms
    if msg.mentions:
        metadata["mentions"] = [{"uuid": m.uuid, "name": m.name} for m in msg.mentions]
        metadata["self_mentioned"] = any(m.uuid == bot_uuid for m in msg.mentions)
    if msg.reply is not None:
        metadata["reply_to"] = {
            "author_uuid": msg.reply.author_uuid,
            "timestamp_ms": msg.reply.timestamp_ms,
            "text": msg.reply.text,
        }
    if msg.reaction is not None:
        metadata["reaction"] = {
            "emoji": msg.reaction.emoji,
            "target_author_uuid": msg.reaction.target_author_uuid,
            "target_timestamp_ms": msg.reaction.target_timestamp_ms,
        }
    return metadata


def _extract_timestamp(rpc_result: Any) -> int | None:
    """Pull the send timestamp from signal-cli's ``send`` result, or ``None``.

    signal-cli delivers DM sends with ``{"timestamp": <ms>, ...}``, but
    group sends in 0.14.x return a bare ``null`` even on success — the
    RPC doesn't carry a timestamp.  RPC-level delivery failures raise
    ``RpcError`` in the transport layer, so if we reach this function
    the send *did* happen; a missing timestamp just means we don't
    have an ID to hand back.
    """
    if not isinstance(rpc_result, dict):
        return None
    ts = rpc_result.get("timestamp")
    return ts if isinstance(ts, int) else None
