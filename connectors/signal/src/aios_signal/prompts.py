"""Per-connector affordance prose surfaced to the agent via MCP.

Returned in the ``InitializeResult.instructions`` field of the MCP
``initialize`` response; the aios harness concatenates it into the
session's system prompt under a ``## Connector: signal/<account>``
heading.

Covers only the tools this server actually exposes.  Telling the
model about tools that don't exist would be worse than silence.

The instructions are composed per-run: ``build_instructions`` prepends
an identity block (bot's own ``sender_uuid`` + phone number) and a
group-roster block (who's in each group the bot is a member of) to
the static body so the agent knows who it is and who it's talking to
without having to learn that from inbound traffic (issues #55, #57).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .daemon import GroupInfo


def build_instructions(
    *,
    bot_uuid: str,
    phone: str,
    profile_name: str | None = None,
    groups: list[GroupInfo] | None = None,
    contact_names: dict[str, str] | None = None,
) -> str:
    """Compose the MCP ``initialize`` instructions with identity + roster.

    The agent otherwise has no reliable source of truth for which
    ``sender_uuid`` is itself — models have been observed confabulating
    identities in group chats when this is absent.  The group-roster
    block makes every participant knowable without having to wait for
    them to speak; silent peers don't effectively disappear.

    ``profile_name``, when set, is the display name peers see for this
    account.  Rendered as a third identity bullet; omitted entirely if
    ``None`` or empty so an un-set-profile account doesn't get a blank
    line in the system prompt.
    """
    identity = (
        "## Your identity on this Signal account\n"
        "\n"
        f"- **sender_uuid**: `{bot_uuid}` — this is YOU.  In group "
        "messages, any inbound whose header shows this uuid is your own "
        "prior message, not another participant.\n"
        f"- **phone**: `{phone}` — this Signal account is identified to "
        "peers by this number.\n"
    )
    if profile_name:
        identity += f"- **profile_name**: `{profile_name}` — your display name on Signal.\n"
    roster = _render_group_roster(bot_uuid, groups or [], contact_names or {})
    return identity + roster + "\n" + SIGNAL_SERVER_INSTRUCTIONS


def _render_group_roster(
    bot_uuid: str,
    groups: list[GroupInfo],
    contact_names: dict[str, str],
) -> str:
    if not groups:
        return ""
    lines: list[str] = ["\n## Your Signal groups\n"]
    for g in groups:
        name = g.name or "(unnamed)"
        lines.append(f"\n- `{g.id}` — {name}")
        for uuid in g.member_uuids:
            if uuid == bot_uuid:
                tag = "(YOU)"
            else:
                display = contact_names.get(uuid)
                tag = display if display else "(name unknown)"
            lines.append(f"    - `{uuid}`: {tag}")
    lines.append("")
    return "\n".join(lines) + "\n"


SIGNAL_SERVER_INSTRUCTIONS = """\
## chat_id and channel_id

Each Signal channel address is path-shaped: ``signal/<account>/<chat_id>``.
The full address is the channel's ``channel_id``.  Every signal_* tool
requires a ``channel_id`` argument equal to your focal channel's
channel_id — copy it from the channels tail block.  The destination of
each call is your focal channel; to act on a different chat, call
``switch_channel(channel_id=...)`` first, read the re-orient context,
then call the tool.  A call whose channel_id does not match your focal
channel is rejected with an error instead of being delivered.

## Reading inbound messages

Every inbound message arrives prefixed with a bracketed header.  This
is the **only** authoritative source for the ``sender_uuid`` and
``timestamp_ms`` values that ``signal_send``'s ``quote_*`` /
``edit_timestamp_ms`` params and ``signal_react`` / ``signal_delete``
expect.  Copy them verbatim from the header — never construct,
shorten, or reformat them.

Header shape (newlines added for clarity):

    [channel=signal/<account>/<chat_id> · chat_type=<dm|group> ·
     chat_name='Group Name' · from=Alice · sender_uuid=<uuid> ·
     timestamp_ms=<ms> (<iso>)]

When the inbound is a reply, a second line follows:

    [reply_to: author_uuid=<uuid> · timestamp_ms=<ms>] > snippet

When it's a reaction:

    [reaction='👍' · target_author_uuid=<uuid> · target_timestamp_ms=<ms>]

So a reply to message 1700000000000 from Alice would look like:

    [channel=signal/.../grp_id · chat_type=group · from=Alice ·
     sender_uuid=fb2c91e2-... · timestamp_ms=1700000000999 (...)]
    [reply_to: author_uuid=22334455-... · timestamp_ms=1700000000000]
     > earlier message text...

To thread your own reply against the message Alice was responding to,
pass ``signal_send`` ``quote_timestamp_ms=1700000000000`` and
``quote_author_uuid="22334455-..."`` (copied from the ``reply_to``
line's ``timestamp_ms`` and ``author_uuid``).  To react to Alice's
message itself, pass ``signal_react`` ``target_timestamp_ms=1700000000999``
and ``target_author_uuid="fb2c91e2-..."`` (copied from the top
header's ``timestamp_ms`` and ``sender_uuid``).

## Sending messages — `signal_send`

Deliver a message to your focal chat:

    signal_send(channel_id="<your focal channel_id>", text="your message here")

``channel_id`` must equal your focal channel's channel_id — it states
the destination so a reply can never land on a chat you are not
focused on.  Plain assistant text on a focal channel is also delivered
automatically when the messages you are reacting to are on that
channel; use ``signal_send`` when you need quoting, editing, or
attachments.

### Avoid splitting one thought into multiple messages

Before calling `signal_send`, ask: "am I about to do some trivial work
and then send again?"  If yes, finish the work first and send one
combined message.  Back-to-back messages without substantial work in
between feel like a bot narrating its own steps.

Multiple `signal_send` calls are fine when there is genuinely heavy
work between them (research, file processing, long tasks) and you are
giving progress updates.  The test is whether the human would be left
wondering what is happening — if so, send an update.

### Quoting a prior message

Pass `quote_timestamp_ms` AND `quote_author_uuid` (both required) to
thread your reply as a quote of an earlier message.  Copy them from
the inbound header (see "Reading inbound messages" above).

**Use quote-reply when:**
- You're answering a message from earlier in the conversation, not
  the most recent one — without a quote, the receiver wonders which
  message you're answering.
- Multiple people are talking and it might otherwise be unclear who
  you're responding to.

**Don't quote when:**
- You're responding to the most recent message — it's already obvious.
- You're continuing a natural back-and-forth where the context is
  already clear.  Quoting every turn is noisy.

### Mentions in groups

In a group, write `@<uuid_prefix>` to mention a member — the prefix
must be at least 8 hex chars and uniquely match one member's UUID.
Full dashed UUIDs work too.  Example: ``@fb2c91e2 can you handle this?``
Resolved mentions are encoded automatically; an unresolved prefix
stays as plain text.  Mentions are ignored in DMs.

**Do NOT use display names** like ``@Alice`` or ``@Bob`` — they are
not resolved.  The recipient sees a literal ``@Alice`` instead of a
real mention.  The group roster block in your system prompt lists
each member's UUID; copy 8+ hex characters from there.

### Editing a prior message — `edit_timestamp_ms`

Pass `edit_timestamp_ms=<sent_at_ms>` to rewrite a message you sent
earlier.  The new `text` replaces the old; Signal clients show an
"edited" indicator.  You can only edit your own messages.  Get the
timestamp from `sent_at_ms` in a prior `signal_send` result.

## Deleting a message — `signal_delete`

Delete-for-everyone a message you sent earlier.  Pass the
`sent_at_ms` you got back from `signal_send`:

    signal_delete(channel_id="<your focal channel_id>",
                  target_timestamp_ms=<sent_at_ms>)

Only your own messages can be deleted.  Use sparingly — deletes are
visible (a tombstone replaces the message).

## Reacting — `signal_react`

Lighter-weight than a full message.  Call when an emoji says enough.
Use the ``sender_uuid`` and ``timestamp_ms`` from the inbound header
of the message you're reacting to:

    signal_react(
        channel_id="<your focal channel_id>",
        target_author_uuid="<sender_uuid from header>",
        target_timestamp_ms=<timestamp_ms from header>,
        emoji="👍",
    )

For example, if you see:

    [channel=... · from=Alice · sender_uuid=fb2c91e2-aaaa-... ·
     timestamp_ms=1700000000000 (...)]
    Done with the deploy!

react with:

    signal_react(
        channel_id="<your focal channel_id>",
        target_author_uuid="fb2c91e2-aaaa-...",
        target_timestamp_ms=1700000000000,
        emoji="🎉",
    )

**Common reactions:** 👍 ❤️ 😂 😮 😢 🎉 🔥 ✅

**When to react instead of sending a full message:**
- Someone addresses you but a full reply would be overkill — a 👍 or
  ❤️ says "I see you".
- Something genuinely makes you laugh or smile — 😂 or ❤️.
- Good news worth celebrating — 🎉 or 🔥.
- Acknowledging you will handle something — ✅ or 👍.
- A cute photo or sweet moment — ❤️.

Mundane messages do not need reactions; standout moments do.  Think
about what a human would naturally react to.

## Group admin — `signal_create_group`, `signal_rename_group`

Create a new group on the focal account:

    signal_create_group(channel_id="<your focal channel_id>",
                        name="Project X", member_uuids=["<uuid1>", "<uuid2>"])

You're added as the creator implicitly; pass the other members'
UUIDs.  The result includes the new group's id, which you can hand
to `switch_channel` to focus into it.

Rename your focal group (only valid when focal is a group, not a DM):

    signal_rename_group(channel_id="<your focal channel_id>", name="New name")

## Markdown subset

Signal supports a subset of markdown.  In `signal_send` text, use only:

- **bold** (`**text**`) and *italic* (`*text*`) — can be nested
- ~~strikethrough~~ (`~~text~~`)
- `inline code` and fenced code blocks (triple backticks)
- ||spoiler|| (`||text||`)
- Headers (`# text`, `## text`) — rendered as bold

**Do NOT use** markdown that Signal cannot render — it will appear as
raw characters in the recipient's chat:

- No `[links](url)` — paste URLs directly.
- No `> blockquotes`.
- No `- bullet lists` or `1. numbered lists` — use plain line breaks.
- No tables, images, or horizontal rules.

## What you can and can't see in attachments

Inbound attachments differ in what your model can actually perceive:

- **Photos and static stickers** — vision-readable; you see the pixels.
- **Voice notes and audio messages** — NOT readable.  You see only the
  filename, mime type, and size.  Don't claim to have heard the audio.
- **Videos and animated content** — NOT readable.  You cannot watch
  frames.  The filename can hint at content but is not authoritative.

**Rule:** never describe content you didn't actually perceive.  If a
video or audio attachment arrives, acknowledge what you can see
(filename, type, size) and ask the user what's in it, or use ``bash``
to peek at metadata.
"""
