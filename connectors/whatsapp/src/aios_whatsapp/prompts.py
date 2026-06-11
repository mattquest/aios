"""Per-connector affordance prose for the WhatsApp account.

Returned in the ``InitializeResult.instructions`` field of the MCP
``initialize`` response; the aios harness concatenates it into the
session's system prompt under a ``## Connector: whatsapp/<phone>``
heading.

Mirrors ``connectors/signal/src/aios_signal/prompts.py`` in shape:
``build_instructions`` prepends an identity block (the bot's own JID
+ phone + push name) and a group-roster block to the static body so
the agent knows who it is and who else is in each group without
having to learn that from inbound traffic.

The static body covers only the tools this connector actually
exposes — telling the model about tools that don't exist would be
worse than silence.

Currently test-covered but not yet wired through the
aios-connector-http SDK; lights up automatically once the SDK's
``InitializeResult.instructions`` hook lands.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GroupRosterEntry:
    """One group's roster entry rendered into the system prompt.

    ``jid``: the group's WhatsApp JID (``...@g.us``) — what
    ``whatsapp_send`` and friends accept as ``chat_id``.
    ``name``: the group's display name; ``(unnamed)`` falls through
    for groups whose name field is empty.
    ``member_jids``: full JIDs of the bot's fellow participants.
    The bot itself is tagged ``(YOU)`` in the rendered output.
    """

    jid: str
    name: str
    member_jids: list[str]


def build_instructions(
    *,
    bot_jid: str,
    phone: str,
    push_name: str | None = None,
    groups: list[GroupRosterEntry] | None = None,
) -> str:
    """Compose the MCP ``initialize`` instructions with identity + roster.

    The agent otherwise has no reliable way to know which JID is
    itself — without this prelude, models confabulate identities in
    group chats.  The roster block makes every participant knowable
    upfront; silent peers don't effectively disappear.

    ``push_name``, when set, is the display name peers see for the
    bot.  Rendered as a third identity bullet; omitted entirely if
    ``None`` or empty so an un-set account doesn't ship a blank line.
    """
    identity = (
        "## Your identity on this WhatsApp account\n"
        "\n"
        f"- **jid**: `{bot_jid}` — this is YOU.  Any inbound whose "
        "header shows this jid as the sender is your own prior "
        "message, not another participant.\n"
        f"- **phone**: `{phone}` — this WhatsApp account is identified "
        "to peers by this number.\n"
    )
    if push_name:
        identity += f"- **push_name**: `{push_name}` — your display name on WhatsApp.\n"
    roster = _render_group_roster(bot_jid, groups or [])
    return identity + roster + "\n" + WHATSAPP_SERVER_INSTRUCTIONS


def _render_group_roster(bot_jid: str, groups: list[GroupRosterEntry]) -> str:
    if not groups:
        return ""
    bot_key = _jid_identity_key(bot_jid)
    lines: list[str] = ["\n## Your WhatsApp groups\n"]
    for g in groups:
        name = g.name or "(unnamed)"
        lines.append(f"\n- `{g.jid}` — {name}")
        for jid in g.member_jids:
            tag = "(YOU)" if _jid_identity_key(jid) == bot_key else ""
            line = f"    - `{jid}`"
            if tag:
                line += f" {tag}"
            lines.append(line)
    lines.append("")
    return "\n".join(lines) + "\n"


def _jid_identity_key(jid: str) -> str:
    """Normalize a WhatsApp JID to its identity-bearing local part.

    Strips the device suffix (``<phone>:<n>@s.whatsapp.net`` →
    ``<phone>``) and the host so a literal string compare across
    JID variants (with/without device suffix, ``@s.whatsapp.net``
    vs ``@lid``) doesn't silently lose the (YOU) self-tag.  LID
    JIDs use a different identifier space than phone JIDs and
    can't be equated; callers passing a phone-shape bot_jid into
    a LID-mode group still won't match, but the within-host
    variants now collapse correctly.
    """
    local = jid.split("@", 1)[0]
    return local.split(":", 1)[0]


WHATSAPP_SERVER_INSTRUCTIONS = """\
## channel_id

Each WhatsApp channel address is path-shaped:
``whatsapp/<phone>/<chat_id>``.  The full address is the channel's
``channel_id`` — the value the chat-targeting tools below require,
equal to your focal channel's channel_id (copy it from the channels
tail block or from ``switch_channel``).  No tool takes a bare
``chat_id`` segment; the destination chat is derived from your focal
channel.  The ``<chat_id>`` segment shapes you will see in addresses:
DMs use ``<digits>@s.whatsapp.net`` (or ``<digits>@lid`` for
linked-device-identity peers); groups use ``<id>@g.us``.

## Reading inbound messages

Every inbound message arrives prefixed with a bracketed header
carrying the ``message_id`` value that ``whatsapp_react`` /
``whatsapp_edit_message`` / ``whatsapp_delete_message`` expect.
Copy it verbatim — never construct, shorten, or reformat it.

Header shape (newlines added for clarity):

    [channel=whatsapp/<account>/<chat_id> · chat_type=<dm|group> ·
     chat_name='Group Name' · from=Alice · sender_jid=<jid> ·
     timestamp_ms=<ms> (<iso>) · message_id='<id>']

WhatsApp message ids are hex strings like ``3EB0E03B46303C22D750E2``
— pass them verbatim.

## Sending messages — `whatsapp_send`

Deliver a message to your focal chat:

    whatsapp_send(channel_id="<your focal channel_id>", text="your message here")

``channel_id`` must equal your focal channel's channel_id (copy it
from the channels tail block) — it states the destination so a reply
can never land on a chat you are not focused on.  To message a
different chat, call the built-in ``switch_channel`` tool first.
Plain assistant text on a focal channel is also delivered
automatically when the messages you are reacting to are on that
channel; use ``whatsapp_send`` when you need attachments or
quote-replies.

### Markdown subset

WhatsApp supports a compact inline grammar:

- *bold* (`**text**` or `__text__` in your CommonMark input)
- _italic_ (`*text*` or `_text_`)
- ~strike~ (`~~text~~`)
- ```inline code``` and ``` ```fenced code``` ```

Block-level constructs (headings, bullet lists, blockquotes, links,
images, tables) have **no WhatsApp rendering** and will appear as
literal characters in the recipient's chat.  Avoid them; paste URLs
directly instead of using `[link](url)` syntax.

### Mentions in groups

In a group, write `@<E.164>` to mention a member — for example
``@+15551234567 can you handle this?``.  The connector resolves the
phone to the participant's WhatsApp JID and attaches the mention to
the message's ContextInfo so WhatsApp clients render the @-tag as a
pill.  Mentions silently fall through as plain text if the phone
isn't a chat participant.

The group roster block above lists each member's JID — derive the
phone from the digits before `@s.whatsapp.net`.

### Attachments

Pass ``attachments=[<sandbox path>, ...]`` to send media alongside
text.  The text becomes the caption on the FIRST attachment (WhatsApp
has no media-group equivalent — each attachment is its own message).
Audio messages can't carry a caption, so when the first attachment is
audio the text is sent as a separate Conversation message instead.

## Reacting — `whatsapp_react`

Lighter-weight than a full message.  Pass the ``message_id`` from
the inbound header of the target:

    whatsapp_react(message_id="<id from header>", reaction="👍")

Pass an empty string to clear a prior reaction you placed:

    whatsapp_react(message_id="<id>", reaction="")

**Common reactions:** 👍 ❤️ 😂 😮 😢 🎉 🔥 ✅

**When to react instead of sending a full message:**

- Someone addresses you but a full reply would be overkill — a 👍 or
  ❤️ says "I see you".
- Good news worth celebrating — 🎉 or 🔥.
- Acknowledging you will handle something — ✅ or 👍.

Mundane messages do not need reactions; standout moments do.

## Editing — `whatsapp_edit_message`

Pass the ``message_id`` of a prior outbound to rewrite its body:

    whatsapp_edit_message(message_id="<id>", text="corrected version")

WhatsApp only allows editing your own messages, and only within ~15
minutes of the original send; the daemon surfaces the server's
rejection if either window is exceeded.  Empty text is refused —
use ``whatsapp_delete_message`` if you want the message gone.

## Deleting — `whatsapp_delete_message`

Delete-for-everyone a message you sent earlier.  Pass its
``message_id``:

    whatsapp_delete_message(message_id="<id>")

Only your own messages can be deleted.  Use sparingly — deletes are
visible (a tombstone replaces the message).

## Group admin — `whatsapp_list_groups`, `whatsapp_create_group`, `whatsapp_rename_group`

List every group the bot is in:

    whatsapp_list_groups()

Create a new group; pass +E.164 phones for the participants (the
bot is added implicitly as the creator):

    whatsapp_create_group(
        name="Project X",
        participants=["+15551234567", "+18007654321"],
    )

The result includes the new group's JID; the group's channel address
is ``whatsapp/<phone>/<jid>`` — hand that to ``switch_channel`` to
focus into it.

Rename your focal group (the bot must be an admin in it).  If the
group is not your focal channel, ``switch_channel`` into it first:

    whatsapp_rename_group(channel_id="<your focal channel_id>", name="New name")

## What you can and can't see in attachments

Inbound attachments differ in what your model can actually perceive:

- **Photos and static stickers** — vision-readable; you see the pixels.
  Animated stickers surface their emoji label via
  ``metadata.sticker_emoji`` so you can describe them without bytes.
- **Voice notes and audio messages** — NOT readable.  You see only the
  filename, mime type, and size.  Don't claim to have heard the audio.
- **Videos and animated content** — NOT readable.  Acknowledge what
  you can see (filename, type, size) and ask the user what's in it.
"""
