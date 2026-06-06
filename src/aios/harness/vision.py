"""Vision-policy helper: single source of truth for image-into-vision decisions.

LiteLLM's :func:`token_counter` returns a flat ~85 tokens per
``image_url`` part regardless of provider; under-counting only
matters near the window boundary and provider rejection there is
recoverable.
"""

from __future__ import annotations

import base64
import binascii
from typing import Any

from aios.logging import get_logger
from aios_connector_http.mime import sniff_image_mime

log = get_logger("aios.harness.vision")


def correct_image_mime_b64(declared: str, data_b64: str) -> str:
    """Return the magic-byte-detected mime for a base64-encoded image,
    or ``declared`` unchanged when sniffing yields nothing.  Warns on
    substitution so operators see when a persisted event's declared
    mime disagreed with its bytes.
    """
    head_b64 = data_b64[:24]
    pad = (-len(head_b64)) % 4
    try:
        head = base64.b64decode(head_b64 + "=" * pad)
    except binascii.Error:
        return declared
    sniffed = sniff_image_mime(head)
    if sniffed is None or sniffed == declared:
        return declared
    log.warning("vision.image_mime_corrected", declared=declared, actual=sniffed)
    return sniffed


INLINE_SIZE_CAP_BYTES = 3_932_160  # 3.75 MiB — matches Anthropic's 5 MB base64 API ceiling.

# Largest edge (px) for downsampled inline copies.  See
# :mod:`aios.harness.image_resize` for the resize implementation.
INLINE_MAX_DIMENSION = 2000

# Upper bound on raw image size that we'll feed to Pillow.  Above this,
# downsampling is skipped and the renderer falls through to the marker —
# the worker would otherwise spend seconds decoding pathological inputs
# (uploaded TIFFs, multi-100MP camera RAWs, etc.).
PRE_RESIZE_CEILING_BYTES = 50 * 1024 * 1024

# Explicit per-model vision escape hatch (force True/False).  Empty in
# production — Claude is matched by name in ``supports_vision`` and everything
# else defers to litellm — but kept as the stub point for tests.
_VISION_OVERRIDES: dict[str, bool] = {}


def supports_vision(model: str) -> bool:
    """True when ``model`` accepts ``image_url`` content parts.

    Resolution order:

    1. :data:`_VISION_OVERRIDES` — explicit per-model escape hatch (force
       ``True`` or ``False``).
    2. Any Claude family is assumed vision-capable (3.x onward; aios targets
       4.x).  A long-running worker fetches litellm's catalog once at startup,
       so a Claude model released afterwards makes ``litellm.get_model_info``
       raise "isn't mapped yet" and we would otherwise collapse to "no vision"
       — silently degrading image reads to a text marker.  Asserting the family
       by name needs no edit when the next Claude lands.  The match is a
       substring rather than an ``anthropic/`` prefix because aios routes Claude
       through several providers whose strings all still contain ``claude`` (the
       routes ``_supports_anthropic_cache_control`` enumerates in
       :mod:`aios.harness.completion`).
    3. ``litellm.get_model_info`` for every other provider/model.
    """
    if model in _VISION_OVERRIDES:
        return _VISION_OVERRIDES[model]
    if "claude" in model.lower():
        return True
    # Grok 4.x is multimodal but absent from litellm's model catalog, so the
    # get_model_info lookup below raises "isn't mapped yet" and collapses to
    # no-vision — silently degrading image reads to a text marker. Assert the
    # family by name (same rationale as the Claude match above).
    if "grok-4" in model.lower():
        return True
    # Defer the heavy ``litellm`` import: every harness consumer of this
    # module pays ~1.18s of bootstrap otherwise, and most call sites never
    # reach this branch (Claude short-circuits above).
    import litellm

    try:
        info = litellm.get_model_info(model)
    except Exception as err:
        # ``get_model_info`` raises a mix of ``BadRequestError`` (unknown
        # model), KeyError, and import/network errors depending on the
        # failure mode.  Collapsing to "no vision" is the safe fallback
        # (we degrade to a text marker the model can still ``read``), but
        # the silence makes a transient outage look identical to "unknown
        # model" — log warn-level so operators have a grep target when
        # vision unexpectedly degrades across a deploy or provider blip.
        log.warning("vision.litellm_lookup_failed", model=model, error=str(err))
        return False
    return bool(info.get("supports_vision"))


def can_inline_image(*, model: str, content_type: str, size_bytes: int) -> bool:
    """True when ``model`` can see image bytes inlined as ``image_url``.

    Returns ``False`` for non-image content_types, oversize files
    (over :data:`INLINE_SIZE_CAP_BYTES`), and models without vision
    support.  Callers fall back to a text marker referencing the
    in-sandbox path so the model can still ``read`` the file later.
    """
    if not content_type.startswith("image/"):
        return False
    if size_bytes > INLINE_SIZE_CAP_BYTES:
        return False
    return supports_vision(model)


def make_image_url_part(*, content_type: str, data_b64: str) -> dict[str, Any]:
    """Build a chat-completions ``image_url`` content part.

    Reconciles the declared ``content_type`` against the magic bytes —
    inbound platform metadata and extension-based guesses both
    occasionally lie, and Anthropic rejects mime-vs-magic mismatches.
    Centralising the sniff here means every caller is covered without
    having to remember to wire correction at the call site.

    Also strips RFC-7231 parameters (anything after ``;``) from the mime
    before building the data URI. A connector posting an attachment with
    ``Content-Type: image/svg+xml; charset=utf-8`` would otherwise produce
    ``data:image/svg+xml; charset=utf-8;base64,...``, which Anthropic and
    most providers reject as malformed — bricking every wake of any
    session whose context now includes that part. ``correct_image_mime_b64``
    only rewrites for PNG/JPEG/GIF/WebP magic, so SVG/HEIC/AVIF/BMP
    declared values flow through unchanged unless stripped here.
    """
    content_type = correct_image_mime_b64(content_type, data_b64)
    bare_mime = content_type.split(";", 1)[0].strip()
    return {
        "type": "image_url",
        "image_url": {"url": f"data:{bare_mime};base64,{data_b64}"},
    }


def text_marker(record: dict[str, Any]) -> str:
    """Inert text marker for an attachment that won't be inlined.

    Used when the model can't see the pixels (non-vision model, oversize
    image, non-image attachment, legacy stub without ``in_sandbox_path``).
    The marker carries enough info for the model to ``read`` the path
    if the file is in fact reachable.
    """
    filename = record.get("filename") or "unnamed"
    content_type = record.get("content_type") or "application/octet-stream"
    size = record.get("size")
    path = record.get("in_sandbox_path")

    size_str = human_size(size) if isinstance(size, int) else "unknown size"
    kind = (
        "image"
        if isinstance(content_type, str) and content_type.startswith("image/")
        else "attachment"
    )
    if path:
        return f"[{kind}: {filename} ({content_type}, {size_str}) at {path}]"
    return f"[{kind}: {filename} ({content_type}, {size_str})]"


def human_size(n: int) -> str:
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n / (1024 * 1024):.1f}MB"
