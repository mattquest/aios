"""Wrapper around :func:`litellm.acompletion`.

Provides two variants:

* :func:`call_litellm` — non-streaming, returns ``(message, usage, cost)``.
* :func:`stream_litellm` — streaming, delivers per-token deltas via
  ``pg_notify`` and returns ``(message, usage, cost)``.

``cost`` is the LiteLLM-computed USD cost for the request, or ``None``
when the provider/model didn't report one.

Model API keys are resolved by LiteLLM from standard environment variables
(``OPENAI_API_KEY``, ``ANTHROPIC_API_KEY``, etc.) based on the model string
prefix.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from functools import cache
from typing import TYPE_CHECKING, Any

import litellm

from aios.config import get_settings
from aios.harness.context import _USER_MESSAGE_SEPARATOR_CONTENT

# Anthropic rejects empty text blocks that some OpenRouter models emit on
# tool-call-only turns; modify_params tells LiteLLM to sanitize them.
litellm.modify_params = True

# litellm 1.83.4 predates claude-fable-5, so its model_cost map has no entry for
# it — ``response_cost`` comes back None and every fable-5 turn records
# ``cost_usd=null``. Register pricing + capabilities explicitly so ``_extract_cost``
# works and so per-agent thinking params are accepted for the model. Both the bare
# id and the ``anthropic/``-prefixed routing id are registered; litellm may look up
# by either. ``setdefault`` so a future litellm that ships its own entry wins.
_FABLE5_MODEL_COST = {
    "input_cost_per_token": 10e-6,
    "output_cost_per_token": 50e-6,
    "litellm_provider": "anthropic",
    "mode": "chat",
    "supports_reasoning": True,
    "supports_function_calling": True,
    "supports_prompt_caching": True,
    "max_input_tokens": 1_000_000,
    "max_output_tokens": 128_000,
}
for _fable5_key in ("claude-fable-5", "anthropic/claude-fable-5"):
    litellm.model_cost.setdefault(_fable5_key, _FABLE5_MODEL_COST)

# LiteLLM 1.83.4's Anthropic adapter silently DROPS a requested ``thinking``
# param whenever the last tool-calling assistant message in the replayed
# history lacks ``thinking_blocks`` (guard for upstream issue #18926). The
# guard is over-broad: Anthropic accepts a thinking-enabled request against a
# thinking-less history (verified live against claude-fable-5 and
# claude-opus-4-8, 2026-06-10) — the real contract only requires that
# *previously emitted* thinking blocks be preserved, which ``_normalize_message``'s
# lift + ``_strip_to_spec``'s whitelist now do. Left in place, the guard also
# creates a bootstrap deadlock: thinking can never turn on for an existing
# session because no prior turn has thinking blocks, and no turn can produce
# them while the param keeps being dropped. Neutralize it. Remove when a
# litellm upgrade narrows the guard upstream.
try:  # defensive: private module path, may move across litellm versions
    from litellm.llms.anthropic.chat import transformation as _anthropic_transformation

    _anthropic_transformation.last_assistant_with_tool_calls_has_no_thinking_blocks = (  # type: ignore[attr-defined]
        lambda *args, **kwargs: False
    )
except (ImportError, AttributeError):  # pragma: no cover - litellm layout drift
    pass

# Default per-call bounds. Kept here so they're visible at the wrapper boundary
# rather than buried in defaults that drift between LiteLLM versions. Agents
# can override either via ``litellm_extra``; the spread happens after these
# defaults so user values win. The harness's job-level cap in ``run_session_step``
# is the safety net if both are bypassed somehow.
_REQUEST_TIMEOUT_S = 300.0
_STREAM_TTFT_TIMEOUT_S = 300.0
_STREAM_INTER_CHUNK_TIMEOUT_S = 60.0


class ModelCallDeadlineError(Exception):
    """The model call exceeded the configured total-duration deadline."""

    def __init__(
        self,
        message: str,
        *,
        usage: dict[str, int],
        cost_usd: float | None,
        chunks_seen: int,
    ) -> None:
        super().__init__(message)
        self.usage = usage
        self.cost_usd = cost_usd
        self.chunks_seen = chunks_seen


if TYPE_CHECKING:
    import asyncpg


def _normalize_message(msg: dict[str, Any]) -> dict[str, Any]:
    """Normalize provider quirks that break downstream consumers.

    Some LiteLLM providers return ``tool_calls: null`` instead of omitting
    the key (breaks ``jsonb_array_length``), and ``content: null`` instead
    of ``content: ""`` (breaks providers like MiniMax and Gemma when the
    message is replayed in a cross-model session).

    Anthropic thinking blocks are lifted from
    ``provider_specific_fields.thinking_blocks`` to the top-level
    ``thinking_blocks`` key. LiteLLM parks them in the former, but only the
    latter survives replay (``_strip_to_spec`` whitelists top-level
    ``thinking_blocks`` for thinking-capable targets). Without the lift,
    replayed assistant turns carry no thinking blocks, which (a) violates
    Anthropic's thinking-preservation contract across tool-use turns and
    (b) trips LiteLLM's guard that silently drops the requested
    ``thinking`` param — leaving models like claude-fable-5 running
    thinking-less, where they stochastically emit literal-empty turns that
    then poison the transcript (fable imitates degenerate turns in its own
    history; see the empty-turn cascade incident, 2026-06-09). Blocks with
    empty thinking text (the ``display: "omitted"`` default) are NOT
    lifted: a signature without its content fails Anthropic-side
    validation on replay ("Invalid `signature` in `thinking` block").
    """
    if "tool_calls" in msg and msg["tool_calls"] is None:
        del msg["tool_calls"]
    if msg.get("content") is None:
        msg["content"] = ""
    if not msg.get("thinking_blocks"):
        psf = msg.get("provider_specific_fields")
        blocks = psf.get("thinking_blocks") if isinstance(psf, dict) else None
        if isinstance(blocks, list):
            kept = [b for b in blocks if isinstance(b, dict) and (b.get("thinking") or "").strip()]
            if kept:
                msg["thinking_blocks"] = kept
            else:
                msg.pop("thinking_blocks", None)
        else:
            msg.pop("thinking_blocks", None)
    return msg


_CACHE_CONTROL = {"type": "ephemeral"}


def _set_content_block_cache(msg: dict[str, Any]) -> None:
    """Place ``cache_control`` on the last content block of a message.

    Anthropic requires ``cache_control`` on content blocks, not on the
    message dict itself.  If ``content`` is a plain string, it is converted
    to content-block format so the marker has somewhere to live.
    """
    content = msg.get("content")
    if isinstance(content, str):
        msg["content"] = [{"type": "text", "text": content, "cache_control": _CACHE_CONTROL}]
    elif isinstance(content, list) and content:
        content[-1]["cache_control"] = _CACHE_CONTROL


# LiteLLM providers that proxy Anthropic models and forward ``cache_control``
# markers unchanged (OpenRouter's ``anthropic/*`` routes, Bedrock's
# ``anthropic.*`` SKUs, Vertex AI's ``claude-*`` SKUs). Matching on provider
# alone isn't sufficient — the same providers also host non-Anthropic models
# (``openrouter/openai/*``, ``bedrock/amazon.titan-*``) that break on the
# content-block format. We additionally require the model name to carry
# ``claude`` or ``anthropic``.
_ANTHROPIC_PROXY_PROVIDERS = frozenset({"openrouter", "bedrock", "vertex_ai"})


@cache
def _supports_anthropic_cache_control(model: str) -> bool:
    """True when ``model`` accepts Anthropic ``cache_control`` markers.

    Used to gate ``inject_cache_breakpoints`` — see its docstring for why
    the gate is necessary. Unknown model strings return ``False`` so we
    default to the safe no-op.

    Covers direct Anthropic plus Anthropic-backed routes through
    OpenRouter / Bedrock / Vertex (all of which preserve ``cache_control``
    for Claude models). Non-Claude models on those same proxies stay
    gated out because they don't necessarily handle the content-block
    content shape that applying cache markers forces us into.

    Cached: called once per inference step, result is a pure function of
    the model string, and the distinct-model-string cardinality is low
    (agents typically reuse one or two).
    """
    try:
        model_name, provider, _, _ = litellm.get_llm_provider(model)
    except Exception:
        return False
    if provider == "anthropic":
        return True
    if provider in _ANTHROPIC_PROXY_PROVIDERS:
        lower = (model_name or model).lower()
        return "claude" in lower or "anthropic" in lower
    return False


_OPENAI_NATIVE_PROVIDERS = frozenset({"openai", "azure"})
_OPENAI_PROXY_PROVIDERS = frozenset({"openrouter"})
_XAI_CONVERSATION_HEADER = "x-grok-conv-id"
_USD_TICKS_PER_USD = 10_000_000_000


@cache
def _supports_openai_prompt_cache_key(model: str) -> bool:
    """True when ``model`` accepts OpenAI's ``prompt_cache_key`` field.

    OpenAI's Responses / Chat Completions APIs group requests by an
    explicit ``prompt_cache_key`` for cache eligibility. Anthropic uses
    ``cache_control`` content-block markers instead, so the two cache
    channels are mutually exclusive — the gate here mirrors
    :func:`_supports_anthropic_cache_control`, scoped to the OpenAI
    side: native OpenAI (direct ``openai`` plus Azure OpenAI, which is
    the same Responses / Chat Completions API on Microsoft infra and
    [documents the field](https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/prompt-caching)
    natively) plus OpenAI-backed routes through OpenRouter (which
    forwards unknown ``extra_body`` params to the backing provider).
    Non-OpenAI models on OpenRouter stay gated out — the field is
    silently dropped by OpenRouter for non-OpenAI backends and could
    trip parameter validation on some adapter versions.

    Unknown model strings return False (safe no-op) — same posture as
    the Anthropic gate.

    Cached for the same reason as the Anthropic counterpart: pure
    function of the model string, low cardinality.
    """
    try:
        model_name, provider, _, _ = litellm.get_llm_provider(model)
    except Exception:
        return False
    if provider in _OPENAI_NATIVE_PROVIDERS:
        return True
    if provider in _OPENAI_PROXY_PROVIDERS:
        lower = (model_name or model).lower()
        return lower.startswith("openai/")
    return False


@cache
def _supports_xai_conversation_id(model: str) -> bool:
    """True for direct xAI Grok Chat Completions routes.

    xAI's ``x-grok-conv-id`` request header gives the provider a stable
    conversation identity across calls.  Scope the shim to LiteLLM's direct
    ``xai`` provider and Grok model ids: OpenRouter and other OpenAI-compatible
    routes do not document this header and must remain unchanged.
    """
    try:
        model_name, provider, _, _ = litellm.get_llm_provider(model)
    except Exception:
        return False
    return provider == "xai" and (model_name or "").lower().startswith("grok")


def _apply_provider_cache_hints(
    kwargs: dict[str, Any],
    model: str,
    session_id: str | None,
) -> None:
    """Inject the provider-appropriate cache hint into outbound kwargs.

    Provider-specific request hints are dispatched here after agent extras merge:

    * **Anthropic** — content-block ``cache_control`` markers, set by
      :func:`inject_cache_breakpoints` directly on the messages list.
      Nothing to do here.
    * **OpenAI** — explicit ``prompt_cache_key`` field, nested under
      ``extra_body`` so it survives the litellm boundary. litellm 1.83.4
      strips unknown top-level kwargs from the outbound OpenAI HTTP body;
      ``extra_body`` is the documented pass-through that the OpenAI
      Python SDK merges into the request JSON. OpenAI's Responses / Chat
      Completions APIs group requests by ``prompt_cache_key`` for cache
      eligibility. The natural per-session scope keeps successive turns
      of the same session in the same bucket while distinct sessions
      don't collide.
    * **xAI Grok** — ``x-grok-conv-id`` in ``extra_headers``.  xAI uses this
      stable per-session conversation identity to improve cache affinity.

    Skips when ``session_id`` is unset — a caller that doesn't know the
    session (rare; only the harness's two call sites invoke these
    wrappers, and both have it) gets the safe no-op rather than a
    synthetic key that would re-bucket every call.

    **Merge order:** this helper runs AFTER the caller's ``extra``
    mapping (typically the agent's ``litellm_extra``) is merged into
    ``kwargs``, so agent-provided ``extra_body`` siblings (e.g.,
    OpenRouter ``provider.order``) are preserved alongside the cache
    key. The inner ``setdefault`` preserves an agent-provided explicit
    ``extra_body["prompt_cache_key"]`` override — agents may want a
    custom scope, e.g. to share a bucket across multiple sessions of
    the same conversation.
    """
    if session_id is None:
        return
    if _supports_openai_prompt_cache_key(model):
        extra_body = kwargs.setdefault("extra_body", {})
        extra_body.setdefault("prompt_cache_key", session_id)
    if _supports_xai_conversation_id(model):
        # Clone rather than mutate the nested mapping from agent.litellm_extra.
        # Keep every agent-provided sibling header, while making this one
        # provider contract authoritative and case-insensitively unique.
        extra_headers = dict(kwargs.get("extra_headers") or {})
        for name in tuple(extra_headers):
            if name.lower() == _XAI_CONVERSATION_HEADER:
                del extra_headers[name]
        extra_headers[_XAI_CONVERSATION_HEADER] = session_id
        kwargs["extra_headers"] = extra_headers


def inject_cache_breakpoints(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
    model: str,
) -> None:
    """Annotate messages and tools with Anthropic ``cache_control`` breakpoints.

    Anthropic's prompt caching requires explicit ``cache_control`` markers
    on **content blocks** (not on message dicts) to create cache entries.
    Applying them means converting string ``content`` into a list of
    content blocks — because only blocks can carry the marker.

    **Gated on model.** The earlier implementation applied this
    unconditionally under the assumption that "LiteLLM strips
    cache_control for providers that don't support it." In practice
    LiteLLM strips the ``cache_control`` key but leaves the list-of-
    blocks content format, and some OpenAI-compatible servers (notably
    MLX-based local Qwen servers) silently return empty completions
    when a ``tool``-role message arrives as a content-block list. The
    gate keeps the feature for Anthropic-backed routes (direct
    Anthropic, plus ``openrouter/anthropic/*``, ``bedrock/anthropic.*``,
    and ``vertex_ai/claude-*`` — all of which forward cache markers to
    Anthropic) and leaves string content untouched for everyone else.

    Places breakpoints on:

    1. **System message** — cache-stable across steps.
    2. **Last tool definition** — cache-stable while tools don't change.
    3. **Last stable conversation message** — the last event-sourced
       message, skipping the trailing channels tail block (which
       mutates every step: unread counts, previews) and any
       empty-assistant separator inserted before it by
       :func:`~aios.harness.context.merge_adjacent_user_messages`.

    Skipping the tail is load-bearing: with the breakpoint on the tail
    itself, the conversation prefix never gets its own cache entry and
    has to be re-cache-created every step.  Placing it on the last
    stable message lets the prefix cache across steps — next step's
    conversation-through-last-event is byte-identical and hits.
    """
    if not messages:
        return
    if not _supports_anthropic_cache_control(model):
        return

    if messages[0].get("role") == "system":
        _set_content_block_cache(messages[0])

    if tools:
        tools[-1]["cache_control"] = _CACHE_CONTROL

    idx = _last_stable_message_index(messages)
    if idx is not None and messages[idx].get("role") != "system":
        _set_content_block_cache(messages[idx])


def _last_stable_message_index(messages: list[dict[str, Any]]) -> int | None:
    """Return the index of the last cache-stable message, or ``None``.

    Walks backward from the end, skipping:

    * The channels tail block — identified by its content signature
      ``━━━ Channels ━━━`` (always the last user-role message when
      present).
    * Any role-transition separator — inserted by
      the former separator mechanism (now
      :func:`~aios.harness.context.merge_adjacent_user_messages`) to
      defeat Anthropic's adjacent-user-merge; carries only a
      single-byte placeholder and would be a wasted breakpoint.

    If nothing stable remains (messages list is just system + tail +
    separator), returns ``None``.
    """
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if _is_tail_block(msg) or _is_separator_placeholder(msg):
            continue
        return i
    return None


def _is_tail_block(msg: dict[str, Any]) -> bool:
    """Detect the channels tail block by its content signature.

    The tail block renders with a ``━━━ Channels ━━━`` header as the
    first line of its user-role content.  That string is unlikely to
    appear in genuine peer text, so a substring-match is safe enough
    for cache-breakpoint placement.
    """
    if msg.get("role") != "user":
        return False
    content = msg.get("content")
    if isinstance(content, str):
        return content.startswith("━━━ Channels ━━━")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text")
                if isinstance(text, str) and text.startswith("━━━ Channels ━━━"):
                    return True
    return False


def _is_separator_placeholder(msg: dict[str, Any]) -> bool:
    """Detect the role-transition separator placeholder.

    Matches the exact shape produced by
    the former separator mechanism:
    ``assistant`` role, no tool calls, content equal to
    :data:`~aios.harness.context._USER_MESSAGE_SEPARATOR_CONTENT`.

    Strict matching (not a broader "empty-ish assistant" check) keeps
    this recognizer aligned with the producer — if a genuine assistant
    turn happens to be short, it still gets a cache breakpoint.
    """
    if msg.get("role") != "assistant":
        return False
    if msg.get("tool_calls"):
        return False
    return msg.get("content") == _USER_MESSAGE_SEPARATOR_CONTENT


def estimate_cost_usd(model: str, usage: dict[str, int]) -> float | None:
    """Estimate USD cost from canonical token counters via LiteLLM's cost map."""
    try:
        from litellm.types.utils import Usage

        usage_object = Usage(
            prompt_tokens=usage.get("input_tokens", 0),
            completion_tokens=usage.get("output_tokens", 0),
            total_tokens=usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            prompt_tokens_details={
                "cached_tokens": usage.get("cache_read_input_tokens", 0),
            },
            cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0),
        )
        prompt_cost, completion_cost = litellm.cost_per_token(
            model=model, usage_object=usage_object
        )
    except Exception:
        return None
    return float(prompt_cost) + float(completion_cost)


def _extract_billed_cost_usd(response: Any) -> float | None:
    """Return xAI's exact billed ticks when present on a response/chunk."""
    usage = response.get("usage") if hasattr(response, "get") else getattr(response, "usage", None)
    if usage is not None:
        ticks = (
            usage.get("cost_in_usd_ticks")
            if hasattr(usage, "get")
            else getattr(usage, "cost_in_usd_ticks", None)
        )
        if isinstance(ticks, (int, float)) and not isinstance(ticks, bool) and ticks >= 0:
            return float(ticks) / _USD_TICKS_PER_USD
    return None


def _extract_cost(response: Any) -> float | None:
    """Pull exact provider cost, falling back to LiteLLM's estimate.

    xAI includes ``usage.cost_in_usd_ticks`` in Chat Completions responses,
    where one USD is exactly 10^10 ticks. Prefer that billed value over
    LiteLLM's hidden ``response_cost`` estimate. Streaming callers also
    capture this value directly from chunks because LiteLLM may omit the
    provider extension while assembling its final response.
    """
    billed_cost = _extract_billed_cost_usd(response)
    if billed_cost is not None:
        return billed_cost

    hidden = getattr(response, "_hidden_params", None)
    if not hidden:
        return None
    cost = hidden.get("response_cost")
    if cost is None:
        return None
    return float(cost)


def _normalize_usage(raw: dict[str, Any]) -> dict[str, int]:
    """Map LiteLLM's usage field names to our canonical names.

    LiteLLM uses OpenAI-style ``prompt_tokens`` / ``completion_tokens``.
    Some providers (Anthropic via LiteLLM) also pass through
    ``cache_creation_input_tokens`` and ``cache_read_input_tokens``
    at the top level. OpenAI-compatible providers put cache reads in
    ``prompt_tokens_details.cached_tokens``.
    """
    prompt_details = raw.get("prompt_tokens_details") or {}
    cache_read = raw.get("cache_read_input_tokens") or prompt_details.get("cached_tokens") or 0
    return {
        "input_tokens": raw.get("prompt_tokens") or 0,
        "output_tokens": raw.get("completion_tokens") or 0,
        "cache_read_input_tokens": cache_read,
        "cache_creation_input_tokens": raw.get("cache_creation_input_tokens") or 0,
    }


def _build_litellm_kwargs(
    *,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
    api_base: str | None,
    extra: dict[str, Any] | None,
    session_id: str | None,
    stream: bool,
) -> dict[str, Any]:
    """Assemble shared kwargs; stream adds ``stream=True`` + ``stream_timeout``."""
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "timeout": _REQUEST_TIMEOUT_S,
    }
    if stream:
        kwargs["stream"] = True
        kwargs["stream_timeout"] = _STREAM_INTER_CHUNK_TIMEOUT_S
    if tools:
        kwargs["tools"] = tools
    if api_base is not None:
        kwargs["api_base"] = api_base
    if extra:
        kwargs.update(extra)
    _apply_provider_cache_hints(kwargs, model, session_id)
    return kwargs


def _unpack_litellm_response(
    obj: Any, *, source: str
) -> tuple[dict[str, Any], dict[str, int], float | None, str | None]:
    """Extract ``(message, usage, cost, finish_reason)``.

    ``finish_reason`` is litellm's standardized stop reason for the choice
    (``"stop"``, ``"tool_calls"``, ``"length"``, ``"content_filter"`` for a
    safety refusal, …). The harness branches on ``"content_filter"`` to treat
    a refusal as a bricked turn rather than a normal completion (see
    ``loop.REFUSAL_FINISH_REASON``). ``source`` labels the TypeError on bad
    message shape.
    """
    usage_obj = obj.get("usage")
    usage = _normalize_usage(
        usage_obj.model_dump() if hasattr(usage_obj, "model_dump") else usage_obj or {}
    )
    cost = _extract_cost(obj)
    choice = obj["choices"][0]
    finish_reason: str | None = choice.get("finish_reason")
    message = choice["message"]
    # litellm returns a Message object that supports model_dump()
    if hasattr(message, "model_dump"):
        result: dict[str, Any] = message.model_dump()
        return _normalize_message(result), usage, cost, finish_reason
    if isinstance(message, dict):
        return _normalize_message(message), usage, cost, finish_reason
    raise TypeError(f"unexpected message type from {source}: {type(message).__name__}")


async def call_litellm(
    *,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    api_base: str | None = None,
    extra: dict[str, Any] | None = None,
    session_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, int], float | None, str | None]:
    """Call ``litellm.acompletion`` and return ``(message, usage, cost, finish_reason)``.

    Returns the message exactly as litellm produced it, including any
    provider-specific extensions like ``reasoning_content`` or
    ``thinking_blocks``. The harness stores the message dict opaquely.
    Usage is normalized to our canonical field names. Cost is LiteLLM's
    per-request USD figure, or ``None`` when the provider doesn't report it.
    ``finish_reason`` is litellm's standardized stop reason for the choice
    (notably ``"content_filter"`` for a safety refusal — see
    ``_unpack_litellm_response``).

    ``session_id`` (when provided on the openai provider path) is forwarded
    as OpenAI's ``prompt_cache_key`` so successive turns of the same
    session share a cache bucket. See ``_apply_provider_cache_hints``.
    """
    inject_cache_breakpoints(messages, tools, model)
    kwargs = _build_litellm_kwargs(
        model=model,
        messages=messages,
        tools=tools,
        api_base=api_base,
        extra=extra,
        session_id=session_id,
        stream=False,
    )
    deadline_s = get_settings().model_call_deadline_s
    try:
        response = await asyncio.wait_for(litellm.acompletion(**kwargs), timeout=deadline_s)
    except TimeoutError as exc:
        raise ModelCallDeadlineError(
            f"model call exceeded its {deadline_s:.0f}s total deadline before returning",
            usage={},
            cost_usd=None,
            chunks_seen=0,
        ) from exc
    return _unpack_litellm_response(response, source="litellm.acompletion")


async def stream_litellm(
    *,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    api_base: str | None = None,
    extra: dict[str, Any] | None = None,
    pool: asyncpg.Pool[Any],
    session_id: str,
) -> tuple[dict[str, Any], dict[str, int], float | None, str | None]:
    """Call ``litellm.acompletion`` with streaming, returning ``(message, usage, cost, finish_reason)``.

    Each content delta fires a transient ``pg_notify`` on the session's
    event channel. SSE clients receive these as ``event: delta`` — no DB
    row is created. After the stream exhausts, the complete message is
    assembled via ``litellm.stream_chunk_builder`` and returned for
    storage as a normal event. ``stream_chunk_builder`` reassembles the
    ``finish_reason`` with an unconditional last-wins loop, so a trailing
    chunk can clobber a ``content_filter`` refusal; the loop below captures
    the refusal off the wire and re-asserts it so the signal survives the
    streaming path too.
    """
    inject_cache_breakpoints(messages, tools, model)
    kwargs = _build_litellm_kwargs(
        model=model,
        messages=messages,
        tools=tools,
        api_base=api_base,
        extra=extra,
        session_id=session_id,
        stream=True,
    )
    deadline_s = get_settings().model_call_deadline_s
    loop = asyncio.get_running_loop()
    deadline_at = loop.time() + deadline_s
    response = await litellm.acompletion(**kwargs)

    # Per-chunk inactivity guard. The ``stream_timeout`` kwarg above is
    # LiteLLM's own per-chunk bound, but its behavior varies by provider
    # adapter. Wrapping each ``__anext__`` with our own ``wait_for`` makes
    # the bound deterministic regardless of provider — a stalled connection
    # raises ``TimeoutError`` rather than hanging the worker. (Required for
    # the harness's zero-hang guarantee — see also ``run_session_step``'s
    # job-level cap.) The first ``__anext__`` waits for TTFT, which on
    # cold-cache long-prompt requests can legitimately exceed the
    # inter-chunk bound; the per-iteration timeout select uses the wider
    # TTFT ceiling until the first chunk arrives.
    chunks: list[Any] = []
    aiter = response.__aiter__()
    first = True
    # Capture a ``content_filter`` refusal directly off the wire. litellm
    # 1.83.4's ``stream_chunk_builder`` derives the assembled ``finish_reason``
    # via an UNCONDITIONAL last-wins loop over chunks, and its Anthropic
    # streaming adapter defaults ``finish_reason=""`` on every chunk (setting
    # the mapped value only on the ``message_delta`` event). So any
    # choice-bearing chunk arriving AFTER the refusal (e.g. an auto
    # ``include_usage`` trailer carrying ``finish_reason`` in {"", None,
    # "stop"}) silently clobbers ``content_filter`` back to ``"stop"`` —
    # defeating ``loop.REFUSAL_FINISH_REASON`` gating on the streaming path.
    # Make it sticky: once seen on the wire, override the assembled value.
    saw_content_filter = False
    # LiteLLM's stream assembler does not preserve provider-specific usage
    # extensions consistently. xAI reports a running billed-ticks total on
    # usage chunks, so retain the latest exact value directly off the wire.
    billed_cost: float | None = None
    try:
        while True:
            guard_timeout = _STREAM_TTFT_TIMEOUT_S if first else _STREAM_INTER_CHUNK_TIMEOUT_S
            remaining_deadline_s = deadline_at - loop.time()
            timeout = min(guard_timeout, remaining_deadline_s)
            try:
                chunk = await asyncio.wait_for(aiter.__anext__(), timeout=timeout)
            except TimeoutError as exc:
                if loop.time() >= deadline_at:
                    usage: dict[str, int] = {}
                    cost: float | None = None
                    if chunks:
                        partial_assembled: Any = litellm.stream_chunk_builder(chunks=chunks)
                        if partial_assembled is not None:
                            _, usage, cost, _ = _unpack_litellm_response(
                                partial_assembled, source="stream_chunk_builder"
                            )
                            if billed_cost is not None:
                                cost = billed_cost
                    raise ModelCallDeadlineError(
                        f"model call exceeded its {deadline_s:.0f}s total deadline while still streaming",
                        usage=usage,
                        cost_usd=cost,
                        chunks_seen=len(chunks),
                    ) from exc
                raise
            except StopAsyncIteration:
                break
            first = False
            chunks.append(chunk)
            chunk_billed_cost = _extract_billed_cost_usd(chunk)
            if chunk_billed_cost is not None:
                billed_cost = chunk_billed_cost
            # Some providers (OpenRouter, Grok, vLLM, OpenAI with stream_options.
            # include_usage) emit a terminal usage-summary chunk with empty choices.
            if not chunk.choices:
                continue
            # ``"content_filter"`` == ``loop.REFUSAL_FINISH_REASON`` (literal here
            # to avoid a loop<->completion import cycle). ``getattr`` guard: real
            # litellm chunks always carry ``finish_reason`` (defaults None/""), but
            # a partial/edge chunk that omits it must not crash the stream loop.
            if getattr(chunk.choices[0], "finish_reason", None) == "content_filter":
                saw_content_filter = True
            content = chunk.choices[0].delta.content
            if content:
                await _notify_delta(pool, session_id, content)
    finally:
        # Close the litellm CustomStreamWrapper on every exit path — normal
        # full drain (no-op), TTFT/inter-chunk TimeoutError, or any in-loop
        # exception — so the underlying httpx streaming response and its
        # socket are released immediately rather than leaking until GC.
        # ``aclose()`` nulls ``completion_stream``, so a post-drain call is a
        # safe no-op; suppress everything because cleanup must not mask the
        # original error propagating out of the loop. (Issue #855.)
        with contextlib.suppress(Exception):
            await response.aclose()

    assembled: Any = litellm.stream_chunk_builder(chunks=chunks)
    # ``litellm.stream_chunk_builder(chunks=[])`` returns ``None`` rather
    # than raising, so a provider that closes the connection without
    # emitting any chunks (Bedrock cold start, OpenRouter mid-handshake
    # disconnect, vLLM under load) would otherwise crash at the
    # ``assembled.get("usage")`` below with an opaque
    # ``AttributeError: 'NoneType' object has no attribute 'get'``.
    # Surface a typed error so operators see the actual failure mode in
    # ``step.litellm_failed`` logs and the retry path's reason is
    # meaningful.
    if assembled is None:
        raise RuntimeError(
            f"litellm returned an empty completion (zero chunks) for model "
            f"{model!r}; the provider closed the connection without emitting "
            f"any data"
        )
    message, usage, cost, finish_reason = _unpack_litellm_response(
        assembled, source="stream_chunk_builder"
    )
    if billed_cost is not None:
        cost = billed_cost
    # Restore a refusal that stream_chunk_builder's last-wins loop clobbered
    # (see ``saw_content_filter`` above). Zero behavior change on the happy
    # path: only fires when the wire actually carried a ``content_filter``.
    if saw_content_filter and finish_reason != "content_filter":
        finish_reason = "content_filter"
    return message, usage, cost, finish_reason


async def _notify_delta(
    pool: asyncpg.Pool[Any],
    session_id: str,
    content: str,
) -> None:
    """Send a transient content delta via pg_notify.

    Uses the same ``events_{session_id}`` channel as persisted events.
    The JSON payload is distinguishable from event-id payloads because
    it starts with ``{``.
    """
    payload = json.dumps({"delta": content})
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT pg_notify($1, $2)",
            f"events_{session_id}",
            payload,
        )
