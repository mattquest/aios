# Operating manual for this memory (read me when unsure)

This is $user_name's **second brain**: a PARA-organized, agent-curated knowledge store. You (the assistant) are its sole curator. It is the durable source of truth about $user_name's life and outlives any single conversation. Keep it accurate, atomic, and well-indexed.

## The method (PARA, by actionability)

Decide where a note goes by asking "how actionable is this *right now*?":

| Folder | Holds | Test |
|---|---|---|
| `projects/` | Active efforts with a concrete goal and an end | "Is there a finish line I'm working toward?" |
| `areas/` | Ongoing responsibilities & standing context | "Is this a part of life I maintain indefinitely?" |
| `resources/` | Reference knowledge by topic | "Would I want this later, regardless of any project?" |
| `archive/` | Completed or dormant items | "Is this done or paused?" → move it here, never delete |
| `people/` | One file per person (`people/<name>.md`) | Anyone $user_name deals with; `$user_slug.md` is $user_name |

When a project finishes, move its file to `archive/` and update its `status`. When an archived thing reactivates, move it back to `projects/`.

## Note format

Every note is a markdown file with YAML frontmatter and atomic bullets:

```markdown
---
id: p-acme-launch          # stable, kebab-case, unique. NEVER reused. Survives file moves.
title: Acme launch
type: project              # project | area | resource | person
status: active             # active | someday | done | archived
keywords: [acme, launch, marketing]
updated: $today
links: [a-side-projects, people-jane]
---

- One fact per bullet. Atomic and independently editable.
- Decisions, deadlines, state — distilled, not transcribed.
- ## Sub-headings to group bullets within a longer note.
```

Rules:

- **Atomic**: one fact per bullet. Easy to edit, supersede, or delete a single fact.
- **Stable ids**: kebab-case, prefixed by type when helpful (`p-` project, `a-` area, `r-` resource, `people-` person). Once assigned, never change an id — links depend on it.
- **Link by id** in `links:` and inline as `[[id]]` when referencing another note.
- **No secrets**: never write passwords, tokens, API keys, or full account numbers here.

## index.md is the system

`index.md` is the master index / map-of-content — your primary search surface. Every note appears there with its id, title, path, keywords, and status. **Update it on every create, move, or archive.** When recalling, grep `index.md` first; only open the full notes you actually need.

## Capture → file → consolidate

1. **Capture** (in the moment): when $user_name tells you something durable, write it immediately. If you can't file it cleanly yet, append a dated bullet to `00-inbox.md`.
2. **File**: place it in the right PARA folder with proper frontmatter, and add/update its `index.md` line.
3. **Consolidate** (scheduled nightly reflection): read the day's events via `search_events`, promote anything important from `00-inbox.md` into well-filed notes, merge duplicates, fix stale statuses, archive finished projects, and prune noise. This is what keeps the brain trustworthy over years.

## Recall

- Fast path: grep `index.md`, then grep the tree for keywords.
- Deep path: `search_events` (SQL over the full conversation log) for episodic detail that was never filed or has scrolled out of context.
