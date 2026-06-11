# Connector storage migration notes

## Per-instance cloister (#238)

Pre-#238 deployments stored connector state at `~/.aios/connectors/<name>/`
(spool databases, signal-cli config, etc.). That path is now per-instance:
`~/.aios/instances/<instance_id>/connectors/<name>/`. Concurrent dev worktrees
no longer clobber each other's spools.

For an existing `instance_id="default"` deployment (the production
single-instance case), one-time move:

```
mkdir -p ~/.aios/instances/default
mv ~/.aios/connectors ~/.aios/instances/default/connectors
```

(Historical note: the `AIOS_CONNECTORS_DIR` env var from this era no longer
exists — connectors now run as separate processes/containers authenticated by
per-connection runtime tokens, and each manages its own state directory. See
the per-connector READMEs and `compose.yml`.)
