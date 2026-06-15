# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build all packages
go build ./...

# Run all tests
go test ./...

# Run tests for a single package
go test ./engine/export/

# Run a single test by name
go test -run TestEngine_NormalFlow ./engine/export/

# Run tests with verbose output
go test -v ./...

# Run the demo (export + import + recovery scenarios)
go run ./cmd/demo/
```

No linting or code generation tools are configured.

## Architecture

This is a **Go library (module: `state-store`, Go 1.26)** for orchestrating asynchronous, long-running tasks with **checkpoint/resume** support. It has zero external dependencies.

### Layer Model (top to bottom)

**`engine/` — Orchestration Framework**
- `engine.Engine` interface defines lifecycle hooks: `TaskType()`, `Execute()`, `Compensate()`, `Progress()`.
- `engine.Run()` is the core loop: **Load state → Compensate if recovering → Execute→Save loop** until terminal phase. The framework owns the checkpoint protocol; engines own business logic.
- Every `Execute()` call returns a new LSN (Log Sequence Number) that the framework persists atomically via `StateRepository`.

**`statestore/` — Domain Types & Contracts**
- `BaseTaskState`: JSON-serializable struct with `TaskID`, `TaskType`, `Phase`, `Message`, `Progress`, `CheckpointLSN`, and a `Payload` (`json.RawMessage`) for engine-specific state.
- `TaskPhase` enum: `pending → running → merging → verifying → completed/failed/canceled`. Export uses `merging`; import goes directly from `running` to `completed`.
- `StateRepository` interface: `Load` (nil,nil for missing), `Save` (atomic full replacement), `Delete` (idempotent).

**`engine/export/` — Export Engine**
- Paginates from a `phys.DataSource`, writing rows to numbered chunk files. When `io.EOF` is returned, transitions to `merging` phase and concatenates chunks into a single output file.
- Chunk files (`.chunk_N.tmp`) are intermediate artifacts; call `Cleanup()` after `Run()` succeeds to remove them.
- `Compensate` truncates the final file to the checkpoint LSN on recovery.

**`engine/import/` — Import Engine**
- Reads a JSONL file line-by-line using `bufio.Scanner` (not `json.Decoder`, to avoid internal buffering issues), tracking precise byte offsets.
- Writes batches to a `phys.DataTarget`. On recovery, seeks to the checkpointed offset and resumes — the `DataTarget` must handle idempotency (`WriteBatch` may be replayed).
- `Compensate` verifies the source file hasn't been truncated below the LSN.

**`phys/` — Physical Layer Abstractions**
- `DataSource.FetchPage(ctx, page, pageSize)` returns `[]Row` or `io.EOF`. Callers implement real DB queries / API calls.
- `DataTarget.WriteBatch(ctx, rows)` must be idempotent (UPSERT, unique-key dedup, etc.) since crash recovery replays batches.
- `Row` is `map[string]interface{}`.

**`filestore/` — File-based Persistence**
- Implements `StateRepository` using local filesystem: **tmp write → Sync → Rename** for atomicity. Has `Cleanup()` to remove orphan `.state.tmp` files from crashed processes.

### Key Design Decisions

- **No partial merges on Save** — each save is a full state replacement. Engines marshal their entire Payload each step.
- **LSN semantics are engine-specific** — export LSN is cumulative bytes; import LSN is byte offset in the source file. The framework treats it as opaque.
- **Compensate is called before Execute on recovery** — allows engines to align physical systems (files, DBs) to the checkpoint before resuming work.
- **Interface satisfaction verified at compile time** (e.g., `var _ engine.Engine = (*Engine)(nil)`).

### Testing Patterns

- All tests use `t.TempDir()` for isolation — never hardcoded paths.
- Stub/mock implementations of `phys.DataSource`, `phys.DataTarget`, and `statestore.StateRepository` are defined inline in test files.
- Recovery tests simulate crash-restart by creating a **new engine instance** with a **new target/datasource** after checkpointing state with the old engine.
- Integration tests in `integration/` cover end-to-end export→import and multi-step crash-recovery scenarios.
