# Translate Visualizer

Small local web app for inspecting multilingual conversation JSONL files.

It serves a single HTML page via Python and lets you:

- choose a dataset (`*.jsonl`) from the current folder
- keep `eng` fixed as the left column
- choose a right-side language column (for example `swe`)
- view conversations aligned by row and by message index
- view messages formatted as `role: content`
- visually grey text inside `<think>...</think>` while preserving the original text
- write per-row notes that persist between sessions

## Requirements

- `uv`
- Python available through `uv run`

## Run

From the project root:

```bash
uv run python main.py
```

Then open:

- `http://127.0.0.1:8000`

If your environment has restricted cache permissions, run with:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python main.py
```

## Data Files

- The app auto-detects all `*.jsonl` files in the current directory.
- Default dataset is `qwen.jsonl` (if present).
- You can switch datasets in the UI dropdown.

### Expected Row Shape

Each JSONL line should be a JSON object. Conversation columns are expected to look like:

```json
{
  "id": "example_1",
  "eng": [
    {"role": "user", "content": "Hello"},
    {"role": "assistant", "content": "Hi"}
  ],
  "swe": [
    {"role": "user", "content": "Hej"},
    {"role": "assistant", "content": "Hej!"}
  ]
}
```

If values are not arrays of `{role, content}`, they are still rendered as text.

## Notes Persistence

- Notes are saved to `notes.json` in the project root.
- Notes are scoped by dataset, fixed left column (`eng`), selected right column, and row key.
- `notes.json` is ignored by git.

## API Endpoints

- `GET /` serves the viewer page
- `GET /api/messages?dataset=<name>.jsonl` returns rows, columns, available datasets, row keys, and notes
- `POST /api/notes` saves one note entry with body:

```json
{
  "key": "dataset|eng|right_column|row_key",
  "note": "your note text"
}
```
