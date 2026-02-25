import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HOST = "127.0.0.1"
PORT = 8000
DEFAULT_DATASET_NAME = "qwen.jsonl"
NOTES_FILE = Path("notes.json")


def read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc.msg}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"Line {line_number} is not a JSON object")
            rows.append(parsed)
    return rows


def collect_columns(rows: list[dict]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    return ordered


def available_datasets() -> list[str]:
    datasets = sorted(path.name for path in Path(".").glob("*.jsonl") if path.is_file())
    return datasets


def resolve_dataset(dataset_name: str | None) -> Path:
    datasets = available_datasets()
    if not datasets:
        raise ValueError("No JSONL datasets were found in this directory.")

    selected = (dataset_name or DEFAULT_DATASET_NAME).strip()
    if not selected:
        selected = DEFAULT_DATASET_NAME
    if "/" in selected or "\\" in selected or selected in {".", ".."}:
        raise ValueError("Invalid dataset name.")
    if not selected.endswith(".jsonl"):
        raise ValueError("Dataset must be a .jsonl file.")
    if selected not in datasets:
        raise ValueError(
            f"Dataset '{selected}' was not found. Available datasets: {', '.join(datasets)}"
        )

    return Path(selected)


def row_key(row: dict, index: int) -> str:
    row_id = row.get("id")
    if isinstance(row_id, str) and row_id:
        return f"{index}:{row_id}"
    return str(index)


def read_notes(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid notes JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Notes file must contain a JSON object")

    notes: dict[str, str] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, str):
            notes[key] = value
        else:
            notes[key] = json.dumps(value, ensure_ascii=False)
    return notes


def write_notes(path: Path, notes: dict[str, str]) -> None:
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(notes, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


class JsonlViewerHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_html(self, html: str) -> None:
        encoded = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path == "/":
            self._send_html(INDEX_HTML)
            return

        if path == "/api/messages":
            query = parse_qs(parsed_url.query)
            dataset_name = query.get("dataset", [None])[0]

            try:
                dataset_path = resolve_dataset(dataset_name)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            try:
                rows = read_jsonl(dataset_path)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            try:
                notes = read_notes(NOTES_FILE)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            columns = collect_columns(rows)
            row_keys = [row_key(row, index) for index, row in enumerate(rows)]
            self._send_json(
                HTTPStatus.OK,
                {
                    "dataset": dataset_path.name,
                    "datasets": available_datasets(),
                    "columns": columns,
                    "rows": rows,
                    "row_keys": row_keys,
                    "notes": notes,
                },
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path != "/api/notes":
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        content_length_raw = self.headers.get("Content-Length", "0")
        try:
            content_length = int(content_length_raw)
        except ValueError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Invalid Content-Length header"})
            return

        raw_payload = self.rfile.read(max(content_length, 0))
        try:
            payload = json.loads(raw_payload or b"{}")
        except json.JSONDecodeError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"Invalid JSON body: {exc.msg}"})
            return
        if not isinstance(payload, dict):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Request body must be a JSON object"})
            return

        key = payload.get("key")
        note = payload.get("note")
        if not isinstance(key, str):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Field 'key' must be a string"})
            return
        if not isinstance(note, str):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Field 'note' must be a string"})
            return

        try:
            notes = read_notes(NOTES_FILE)
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        if note:
            notes[key] = note
        elif key in notes:
            del notes[key]

        try:
            write_notes(NOTES_FILE, notes)
        except OSError as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"Failed to write notes: {exc}"})
            return

        self._send_json(HTTPStatus.OK, {"ok": True})


INDEX_HTML = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>JSONL Viewer</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --surface: #ffffff;
      --border: #d9e2ec;
      --text: #1f2933;
      --muted: #52606d;
      --header: #0f172a;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--text);
      background: linear-gradient(160deg, #f8fafc 0%, #eef2ff 100%);
    }

    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }

    h1 {
      margin: 0 0 8px;
      color: var(--header);
      font-size: 1.4rem;
    }

    p {
      margin: 0 0 16px;
      color: var(--muted);
    }

    .controls {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }

    label {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.95rem;
      color: var(--text);
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 12px;
    }

    select {
      font-size: 0.95rem;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 4px 8px;
      color: var(--text);
      background: #fff;
    }

    .table-wrap {
      border: 1px solid var(--border);
      background: var(--surface);
      border-radius: 12px;
      overflow: auto;
      max-height: calc(100vh - 180px);
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.05);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th, td {
      border-bottom: 1px solid var(--border);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
      white-space: pre-wrap;
      word-wrap: break-word;
      line-height: 1.35;
      font-size: 0.92rem;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #f1f5f9;
      color: var(--header);
      font-weight: 600;
    }

    th:nth-child(1), td:nth-child(1) { width: 190px; }
    th:nth-child(2), td:nth-child(2) { width: calc((100% - 550px) / 2); }
    th:nth-child(3), td:nth-child(3) { width: calc((100% - 550px) / 2); }
    th:nth-child(4), td:nth-child(4) { width: 360px; }

    .id-cell {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.8rem;
      color: #334155;
      word-break: break-word;
    }

    .conversation {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .message-row {
      border: 1px solid #e2e8f0;
      background: #f8fafc;
      border-radius: 6px;
      padding: 8px 10px;
      line-height: 1.4;
    }

    .think-text {
      color: #64748b;
    }

    .message-row.empty {
      background: #f8fafc;
      border-style: dashed;
      color: #94a3b8;
    }

    .fixed-col {
      font-weight: 600;
      color: #0f172a;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      background: #f8fafc;
    }

    .notes-cell {
      white-space: normal;
    }

    .note-input {
      width: 100%;
      min-height: 90px;
      resize: vertical;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      padding: 8px;
      font-size: 0.9rem;
      line-height: 1.35;
      color: #1f2937;
      background: #ffffff;
    }

    .note-input.saving {
      border-color: #f59e0b;
      background: #fffbeb;
    }

    .note-input.saved {
      border-color: #14b8a6;
      background: #f0fdfa;
    }

    .note-input.save-error {
      border-color: #dc2626;
      background: #fef2f2;
    }

    #status {
      margin: 12px 0;
      color: var(--muted);
      font-size: 0.9rem;
    }

    .error {
      color: #b91c1c;
      font-weight: 600;
    }
  </style>
</head>
<body>
  <main class=\"container\">
    <h1>JSONL Message Viewer</h1>
    <p>Showing <code>id</code>, fixed left column <code>eng</code>, a selectable right column, and per-row notes.</p>

    <div class=\"controls\">
      <label>
        Dataset
        <select id=\"datasetSelect\"></select>
      </label>
      <label>
        Left column
        <span class=\"fixed-col\">eng</span>
      </label>
      <label>
        Right column
        <select id=\"rightColumn\"></select>
      </label>
    </div>

    <div id=\"status\">Loading...</div>

    <div class=\"table-wrap\">
      <table>
        <thead>
          <tr>
            <th id=\"idHeader\">id</th>
            <th id=\"leftHeader\">eng</th>
            <th id=\"rightHeader\">swe</th>
            <th id=\"notesHeader\">notes (eng->swe)</th>
          </tr>
        </thead>
        <tbody id=\"rows\"></tbody>
      </table>
    </div>
  </main>

  <script>
    const LEFT_COLUMN = 'eng';

    const datasetSelect = document.getElementById('datasetSelect');
    const rightSelect = document.getElementById('rightColumn');
    const statusEl = document.getElementById('status');
    const rowsEl = document.getElementById('rows');
    const leftHeader = document.getElementById('leftHeader');
    const rightHeader = document.getElementById('rightHeader');
    const notesHeader = document.getElementById('notesHeader');

    let currentDataset = '';
    let dataRows = [];
    let rowKeys = [];
    let notesByKey = {};

    const noteSaveTimers = new Map();
    const noteSaveSequences = new Map();

    function textFor(value) {
      if (value === null || value === undefined) return '';
      if (typeof value === 'string') return value;
      return JSON.stringify(value);
    }

    function asConversation(value) {
      if (Array.isArray(value)) {
        return value.map((entry) => {
          if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
            const role = entry.role ?? 'unknown';
            const content = textFor(entry.content);
            return `${role}: ${content}`;
          }
          return textFor(entry);
        });
      }
      return [textFor(value)];
    }

    function makeMessageRow(text) {
      const div = document.createElement('div');
      div.className = 'message-row';
      if (!text) {
        div.classList.add('empty');
        div.textContent = '';
      } else {
        renderThinkStyledText(div, text);
      }
      return div;
    }

    function renderThinkStyledText(container, text) {
      const thinkTagPattern = /<think>([\\s\\S]*?)<\\/think>/gi;
      let lastIndex = 0;
      let match = null;

      while ((match = thinkTagPattern.exec(text)) !== null) {
        if (match.index > lastIndex) {
          container.appendChild(document.createTextNode(text.slice(lastIndex, match.index)));
        }
        const thinkSpan = document.createElement('span');
        thinkSpan.className = 'think-text';
        thinkSpan.textContent = match[0];
        container.appendChild(thinkSpan);
        lastIndex = thinkTagPattern.lastIndex;
      }

      if (lastIndex < text.length) {
        container.appendChild(document.createTextNode(text.slice(lastIndex)));
      }
    }

    function alignMessageHeights(leftItems, rightItems) {
      for (let i = 0; i < leftItems.length; i += 1) {
        leftItems[i].style.minHeight = '';
        rightItems[i].style.minHeight = '';
      }
      for (let i = 0; i < leftItems.length; i += 1) {
        const maxHeight = Math.max(leftItems[i].offsetHeight, rightItems[i].offsetHeight);
        leftItems[i].style.minHeight = `${maxHeight}px`;
        rightItems[i].style.minHeight = `${maxHeight}px`;
      }
    }

    function noteStorageKey(datasetName, rowKey, rightKey) {
      return `${datasetName}|${LEFT_COLUMN}|${rightKey}|${rowKey}`;
    }

    async function saveNote(noteKey, noteValue, inputEl, sequence) {
      noteSaveTimers.delete(noteKey);
      try {
        const response = await fetch('/api/notes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key: noteKey, note: noteValue }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || `Request failed with ${response.status}`);
        }
        if (noteSaveSequences.get(noteKey) !== sequence) {
          return;
        }

        inputEl.classList.remove('saving', 'save-error');
        inputEl.classList.add('saved');
        setTimeout(() => {
          if (noteSaveSequences.get(noteKey) === sequence) {
            inputEl.classList.remove('saved');
          }
        }, 600);
      } catch (error) {
        if (noteSaveSequences.get(noteKey) !== sequence) {
          return;
        }
        inputEl.classList.remove('saving', 'saved');
        inputEl.classList.add('save-error');
        statusEl.innerHTML = `<span class=\"error\">Failed to save note: ${error.message}</span>`;
      }
    }

    function queueNoteSave(noteKey, noteValue, inputEl) {
      const previousTimer = noteSaveTimers.get(noteKey);
      if (previousTimer) {
        clearTimeout(previousTimer);
      }
      inputEl.classList.remove('saved', 'save-error');
      inputEl.classList.add('saving');

      const sequence = (noteSaveSequences.get(noteKey) ?? 0) + 1;
      noteSaveSequences.set(noteKey, sequence);
      const timerId = setTimeout(() => {
        void saveNote(noteKey, noteValue, inputEl, sequence);
      }, 350);
      noteSaveTimers.set(noteKey, timerId);
    }

    function flushNoteSave(noteKey, noteValue, inputEl) {
      const previousTimer = noteSaveTimers.get(noteKey);
      if (previousTimer) {
        clearTimeout(previousTimer);
        noteSaveTimers.delete(noteKey);
      }
      inputEl.classList.remove('saved', 'save-error');
      inputEl.classList.add('saving');

      const sequence = (noteSaveSequences.get(noteKey) ?? 0) + 1;
      noteSaveSequences.set(noteKey, sequence);
      void saveNote(noteKey, noteValue, inputEl, sequence);
    }

    function renderRows() {
      const leftKey = LEFT_COLUMN;
      const rightKey = rightSelect.value;
      leftHeader.textContent = leftKey;
      rightHeader.textContent = rightKey;
      notesHeader.textContent = `notes (${currentDataset} | ${leftKey}->${rightKey})`;

      const fragment = document.createDocumentFragment();
      const pendingAlignments = [];
      dataRows.forEach((row, index) => {
        const tr = document.createElement('tr');
        const rowKey = rowKeys[index] ?? `${index}`;

        const idCell = document.createElement('td');
        idCell.className = 'id-cell';
        idCell.textContent = textFor(row.id ?? index + 1);
        tr.appendChild(idCell);

        const leftCell = document.createElement('td');
        const rightCell = document.createElement('td');
        const notesCell = document.createElement('td');
        notesCell.className = 'notes-cell';

        const leftConversation = document.createElement('div');
        leftConversation.className = 'conversation';
        const rightConversation = document.createElement('div');
        rightConversation.className = 'conversation';

        const leftMessages = asConversation(row[leftKey]);
        const rightMessages = asConversation(row[rightKey]);
        const messageCount = Math.max(leftMessages.length, rightMessages.length);
        const leftMessageRows = [];
        const rightMessageRows = [];

        for (let i = 0; i < messageCount; i += 1) {
          const leftItem = makeMessageRow(leftMessages[i] ?? '');
          const rightItem = makeMessageRow(rightMessages[i] ?? '');
          leftConversation.appendChild(leftItem);
          rightConversation.appendChild(rightItem);
          leftMessageRows.push(leftItem);
          rightMessageRows.push(rightItem);
        }

        const noteKey = noteStorageKey(currentDataset, rowKey, rightKey);
        const noteInput = document.createElement('textarea');
        noteInput.className = 'note-input';
        noteInput.placeholder = 'Add note for this row and language pair...';
        noteInput.value = notesByKey[noteKey] ?? '';
        noteInput.addEventListener('input', () => {
          notesByKey[noteKey] = noteInput.value;
          queueNoteSave(noteKey, noteInput.value, noteInput);
        });
        noteInput.addEventListener('blur', () => {
          flushNoteSave(noteKey, noteInput.value, noteInput);
        });

        notesCell.appendChild(noteInput);
        leftCell.appendChild(leftConversation);
        rightCell.appendChild(rightConversation);

        tr.appendChild(leftCell);
        tr.appendChild(rightCell);
        tr.appendChild(notesCell);

        fragment.appendChild(tr);
        pendingAlignments.push([leftMessageRows, rightMessageRows]);
      });

      rowsEl.replaceChildren(fragment);
      pendingAlignments.forEach(([leftItems, rightItems]) => {
        alignMessageHeights(leftItems, rightItems);
      });
      statusEl.textContent = `Loaded ${dataRows.length.toLocaleString()} rows from ${currentDataset}`;
    }

    function setDatasetOptions(datasets, selectedDataset) {
      const available = Array.isArray(datasets) ? datasets : [];
      datasetSelect.innerHTML = '';
      available.forEach((name) => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        datasetSelect.appendChild(option);
      });
      if (selectedDataset && available.includes(selectedDataset)) {
        datasetSelect.value = selectedDataset;
      } else if (available.length > 0) {
        datasetSelect.value = available[0];
      }
    }

    function setOptions(columns) {
      if (!columns.includes(LEFT_COLUMN)) {
        throw new Error(`Column '${LEFT_COLUMN}' was not found in the data.`);
      }
      const rightColumns = columns.filter((key) => key !== 'id' && key !== LEFT_COLUMN);
      if (rightColumns.length === 0) {
        throw new Error('No right-side columns available besides eng and id.');
      }

      rightSelect.innerHTML = '';
      rightColumns.forEach((key) => {
        const option = document.createElement('option');
        option.value = key;
        option.textContent = key;
        rightSelect.appendChild(option);
      });

      rightSelect.value = rightColumns.includes('swe') ? 'swe' : rightColumns[0];
    }

    async function loadData(requestedDataset = null) {
      try {
        const params = new URLSearchParams();
        if (requestedDataset) {
          params.set('dataset', requestedDataset);
        }
        const endpoint = params.size > 0 ? `/api/messages?${params.toString()}` : '/api/messages';
        const response = await fetch(endpoint);
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.error || `Request failed with ${response.status}`);
        }

        const datasets = payload.datasets || [];
        currentDataset = payload.dataset || requestedDataset || datasetSelect.value || '';
        setDatasetOptions(datasets, currentDataset);
        dataRows = payload.rows || [];
        rowKeys = payload.row_keys || dataRows.map((_, index) => `${index}`);
        notesByKey = payload.notes && typeof payload.notes === 'object' ? payload.notes : {};
        const columns = payload.columns || [];
        setOptions(columns);
        renderRows();
      } catch (error) {
        statusEl.innerHTML = `<span class=\"error\">Failed to load data: ${error.message}</span>`;
      }
    }

    datasetSelect.addEventListener('change', () => {
      void loadData(datasetSelect.value);
    });
    rightSelect.addEventListener('change', renderRows);

    void loadData();
  </script>
</body>
</html>
"""


def main() -> None:
    server = HTTPServer((HOST, PORT), JsonlViewerHandler)
    print(f"Serving on http://{HOST}:{PORT}")
    datasets = available_datasets()
    print(f"Available datasets: {', '.join(datasets) if datasets else '(none)'}")
    server.serve_forever()


if __name__ == "__main__":
    main()
