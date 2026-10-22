#!/usr/bin/env python3
"""Web UI for reviewing Kafka rsyslog events and training importance ratings."""

from __future__ import annotations

import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import torch
from flask import Flask, jsonify, render_template_string, request
from torch import nn

from rsyslog_analyzer import vectorize


BROKERS = os.environ.get(
    "KAFKA_BROKERS",
    "172.16.108.245:9092,172.16.108.246:9092,172.16.108.247:9092",
)
TOPIC = os.environ.get("KAFKA_TOPIC", "rsyslog")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "rsyslog-review-web")
DB_PATH = Path(os.environ.get("RSYSLOG_DB", "rsyslog-events.sqlite3"))
MODEL_PATH = Path(os.environ.get("IMPORTANCE_MODEL", "importance-model.pt"))
DIMENSIONS = 2048
LABELS = {0: "Not important", 1: "Important"}
MODEL_LOCK = threading.Lock()
TRAIN_STATE: dict[str, Any] = {"status": "not trained", "message": "Rate events, then train the model."}
KAFKA_STATE: dict[str, str] = {"status": "starting", "message": "Starting Kafka consumer…"}


class ImportanceNet(nn.Module):
    def __init__(self, dimensions: int = DIMENSIONS):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(dimensions, 256), nn.ReLU(), nn.Dropout(0.15),
            nn.Linear(256, 64), nn.ReLU(), nn.Linear(64, 2),
        )

    def forward(self, inputs: torch.Tensor) -> torch.Tensor:
        return self.layers(inputs)


@contextmanager
def connect_db() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(DB_PATH, timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect_db() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                offset_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                importance INTEGER CHECK (importance BETWEEN 1 AND 5),
                is_important INTEGER CHECK (is_important IN (0, 1)),
                UNIQUE(topic, partition_id, offset_id)
            )
        """)
        columns = {row[1] for row in connection.execute("PRAGMA table_info(events)")}
        if "is_important" not in columns:
            connection.execute("ALTER TABLE events ADD COLUMN is_important INTEGER CHECK (is_important IN (0, 1))")
        if "importance" in columns:
            connection.execute(
                "UPDATE events SET is_important=CASE WHEN importance >= 4 THEN 1 ELSE 0 END "
                "WHERE is_important IS NULL AND importance IS NOT NULL"
            )
        connection.execute("CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")


def consume_kafka() -> None:
    """Persist incoming Kafka records; retry broker connections after errors."""
    global KAFKA_STATE
    while True:
        consumer = None
        try:
            KAFKA_STATE = {"status": "connecting", "message": f"Connecting to Kafka topic {TOPIC}…"}
            from kafka import KafkaConsumer

            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[item.strip() for item in BROKERS.split(",")],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
            )
            # Assign partitions before seeking. The first startup performs a full
            # retained-history backfill even if older app versions already stored
            # some recent messages. Later startups resume from the local database.
            while not consumer.assignment():
                consumer.poll(timeout_ms=1000)
            assignments = consumer.assignment()
            targets: dict[Any, int] = {}
            with connect_db() as connection:
                backfill_done = connection.execute(
                    "SELECT value FROM app_state WHERE key='history_backfill_complete'"
                ).fetchone()
                backfill_done = backfill_done is not None and backfill_done[0] == "1"
            end_offsets = consumer.end_offsets(assignments) if not backfill_done else {}
            for partition in assignments:
                with connect_db() as connection:
                    previous = connection.execute(
                        "SELECT MAX(offset_id) FROM events WHERE topic=? AND partition_id=?",
                        (partition.topic, partition.partition),
                    ).fetchone()[0]
                if not backfill_done or previous is None:
                    consumer.seek_to_beginning(partition)
                else:
                    consumer.seek(partition, previous + 1)
            if not backfill_done:
                targets.update(end_offsets)
            if targets:
                KAFKA_STATE = {"status": "backfill", "message": "Importing retained Kafka history…"}
            else:
                KAFKA_STATE = {"status": "connected", "message": f"Connected to {TOPIC}; waiting for messages."}
            print(f"Connected to Kafka topic {TOPIC} at {BROKERS}", file=sys.stderr)
            while True:
                records = consumer.poll(timeout_ms=1000, max_records=500)
                pending = []
                for batch in records.values():
                    for record in batch:
                        message = record.value.strip()
                        if message:
                            pending.append((record.topic, record.partition, record.offset, message[:20000]))
                with connect_db() as connection:
                    connection.executemany(
                        "INSERT OR IGNORE INTO events(topic, partition_id, offset_id, message) VALUES (?, ?, ?, ?)",
                        pending,
                    )
                if targets and all(consumer.position(partition) >= offset for partition, offset in targets.items()):
                    with connect_db() as connection:
                        connection.execute(
                            "INSERT OR REPLACE INTO app_state(key, value) VALUES ('history_backfill_complete', '1')"
                        )
                    targets.clear()
                status = "backfill" if targets else "connected"
                message = f"Importing retained history from {TOPIC}…" if targets else f"Receiving messages from {TOPIC}."
                KAFKA_STATE = {"status": status, "message": message}
        except Exception as error:  # broker errors should not take down the web UI
            KAFKA_STATE = {"status": "error", "message": f"Kafka connection error: {error}"}
            print(f"Kafka consumer error: {error}; retrying in 5 seconds", file=sys.stderr)
            time.sleep(5)
        finally:
            if consumer is not None:
                consumer.close()


def train_model() -> None:
    global TRAIN_STATE
    with MODEL_LOCK:
        TRAIN_STATE = {"status": "training", "message": "Training from rated events…"}
    try:
        with connect_db() as connection:
            rows = connection.execute(
                "SELECT message, is_important FROM events WHERE is_important IS NOT NULL ORDER BY id"
            ).fetchall()
        if len(rows) < 2 or len({row["is_important"] for row in rows}) < 2:
            raise ValueError("Rate at least one event Important and one Not important before training.")
        messages = [row["message"] for row in rows]
        features = vectorize(messages, DIMENSIONS)
        targets = torch.tensor([row["is_important"] for row in rows], dtype=torch.long)
        model = ImportanceNet()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
        loss_fn = nn.CrossEntropyLoss()
        model.train()
        for _ in range(60):
            optimizer.zero_grad()
            loss = loss_fn(model(features), targets)
            loss.backward()
            optimizer.step()
        artifact = {"dimensions": DIMENSIONS, "state_dict": model.state_dict(), "rated_events": len(rows)}
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = MODEL_PATH.with_suffix(MODEL_PATH.suffix + ".tmp")
        torch.save(artifact, temporary_path)
        temporary_path.replace(MODEL_PATH)
        with MODEL_LOCK:
            TRAIN_STATE = {
                "status": "ready",
                "message": f"Trained on {len(rows)} rated events.",
                "rated_events": len(rows),
            }
    except Exception as error:
        with MODEL_LOCK:
            TRAIN_STATE = {"status": "error", "message": str(error)}


def load_model() -> ImportanceNet | None:
    if not MODEL_PATH.is_file():
        return None
    try:
        try:
            artifact = torch.load(MODEL_PATH, map_location="cpu", weights_only=False)
        except TypeError:
            artifact = torch.load(MODEL_PATH, map_location="cpu")
        model = ImportanceNet(int(artifact["dimensions"]))
        model.load_state_dict(artifact["state_dict"])
        model.eval()
        return model
    except (OSError, KeyError, RuntimeError, ValueError):
        return None


PAGE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>rsyslog importance review</title>
  <style>
    :root{font:15px/1.45 system-ui,sans-serif;color:#17212b;background:#f3f6f8}
    body{max-width:1200px;margin:28px auto;padding:0 18px}h1{margin-bottom:4px}
    .sub,.muted{color:#62717d}.bar,.event{background:white;border:1px solid #dce3e8;border-radius:9px;padding:15px 18px;margin:12px 0}
    .event.predicted-important{background:#eef9f1;border-left:6px solid #258454}.event.predicted-not-important{background:#fff1f1;border-left:6px solid #bd4b4b}
    .bar{display:flex;gap:16px;align-items:center;flex-wrap:wrap}.bar button{background:#176b87;color:white;border:0;border-radius:6px;padding:10px 16px;font-weight:650;cursor:pointer}
    .bar button:disabled{opacity:.55;cursor:wait}.event{display:grid;grid-template-columns:28px minmax(0,1fr) 190px;gap:14px;align-items:start}
    .event-check{margin-top:4px;width:18px;height:18px;cursor:pointer}.bulk{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.bulk button{background:#176b87;color:white;border:0;border-radius:6px;padding:8px 12px;font-weight:650;cursor:pointer}.bulk button.no{background:#8d2828}
    pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:8px 0 0;font:13px/1.5 ui-monospace,monospace}
    .ratings{display:flex;gap:6px;margin-top:8px}.rating{flex:1;min-width:0;padding:7px 5px;border:1px solid #258454;border-radius:6px;background:#e5f5eb;color:#17663e;cursor:pointer;text-align:center;font-size:12px;white-space:nowrap}
    .rating.no{border-color:#bd4b4b;background:#fceaea;color:#8d2828}.rating.selected{font-weight:700;box-shadow:0 0 0 2px currentColor}
    .tag{font-size:12px;color:#465762}
    @media(max-width:650px){.event{grid-template-columns:24px 1fr}.event>div:last-child{grid-column:2}}
  </style>
</head>
<body>
  <h1>rsyslog importance review</h1>
  <div class="sub">Kafka topic <b>{{ topic }}</b> · {{ brokers }}</div>
  <section class="bar">
    <button id="train" onclick="trainModel()">Train PyTorch model from my ratings</button>
    <span id="training">{{ train_message }}</span><span class="muted" id="kafka"></span><span class="muted" id="counts"></span>
  </section>
  <section class="bar"><button id="important-view" disabled onclick="switchView('important')">Important</button><button id="unrated-view" onclick="switchView('unrated')">Unrated</button><button id="not-important-view" onclick="switchView('not_important')">Not important</button></section>
  <section class="bar bulk"><label><input id="select-all" type="checkbox" onchange="toggleAll(this.checked)"> Select all loaded events</label><button onclick="bulkRate(1)">Mark selected important</button><button class="no" onclick="bulkRate(0)">Mark selected not important</button><span class="muted" id="selection-count">0 selected</span></section>
    <div id="events"></div><div id="pagecontrols"></div>
<script>
const labels={0:'Not important',1:'Important'};
let oldestId=null, hasMore=false, browsingOlder=false;
let activeView='important';
const selected=new Set();
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function eventMarkup(e){
  const actual=e.is_important===null?'Unrated':labels[e.is_important];
  const predicted=e.predicted===null?'Model not trained':`${labels[e.predicted]} (${Math.round(e.confidence*100)}%)`;
  const predictionClass=e.predicted===1?'predicted-important':e.predicted===0?'predicted-not-important':'';
  return `<article class="event ${predictionClass}"><input class="event-check" type="checkbox" aria-label="Select event ${e.id}" ${selected.has(e.id)?'checked':''} onchange="toggleOne(${e.id},this.checked)"><div><div class="tag">${esc(e.received_at)} · partition ${e.partition_id}, offset ${e.offset_id} · model: ${esc(predicted)}</div><pre>${esc(e.message)}</pre></div><div><div class="tag">Your choice: <b>${esc(actual)}</b></div><div class="ratings"><button class="rating ${e.is_important===1?'selected':''}" aria-label="Mark important" aria-pressed="${e.is_important===1}" onclick="rate(${e.id},1)">✓ Important</button><button class="rating no ${e.is_important===0?'selected':''}" aria-label="Mark not important" aria-pressed="${e.is_important===0}" onclick="rate(${e.id},0)">✕ Not important</button></div></div></article>`;
}
function updateSelectionUI(){const checks=[...document.querySelectorAll('.event-check')];const count=checks.filter(c=>c.checked).length;const all=document.getElementById('select-all');all.checked=checks.length>0&&count===checks.length;all.indeterminate=count>0&&count<checks.length;document.getElementById('selection-count').textContent=`${selected.size} selected`;}
function toggleOne(id,checked){if(checked)selected.add(id);else selected.delete(id);updateSelectionUI();}
function toggleAll(checked){document.querySelectorAll('.event-check').forEach(c=>{c.checked=checked;const id=Number(c.getAttribute('aria-label').match(/\d+$/)[0]);if(checked)selected.add(id);else selected.delete(id);});updateSelectionUI();}
function showControls(){document.getElementById('pagecontrols').innerHTML=`${hasMore?'<button onclick="loadOlder()">Load older events</button>':''} ${browsingOlder?'<button onclick="refresh()">Back to newest</button>':''}`;}
function switchView(view){if(view===activeView)return;activeView=view;selected.clear();oldestId=null;hasMore=false;browsingOlder=false;document.getElementById('important-view').disabled=view==='important';document.getElementById('unrated-view').disabled=view==='unrated';document.getElementById('not-important-view').disabled=view==='not_important';document.getElementById('events').innerHTML='';updateSelectionUI();refresh();}
async function refresh(){
  const view=activeView; const response=await fetch(`/api/events?view=${view}`); if(!response.ok||view!==activeView)return;
  const data=await response.json(); document.getElementById('counts').textContent=`${data.total} imported events · ${data.rated} rated`;
  document.getElementById('training').textContent=data.training.message;
  document.getElementById('kafka').textContent=data.kafka.message;
  const root=document.getElementById('events');
  if(!data.events.length){const emptyText={important:'No important events found.',unrated:'No unrated events found.',not_important:'No events marked Not important.'}[activeView];root.innerHTML=`<section class="event muted">${emptyText}</section>`;oldestId=null;hasMore=false;browsingOlder=false;showControls();updateSelectionUI();return;}
  root.innerHTML=data.events.map(eventMarkup).join('');
  updateSelectionUI();
  oldestId=data.events[data.events.length-1].id; hasMore=data.has_more; browsingOlder=false; showControls();
}
async function loadOlder(){
  if(!oldestId||!hasMore)return;
  const view=activeView; const response=await fetch(`/api/events?view=${view}&before_id=${oldestId}`); if(!response.ok||view!==activeView)return;
  const data=await response.json();
  document.getElementById('events').insertAdjacentHTML('beforeend',data.events.map(eventMarkup).join(''));
  updateSelectionUI();
  if(data.events.length)oldestId=data.events[data.events.length-1].id;
  hasMore=data.has_more; browsingOlder=true; showControls();
}
async function rate(id,value){await fetch(`/api/events/${id}/importance`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({is_important:Boolean(value)})});refresh();}
async function bulkRate(value){const ids=[...selected];if(!ids.length)return;const response=await fetch('/api/events/importance',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids,is_important:Boolean(value)})});if(response.ok){selected.clear();refresh();}}
async function trainModel(){const b=document.getElementById('train');b.disabled=true;document.getElementById('training').textContent='Training…';await fetch('/api/train',{method:'POST'});b.disabled=false;refresh();}
refresh();setInterval(()=>{if(!browsingOlder)refresh();},4000);
</script>
</body></html>"""


def create_app(start_consumer: bool = True) -> Flask:
    initialize_db()
    app = Flask(__name__)
    if start_consumer and os.environ.get("DISABLE_KAFKA_CONSUMER") != "1":
        threading.Thread(target=consume_kafka, daemon=True, name="rsyslog-kafka-consumer").start()

    @app.get("/")
    def index():
        with MODEL_LOCK:
            message = TRAIN_STATE["message"]
        return render_template_string(PAGE, topic=TOPIC, brokers=BROKERS, train_message=message)

    @app.get("/api/events")
    def events():
        limit = min(max(request.args.get("limit", 200, type=int), 1), 500)
        before_id = request.args.get("before_id", type=int)
        view = request.args.get("view", "main")
        if view not in {"important", "unrated", "not_important"}:
            return jsonify(error="view must be important, unrated, or not_important"), 400
        model = load_model()
        filter_sql = {
            "important": "1=1" if model else "is_important=1",
            "unrated": "is_important IS NULL",
            "not_important": "is_important=0",
        }[view]
        with connect_db() as connection:
            total = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            rated = connection.execute("SELECT COUNT(*) FROM events WHERE is_important IS NOT NULL").fetchone()[0]
            cursor_id = before_id
            result: list[dict[str, Any]] = []
            chunk_size = min(max(limit, 100), 500)
            while len(result) <= limit:
                if cursor_id is None:
                    rows = connection.execute(
                        f"SELECT id, partition_id, offset_id, message, received_at, is_important FROM events WHERE {filter_sql} ORDER BY id DESC LIMIT ?",
                        (chunk_size,),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        f"SELECT id, partition_id, offset_id, message, received_at, is_important FROM events WHERE {filter_sql} AND id < ? ORDER BY id DESC LIMIT ?",
                        (cursor_id, chunk_size),
                    ).fetchall()
                if not rows:
                    break
                cursor_id = rows[-1]["id"]
                batch = [dict(row) for row in rows]
                if model and batch:
                    with torch.no_grad():
                        probabilities = torch.softmax(
                            model(vectorize([item["message"] for item in batch], DIMENSIONS)), dim=1
                        )
                    for item, probability in zip(batch, probabilities):
                        item["predicted"] = int(probability.argmax().item())
                        item["confidence"] = float(probability.max().item())
                else:
                    for item in batch:
                        item["predicted"] = None
                        item["confidence"] = None
                for item in batch:
                    if view != "important" or item["is_important"] == 1 or item["predicted"] == 1:
                        result.append(item)
                        if len(result) > limit:
                            break
                if len(result) > limit or len(rows) < chunk_size:
                    break
            has_more = len(result) > limit
            result = result[:limit]
        with MODEL_LOCK:
            training = dict(TRAIN_STATE)
        return jsonify(events=result, total=total, rated=rated, has_more=has_more, training=training, kafka=dict(KAFKA_STATE))

    @app.post("/api/events/<int:event_id>/importance")
    def set_importance(event_id: int):
        body = request.get_json(silent=True) or {}
        value = body.get("is_important")
        if not isinstance(value, bool):
            return jsonify(error="is_important must be true or false"), 400
        with connect_db() as connection:
            cursor = connection.execute("UPDATE events SET is_important=? WHERE id=?", (int(value), event_id))
        if cursor.rowcount == 0:
            return jsonify(error="event not found"), 404
        return jsonify(ok=True)

    @app.post("/api/events/importance")
    def set_many_importance():
        body = request.get_json(silent=True) or {}
        ids = body.get("ids")
        value = body.get("is_important")
        if not isinstance(ids, list) or not ids or any(type(item) is not int or item <= 0 for item in ids):
            return jsonify(error="ids must be a non-empty list of positive event IDs"), 400
        if not isinstance(value, bool):
            return jsonify(error="is_important must be true or false"), 400
        with connect_db() as connection:
            cursor = connection.executemany(
                "UPDATE events SET is_important=? WHERE id=?",
                [(int(value), event_id) for event_id in set(ids)],
            )
            updated = cursor.rowcount
        return jsonify(ok=True, updated=updated)

    @app.post("/api/train")
    def start_training():
        with MODEL_LOCK:
            if TRAIN_STATE["status"] == "training":
                return jsonify(error="training is already in progress"), 409
            TRAIN_STATE.update(status="training", message="Training from rated events…")
        threading.Thread(target=train_model, daemon=True, name="importance-model-training").start()
        return jsonify(ok=True), 202

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host=os.environ.get("WEB_HOST", "127.0.0.1"), port=int(os.environ.get("WEB_PORT", "5000")), debug=False)
