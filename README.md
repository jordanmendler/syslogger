# rsyslog importance review web app

The web app consumes messages from Kafka and shows them in a browser. You can
mark each event with **✓ Important** or **✕ Not important**. A local Qwen model
scores events from 1 (routine) to 5 (urgent) and gives a short reason. Scores
are cached in SQLite; the Important view shows events scored 4 or 5. Manual
ratings remain available for review.

By default it connects to topic `rsyslog` on
`172.16.108.245:9092,172.16.108.246:9092,172.16.108.247:9092`.

## Run

```sh
python -m pip install -r requirements.txt
python app.py
```

Open <http://127.0.0.1:5000>. The app stores received events, Qwen scores, and
ratings in `rsyslog-events.sqlite3` in the current directory. It listens on localhost by default. To listen on another
interface, set `WEB_HOST=0.0.0.0` before starting it.

## Configuration

Use environment variables to change Kafka or storage settings:

```sh
KAFKA_BROKERS=broker1:9092,broker2:9092 \
KAFKA_TOPIC=rsyslog \
KAFKA_GROUP_ID=rsyslog-review-web \
RSYSLOG_DB=/path/to/events.sqlite3 \
QWEN_URL=http://127.0.0.1:11434/api/generate \
QWEN_MODEL=qwen3.8 \
python app.py
```

Install and start Ollama locally, then pull the configured model with
`ollama pull qwen3.8`. Ollama lists this model as a 27B model with an 18 GB
download, so confirm your machine has enough memory and storage first. Qwen analysis starts automatically when events
are viewed; event data is not sent to a remote service.

The consumer imports from the earliest retained offset the first time it sees
each Kafka partition, regardless of prior Kafka group offsets. It stores every
message locally and resumes after the last stored offset on restart. Kafka's
retention policy still determines how far back history is available. The page
shows the newest 200 events first; use **Load older events** to browse the rest.
The page also displays Kafka connection status.
Existing 1–5 ratings are migrated to binary labels: 4–5 become Important and
1–3 become Not important.
The original `rsyslog_analyzer.py` CLI remains available for offline log
analysis; the browser workflow is the main way to curate importance ratings.
