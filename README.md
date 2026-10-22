# rsyslog importance review web app

The web app consumes messages from Kafka and shows them in a browser. You can
mark each event with **✓ Important** or **✕ Not important**. Selecting **Train
PyTorch model from my ratings** trains a binary classifier on all rated events.
Once trained, the app displays its predicted choice and confidence beside each
new or existing event. Your choices remain editable and are used the next time
you train. Training requires at least one event in each category.

By default it connects to topic `rsyslog` on
`172.16.108.245:9092,172.16.108.246:9092,172.16.108.247:9092`.

## Run

```sh
python -m pip install -r requirements.txt
python app.py
```

Open <http://127.0.0.1:5000>. The app stores received events and ratings in
`rsyslog-events.sqlite3` and the trained model in `importance-model.pt` in the
current directory. It listens on localhost by default. To listen on another
interface, set `WEB_HOST=0.0.0.0` before starting it.

## Configuration

Use environment variables to change Kafka or storage settings:

```sh
KAFKA_BROKERS=broker1:9092,broker2:9092 \
KAFKA_TOPIC=rsyslog \
KAFKA_GROUP_ID=rsyslog-review-web \
RSYSLOG_DB=/path/to/events.sqlite3 \
IMPORTANCE_MODEL=/path/to/importance-model.pt \
python app.py
```

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
