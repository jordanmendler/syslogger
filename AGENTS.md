# Repository Guidelines

## Project Structure

- `app.py` contains the Flask web app, Kafka consumer, SQLite event store, rating UI, and PyTorch binary classifier.
- `rsyslog_analyzer.py` provides optional command-line log analysis and shared token vectorization used by the app.
- `requirements.txt` lists Python dependencies; `README.md` documents installation, configuration, and usage.
- There is no checked-in test or asset directory. The app creates its SQLite database and model checkpoint at runtime in the working directory unless configured otherwise.

## Run and Develop Locally

```sh
python -m pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5000`. The app connects to the configured Kafka brokers and stores events in SQLite. Set `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `RSYSLOG_DB`, or `IMPORTANCE_MODEL` in the environment to override defaults. Keep the default web binding on localhost unless remote access is intended.

## Coding Style

Use Python 3, four spaces for indentation, `snake_case` for functions and variables, and `PascalCase` for classes. Keep Flask route handlers focused on request handling and put reusable processing in named functions. Use parameterized SQL queries and preserve the app’s bounded event paging. No formatter or linter is currently configured; keep imports standard-library, third-party, then local, and follow surrounding style.

## Testing

No automated test framework or test suite is currently present. For changes to Kafka ingestion, persistence, or rating behavior, add focused tests (prefer Python’s built-in `unittest`) and isolate Kafka and database dependencies so tests do not require live brokers. Cover rating migration and the binary training labels when changing the learning workflow.

## Commits and Pull Requests

This directory has no available Git history to establish a commit convention. Use short, imperative commit subjects (for example, `Add binary event importance labels`). Pull requests should summarize behavior changes, note configuration or database migrations, include relevant UI screenshots, and describe validation performed. Link related issues when applicable.

## Configuration and Generated Data

Do not commit broker credentials, production log data, SQLite databases, or trained model files. Keep deployment-specific settings in environment variables and document new options in `README.md`.
