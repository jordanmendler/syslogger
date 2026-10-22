#!/usr/bin/env python3
"""Train and use a small PyTorch autoencoder to flag unusual rsyslog entries."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from pathlib import Path
from typing import Iterable

import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset


TOKEN_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_.-]*|\d+|[^\s]")
NORMALIZERS = (
    (re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}\b"), "<uuid>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<ip>"),
    (re.compile(r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?![\w:])", re.I), "<ipv6>"),
    (re.compile(r"\b\d+\b"), "<num>"),
)


def read_lines(paths: list[str]) -> list[str]:
    lines: list[str] = []
    for name in paths:
        if name == "-":
            lines.extend(sys.stdin)
            continue
        path = Path(name)
        if not path.is_file():
            raise ValueError(f"not a readable file: {name}")
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            lines.extend(stream)
    return [line.rstrip("\r\n") for line in lines if line.strip()]


def input_lines(args: argparse.Namespace, limit: int | None = None) -> list[str]:
    if args.source == "files":
        if not args.logs:
            raise ValueError("provide one or more log files when --source=files")
        return read_lines(args.logs)
    if args.logs:
        raise ValueError("log file arguments cannot be combined with --source=kafka")
    try:
        from kafka import KafkaConsumer
    except ImportError as error:
        raise ValueError("Kafka input requires kafka-python; install requirements.txt") from error

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.brokers,
        group_id=args.group_id,
        auto_offset_reset=args.offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
    )
    lines: list[str] = []
    last_message = time.monotonic()
    try:
        while limit is None or len(lines) < limit:
            records = consumer.poll(timeout_ms=1000, max_records=min(500, limit - len(lines)) if limit else 500)
            for batch in records.values():
                for message in batch:
                    value = message.value.strip()
                    if value:
                        lines.append(value)
                        last_message = time.monotonic()
                        if limit is not None and len(lines) >= limit:
                            break
                if limit is not None and len(lines) >= limit:
                    break
            if limit is not None and time.monotonic() - last_message >= args.idle_timeout:
                break
    finally:
        consumer.close()
    return lines


def score_kafka(args: argparse.Namespace, model: nn.Module, dimensions: int, threshold: float) -> None:
    try:
        from kafka import KafkaConsumer
    except ImportError as error:
        raise ValueError("Kafka input requires kafka-python; install requirements.txt") from error
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.brokers,
        group_id=args.group_id,
        auto_offset_reset=args.offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
    )
    seen = 0
    try:
        while args.max_messages == 0 or seen < args.max_messages:
            records = consumer.poll(timeout_ms=1000, max_records=min(500, args.max_messages - seen) if args.max_messages else 500)
            lines = [message.value.strip() for batch in records.values() for message in batch if message.value.strip()]
            if not lines:
                continue
            features = vectorize(lines, dimensions)
            with torch.no_grad():
                errors = ((model(features) - features) ** 2).mean(dim=1).tolist()
            results = sorted(zip(lines, errors), key=lambda item: item[1], reverse=True)
            for line, error in results:
                label = "ANOMALY" if error > threshold else "normal"
                print(f"{label}\terror={error:.6f}\t{line}", flush=True)
            seen += len(lines)
    finally:
        consumer.close()


def tokens(line: str) -> list[str]:
    text = line.lower()
    for pattern, replacement in NORMALIZERS:
        text = pattern.sub(replacement, text)
    return TOKEN_RE.findall(text)


def vectorize(lines: Iterable[str], dimensions: int) -> torch.Tensor:
    rows = []
    for line in lines:
        row = [0.0] * dimensions
        for token in set(tokens(line)):
            digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
            index = int.from_bytes(digest, "little") % dimensions
            row[index] = 1.0
        rows.append(row)
    if not rows:
        return torch.empty((0, dimensions), dtype=torch.float32)
    return torch.tensor(rows, dtype=torch.float32)


class LogAutoencoder(nn.Module):
    def __init__(self, dimensions: int, latent: int):
        super().__init__()
        hidden = max(latent * 2, dimensions // 4)
        self.encoder = nn.Sequential(nn.Linear(dimensions, hidden), nn.ReLU(), nn.Linear(hidden, latent), nn.ReLU())
        self.decoder = nn.Sequential(nn.Linear(latent, hidden), nn.ReLU(), nn.Linear(hidden, dimensions), nn.Sigmoid())

    def forward(self, inputs: torch.Tensor) -> torch.Tensor:
        return self.decoder(self.encoder(inputs))


def train(args: argparse.Namespace) -> None:
    lines = input_lines(args, args.max_messages if args.source == "kafka" else None)
    if len(lines) < 2:
        raise ValueError("training requires at least two non-empty log lines")
    features = vectorize(lines, args.dimensions)
    model = LogAutoencoder(args.dimensions, args.latent)
    loader = DataLoader(TensorDataset(features), batch_size=args.batch_size, shuffle=True)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    loss_fn = nn.BCELoss()
    model.train()
    for epoch in range(1, args.epochs + 1):
        total = 0.0
        for (batch,) in loader:
            optimizer.zero_grad()
            loss = loss_fn(model(batch), batch)
            loss.backward()
            optimizer.step()
            total += loss.item() * len(batch)
        print(f"epoch {epoch:03d}/{args.epochs}: loss={total / len(features):.6f}")

    model.eval()
    with torch.no_grad():
        errors = ((model(features) - features) ** 2).mean(dim=1)
    threshold = float(torch.quantile(errors, args.quantile).item())
    artifact = {
        "dimensions": args.dimensions,
        "latent": args.latent,
        "state_dict": model.state_dict(),
        "threshold": threshold,
        "quantile": args.quantile,
    }
    torch.save(artifact, args.model)
    print(f"saved {args.model} ({len(lines)} lines; anomaly threshold={threshold:.6f})")


def score(args: argparse.Namespace) -> None:
    # weights_only=False is needed for the state-dict checkpoint format on newer PyTorch.
    try:
        artifact = torch.load(args.model, map_location="cpu", weights_only=False)
    except TypeError:  # PyTorch versions before weights_only was added.
        artifact = torch.load(args.model, map_location="cpu")
    dimensions = int(artifact["dimensions"])
    model = LogAutoencoder(dimensions, int(artifact["latent"]))
    model.load_state_dict(artifact["state_dict"])
    model.eval()
    threshold = float(artifact["threshold"])
    if args.source == "kafka" and args.max_messages == 0:
        if args.logs:
            raise ValueError("log file arguments cannot be combined with --source=kafka")
        score_kafka(args, model, dimensions, threshold)
        return
    lines = input_lines(args, args.max_messages if args.source == "kafka" and args.max_messages else None)
    if not lines:
        raise ValueError("no non-empty log lines to score")
    features = vectorize(lines, dimensions)
    with torch.no_grad():
        errors = ((model(features) - features) ** 2).mean(dim=1).tolist()
    results = sorted(zip(lines, errors), key=lambda item: item[1], reverse=True)
    limit = args.top if args.top else len(results)
    for line, error in results[:limit]:
        label = "ANOMALY" if error > threshold else "normal"
        print(f"{label}\terror={error:.6f}\t{line}", flush=True)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    fit = commands.add_parser("train", help="train a model from baseline logs")
    fit.add_argument("logs", nargs="*", help="log files (use --source=files), or - for stdin")
    add_input_options(fit)
    fit.add_argument("--model", default="rsyslog-model.pt", help="checkpoint output path")
    fit.add_argument("--dimensions", type=int, default=2048, help="hashed token feature count")
    fit.add_argument("--latent", type=int, default=64, help="autoencoder bottleneck width")
    fit.add_argument("--epochs", type=int, default=20)
    fit.add_argument("--batch-size", type=int, default=128)
    fit.add_argument("--learning-rate", type=float, default=1e-3)
    fit.add_argument("--quantile", type=float, default=0.99, help="training error quantile used as threshold")
    fit.add_argument("--max-messages", type=int, default=10000, help="maximum Kafka records used for training")
    fit.set_defaults(handler=train)

    analyze = commands.add_parser("score", help="rank log lines by reconstruction error")
    analyze.add_argument("logs", nargs="*", help="log files (use --source=files), or - for stdin")
    add_input_options(analyze)
    analyze.add_argument("--model", default="rsyslog-model.pt", help="checkpoint to load")
    analyze.add_argument("--top", type=int, default=0, help="print only this many highest-scoring lines")
    analyze.add_argument("--max-messages", type=int, default=0, help="stop after this many Kafka records (default: keep running)")
    analyze.set_defaults(handler=score)
    return root


def add_input_options(command: argparse.ArgumentParser) -> None:
    command.add_argument("--source", choices=("kafka", "files"), default="kafka", help="input source (default: Kafka)")
    command.add_argument("--brokers", default="172.16.108.245:9092,172.16.108.246:9092,172.16.108.247:9092")
    command.add_argument("--topic", default="rsyslog")
    command.add_argument("--group-id", default="rsyslog-analyzer")
    command.add_argument("--offset-reset", choices=("earliest", "latest"), default="latest")
    command.add_argument("--idle-timeout", type=float, default=10.0, help="stop bounded Kafka reads after this many idle seconds")


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "train":
            if args.dimensions < 8 or args.latent < 1 or args.epochs < 1 or args.batch_size < 1:
                raise ValueError("dimensions must be >= 8; latent, epochs and batch-size must be positive")
            if not 0.0 < args.quantile < 1.0:
                raise ValueError("quantile must be between 0 and 1")
        if args.command == "score" and args.top < 0:
            raise ValueError("top must be zero or positive")
        if args.idle_timeout <= 0 or (args.source == "kafka" and args.command == "train" and args.max_messages < 2):
            raise ValueError("idle-timeout must be positive and Kafka training max-messages must be at least 2")
        if args.command == "score" and args.max_messages < 0:
            raise ValueError("max-messages must be zero or positive")
        args.handler(args)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
