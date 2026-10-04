"""Generate the clickstream sample (`data/samples/clickstream_events.jsonl`).

Sessions follow a realistic funnel (product_view -> add_to_cart -> purchase)
over the same product catalog and customer ids as the Azure SQL seed, so the
events can be joined with Silver. Intentional streaming edge cases:
- duplicated events (same event_id delivered twice: at-least-once delivery);
- late events (event_time hours older than the arrival order);
- anonymous sessions (customer_id = null until login);
- schema evolution: newer app versions add `campaign` (optional field).

Usage:
    python scripts/generate_clickstream.py [--sessions 1200] [--seed 2026]
"""

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "sql"))

from synthetic_catalog import INACTIVE_SKUS, PRODUCT_CATALOG  # noqa: E402

OUTPUT = PROJECT_ROOT / "data" / "samples" / "clickstream_events.jsonl"
NUM_CUSTOMERS = 2000
CAMPAIGNS = ["black_friday", "dia_sin_iva", "newsletter_semana", "retargeting_carrito"]


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_events(sessions: int, rng: random.Random, now: datetime):
    products = [(pid, sku, name, category, price)
                for pid, (sku, name, category, price, _w, _d) in enumerate(PRODUCT_CATALOG, start=1)
                if sku not in INACTIVE_SKUS]
    events = []
    for _ in range(sessions):
        channel = rng.choices(["web", "app"], weights=[0.55, 0.45], k=1)[0]
        device = rng.choice(["ios", "android"]) if channel == "app" else rng.choice(["desktop", "mobile_web"])
        customer_id = None if rng.random() < 0.25 else rng.randint(1, NUM_CUSTOMERS)
        session_id = f"s-{uuid.UUID(int=rng.getrandbits(128)).hex[:16]}"
        app_version = rng.choice(["5.2.0", "5.3.0"]) if channel == "app" else None
        campaign = rng.choice(CAMPAIGNS) if rng.random() < 0.3 else None
        ts = now - timedelta(minutes=rng.randint(5, 72 * 60))
        category = rng.choice(sorted({p[3] for p in products}))
        candidates = [p for p in products if p[3] == category] or products

        def emit(event_type, product, quantity=None):
            nonlocal ts
            ts += timedelta(seconds=rng.randint(5, 180), milliseconds=rng.randint(0, 999))
            event = {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "event_time": iso(ts),
                "event_type": event_type,
                "customer_id": customer_id,
                "session_id": session_id,
                "product_id": product[0],
                "sku": product[1],
                "category": product[3],
                "price": float(product[4]),
                "quantity": quantity,
                "channel": channel,
                "device": device,
            }
            if app_version:
                event["app_version"] = app_version
                if app_version == "5.3.0" and campaign:
                    event["campaign"] = campaign  # optional field added in a newer app version
            events.append(event)

        viewed = rng.sample(candidates, k=min(len(candidates), rng.choice([1, 1, 2, 3, 4, 6])))
        for product in viewed:
            emit("product_view", product)
        if rng.random() < 0.30:
            cart = rng.sample(viewed, k=rng.choice([1, 1, 2]) if len(viewed) > 1 else 1)
            for product in cart:
                emit("add_to_cart", product, quantity=rng.choice([1, 1, 2]))
            if rng.random() < 0.40:
                if customer_id is None:
                    customer_id = rng.randint(1, NUM_CUSTOMERS)  # login at checkout
                for product in cart:
                    emit("purchase", product, quantity=1)

    events.sort(key=lambda e: e["event_time"])
    for index in rng.sample(range(len(events)), k=max(1, len(events) // 100)):  # ~1% duplicates
        events.insert(index + rng.randint(1, 20), dict(events[index]))
    for index in rng.sample(range(len(events)), k=max(1, len(events) // 200)):  # ~0.5% late arrivals
        late = dict(events[index])
        late["event_time"] = iso(datetime.fromisoformat(late["event_time"].replace("Z", "+00:00"))
                                 - timedelta(hours=rng.randint(2, 30)))
        events[index] = late
    return events


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sessions", type=int, default=1200)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--now", default="2026-06-15T18:00:00+00:00", help="Fixed reference time (reproducible)")
    args = parser.parse_args()

    events = build_events(args.sessions, random.Random(args.seed), datetime.fromisoformat(args.now).astimezone(timezone.utc))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"{len(events)} eventos -> {OUTPUT.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
