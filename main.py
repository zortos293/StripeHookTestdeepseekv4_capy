import json
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import stripe
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

DATABASE_PATH = os.environ.get("DATABASE_PATH", "stripe_events.db")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stripe_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stripe_event_id TEXT UNIQUE NOT NULL,
            event_type TEXT NOT NULL,
            created_timestamp INTEGER NOT NULL,
            payload TEXT NOT NULL,
            discord_status TEXT NOT NULL DEFAULT 'pending',
            discord_sent_at TEXT,
            discord_error TEXT,
            received_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=lifespan)

discord_client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))


def insert_event(stripe_event_id: str, event_type: str, created_timestamp: int, payload: str) -> bool:
    """Insert a Stripe event into SQLite. Returns True if inserted, False if duplicate."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO stripe_events (stripe_event_id, event_type, created_timestamp, payload) VALUES (?, ?, ?, ?)",
            (stripe_event_id, event_type, created_timestamp, payload),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def update_discord_status(stripe_event_id: str, status: str, error: str | None = None):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE stripe_events SET discord_status = ?, discord_sent_at = ?, discord_error = ?, updated_at = ? WHERE stripe_event_id = ?",
        (status, now, error, now, stripe_event_id),
    )
    conn.commit()
    conn.close()


def build_discord_embed(event: stripe.Event) -> dict:
    obj = event.data.object
    obj_type = obj.get("object", "unknown") if isinstance(obj, dict) else "unknown"

    embed = {
        "title": f"Stripe: {event.type}",
        "color": 0x635BFF,  # Stripe blurple
        "fields": [
            {"name": "Event ID", "value": event.id, "inline": True},
            {"name": "Object Type", "value": obj_type, "inline": True},
            {"name": "Created", "value": f"<t:{event.created}:F>", "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if isinstance(obj, dict):
        extras = {}
        if "amount" in obj:
            extras["Amount"] = f"{obj['amount'] / 100:.2f} {obj.get('currency', 'usd').upper()}"
        if "customer" in obj and obj["customer"]:
            extras["Customer"] = str(obj["customer"])
        if "status" in obj:
            extras["Status"] = str(obj["status"])
        if "payment_method" in obj and obj["payment_method"]:
            extras["Payment Method"] = str(obj["payment_method"])
        if "amount_received" in obj:
            extras["Amount Received"] = f"{obj['amount_received'] / 100:.2f} {obj.get('currency', 'usd').upper()}"

        for label, value in extras.items():
            embed["fields"].append({"name": label, "value": value, "inline": True})

    return embed


async def send_discord_notification(event: stripe.Event) -> tuple[bool, str | None]:
    if not DISCORD_WEBHOOK_URL:
        return False, "DISCORD_WEBHOOK_URL not configured"

    embed = build_discord_embed(event)
    payload = {"embeds": [embed]}

    try:
        resp = await discord_client.post(DISCORD_WEBHOOK_URL, json=payload)
        resp.raise_for_status()
        return True, None
    except httpx.HTTPError as e:
        return False, str(e)


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET not configured")

    body = await request.body()
    signature_header = request.headers.get("stripe-signature")

    if not signature_header:
        raise HTTPException(status_code=400, detail="Missing stripe-signature header")

    # Verify Stripe signature
    try:
        event = stripe.Webhook.construct_event(
            payload=body,
            sig_header=signature_header,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")

    payload_str = json.dumps(event.data.object) if hasattr(event.data, "object") else "{}"

    # Insert event (idempotent)
    inserted = insert_event(
        stripe_event_id=event.id,
        event_type=event.type,
        created_timestamp=event.created,
        payload=payload_str,
    )

    if not inserted:
        # Already processed — still return 200 for Stripe
        return JSONResponse(
            content={"status": "duplicate", "event_id": event.id},
            status_code=200,
        )

    # Send Discord notification
    success, error = await send_discord_notification(event)
    discord_status = "sent" if success else "failed"
    update_discord_status(event.id, discord_status, error)

    return JSONResponse(
        content={"status": "ok", "event_id": event.id, "discord": discord_status},
        status_code=200,
    )
