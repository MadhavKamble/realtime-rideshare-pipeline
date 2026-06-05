from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone


PAYMENT_METHODS = ["cash", "card", "upi", "wallet"]
PAYMENT_STATUSES = ["success", "failed", "refunded"]


def generate_payment_event() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "payment_id": str(uuid.uuid4()),
        "ride_id": str(uuid.uuid4()),
        "event_timestamp": now.isoformat(),
        "payment_method": random.choices(PAYMENT_METHODS, weights=[0.10, 0.28, 0.52, 0.10], k=1)[0],
        "amount_inr": round(random.uniform(45, 1500), 2),
        "currency": "INR",
        "status": random.choices(PAYMENT_STATUSES, weights=[0.96, 0.02, 0.02], k=1)[0],
        "schema_version": "1.0",
    }
