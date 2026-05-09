# api/core/alerts.py
import os
from datetime import datetime, timezone

import httpx

SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL", "")
FRAUD_THRESHOLD    = float(os.getenv("FRAUD_ALERT_THRESHOLD", "0.80"))
PIPELINE_ALERT_URL = os.getenv("PIPELINE_ALERT_WEBHOOK_URL", "")


def _decision_emoji(decision: str) -> str:
    return {"BLOCK": "🚫", "3DS_REQUIRED": "⚠️", "APPROVED": "✅"}.get(decision, "❓")


async def send_fraud_alert(
    transaction_id: str,
    user_id: str,
    score: float,
    decision: str,
) -> None:
    """Fire-and-forget Slack alert when fraud_probability >= FRAUD_THRESHOLD."""
    if score < FRAUD_THRESHOLD or not SLACK_WEBHOOK_URL:
        return

    emoji   = _decision_emoji(decision)
    payload = {
        "text": f"{emoji} *ALERTE FRAUDE KiVendTout* — Score : `{score:.4f}`",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {"title": "Transaction ID", "value": transaction_id,         "short": True},
                    {"title": "User ID",         "value": user_id,               "short": True},
                    {"title": "Score",           "value": f"{score:.4f}",        "short": True},
                    {"title": "Décision",        "value": f"{emoji} {decision}", "short": True},
                    {
                        "title": "Timestamp (UTC)",
                        "value": datetime.now(timezone.utc).isoformat(),
                        "short": False,
                    },
                ],
                "footer": "KiVendTout Fraud Detection API",
            }
        ],
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(SLACK_WEBHOOK_URL, json=payload)
        except Exception:
            pass


async def send_pipeline_alert(component: str, message: str, level: str = "warning") -> None:
    """Generic operational alert for pipeline monitoring."""
    if not PIPELINE_ALERT_URL:
        return
    color   = {"error": "danger", "warning": "warning", "info": "good"}.get(level, "warning")
    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"[KiVendTout Pipeline] {component}",
                "text":  message,
                "footer": f"UTC: {datetime.now(timezone.utc).isoformat()}",
            }
        ]
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(PIPELINE_ALERT_URL, json=payload)
        except Exception:
            pass
