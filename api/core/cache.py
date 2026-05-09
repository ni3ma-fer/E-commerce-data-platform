# api/core/cache.py
import json
import os
from typing import Any, Optional

_redis_client = None


def _get_client():
    global _redis_client
    if _redis_client is None:
        import redis
        _redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0,
            decode_responses=True,
            socket_connect_timeout=1,
        )
    return _redis_client


def cache_get(key: str) -> Optional[Any]:
    try:
        val = _get_client().get(key)
        return json.loads(val) if val else None
    except Exception:
        return None


def cache_set(key: str, value: Any, ttl: int = 300) -> None:
    try:
        _get_client().setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        pass  # degrade gracefully when Redis unavailable


def cache_delete(key: str) -> None:
    try:
        _get_client().delete(key)
    except Exception:
        pass


def cache_keys(pattern: str) -> list[str]:
    try:
        return _get_client().keys(pattern)
    except Exception:
        return []
