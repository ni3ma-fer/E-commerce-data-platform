# api/core/auth.py
import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "kivendtout-dev-secret-change-in-prod")
ALGORITHM  = "HS256"
TOKEN_TTL  = int(os.getenv("JWT_TOKEN_TTL_MINUTES", "60"))

# Hardcoded users for demo — replace with DB lookup in production
_USERS = {
    "admin":  {"password": "admin",  "role": "admin"},
    "analyst":{"password": "analyst","role": "analyst"},
}

security = HTTPBearer()


def create_access_token(subject: str, role: str, expires_delta: Optional[timedelta] = None) -> str:
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=TOKEN_TTL))
    payload = {"sub": subject, "role": role, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def authenticate_user(username: str, password: str) -> Optional[dict]:
    user = _USERS.get(username)
    if user and user["password"] == password:
        return {"username": username, "role": user["role"]}
    return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            raise exc
        return {"username": username, "role": payload.get("role", "user")}
    except JWTError:
        raise exc


async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Accès admin requis")
    return user
