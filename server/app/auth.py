"""
API-key auth. Single shared key in the `X-API-Key` header.
Disable entirely with REQUIRE_AUTH=false (local/trusted use).
"""
from fastapi import Header, HTTPException, status

from .settings import get_settings


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    settings = get_settings()
    if not settings.REQUIRE_AUTH:
        return
    if x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header.",
        )
