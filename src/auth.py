from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
import os

from typing import Optional

security = HTTPBearer(auto_error=False)

def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)):
    """
    Returns a static user payload. 
    Authentication is handled by the 'check_access_key' middleware 
    via the X-BASEMENT-KEY header (Basement Password).
    """
    # Always return a consistent user object to maintain compatibility with 
    # endpoints that group data by user_id (sub).
    return {
        "sub": "00000000-0000-0000-0000-000000000000", 
        "email": "admin@basement.bets"
    }
