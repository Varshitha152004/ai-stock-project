from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text
from database import engine

# ============================================================
# CONFIG
# ============================================================

SECRET_KEY = "change_this_secret_key"  # Move to env later
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

#  IMPORTANT FIX: match your login route exactly
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# ============================================================
# PASSWORD UTILS
# ============================================================

def hash_password(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)

# ============================================================
# JWT TOKEN CREATION
# ============================================================

def create_access_token(data: dict):
    to_encode = data.copy()

    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# ============================================================
# AUTH DEPENDENCY
# ============================================================

def get_current_user(token: str = Depends(oauth2_scheme)):

    credentials_exception = HTTPException(
        status_code=401,
        detail="Invalid or expired token"
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")

        if username is None:
            raise credentials_exception

    except JWTError:
        raise credentials_exception

    with engine.connect() as conn:
        user = conn.execute(
            text("SELECT id, username FROM users WHERE username=:username"),
            {"username": username}
        ).fetchone()

    if user is None:
        raise credentials_exception

    return {"id": user[0], "username": user[1]}
