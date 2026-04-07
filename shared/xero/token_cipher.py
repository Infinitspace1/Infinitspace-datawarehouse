"""
shared/xero/token_cipher.py

Encrypts/decrypts OAuth tokens at rest.
"""
from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken

DEFAULT_ENCRYPTION_KEY_ENV = "INTEGRATIONS_ENCRYPTION_KEY"


class TokenCipher:
    def __init__(self, encryption_key: str):
        if not encryption_key:
            raise ValueError("Encryption key is required")
        self._fernet = Fernet(encryption_key.encode("utf-8"))

    @classmethod
    def from_env(cls, env_var: str = DEFAULT_ENCRYPTION_KEY_ENV) -> "TokenCipher":
        key = os.getenv(env_var)
        if not key:
            raise EnvironmentError(
                f"Missing {env_var}. Generate one with TokenCipher.generate_key()."
            )
        return cls(key)

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode("utf-8")

    def encrypt(self, value: str) -> str:
        if value is None:
            raise ValueError("Cannot encrypt None")
        token = self._fernet.encrypt(value.encode("utf-8"))
        return token.decode("utf-8")

    def decrypt(self, value: str) -> str:
        if value is None:
            raise ValueError("Cannot decrypt None")
        try:
            clear = self._fernet.decrypt(value.encode("utf-8"))
        except InvalidToken as exc:
            raise ValueError("Stored token cannot be decrypted with current key") from exc
        return clear.decode("utf-8")
