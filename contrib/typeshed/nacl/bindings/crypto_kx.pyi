from typing import Any

crypto_kx_PUBLIC_KEY_BYTES: Any
crypto_kx_SECRET_KEY_BYTES: Any
crypto_kx_SEED_BYTES: Any
crypto_kx_SESSION_KEY_BYTES: Any

def crypto_kx_keypair(): ...
def crypto_kx_client_session_keys(client_public_key: Any, client_secret_key: Any, server_public_key: Any): ...
def crypto_kx_server_session_keys(server_public_key: Any, server_secret_key: Any, client_public_key: Any): ...
