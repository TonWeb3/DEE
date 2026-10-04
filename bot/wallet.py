"""
Trading-credential helpers.

A user may supply EITHER a hex private key OR a 12/15/18/21/24-word BIP-39 seed
phrase. These helpers normalise whichever was entered into a private key for the
CLOB client and derive the wallet address (used as the CLOB `funder`), so the user
never has to enter the funder address by hand.

Seed phrases derive the first account at the standard Ethereum path m/44'/60'/0'/0/0
(the same account MetaMask shows first).
"""
from eth_account import Account
from eth_utils import to_hex

# Required before Account.from_mnemonic() can be used.
Account.enable_unaudited_hdwallet_features()


def _is_mnemonic(secret: str) -> bool:
    """A seed phrase is several space-separated words; a private key is one token."""
    return len((secret or "").strip().split()) >= 12


def resolve_private_key(secret: str) -> str:
    """Return a 0x-prefixed hex private key from a key OR a seed phrase ('' if blank)."""
    s = (secret or "").strip()
    if not s:
        return ""
    if _is_mnemonic(s):
        return to_hex(Account.from_mnemonic(s).key)
    return s if s.startswith("0x") else "0x" + s


def derive_address(secret: str) -> str:
    """The EOA wallet address for a key or seed phrase ('' if blank/invalid)."""
    s = (secret or "").strip()
    if not s:
        return ""
    try:
        if _is_mnemonic(s):
            return Account.from_mnemonic(s).address
        return Account.from_key(s if s.startswith("0x") else "0x" + s).address
    except Exception:
        return ""


def mask_secret(secret: str) -> str:
    """A safe display form that never reveals the secret. Always contains '...' so the
    settings endpoint can detect a still-masked value and avoid overwriting it."""
    s = (secret or "").strip()
    if not s:
        return s
    if _is_mnemonic(s):
        return "... (seed phrase hidden)"
    if len(s) > 10:
        return s[:6] + "..." + s[-4:]
    return s
