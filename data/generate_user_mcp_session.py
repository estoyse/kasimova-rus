#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone
import os
import sys
import time
import qrcode
from telethon.sync import TelegramClient
from telethon import errors
from telethon.sessions import StringSession

API_ID = 29133012
API_HASH = "ae51232178bfbb34e2d8d8650561e70c"
ARTIFACT_DIR = "/root/.gemini/antigravity-cli/brain/acf38fd8-e942-4c1d-a141-c9d590025166"
QR_IMG_PATH = os.path.join(ARTIFACT_DIR, "telegram_mcp_login_qr.png")
STATUS_FILE = os.path.join(ARTIFACT_DIR, "telegram_mcp_status.txt")
ENV_OUTPUT = "/opt/hermes-clients/clients/kasimova-rus/data/test_user_mcp.env"

def save_qr_image(url: str):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img.save(QR_IMG_PATH)

def seconds_until_expiry(qr) -> float:
    expires = qr.expires
    return max(1.0, (expires - datetime.now(timezone.utc)).total_seconds())

def main():
    with open(STATUS_FILE, "w") as f:
        f.write("STARTING\n")

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    client.connect()

    if client.is_user_authorized():
        print("Already authorized!")
        me = client.get_me()
        print(f"Logged in as {me.id} (@{me.username})")
        return

    qr = client.qr_login()
    print(f"QR_URL: {qr.url}", flush=True)
    save_qr_image(qr.url)

    with open(STATUS_FILE, "w") as f:
        f.write(f"WAITING_SCAN\n{qr.url}\n")

    max_refreshes = 15
    for i in range(max_refreshes):
        try:
            print(f"Waiting for scan attempt {i+1}/{max_refreshes}...", flush=True)
            timeout = seconds_until_expiry(qr)
            client.loop.run_until_complete(qr.wait(timeout=timeout))
            print("Scan confirmed by Telegram!", flush=True)
            break
        except asyncio.TimeoutError:
            print("QR expired, recreating...", flush=True)
            try:
                client.loop.run_until_complete(qr.recreate())
                print(f"QR_URL: {qr.url}", flush=True)
                save_qr_image(qr.url)
                with open(STATUS_FILE, "w") as f:
                    f.write(f"WAITING_SCAN\n{qr.url}\n")
            except Exception as e:
                print(f"Failed to recreate QR: {e}", flush=True)
                break
        except errors.SessionPasswordNeededError:
            print("2FA_PASSWORD_NEEDED", flush=True)
            with open(STATUS_FILE, "w") as f:
                f.write("2FA_PASSWORD_NEEDED\n")
            pwd_file = os.path.join(ARTIFACT_DIR, "2fa_password.txt")
            signed_in = False
            for _ in range(90):
                if os.path.exists(pwd_file):
                    with open(pwd_file, "r") as pf:
                        pwd = pf.read().strip()
                    try:
                        os.remove(pwd_file)
                    except Exception:
                        pass
                    try:
                        client.sign_in(password=pwd)
                        print("2FA sign in successful!", flush=True)
                        signed_in = True
                        break
                    except Exception as pe:
                        print(f"Invalid 2FA password: {pe}", flush=True)
                        with open(STATUS_FILE, "w") as f:
                            f.write(f"2FA_ERROR: {pe}\n")
                time.sleep(2)
            if signed_in:
                break
            else:
                print("2FA timed out.", flush=True)
                break
        except Exception as e:
            print(f"QR login error: {e}", flush=True)
            break

    if not client.is_user_authorized():
        print("Authentication not completed.", flush=True)
        with open(STATUS_FILE, "w") as f:
            f.write("FAILED\n")
        client.disconnect()
        return

    me = client.get_me()
    session_str = StringSession.save(client.session)
    username = me.username or ""
    print(f"Authenticated as: {me.id} (@{username})", flush=True)

    with open(ENV_OUTPUT, "w") as f:
        f.write(f"TELEGRAM_SESSION_STRING={session_str}\n")
        if username:
            f.write(f"TELEGRAM_EXPECTED_USERNAME={username}\n")

    with open(STATUS_FILE, "w") as f:
        f.write(f"SUCCESS\n{me.id}\n{username}\n")

    client.disconnect()
    print("Session generated and saved to test_user_mcp.env!", flush=True)

if __name__ == "__main__":
    main()
