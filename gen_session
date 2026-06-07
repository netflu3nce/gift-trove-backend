"""
Run this ONCE on your own machine to mint a StringSession, then paste the output
into STRING_SESSION (env var on Render). Use a dedicated Telegram account.

    pip install telethon
    API_ID=123456 API_HASH=xxxx python gen_session.py

It will ask for the phone number + login code (and 2FA password if set).
"""
import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    api_id = int(os.getenv("API_ID") or input("API_ID: ").strip())
    api_hash = os.getenv("API_HASH") or input("API_HASH: ").strip()
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\n================ STRING_SESSION (keep secret) ================\n")
        print(client.session.save())
        print("\n=============================================================\n")
        me = await client.get_me()
        print("Logged in as:", getattr(me, "username", None) or me.id)


if __name__ == "__main__":
    asyncio.run(main())
