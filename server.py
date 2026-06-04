import os
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow your frontend Vercel app to talk to this backend securely
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BOT_TOKEN = os.getenv("BOT_TOKEN")

@app.get("/")
async def root():
    return {"status": "online", "project": "GiftTrove Backend", "version": "1.0.0"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    
    # Handle Telegram Stars Payment Validation
    if "pre_checkout_query" in data:
        query_id = data["pre_checkout_query"]["id"]
        import httpx
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/answerPreCheckoutQuery",
                json={"pre_checkout_query_id": query_id, "ok": True}
            )
        return {"status": "ok"}

    return {"status": "ignored"}
