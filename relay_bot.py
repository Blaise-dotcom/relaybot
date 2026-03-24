import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────
#  CONFIG  –  variables d'environnement Render
# ─────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_ID  = int(os.environ["GROUP_ID"])
# ─────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Mapping :  message_id (du message bot dans le groupe)  →  user_id
pending: dict[int, int] = {}


# ──────────────────────────────────────────────────────────────
#  SERVEUR HTTP  –  requis pour Render Web Service (plan gratuit)
# ──────────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot OK")
    def log_message(self, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info("Health server sur port %s", port)
    server.serve_forever()


# ──────────────────────────────────────────────────────────────
#  /start  –  Message de bienvenue
# ──────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Salut ! Écris-nous pour l'inscription au tournoi. 🏆\n"
        "Un admin va bientôt te répondre !"
    )


# ──────────────────────────────────────────────────────────────
#  1.  Utilisateur  →  Bot  →  Groupe
# ──────────────────────────────────────────────────────────────
async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    message = update.message

    if update.effective_chat.id == GROUP_ID:
        return

    username = f"@{user.username}" if user.username else "(pas de username)"
    display  = user.full_name or username

    header = (
        f"📨 *Nouveau message*\n"
        f"👤 Nom : {display}\n"
        f"🔖 Username : {username}\n"
        f"🆔 ID : `{user.id}`\n"
        f"{'─' * 28}\n"
    )

    if message.text:
        sent = await context.bot.send_message(
            chat_id=GROUP_ID, text=header + message.text, parse_mode="Markdown"
        )
    elif message.photo:
        sent = await context.bot.send_photo(
            chat_id=GROUP_ID, photo=message.photo[-1].file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.document:
        sent = await context.bot.send_document(
            chat_id=GROUP_ID, document=message.document.file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.video:
        sent = await context.bot.send_video(
            chat_id=GROUP_ID, video=message.video.file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.audio:
        sent = await context.bot.send_audio(
            chat_id=GROUP_ID, audio=message.audio.file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.voice:
        sent = await context.bot.send_voice(
            chat_id=GROUP_ID, voice=message.voice.file_id, caption=header
        )
    elif message.sticker:
        await context.bot.send_message(
            chat_id=GROUP_ID, text=header + "🎭 *Sticker*", parse_mode="Markdown"
        )
        sent = await context.bot.send_sticker(
            chat_id=GROUP_ID, sticker=message.sticker.file_id
        )
    else:
        logger.warning("Type de message non géré : %s", message)
        return

    pending[sent.message_id] = user.id
    logger.info("Relayé : user %s → groupe (msg_id=%s)", user.id, sent.message_id)
    await message.reply_text("✅ Votre message a été transmis. Attendez la réponse.")


# ──────────────────────────────────────────────────────────────
#  2.  Membre du groupe  →  Bot  →  Utilisateur
# ──────────────────────────────────────────────────────────────
async def handle_group_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message

    if not message.reply_to_message:
        return

    replied_id = message.reply_to_message.message_id
    user_id    = pending.get(replied_id)

    if not user_id:
        return

    member = update.effective_user
    m_name = member.full_name or f"@{member.username}"
    header = f"💬 *Réponse du support* — {m_name}\n{'─' * 28}\n"

    if message.text:
        await context.bot.send_message(
            chat_id=user_id, text=header + message.text, parse_mode="Markdown"
        )
    elif message.photo:
        await context.bot.send_photo(
            chat_id=user_id, photo=message.photo[-1].file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.document:
        await context.bot.send_document(
            chat_id=user_id, document=message.document.file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.video:
        await context.bot.send_video(
            chat_id=user_id, video=message.video.file_id,
            caption=header + (message.caption or ""), parse_mode="Markdown"
        )
    elif message.voice:
        await context.bot.send_voice(chat_id=user_id, voice=message.voice.file_id)
    elif message.sticker:
        await context.bot.send_sticker(chat_id=user_id, sticker=message.sticker.file_id)
    else:
        logger.warning("Type de réponse non géré : %s", message)
        return

    logger.info("Réponse renvoyée : groupe → user %s", user_id)
    await message.reply_text("✅ Réponse transmise à l'utilisateur.", quote=True)


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────
def main():
    # Lancer le serveur HTTP en arrière-plan AVANT le bot
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(
        filters.ALL & ~filters.Chat(GROUP_ID) & ~filters.COMMAND,
        handle_user_message,
    ))
    app.add_handler(MessageHandler(
        filters.Chat(GROUP_ID) & filters.REPLY,
        handle_group_reply,
    ))

    logger.info("Bot démarré — en attente de messages...")
    app.run_polling()


if __name__ == "__main__":
    main()
