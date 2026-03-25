import logging
import os
import re
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


def escape_md(text: str) -> str:
    """Échappe les caractères spéciaux pour MarkdownV2 de Telegram."""
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


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
    """Relaie le message de l'utilisateur vers le groupe."""
    user    = update.effective_user
    message = update.message

    # Ignorer les messages venant du groupe lui-même
    if update.effective_chat.id == GROUP_ID:
        return

    username = f"@{escape_md(user.username)}" if user.username else "\\(pas de username\\)"
    display  = escape_md(user.full_name or user.username or str(user.id))

    header = (
        f"📨 *Nouveau message*\n"
        f"👤 Nom : {display}\n"
        f"🔖 Username : {username}\n"
        f"🆔 ID : `{user.id}`\n"
        f"{'─' * 28}\n"
    )

    sent = None
    try:
        # ── Texte ──────────────────────────────────
        if message.text:
            sent = await context.bot.send_message(
                chat_id    = GROUP_ID,
                text       = header + escape_md(message.text),
                parse_mode = "MarkdownV2",
            )

        # ── Photo ──────────────────────────────────
        elif message.photo:
            caption = escape_md(message.caption or "")
            sent = await context.bot.send_photo(
                chat_id    = GROUP_ID,
                photo      = message.photo[-1].file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Document ───────────────────────────────
        elif message.document:
            caption = escape_md(message.caption or "")
            sent = await context.bot.send_document(
                chat_id    = GROUP_ID,
                document   = message.document.file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Vidéo ──────────────────────────────────
        elif message.video:
            caption = escape_md(message.caption or "")
            sent = await context.bot.send_video(
                chat_id    = GROUP_ID,
                video      = message.video.file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Audio ──────────────────────────────────
        elif message.audio:
            caption = escape_md(message.caption or "")
            sent = await context.bot.send_audio(
                chat_id    = GROUP_ID,
                audio      = message.audio.file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Vocal ──────────────────────────────────
        elif message.voice:
            sent = await context.bot.send_voice(
                chat_id = GROUP_ID,
                voice   = message.voice.file_id,
                caption = header,
                parse_mode = "MarkdownV2",
            )

        # ── Sticker ────────────────────────────────
        elif message.sticker:
            await context.bot.send_message(
                chat_id    = GROUP_ID,
                text       = header + "🎭 *Sticker*",
                parse_mode = "MarkdownV2",
            )
            sent = await context.bot.send_sticker(
                chat_id = GROUP_ID,
                sticker = message.sticker.file_id,
            )

        else:
            logger.warning("Type de message non géré : %s", message)
            return

    except Exception as e:
        logger.error("Erreur lors du relais vers le groupe : %s", e)
        await message.reply_text("⚠️ Une erreur s'est produite lors de la transmission. Réessayez.")
        return

    if sent:
        pending[sent.message_id] = user.id
        logger.info("Relayé : user %s → groupe (msg_id=%s)", user.id, sent.message_id)

    await message.reply_text("✅ Votre message a été transmis. Attendez la réponse.")


# ──────────────────────────────────────────────────────────────
#  2.  Membre du groupe  →  Bot  →  Utilisateur
# ──────────────────────────────────────────────────────────────
async def handle_group_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Renvoie la réponse d'un membre du groupe à l'utilisateur concerné."""
    message = update.message

    if not message.reply_to_message:
        return

    replied_id = message.reply_to_message.message_id
    user_id    = pending.get(replied_id)

    if not user_id:
        return

    member = update.effective_user
    m_name = escape_md(member.full_name or f"@{member.username}")
    header = f"💬 *Réponse du support* — {m_name}\n{'─' * 28}\n"

    try:
        # ── Texte ──────────────────────────────────
        if message.text:
            await context.bot.send_message(
                chat_id    = user_id,
                text       = header + escape_md(message.text),
                parse_mode = "MarkdownV2",
            )

        # ── Photo ──────────────────────────────────
        elif message.photo:
            caption = escape_md(message.caption or "")
            await context.bot.send_photo(
                chat_id    = user_id,
                photo      = message.photo[-1].file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Document ───────────────────────────────
        elif message.document:
            caption = escape_md(message.caption or "")
            await context.bot.send_document(
                chat_id    = user_id,
                document   = message.document.file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Vidéo ──────────────────────────────────
        elif message.video:
            caption = escape_md(message.caption or "")
            await context.bot.send_video(
                chat_id    = user_id,
                video      = message.video.file_id,
                caption    = header + caption,
                parse_mode = "MarkdownV2",
            )

        # ── Vocal ──────────────────────────────────
        elif message.voice:
            await context.bot.send_voice(
                chat_id = user_id,
                voice   = message.voice.file_id,
            )

        # ── Sticker ────────────────────────────────
        elif message.sticker:
            await context.bot.send_sticker(
                chat_id = user_id,
                sticker = message.sticker.file_id,
            )

        else:
            logger.warning("Type de réponse non géré : %s", message)
            return

    except Exception as e:
        logger.error("Erreur lors du relais vers l'utilisateur %s : %s", user_id, e)
        await message.reply_text("⚠️ Impossible de transmettre la réponse à l'utilisateur.", quote=True)
        return

    logger.info("Réponse renvoyée : groupe → user %s", user_id)
    await message.reply_text("✅ Réponse transmise à l'utilisateur.", quote=True)


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────
def main():
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
