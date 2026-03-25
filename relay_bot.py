# -*- coding: utf-8 -*-
# ⚔ BOT TELEGRAM — OTAKU WARS v9
# ================================
# NOUVEAUTES v8 :
# ✅ /roulette — Pari XP (doubler ou perdre, cooldown 24h, rang min C)
# ✅ /quetes   — Quêtes journalières et hebdomadaires avec récompenses
# ✅ /succes   — Badges débloquables automatiquement (12 succès)
# ✅ /coffre   — Coffre mystère tous les 50 messages du groupe
# ✅ /royale   — Mode Battle Royale quiz collectif (tout le groupe)
# ✅ /tournoi  — Tournois planifiés avec brackets et finales
# ✅ /carte    — Carte de joueur stylisée en Markdown Telegram
# ✅ Notifications streak en danger (MP auto à 20h si streak >= 3)
#
# NOUVEAUTES v9 :
# ✅ Système anti-répétition — 50 dernières questions mémorisées par groupe
# ✅ Open Trivia DB — questions anime auto depuis internet (fallback rapide)
# ✅ questions.json — 97 nouvelles questions hardcore FR chargées au démarrage
# ✅ Pool total : 500+ questions sur 3 niveaux (Normal/Hardcore/Duel)
# ✅ /addquestion — Admin peut ajouter des questions via Telegram
#
# INSTALLATION :


# 1. pip install "python-telegram-bot[job-queue]" python-dotenv psycopg2-binary --upgrade
# 2. Cree un fichier .env :
#    BOT_TOKEN=ton_token_ici
#    ADMIN_IDS=123456789,987654321
#    DATABASE_URL=postgresql://...
# 3. python otaku_wars_bot_v7.py
import os
import re
import asyncio
import logging
import random
from contextlib import contextmanager
from datetime import datetime, timedelta, date

import json as _json_module
import aiohttp
import io
try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_OK = True
except ImportError:
    PILLOW_OK = False
    logger_tmp = logging.getLogger(__name__)
    logger_tmp.warning("[carte] Pillow non installé — cartes image désactivées")
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest, ChatMigrated, TelegramError
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# ⚙️ CONFIGURATION — XP & GAMEPLAY
# ─────────────────────────────────────────

BOT_TOKEN   = os.getenv("BOT_TOKEN", "REMPLACE_MOI")
_raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS   = [int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()]
DATABASE_URL = os.getenv("DATABASE_URL", "")

UNIVERS_NOM     = "⚔️ Otaku Conquest"
MEMBRES_MINIMUM = int(os.getenv("MEMBRES_MINIMUM", "2"))   # mettre 500+ en production
CLANWAR_DUREE_H = 72

# ── Gains XP ──
XP_PAR_MESSAGE          = 2
XP_PAR_QUIZ_NORMAL      = 30
XP_PAR_QUIZ_HARDCORE    = 80
XP_PAR_COMBAT_WIN       = 50
XP_DAILY_BASE           = 100
XP_MYSTERE              = 60

# ── Points clan ──
POINTS_MESSAGE          = 1
POINTS_QUIZ_NORMAL      = 10
POINTS_QUIZ_HARDCORE    = 25
POINTS_COMBAT_WIN       = 25
POINTS_MONTEE_RANG      = 15
POINTS_CLANWAR_ACTION   = 50
POINTS_CLANWAR_VICTOIRE = 200

# ── Cooldowns (secondes) ──
COOLDOWN_MSG_SECONDES      = 60
COOLDOWN_QUIZ_SECONDES     = 5     # 5s entre chaque quiz
COOLDOWN_GIVE_SECONDES     = 3600    # 1h entre transferts XP
COOLDOWN_WAR_SECONDES      = 300     # 5 min entre défis /war (NOUVEAU)
TAXE_GIVE_POURCENT         = 10      # 10% prélevés sur /give
QUIZ_TIMEOUT_SECONDES      = 30
COMBAT_TIMEOUT_SECONDES    = 120
GIVE_ACCEPT_TIMEOUT_SEC    = 60
MYSTERE_INTERVAL_SECONDES  = 21600    # toutes les 6 heures

DUEL_NB_QUESTIONS = 3
DUEL_TIMEOUT_SEC  = 30

# ── Roulette ──
ROULETTE_COOLDOWN_SEC  = 86400   # 24h entre paris
ROULETTE_MISE_MIN      = 100     # XP minimum
ROULETTE_RANG_MIN      = "C"     # rang min pour jouer

# ── Coffre mystère ──
COFFRE_INTERVAL_MSG   = 50
COFFRE_TIMEOUT_SEC    = 30

# ── Questions ──
QUESTIONS_JSON_PATH    = "questions.json"   # fichier de questions externes
ANTI_REPEAT_SIZE       = 50                 # nb de questions mémorisées par groupe

# ── Groq AI — Génération automatique de questions ──
GROQ_API_KEY           = os.getenv("GROQ_API_KEY", "")
GROQ_API_URL           = "https://api.groq.com/openai/v1/chat/completions"
# Plusieurs modèles en rotation pour contourner les limites par modèle
GROQ_MODELS            = [
    "llama-3.1-8b-instant",       # 750k tokens/jour
    "llama3-8b-8192",             # 500k tokens/jour
    "gemma2-9b-it",               # 500k tokens/jour
]
GROQ_MODEL             = "llama-3.1-8b-instant"   # modèle par défaut
GROQ_QUESTIONS_INIT    = 25    # questions par thème au 1er démarrage (25 max par appel)
GROQ_QUESTIONS_DAILY   = 25    # questions par thème par session planifiée
# Horaires de génération automatique (heure UTC)
GROQ_SCHEDULE_HOURS    = [3, 6, 12, 18]

# ── Tournoi ──
TOURNOI_TIMEOUT_INSCR  = 600   # 10 min pour les inscriptions
TOURNOI_DUEL_QUESTIONS = 5

# ── Quêtes ──
QUETES_JOURNALIERES = [
    {"id": "q_msg_10",  "label": "Bavard",       "desc": "Envoie 10 messages",       "type": "messages",       "cible": 10, "xp": 50,  "freq": "jour"},
    {"id": "q_quiz_3",  "label": "Quiz du jour", "desc": "Gagne 3 quiz",             "type": "quiz_gagnes",    "cible": 3,  "xp": 100, "freq": "jour"},
    {"id": "q_duel_1",  "label": "Guerrier",     "desc": "Remporte 1 duel",          "type": "combats_gagnes", "cible": 1,  "xp": 80,  "freq": "jour"},
    {"id": "q_daily",   "label": "Discipline",   "desc": "Fais ton /daily",          "type": "daily",          "cible": 1,  "xp": 30,  "freq": "jour"},
]
QUETES_HEBDOMADAIRES = [
    {"id": "q_msg_100", "label": "Hyper actif",  "desc": "100 messages cette semaine",  "type": "messages",       "cible": 100, "xp": 300, "freq": "semaine"},
    {"id": "q_quiz_15", "label": "Master Quiz",  "desc": "15 quiz gagnés cette semaine","type": "quiz_gagnes",    "cible": 15,  "xp": 500, "freq": "semaine"},
    {"id": "q_duel_5",  "label": "Conquérant",   "desc": "5 duels gagnés cette semaine","type": "combats_gagnes", "cible": 5,   "xp": 400, "freq": "semaine"},
    {"id": "q_streak7", "label": "Fidèle",       "desc": "Streak de 7 jours",           "type": "streak",         "cible": 7,   "xp": 600, "freq": "semaine"},
]
TOUTES_QUETES = QUETES_JOURNALIERES + QUETES_HEBDOMADAIRES

# ── Succès ──
SUCCES_LIST = [
    {"id": "s_first",   "emoji": "🌱", "label": "Premier pas",    "desc": "Premier message envoyé"},
    {"id": "s_msg100",  "emoji": "💬", "label": "Bavard",         "desc": "100 messages envoyés"},
    {"id": "s_msg1000", "emoji": "🗣️", "label": "Orateur",        "desc": "1000 messages envoyés"},
    {"id": "s_quiz10",  "emoji": "🎯", "label": "Quiz Rookie",    "desc": "10 quiz gagnés"},
    {"id": "s_quiz100", "emoji": "🏹", "label": "Quiz Master",    "desc": "100 quiz gagnés"},
    {"id": "s_duel10",  "emoji": "⚔️", "label": "Guerrier",       "desc": "10 duels gagnés"},
    {"id": "s_duel50",  "emoji": "🗡️", "label": "Champion",       "desc": "50 duels gagnés"},
    {"id": "s_streak7", "emoji": "🔥", "label": "Semaine de feu", "desc": "Streak de 7 jours"},
    {"id": "s_streak30","emoji": "💎", "label": "Mois parfait",   "desc": "Streak de 30 jours"},
    {"id": "s_rang_s",  "emoji": "💛", "label": "Élite",          "desc": "Rang S atteint"},
    {"id": "s_rang_sss","emoji": "♦️", "label": "Maître",         "desc": "Rang SSS atteint"},
    {"id": "s_rang_nat","emoji": "🌐", "label": "Légende",        "desc": "Rang NATION atteint"},
]

_COLONNES_MEMBRES = frozenset({
    "username", "xp", "xp_semaine", "rang", "messages",
    "quiz_gagnes", "combats_gagnes", "titre", "titres_possedes",
    "dernier_message", "dernier_quiz", "dernier_daily",
    "dernier_give", "dernier_combat", "dernier_roulette", "streak", "langue",
    "succes", "coffres_ouverts"
})
# ─────────────────────────────────────────
# 🗄️ BASE DE DONNÉES (Supabase / PostgreSQL)
# ─────────────────────────────────────────

USE_POSTGRES = bool(DATABASE_URL and DATABASE_URL.startswith("postgres"))

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras

    @contextmanager
    def get_db():
        con = psycopg2.connect(DATABASE_URL)
        con.autocommit = False
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _row_to_dict(row, cursor):
        if row is None:
            return None
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))

    def _rows_to_dicts(rows, cursor):
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, r)) for r in rows]

    PH = "%s"   # placeholder PostgreSQL

else:
    import sqlite3

    DB_FILE = "otaku_wars.db"

    @contextmanager
    def get_db():
        con = sqlite3.connect(DB_FILE, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _row_to_dict(row, cursor=None):
        return dict(row) if row else None

    def _rows_to_dicts(rows, cursor=None):
        return [dict(r) for r in rows]

    PH = "?"    # placeholder SQLite


def _fetchone(con, sql, params=()):
    cur = con.cursor()
    if USE_POSTGRES:
        cur.execute(sql.replace("?", "%s"), params)
        row = cur.fetchone()
        return _row_to_dict(row, cur)
    else:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def _fetchall(con, sql, params=()):
    cur = con.cursor()
    if USE_POSTGRES:
        cur.execute(sql.replace("?", "%s"), params)
        rows = cur.fetchall()
        return _rows_to_dicts(rows, cur)
    else:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _execute(con, sql, params=()):
    cur = con.cursor()
    if USE_POSTGRES:
        cur.execute(sql.replace("?", "%s"), params)
    else:
        cur.execute(sql, params)


def init_db():
    with get_db() as con:
        if USE_POSTGRES:
            _execute(con, """CREATE TABLE IF NOT EXISTS private_users (
                user_id   BIGINT PRIMARY KEY,
                username  TEXT,
                created_at TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS clans (
                clan_id       BIGSERIAL PRIMARY KEY,
                chat_id       BIGINT,
                nom           TEXT    NOT NULL,
                chef_id       BIGINT,
                points        INTEGER DEFAULT 0,
                created_at    TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS membres (
                user_id         BIGINT,
                chat_id         BIGINT,
                username        TEXT,
                xp              INTEGER DEFAULT 0,
                xp_semaine      INTEGER DEFAULT 0,
                rang            TEXT    DEFAULT 'E',
                messages        INTEGER DEFAULT 0,
                quiz_gagnes     INTEGER DEFAULT 0,
                combats_gagnes  INTEGER DEFAULT 0,
                titre           TEXT    DEFAULT '',
                dernier_message TEXT,
                dernier_quiz    TEXT,
                dernier_daily   TEXT,
                dernier_give    TEXT,
                streak          INTEGER DEFAULT 0,
                langue          TEXT    DEFAULT 'fr',
                clan_id         BIGINT  DEFAULT NULL,
                titres_possedes   TEXT    DEFAULT '',
                dernier_combat    TEXT,
                dernier_roulette  TEXT,
                succes            TEXT    DEFAULT '',
                coffres_ouverts   INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS clan_wars (
                id              SERIAL PRIMARY KEY,
                challenger_id   BIGINT,
                defender_id     BIGINT,
                pts_challenger  INTEGER DEFAULT 0,
                pts_defender    INTEGER DEFAULT 0,
                statut          TEXT    DEFAULT 'active',
                fin_at          TEXT,
                nom_challenger  TEXT,
                nom_defender    TEXT
            )""")
            # Tables v8
            _execute(con, """CREATE TABLE IF NOT EXISTS quetes_progress (
                user_id   BIGINT,
                chat_id   BIGINT,
                quete_id  TEXT,
                progress  INTEGER DEFAULT 0,
                done      INTEGER DEFAULT 0,
                jour      TEXT,
                PRIMARY KEY (user_id, chat_id, quete_id, jour)
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS tournois (
                id           BIGSERIAL PRIMARY KEY,
                chat_id      BIGINT,
                createur_id  BIGINT,
                statut       TEXT DEFAULT 'inscription',
                participants TEXT DEFAULT '[]',
                bracket      TEXT DEFAULT '[]',
                round_actuel INTEGER DEFAULT 0,
                created_at   TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS transferts_pending (
                id          SERIAL PRIMARY KEY,
                donneur_id  BIGINT,
                receveur_id BIGINT,
                chat_id     BIGINT,
                montant     INTEGER,
                montant_net INTEGER,
                taxe        INTEGER,
                created_at  TEXT
            )""")
            # Migrations PostgreSQL — ajout colonnes v8/v9
            for col_pg, dflt_pg in [
                ("titres_possedes",  "TEXT DEFAULT ''"),
                ("dernier_combat",   "TEXT"),
                ("dernier_roulette", "TEXT"),
                ("succes",           "TEXT DEFAULT ''"),
                ("coffres_ouverts",  "INTEGER DEFAULT 0"),
            ]:
                try:
                    _execute(con, f"ALTER TABLE membres ADD COLUMN IF NOT EXISTS {col_pg} {dflt_pg}")
                except Exception:
                    pass
            # Migrations tables v8/v9
            try:
                _execute(con, """CREATE TABLE IF NOT EXISTS quetes_progress (
                    user_id   BIGINT, chat_id BIGINT, quete_id TEXT,
                    progress  INTEGER DEFAULT 0, done INTEGER DEFAULT 0, jour TEXT,
                    PRIMARY KEY (user_id, chat_id, quete_id, jour))""")
            except Exception: pass
            try:
                _execute(con, """CREATE TABLE IF NOT EXISTS tournois (
                    id BIGSERIAL PRIMARY KEY, chat_id BIGINT, createur_id BIGINT,
                    statut TEXT DEFAULT 'inscription', participants TEXT DEFAULT '[]',
                    bracket TEXT DEFAULT '[]', round_actuel INTEGER DEFAULT 0, created_at TEXT)""")
            except Exception: pass
            # Table questions générées par IA (v10)
            try:
                _execute(con, """CREATE TABLE IF NOT EXISTS questions_ia (
                    id         BIGSERIAL PRIMARY KEY,
                    niveau     TEXT NOT NULL,
                    question   TEXT NOT NULL UNIQUE,
                    reponses   TEXT NOT NULL,
                    bonne      INTEGER NOT NULL,
                    created_at TEXT
                )""")
            except Exception: pass
        else:
            _execute(con, """CREATE TABLE IF NOT EXISTS private_users (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                created_at TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS clans (
                clan_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id    INTEGER,
                nom        TEXT    NOT NULL,
                chef_id    INTEGER,
                points     INTEGER DEFAULT 0,
                created_at TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS membres (
                user_id         INTEGER,
                chat_id         INTEGER,
                username        TEXT,
                xp              INTEGER DEFAULT 0,
                xp_semaine      INTEGER DEFAULT 0,
                rang            TEXT    DEFAULT 'E',
                messages        INTEGER DEFAULT 0,
                quiz_gagnes     INTEGER DEFAULT 0,
                combats_gagnes  INTEGER DEFAULT 0,
                titre           TEXT    DEFAULT '',
                dernier_message TEXT,
                dernier_quiz    TEXT,
                dernier_daily   TEXT,
                dernier_give    TEXT,
                streak          INTEGER DEFAULT 0,
                langue          TEXT    DEFAULT 'fr',
                clan_id         INTEGER DEFAULT NULL,
                titres_possedes TEXT    DEFAULT '',
                dernier_combat  TEXT,
                PRIMARY KEY (user_id, chat_id)
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS clan_wars (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                challenger_id   INTEGER,
                defender_id     INTEGER,
                pts_challenger  INTEGER DEFAULT 0,
                pts_defender    INTEGER DEFAULT 0,
                statut          TEXT    DEFAULT 'active',
                fin_at          TEXT,
                nom_challenger  TEXT,
                nom_defender    TEXT
            )""")
            # Tables v8
            _execute(con, """CREATE TABLE IF NOT EXISTS quetes_progress (
                user_id   INTEGER,
                chat_id   INTEGER,
                quete_id  TEXT,
                progress  INTEGER DEFAULT 0,
                done      INTEGER DEFAULT 0,
                jour      TEXT,
                PRIMARY KEY (user_id, chat_id, quete_id, jour)
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS tournois (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id      INTEGER,
                createur_id  INTEGER,
                statut       TEXT DEFAULT 'inscription',
                participants TEXT DEFAULT '[]',
                bracket      TEXT DEFAULT '[]',
                round_actuel INTEGER DEFAULT 0,
                created_at   TEXT
            )""")
            _execute(con, """CREATE TABLE IF NOT EXISTS transferts_pending (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                donneur_id  INTEGER,
                receveur_id INTEGER,
                chat_id     INTEGER,
                montant     INTEGER,
                montant_net INTEGER,
                taxe        INTEGER,
                created_at  TEXT
            )""")
            # Table questions générées par IA (v10)
            _execute(con, """CREATE TABLE IF NOT EXISTS questions_ia (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                niveau     TEXT NOT NULL,
                question   TEXT NOT NULL UNIQUE,
                reponses   TEXT NOT NULL,
                bonne      INTEGER NOT NULL,
                created_at TEXT
            )""")
            # Migrations SQLite
            for col, dflt in [
                ("dernier_quiz",    "TEXT"),
                ("langue",          "TEXT DEFAULT 'fr'"),
                ("xp_semaine",      "INTEGER DEFAULT 0"),
                ("titre",           "TEXT DEFAULT ''"),
                ("dernier_daily",   "TEXT"),
                ("dernier_give",    "TEXT"),
                ("streak",          "INTEGER DEFAULT 0"),
                ("clan_id",         "INTEGER DEFAULT NULL"),
                ("titres_possedes", "TEXT DEFAULT ''"),
                ("dernier_combat",    "TEXT"),
                ("dernier_roulette",  "TEXT"),
                ("succes",            "TEXT DEFAULT ''"),
                ("coffres_ouverts",   "INTEGER DEFAULT 0"),
            ]:
                try:
                    _execute(con, f"ALTER TABLE membres ADD COLUMN {col} {dflt}")
                except Exception:
                    pass


# ─────────────────────────────────────────
# 🗃️ FONCTIONS DB
# ─────────────────────────────────────────

def get_clan(chat_id: int):
    """Compatibilité V5 — retourne le clan lié à un groupe (territoire)"""
    with get_db() as con:
        return _fetchone(con, "SELECT * FROM clans WHERE chat_id=?", (chat_id,))


def get_clan_by_id(clan_id: int):
    with get_db() as con:
        return _fetchone(con, "SELECT * FROM clans WHERE clan_id=?", (clan_id,))


def get_clan_of_user(user_id: int, chat_id: int):
    """Retourne le clan d'un joueur via sa colonne clan_id dans membres"""
    with get_db() as con:
        row = _fetchone(con, "SELECT clan_id FROM membres WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        if not row or not row.get("clan_id"):
            return None
        return _fetchone(con, "SELECT * FROM clans WHERE clan_id=?", (row["clan_id"],))


def get_clan_member_count(clan_id: int) -> int:
    with get_db() as con:
        row = _fetchone(con, "SELECT COUNT(DISTINCT user_id) AS c FROM membres WHERE clan_id=?", (clan_id,))
        return (row["c"] if row else 0) or 0


def create_clan_db(nom: str, chef_id: int, chat_id: int):
    with get_db() as con:
        if USE_POSTGRES:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO clans (chat_id, nom, chef_id, points, created_at) VALUES (%s,%s,%s,0,%s) RETURNING clan_id",
                (chat_id, nom, chef_id, datetime.now().isoformat())
            )
            return cur.fetchone()[0]
        else:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO clans (chat_id, nom, chef_id, points, created_at) VALUES (?,?,?,0,?)",
                (chat_id, nom, chef_id, datetime.now().isoformat())
            )
            return cur.lastrowid


def get_membre_db(user_id: int, chat_id: int, username: str = "Otaku") -> dict:
    with get_db() as con:
        row = _fetchone(con, "SELECT * FROM membres WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        if not row:
            _execute(con, """INSERT INTO membres
                (user_id,chat_id,username,xp,xp_semaine,rang,messages,
                 quiz_gagnes,combats_gagnes,titre,dernier_message,
                 dernier_quiz,dernier_daily,dernier_give,streak,langue)
                VALUES (?,?,?,0,0,'E',0,0,0,'',NULL,NULL,NULL,NULL,0,'fr')""",
                (user_id, chat_id, username))
            row = _fetchone(con, "SELECT * FROM membres WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    # Normaliser les valeurs None pour éviter les crashes
    if row:
        row = dict(row)
        row.setdefault("xp", 0);            row["xp"] = row["xp"] or 0
        row.setdefault("xp_semaine", 0);    row["xp_semaine"] = row["xp_semaine"] or 0
        row.setdefault("rang", "E");         row["rang"] = row["rang"] or "E"
        row.setdefault("messages", 0);       row["messages"] = row["messages"] or 0
        row.setdefault("quiz_gagnes", 0);    row["quiz_gagnes"] = row["quiz_gagnes"] or 0
        row.setdefault("combats_gagnes", 0); row["combats_gagnes"] = row["combats_gagnes"] or 0
        row.setdefault("streak", 0);         row["streak"] = row["streak"] or 0
        row.setdefault("titre", "");         row["titre"] = row["titre"] or ""
        row.setdefault("succes", "");        row["succes"] = row["succes"] or ""
        row.setdefault("titres_possedes", ""); row["titres_possedes"] = row["titres_possedes"] or ""
        row.setdefault("coffres_ouverts", 0); row["coffres_ouverts"] = row["coffres_ouverts"] or 0
        row.setdefault("langue", "fr");      row["langue"] = row["langue"] or "fr"
    return row


def get_user_lang(user_id: int, chat_id: int) -> str:
    with get_db() as con:
        row = _fetchone(con, "SELECT langue FROM membres WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    return (row["langue"] if row and row.get("langue") else "fr")


def update_membre(user_id: int, chat_id: int, **kwargs):
    invalid = set(kwargs) - _COLONNES_MEMBRES
    if invalid:
        raise ValueError(f"Colonnes non autorisées : {invalid}")
    if not kwargs:
        return
    sets   = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [user_id, chat_id]
    with get_db() as con:
        _execute(con, f"UPDATE membres SET {sets} WHERE user_id=? AND chat_id=?", values)


def update_clan_points(chat_id: int, pts: int):
    """Compatibilité V5 — met à jour les points du clan lié au groupe"""
    with get_db() as con:
        _execute(con, "UPDATE clans SET points=points+? WHERE chat_id=?", (pts, chat_id))


def get_active_clanwar(chat_id: int):
    with get_db() as con:
        return _fetchone(con,
            "SELECT * FROM clan_wars WHERE (challenger_id=? OR defender_id=?) AND statut='active'",
            (chat_id, chat_id))


def add_clanwar_points(chat_id: int, pts: int):
    war = get_active_clanwar(chat_id)
    if not war:
        return
    with get_db() as con:
        if war["challenger_id"] == chat_id:
            _execute(con, "UPDATE clan_wars SET pts_challenger=pts_challenger+? WHERE id=?", (pts, war["id"]))
        else:
            _execute(con, "UPDATE clan_wars SET pts_defender=pts_defender+? WHERE id=?", (pts, war["id"]))


def terminer_clanwar(war: dict) -> dict:
    pts_c  = war["pts_challenger"]
    pts_d  = war["pts_defender"]
    c_id   = war["challenger_id"]
    d_id   = war["defender_id"]
    clan_c = get_clan(c_id)
    clan_d = get_clan(d_id)
    nom_c  = clan_c["nom"] if clan_c else "Clan inconnu"
    nom_d  = clan_d["nom"] if clan_d else "Clan inconnu"
    egalite = False
    if pts_c > pts_d:
        update_clan_points(c_id, POINTS_CLANWAR_VICTOIRE)
        vainqueur_nom, perdant_nom, pts_v, pts_p = nom_c, nom_d, pts_c, pts_d
    elif pts_d > pts_c:
        update_clan_points(d_id, POINTS_CLANWAR_VICTOIRE)
        vainqueur_nom, perdant_nom, pts_v, pts_p = nom_d, nom_c, pts_d, pts_c
    else:
        vainqueur_nom = perdant_nom = None
        pts_v = pts_p = pts_c
        egalite = True
    with get_db() as con:
        _execute(con, "UPDATE clan_wars SET statut='terminée' WHERE id=?", (war["id"],))
    return {"egalite": egalite, "vainqueur_nom": vainqueur_nom, "perdant_nom": perdant_nom,
            "pts_v": pts_v, "pts_p": pts_p, "nom_c": nom_c, "nom_d": nom_d,
            "pts_c": pts_c, "pts_d": pts_d}


# ─────────────────────────────────────────
# 🌍 TRADUCTIONS FR / EN
# ─────────────────────────────────────────

T = {
    "choose_lang":   {"fr": "🌍 Choisissez votre langue :", "en": "🌍 Choose your language:"},
    "lang_set":      {"fr": "🇫🇷 Langue définie sur *Français* ! Tape /start.", "en": "🇬🇧 Language set to *English*! Type /start."},
    "welcome": {
        "fr": (
            "🌍 *Bienvenue dans {univers} v5 !*\n\n{clan_info}\n\n"
            "📋 *Commandes :*\n"
            "• /rang — Profil & XP\n• /quiz — Quiz Normal (+{xp_n} XP)\n"
            "• /quizhc — Quiz Hardcore 💀 (+{xp_h} XP)\n• /war — Duel\n"
            "• /daily — Récompense quotidienne 🎁\n• /give @pseudo montant — Transférer XP\n"
            "• /shop — Boutique titres\n• /top — Top clan\n• /globalrank — 🌍 Top mondial\n"
            "• /worldtop — 🏆 Top clans\n• /mystere — Quiz mystère ✨\n• /aide — Aide complète"
        ),
        "en": (
            "🌍 *Welcome to {univers} v5!*\n\n{clan_info}\n\n"
            "📋 *Commands:*\n"
            "• /rang — Profile & XP\n• /quiz — Normal Quiz (+{xp_n} XP)\n"
            "• /quizhc — Hardcore Quiz 💀 (+{xp_h} XP)\n• /war — Duel\n"
            "• /daily — Daily reward 🎁\n• /give @user amount — Transfer XP\n"
            "• /shop — Title shop\n• /top — Clan top\n• /globalrank — 🌍 World top\n"
            "• /worldtop — 🏆 Top clans\n• /mystere — Mystery quiz ✨\n• /aide — Full help"
        ),
    },
    "no_clan_info":  {"fr": "⚠️ Pas de clan — `/createclan NomDuClan`", "en": "⚠️ No clan — `/createclan ClanName`"},
    "has_clan_info": {"fr": "⚔️ Clan : *{nom}*", "en": "⚔️ Clan: *{nom}*"},
    "profile_title": {"fr": "⚔️ *Profil de @{user}*", "en": "⚔️ *Profile of @{user}*"},
    "profile_body": {
        "fr": (
            "{emoji} Rang : *{label}*\n"
            "{badge_titre}\n"
            "⚡ Clan : *{clan}*\n🌍 Rang mondial : *#{rang_global}*\n"
            "✨ XP : *{xp}* | 📅 XP semaine : *{xp_semaine}*\n"
            "🔥 Streak : *{streak} jour(s)*\n"
            "💬 Messages : *{messages}*\n🎯 Quiz gagnés : *{quiz}*\n"
            "⚔️ Combats gagnés : *{combats}*\n\n{barre_info}"
        ),
        "en": (
            "{emoji} Rank: *{label}*\n"
            "{badge_titre}\n"
            "⚡ Clan: *{clan}*\n🌍 World rank: *#{rang_global}*\n"
            "✨ XP: *{xp}* | 📅 Weekly XP: *{xp_semaine}*\n"
            "🔥 Streak: *{streak} day(s)*\n"
            "💬 Messages: *{messages}*\n🎯 Quizzes won: *{quiz}*\n"
            "⚔️ Battles won: *{combats}*\n\n{barre_info}"
        ),
    },
    "rank_max":      {"fr": "🏆 *Rang maximum — NATION LÉGENDE !*", "en": "🏆 *Maximum rank — NATION LEGEND!*"},
    "no_clan":       {"fr": "Aucun", "en": "None"},
    "top_title":     {"fr": "🏆 *TOP — {clan}*\n\n", "en": "🏆 *TOP — {clan}*\n\n"},
    "no_members":    {"fr": "Aucun membre enregistré !", "en": "No members registered!"},
    "global_title":  {"fr": "🌍 *TOP JOUEURS MONDIAL — {univers}*\n_Tous clans confondus_\n\n", "en": "🌍 *WORLD TOP PLAYERS — {univers}*\n_All clans combined_\n\n"},
    "no_players":    {"fr": "🌍 Aucun joueur enregistré !", "en": "🌍 No players registered!"},
    "no_clan_tag":   {"fr": "[Sans clan]", "en": "[No clan]"},
    "worldtop_title":{"fr": "🌍 *CLASSEMENT CLANS — {univers}*\n\n", "en": "🌍 *CLAN RANKINGS — {univers}*\n\n"},
    "no_clans":      {"fr": "🌍 Aucun clan enregistré !", "en": "🌍 No clans registered!"},
    "quiz_already":  {"fr": "⚠️ Un quiz est déjà en cours !", "en": "⚠️ A quiz is already running!"},
    "quiz_cooldown": {"fr": "⏳ Cooldown ! Attends encore *{reste}s*.", "en": "⏳ Cooldown! Wait *{reste}s* more."},
    "quiz_normal_header": {"fr": "🎌 *QUIZ NORMAL* (+{xp} XP)\n\n{question}\n\n⏱️ {timeout}s", "en": "🎌 *NORMAL QUIZ* (+{xp} XP)\n\n{question}\n\n⏱️ {timeout}s"},
    "quiz_hc_header":     {"fr": "💀 *QUIZ HARDCORE* (+{xp} XP)\n_Pour vrais otakus_\n\n{question}\n\n⏱️ {timeout}s", "en": "💀 *HARDCORE QUIZ* (+{xp} XP)\n_For true otakus_\n\n{question}\n\n⏱️ {timeout}s"},
    "quiz_mystere_header":{"fr": "✨ *QUIZ MYSTÈRE* (+{xp} XP)\n_Surprise !_\n\n{question}\n\n⏱️ {timeout}s", "en": "✨ *MYSTERY QUIZ* (+{xp} XP)\n_Surprise!_\n\n{question}\n\n⏱️ {timeout}s"},
    "quiz_timeout_msg":   {"fr": "⏰ *Temps écoulé !* Quiz annulé.", "en": "⏰ *Time's up!* Quiz cancelled."},
    "quiz_correct":       {"fr": "✅ *Bonne réponse, @{user} !* [{label}]\n+{xp} XP | +{pts} pts clan{montee}", "en": "✅ *Correct, @{user}!* [{label}]\n+{xp} XP | +{pts} clan pts{montee}"},
    "quiz_wrong":         {"fr": "❌ *Mauvaise réponse, @{user}...*\nBonne réponse : *{bonne}*", "en": "❌ *Wrong, @{user}...*\nCorrect answer: *{bonne}*"},
    "quiz_expired":       {"fr": "⚠️ Ce quiz est terminé !", "en": "⚠️ This quiz is over!"},
    "quiz_label_normal":  {"fr": "🎌 NORMAL", "en": "🎌 NORMAL"},
    "quiz_label_hc":      {"fr": "💀 HARDCORE", "en": "💀 HARDCORE"},
    "quiz_label_mystere": {"fr": "✨ MYSTÈRE", "en": "✨ MYSTERY"},
    "rank_up":            {"fr": "\n🎉 *MONTÉE DE RANG → {label}* !", "en": "\n🎉 *RANK UP → {label}*!"},
    "rank_up_msg":        {"fr": "🎉 *MONTÉE DE RANG !*\n\n*@{user}* → {emoji} *{label}* !\n⚡ +{pts} pts clan !", "en": "🎉 *RANK UP!*\n\n*@{user}* → {emoji} *{label}*!\n⚡ +{pts} clan pts!"},
    "war_no_reply":       {"fr": "⚔️ *Réponds au message* d'un membre puis `/war` !", "en": "⚔️ *Reply to a member's message* then `/war`!"},
    "war_self":           {"fr": "😅 Tu ne peux pas te battre toi-même !", "en": "😅 You can't fight yourself!"},
    "war_vs_bot":         {"fr": "🤖 Tu ne peux pas défier un bot !", "en": "🤖 You can't challenge a bot!"},
    "war_already":        {"fr": "⚠️ Un combat est déjà en cours !", "en": "⚠️ A battle is already ongoing!"},
    "war_expired":        {"fr": "⚠️ Ce combat a expiré !", "en": "⚠️ This battle has expired!"},
    "clan_info": {
        "fr": ("⚔️ *CLAN : {nom}*\n🌍 {univers}\n\n🏆 Rang : *#{rang} / {total}*\n"
               "⚡ Points : *{pts}*\n👥 Membres : *{membres}*\n📅 Fondé : *{date}*{war_info}"),
        "en": ("⚔️ *CLAN: {nom}*\n🌍 {univers}\n\n🏆 Rank: *#{rang} / {total}*\n"
               "⚡ Points: *{pts}*\n👥 Members: *{membres}*\n📅 Founded: *{date}*{war_info}"),
    },
    "clan_war_ongoing":   {"fr": "\n\n⚔️ *CLAN WAR EN COURS !* `/warstat`", "en": "\n\n⚔️ *CLAN WAR IN PROGRESS!* `/warstat`"},
    "no_clan_group":      {"fr": "❌ Pas de clan. `/createclan NomDuClan`", "en": "❌ No clan. `/createclan ClanName`"},
    "clan_id_msg":        {"fr": "🆔 *ID clan :*\n`{cid}`\n\nDonne à l'ennemi :\n`/clanwar {cid}`", "en": "🆔 *Clan ID:*\n`{cid}`\n\nGive to enemy:\n`/clanwar {cid}`"},
    "no_clan_here":       {"fr": "❌ Ce groupe n'a pas de clan.", "en": "❌ This group has no clan."},
    "group_only":         {"fr": "❌ Commande à utiliser dans un groupe.", "en": "❌ Group only command."},
    "admin_only":         {"fr": "🚫 Réservé aux admins du groupe.", "en": "🚫 Group admins only."},
    "not_enough_members": {"fr": "❌ *{nb} membres*. Minimum *{min}*. Manque *{diff}*. 💪", "en": "❌ *{nb} members*. Need *{min}*. Missing *{diff}*. 💪"},
    "createclan_usage":   {"fr": "❌ Usage : `/createclan NomDuClan`", "en": "❌ Usage: `/createclan ClanName`"},
    "clan_exists":        {"fr": "⚠️ Clan existant. Utilise `/renameclan`.", "en": "⚠️ Clan exists. Use `/renameclan`."},
    "clan_created":       {"fr": "🎉 *Clan créé !*\n\n⚔️ *{nom}* rejoint {univers} !\n👥 {nb} membres\n\nTape `/worldtop` 🔥", "en": "🎉 *Clan created!*\n\n⚔️ *{nom}* joined {univers}!\n👥 {nb} members\n\nType `/worldtop` 🔥"},
    "renameclan_usage":   {"fr": "❌ Usage : `/renameclan NouveauNom`", "en": "❌ Usage: `/renameclan NewName`"},
    "renameclan_done":    {"fr": "✅ Clan renommé en *{nom}* !", "en": "✅ Clan renamed to *{nom}*!"},
    "clanwar_already":    {"fr": "⚠️ Ton clan est déjà en guerre ! `/warstat`", "en": "⚠️ Already at war! `/warstat`"},
    "clanwar_usage":      {"fr": "❌ Usage : `/clanwar ID_CLAN`\nL'admin ennemi tape `/clanid`.", "en": "❌ Usage: `/clanwar CLAN_ID`\nEnemy admin types `/clanid`."},
    "clanwar_invalid_id": {"fr": "❌ ID invalide.", "en": "❌ Invalid ID."},
    "clanwar_self":       {"fr": "😅 Tu ne peux pas attaquer ton propre clan !", "en": "😅 You can't attack your own clan!"},
    "clanwar_not_found":  {"fr": "❌ Clan introuvable.", "en": "❌ Clan not found."},
    "clanwar_enemy_busy": {"fr": "⚠️ *{nom}* est déjà en guerre !", "en": "⚠️ *{nom}* is already at war!"},
    "clanwar_declared":   {"fr": "🏴 *CLAN WAR !*\n\n⚔️ *{c1}* VS *{c2}*\n⏱️ {h}h\n🎯 Quiz/Combat = +{pts} pts guerre !\n\nTape `/warstat` 🔥", "en": "🏴 *CLAN WAR!*\n\n⚔️ *{c1}* VS *{c2}*\n⏱️ {h}h\n🎯 Quiz/Battle = +{pts} war pts!\n\nType `/warstat` 🔥"},
    "clanwar_attacked":   {"fr": "🏴 *VOUS ÊTES ATTAQUÉS !*\n*{nom}* vous a déclaré la guerre !\n⏱️ {h}h. Tape `/warstat` ⚔️", "en": "🏴 *UNDER ATTACK!*\n*{nom}* declared war!\n⏱️ {h}h. Type `/warstat` ⚔️"},
    "warstat_none":       {"fr": "⚔️ Pas de Clan War active.", "en": "⚔️ No active Clan War."},
    "warstat_live":       {"fr": "🏴 *CLAN WAR EN COURS !*\n\n🔵 *{c1}* : {p1} pts\n🔴 *{c2}* : {p2} pts\n\n`{barre}`\n{leader}\n\n⏱️ *{h}h {m}min* restants", "en": "🏴 *CLAN WAR!*\n\n🔵 *{c1}*: {p1} pts\n🔴 *{c2}*: {p2} pts\n\n`{barre}`\n{leader}\n\n⏱️ *{h}h {m}min* left"},
    "warstat_leader":     {"fr": "⚡ *{nom}* mène !", "en": "⚡ *{nom}* is leading!"},
    "warstat_equal":      {"fr": "⚡ *Égalité !*", "en": "⚡ *Tied!*"},
    "warstat_end_win":    {"fr": "🏆 *WAR TERMINÉE !*\n\n🥇 *{winner}* ({pv} pts)\n💀 *{loser}* ({pl} pts)\n\n*{winner}* gagne *+{bonus} pts* ! 🎉", "en": "🏆 *WAR OVER!*\n\n🥇 *{winner}* ({pv} pts)\n💀 *{loser}* ({pl} pts)\n\n*{winner}* earns *+{bonus} pts*! 🎉"},
    "warstat_end_draw":   {"fr": "🤝 *WAR TERMINÉE — ÉGALITÉ !*\n*{c1}* : {p1} pts\n*{c2}* : {p2} pts", "en": "🤝 *WAR OVER — DRAW!*\n*{c1}*: {p1} pts\n*{c2}*: {p2} pts"},
    "warhistory_none":    {"fr": "Aucun historique.", "en": "No history."},
    "warhistory_title":   {"fr": "📜 *HISTORIQUE WARS — {nom}*\n\n", "en": "📜 *WAR HISTORY — {nom}*\n\n"},
    "superadmin_only":    {"fr": "🚫 Réservé aux super-admins.", "en": "🚫 Super-admins only."},
    "deleteclan_invalid": {"fr": "❌ ID invalide. `/deleteclan ID`", "en": "❌ Invalid ID. `/deleteclan ID`"},
    "deleteclan_notfound":{"fr": "❌ Clan introuvable.", "en": "❌ Clan not found."},
    "deleteclan_done":    {"fr": "🗑️ *Clan supprimé !*\n⚔️ *{nom}* retiré de {univers}.", "en": "🗑️ *Clan deleted!*\n⚔️ *{nom}* removed from {univers}."},
    "admin_or_superadmin":{"fr": "🚫 Réservé aux admins ou super-admins.", "en": "🚫 Admins or super-admins only."},
    "stats_msg":          {"fr": "📊 *STATS — {univers}*\n\n⚔️ Clans : *{tc}*\n👥 Joueurs : *{tm}*\n✨ XP total : *{tx}*\n🏴 Wars : *{tw}*", "en": "📊 *STATS — {univers}*\n\n⚔️ Clans: *{tc}*\n👥 Players: *{tm}*\n✨ Total XP: *{tx}*\n🏴 Wars: *{tw}*"},
    "givexp_usage":       {"fr": "❌ Usage : `/givexp @pseudo montant`", "en": "❌ Usage: `/givexp @username amount`"},
    "givexp_invalid":     {"fr": "❌ Montant invalide.", "en": "❌ Invalid amount."},
    "givexp_notfound":    {"fr": "❌ *{pseudo}* introuvable.", "en": "❌ *{pseudo}* not found."},
    "givexp_done":        {"fr": "✅ *{signe}{montant} XP* à *{user}* ! Total : *{total} XP*{montee}", "en": "✅ *{signe}{montant} XP* to *{user}*! Total: *{total} XP*{montee}"},
    "resetxp_usage":      {"fr": "❌ Usage : `/resetxp @pseudo`", "en": "❌ Usage: `/resetxp @username`"},
    "resetxp_done":       {"fr": "🔄 XP de *{user}* remis à zéro.", "en": "🔄 *{user}*'s XP reset."},
    "removemembre_usage": {"fr": "❌ Usage : `/removemembre @pseudo`", "en": "❌ Usage: `/removemembre @username`"},
    "removemembre_done":  {"fr": "🗑️ *{user}* supprimé.", "en": "🗑️ *{user}* deleted."},
    "notfound":           {"fr": "❌ *{pseudo}* introuvable.", "en": "❌ *{pseudo}* not found."},
    "listclans_none":     {"fr": "Aucun clan.", "en": "No clans."},
    "listclans_title":    {"fr": "📋 *LISTE DES CLANS*\n\n", "en": "📋 *CLAN LIST*\n\n"},
    "listclans_footer":   {"fr": "_/deleteclan ID pour supprimer_", "en": "_/deleteclan ID to delete_"},
    # Daily
    "daily_already":      {"fr": "⏳ Déjà réclamé ! Reviens dans *{reste}h {min}min*.", "en": "⏳ Already claimed! Come back in *{reste}h {min}min*."},
    "daily_reward":       {"fr": "🎁 *RÉCOMPENSE QUOTIDIENNE !*\n\n@{user} reçoit *+{xp} XP* !\n🔥 Streak : *{streak} jour(s)*{bonus_txt}\n✨ Total : *{total} XP*", "en": "🎁 *DAILY REWARD!*\n\n@{user} receives *+{xp} XP*!\n🔥 Streak: *{streak} day(s)*{bonus_txt}\n✨ Total: *{total} XP*"},
    "daily_bonus":        {"fr": "\n⚡ *Bonus streak x{mult}* appliqué !", "en": "\n⚡ *Streak bonus x{mult}* applied!"},
    # Give / Transfert
    "give_usage":         {"fr": "❌ Usage : `/give @pseudo montant`", "en": "❌ Usage: `/give @username amount`"},
    "give_zero":          {"fr": "❌ Montant supérieur à 0 requis.", "en": "❌ Amount must be greater than 0."},
    "give_not_int":       {"fr": "❌ Montant invalide.", "en": "❌ Invalid amount."},
    "give_self":          {"fr": "😅 Tu ne peux pas te donner des XP !", "en": "😅 You can't give XP to yourself!"},
    "give_not_registered":{"fr": "❌ Tu n'es pas enregistré.", "en": "❌ You're not registered."},
    "give_not_found":     {"fr": "❌ @{cible} introuvable dans ce groupe.", "en": "❌ @{cible} not found in this group."},
    "give_not_enough":    {"fr": "❌ Pas assez d'XP ! Tu as *{xp} XP*.", "en": "❌ Not enough XP! You have *{xp} XP*."},
    "give_cooldown":      {"fr": "⏳ Cooldown transfert ! Attends encore *{reste}min*.", "en": "⏳ Transfer cooldown! Wait *{reste}min*."},
    "give_pending":       {"fr": "⏳ *@{receveur}*, @{donneur} veut te transférer *{montant} XP* !\n💸 Taxe : *{taxe} XP*\n✨ Tu recevras : *{net} XP*\n\nTu acceptes ? ({timeout}s)", "en": "⏳ *@{receveur}*, @{donneur} wants to transfer *{montant} XP*!\n💸 Tax: *{taxe} XP*\n✨ You'll receive: *{net} XP*\n\nDo you accept? ({timeout}s)"},
    "give_accept_btn":    {"fr": "✅ J'accepte", "en": "✅ Accept"},
    "give_refuse_btn":    {"fr": "❌ Je refuse", "en": "❌ Decline"},
    "give_accepted":      {"fr": "✅ *Transfert accepté !*\n\n💸 *@{donneur}* → *@{receveur}*\n*{montant} XP* transférés\nTaxe : *{taxe} XP*\n@{donneur} restant : *{restant} XP*", "en": "✅ *Transfer accepted!*\n\n💸 *@{donneur}* → *@{receveur}*\n*{montant} XP* transferred\nTax: *{taxe} XP*\n@{donneur} remaining: *{restant} XP*"},
    "give_refused":       {"fr": "❌ *@{receveur}* a refusé le transfert.", "en": "❌ *@{receveur}* declined the transfer."},
    "give_timeout":       {"fr": "⏰ *@{receveur}* n'a pas répondu. Transfert annulé.", "en": "⏰ *@{receveur}* didn't respond. Transfer cancelled."},
    "give_not_yours":     {"fr": "❌ Ce n'est pas ton transfert !", "en": "❌ This is not your transfer!"},
    # Shop
    "shop_title":         {"fr": "🏪 *BOUTIQUE DE TITRES*\n_Dépense ton XP pour un titre unique !_\n\n", "en": "🏪 *TITLE SHOP*\n_Spend XP for a unique title!_\n\n"},
    "shop_not_enough":    {"fr": "❌ Pas assez d'XP ! Il te faut *{requis} XP*.", "en": "❌ Not enough XP! You need *{requis} XP*."},
    "shop_already_owned": {"fr": "✅ Tu possèdes déjà ce titre !", "en": "✅ You already own this title!"},
    "shop_bought":        {"fr": "🎉 Titre *{titre}* acheté ! -{xp} XP\nUtilise `/equip {id}` pour l'équiper.", "en": "🎉 Title *{titre}* bought! -{xp} XP\nUse `/equip {id}` to equip it."},
    "equip_done":         {"fr": "✅ Titre *{titre}* équipé !", "en": "✅ Title *{titre}* equipped!"},
    "equip_notfound":     {"fr": "❌ Titre introuvable. Tape `/shop`.", "en": "❌ Title not found. Type `/shop`."},
    # Leaderboard semaine
    "weekly_title":       {"fr": "📅 *TOP SEMAINE — {clan}*\n_Remis à zéro chaque lundi_\n\n", "en": "📅 *WEEKLY TOP — {clan}*\n_Reset every Monday_\n\n"},
    "aide_title":         {"fr": "📋 *COMMANDES — {univers} v5*\n\n", "en": "📋 *COMMANDS — {univers} v5*\n\n"},
    "aide_membres": {
        "fr": (
            "*👤 Membres :*\n"
            "• `/start` — Bienvenue\n• `/rang` — Profil & XP\n"
            "• `/top` — Top clan\n• `/globalrank` — 🌍 Top mondial\n"
            "• `/worldtop` — 🏆 Top clans\n• `/weekly` — 📅 Top semaine\n"
            "• `/quiz` — Quiz Normal (+{xp_n} XP)\n• `/quizhc` — Hardcore 💀 (+{xp_h} XP)\n"
            "• `/mystere` — Quiz mystère ✨\n• `/war` — Duel\n"
            "• `/daily` — Récompense quotidienne 🎁\n"
            "• `/give @pseudo montant` — Transférer XP 💸\n"
            "• `/shop` — Boutique titres\n• `/langue` — Changer langue\n\n"
        ),
        "en": (
            "*👤 Members:*\n"
            "• `/start` — Welcome\n• `/rang` — Profile & XP\n"
            "• `/top` — Clan top\n• `/globalrank` — 🌍 World top\n"
            "• `/worldtop` — 🏆 Top clans\n• `/weekly` — 📅 Weekly top\n"
            "• `/quiz` — Normal Quiz (+{xp_n} XP)\n• `/quizhc` — Hardcore 💀 (+{xp_h} XP)\n"
            "• `/mystere` — Mystery quiz ✨\n• `/war` — Duel\n"
            "• `/daily` — Daily reward 🎁\n"
            "• `/give @username amount` — Transfer XP 💸\n"
            "• `/shop` — Title shop\n• `/language` — Change language\n\n"
        ),
    },
    "aide_clan": {
        "fr": "*🏴 Clan :*\n• `/clan` `/clanid` `/createclan` `/renameclan`\n• `/clanwar ID` `/warstat` `/warhistory`\n\n",
        "en": "*🏴 Clan:*\n• `/clan` `/clanid` `/createclan` `/renameclan`\n• `/clanwar ID` `/warstat` `/warhistory`\n\n",
    },
    "aide_admin": {
        "fr": "*👑 Admins :*\n• `/createclan` `/renameclan` `/deleteclan` `/resetwar` `/checkbot`\n\n",
        "en": "*👑 Admins:*\n• `/createclan` `/renameclan` `/deleteclan` `/resetwar` `/checkbot`\n\n",
    },
    "aide_superadmin": {
        "fr": "*🔐 Super-admins :*\n• `/stats` `/listclans` `/givexp` `/resetxp` `/removemembre` `/broadcast`\n\n",
        "en": "*🔐 Super-admins:*\n• `/stats` `/listclans` `/givexp` `/resetxp` `/removemembre` `/broadcast`\n\n",
    },
    "aide_rangs":         {"fr": "*🏆 Rangs :*\n", "en": "*🏆 Ranks:*\n"},
}


def t(key: str, lang: str, **kwargs) -> str:
    entry = T.get(key, {})
    text  = entry.get(lang) or entry.get("fr") or f"[{key}]"
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text


# ─────────────────────────────────────────
# 🏆 RANGS
# ─────────────────────────────────────────

RANGS = [
    {"rang": "E",      "emoji": "🩶", "label": "『 Otaku • Rang E 』",         "xp_requis": 0},
    {"rang": "D",      "emoji": "🤍", "label": "『 Otaku • Rang D 』",         "xp_requis": 2_000},
    {"rang": "C",      "emoji": "💚", "label": "『 Otaku • Rang C 』",         "xp_requis": 5_000},
    {"rang": "B",      "emoji": "💙", "label": "『 Otaku • Rang B 』",         "xp_requis": 10_000},
    {"rang": "A",      "emoji": "❤️", "label": "『 Otaku • Rang A 』",         "xp_requis": 30_000},
    {"rang": "S",      "emoji": "💛", "label": "『 Otaku • Rang S 』",         "xp_requis": 70_000},
    {"rang": "SS",     "emoji": "🧡", "label": "『 Élite • Rang SS 』",        "xp_requis": 100_000},
    {"rang": "SSS",    "emoji": "♦️", "label": "『 Maître • Rang SSS 』",      "xp_requis": 250_000},
    {"rang": "NATION", "emoji": "🌐", "label": "『 ⚡ NATION • LÉGENDE ⚡ 』",  "xp_requis": 500_000},
]


def get_rang(xp: int) -> dict:
    rang_actuel = RANGS[0]
    for r in RANGS:
        if xp >= r["xp_requis"]:
            rang_actuel = r
    return rang_actuel


def xp_prochain_rang(xp: int):
    for r in RANGS:
        if xp < r["xp_requis"]:
            return r["xp_requis"], r["label"]
    return None, None


def barre_xp(xp: int, longueur: int = 10) -> str:
    ri = get_rang(xp)
    xp_next, _ = xp_prochain_rang(xp)
    if xp_next is None:
        return "█" * longueur
    base = ri["xp_requis"]
    pct  = max(0, min(longueur, int(((xp - base) / (xp_next - base)) * longueur)))
    return "█" * pct + "░" * (longueur - pct)


# ─────────────────────────────────────────
# 🏪 BOUTIQUE DE TITRES
# ─────────────────────────────────────────

SHOP_TITRES = [
    {"id": "kage",    "titre": "🌑 Kage des Ombres",      "prix": 5_000,   "emoji": "🌑"},
    {"id": "shinigami","titre": "💀 Shinigami",            "prix": 8_000,   "emoji": "💀"},
    {"id": "nakama",  "titre": "🤝 Nakama Forever",        "prix": 3_000,   "emoji": "🤝"},
    {"id": "titan",   "titre": "🔱 Porteur de Titan",      "prix": 15_000,  "emoji": "🔱"},
    {"id": "sensei",  "titre": "📖 Grand Sensei",          "prix": 20_000,  "emoji": "📖"},
    {"id": "dieu",    "titre": "⚡ Dieu de l'Anime",       "prix": 50_000,  "emoji": "⚡"},
    {"id": "demon",   "titre": "👺 Roi des Démons",        "prix": 30_000,  "emoji": "👺"},
    {"id": "otaku_s", "titre": "🌟 Otaku Suprême",         "prix": 100_000, "emoji": "🌟"},
]


# ─────────────────────────────────────────
# 🎯 QUESTIONS — ANIME
# ─────────────────────────────────────────

QUIZ_NORMAL = [
    {"q": "🎌 Dans quel anime trouve-t-on **Luffy** ?",                               "r": ["Naruto","One Piece","Bleach","Dragon Ball"],                "b": 1},
    {"q": "🎌 Qui est le 4ᵉ Hokage dans **Naruto** ?",                                "r": ["Hiruzen","Tsunade","Minato","Kakashi"],                     "b": 2},
    {"q": "🎌 Dans **Demon Slayer**, la respiration de départ de Tanjiro ?",           "r": ["Feu","Eau","Vent","Tonnerre"],                              "b": 1},
    {"q": "🎌 Dans **AOT**, qui possède le Titan Fondateur ?",                         "r": ["Armin","Mikasa","Levi","Eren"],                             "b": 3},
    {"q": "🎌 Dans **Dragon Ball Z**, combien de boules de cristal ?",                 "r": ["5","6","7","8"],                                           "b": 2},
    {"q": "🎌 Dans **MHA**, le Quirk de Deku ?",                                      "r": ["Zero Gravity","Explosion","Half-Cold","One For All"],       "b": 3},
    {"q": "🎌 Dans **Bleach**, l'épée de Ichigo s'appelle ?",                         "r": ["Senbonzakura","Zangetsu","Ryujin Jakka","Wabisuke"],        "b": 1},
    {"q": "🎌 Dans **One Piece**, qui est le Roi des Pirates ?",                       "r": ["Shanks","Luffy","Roger","Whitebeard"],                      "b": 2},
    {"q": "🎌 Dans **Naruto**, combien de queues a le Kyubi ?",                        "r": ["7","8","9","10"],                                          "b": 2},
    {"q": "🎌 Dans **Dragon Ball Super**, dieu de la destruction univers 7 ?",        "r": ["Whis","Champa","Goku","Beerus"],                            "b": 3},
    {"q": "🎌 Dans **Naruto**, le clan de Sasuke ?",                                   "r": ["Uzumaki","Hyuga","Senju","Uchiha"],                         "b": 3},
    {"q": "🎌 Dans **MHA**, le symbole de la Paix ?",                                 "r": ["Endeavor","Hawks","Edgeshot","All Might"],                  "b": 3},
    {"q": "🎌 Dans **Fairy Tail**, le dragon de Natsu ?",                             "r": ["Atlas Flame","Grandeeney","Metalicana","Igneel"],           "b": 3},
    {"q": "🎌 Dans **SAO**, vrai prénom de Kirito ?",                                 "r": ["Kenji","Koichi","Katsuki","Kazuto"],                        "b": 3},
    {"q": "🎌 Dans **Tokyo Ghoul**, Ken Kaneki est un ?",                             "r": ["Humain","Goule","Enquêteur","Demi-goule"],                  "b": 3},
    {"q": "🎌 Dans **Jujutsu Kaisen**, le Roi des malédictions ?",                    "r": ["Gojo","Mahito","Choso","Sukuna"],                           "b": 3},
    {"q": "🎌 Dans **FMA**, qu'a sacrifié Edward pour ramener son frère ?",            "r": ["Jambe gauche","Bras droit","Son œil","Bras gauche"],        "b": 1},
    {"q": "🎌 Dans **One Piece**, le fruit du démon de Luffy ?",                      "r": ["Flame-Flame","Ice-Ice","Dark-Dark","Gum-Gum"],              "b": 3},
    {"q": "🎌 Dans **Naruto**, qui a tué Itachi ?",                                   "r": ["Naruto","Kakashi","Il est mort de maladie","Sasuke"],        "b": 2},
    {"q": "🎌 Dans **AOT**, vrai identité du Titan Colossal (début) ?",               "r": ["Reiner","Annie","Ymir","Bertholdt"],                        "b": 3},
    {"q": "🎌 Dans **One Piece**, le fruit de Ace ?",                                 "r": ["Flame-Flame","Magu-Magu","Hie-Hie","Mera-Mera"],            "b": 3},
    {"q": "🎌 Dans **MHA**, le vrai nom de Bakugo ?",                                 "r": ["Izuku","Shoto","Tenya","Katsuki"],                          "b": 3},
    {"q": "🎌 Dans **Demon Slayer**, chef des Piliers ?",                             "r": ["Gyomei","Rengoku","Tengen","Kagaya"],                       "b": 3},
    {"q": "🎌 Dans **JJK**, professeur de Yuji Itadori ?",                           "r": ["Nanami","Megumi","Nobara","Gojo"],                           "b": 3},
    {"q": "🎌 Dans **Demon Slayer**, la sœur de Tanjiro ?",                          "r": ["Kanao","Nezuko","Shinobu","Mitsuri"],                        "b": 1},
    {"q": "🎌 Dans **HxH**, famille d'assassins de Killua ?",                        "r": ["Fantômes","Zoldyck","Genei Ryodan","Kurta"],                 "b": 1},
    {"q": "🎌 Dans **Fairy Tail**, nom de la guilde de Natsu ?",                     "r": ["Blue Pegasus","Lamia Scale","Sabertooth","Fairy Tail"],      "b": 3},
    {"q": "🎌 Dans **One Piece**, cuisinier de l'équipage de Luffy ?",               "r": ["Usopp","Franky","Sanji","Brook"],                           "b": 2},
    {"q": "🎌 Dans **Bleach**, nom du monde des Shinigami ?",                        "r": ["Hueco Mundo","Soul Society","Dangai","Rukongai"],            "b": 1},
    {"q": "🎌 Dans **Dragon Ball Z**, père de Goku ?",                               "r": ["Vegeta","Nappa","Bardock","Raditz"],                        "b": 2},
    {"q": "🎌 Dans **Chainsaw Man**, supérieure de Denji ?",                         "r": ["Power","Reze","Himeno","Makima"],                           "b": 3},
    {"q": "🎌 Dans **Naruto**, quel Hokage est Tsunade ?",                           "r": ["3ème","4ème","5ème","6ème"],                               "b": 2},
    {"q": "🎌 Dans **AOT**, qui est le capitaine des Éclaireurs ?",                  "r": ["Erwin","Hange","Levi","Mike"],                              "b": 2},
    {"q": "🎌 Dans **Naruto**, quel clan possède le Byakugan ?",                     "r": ["Uchiha","Senju","Uzumaki","Hyuga"],                         "b": 3},
    {"q": "🎌 Dans **One Piece**, qui est le médecin de l'équipage ?",               "r": ["Nami","Robin","Chopper","Franky"],                          "b": 2},
    {"q": "🎌 Dans **MHA**, vrai nom de Deku ?",                                     "r": ["Katsuki Bakugo","Shoto Todoroki","Izuku Midoriya","Tenya Iida"],"b": 2},
    {"q": "🎌 Dans **Demon Slayer**, couleur des yeux de Nezuko ?",                  "r": ["Rouge","Rose","Bleu","Vert"],                               "b": 1},
    {"q": "🎌 Dans **One Piece**, équipage de Luffy ?",                              "r": ["Équipage du Chapeau de Paille","Équipage de Roger","Équipage de Barbe Blanche","Équipage de Shanks"],"b": 0},
    {"q": "🎌 Dans **Dragon Ball Z**, fils de Goku ?",                               "r": ["Vegeta","Piccolo","Gohan","Krillin"],                       "b": 2},
    {"q": "🎌 Dans **Bleach**, comment s'appelle le monde des morts ?",              "r": ["Hueco Mundo","Seireitei","Soul Society","Rukongai"],         "b": 2},
    {"q": "🎌 Dans **AOT**, comment s'appelle la femelle Titan ?",                   "r": ["Historia","Mikasa","Ymir","Annie"],                         "b": 3},
    {"q": "🎌 Dans **Fairy Tail**, magie de Gray Fullbuster ?",                      "r": ["Feu","Glace","Foudre","Vent"],                              "b": 1},
    {"q": "🎌 Dans **Demon Slayer**, ennemi final ?",                                "r": ["Akaza","Doma","Kokushibo","Muzan Kibutsuji"],                "b": 3},
    {"q": "🎌 Dans **JJK**, quel personnage mange le doigt de Sukuna ?",             "r": ["Megumi","Nobara","Gojo","Yuji Itadori"],                    "b": 3},
    {"q": "🎌 Dans **One Piece**, quel mur est percé en premier dans AOT ?",         "r": ["Maria","Rose","Sina","Eden"],                              "b": 0},
    {"q": "🎌 Dans **Naruto**, quel animal invoque Jiraya ?",                        "r": ["Serpents","Escargots","Crapauds","Limaces"],                "b": 2},
    {"q": "🎌 Dans **MHA**, école de héros de Deku ?",                               "r": ["Shiketsu High","Ketsubutsu Academy","UA High School","Seiai Academy"],"b": 2},
    {"q": "🎌 Dans **Bleach**, Shikai de Rukia ?",                                   "r": ["Sode no Shirayuki","Senbonzakura","Wabisuke","Tobiume"],    "b": 0},
    {"q": "🎌 Dans **One Piece**, la technique de jambes de Sanji ?",                "r": ["Diable Jambe","Black Leg","Hell Memories","Poêle à Frire"], "b": 0},
    {"q": "🎌 Dans **HxH**, continent inconnu exploré par Ging ?",                   "r": ["Dark Continent","New World","Chimera Continent","Beyond"],  "b": 0},
]

QUIZ_HARDCORE = [
    {"q": "💀 Dans **HxH**, rang de Ging en tant que chasseur ?",                             "r": ["#1 Étoile","Botaniste","Chasseur Ruins","Double Étoile"],                  "b": 3},
    {"q": "💀 Dans **Naruto**, quel jutsu Minato a-t-il inventé ?",                           "r": ["Rasengan","Chidori","Kage Bunshin","Hiraishin"],                           "b": 3},
    {"q": "💀 Dans **One Piece**, vrai nom du fruit de Luffy ?",                              "r": ["Gum-Gum","Nika-Nika","Human-Human","Hito-Hito modèle Nika"],              "b": 3},
    {"q": "💀 Dans **Bleach**, vrai nom du Zanpakuto d'Aizen ?",                              "r": ["Senbonzakura","Tensa Zangetsu","Shinso","Kyoka Suigetsu"],                 "b": 3},
    {"q": "💀 Dans **FMA Brotherhood**, vrai nom de Father ?",                                "r": ["Homunculus","Van Hohenheim","Dwarf","Le Petit Homme dans le Flacon"],      "b": 3},
    {"q": "💀 Dans **JJK**, technique innée de Gojo ?",                                       "r": ["Domaine de l'Infini","Six Yeux","Mukagen","Infinity"],                     "b": 3},
    {"q": "💀 Dans **Evangelion**, que signifie NERV en allemand ?",                          "r": ["Nerf","Force","Acier","Nerve"],                                             "b": 3},
    {"q": "💀 Dans **Vinland Saga**, qui a tué le père de Thorfinn ?",                        "r": ["Bjorn","Canute","Thorkell","Askeladd"],                                    "b": 3},
    {"q": "💀 Dans **Berserk**, surnom de Guts ?",                                            "r": ["Le Guerrier Noir","L'Épéiste Fou","Le Berserker","Le Chien Noir de la Guerre"],"b": 3},
    {"q": "💀 Dans **One Piece**, âge de Shanks au début de la série ?",                      "r": ["29 ans","31 ans","33 ans","27 ans"],                                       "b": 3},
    {"q": "💀 Dans **JJK**, vrai nom du domaine d'expansion de Sukuna ?",                     "r": ["Coffin of the Iron Mountain","Chimera Shadow Garden","Self-Embodiment","Malevolent Shrine"],"b": 3},
    {"q": "💀 Dans **HxH**, âge de Gon lors de l'examen des chasseurs ?",                    "r": ["11 ans","13 ans","14 ans","12 ans"],                                       "b": 3},
    {"q": "💀 Dans **Naruto**, vrai nom de Pain (chef Akatsuki) ?",                           "r": ["Obito","Yahiko","Konan","Nagato"],                                         "b": 3},
    {"q": "💀 Dans **AOT**, taille du Titan Colossal de Bertholdt ?",                         "r": ["30m","50m","80m","60m"],                                                   "b": 3},
    {"q": "💀 Dans **One Piece**, vrai nom de l'île de Roger ?",                              "r": ["Raftel","Elbaf","Mariejoa","Laugh Tale"],                                  "b": 3},
    {"q": "💀 Dans **Bleach**, bankai de Byakuya ?",                                          "r": ["Daiguren Hyorinmaru","Jakuho Raikoben","Kamishini no Yari","Senbonzakura Kageyoshi"],"b": 3},
    {"q": "💀 Dans **Re:Zero**, comment Subaru revient-il à la vie ?",                        "r": ["Un grimoire","Une déesse","Il ne meurt jamais","Retour par la Mort"],       "b": 3},
    {"q": "💀 Dans **Code Geass**, pouvoir de Lelouch ?",                                     "r": ["Sharingan","Teigu","Bankai","Geass"],                                       "b": 3},
    {"q": "💀 Dans **Mob Psycho 100**, vrai nom de Mob ?",                                    "r": ["Ritsu Kageyama","Reigen Arataka","Teru Hanazawa","Shigeo Kageyama"],        "b": 3},
    {"q": "💀 Dans **Chainsaw Man**, le diable que Denji fusionne ?",                         "r": ["Diable du Pistolet","Diable de la Mort","Diable du Feu","Diable de la Tronçonneuse"],"b": 3},
    {"q": "💀 Dans **HxH**, combien de types de Nen ?",                                       "r": ["4","5","7","6"],                                                           "b": 3},
    {"q": "💀 Dans **Steins;Gate**, surnom du labo de Rintaro ?",                             "r": ["Future Gadget Lab","Time Research Lab","Divergence Lab","Akihabara Lab"],   "b": 0},
    {"q": "💀 Dans **Bleach**, numéro d'Espada d'Ulquiorra ?",                                "r": ["3","4","5","6"],                                                           "b": 1},
    {"q": "💀 Dans **HxH**, vrai nom de Killua ?",                                            "r": ["Killua Zoldyck","Kiru Zorudikku","Wilhelm Zoldyck","Killua Godspeed"],      "b": 0},
    {"q": "💀 Dans **JJK**, personnage avec Six Yeux + sans-limite ?",                        "r": ["Yuta Okkotsu","Gojo Satoru","Kenjaku","Tengen"],                            "b": 1},
    {"q": "💀 Dans **FMA**, alchimiste 'Bras d'Acier' qui éduqua les Elric ?",               "r": ["Roy Mustang","Izumi Curtis","Scar","Van Hohenheim"],                        "b": 1},
    {"q": "💀 Dans **Naruto**, jutsu d'Orochimaru pour transférer son âme ?",                 "r": ["Edo Tensei","Hiraishin","Fushi Tensei","Kuchiyose"],                        "b": 2},
    {"q": "💀 Dans **JJK**, premier mort parmi les élèves ?",                                 "r": ["Nobara Kugisaki","Junpei Yoshino","Yuji Itadori","Megumi Fushiguro"],       "b": 1},
    {"q": "💀 Dans **One Piece**, taille de Whitebeard ?",                                    "r": ["6m","7m","8m","9m"],                                                       "b": 1},
    {"q": "💀 Dans **AOT**, durée de vie des porteurs de titan ?",                            "r": ["5 ans","10 ans","13 ans","20 ans"],                                        "b": 2},
    {"q": "💀 Dans **HxH**, âge de Netero lors du combat final ?",                            "r": ["106 ans","110 ans","100 ans","120 ans"],                                   "b": 0},
    {"q": "💀 Dans **MHA**, vrai Quirk de All For One ?",                                     "r": ["Vol de Quirk","One For All inversé","Stockage de Quirk","Vol et transfert de Quirks"],"b": 3},
    {"q": "💀 Dans **One Piece**, quel fruit a Trafalgar Law ?",                               "r": ["Bari-Bari","Ope-Ope","Nagi-Nagi","Mori-Mori"],                             "b": 1},
    {"q": "💀 Dans **Naruto**, combien de Tailed Beasts ?",                                   "r": ["7","8","9","10"],                                                          "b": 2},
    {"q": "💀 Dans **Berserk**, nom de l'épée de Guts ?",                                     "r": ["Dragonslayer","Excalibur","Berserker Blade","Godhand Killer"],               "b": 0},
    {"q": "💀 Dans **Overlord**, niveau max dans YGGDRASIL ?",                                "r": ["99","100","150","200"],                                                     "b": 1},
    {"q": "💀 Dans **Naruto**, pouvoir absolu du Rinne-Sharingan de Kaguya ?",                "r": ["Tsukuyomi Infini","Chibaku Tensei","Infinite Tsukuyomi","Susanoo Total"],   "b": 2},
    {"q": "💀 Dans **FMA**, combien de portes de la Vérité pour les frères Elric ?",          "r": ["1 seule","2 — une chacun","3","4"],                                        "b": 1},
    {"q": "💀 Dans **Chainsaw Man**, vrai diable derrière Makima ?",                          "r": ["Diable du Contrôle","Diable de la Peur","Diable de la Mort","Diable du Destin"],"b": 0},
    {"q": "💀 Dans **Re:Zero**, vrai nom de la Sorcière de l'Envie ?",                        "r": ["Satella","Emilia","Echidna","Typhon"],                                      "b": 0},
]

# ─────────────────────────────────────────
# 🌍 QUESTIONS CULTURE GÉNÉRALE
# ─────────────────────────────────────────

QUIZ_CULTURE = [
    # Géographie
    {"q": "🌍 Quelle est la capitale de l'Australie ?",          "r": ["Sydney","Melbourne","Canberra","Brisbane"],         "b": 2},
    {"q": "🌍 Quel est le plus grand océan du monde ?",           "r": ["Atlantique","Indien","Arctique","Pacifique"],       "b": 3},
    {"q": "🌍 Dans quel pays se trouve le Mont Everest ?",        "r": ["Inde","Tibet/Népal","Chine","Pakistan"],            "b": 1},
    {"q": "🌍 Combien de continents y a-t-il sur Terre ?",        "r": ["5","6","7","8"],                                   "b": 2},
    {"q": "🌍 Quelle est la capitale du Brésil ?",                "r": ["Rio de Janeiro","São Paulo","Brasília","Salvador"],  "b": 2},
    {"q": "🌍 Quel est le plus long fleuve du monde ?",           "r": ["Mississippi","Amazone","Nil","Yangtsé"],            "b": 2},
    {"q": "🌍 Quel pays a la plus grande superficie ?",           "r": ["États-Unis","Chine","Canada","Russie"],             "b": 3},
    {"q": "🌍 Quelle est la capitale du Japon ?",                 "r": ["Osaka","Kyoto","Tokyo","Hiroshima"],               "b": 2},
    {"q": "🌍 Quel désert est le plus grand du monde ?",          "r": ["Sahara","Gobi","Arctique","Antarctique"],           "b": 3},
    {"q": "🌍 Dans quel pays est la Tour Eiffel ?",               "r": ["Belgique","Italie","France","Espagne"],             "b": 2},
    {"q": "🌍 Quelle est la capitale de l'Égypte ?",              "r": ["Alexandrie","Louxor","Le Caire","Assouan"],         "b": 2},
    {"q": "🌍 Quel est le pays le plus peuplé du monde ?",        "r": ["Inde","États-Unis","Chine","Indonésie"],            "b": 0},
    {"q": "🌍 Quelle mer se trouve entre l'Europe et l'Afrique ?","r": ["Mer Rouge","Mer Noire","Mer Méditerranée","Mer Caspienne"],"b": 2},
    {"q": "🌍 La ville de New York est dans quel État américain ?","r": ["New Jersey","Floride","New York","Connecticut"],    "b": 2},
    {"q": "🌍 Quel pays produit le plus de café ?",               "r": ["Colombie","Vietnam","Éthiopie","Brésil"],           "b": 3},
    # Science
    {"q": "🔬 Quelle planète est la plus proche du Soleil ?",     "r": ["Vénus","Terre","Mars","Mercure"],                  "b": 3},
    {"q": "🔬 Combien d'os y a-t-il dans le corps humain adulte ?","r": ["196","206","216","226"],                           "b": 1},
    {"q": "🔬 Quelle est la formule chimique de l'eau ?",         "r": ["HO","H2O","H3O","H2O2"],                          "b": 1},
    {"q": "🔬 Quel scientifique a découvert la gravité ?",        "r": ["Einstein","Galilée","Newton","Darwin"],             "b": 2},
    {"q": "🔬 Combien de chromosomes a l'être humain ?",          "r": ["23","44","46","48"],                               "b": 2},
    {"q": "🔬 Quelle est la vitesse de la lumière (km/s) ?",      "r": ["150 000","200 000","300 000","500 000"],           "b": 2},
    {"q": "🔬 Quel élément chimique est le plus abondant sur Terre ?","r": ["Oxygène","Azote","Carbone","Hydrogène"],        "b": 0},
    {"q": "🔬 Combien de planètes dans notre système solaire ?",   "r": ["7","8","9","10"],                                  "b": 1},
    {"q": "🔬 Quelle planète est surnommée la planète rouge ?",   "r": ["Jupiter","Vénus","Saturne","Mars"],                "b": 3},
    {"q": "🔬 De quoi est composée l'ADN ?",                      "r": ["Acides aminés","Nucléotides","Lipides","Glucides"], "b": 1},
    {"q": "🔬 Quelle est la température d'ébullition de l'eau à pression normale ?","r": ["90°C","95°C","100°C","105°C"],   "b": 2},
    {"q": "🔬 Quel gaz les plantes absorbent-elles ?",            "r": ["Oxygène","Azote","CO2","Hydrogène"],               "b": 2},
    {"q": "🔬 Combien de secondes dans une heure ?",              "r": ["360","3600","600","6000"],                         "b": 1},
    {"q": "🔬 Quelle est la plus petite planète du système solaire ?","r": ["Mars","Pluton (exclue)","Mercure","Vénus"],     "b": 2},
    {"q": "🔬 Qu'est-ce que l'ADN ?",                             "r": ["Une hormone","Une protéine","L'acide désoxyribonucléique","Un organe"],"b": 2},
    # Histoire
    {"q": "📜 En quelle année a eu lieu la Révolution française ?","r": ["1776","1789","1799","1815"],                       "b": 1},
    {"q": "📜 Qui était le premier président des États-Unis ?",   "r": ["Lincoln","Jefferson","Adams","Washington"],         "b": 3},
    {"q": "📜 En quelle année a commencé la Seconde Guerre mondiale ?","r": ["1936","1937","1938","1939"],                   "b": 3},
    {"q": "📜 Qui a peint la Joconde ?",                          "r": ["Raphaël","Michel-Ange","Léonard de Vinci","Botticelli"],"b": 2},
    {"q": "📜 Quelle civilisation a construit les pyramides de Gizeh ?","r": ["Sumérienne","Grecque","Egyptienne","Romaine"],"b": 2},
    {"q": "📜 En quelle année l'homme a-t-il marché sur la Lune ?","r": ["1965","1967","1969","1971"],                       "b": 2},
    {"q": "📜 Qui était Napoléon Bonaparte ?",                    "r": ["Roi de France","Président","Général et Empereur","Cardinal"],"b": 2},
    {"q": "📜 Quelle est la langue la plus parlée dans le monde ?","r": ["Espagnol","Mandarin","Anglais","Hindi"],            "b": 1},
    {"q": "📜 En quelle année le mur de Berlin est-il tombé ?",   "r": ["1985","1987","1989","1991"],                       "b": 2},
    {"q": "📜 Qui a écrit 'Les Misérables' ?",                    "r": ["Balzac","Zola","Victor Hugo","Flaubert"],           "b": 2},
    {"q": "📜 Quelle est la monnaie du Japon ?",                  "r": ["Won","Yuan","Ringgit","Yen"],                      "b": 3},
    {"q": "📜 Dans quel pays est née la démocratie ?",            "r": ["Rome","Égypte","Grèce","Perse"],                   "b": 2},
    {"q": "📜 Qui a découvert l'Amérique selon les livres d'histoire ?","r": ["Magellan","Vespucci","Colomb","Cabot"],       "b": 2},
    # Culture pop / Sport
    {"q": "🎮 Quel jeu met en scène Mario ?",                     "r": ["Sega","Atari","Nintendo","Sony"],                  "b": 2},
    {"q": "🎮 Dans quel pays est née la K-Pop ?",                 "r": ["Japon","Chine","Corée du Sud","Taiwan"],           "b": 2},
    {"q": "⚽ Combien de joueurs dans une équipe de foot ?",       "r": ["9","10","11","12"],                                "b": 2},
    {"q": "⚽ Combien de joueurs dans une équipe de basket ?",     "r": ["4","5","6","7"],                                   "b": 1},
    {"q": "🎵 Quel groupe a sorti 'Thriller' ?",                  "r": ["The Beatles","ABBA","Michael Jackson (solo)","Queen"],"b": 2},
    {"q": "🎵 De quel pays vient BTS ?",                          "r": ["Japon","Chine","Corée du Sud","Thaïlande"],        "b": 2},
    {"q": "🎬 Dans **Harry Potter**, quelle maison est Harry ?",  "r": ["Serpentard","Serdaigle","Poufsouffle","Gryffondor"],"b": 3},
    {"q": "🎬 Combien d'Infinity Stones dans **Avengers** ?",     "r": ["4","5","6","7"],                                   "b": 2},
    {"q": "🎬 Dans **Star Wars**, père de Luke Skywalker ?",      "r": ["Obi-Wan","Palpatine","Yoda","Dark Vador"],         "b": 3},
    {"q": "🎮 Dans **Minecraft**, quel est le boss final ?",       "r": ["Creeper","Enderman","Ender Dragon","Wither"],      "b": 2},
    {"q": "🎮 Dans **Fortnite**, combien de joueurs max dans une partie standard ?","r": ["50","75","100","150"],             "b": 2},
    {"q": "🎬 Combien de films dans la saga **Star Wars** principale ?","r": ["6","7","8","9"],                              "b": 3},
    {"q": "🎵 Qui a composé la Cinquième Symphonie ?",            "r": ["Mozart","Bach","Beethoven","Chopin"],              "b": 2},
    {"q": "📚 Qui a écrit **1984** ?",                            "r": ["Aldous Huxley","Ray Bradbury","George Orwell","Philip Dick"],"b": 2},
    {"q": "📚 Qui a écrit **Le Petit Prince** ?",                  "r": ["Jules Verne","Molière","Antoine de Saint-Exupéry","Voltaire"],"b": 2},
    # Maths / Logique
    {"q": "🔢 Combien font 15 × 15 ?",                            "r": ["175","200","225","250"],                           "b": 2},
    {"q": "🔢 Quelle est la racine carrée de 144 ?",              "r": ["10","11","12","13"],                               "b": 2},
    {"q": "🔢 Combien font 7 × 8 ?",                              "r": ["48","54","56","64"],                               "b": 2},
    {"q": "🔢 Quel est le nombre pi (2 décimales) ?",             "r": ["3.12","3.14","3.16","3.18"],                      "b": 1},
    {"q": "🔢 Combien font 100 ÷ 4 ?",                            "r": ["20","25","30","40"],                               "b": 1},
    {"q": "🔢 Quel est le résultat de 2^10 ?",                    "r": ["512","1024","2048","256"],                         "b": 1},
    {"q": "🔢 Combien de zéros dans un million ?",                "r": ["5","6","7","8"],                                   "b": 1},
]


# ─────────────────────────────────────────
# 📚 SYSTÈME DE QUESTIONS V9
# Chargement JSON + Open Trivia + Anti-répétition
# ─────────────────────────────────────────

# Stockage anti-répétition par groupe
# chat_id → deque des 50 dernières questions posées (leur texte)
from collections import deque
_questions_recentes = {}   # chat_id → deque(maxlen=ANTI_REPEAT_SIZE)

# Pool chargé depuis questions.json au démarrage
_POOL_JSON_HARDCORE = []
_POOL_JSON_NORMAL   = []
_POOL_JSON_DUEL     = []


def _charger_questions_json():
    """Charge les questions depuis la DB (priorité) puis depuis questions.json (fallback)."""
    global _POOL_JSON_HARDCORE, _POOL_JSON_NORMAL, _POOL_JSON_DUEL

    # 1. Chargement depuis la base de données (permanent, Railway-safe)
    _charger_questions_db()

    # 2. Fallback : charger questions.json si présent et compléter les pools
    if not os.path.exists(QUESTIONS_JSON_PATH):
        return
    try:
        with open(QUESTIONS_JSON_PATH, "r", encoding="utf-8") as f:
            data = _json_module.load(f)
        hc_json = data.get("hardcore", [])
        n_json  = data.get("normal",   [])
        d_json  = data.get("duel",     [])
        # Ajouter seulement les questions pas déjà en mémoire (évite doublons)
        questions_existantes_hc = {q["q"] for q in _POOL_JSON_HARDCORE}
        questions_existantes_n  = {q["q"] for q in _POOL_JSON_NORMAL}
        questions_existantes_d  = {q["q"] for q in _POOL_JSON_DUEL}
        _POOL_JSON_HARDCORE += [q for q in hc_json if q["q"] not in questions_existantes_hc]
        _POOL_JSON_NORMAL   += [q for q in n_json  if q["q"] not in questions_existantes_n]
        _POOL_JSON_DUEL     += [q for q in d_json  if q["q"] not in questions_existantes_d]
        total = len(_POOL_JSON_HARDCORE) + len(_POOL_JSON_NORMAL) + len(_POOL_JSON_DUEL)
        logger.info(f"[questions] Chargement total — {total} questions (HC:{len(_POOL_JSON_HARDCORE)} N:{len(_POOL_JSON_NORMAL)} D:{len(_POOL_JSON_DUEL)})")
    except Exception as e:
        logger.error(f"[questions] Erreur chargement JSON : {e}")


# ─────────────────────────────────────────
# 🤖 GÉNÉRATION AUTOMATIQUE VIA GROQ AI
# ─────────────────────────────────────────

# Thèmes et prompts par niveau
_GROQ_THEMES = {
    "normal": [
        ("Anime populaire (Naruto, One Piece, Dragon Ball, Bleach, MHA, Demon Slayer, AOT)", "🎌"),
        ("Manga célèbre (Death Note, Fullmetal Alchemist, Fairy Tail, SAO, Tokyo Ghoul)", "🎌"),
        ("Manhwa / Webtoon (Solo Leveling, Tower of God, Lookism, Noblesse, The God of High School)", "📖"),
        ("Jeux vidéo populaires (Minecraft, GTA, FIFA, Fortnite, League of Legends, Valorant)", "🎮"),
        ("Culture générale (géographie, science, histoire, sport)", "🌍"),
    ],
    "hardcore": [
        ("Anime — détails très précis, personnages secondaires, techniques rares, épisodes spécifiques", "💀"),
        ("Manga — arcs narratifs, auteurs, dates de publication, détails de lore avancés", "💀"),
        ("Manhwa / Webtoon — détails avancés, pouvoirs précis, noms coréens originaux", "💀"),
        ("Jeux vidéo — détails techniques, lore avancé, succès rares, easter eggs", "💀"),
    ],
    "duel": [
        ("Anime et Manga — questions de difficulté moyenne, varié, adapté aux duels rapides", "⚔️"),
        ("Manhwa, Webtoon et Jeux vidéo — questions de difficulté moyenne pour duels", "⚔️"),
    ],
}

_GROQ_INIT_DONE = False  # flag pour savoir si l'init a déjà été faite


def _sauvegarder_questions_db(nouvelles: list, niveau: str):
    """Sauvegarde les nouvelles questions dans la base de données (ignore les doublons)."""
    if not nouvelles:
        return 0
    inseres = 0
    try:
        with get_db() as con:
            for q in nouvelles:
                try:
                    if USE_POSTGRES:
                        _execute(con, """
                            INSERT INTO questions_ia (niveau, question, reponses, bonne, created_at)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (question) DO NOTHING
                        """, (niveau, q["q"], _json_module.dumps(q["r"], ensure_ascii=False), q["b"], datetime.now().isoformat()))
                    else:
                        _execute(con, """
                            INSERT OR IGNORE INTO questions_ia (niveau, question, reponses, bonne, created_at)
                            VALUES (?, ?, ?, ?, ?)
                        """, (niveau, q["q"], _json_module.dumps(q["r"], ensure_ascii=False), q["b"], datetime.now().isoformat()))
                    inseres += 1
                except Exception:
                    pass
        logger.info(f"[db] {inseres}/{len(nouvelles)} questions sauvegardées en DB (niveau={niveau})")
    except Exception as e:
        logger.error(f"[db] Erreur sauvegarde questions : {e}")
    return inseres


def _charger_questions_db():
    """Charge toutes les questions IA depuis la base de données dans les pools mémoire."""
    global _POOL_JSON_HARDCORE, _POOL_JSON_NORMAL, _POOL_JSON_DUEL
    try:
        with get_db() as con:
            rows = _fetchall(con, "SELECT niveau, question, reponses, bonne FROM questions_ia")
        for row in rows:
            q = {"q": row["question"], "r": _json_module.loads(row["reponses"]), "b": row["bonne"]}
            if row["niveau"] == "hardcore":
                _POOL_JSON_HARDCORE.append(q)
            elif row["niveau"] == "normal":
                _POOL_JSON_NORMAL.append(q)
            elif row["niveau"] == "duel":
                _POOL_JSON_DUEL.append(q)
        # Dédupliquer
        _POOL_JSON_HARDCORE = _dedup_pool(_POOL_JSON_HARDCORE)
        _POOL_JSON_NORMAL   = _dedup_pool(_POOL_JSON_NORMAL)
        _POOL_JSON_DUEL     = _dedup_pool(_POOL_JSON_DUEL)
        total = len(_POOL_JSON_HARDCORE) + len(_POOL_JSON_NORMAL) + len(_POOL_JSON_DUEL)
        logger.info(f"[db] {total} questions IA chargées depuis DB (HC:{len(_POOL_JSON_HARDCORE)} N:{len(_POOL_JSON_NORMAL)} D:{len(_POOL_JSON_DUEL)})")
    except Exception as e:
        logger.error(f"[db] Erreur chargement questions : {e}")


def _sauvegarder_questions_json():
    """Sauvegarde les pools en mémoire dans questions.json (fallback si pas de DB)."""
    try:
        if os.path.exists(QUESTIONS_JSON_PATH):
            with open(QUESTIONS_JSON_PATH, "r", encoding="utf-8") as f:
                data = _json_module.load(f)
        else:
            data = {}
        data["hardcore"] = _POOL_JSON_HARDCORE
        data["normal"]   = _POOL_JSON_NORMAL
        data["duel"]     = _POOL_JSON_DUEL
        with open(QUESTIONS_JSON_PATH, "w", encoding="utf-8") as f:
            _json_module.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"[json] questions.json sauvegardé — HC:{len(_POOL_JSON_HARDCORE)} N:{len(_POOL_JSON_NORMAL)} D:{len(_POOL_JSON_DUEL)}")
    except Exception as e:
        logger.error(f"[json] Erreur sauvegarde JSON : {e}")


def _dedup_pool(pool: list) -> list:
    """Supprime les doublons d'un pool basé sur le texte de la question."""
    seen = set()
    result = []
    for q in pool:
        key = q["q"].strip().lower()
        if key not in seen:
            seen.add(key)
            result.append(q)
    return result


async def _groq_generer_questions(niveau: str, theme: str, emoji: str, nb: int) -> list:
    """
    Appelle l'API Groq pour générer `nb` questions sur un thème donné.
    Tourne automatiquement entre les modèles si l'un est en rate limit.
    Retourne une liste de dicts {q, r, b} ou [] en cas d'erreur.
    """
    if not GROQ_API_KEY:
        logger.warning("[groq] GROQ_API_KEY non définie — génération ignorée.")
        return []

    nb_reel = min(nb, 25)

    prompt = f"""Tu es un expert en quiz. Génère exactement {nb_reel} questions de quiz en FRANÇAIS sur le thème : {theme}.

Règles STRICTES :
- Niveau : {"FACILE à MOYEN (grand public)" if niveau == "normal" else "DIFFICILE (fans hardcore)" if niveau == "hardcore" else "MOYEN (duels rapides)"}
- Chaque question a EXACTEMENT 4 réponses COURTES (max 5 mots chacune)
- UNE SEULE bonne réponse par question
- Les mauvaises réponses doivent être plausibles mais clairement fausses
- Questions variées, pas de répétitions
- UNIQUEMENT du JSON valide, rien d'autre

Format JSON obligatoire (tableau de {nb_reel} objets) :
[
  {{"q": "{emoji} [SÉRIE] Question ?", "r": ["Rep1", "Rep2", "Rep3", "Rep4"], "b": 0}},
  {{"q": "{emoji} [SÉRIE] Autre question ?", "r": ["Rep1", "Rep2", "Rep3", "Rep4"], "b": 2}}
]

Le champ "b" est l'INDEX (0-3) de la bonne réponse dans le tableau "r".
IMPORTANT : Réponds UNIQUEMENT avec le tableau JSON complet et valide, sans texte avant ou après, sans commentaires."""

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    # Tourner entre les modèles disponibles
    for model in GROQ_MODELS:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 6000,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(GROQ_API_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 429:
                        txt = await resp.text()
                        logger.warning(f"[groq] Modèle {model} en rate limit — essai suivant...")
                        await asyncio.sleep(2)
                        continue  # essayer le modèle suivant
                    if resp.status != 200:
                        txt = await resp.text()
                        logger.error(f"[groq] HTTP {resp.status} ({model}) : {txt[:200]}")
                        continue
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    content = re.sub(r"```json\s*", "", content)
                    content = re.sub(r"```\s*", "", content)
                    questions = _json_module.loads(content)
                    valides = []
                    for q in questions:
                        if (isinstance(q, dict)
                                and "q" in q and "r" in q and "b" in q
                                and isinstance(q["r"], list) and len(q["r"]) == 4
                                and isinstance(q["b"], int) and 0 <= q["b"] <= 3):
                            valides.append(q)
                    logger.info(f"[groq] {len(valides)}/{nb_reel} questions valides ({model}, {theme[:35]})")
                    return valides
        except Exception as e:
            logger.error(f"[groq] Erreur ({model}) : {e}")
            continue

    logger.error(f"[groq] Tous les modèles ont échoué pour : {theme[:40]}")
    return []


async def _groq_session_generation(nb_par_theme: int):
    """
    Lance une session de génération pour tous les niveaux et thèmes.
    Sauvegarde dans la DB (persistant) ET en mémoire.
    """
    global _POOL_JSON_HARDCORE, _POOL_JSON_NORMAL, _POOL_JSON_DUEL

    if not GROQ_API_KEY:
        logger.warning("[groq] Clé API manquante — session annulée.")
        return

    total_ajout = {"normal": 0, "hardcore": 0, "duel": 0}

    for niveau, themes in _GROQ_THEMES.items():
        for theme, emoji in themes:
            nouvelles = await _groq_generer_questions(niveau, theme, emoji, nb_par_theme)
            if nouvelles:
                # 1. Sauvegarder en base de données (permanent, survit aux redémarrages)
                inseres = _sauvegarder_questions_db(nouvelles, niveau)
                # 2. Ajouter en mémoire pour utilisation immédiate
                if niveau == "hardcore":
                    _POOL_JSON_HARDCORE.extend(nouvelles)
                    _POOL_JSON_HARDCORE = _dedup_pool(_POOL_JSON_HARDCORE)
                elif niveau == "normal":
                    _POOL_JSON_NORMAL.extend(nouvelles)
                    _POOL_JSON_NORMAL = _dedup_pool(_POOL_JSON_NORMAL)
                elif niveau == "duel":
                    _POOL_JSON_DUEL.extend(nouvelles)
                    _POOL_JSON_DUEL = _dedup_pool(_POOL_JSON_DUEL)
                total_ajout[niveau] += inseres
            # Petite pause pour ne pas dépasser la limite de rate
            await asyncio.sleep(2)

    logger.info(f"[groq] Session terminée — ajout DB: N:{total_ajout['normal']} HC:{total_ajout['hardcore']} D:{total_ajout['duel']}")


async def job_groq_generation(context):
    """Job planifié — génération quotidienne de questions."""
    logger.info("[groq] Démarrage session génération planifiée...")
    await _groq_session_generation(nb_par_theme=GROQ_QUESTIONS_DAILY)


async def job_groq_init(context):
    """Job run_once — init Groq 60s après le démarrage."""
    await _groq_init_si_necessaire()


async def _groq_init_si_necessaire():
    """
    Au 1er démarrage, si la clé Groq est présente et que le stock est faible,
    génère un grand lot initial de questions.
    """
    global _GROQ_INIT_DONE
    if _GROQ_INIT_DONE or not GROQ_API_KEY:
        return
    _GROQ_INIT_DONE = True

    total_actuel = len(_POOL_JSON_NORMAL) + len(_POOL_JSON_HARDCORE) + len(_POOL_JSON_DUEL)
    seuil = 500  # génère jusqu'à avoir 500 questions au total

    if total_actuel < seuil:
        logger.info(f"[groq] Stock faible ({total_actuel} questions) — lancement génération initiale ({GROQ_QUESTIONS_INIT} par thème)...")
        await _groq_session_generation(nb_par_theme=GROQ_QUESTIONS_INIT)
        logger.info("[groq] ✅ Génération initiale terminée !")
    else:
        logger.info(f"[groq] Stock suffisant ({total_actuel} questions) — génération initiale ignorée.")


def _pick_question(chat_id: int, pool: list) -> dict:
    """
    Choisit une question dans le pool en évitant les 50 dernières posées dans ce groupe.
    Si toutes les questions ont été posées récemment, repart de zéro.
    """
    if chat_id not in _questions_recentes:
        _questions_recentes[chat_id] = deque(maxlen=ANTI_REPEAT_SIZE)

    recentes = _questions_recentes[chat_id]
    # Filtrer les questions non récentes
    disponibles = [q for q in pool if q["q"] not in recentes]

    if not disponibles:
        # Toutes déjà posées — on remet à zéro et on prend une au hasard
        logger.info(f"[questions] chat {chat_id} — pool épuisé, reset anti-répétition")
        recentes.clear()
        disponibles = pool

    q = random.choice(disponibles)
    recentes.append(q["q"])
    return q


async def _get_question(chat_id: int, mode: str) -> dict:
    """
    Retourne une question selon le mode, en gérant :
    - Anti-répétition
    - Questions JSON externes
    - Fallback Open Trivia pour le mode normal
    """
    if mode == "hardcore":
        # Pool HC = questions internes + questions JSON HC
        pool_hc = QUIZ_HARDCORE + _POOL_JSON_HARDCORE
        return _pick_question(chat_id, pool_hc)

    elif mode == "duel":
        pool_duel = QUIZ_DUEL + _POOL_JSON_DUEL
        return _pick_question(chat_id, pool_duel)

    elif mode == "mystere":
        pool_all = QUIZ_NORMAL + QUIZ_HARDCORE + QUIZ_CULTURE + _POOL_JSON_HARDCORE + _POOL_JSON_NORMAL
        return _pick_question(chat_id, pool_all)

    else:
        # Mode normal : 70% pool local, 30% Open Trivia pour renouveler
        # Open Trivia désactivé (questions en anglais)
        # Pool 100% français : local + JSON
        pool_normal = QUIZ_NORMAL + QUIZ_CULTURE + _POOL_JSON_NORMAL
        return _pick_question(chat_id, pool_normal)


# ─────────────────────────────────────────
# ➕ /addquestion — Admin ajoute une question
# ─────────────────────────────────────────

async def cmd_addquestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Permet à un super-admin d'ajouter une question via Telegram.
    Format : /addquestion niveau|Question|Rep1|Rep2|Rep3|Rep4|index_bonne(0-3)
    Exemple : /addquestion hardcore|Qui est le créateur de Naruto ?|Kishimoto|Toriyama|Oda|Kubo|0
    """
    if not update.message or not _check_superadmin(update):
        await update.message.reply_text("🚫 Réservé aux super-admins.")
        return

    if not context.args:
        await update.message.reply_text(
            "📝 *Ajouter une question*\n\n"
            "Format :\n"
            "`/addquestion niveau|Question|Rep1|Rep2|Rep3|Rep4|index`\n\n"
            "• `niveau` : `normal`, `hardcore` ou `duel`\n"
            "• `index` : position de la bonne réponse (0, 1, 2 ou 3)\n\n"
            "*Exemple :*\n"
            "`/addquestion hardcore|Créateur de Naruto ?|Kishimoto|Toriyama|Oda|Kubo|0`",
            parse_mode="Markdown"
        )
        return

    raw = " ".join(context.args)
    parts = raw.split("|")
    if len(parts) != 7:
        await update.message.reply_text(
            "❌ Format invalide — il faut exactement 7 parties séparées par `|`\n"
            "Ex : `niveau|Question|Rep1|Rep2|Rep3|Rep4|index`",
            parse_mode="Markdown"
        )
        return

    niveau, question, r1, r2, r3, r4, idx_str = [p.strip() for p in parts]

    if niveau not in ("normal", "hardcore", "duel"):
        await update.message.reply_text("❌ Niveau invalide. Choisis : `normal`, `hardcore` ou `duel`", parse_mode="Markdown")
        return

    try:
        idx = int(idx_str)
        if idx not in (0, 1, 2, 3):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ L'index de la bonne réponse doit être 0, 1, 2 ou 3.")
        return

    # Construire la question
    new_q = {"q": question, "r": [r1, r2, r3, r4], "b": idx}

    # Charger le JSON existant
    if os.path.exists(QUESTIONS_JSON_PATH):
        try:
            with open(QUESTIONS_JSON_PATH, "r", encoding="utf-8") as f:
                data = _json_module.load(f)
        except Exception:
            data = {"normal": [], "hardcore": [], "duel": []}
    else:
        data = {"normal": [], "hardcore": [], "duel": []}

    # Ajouter et sauvegarder
    data.setdefault(niveau, []).append(new_q)
    try:
        with open(QUESTIONS_JSON_PATH, "w", encoding="utf-8") as f:
            _json_module.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur sauvegarde : {e}")
        return

    # Recharger en mémoire
    _charger_questions_json()

    bonne = [r1, r2, r3, r4][idx]
    await update.message.reply_text(
        f"✅ *Question ajoutée !*\n\n"
        f"📂 Niveau : *{niveau}*\n"
        f"❓ {question}\n"
        f"✅ Bonne réponse : *{bonne}*\n\n"
        f"📊 Total {niveau} : *{len(data[niveau])}* questions",
        parse_mode="Markdown"
    )


async def cmd_questions_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Affiche les statistiques des pools de questions."""
    if not update.message or not _check_superadmin(update):
        await update.message.reply_text("🚫 Réservé aux super-admins.")
        return

    pool_normal   = len(QUIZ_NORMAL) + len(QUIZ_CULTURE) + len(_POOL_JSON_NORMAL)
    pool_hardcore = len(QUIZ_HARDCORE) + len(_POOL_JSON_HARDCORE)
    pool_duel     = len(QUIZ_DUEL) + len(_POOL_JSON_DUEL)
    pool_total    = pool_normal + pool_hardcore + pool_duel

    groq_status = f"✅ Active (clé configurée)\nSessions : {GROQ_SCHEDULE_HOURS} UTC\nInit/session : {GROQ_QUESTIONS_INIT} | Daily : {GROQ_QUESTIONS_DAILY} par thème" if GROQ_API_KEY else "❌ Inactive (GROQ_API_KEY manquante)"

    recentes_info = ""
    if _questions_recentes:
        recentes_info = "\n\n📊 *Anti-répétition actif :*\n"
        for cid, deq in list(_questions_recentes.items())[:5]:
            recentes_info += f"  Groupe `{cid}` : {len(deq)}/{ANTI_REPEAT_SIZE} mémorisées\n"

    await update.message.reply_text(
        f"📚 *STATISTIQUES QUESTIONS*\n\n"
        f"🎌 Normal (local)    : *{len(QUIZ_NORMAL) + len(QUIZ_CULTURE)}*\n"
        f"🌐 Normal (JSON/IA)  : *{len(_POOL_JSON_NORMAL)}*\n"
        f"💀 Hardcore (local)  : *{len(QUIZ_HARDCORE)}*\n"
        f"💀 Hardcore (JSON/IA): *{len(_POOL_JSON_HARDCORE)}*\n"
        f"⚔️ Duel (local)      : *{len(QUIZ_DUEL)}*\n"
        f"⚔️ Duel (JSON/IA)    : *{len(_POOL_JSON_DUEL)}*\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📦 *Total pool : {pool_total} questions*\n"
        f"🔄 Anti-répétition : *{ANTI_REPEAT_SIZE}* par groupe\n\n"
        f"🤖 *Groq IA :* {groq_status}{recentes_info}",
        parse_mode="Markdown"
    )


async def cmd_genererquestions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force une session de génération Groq immédiatement (super-admin)."""
    if not update.message or not _check_superadmin(update):
        await update.message.reply_text("🚫 Réservé aux super-admins.")
        return
    if not GROQ_API_KEY:
        await update.message.reply_text("❌ GROQ_API_KEY non configurée dans le .env !")
        return
    msg = await update.message.reply_text(
        "🤖 *Génération Groq lancée...*\nCela peut prendre 2-3 minutes. Je te préviens quand c'est fini !",
        parse_mode="Markdown"
    )
    await _groq_session_generation(nb_par_theme=GROQ_QUESTIONS_DAILY)
    total = len(_POOL_JSON_NORMAL) + len(_POOL_JSON_HARDCORE) + len(_POOL_JSON_DUEL)
    await msg.edit_text(
        f"✅ *Génération terminée !*\n\n"
        f"🎌 Normal : *{len(_POOL_JSON_NORMAL)}* questions JSON\n"
        f"💀 Hardcore : *{len(_POOL_JSON_HARDCORE)}* questions JSON\n"
        f"⚔️ Duel : *{len(_POOL_JSON_DUEL)}* questions JSON\n\n"
        f"📦 *Total JSON : {total} questions*",
        parse_mode="Markdown"
    )


QUIZ_DUEL = [
    {"q": "⚔️ Dans **One Piece**, vrai nom du fruit de Luffy ?",          "r": ["Gum-Gum","Nika-Nika","Human-Human","Hito-Hito modèle Nika"],"b": 3},
    {"q": "⚔️ Dans **One Piece**, père de Luffy ?",                       "r": ["Garp","Dragon","Shanks","Whitebeard"],                     "b": 1},
    {"q": "⚔️ Dans **One Piece**, vrai nom de l'île de Roger ?",          "r": ["Raftel","Elbaf","Mariejoa","Laugh Tale"],                  "b": 3},
    {"q": "⚔️ Dans **One Piece**, combien d'épées Zoro ?",                "r": ["1","2","3","4"],                                          "b": 2},
    {"q": "⚔️ Dans **One Piece**, qui a tué Ace ?",                       "r": ["Blackbeard","Aokiji","Akainu","Kizaru"],                   "b": 2},
    {"q": "⚔️ Dans **Naruto**, vrai chef de l'Akatsuki ?",                "r": ["Pain","Obito","Madara","Black Zetsu"],                     "b": 2},
    {"q": "⚔️ Dans **Naruto**, technique interdite de Minato ?",          "r": ["Rasengan","Hiraishin","Kage Bunshin","Edo Tensei"],        "b": 1},
    {"q": "⚔️ Dans **Naruto**, kekkai genkai clan Hyuga ?",               "r": ["Sharingan","Byakugan","Rinnegan","Tenseigan"],             "b": 1},
    {"q": "⚔️ Dans **Bleach**, vrai nom du Zanpakuto d'Aizen ?",          "r": ["Senbonzakura","Tensa Zangetsu","Shinso","Kyoka Suigetsu"],"b": 3},
    {"q": "⚔️ Dans **Bleach**, père biologique d'Ichigo ?",               "r": ["Kisuke Urahara","Isshin Kurosaki","Ryuken","Yamamoto"],    "b": 1},
    {"q": "⚔️ Dans **Demon Slayer**, Lune Supérieure 1 ?",                "r": ["Doma","Akaza","Kokushibo","Muzan"],                        "b": 2},
    {"q": "⚔️ Dans **Demon Slayer**, créateur de tous les démons ?",      "r": ["Kokushibo","Doma","Muzan Kibutsuji","Akaza"],              "b": 2},
    {"q": "⚔️ Dans **AOT**, père d'Eren Jaeger ?",                        "r": ["Grisha Jaeger","Zeke Jaeger","Rod Reiss","Uri Reiss"],     "b": 0},
    {"q": "⚔️ Dans **AOT**, vrai nom du Titan Bestial ?",                 "r": ["Reiner Braun","Bertholdt Hoover","Zeke Jaeger","Porco"],   "b": 2},
    {"q": "⚔️ Dans **JJK**, grade de Gojo Satoru ?",                      "r": ["Grade 1","Semi-grade 1","Grade spécial 1","Hors-grade"],   "b": 3},
    {"q": "⚔️ Dans **JJK**, qui a scellé Gojo dans la Prison Realm ?",    "r": ["Sukuna","Mahito","Kenjaku","Jogo"],                        "b": 2},
    {"q": "⚔️ Dans **HxH**, type de Nen de Kurapika ?",                   "r": ["Émission","Spécialisation","Renforcement","Manipulation"], "b": 1},
    {"q": "⚔️ Dans **MHA**, vrai nom de Shigaraki ?",                     "r": ["Tenko Shimura","Tomura","Yoichi","Kotaro Shimura"],        "b": 0},
    {"q": "⚔️ Dans **FMA**, péché capital de Greed ?",                    "r": ["Colère","Envie","Avarice","Orgueil"],                      "b": 2},
    {"q": "⚔️ Dans **Dragon Ball Super**, dieu destruction univers 6 ?",  "r": ["Beerus","Champa","Sidra","Belmod"],                        "b": 1},
    {"q": "⚔️ Dans **Re:Zero**, pouvoir de Subaru ?",                     "r": ["Voyage dans le temps","Retour par la Mort","Invincibilité","Prophétie"],"b": 1},
    {"q": "⚔️ Dans **Code Geass**, vrai nom de Lelouch ?",                "r": ["Lelouch Lamperouge","Lelouch vi Britannia","Zero","L.L."], "b": 1},
    {"q": "⚔️ Dans **Chainsaw Man**, diable Denji fusionne ?",            "r": ["Diable du Pistolet","Diable du Feu","Diable Tronçonneuse","Mort"],"b": 2},
    {"q": "⚔️ Dans **Berserk**, surnom de Guts ?",                        "r": ["Guerrier Noir","Épéiste Fou","Chien Noir de la Guerre","Berserker"],"b": 2},
    {"q": "⚔️ Dans **Mob Psycho 100**, vrai nom de Mob ?",                "r": ["Ritsu Kageyama","Reigen Arataka","Shigeo Kageyama","Teru"],"b": 2},
    {"q": "⚔️ Dans **Tokyo Ghoul**, rang de Kaneki après transformation ?","r": ["Goule B","Semi-goule","Roi des Goules","Goule SSS"],       "b": 1},
    {"q": "⚔️ Dans **Steins;Gate**, surnom de Rintaro Okabe ?",           "r": ["El Psy Kongroo","Mad Scientist","Hououin Kyouma","Doctor"],"b": 2},
    {"q": "⚔️ Dans **Black Clover**, grimoire d'Asta ?",                  "r": ["3 feuilles noires","4 feuilles noires","5 feuilles (diable)","Pas de grimoire"],"b": 2},
    {"q": "⚔️ Dans **Overlord**, vrai nom d'Ainz Ooal Gown ?",            "r": ["Momonga","Suzuki Satoru","Touch Me","Ulbert"],             "b": 1},
    {"q": "⚔️ Dans **MHA**, vrai Quirk héréditaire famille Todoroki ?",   "r": ["Glace","Feu","Mi-froid mi-chaud","Les deux héréditaires"], "b": 3},
    # Culture générale dans les duels
    {"q": "⚔️ Quelle est la capitale de l'Australie ?",                   "r": ["Sydney","Melbourne","Canberra","Brisbane"],                "b": 2},
    {"q": "⚔️ Quelle planète est la plus proche du Soleil ?",             "r": ["Vénus","Terre","Mars","Mercure"],                         "b": 3},
    {"q": "⚔️ En quelle année l'homme a marché sur la Lune ?",            "r": ["1965","1967","1969","1971"],                              "b": 2},
    {"q": "⚔️ Qui a peint la Joconde ?",                                  "r": ["Raphaël","Michel-Ange","Léonard de Vinci","Botticelli"],   "b": 2},
    {"q": "⚔️ Formule chimique de l'eau ?",                               "r": ["HO","H2O","H3O","H2O2"],                                 "b": 1},
    {"q": "⚔️ Dans **Harry Potter**, maison de Harry ?",                  "r": ["Serpentard","Serdaigle","Poufsouffle","Gryffondor"],       "b": 3},
    {"q": "⚔️ Dans **Star Wars**, père de Luke Skywalker ?",              "r": ["Obi-Wan","Palpatine","Yoda","Dark Vador"],                 "b": 3},
    {"q": "⚔️ Combien font 15 × 15 ?",                                    "r": ["175","200","225","250"],                                  "b": 2},
    {"q": "⚔️ Racine carrée de 144 ?",                                    "r": ["10","11","12","13"],                                      "b": 2},
    {"q": "⚔️ Quel est le plus grand océan du monde ?",                   "r": ["Atlantique","Indien","Arctique","Pacifique"],              "b": 3},
]


# ─────────────────────────────────────────
# 🗃️ STATE EN MÉMOIRE
# ─────────────────────────────────────────

quiz_en_cours      = {}   # chat_id → quiz data
combats_en_cours   = {}   # chat_id → duel data
_derniers_mysteres = {}   # chat_id → datetime du dernier mystère envoyé
transferts_pending = {}   # message_id → transfert data

# ─────────────────────────────────────────
# 🌍 LANGUE
# ─────────────────────────────────────────

async def cmd_langue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    keyboard = [[
        InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr"),
        InlineKeyboardButton("🇬🇧 English",  callback_data="lang_en"),
    ]]
    await update.message.reply_text(
        "🌍 *Choisissez votre langue / Choose your language:*",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def callback_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = query.data.split("_")[1]
    get_membre_db(user.id, chat_id, user.first_name)
    update_membre(user.id, chat_id, langue=lang)
    await query.edit_message_text(t("lang_set", lang), parse_mode="Markdown")
    clan = get_clan(chat_id)
    clan_info = t("has_clan_info", lang, nom=clan["nom"]) if clan else t("no_clan_info", lang)
    await context.bot.send_message(
        chat_id,
        t("welcome", lang, univers=UNIVERS_NOM, clan_info=clan_info,
          xp_n=XP_PAR_QUIZ_NORMAL, xp_h=XP_PAR_QUIZ_HARDCORE),
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 🤖 COMMANDES PRINCIPALES
# ─────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id

    # Enregistre les utilisateurs privés pour le broadcast
    if update.effective_chat.type == "private":
        with get_db() as con:
            existing = _fetchone(con, "SELECT user_id FROM private_users WHERE user_id=?", (user.id,))
            if not existing:
                _execute(con, "INSERT INTO private_users (user_id, username, created_at) VALUES (?,?,?)",
                         (user.id, user.username or user.first_name, datetime.now().isoformat()))

    with get_db() as con:
        row = _fetchone(con, "SELECT langue FROM membres WHERE user_id=? AND chat_id=?", (user.id, chat_id))
    if row is None:
        get_membre_db(user.id, chat_id, user.first_name)
        keyboard = [[
            InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr"),
            InlineKeyboardButton("🇬🇧 English",  callback_data="lang_en"),
        ]]
        await update.message.reply_text(
            "🌍 *Choisissez votre langue / Choose your language:*",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        return
    lang  = get_user_lang(user.id, chat_id)
    clan  = get_clan(chat_id)
    clan_info = t("has_clan_info", lang, nom=clan["nom"]) if clan else t("no_clan_info", lang)
    await update.message.reply_text(
        t("welcome", lang, univers=UNIVERS_NOM, clan_info=clan_info,
          xp_n=XP_PAR_QUIZ_NORMAL, xp_h=XP_PAR_QUIZ_HARDCORE),
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 🆕 BIENVENUE QUAND LE BOT REJOINT UN GROUPE
# ─────────────────────────────────────────

async def cmd_new_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envoie un message de bienvenue quand le bot est ajouté à un groupe."""
    if not update.message:
        return
    for member in (update.message.new_chat_members or []):
        if member.id == context.bot.id:
            await update.message.reply_text(
                f"👋 *Bonjour ! Je suis {UNIVERS_NOM}*\n\n"
                "⚔️ Le bot de jeu anime pour votre groupe !\n\n"
                "📋 *Pour commencer :*\n"
                "• `/start` — Inscription & langue\n"
                "• `/quiz` — Quiz anime (+30 XP)\n"
                "• `/war` — Duel contre un membre\n"
                "• `/daily` — Récompense quotidienne 🎁\n"
                "• `/rang` — Voir son profil\n"
                "• `/createclan NomDuClan` — Créer un clan\n"
                "• `/aide` — Liste complète des commandes\n\n"
                "⚙️ *Important :* Rendez-moi *administrateur* pour que le système d'XP automatique fonctionne !",
                parse_mode="Markdown"
            )
            break


# ─────────────────────────────────────────
# 📊 PROFIL
# ─────────────────────────────────────────

async def cmd_rang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)

    # Support /rang @user ou reply
    target_id   = user.id
    target_name = user.username or user.first_name

    if update.message.reply_to_message:
        ru = update.message.reply_to_message.from_user
        target_id   = ru.id
        target_name = ru.username or ru.first_name
    elif context.args:
        pseudo = context.args[0].lstrip("@").lower()
        with get_db() as con:
            row = _fetchone(con, "SELECT user_id, username FROM membres WHERE LOWER(username)=? AND chat_id=?", (pseudo, chat_id))
            if not row:
                row = _fetchone(con, "SELECT user_id, username FROM membres WHERE LOWER(username)=? LIMIT 1", (pseudo,))
        if not row:
            await update.message.reply_text(f"❌ @{pseudo} introuvable.")
            return
        target_id   = row["user_id"]
        target_name = row["username"] or pseudo

    try:
        with get_db() as con:
            # Agréger toutes les lignes du joueur (tous groupes)
            rows = _fetchall(con, "SELECT * FROM membres WHERE user_id=?", (target_id,))

        if not rows:
            await update.message.reply_text("Tu n'es pas encore enregistré. Envoie un message dans le groupe !")
            return

        # Stats agrégées
        xp_total       = sum((r.get("xp") or 0) for r in rows)
        xp_semaine     = sum((r.get("xp_semaine") or 0) for r in rows)
        messages_total = sum((r.get("messages") or 0) for r in rows)
        quiz_total     = sum((r.get("quiz_gagnes") or 0) for r in rows)
        duels_total    = sum((r.get("combats_gagnes") or 0) for r in rows)
        streak_max     = max((r.get("streak") or 0) for r in rows)

        # Rang basé sur XP total
        ri = get_rang(xp_total)
        xp_next, label_next = xp_prochain_rang(xp_total)

        if xp_next:
            barre      = barre_xp(xp_total)
            barre_info = f"\U0001f4c8 `{barre}` {xp_total:,}/{xp_next:,} XP\n_{label_next}_"
        else:
            barre_info = t("rank_max", lang)

        # Meilleure ligne pour titre et clan
        best = dict(max(rows, key=lambda r: r.get("xp") or 0))
        for k, d in [("titre",""),("succes",""),("clan_id",None)]:
            best.setdefault(k, d)
            if best[k] is None: best[k] = d

        titre       = best.get("titre") or ""
        badge_titre = f"🏷️ Titre : *{titre}*" if titre else ""

        # Clan
        clan_nom = t("no_clan", lang)
        if best.get("clan_id"):
            with get_db() as con:
                clan_row = _fetchone(con, "SELECT nom FROM clans WHERE clan_id=?", (best["clan_id"],))
            if clan_row:
                clan_nom = clan_row["nom"]

        # Rang mondial
        with get_db() as con:
            rang_global = (_fetchone(con, """
                SELECT COUNT(*) AS c FROM (
                    SELECT user_id, SUM(xp) AS total_xp FROM membres GROUP BY user_id
                ) sub WHERE total_xp > ?
            """, (xp_total,)) or {"c": 0})["c"] + 1

        await update.message.reply_text(
            t("profile_title", lang, user=target_name) + "\n\n" +
            t("profile_body", lang,
              emoji=ri["emoji"], label=ri["label"],
              badge_titre=badge_titre,
              clan=clan_nom,
              rang_global=rang_global,
              xp=xp_total,
              xp_semaine=xp_semaine,
              streak=streak_max,
              messages=messages_total,
              quiz=quiz_total,
              combats=duels_total,
              barre_info=barre_info),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"[cmd_rang] {e}", exc_info=True)
        await update.message.reply_text("Erreur chargement profil.")


async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)

    # FIX AMÉLIORATION: pagination avec boutons
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    page = max(1, page)
    offset = (page - 1) * 10
    with get_db() as con:
        membres = _fetchall(con, "SELECT username, xp FROM membres WHERE chat_id=? ORDER BY xp DESC LIMIT 10 OFFSET ?", (chat_id, offset))
        total_joueurs = (_fetchone(con, "SELECT COUNT(*) AS c FROM membres WHERE chat_id=?", (chat_id,)) or {"c": 0})["c"]
        # Position du demandeur
        user = update.effective_user
        ma_position = _fetchone(con, """
            SELECT COUNT(*)+1 AS pos FROM membres
            WHERE chat_id=? AND xp > (SELECT xp FROM membres WHERE user_id=? AND chat_id=?)
        """, (chat_id, user.id, chat_id))
        ma_pos = ma_position["pos"] if ma_position else 0
    if not membres:
        await update.message.reply_text(t("no_members", lang))
        return
    medailles = ["🥇", "🥈", "🥉"]
    texte = t("top_title", lang, clan=clan["nom"] if clan else "Ce groupe")
    for i, row in enumerate(membres):
        r = get_rang(row["xp"])
        m = medailles[i + offset] if (i + offset) < 3 else f"`{i + offset + 1}.`"
        texte += f"{m} *@{row['username']}* — {r['emoji']} {r['rang']} — {row['xp']} XP\n"

    total_pages = max(1, (total_joueurs + 9) // 10)
    texte += f"\n📄 Page {page}/{total_pages} • 🌍 Tu es #{ma_pos}"

    keyboard = []
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(f"⬅ Page {page-1}", callback_data=f"top_{page-1}_{chat_id}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(f"Page {page+1} ➡", callback_data=f"top_{page+1}_{chat_id}"))
    if nav:
        keyboard = [nav]

    await update.message.reply_text(
        texte,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        parse_mode="Markdown"
    )


async def callback_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback pour la pagination du /top."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    page    = int(parts[1])
    chat_id = int(parts[2])
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)
    offset  = (page - 1) * 10
    with get_db() as con:
        membres = _fetchall(con, "SELECT username, xp FROM membres WHERE chat_id=? ORDER BY xp DESC LIMIT 10 OFFSET ?", (chat_id, offset))
        total_joueurs = (_fetchone(con, "SELECT COUNT(*) AS c FROM membres WHERE chat_id=?", (chat_id,)) or {"c": 0})["c"]
        user = update.effective_user
        ma_position = _fetchone(con, """
            SELECT COUNT(*)+1 AS pos FROM membres
            WHERE chat_id=? AND xp > (SELECT xp FROM membres WHERE user_id=? AND chat_id=?)
        """, (chat_id, user.id, chat_id))
        ma_pos = ma_position["pos"] if ma_position else 0
    medailles = ["🥇", "🥈", "🥉"]
    texte = t("top_title", lang, clan=clan["nom"] if clan else "Ce groupe")
    for i, row in enumerate(membres):
        r = get_rang(row["xp"])
        m = medailles[i + offset] if (i + offset) < 3 else f"`{i + offset + 1}.`"
        texte += f"{m} *@{row['username']}* — {r['emoji']} {r['rang']} — {row['xp']} XP\n"
    total_pages = max(1, (total_joueurs + 9) // 10)
    texte += f"\n📄 Page {page}/{total_pages} • 🌍 Tu es #{ma_pos}"
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(f"⬅ Page {page-1}", callback_data=f"top_{page-1}_{chat_id}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(f"Page {page+1} ➡", callback_data=f"top_{page+1}_{chat_id}"))
    keyboard = [nav] if nav else []
    try:
        await query.edit_message_text(
            texte,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[callback_top] {e}")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)
    with get_db() as con:
        membres = _fetchall(con, "SELECT username, xp_semaine FROM membres WHERE chat_id=? ORDER BY xp_semaine DESC LIMIT 10", (chat_id,))
    if not membres:
        await update.message.reply_text(t("no_members", lang))
        return
    medailles = ["🥇", "🥈", "🥉"]
    texte = t("weekly_title", lang, clan=clan["nom"] if clan else "Ce groupe")
    for i, row in enumerate(membres):
        m = medailles[i] if i < 3 else f"`{i+1}.`"
        texte += f"{m} *@{row['username']}* — {row.get('xp_semaine') or 0} XP\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


async def cmd_globalrank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    with get_db() as con:
        if USE_POSTGRES:
            joueurs = _fetchall(con, """
                SELECT m.user_id,
                       MAX(m.username) AS username,
                       SUM(m.xp) AS xp_total,
                       STRING_AGG(DISTINCT c.nom, ', ') AS clans_list
                FROM membres m
                LEFT JOIN clans c ON m.chat_id = c.chat_id
                GROUP BY m.user_id
                ORDER BY xp_total DESC LIMIT 25
            """)
        else:
            joueurs = _fetchall(con, """
                SELECT m.user_id,
                       MAX(m.username) AS username,
                       SUM(m.xp) AS xp_total,
                       GROUP_CONCAT(DISTINCT c.nom) AS clans_list
                FROM membres m
                LEFT JOIN clans c ON m.chat_id = c.chat_id
                GROUP BY m.user_id
                ORDER BY xp_total DESC LIMIT 25
            """)
    if not joueurs:
        await update.message.reply_text(t("no_players", lang))
        return
    medailles = ["🥇", "🥈", "🥉"]
    texte = t("global_title", lang, univers=UNIVERS_NOM)
    for i, row in enumerate(joueurs):
        xp_total  = row["xp_total"] or 0
        r         = get_rang(xp_total)
        m         = medailles[i] if i < 3 else f"`{i+1}.`"
        clans_str = row["clans_list"] if row.get("clans_list") else t("no_clan_tag", lang)
        texte    += f"{m} *@{row['username']}* {r['emoji']} — {xp_total} XP — _{clans_str}_\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


async def cmd_worldtop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    with get_db() as con:
        clans = _fetchall(con, "SELECT nom, points FROM clans ORDER BY points DESC LIMIT 10")
    if not clans:
        await update.message.reply_text(t("no_clans", lang))
        return
    medailles = ["🥇", "🥈", "🥉"]
    texte = t("worldtop_title", lang, univers=UNIVERS_NOM)
    for i, row in enumerate(clans):
        m = medailles[i] if i < 3 else f"`{i+1}.`"
        texte += f"{m} *{row['nom']}* — ⚡ {row['points']} pts\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


# ─────────────────────────────────────────
# 🎁 DAILY REWARD
# ─────────────────────────────────────────

async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)
    m       = get_membre_db(user.id, chat_id, user.first_name)
    now     = datetime.now()
    # FIX BUG: comparaison par date calendaire, pas par secondes
    dernier = m.get("dernier_daily")
    streak  = m.get("streak") or 0
    if dernier:
        dernier_dt = datetime.fromisoformat(dernier)
        diff_sec   = (now - dernier_dt).total_seconds()
        if dernier_dt.date() == now.date():
            # Même jour calendaire — déjà réclamé
            reste_sec = 86400 - diff_sec
            h  = int(reste_sec // 3600)
            mn = int((reste_sec % 3600) // 60)
            await update.message.reply_text(t("daily_already", lang, reste=h, min=mn), parse_mode="Markdown")
            return
        # Streak : si moins de 48h on continue, sinon reset
        if diff_sec < 172800:
            streak += 1
        else:
            streak = 1
    else:
        streak = 1
    # Multiplicateur streak
    mult = 1
    if streak >= 30:  mult = 3
    elif streak >= 14: mult = 2.5
    elif streak >= 7:  mult = 2
    elif streak >= 3:  mult = 1.5
    xp_gain      = int(XP_DAILY_BASE * mult)
    ancien        = get_rang(m["xp"])
    nouveau_xp    = m["xp"] + xp_gain
    nouveau_rang  = get_rang(nouveau_xp)
    update_membre(user.id, chat_id,
                  xp=nouveau_xp,
                  xp_semaine=(m.get("xp_semaine") or 0) + xp_gain,
                  streak=streak,
                  rang=nouveau_rang["rang"],
                  dernier_daily=now.isoformat())
    if get_clan(chat_id):
        update_clan_points(chat_id, xp_gain // 10)
    asyncio.create_task(_progresser_quetes(user.id, chat_id, context, "daily"))
    asyncio.create_task(_progresser_quetes(user.id, chat_id, context, "streak", streak))
    asyncio.create_task(_verifier_succes(context, user.id, chat_id, m, nouveau_xp))
    bonus_txt  = t("daily_bonus", lang, mult=mult) if mult > 1 else ""
    montee_txt = t("rank_up", lang, label=nouveau_rang["label"]) if nouveau_rang["rang"] != ancien["rang"] else ""
    await update.message.reply_text(
        t("daily_reward", lang,
          user=user.username or user.first_name,
          xp=xp_gain, streak=streak,
          bonus_txt=bonus_txt + montee_txt,
          total=nouveau_xp),
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 💸 TRANSFERT XP AVEC CONFIRMATION
# ─────────────────────────────────────────

async def cmd_give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)
    if len(context.args) != 2:
        await update.message.reply_text(t("give_usage", lang), parse_mode="Markdown")
        return
    try:
        montant = int(context.args[1])
    except ValueError:
        await update.message.reply_text(t("give_not_int", lang))
        return
    if montant <= 0:
        await update.message.reply_text(t("give_zero", lang))
        return
    cible_username = context.args[0].lstrip("@")
    with get_db() as con:
        donneur = _fetchone(con, "SELECT * FROM membres WHERE user_id=? AND chat_id=?", (user.id, chat_id))
        cible   = _fetchone(con, "SELECT * FROM membres WHERE LOWER(username)=? AND chat_id=?", (cible_username.lower(), chat_id))
    if not donneur:
        await update.message.reply_text(t("give_not_registered", lang))
        return
    if not cible:
        await update.message.reply_text(t("give_not_found", lang, cible=cible_username), parse_mode="Markdown")
        return
    if donneur["user_id"] == cible["user_id"]:
        await update.message.reply_text(t("give_self", lang))
        return
    dernier_give = donneur.get("dernier_give")
    if dernier_give:
        diff = (datetime.now() - datetime.fromisoformat(dernier_give)).total_seconds()
        if diff < COOLDOWN_GIVE_SECONDES:
            reste = int((COOLDOWN_GIVE_SECONDES - diff) / 60)
            await update.message.reply_text(t("give_cooldown", lang, reste=max(1, reste)), parse_mode="Markdown")
            return
    if donneur["xp"] < montant:
        await update.message.reply_text(t("give_not_enough", lang, xp=donneur["xp"]), parse_mode="Markdown")
        return
    taxe        = max(1, int(montant * TAXE_GIVE_POURCENT / 100))
    montant_net = montant - taxe
    donneur_nom  = user.username or user.first_name
    receveur_nom = cible["username"]
    keyboard = [[
        InlineKeyboardButton(t("give_accept_btn", lang), callback_data=f"give_accept"),
        InlineKeyboardButton(t("give_refuse_btn", lang), callback_data=f"give_refuse"),
    ]]
    msg = await update.message.reply_text(
        t("give_pending", lang,
          receveur=receveur_nom, donneur=donneur_nom,
          montant=montant, taxe=taxe, net=montant_net,
          timeout=GIVE_ACCEPT_TIMEOUT_SEC),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    transferts_pending[msg.message_id] = {
        "donneur_id":   user.id,
        "donneur_nom":  donneur_nom,
        "receveur_id":  cible["user_id"],
        "receveur_nom": receveur_nom,
        "chat_id":      chat_id,
        "montant":      montant,
        "montant_net":  montant_net,
        "taxe":         taxe,
        "msg_id":       msg.message_id,
    }
    context.job_queue.run_once(
        _give_timeout,
        GIVE_ACCEPT_TIMEOUT_SEC,
        data={"msg_id": msg.message_id, "chat_id": chat_id, "receveur_nom": receveur_nom},
        name=f"give_timeout_{msg.message_id}"
    )


async def _give_timeout(context: ContextTypes.DEFAULT_TYPE):
    data   = context.job.data
    msg_id = data["msg_id"]
    if msg_id not in transferts_pending:
        return
    transferts_pending.pop(msg_id, None)
    try:
        await context.bot.edit_message_text(
            chat_id=data["chat_id"], message_id=msg_id,
            text=t("give_timeout", "fr", receveur=data["receveur_nom"]),
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_give_timeout] {e}")


async def callback_give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    msg_id  = query.message.message_id
    await query.answer()
    if msg_id not in transferts_pending:
        await query.edit_message_text("⚠️ Ce transfert a expiré.", reply_markup=None)
        return
    tr   = transferts_pending[msg_id]
    lang = get_user_lang(user.id, chat_id)
    # FIX BUG: seuls le donneur et le receveur peuvent interagir
    if user.id not in (tr["donneur_id"], tr["receveur_id"]):
        await query.answer(t("give_not_yours", lang), show_alert=True)
        return
    # Seul le receveur peut accepter
    if query.data == "give_accept" and user.id != tr["receveur_id"]:
        await query.answer("❌ Seul le destinataire peut accepter !", show_alert=True)
        return
    for job in context.job_queue.get_jobs_by_name(f"give_timeout_{msg_id}"):
        job.schedule_removal()
    transferts_pending.pop(msg_id, None)
    if query.data == "give_refuse":
        await query.edit_message_text(
            t("give_refused", lang, receveur=tr["receveur_nom"]),
            reply_markup=None, parse_mode="Markdown"
        )
        return
    # Accepté — vérifier que le donneur a toujours assez
    with get_db() as con:
        donneur_row = _fetchone(con, "SELECT * FROM membres WHERE user_id=? AND chat_id=?", (tr["donneur_id"], chat_id))
    if not donneur_row or donneur_row["xp"] < tr["montant"]:
        await query.edit_message_text("❌ Le donneur n'a plus assez d'XP !", reply_markup=None)
        return
    nouveau_xp_donneur = donneur_row["xp"] - tr["montant"]
    with get_db() as con:
        receveur_row = _fetchone(con, "SELECT * FROM membres WHERE user_id=? AND chat_id=?", (tr["receveur_id"], chat_id))
    nouveau_xp_receveur = (receveur_row["xp"] if receveur_row else 0) + tr["montant_net"]
    # FIX AMÉLIORATION: notifier la montée de rang du receveur
    ancien_rang_recv = get_rang(receveur_row["xp"] if receveur_row else 0)
    nouveau_rang_recv = get_rang(nouveau_xp_receveur)
    update_membre(tr["donneur_id"], chat_id,
                  xp=nouveau_xp_donneur, rang=get_rang(nouveau_xp_donneur)["rang"],
                  dernier_give=datetime.now().isoformat())
    update_membre(tr["receveur_id"], chat_id,
                  xp=nouveau_xp_receveur, rang=nouveau_rang_recv["rang"])
    montee_txt = t("rank_up", lang, label=nouveau_rang_recv["label"]) if nouveau_rang_recv["rang"] != ancien_rang_recv["rang"] else ""
    await query.edit_message_text(
        t("give_accepted", lang,
          donneur=tr["donneur_nom"], receveur=tr["receveur_nom"],
          montant=tr["montant_net"], taxe=tr["taxe"],
          restant=nouveau_xp_donneur) + montee_txt,
        reply_markup=None, parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 🏪 SHOP — avec vérification d'ownership
# ─────────────────────────────────────────

async def cmd_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    texte = t("shop_title", lang)
    for item in SHOP_TITRES:
        texte += f"{item['emoji']} *{item['titre']}* — `{item['prix']:,} XP`\n  `/buy {item['id']}`\n\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)
    if not context.args:
        await update.message.reply_text("❌ Usage : `/buy ID`\nEx : `/buy kage`\nTape `/shop` pour la liste.", parse_mode="Markdown")
        return
    item_id = context.args[0].lower()
    item    = next((i for i in SHOP_TITRES if i["id"] == item_id), None)
    if not item:
        await update.message.reply_text(t("equip_notfound", lang), parse_mode="Markdown")
        return
    m = get_membre_db(user.id, chat_id, user.first_name)
    # FIX AMÉLIORATION: vérifier si déjà possédé
    titres_possedes = m.get("titres_possedes") or ""
    if item_id in titres_possedes.split(","):
        await update.message.reply_text(t("shop_already_owned", lang), parse_mode="Markdown")
        return
    if m["xp"] < item["prix"]:
        await update.message.reply_text(t("shop_not_enough", lang, requis=item["prix"]), parse_mode="Markdown")
        return
    nouveau_xp = m["xp"] - item["prix"]
    # Ajouter à la liste des titres possédés et équiper
    titres_list = [x for x in titres_possedes.split(",") if x]
    titres_list.append(item_id)
    update_membre(user.id, chat_id,
                  xp=nouveau_xp,
                  rang=get_rang(nouveau_xp)["rang"],
                  titre=item["titre"],
                  titres_possedes=",".join(titres_list))
    await update.message.reply_text(
        t("shop_bought", lang, titre=item["titre"], xp=item["prix"], id=item_id),
        parse_mode="Markdown"
    )


async def cmd_equip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)
    if not context.args:
        await update.message.reply_text("❌ Usage : `/equip ID`", parse_mode="Markdown")
        return
    item_id = context.args[0].lower()
    item    = next((i for i in SHOP_TITRES if i["id"] == item_id), None)
    if not item:
        await update.message.reply_text(t("equip_notfound", lang), parse_mode="Markdown")
        return
    # FIX BUG: vérifier que le joueur possède bien ce titre
    m = get_membre_db(user.id, chat_id, user.first_name)
    titres_possedes = m.get("titres_possedes") or ""
    if item_id not in titres_possedes.split(","):
        await update.message.reply_text(
            f"❌ Tu ne possèdes pas ce titre ! Achète-le d'abord avec `/buy {item_id}`.",
            parse_mode="Markdown"
        )
        return
    update_membre(user.id, chat_id, titre=item["titre"])
    await update.message.reply_text(t("equip_done", lang, titre=item["titre"]), parse_mode="Markdown")


# ─────────────────────────────────────────
# 🎯 QUIZ
# ─────────────────────────────────────────

async def _lancer_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str):
    chat_id = update.effective_chat.id
    user    = update.effective_user
    lang    = get_user_lang(user.id, chat_id)
    if chat_id in quiz_en_cours:
        await update.message.reply_text(t("quiz_already", lang))
        return
    m = get_membre_db(user.id, chat_id, user.first_name)
    dernier_quiz = m.get("dernier_quiz")
    if dernier_quiz:
        diff = (datetime.now() - datetime.fromisoformat(dernier_quiz)).total_seconds()
        if diff < COOLDOWN_QUIZ_SECONDES:
            reste = int(COOLDOWN_QUIZ_SECONDES - diff)
            await update.message.reply_text(t("quiz_cooldown", lang, reste=reste), parse_mode="Markdown")
            return
    if mode == "hardcore":
        xp_g, pts_g = XP_PAR_QUIZ_HARDCORE, POINTS_QUIZ_HARDCORE
        header_key = "quiz_hc_header"
    elif mode == "mystere":
        xp_g, pts_g = XP_MYSTERE, POINTS_QUIZ_NORMAL
        header_key = "quiz_mystere_header"
    else:
        xp_g, pts_g = XP_PAR_QUIZ_NORMAL, POINTS_QUIZ_NORMAL
        header_key = "quiz_normal_header"
    # v9: système anti-répétition + Open Trivia
    q = await _get_question(chat_id, mode)
    keyboard = [
        [InlineKeyboardButton(f"{['🅰️','🅱️','🅲️','🅳️'][i]} {r}", callback_data=f"quiz_{i}")]
        for i, r in enumerate(q["r"])
    ]
    msg = await update.message.reply_text(
        t(header_key, lang, xp=xp_g, question=q["q"], timeout=QUIZ_TIMEOUT_SECONDES),
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )
    # FIX BUG: stocker le lanceur_id pour le cooldown correct
    quiz_en_cours[chat_id] = {
        "q": q, "type": mode, "message_id": msg.message_id,
        "xp": xp_g, "pts": pts_g,
        "lanceur_id": user.id,  # cooldown sur le répondeur dans callback_quiz
    }
    # FIX BUG: cooldown mis sur le lanceur au moment du lancement
    update_membre(user.id, chat_id, dernier_quiz=datetime.now().isoformat())
    context.job_queue.run_once(
        _quiz_timeout, QUIZ_TIMEOUT_SECONDES,
        data={"chat_id": chat_id, "message_id": msg.message_id},
        name=f"quiz_timeout_{chat_id}"
    )


async def _quiz_timeout(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    if chat_id not in quiz_en_cours:
        return
    del quiz_en_cours[chat_id]
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["message_id"],
            text=T["quiz_timeout_msg"]["fr"] + " / " + T["quiz_timeout_msg"]["en"],
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_quiz_timeout] {e}")


async def cmd_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _lancer_quiz(update, context, "normal")

async def cmd_quizhc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _lancer_quiz(update, context, "hardcore")

async def cmd_mystere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _lancer_quiz(update, context, "mystere")


async def callback_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    user    = update.effective_user
    lang    = get_user_lang(user.id, chat_id)
    if chat_id not in quiz_en_cours:
        await query.edit_message_text(t("quiz_expired", lang))
        return
    quiz    = quiz_en_cours[chat_id]
    q       = quiz["q"]
    xp_gain = quiz["xp"]
    pts_gain = quiz["pts"]
    reponse = int(query.data.split("_")[1])
    del quiz_en_cours[chat_id]
    for job in context.job_queue.get_jobs_by_name(f"quiz_timeout_{chat_id}"):
        job.schedule_removal()
    m = get_membre_db(user.id, chat_id, user.first_name)
    ancien_rang = get_rang(m["xp"])
    # FIX BUG: cooldown sur le répondeur, pas seulement sur le lanceur
    update_membre(user.id, chat_id, dernier_quiz=datetime.now().isoformat())
    if reponse == q["b"]:
        nouveau_xp   = m["xp"] + xp_gain
        nouveau_rang = get_rang(nouveau_xp)
        update_membre(user.id, chat_id,
                      xp=nouveau_xp,
                      xp_semaine=(m.get("xp_semaine") or 0) + xp_gain,
                      quiz_gagnes=m["quiz_gagnes"] + 1,
                      rang=nouveau_rang["rang"])
        asyncio.create_task(_progresser_quetes(user.id, chat_id, context, "quiz_gagnes"))
        asyncio.create_task(_verifier_succes(context, user.id, chat_id, m, nouveau_xp))
        if get_clan(chat_id):
            update_clan_points(chat_id, pts_gain)
            add_clanwar_points(chat_id, POINTS_CLANWAR_ACTION)
        montee = t("rank_up", lang, label=nouveau_rang["label"]) if nouveau_rang["rang"] != ancien_rang["rang"] else ""
        mode   = quiz["type"]
        if mode == "hardcore":
            label = t("quiz_label_hc", lang)
        elif mode == "mystere":
            label = t("quiz_label_mystere", lang)
        else:
            label = t("quiz_label_normal", lang)
        await query.edit_message_text(
            t("quiz_correct", lang, user=user.username or user.first_name,
              label=label, xp=xp_gain, pts=pts_gain, montee=montee),
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            t("quiz_wrong", lang, user=user.username or user.first_name, bonne=q["r"][q["b"]]),
            parse_mode="Markdown"
        )


# ─────────────────────────────────────────
# ⚔️ DUEL — CORRIGÉ: les deux joueurs répondent
# ─────────────────────────────────────────

def _nettoyer_texte(texte: str) -> str:
    return re.sub(r'\*+', '', texte).strip()


async def _envoyer_question_duel(context, chat_id: int, lang: str = "fr"):
    if chat_id not in combats_en_cours:
        return
    combat = combats_en_cours[chat_id]
    if combat.get("phase") != "duel":
        return
    q_idx = combat["question_idx"]
    if q_idx >= len(combat["questions"]):
        await _finir_duel(context, chat_id, lang)
        return
    question = combat["questions"][q_idx]
    keyboard = [
        [InlineKeyboardButton(f"{['🅰️','🅱️','🅲️','🅳️'][i]} {r}", callback_data=f"duel_{i}")]
        for i, r in enumerate(question["r"])
    ]
    c_id, a_id = combat["challenger_id"], combat["adversaire_id"]
    c_nom, a_nom = combat["challenger_nom"], combat["adversaire_nom"]
    sc = combat["scores"][c_id]
    sa = combat["scores"][a_id]
    msg = await context.bot.send_message(
        chat_id,
        f"❓ *Question {q_idx + 1}/{len(combat['questions'])}*\n\n"
        f"{question['q']}\n\n"
        f"🔵 @{c_nom} : {sc} | 🔴 @{a_nom} : {sa}\n\n"
        f"⏱️ {DUEL_TIMEOUT_SEC}s — Les deux joueurs répondent !",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    # FIX BUG: réponses des DEUX joueurs trackées séparément
    combat["reponses"] = {}   # {user_id: reponse_index}
    combat["current_msg_id"] = msg.message_id
    context.job_queue.run_once(
        _duel_question_timeout, DUEL_TIMEOUT_SEC,
        data={"chat_id": chat_id, "q_idx": q_idx, "msg_id": msg.message_id},
        name=f"duel_q_{chat_id}"
    )


async def _duel_question_timeout(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    if chat_id not in combats_en_cours:
        return
    combat = combats_en_cours[chat_id]
    if combat.get("question_idx") != data["q_idx"]:
        return
    # Timeout: avancer à la prochaine question sans pénaliser
    c_id = combat["challenger_id"]
    a_id = combat["adversaire_id"]
    lang = "fr"
    reponses = combat.get("reponses", {})
    question = combat["questions"][data["q_idx"]]
    bonne = _nettoyer_texte(question["r"][question["b"]])
    # Attribuer les points à ceux qui ont répondu correctement
    for uid in (c_id, a_id):
        if reponses.get(uid) == question["b"]:
            combat["scores"][uid] += 1
    sc = combat["scores"][c_id]
    sa = combat["scores"][a_id]
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["msg_id"],
            text=f"⏰ *Temps écoulé !*\nRéponse : *{bonne}*\n\n🔵 @{combat['challenger_nom']} : {sc} | 🔴 @{combat['adversaire_nom']} : {sa}",
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_duel_question_timeout] {e}")
    combat["question_idx"] += 1
    await asyncio.sleep(1.5)
    await _envoyer_question_duel(context, chat_id, lang)


async def _finir_duel(context, chat_id: int, lang: str):
    if chat_id not in combats_en_cours:
        return
    combat = combats_en_cours.pop(chat_id)
    c_id, a_id = combat["challenger_id"], combat["adversaire_id"]
    c_nom, a_nom = combat["challenger_nom"], combat["adversaire_nom"]
    sc = combat["scores"][c_id]
    sa = combat["scores"][a_id]
    score_txt = f"🔵 @{c_nom} : {sc} | 🔴 @{a_nom} : {sa}"
    if sc > sa:
        gagnant_id, gagnant_nom = c_id, c_nom
        perdant_id,  perdant_nom  = a_id, a_nom
    elif sa > sc:
        gagnant_id, gagnant_nom = a_id, a_nom
        perdant_id,  perdant_nom  = c_id, c_nom
    else:
        # Égalité
        await context.bot.send_message(
            chat_id,
            f"🤝 *DUEL TERMINÉ — ÉGALITÉ !*\n\n{score_txt}\n\nBonne partie à tous !",
            parse_mode="Markdown"
        )
        return
    mg = get_membre_db(gagnant_id, chat_id, gagnant_nom)
    ancien_rang  = get_rang(mg["xp"])
    nouveau_xp   = mg["xp"] + XP_PAR_COMBAT_WIN
    nouveau_rang = get_rang(nouveau_xp)
    update_membre(gagnant_id, chat_id,
                  xp=nouveau_xp,
                  xp_semaine=(mg.get("xp_semaine") or 0) + XP_PAR_COMBAT_WIN,
                  combats_gagnes=mg["combats_gagnes"] + 1,
                  rang=nouveau_rang["rang"])
    asyncio.create_task(_progresser_quetes(gagnant_id, chat_id, context, "combats_gagnes"))
    asyncio.create_task(_verifier_succes(context, gagnant_id, chat_id, mg, nouveau_xp))
    if get_clan(chat_id):
        update_clan_points(chat_id, POINTS_COMBAT_WIN)
        add_clanwar_points(chat_id, POINTS_CLANWAR_ACTION)
    montee = t("rank_up", lang, label=nouveau_rang["label"]) if nouveau_rang["rang"] != ancien_rang["rang"] else ""
    await context.bot.send_message(
        chat_id,
        f"🏆 *DUEL TERMINÉ !*\n\n🥇 Vainqueur : @{gagnant_nom}\n💀 Vaincu : @{perdant_nom}\n\nScore : {score_txt}\n+{XP_PAR_COMBAT_WIN} XP | +{POINTS_COMBAT_WIN} pts clan{montee}",
        parse_mode="Markdown"
    )


async def cmd_war(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    lang    = get_user_lang(user.id, chat_id)

    # FIX AMÉLIORATION: accepter /war @username en plus du reply
    adversaire = None
    if update.message.reply_to_message:
        adversaire = update.message.reply_to_message.from_user
    elif context.args:
        pseudo = context.args[0].lstrip("@").lower()
        with get_db() as con:
            # Cherche par username exact (insensible à la casse)
            row = _fetchone(con, "SELECT user_id, username FROM membres WHERE LOWER(username)=? AND chat_id=?", (pseudo, chat_id))
            # Fallback: cherche dans tous les groupes si pas trouvé dans ce groupe
            if not row:
                row = _fetchone(con, "SELECT user_id, username FROM membres WHERE LOWER(username)=? LIMIT 1", (pseudo,))
        if not row:
            await update.message.reply_text(
                f"❌ *@{pseudo}* introuvable.\n"
                f"_Assure-toi que le joueur a déjà écrit dans ce groupe._",
                parse_mode="Markdown"
            )
            return
        class FakeUser:
            def __init__(self, uid, uname): self.id = uid; self.username = uname; self.first_name = uname; self.is_bot = False
        adversaire = FakeUser(row["user_id"], row["username"])
    else:
        await update.message.reply_text(t("war_no_reply", lang), parse_mode="Markdown")
        return

    if adversaire.id == user.id:
        await update.message.reply_text(t("war_self", lang))
        return
    if adversaire.is_bot:
        await update.message.reply_text(t("war_vs_bot", lang))
        return
    if chat_id in combats_en_cours:
        await update.message.reply_text(t("war_already", lang))
        return

    # FIX AMÉLIORATION: cooldown sur /war pour éviter le spam
    m_challenger = get_membre_db(user.id, chat_id, user.first_name)
    dernier_combat = m_challenger.get("dernier_combat")
    if dernier_combat:
        diff = (datetime.now() - datetime.fromisoformat(dernier_combat)).total_seconds()
        if diff < COOLDOWN_WAR_SECONDES:
            reste = int((COOLDOWN_WAR_SECONDES - diff) / 60)
            await update.message.reply_text(
                f"⏳ Tu peux relancer un duel dans *{max(1, reste)} min*.",
                parse_mode="Markdown"
            )
            return

    m1 = get_membre_db(user.id, chat_id, user.first_name)
    m2 = get_membre_db(adversaire.id, chat_id, adversaire.username or adversaire.first_name)
    r1, r2 = get_rang(m1["xp"]), get_rang(m2["xp"])
    c_nom  = user.username or user.first_name
    a_nom  = adversaire.username or adversaire.first_name
    combats_en_cours[chat_id] = {
        "phase": "attente",
        "challenger_id": user.id, "challenger_nom": c_nom,
        "adversaire_id": adversaire.id, "adversaire_nom": a_nom,
    }
    keyboard = [[
        InlineKeyboardButton("⚔️ J'accepte !", callback_data="war_accept"),
        InlineKeyboardButton("🏳️ Je refuse",   callback_data="war_refuse"),
    ]]
    msg = await update.message.reply_text(
        f"⚔️ *DÉFI !*\n\n{r1['emoji']} @{c_nom} ({r1['rang']}) défie\n{r2['emoji']} @{a_nom} ({r2['rang']})\n\n"
        f"🏆 +{XP_PAR_COMBAT_WIN} XP | +{POINTS_COMBAT_WIN} pts clan\n\n*@{a_nom}*, tu acceptes ? ({COMBAT_TIMEOUT_SECONDES}s)",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )
    combats_en_cours[chat_id]["message_id"] = msg.message_id
    context.job_queue.run_once(
        _combat_timeout, COMBAT_TIMEOUT_SECONDES,
        data={"chat_id": chat_id, "message_id": msg.message_id},
        name=f"combat_timeout_{chat_id}"
    )


async def _combat_timeout(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    if chat_id not in combats_en_cours:
        return
    combat = combats_en_cours.pop(chat_id)
    if combat.get("phase") != "attente":
        return
    texte = f"⏰ @{combat['adversaire_nom']} n'a pas répondu. Combat annulé."
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=data["message_id"], text=texte, reply_markup=None)
    except Exception:
        try:
            await context.bot.send_message(chat_id, texte)
        except Exception as e:
            logger.warning(f"[_combat_timeout] {e}")


async def callback_war(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    chat_id = update.effective_chat.id
    user    = update.effective_user
    data    = query.data
    try:
        lang = get_user_lang(user.id, chat_id)
        if chat_id not in combats_en_cours:
            await query.answer("⚠️ Ce combat a expiré.", show_alert=True)
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        combat = combats_en_cours[chat_id]
        phase  = combat.get("phase", "?")
        if phase == "attente":
            if user.id != combat["adversaire_id"]:
                await query.answer("❌ Ce n'est pas ton combat !", show_alert=True)
                return
            await query.answer()
            for job in context.job_queue.get_jobs_by_name(f"combat_timeout_{chat_id}"):
                job.schedule_removal()
            if data == "war_refuse":
                combats_en_cours.pop(chat_id, None)
                txt = f"🏳️ @{combat['adversaire_nom']} a refusé. Lâcheur !"
                try:
                    await query.edit_message_text(txt, reply_markup=None)
                except Exception:
                    await context.bot.send_message(chat_id, txt)
                return
            if data != "war_accept":
                return
            questions = random.sample(QUIZ_DUEL, DUEL_NB_QUESTIONS)
            c_id, a_id = combat["challenger_id"], combat["adversaire_id"]
            c_nom, a_nom = combat["challenger_nom"], combat["adversaire_nom"]
            combats_en_cours[chat_id] = {
                "phase": "duel",
                "challenger_id": c_id, "challenger_nom": c_nom,
                "adversaire_id": a_id, "adversaire_nom": a_nom,
                "questions": questions, "question_idx": 0,
                "scores": {c_id: 0, a_id: 0},
                "reponses": {},   # FIX: réponses des deux joueurs
                "current_msg_id": None,
            }
            # Enregistrer le cooldown sur le challenger
            update_membre(c_id, chat_id, dernier_combat=datetime.now().isoformat())
            txt_accept = f"DUEL ACCEPTÉ !\n\n🔵 @{c_nom} VS 🔴 @{a_nom}\n\n{DUEL_NB_QUESTIONS} questions — Les DEUX joueurs répondent !\n+{XP_PAR_COMBAT_WIN} XP au vainqueur\n\nQue le meilleur gagne !"
            try:
                await query.edit_message_text(txt_accept, reply_markup=None)
            except Exception:
                pass
                await asyncio.sleep(1.5)
            await _envoyer_question_duel(context, chat_id, lang)
            return

        if phase == "duel":
            if data in ("war_accept", "war_refuse"):
                await query.answer("Le duel est en cours !", show_alert=True)
                return
            if user.id not in (combat["challenger_id"], combat["adversaire_id"]):
                await query.answer("❌ Tu ne fais pas partie de ce duel !", show_alert=True)
                return
            # FIX BUG: vérifier si cet user a déjà répondu à cette question
            reponses = combat.get("reponses", {})
            if user.id in reponses:
                await query.answer("⚡ Tu as déjà répondu !", show_alert=True)
                return
            try:
                reponse = int(data.split("_")[1])
            except (ValueError, IndexError):
                await query.answer("⚠️ Bouton invalide.", show_alert=True)
                return
            await query.answer()
            reponses[user.id] = reponse
            combat["reponses"] = reponses
            user_nom = user.username or user.first_name

            # Afficher feedback immédiat à ce joueur (edit silencieux)
            question = combat["questions"][combat["question_idx"]]
            correct  = reponse == question["b"]
            c_id, a_id = combat["challenger_id"], combat["adversaire_id"]

            # Si les DEUX ont répondu, avancer
            both_answered = c_id in reponses and a_id in reponses
            if both_answered:
                for job in context.job_queue.get_jobs_by_name(f"duel_q_{chat_id}"):
                    job.schedule_removal()
                # Calculer les points
                for uid in (c_id, a_id):
                    if reponses.get(uid) == question["b"]:
                        combat["scores"][uid] += 1
                sc = combat["scores"][c_id]
                sa = combat["scores"][a_id]
                bonne_rep = _nettoyer_texte(question["r"][question["b"]])
                c_correct = "✅" if reponses.get(c_id) == question["b"] else "❌"
                a_correct = "✅" if reponses.get(a_id) == question["b"] else "❌"
                msg_res = (
                    f"📊 *Résultat Q{combat['question_idx']+1}*\n\n"
                    f"Réponse : *{bonne_rep}*\n\n"
                    f"🔵 @{combat['challenger_nom']} {c_correct} : {sc} pts\n"
                    f"🔴 @{combat['adversaire_nom']} {a_correct} : {sa} pts"
                )
                try:
                    await query.edit_message_text(msg_res, reply_markup=None, parse_mode="Markdown")
                except Exception:
                    pass
                combat["question_idx"] += 1
                await asyncio.sleep(1.5)
                await _envoyer_question_duel(context, chat_id, lang)
            else:
                # Un seul a répondu, montrer qu'on attend l'autre
                autre_nom = combat["adversaire_nom"] if user.id == c_id else combat["challenger_nom"]
                try:
                    await query.edit_message_text(
                        query.message.text + f"\n\n{'✅' if correct else '❌'} @{user_nom} a répondu — ⏳ Attente de @{autre_nom}...",
                        reply_markup=query.message.reply_markup,
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            return
        await query.answer("⚠️ Phase inconnue.", show_alert=True)
    except Exception as e:
        logger.error(f"[callback_war] {e}", exc_info=True)
        try:
            await query.answer("❌ Erreur interne.", show_alert=True)
        except Exception:
            pass


# ─────────────────────────────────────────
# 🏴 CLAN
# ─────────────────────────────────────────

async def cmd_createclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    lang = get_user_lang(user.id, chat.id)
    if chat.type == "private":
        await update.message.reply_text(t("group_only", lang))
        return
    if not context.args:
        await update.message.reply_text("❌ Usage : `/createclan NomDuClan`", parse_mode="Markdown")
        return
    clan_existant = get_clan_of_user(user.id, chat.id)
    if clan_existant:
        await update.message.reply_text(
            f"⚠️ Tu es déjà dans le clan *{clan_existant['nom']}* !\nFais `/leaveclan` pour le quitter d'abord.",
            parse_mode="Markdown"
        )
        return
    nom = " ".join(context.args)[:50]
    with get_db() as con:
        existing = _fetchone(con, "SELECT clan_id FROM clans WHERE LOWER(nom)=?", (nom.lower(),))
    if existing:
        await update.message.reply_text(f"⚠️ Un clan *{nom}* existe déjà ! Choisis un autre nom.", parse_mode="Markdown")
        return
    clan_id = create_clan_db(nom, user.id, chat.id)
    get_membre_db(user.id, chat.id, user.username or user.first_name)
    with get_db() as con:
        _execute(con, "UPDATE membres SET clan_id=? WHERE user_id=? AND chat_id=?", (clan_id, user.id, chat.id))
    await update.message.reply_text(
        f"🎉 *Clan créé !*\n\n⚔️ *{nom}* — ID : `{clan_id}`\n👑 Chef : @{user.username or user.first_name}\n\n"
        f"📢 Partage cet ID pour que tes amis rejoignent :\n`/joinclan {clan_id}`\n\n"
        f"⚠️ Il faut *{MEMBRES_MINIMUM_CLANWAR} membres* pour participer aux Clan Wars !",
        parse_mode="Markdown"
    )


async def cmd_joinclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    lang = get_user_lang(user.id, chat.id)
    if chat.type == "private":
        await update.message.reply_text(t("group_only", lang))
        return
    if not context.args:
        await update.message.reply_text("❌ Usage : `/joinclan ID_DU_CLAN`", parse_mode="Markdown")
        return
    try:
        clan_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID invalide.")
        return
    clan = get_clan_by_id(clan_id)
    if not clan:
        await update.message.reply_text("❌ Clan introuvable. Vérifie l'ID.")
        return
    clan_existant = get_clan_of_user(user.id, chat.id)
    if clan_existant:
        if clan_existant["clan_id"] == clan_id:
            await update.message.reply_text(f"✅ Tu es déjà dans *{clan['nom']}* !", parse_mode="Markdown")
        else:
            await update.message.reply_text(
                f"⚠️ Tu es déjà dans *{clan_existant['nom']}* !\nFais `/leaveclan` pour le quitter d'abord.",
                parse_mode="Markdown"
            )
        return
    get_membre_db(user.id, chat.id, user.username or user.first_name)
    with get_db() as con:
        _execute(con, "UPDATE membres SET clan_id=? WHERE user_id=? AND chat_id=?", (clan_id, user.id, chat.id))
    nb = get_clan_member_count(clan_id)
    reconnu = nb >= MEMBRES_MINIMUM_CLANWAR
    statut = f"✅ Clan reconnu ({nb}/{MEMBRES_MINIMUM_CLANWAR})" if reconnu else f"⏳ {nb}/{MEMBRES_MINIMUM_CLANWAR} membres pour être reconnu"
    await update.message.reply_text(
        f"⚔️ *@{user.username or user.first_name}* a rejoint *{clan['nom']}* !\n\n{statut}",
        parse_mode="Markdown"
    )


async def cmd_leaveclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    clan = get_clan_of_user(user.id, chat.id)
    if not clan:
        await update.message.reply_text("❌ Tu n'es dans aucun clan !")
        return
    nb = get_clan_member_count(clan["clan_id"])
    if clan.get("chef_id") == user.id and nb > 1:
        await update.message.reply_text(
            f"⚠️ Tu es le chef de *{clan['nom']}* ({nb} membres).\nTransfère la direction ou expulse les membres d'abord.",
            parse_mode="Markdown"
        )
        return
    with get_db() as con:
        _execute(con, "UPDATE membres SET clan_id=NULL WHERE user_id=? AND chat_id=?", (user.id, chat.id))
    nb_restants = get_clan_member_count(clan["clan_id"])
    if nb_restants == 0:
        with get_db() as con:
            _execute(con, "DELETE FROM clans WHERE clan_id=?", (clan["clan_id"],))
        await update.message.reply_text(f"🗑️ Tu as quitté *{clan['nom']}* — clan dissous (plus de membres).", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"👋 Tu as quitté *{clan['nom']}*.", parse_mode="Markdown")


async def cmd_myclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    clan = get_clan_of_user(user.id, chat.id)
    if not clan:
        await update.message.reply_text("❌ Tu n'es dans aucun clan !\nUtilise `/createclan NomDuClan` ou `/joinclan ID`.", parse_mode="Markdown")
        return
    nb      = get_clan_member_count(clan["clan_id"])
    reconnu = nb >= MEMBRES_MINIMUM_CLANWAR
    statut  = "✅ Reconnu — peut faire des Clan Wars" if reconnu else f"⏳ {nb}/{MEMBRES_MINIMUM_CLANWAR} membres pour être reconnu"
    chef_mention = f"ID `{clan['chef_id']}`"
    with get_db() as con:
        chef_row = _fetchone(con, "SELECT username FROM membres WHERE user_id=?", (clan["chef_id"],))
    if chef_row:
        chef_mention = f"@{chef_row['username']}"
    date = datetime.fromisoformat(clan["created_at"]).strftime("%d/%m/%Y") if clan.get("created_at") else "?"
    await update.message.reply_text(
        f"⚔️ *{clan['nom']}*\n🆔 ID : `{clan['clan_id']}`\n👑 Chef : {chef_mention}\n"
        f"👥 Membres : *{nb}*\n⚡ Points : *{clan['points']}*\n📅 Créé : {date}\n\n{statut}",
        parse_mode="Markdown"
    )


async def cmd_clanrank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    with get_db() as con:
        clans = _fetchall(con, "SELECT clan_id, nom, points FROM clans ORDER BY points DESC LIMIT 10")
    if not clans:
        await update.message.reply_text("🌍 Aucun clan enregistré !")
        return
    medailles = ["🥇", "🥈", "🥉"]
    texte = f"🌍 *CLASSEMENT CLANS — {UNIVERS_NOM}*\n\n"
    for i, row in enumerate(clans):
        nb = get_clan_member_count(row["clan_id"])
        m = medailles[i] if i < 3 else f"`{i+1}.`"
        reconnu = "✅" if nb >= MEMBRES_MINIMUM_CLANWAR else "⏳"
        texte += f"{m} {reconnu} *{row['nom']}* — ⚡ {row['points']} pts — 👥 {nb}\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


async def cmd_resetclans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    with get_db() as con:
        _execute(con, "UPDATE clans SET points=0")
    await update.message.reply_text("✅ Points de tous les clans remis à zéro !")


async def cmd_renameclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    lang = get_user_lang(user.id, chat.id)
    if chat.type == "private":
        await update.message.reply_text(t("group_only", lang))
        return
    if not context.args:
        await update.message.reply_text(t("renameclan_usage", lang), parse_mode="Markdown")
        return
    clan = get_clan_of_user(user.id, chat.id)
    if not clan or clan.get("chef_id") != user.id:
        await update.message.reply_text("❌ Tu dois être **chef du clan** pour le renommer !", parse_mode="Markdown")
        return
    nom = " ".join(context.args)[:50]
    with get_db() as con:
        _execute(con, "UPDATE clans SET nom=? WHERE clan_id=?", (nom, clan["clan_id"]))
    await update.message.reply_text(t("renameclan_done", lang, nom=nom), parse_mode="Markdown")


# ─────────────────────────────────────────
# 🔄 SYSTÈME DE TRANSFERT DE JOUEURS
# ─────────────────────────────────────────

TRANSFERT_TIMEOUT_SEC  = 120
FORCE_TRANSFERT_COUT   = 200
RANG_BLOCAGE_FORCE     = "S"

transferts_joueurs = {}


async def cmd_transfert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Chef vend un joueur à un autre clan : /transfert @joueur ID_clan prix"""
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Commande à utiliser dans un groupe.")
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ Usage : `/transfert @joueur ID_clan prix`\nEx : `/transfert @Naruto 42 500`",
            parse_mode="Markdown"
        )
        return
    clan_chef = get_clan_of_user(user.id, chat_id)
    if not clan_chef or clan_chef.get("chef_id") != user.id:
        await update.message.reply_text("❌ Tu dois être **chef d'un clan** pour transférer un joueur !", parse_mode="Markdown")
        return
    pseudo_cible = context.args[0].lstrip("@").lower()
    try:
        clan_cible_id = int(context.args[1])
        prix          = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ ID clan et prix doivent être des nombres.")
        return
    if prix <= 0:
        await update.message.reply_text("❌ Le prix doit être supérieur à 0.")
        return
    with get_db() as con:
        joueur_row = _fetchone(con, "SELECT * FROM membres WHERE LOWER(username)=? AND chat_id=?", (pseudo_cible, chat_id))
    if not joueur_row:
        await update.message.reply_text(f"❌ @{pseudo_cible} introuvable dans ce groupe.")
        return
    if joueur_row["user_id"] == user.id:
        await update.message.reply_text("😅 Tu ne peux pas te transférer toi-même !")
        return
    joueur_clan = get_clan_of_user(joueur_row["user_id"], chat_id)
    if not joueur_clan or joueur_clan["clan_id"] != clan_chef["clan_id"]:
        await update.message.reply_text(f"❌ @{pseudo_cible} n'est pas dans ton clan !")
        return
    clan_acheteur = get_clan_by_id(clan_cible_id)
    if not clan_acheteur:
        await update.message.reply_text("❌ Clan acheteur introuvable.")
        return
    if clan_acheteur["clan_id"] == clan_chef["clan_id"]:
        await update.message.reply_text("😅 C'est déjà ton clan !")
        return
    if clan_acheteur["points"] < prix:
        await update.message.reply_text(
            f"❌ *{clan_acheteur['nom']}* n'a que *{clan_acheteur['points']} pts* — prix demandé : *{prix} pts*.",
            parse_mode="Markdown"
        )
        return
    rang_joueur = get_rang(joueur_row["xp"])
    keyboard = [[
        InlineKeyboardButton("✅ J'accepte", callback_data="tj_accept"),
        InlineKeyboardButton("❌ Je refuse", callback_data="tj_refuse"),
    ]]
    msg = await update.message.reply_text(
        f"🔄 *OFFRE DE TRANSFERT !*\n\n"
        f"👤 Joueur : @{joueur_row['username']} {rang_joueur['emoji']} {rang_joueur['rang']}\n"
        f"📤 Clan vendeur : *{clan_chef['nom']}*\n"
        f"📥 Clan acheteur : *{clan_acheteur['nom']}*\n"
        f"💰 Prix : *{prix} pts*\n\n"
        f"@{joueur_row['username']}, tu acceptes ce transfert ? ({TRANSFERT_TIMEOUT_SEC}s)",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    transferts_joueurs[msg.message_id] = {
        "type":            "chef_vend",
        "msg_id":          msg.message_id,
        "chat_id":         chat_id,
        "chef_vendeur_id": user.id,
        "chef_vendeur_nom": user.username or user.first_name,
        "joueur_id":       joueur_row["user_id"],
        "joueur_nom":      joueur_row["username"],
        "clan_vendeur_id": clan_chef["clan_id"],
        "clan_vendeur_nom": clan_chef["nom"],
        "clan_acheteur_id": clan_acheteur["clan_id"],
        "clan_acheteur_nom": clan_acheteur["nom"],
        "prix":            prix,
        "force_possible":  True,
    }
    context.job_queue.run_once(
        _transfert_timeout, TRANSFERT_TIMEOUT_SEC,
        data={"msg_id": msg.message_id, "chat_id": chat_id},
        name=f"transfert_timeout_{msg.message_id}"
    )


async def cmd_proposer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Joueur se propose à un autre clan : /proposer ID_clan prix"""
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Commande à utiliser dans un groupe.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage : `/proposer ID_clan prix`\nEx : `/proposer 42 300`", parse_mode="Markdown")
        return
    try:
        clan_cible_id = int(context.args[0])
        prix          = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ ID clan et prix doivent être des nombres.")
        return
    if prix < 0:
        await update.message.reply_text("❌ Prix invalide.")
        return
    clan_actuel = get_clan_of_user(user.id, chat_id)
    clan_cible  = get_clan_by_id(clan_cible_id)
    if not clan_cible:
        await update.message.reply_text("❌ Clan cible introuvable.")
        return
    if clan_actuel and clan_actuel["clan_id"] == clan_cible_id:
        await update.message.reply_text("😅 Tu es déjà dans ce clan !")
        return
    m = get_membre_db(user.id, chat_id, user.first_name)
    rang_j = get_rang(m["xp"])
    keyboard = [[
        InlineKeyboardButton("✅ Accepter", callback_data="tj_chef_accept"),
        InlineKeyboardButton("❌ Refuser",  callback_data="tj_chef_refuse"),
    ]]
    msg = await update.message.reply_text(
        f"📩 *CANDIDATURE DE TRANSFERT !*\n\n"
        f"👤 Joueur : @{user.username or user.first_name} {rang_j['emoji']} {rang_j['rang']}\n"
        f"📥 Clan visé : *{clan_cible['nom']}*\n"
        f"💰 Prix demandé : *{prix} pts*\n\n"
        f"Chef de *{clan_cible['nom']}*, tu acceptes ?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    transferts_joueurs[msg.message_id] = {
        "type":           "joueur_propose",
        "msg_id":         msg.message_id,
        "chat_id":        chat_id,
        "joueur_id":      user.id,
        "joueur_nom":     user.username or user.first_name,
        "clan_actuel_id": clan_actuel["clan_id"] if clan_actuel else None,
        "clan_cible_id":  clan_cible_id,
        "clan_cible_nom": clan_cible["nom"],
        "clan_cible_chef_id": clan_cible.get("chef_id"),
        "prix":           prix,
    }
    context.job_queue.run_once(
        _transfert_timeout, TRANSFERT_TIMEOUT_SEC,
        data={"msg_id": msg.message_id, "chat_id": chat_id},
        name=f"transfert_timeout_{msg.message_id}"
    )


async def _executer_transfert(context, tr: dict):
    chat_id = tr["chat_id"]
    with get_db() as con:
        _execute(con, "UPDATE membres SET clan_id=? WHERE user_id=? AND chat_id=?",
                 (tr["clan_acheteur_id"], tr["joueur_id"], chat_id))
        _execute(con, "UPDATE clans SET points=points-? WHERE clan_id=?",
                 (tr["prix"], tr["clan_acheteur_id"]))
        _execute(con, "UPDATE clans SET points=points+? WHERE clan_id=?",
                 (tr["prix"], tr["clan_vendeur_id"]))
    try:
        await context.bot.send_message(
            chat_id,
            f"✅ *Transfert effectué !*\n\n"
            f"👤 @{tr['joueur_nom']} rejoint *{tr['clan_acheteur_nom']}*\n"
            f"💰 *{tr['prix']} pts* transférés de *{tr['clan_acheteur_nom']}* vers *{tr['clan_vendeur_nom']}*",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_executer_transfert] {e}")


async def callback_transfert_joueur(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    msg_id  = query.message.message_id
    await query.answer()
    if msg_id not in transferts_joueurs:
        await query.edit_message_text("⚠️ Ce transfert a expiré.", reply_markup=None)
        return
    tr   = transferts_joueurs[msg_id]
    data = query.data
    if tr["type"] == "chef_vend":
        if user.id != tr["joueur_id"]:
            await query.answer("❌ Ce n'est pas ton transfert !", show_alert=True)
            return
        for job in context.job_queue.get_jobs_by_name(f"transfert_timeout_{msg_id}"):
            job.schedule_removal()
        transferts_joueurs.pop(msg_id, None)
        if data == "tj_refuse":
            await query.edit_message_text(
                f"❌ @{tr['joueur_nom']} a refusé le transfert.", reply_markup=None, parse_mode="Markdown"
            )
        elif data == "tj_accept":
            await _executer_transfert(context, tr)
            try:
                await query.edit_message_text("✅ Transfert accepté !", reply_markup=None)
            except Exception:
                pass
    elif tr["type"] == "joueur_propose":
        if user.id != tr.get("clan_cible_chef_id"):
            await query.answer("❌ Seul le chef du clan peut accepter !", show_alert=True)
            return
        for job in context.job_queue.get_jobs_by_name(f"transfert_timeout_{msg_id}"):
            job.schedule_removal()
        transferts_joueurs.pop(msg_id, None)
        if data == "tj_chef_refuse":
            await query.edit_message_text(f"❌ Le chef a refusé la candidature.", reply_markup=None)
        elif data == "tj_chef_accept":
            # Adapter la structure pour _executer_transfert
            tr_adapted = {
                "chat_id":          chat_id,
                "joueur_id":        tr["joueur_id"],
                "joueur_nom":       tr["joueur_nom"],
                "clan_acheteur_id": tr["clan_cible_id"],
                "clan_acheteur_nom": tr["clan_cible_nom"],
                "clan_vendeur_id":  tr.get("clan_actuel_id") or tr["clan_cible_id"],
                "clan_vendeur_nom": "Libre",
                "prix":             tr["prix"],
            }
            # Si le joueur est dans un clan, mettre à jour
            if tr.get("clan_actuel_id"):
                with get_db() as con:
                    _execute(con, "UPDATE membres SET clan_id=? WHERE user_id=? AND chat_id=?",
                             (tr["clan_cible_id"], tr["joueur_id"], chat_id))
                    _execute(con, "UPDATE clans SET points=points-? WHERE clan_id=?",
                             (tr["prix"], tr["clan_cible_id"]))
                    _execute(con, "UPDATE clans SET points=points+? WHERE clan_id=?",
                             (tr["prix"], tr["clan_actuel_id"]))
            else:
                with get_db() as con:
                    _execute(con, "UPDATE membres SET clan_id=? WHERE user_id=? AND chat_id=?",
                             (tr["clan_cible_id"], tr["joueur_id"], chat_id))
            await context.bot.send_message(
                chat_id,
                f"✅ *@{tr['joueur_nom']}* rejoint *{tr['clan_cible_nom']}* !",
                parse_mode="Markdown"
            )
            try:
                await query.edit_message_text("✅ Candidature acceptée !", reply_markup=None)
            except Exception:
                pass


async def _transfert_timeout(context: ContextTypes.DEFAULT_TYPE):
    data   = context.job.data
    msg_id = data["msg_id"]
    if msg_id not in transferts_joueurs:
        return
    tr = transferts_joueurs.pop(msg_id, None)
    if not tr:
        return
    try:
        await context.bot.edit_message_text(
            chat_id=data["chat_id"], message_id=msg_id,
            text="⏰ Offre de transfert expirée.", reply_markup=None
        )
    except Exception as e:
        logger.warning(f"[_transfert_timeout] {e}")


async def cmd_clan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)
    if not clan:
        await update.message.reply_text(t("no_clan_group", lang), parse_mode="Markdown")
        return
    with get_db() as con:
        rang_mondial = (_fetchone(con, "SELECT COUNT(*) AS c FROM clans WHERE points>?", (clan["points"],)) or {"c": 0})["c"] + 1
        total        = (_fetchone(con, "SELECT COUNT(*) AS c FROM clans") or {"c": 0})["c"]
        nb_membres   = (_fetchone(con, "SELECT COUNT(*) AS c FROM membres WHERE chat_id=?", (chat_id,)) or {"c": 0})["c"]
    date     = datetime.fromisoformat(clan["created_at"]).strftime("%d/%m/%Y") if clan.get("created_at") else "?"
    war      = get_active_clanwar(chat_id)
    war_info = t("clan_war_ongoing", lang) if war else ""
    await update.message.reply_text(
        t("clan_info", lang, nom=clan["nom"], univers=UNIVERS_NOM,
          rang=rang_mondial, total=total, pts=clan["points"],
          membres=nb_membres, date=date, war_info=war_info),
        parse_mode="Markdown"
    )


async def cmd_clanid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)
    if not clan:
        await update.message.reply_text(t("no_clan_here", lang))
        return
    await update.message.reply_text(t("clan_id_msg", lang, cid=chat_id), parse_mode="Markdown")


async def cmd_clanwar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat = update.effective_chat
    lang = get_user_lang(user.id, chat.id)
    member = await context.bot.get_chat_member(chat.id, user.id)
    if member.status not in ("administrator", "creator"):
        await update.message.reply_text(t("admin_only", lang))
        return
    clan1 = get_clan(chat.id)
    if not clan1:
        await update.message.reply_text(t("no_clan_group", lang), parse_mode="Markdown")
        return
    if get_active_clanwar(chat.id):
        await update.message.reply_text(t("clanwar_already", lang), parse_mode="Markdown")
        return
    if not context.args:
        await update.message.reply_text(t("clanwar_usage", lang), parse_mode="Markdown")
        return
    try:
        defender_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t("clanwar_invalid_id", lang))
        return
    if defender_id == chat.id:
        await update.message.reply_text(t("clanwar_self", lang))
        return
    clan2 = get_clan(defender_id)
    if not clan2:
        await update.message.reply_text(t("clanwar_not_found", lang), parse_mode="Markdown")
        return
    if get_active_clanwar(defender_id):
        await update.message.reply_text(t("clanwar_enemy_busy", lang, nom=clan2["nom"]), parse_mode="Markdown")
        return
    fin = (datetime.now() + timedelta(hours=CLANWAR_DUREE_H)).isoformat()
    # FIX AMÉLIORATION: stocker les noms dans clan_wars pour l'historique
    with get_db() as con:
        _execute(con, "INSERT INTO clan_wars (challenger_id,defender_id,pts_challenger,pts_defender,statut,fin_at,nom_challenger,nom_defender) VALUES (?,?,0,0,'active',?,?,?)",
                 (chat.id, defender_id, fin, clan1["nom"], clan2["nom"]))
    context.job_queue.run_once(
        _clanwar_expire, CLANWAR_DUREE_H * 3600,
        data={"challenger_id": chat.id, "defender_id": defender_id},
        name=f"clanwar_{chat.id}_vs_{defender_id}"
    )
    await update.message.reply_text(
        t("clanwar_declared", lang, c1=clan1["nom"], c2=clan2["nom"], h=CLANWAR_DUREE_H, pts=POINTS_CLANWAR_ACTION),
        parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(defender_id, t("clanwar_attacked", "fr", nom=clan1["nom"], h=CLANWAR_DUREE_H), parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"[cmd_clanwar notify defender] {e}")


async def _clanwar_expire(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    war  = get_active_clanwar(data["challenger_id"])
    if not war:
        return
    res = terminer_clanwar(war)
    for cid in (data["challenger_id"], data["defender_id"]):
        try:
            if res["egalite"]:
                texte = t("warstat_end_draw", "fr", c1=res["nom_c"], p1=res["pts_c"], c2=res["nom_d"], p2=res["pts_d"])
            else:
                texte = t("warstat_end_win", "fr", winner=res["vainqueur_nom"], pv=res["pts_v"], loser=res["perdant_nom"], pl=res["pts_p"], bonus=POINTS_CLANWAR_VICTOIRE)
            await context.bot.send_message(cid, texte, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"[_clanwar_expire notify {cid}] {e}")


async def cmd_warstat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    war     = get_active_clanwar(chat_id)
    if not war:
        await update.message.reply_text(t("warstat_none", lang))
        return
    clan_c = get_clan(war["challenger_id"])
    clan_d = get_clan(war["defender_id"])
    nom_c  = clan_c["nom"] if clan_c else war.get("nom_challenger", "?")
    nom_d  = clan_d["nom"] if clan_d else war.get("nom_defender", "?")
    pts_c, pts_d = war["pts_challenger"], war["pts_defender"]
    fin   = datetime.fromisoformat(war["fin_at"])
    reste = fin - datetime.now()
    if reste.total_seconds() <= 0:
        res = terminer_clanwar(war)
        if res["egalite"]:
            await update.message.reply_text(t("warstat_end_draw", lang, c1=res["nom_c"], p1=res["pts_c"], c2=res["nom_d"], p2=res["pts_d"]), parse_mode="Markdown")
        else:
            await update.message.reply_text(t("warstat_end_win", lang, winner=res["vainqueur_nom"], pv=res["pts_v"], loser=res["perdant_nom"], pl=res["pts_p"], bonus=POINTS_CLANWAR_VICTOIRE), parse_mode="Markdown")
        return
    heures  = int(reste.total_seconds() // 3600)
    minutes = int((reste.total_seconds() % 3600) // 60)
    total   = max(pts_c + pts_d, 1)
    pct_c   = int((pts_c / total) * 10)
    barre   = "🔵" * pct_c + "🔴" * (10 - pct_c)
    leader  = t("warstat_leader", lang, nom=nom_c) if pts_c > pts_d else (t("warstat_leader", lang, nom=nom_d) if pts_d > pts_c else t("warstat_equal", lang))
    await update.message.reply_text(
        t("warstat_live", lang, c1=nom_c, p1=pts_c, c2=nom_d, p2=pts_d, barre=barre, leader=leader, h=heures, m=minutes),
        parse_mode="Markdown"
    )


async def cmd_warhistory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    lang    = get_user_lang(update.effective_user.id, chat_id)
    clan    = get_clan(chat_id)
    if not clan:
        await update.message.reply_text(t("no_clan_here", lang))
        return
    with get_db() as con:
        rows = _fetchall(con, """
            SELECT challenger_id, defender_id, pts_challenger, pts_defender, statut, fin_at,
                   nom_challenger, nom_defender
            FROM clan_wars WHERE (challenger_id=? OR defender_id=?) AND statut != 'active'
            ORDER BY fin_at DESC LIMIT 5
        """, (chat_id, chat_id))
    if not rows:
        await update.message.reply_text(t("warhistory_none", lang))
        return
    texte = t("warhistory_title", lang, nom=clan["nom"])
    for row in rows:
        # FIX AMÉLIORATION: utiliser les noms stockés si le clan n'existe plus
        nc = row.get("nom_challenger") or (get_clan(row["challenger_id"]) or {}).get("nom", "Inconnu")
        nd = row.get("nom_defender")   or (get_clan(row["defender_id"])   or {}).get("nom", "Inconnu")
        date = datetime.fromisoformat(row["fin_at"]).strftime("%d/%m/%Y") if row.get("fin_at") else "?"
        texte += f"⚔️ *{nc}* vs *{nd}*\n{row['pts_challenger']}—{row['pts_defender']} | _{row['statut']}_ | {date}\n\n"
    await update.message.reply_text(texte, parse_mode="Markdown")


# ─────────────────────────────────────────
# 👑 SUPER ADMIN
# ─────────────────────────────────────────

def _check_superadmin(update: Update) -> bool:
    return update.effective_user.id in ADMIN_IDS


async def cmd_deleteclan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user     = update.effective_user
    chat     = update.effective_chat
    is_super = _check_superadmin(update)
    lang     = get_user_lang(user.id, chat.id)
    if context.args and is_super:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(t("deleteclan_invalid", lang), parse_mode="Markdown")
            return
    else:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator") and not is_super:
            await update.message.reply_text(t("admin_or_superadmin", lang))
            return
        target_id = chat.id
    clan = get_clan(target_id)
    if not clan:
        await update.message.reply_text(t("deleteclan_notfound", lang), parse_mode="Markdown")
        return
    war = get_active_clanwar(target_id)
    if war:
        with get_db() as con:
            _execute(con, "UPDATE clan_wars SET statut='annulée' WHERE id=?", (war["id"],))
    with get_db() as con:
        _execute(con, "DELETE FROM clans   WHERE chat_id=?", (target_id,))
        _execute(con, "DELETE FROM membres WHERE chat_id=?", (target_id,))
    await update.message.reply_text(t("deleteclan_done", lang, nom=clan["nom"], univers=UNIVERS_NOM), parse_mode="Markdown")


async def cmd_listclans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    with get_db() as con:
        clans = _fetchall(con, "SELECT chat_id, nom, points FROM clans ORDER BY points DESC")
    if not clans:
        await update.message.reply_text(t("listclans_none", lang))
        return
    texte = t("listclans_title", lang)
    for row in clans:
        texte += f"⚔️ *{row['nom']}* — {row['points']} pts\n`ID : {row['chat_id']}`\n\n"
    texte += t("listclans_footer", lang)
    await update.message.reply_text(texte, parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    with get_db() as con:
        tc = (_fetchone(con, "SELECT COUNT(*) AS c FROM clans") or {"c": 0})["c"]
        tm = (_fetchone(con, "SELECT COUNT(*) AS c FROM membres") or {"c": 0})["c"]
        tx = (_fetchone(con, "SELECT COALESCE(SUM(xp),0) AS s FROM membres") or {"s": 0})["s"]
        tw = (_fetchone(con, "SELECT COUNT(*) AS c FROM clan_wars WHERE statut='active'") or {"c": 0})["c"]
    await update.message.reply_text(t("stats_msg", lang, univers=UNIVERS_NOM, tc=tc, tm=tm, tx=tx, tw=tw), parse_mode="Markdown")


async def cmd_givexp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    if len(context.args) < 2:
        await update.message.reply_text(t("givexp_usage", lang), parse_mode="Markdown")
        return
    pseudo = context.args[0].lstrip("@").lower()
    try:
        montant = int(context.args[1])
    except ValueError:
        await update.message.reply_text(t("givexp_invalid", lang))
        return
    with get_db() as con:
        row = _fetchone(con, "SELECT * FROM membres WHERE LOWER(username)=?", (pseudo,))
    if not row:
        await update.message.reply_text(t("givexp_notfound", lang, pseudo=pseudo), parse_mode="Markdown")
        return
    ancien = get_rang(row["xp"])
    nxp    = max(0, row["xp"] + montant)
    nrang  = get_rang(nxp)
    update_membre(row["user_id"], row["chat_id"], xp=nxp, rang=nrang["rang"])
    montee = t("rank_up", lang, label=nrang["label"]) if nrang["rang"] != ancien["rang"] else ""
    signe  = "+" if montant >= 0 else ""
    await update.message.reply_text(t("givexp_done", lang, signe=signe, montant=montant, user=row["username"], total=nxp, montee=montee), parse_mode="Markdown")


async def cmd_resetxp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    if not context.args:
        await update.message.reply_text(t("resetxp_usage", lang), parse_mode="Markdown")
        return
    pseudo = context.args[0].lstrip("@").lower()
    with get_db() as con:
        row = _fetchone(con, "SELECT * FROM membres WHERE LOWER(username)=?", (pseudo,))
    if not row:
        await update.message.reply_text(t("notfound", lang, pseudo=pseudo), parse_mode="Markdown")
        return
    # FIX SÉCURITÉ: confirmation avant reset
    keyboard = [[
        InlineKeyboardButton("✅ Confirmer", callback_data=f"resetxp_confirm_{row['user_id']}_{row['chat_id']}"),
        InlineKeyboardButton("❌ Annuler",   callback_data="resetxp_cancel"),
    ]]
    await update.message.reply_text(
        f"⚠️ *Confirmer le reset XP de @{row['username']} ?*\nCette action est irréversible.",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def callback_resetxp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not _check_superadmin(update):
        await query.answer("🚫 Réservé aux super-admins.", show_alert=True)
        return
    if query.data == "resetxp_cancel":
        await query.edit_message_text("❌ Reset annulé.", reply_markup=None)
        return
    parts = query.data.split("_")
    # resetxp_confirm_{user_id}_{chat_id}
    user_id = int(parts[2])
    chat_id = int(parts[3])
    with get_db() as con:
        row = _fetchone(con, "SELECT username FROM membres WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    if not row:
        await query.edit_message_text("❌ Joueur introuvable.", reply_markup=None)
        return
    update_membre(user_id, chat_id, xp=0, rang="E")
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    await query.edit_message_text(t("resetxp_done", lang, user=row["username"]), reply_markup=None, parse_mode="Markdown")


async def cmd_removemembre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(t("superadmin_only", lang))
        return
    lang = get_user_lang(update.effective_user.id, update.effective_chat.id)
    if not context.args:
        await update.message.reply_text(t("removemembre_usage", lang), parse_mode="Markdown")
        return
    pseudo = context.args[0].lstrip("@").lower()
    with get_db() as con:
        row = _fetchone(con, "SELECT * FROM membres WHERE LOWER(username)=?", (pseudo,))
        if not row:
            await update.message.reply_text(t("notfound", lang, pseudo=pseudo), parse_mode="Markdown")
            return
        _execute(con, "DELETE FROM membres WHERE user_id=? AND chat_id=?", (row["user_id"], row["chat_id"]))
    await update.message.reply_text(t("removemembre_done", lang, user=row["username"]), parse_mode="Markdown")


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not _check_superadmin(update):
        await update.message.reply_text("🚫 Réservé au super-admin.")
        return
    with get_db() as con:
        groupes       = _fetchall(con, "SELECT DISTINCT chat_id FROM membres WHERE chat_id < 0")
        private_users = _fetchall(con, "SELECT user_id FROM private_users")
    destinataires = [g["chat_id"] for g in groupes] + [u["user_id"] for u in private_users]
    destinataires = list(set(destinataires))
    if not destinataires:
        await update.message.reply_text("❌ Aucun destinataire trouvé.")
        return
    success = failed = 0
    dead_groups = []
    dead_users  = []
    errors_sample = []  # garde un extrait des erreurs pour le rapport
    msg     = update.message
    caption = " ".join(context.args) if context.args else (msg.caption or "")
    reply   = msg.reply_to_message

    def _safe_caption(text):
        """Retourne (texte, parse_mode) — fallback sans parse_mode si Markdown invalide."""
        return text  # utilisé avec _pm()

    async def _send(dest_id, pm="Markdown"):
        """Envoie le message. pm='Markdown' ou None (texte brut)."""
        cap = caption or ""
        if reply:
            if reply.photo:
                await context.bot.send_photo(dest_id, photo=reply.photo[-1].file_id, caption=cap or reply.caption, parse_mode=pm)
            elif reply.video:
                await context.bot.send_video(dest_id, video=reply.video.file_id, caption=cap or reply.caption, parse_mode=pm)
            elif reply.animation:
                await context.bot.send_animation(dest_id, animation=reply.animation.file_id, caption=cap or reply.caption, parse_mode=pm)
            elif reply.document:
                await context.bot.send_document(dest_id, document=reply.document.file_id, caption=cap or reply.caption, parse_mode=pm)
            elif reply.sticker:
                await context.bot.send_sticker(dest_id, sticker=reply.sticker.file_id)
            elif reply.text:
                await context.bot.send_message(dest_id, reply.text, parse_mode=pm)
            else:
                await context.bot.send_message(dest_id, cap, parse_mode=pm)
        elif msg.photo:
            await context.bot.send_photo(dest_id, photo=msg.photo[-1].file_id, caption=cap, parse_mode=pm)
        elif msg.video:
            await context.bot.send_video(dest_id, video=msg.video.file_id, caption=cap, parse_mode=pm)
        elif msg.animation:
            await context.bot.send_animation(dest_id, animation=msg.animation.file_id, caption=cap, parse_mode=pm)
        else:
            if not cap:
                raise ValueError("empty_caption")
            prefix = "📢 *Annonce :*\n\n" if pm else "📢 Annonce :\n\n"
            await context.bot.send_message(dest_id, f"{prefix}{cap}", parse_mode=pm)

    markdown_broken = False  # si True, on envoie tout en texte brut dès le départ

    for dest_id in destinataires:
        try:
            await _send(dest_id, pm=None if markdown_broken else "Markdown")
            success += 1
        except ValueError:
            await update.message.reply_text("❌ Message vide. Ajoute du texte ou réponds à un média.")
            return
        except BadRequest as e:
            if "can't parse entities" in str(e).lower() or "can't find end of the entity" in str(e).lower():
                # Markdown invalide dans le texte — on bascule définitivement en texte brut
                if not markdown_broken:
                    logger.warning(f"[broadcast] Markdown invalide détecté, bascule en texte brut. Erreur: {e}")
                    markdown_broken = True
                try:
                    await _send(dest_id, pm=None)
                    success += 1
                except Exception as e2:
                    logger.warning(f"[broadcast fallback dest={dest_id}] {e2}")
                    failed += 1
            else:
                logger.warning(f"[broadcast BadRequest dest={dest_id}] {e}")
                if len(errors_sample) < 3:
                    errors_sample.append(f"`{dest_id}`: {e}")
                failed += 1
            continue
        except ChatMigrated as e:
            # Le groupe a migré vers un supergroupe — mettre à jour l'ID
            new_id = e.new_chat_id
            logger.info(f"[broadcast] Migration {dest_id} → {new_id}")
            try:
                await _send(new_id, pm=None if markdown_broken else "Markdown")
                with get_db() as con:
                    con.execute("UPDATE membres SET chat_id=? WHERE chat_id=?", (new_id, dest_id))
                success += 1
            except Exception as e2:
                logger.warning(f"[broadcast migrated dest={new_id}] {e2}")
                failed += 1
        except Forbidden as e:
            # Bot bloqué ou kické — supprimer le destinataire
            err_str = str(e).lower()
            logger.warning(f"[broadcast Forbidden dest={dest_id}] {e}")
            if len(errors_sample) < 3:
                errors_sample.append(f"`{dest_id}`: {e}")
            if dest_id < 0:
                dead_groups.append(dest_id)
            else:
                dead_users.append(dest_id)
            failed += 1
        except TelegramError as e:
            logger.warning(f"[broadcast TelegramError dest={dest_id}] {e}")
            if len(errors_sample) < 3:
                errors_sample.append(f"`{dest_id}`: {e}")
            failed += 1
        except Exception as e:
            logger.warning(f"[broadcast dest={dest_id}] {e}")
            if len(errors_sample) < 3:
                errors_sample.append(f"`{dest_id}`: {e}")
            failed += 1
        # Petite pause pour éviter le FloodWait
        await asyncio.sleep(0.05)

    # Nettoyage des destinataires morts
    if dead_groups or dead_users:
        with get_db() as con:
            for gid in dead_groups:
                con.execute("DELETE FROM membres WHERE chat_id=?", (gid,))
            for uid in dead_users:
                con.execute("DELETE FROM private_users WHERE user_id=?", (uid,))
        logger.info(f"[broadcast] Nettoyage : {len(dead_groups)} groupes, {len(dead_users)} users supprimés")

    rapport = (
        f"✅ Broadcast envoyé !\n\n"
        f"🏘️ Groupes : *{len(groupes)}*\n"
        f"👤 Utilisateurs privés : *{len(private_users)}*\n"
        f"✅ Succès : *{success}*\n"
        f"❌ Échecs : *{failed}*"
    )
    if dead_groups or dead_users:
        rapport += f"\n🧹 Nettoyé : *{len(dead_groups)}* groupes morts, *{len(dead_users)}* users bloqués"
    if markdown_broken:
        rapport += f"\n⚠️ Markdown invalide détecté — message envoyé en texte brut (vérifie tes `*` `_` non fermés)"
    if errors_sample:
        rapport += f"\n\n⚠️ *Exemples d'erreurs :*\n" + "\n".join(errors_sample)
    await update.message.reply_text(rapport, parse_mode="Markdown")


# ─────────────────────────────────────────
# 📊 XP AUTO
# ─────────────────────────────────────────

async def xp_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    user = update.effective_user
    if user.is_bot:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return
    chat_id = chat.id
    try:
        m = get_membre_db(user.id, chat_id, user.username or user.first_name)
        if m.get("username") != (user.username or user.first_name):
            update_membre(user.id, chat_id, username=user.username or user.first_name)
        dernier = m.get("dernier_message")
        if dernier:
            diff = (datetime.now() - datetime.fromisoformat(dernier)).total_seconds()
            if diff < COOLDOWN_MSG_SECONDES:
                return
        lang        = m.get("langue") or "fr"
        ancien_rang = get_rang(m["xp"])
        nouveau_xp  = m["xp"] + XP_PAR_MESSAGE
        nouveau_rang = get_rang(nouveau_xp)
        update_membre(user.id, chat_id,
                      xp=nouveau_xp,
                      xp_semaine=(m.get("xp_semaine") or 0) + XP_PAR_MESSAGE,
                      messages=m["messages"] + 1,
                      rang=nouveau_rang["rang"],
                      dernier_message=datetime.now().isoformat())
        if get_clan(chat_id):
            update_clan_points(chat_id, POINTS_MESSAGE)
        # v8: coffre mystère
        _compteur_messages[chat_id] = _compteur_messages.get(chat_id, 0) + 1
        if _compteur_messages[chat_id] >= COFFRE_INTERVAL_MSG:
            _compteur_messages[chat_id] = 0
            asyncio.create_task(_ouvrir_coffre(update, context))
        # v8: quetes + succes
        asyncio.create_task(_progresser_quetes(user.id, chat_id, context, "messages"))
        asyncio.create_task(_verifier_succes(context, user.id, chat_id, m, nouveau_xp))
        if nouveau_rang["rang"] != ancien_rang["rang"]:
            if get_clan(chat_id):
                update_clan_points(chat_id, POINTS_MONTEE_RANG)
                add_clanwar_points(chat_id, POINTS_CLANWAR_ACTION)
            await update.message.reply_text(
                t("rank_up_msg", lang, user=user.username or user.first_name,
                  emoji=nouveau_rang["emoji"], label=nouveau_rang["label"], pts=POINTS_MONTEE_RANG),
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"[xp_auto] {e}", exc_info=True)


# ─────────────────────────────────────────
# 🛠️ UTILITAIRES ADMIN
# ─────────────────────────────────────────

async def cmd_resetwar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    is_super = _check_superadmin(update)
    if not is_super:
        try:
            member = await context.bot.get_chat_member(chat_id, user.id)
            if member.status not in ("administrator", "creator"):
                await update.message.reply_text("🚫 Réservé aux admins.")
                return
        except Exception as e:
            logger.warning(f"[cmd_resetwar] {e}")
            await update.message.reply_text("🚫 Réservé aux admins.")
            return
    if chat_id in combats_en_cours:
        combats_en_cours.pop(chat_id)
        for job in context.job_queue.get_jobs_by_name(f"combat_timeout_{chat_id}"):
            job.schedule_removal()
        for job in context.job_queue.get_jobs_by_name(f"duel_q_{chat_id}"):
            job.schedule_removal()
        await update.message.reply_text("✅ War réinitialisé. `/war` disponible.", parse_mode="Markdown")
    else:
        await update.message.reply_text("ℹ️ Aucun war en cours.")


async def cmd_checkbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("ℹ️ Utilise dans un *groupe*.", parse_mode="Markdown")
        return
    lines = ["🔍 *Diagnostic :*\n"]
    try:
        bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
        if bot_member.status in ("administrator", "creator"):
            lines.append("✅ Bot ADMIN — XP fonctionne.")
        else:
            lines.append("❌ Bot PAS admin → Privacy Mode actif.\n👉 Rends le bot Administrateur\n👉 OU @BotFather → Bot Settings → Group Privacy → Turn off")
    except Exception as e:
        lines.append(f"⚠️ Impossible de vérifier : {e}")
    clan = get_clan(chat.id)
    lines.append(f"\n✅ Clan *{clan['nom']}*" if clan else "\n⚠️ Pas de clan — `/createclan NomDuClan`")
    if chat.id in combats_en_cours:
        lines.append(f"\n⚠️ War en cours — `/resetwar` pour annuler.")
    else:
        lines.append("\n✅ Aucun war bloqué.")
    db_type = "PostgreSQL (Supabase)" if USE_POSTGRES else "SQLite (local)"
    lines.append(f"\n🗄️ Base de données : *{db_type}*")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─────────────────────────────────────────
# ⏰ JOBS AUTOMATIQUES
# ─────────────────────────────────────────

async def job_reset_weekly(context: ContextTypes.DEFAULT_TYPE):
    """FIX BUG: reset le lundi à minuit via run_daily, pas toutes les heures."""
    with get_db() as con:
        _execute(con, "UPDATE membres SET xp_semaine=0")
    logger.info("✅ XP semaine réinitialisé (lundi).")


async def job_mystere_auto(context: ContextTypes.DEFAULT_TYPE):
    """Envoie un quiz mystère dans les groupes actifs — 1 groupe par déclenchement, espacé."""
    with get_db() as con:
        seuil = (datetime.now() - timedelta(days=7)).isoformat()
        groupes_actifs = _fetchall(con,
            "SELECT DISTINCT chat_id FROM membres WHERE chat_id < 0 AND dernier_message > ?", (seuil,))

    if not groupes_actifs:
        return

    maintenant = datetime.now()
    delai = 0  # espacer les envois de 30s entre chaque groupe

    for row in groupes_actifs:
        chat_id = row["chat_id"]
        if chat_id in quiz_en_cours:
            continue

        # Vérifier quand le dernier mystère a été envoyé dans ce groupe
        dernier = _derniers_mysteres.get(chat_id)
        if dernier:
            diff = (maintenant - dernier).total_seconds()
            if diff < MYSTERE_INTERVAL_SECONDES:
                continue  # pas encore l'heure pour ce groupe

        async def _envoyer_mystere(ctx, cid=chat_id):
            if cid in quiz_en_cours:
                return
            q = await _get_question(cid, "mystere")
            keyboard = [
                [InlineKeyboardButton(f"{['🅰️','🅱️','🅲️','🅳️'][i]} {r}", callback_data=f"quiz_{i}")]
                for i, r in enumerate(q["r"])
            ]
            try:
                msg = await ctx.bot.send_message(
                    cid,
                    f"✨ *QUIZ MYSTÈRE* (+{XP_MYSTERE} XP)\n_Surprise du bot !_\n\n{q['q']}\n\n⏱️ {QUIZ_TIMEOUT_SECONDES}s !",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="Markdown"
                )
                quiz_en_cours[cid] = {"q": q, "type": "mystere", "message_id": msg.message_id, "xp": XP_MYSTERE, "pts": POINTS_QUIZ_NORMAL}
                _derniers_mysteres[cid] = datetime.now()
                ctx.job_queue.run_once(
                    _quiz_timeout, QUIZ_TIMEOUT_SECONDES,
                    data={"chat_id": cid, "message_id": msg.message_id},
                    name=f"quiz_timeout_{cid}"
                )
            except Exception as e:
                logger.warning(f"[job_mystere_auto chat_id={cid}] {e}")

        # Espacer les envois de 30s entre groupes pour ne pas tout envoyer d'un coup
        context.job_queue.run_once(_envoyer_mystere, when=delai, name=f"mystere_delayed_{chat_id}")
        delai += 30


# ─────────────────────────────────────────
# 📋 AIDE
# ─────────────────────────────────────────

async def cmd_aide(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    lang     = get_user_lang(update.effective_user.id, update.effective_chat.id)
    is_super = _check_superadmin(update)
    texte    = t("aide_title", lang, univers=UNIVERS_NOM)
    texte   += t("aide_membres", lang, xp_n=XP_PAR_QUIZ_NORMAL, xp_h=XP_PAR_QUIZ_HARDCORE)
    texte   += t("aide_clan", lang)
    texte   += t("aide_admin", lang)
    if is_super:
        texte += t("aide_superadmin", lang)
    texte += "\n*Nouveautes v8:*\n"
    texte += "• /roulette — Parie tes XP (rang C min, 24h)\n"
    texte += "• /quetes — Quetes du jour et de la semaine\n"
    texte += "• /succes — Tes badges debloques\n"
    texte += "• /carte [@user] — Ta carte de joueur\n"
    texte += "• /royale — Battle Royale quiz\n"
    texte += "• /tournoi — Creer/voir un tournoi\n\n"
    texte += t("aide_rangs", lang)
    texte += "\n".join(f"  {r['emoji']} {r['rang']} — {r['xp_requis']:,} XP" for r in RANGS)
    await update.message.reply_text(texte, parse_mode="Markdown")


# ─────────────────────────────────────────
# 🎰 ROULETTE XP
# ─────────────────────────────────────────

async def cmd_roulette(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id

    if update.effective_chat.type == "private":
        await update.message.reply_text("La roulette se joue dans un groupe !")
        return

    # XP global agrégé tous groupes
    with get_db() as con:
        xp_global = (_fetchone(con, "SELECT COALESCE(SUM(xp),0) AS s FROM membres WHERE user_id=?", (user.id,)) or {"s": 0})["s"] or 0

    rang_global = get_rang(xp_global)["rang"]
    RANGS_ORDRE = ["E", "D", "C", "B", "A", "S", "SS", "SSS", "NATION"]

    if RANGS_ORDRE.index(rang_global) < RANGS_ORDRE.index(ROULETTE_RANG_MIN):
        await update.message.reply_text(
            f"Il faut etre au minimum rang {ROULETTE_RANG_MIN} pour jouer !\n"
            f"Ton rang actuel : {rang_global} ({xp_global:,} XP)"
        )
        return

    # Cooldown 24h (stocké sur la ligne du groupe courant)
    m = get_membre_db(user.id, chat_id, user.first_name)
    dernier = m.get("dernier_roulette")
    if dernier:
        diff = (datetime.now() - datetime.fromisoformat(dernier)).total_seconds()
        if diff < ROULETTE_COOLDOWN_SEC:
            reste_h   = int((ROULETTE_COOLDOWN_SEC - diff) / 3600)
            reste_min = int(((ROULETTE_COOLDOWN_SEC - diff) % 3600) / 60)
            await update.message.reply_text(
                f"Deja joue aujourd'hui ! Prochain tour dans {reste_h}h {reste_min}min."
            )
            return

    if not context.args:
        await update.message.reply_text(
            f"ROULETTE XP\n\n"
            f"Parie tes XP — 50% de chance de doubler !\n\n"
            f"Usage : /roulette [montant|10%|25%|50%]\n"
            f"Ex : /roulette 500 ou /roulette 25%\n\n"
            f"Mise minimum : {ROULETTE_MISE_MIN} XP\n"
            f"Rang minimum : {ROULETTE_RANG_MIN}\n"
            f"Cooldown : 24h\n\n"
            f"Ton solde : {xp_global:,} XP (rang {rang_global})"
        )
        return

    arg = context.args[0].strip()
    if arg.endswith("%"):
        try:
            pct = int(arg[:-1])
            if pct not in (10, 25, 50):
                await update.message.reply_text("Pourcentages acceptes : 10%, 25%, 50%")
                return
            mise = int(xp_global * pct / 100)
        except ValueError:
            await update.message.reply_text("Montant invalide.")
            return
    else:
        try:
            mise = int(arg)
        except ValueError:
            await update.message.reply_text("Montant invalide.")
            return

    if mise < ROULETTE_MISE_MIN:
        await update.message.reply_text(f"Mise minimum : {ROULETTE_MISE_MIN} XP.")
        return
    if mise > xp_global:
        await update.message.reply_text(f"Tu n'as que {xp_global:,} XP !")
        return

    keyboard = [[
        InlineKeyboardButton(f"Parier {mise:,} XP", callback_data=f"roulette_go_{user.id}_{mise}"),
        InlineKeyboardButton("Annuler", callback_data=f"roulette_cancel_{user.id}"),
    ]]
    await update.message.reply_text(
        f"ROULETTE XP\n\n"
        f"Mise : {mise:,} XP\n"
        f"Victoire -> +{mise:,} XP\n"
        f"Defaite  -> -{mise:,} XP\n\n"
        f"Solde actuel : {xp_global:,} XP\n"
        f"Confirmes-tu le pari ?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def callback_roulette(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    await query.answer()

    parts    = query.data.split("_")
    action   = parts[1]
    owner_id = int(parts[2])

    if user.id != owner_id:
        await query.answer("Ce n'est pas ton pari !", show_alert=True)
        return

    if action == "cancel":
        await query.edit_message_text("Pari annule.", reply_markup=None)
        return

    mise = int(parts[3])

    # XP global actuel
    with get_db() as con:
        xp_global = (_fetchone(con, "SELECT COALESCE(SUM(xp),0) AS s FROM membres WHERE user_id=?", (user.id,)) or {"s": 0})["s"] or 0
        rows_all  = _fetchall(con, "SELECT user_id, chat_id, xp FROM membres WHERE user_id=?", (user.id,))

    if xp_global < mise:
        await query.edit_message_text(f"Tu n'as plus assez d'XP ({xp_global:,} XP) !", reply_markup=None)
        return

    # Animation
    for s in ["🔴", "⚫", "🟢"]:
        try:
            await query.edit_message_text(f"La roue tourne... {s}", reply_markup=None)
            await asyncio.sleep(0.7)
        except Exception:
            pass

    victoire     = random.random() < 0.5
    ancien_rang  = get_rang(xp_global)
    nouveau_xp_global = xp_global + mise if victoire else max(0, xp_global - mise)
    nouveau_rang = get_rang(nouveau_xp_global)

    # Distribuer le gain/perte proportionnellement sur toutes les lignes
    if rows_all and xp_global > 0:
        with get_db() as con:
            for row in rows_all:
                xp_local = row.get("xp") or 0
                if victoire:
                    # Ajouter la mise sur la ligne avec le plus d'XP
                    if row["chat_id"] == chat_id:
                        new_local = xp_local + mise
                    else:
                        new_local = xp_local
                else:
                    # Retirer proportionnellement
                    ratio     = xp_local / xp_global if xp_global > 0 else 0
                    new_local = max(0, xp_local - int(mise * ratio))
                _execute(con,
                    "UPDATE membres SET xp=?, rang=? WHERE user_id=? AND chat_id=?",
                    (new_local, get_rang(new_local)["rang"], user.id, row["chat_id"]))
            # Mettre à jour dernier_roulette sur la ligne courante
            _execute(con,
                "UPDATE membres SET dernier_roulette=? WHERE user_id=? AND chat_id=?",
                (datetime.now().isoformat(), user.id, chat_id))
    else:
        # Pas de lignes existantes - créer/mettre à jour la ligne courante
        m = get_membre_db(user.id, chat_id, user.first_name)
        xp_local = m["xp"] or 0
        new_local = xp_local + mise if victoire else max(0, xp_local - mise)
        update_membre(user.id, chat_id,
                      xp=new_local,
                      rang=get_rang(new_local)["rang"],
                      dernier_roulette=datetime.now().isoformat())

    montee = f"\nMONTEE DE RANG -> {nouveau_rang['label']} !" if nouveau_rang["rang"] != ancien_rang["rang"] else ""

    if victoire:
        msg = (
            f"ROULETTE — VICTOIRE !\n\n"
            f"@{user.username or user.first_name} gagne +{mise:,} XP !\n"
            f"Nouveau solde : {nouveau_xp_global:,} XP{montee}"
        )
    else:
        msg = (
            f"ROULETTE — DEFAITE !\n\n"
            f"@{user.username or user.first_name} perd -{mise:,} XP...\n"
            f"Nouveau solde : {nouveau_xp_global:,} XP"
        )

    try:
        await query.edit_message_text(msg, reply_markup=None)
    except Exception as e:
        logger.warning(f"[callback_roulette] {e}")
        await context.bot.send_message(chat_id, msg)


# ─────────────────────────────────────────
# 📋 HELPERS QUÊTES (fonctions manquantes)
# ─────────────────────────────────────────

def _get_quete_progress(user_id: int, chat_id: int, quete_id: str, jour: str) -> dict:
    """Retourne la progression d'une quête pour un joueur à une date donnée."""
    try:
        with get_db() as con:
            row = _fetchone(con,
                "SELECT progress, done FROM quetes_progress "
                "WHERE user_id=? AND chat_id=? AND quete_id=? AND jour=?",
                (user_id, chat_id, quete_id, jour)
            )
        if row:
            return {"progress": row["progress"] or 0, "done": bool(row["done"])}
    except Exception as e:
        logger.warning(f"[_get_quete_progress] {e}")
    return {"progress": 0, "done": False}


def _update_quete_progress(user_id: int, chat_id: int, quete_id: str, jour: str, progress: int, done: int):
    """Met à jour ou crée la progression d'une quête."""
    try:
        with get_db() as con:
            if USE_POSTGRES:
                _execute(con, """
                    INSERT INTO quetes_progress (user_id, chat_id, quete_id, jour, progress, done)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, chat_id, quete_id, jour)
                    DO UPDATE SET progress=EXCLUDED.progress, done=EXCLUDED.done
                """, (user_id, chat_id, quete_id, jour, progress, done))
            else:
                _execute(con, """
                    INSERT OR REPLACE INTO quetes_progress (user_id, chat_id, quete_id, jour, progress, done)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (user_id, chat_id, quete_id, jour, progress, done))
    except Exception as e:
        logger.warning(f"[_update_quete_progress] {e}")


async def cmd_quetes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    m       = get_membre_db(user.id, chat_id, user.first_name)

    aujourd_hui  = date.today().isoformat()
    debut_semaine = (date.today() - timedelta(days=date.today().weekday())).isoformat()

    texte = f"📋 *MES QUÊTES — @{user.username or user.first_name}*\n\n"

    texte += "☀️ *JOURNALIÈRES :*\n"
    for q in QUETES_JOURNALIERES:
        p = _get_quete_progress(user.id, chat_id, q["id"], aujourd_hui)
        done     = p["done"]
        progress = p["progress"]
        cible    = q["cible"]
        barre    = "█" * min(int(progress / cible * 5), 5) + "░" * max(0, 5 - int(progress / cible * 5))
        if done:
            texte += f"✅ ~~{q['emoji'] if 'emoji' in q else '•'} {q['label']}~~ *+{q['xp']} XP*\n"
        else:
            texte += f"⬜ *{q['label']}* — {q['desc']}\n   `{barre}` {progress}/{cible} → *+{q['xp']} XP*\n"

    texte += "\n📅 *HEBDOMADAIRES :*\n"
    for q in QUETES_HEBDOMADAIRES:
        p = _get_quete_progress(user.id, chat_id, q["id"], debut_semaine)
        done     = p["done"]
        progress = p["progress"]
        cible    = q["cible"]
        barre    = "█" * min(int(progress / cible * 5), 5) + "░" * max(0, 5 - int(progress / cible * 5))
        if done:
            texte += f"✅ ~~{q['label']}~~ *+{q['xp']} XP*\n"
        else:
            texte += f"⬜ *{q['label']}* — {q['desc']}\n   `{barre}` {progress}/{cible} → *+{q['xp']} XP*\n"

    await update.message.reply_text(texte, parse_mode="Markdown")


async def _progresser_quetes(user_id: int, chat_id: int, context, type_action: str, valeur_actuelle: int = 0):
    """Appelé après chaque action pour mettre à jour les quêtes en cours."""
    aujourd_hui  = date.today().isoformat()
    debut_semaine = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    m = get_membre_db(user_id, chat_id)

    for q in TOUTES_QUETES:
        if q["type"] != type_action:
            continue
        jour = aujourd_hui if q["freq"] == "jour" else debut_semaine
        p = _get_quete_progress(user_id, chat_id, q["id"], jour)
        if p["done"]:
            continue

        # Calculer la nouvelle progression
        if type_action in ("messages", "quiz_gagnes", "combats_gagnes"):
            # Incrémenter de 1
            new_progress = p["progress"] + 1
        elif type_action == "streak":
            # Valeur absolue du streak
            new_progress = valeur_actuelle
        elif type_action == "daily":
            new_progress = 1
        else:
            new_progress = p["progress"] + 1

        done = 1 if new_progress >= q["cible"] else 0
        _update_quete_progress(user_id, chat_id, q["id"], jour, new_progress, done)

        # Récompense si quête complétée (notification désactivée)
        if done and not p["done"]:
            m_fresh = get_membre_db(user_id, chat_id)
            nouveau_xp = m_fresh["xp"] + q["xp"]
            update_membre(user_id, chat_id, xp=nouveau_xp, rang=get_rang(nouveau_xp)["rang"])


# ─────────────────────────────────────────
# 🏆 BADGES / SUCCÈS
# ─────────────────────────────────────────

async def cmd_succes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    try:
        with get_db() as con:
            m = _fetchone(con,
                "SELECT messages, quiz_gagnes, combats_gagnes, streak, rang, succes "
                "FROM membres WHERE user_id=? AND chat_id=?",
                (user.id, chat_id))

        if not m:
            await update.message.reply_text("Tu n'es pas encore enregistre. Envoie un message dans le groupe !")
            return

        m = dict(m)
        for k, d in [("messages",0),("quiz_gagnes",0),("combats_gagnes",0),
                     ("streak",0),("rang","E"),("succes","")]:
            m.setdefault(k, d)
            if m[k] is None: m[k] = d

        # Calculer les succes acquis
        possedes = [s for s in (m["succes"] or "").split(",") if s]
        conditions = {
            "s_first":   m["messages"] >= 1,
            "s_msg100":  m["messages"] >= 100,
            "s_msg1000": m["messages"] >= 1000,
            "s_quiz10":  m["quiz_gagnes"] >= 10,
            "s_quiz100": m["quiz_gagnes"] >= 100,
            "s_duel10":  m["combats_gagnes"] >= 10,
            "s_duel50":  m["combats_gagnes"] >= 50,
            "s_streak7": m["streak"] >= 7,
            "s_streak30":m["streak"] >= 30,
            "s_rang_s":  m["rang"] in ("S","SS","SSS","NATION"),
            "s_rang_sss":m["rang"] in ("SSS","NATION"),
            "s_rang_nat":m["rang"] == "NATION",
        }
        nouveaux = []
        for s in SUCCES_LIST:
            if s["id"] not in possedes and conditions.get(s["id"], False):
                possedes.append(s["id"])
                nouveaux.append(s["label"])

        # Sauvegarder si nouveaux succes
        if nouveaux:
            with get_db() as con:
                _execute(con,
                    "UPDATE membres SET succes=? WHERE user_id=? AND chat_id=?",
                    (",".join(possedes), user.id, chat_id))

        # Construire le message
        lignes = [f"SUCCES de {user.username or user.first_name}"]
        if nouveaux:
            lignes.append(f"Nouveaux debloques : {', '.join(nouveaux)} !")
        lignes.append("")
        for s in SUCCES_LIST:
            etat = "OK" if s["id"] in possedes else "  "
            lignes.append(f"[{etat}] {s['emoji']} {s['label']} - {s['desc']}")
        lignes.append(f"\n{len(possedes)}/{len(SUCCES_LIST)} succes debloques")
        lignes.append(f"Stats : {m['messages']} msg | {m['quiz_gagnes']} quiz | {m['combats_gagnes']} duels | streak {m['streak']}j")

        await update.message.reply_text("\n".join(lignes))

    except Exception as e:
        logger.error(f"[cmd_succes] {e}", exc_info=True)
        await update.message.reply_text("Erreur chargement succes.")


async def _verifier_succes(context, user_id: int, chat_id: int, m_avant: dict, xp_nouveau: int = None):
    """Vérifie et attribue les nouveaux succès débloqués."""
    m = get_membre_db(user_id, chat_id)
    possedes = (m.get("succes") or "").split(",")
    possedes = [s for s in possedes if s]
    nouveaux = []

    conditions = {
        "s_first":   m["messages"] >= 1,
        "s_msg100":  m["messages"] >= 100,
        "s_msg1000": m["messages"] >= 1000,
        "s_quiz10":  m["quiz_gagnes"] >= 10,
        "s_quiz100": m["quiz_gagnes"] >= 100,
        "s_duel10":  m["combats_gagnes"] >= 10,
        "s_duel50":  m["combats_gagnes"] >= 50,
        "s_streak7": (m.get("streak") or 0) >= 7,
        "s_streak30":(m.get("streak") or 0) >= 30,
        "s_rang_s":  m["rang"] in ("S", "SS", "SSS", "NATION"),
        "s_rang_sss":m["rang"] in ("SSS", "NATION"),
        "s_rang_nat":m["rang"] == "NATION",
    }

    for succes in SUCCES_LIST:
        sid = succes["id"]
        if sid in possedes:
            continue
        if conditions.get(sid, False):
            nouveaux.append(succes)
            possedes.append(sid)

    if nouveaux:
        update_membre(user_id, chat_id, succes=",".join(possedes))
        # Notifications succès désactivées


# ─────────────────────────────────────────
# 🎁 COFFRE MYSTÈRE
# ─────────────────────────────────────────

# Compteur global de messages par groupe pour déclencher le coffre
_compteur_messages = {}   # chat_id → int

COFFRE_RECOMPENSES = [
    {"label": "petit trésor",   "xp": 50,   "poids": 40},
    {"label": "trésor",         "xp": 150,  "poids": 30},
    {"label": "gros trésor",    "xp": 300,  "poids": 20},
    {"label": "jackpot",        "xp": 500,  "poids": 8},
    {"label": "MEGA JACKPOT",   "xp": 1000, "poids": 2},
]


def _choisir_recompense():
    pool  = [r for r in COFFRE_RECOMPENSES for _ in range(r["poids"])]
    return random.choice(pool)


async def _ouvrir_coffre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Déclenché automatiquement tous les COFFRE_INTERVAL_MSG messages."""
    chat_id = update.effective_chat.id
    recompense = _choisir_recompense()

    keyboard = [[InlineKeyboardButton(
        f"🎁 Ouvrir le coffre !",
        callback_data=f"coffre_open_{chat_id}"
    )]]
    msg = await context.bot.send_message(
        chat_id,
        f"🎁 *UN COFFRE MYSTÈRE APPARAÎT !*\n\n"
        f"Le premier qui clique remporte la récompense !\n\n"
        f"⏱️ {COFFRE_TIMEOUT_SEC} secondes...",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    # Stocker en mémoire pour le callback
    context.bot_data[f"coffre_{chat_id}"] = {
        "msg_id":    msg.message_id,
        "recompense": recompense,
        "ouvert":    False,
    }
    context.job_queue.run_once(
        _coffre_expire, COFFRE_TIMEOUT_SEC,
        data={"chat_id": chat_id, "msg_id": msg.message_id},
        name=f"coffre_expire_{chat_id}"
    )


async def _coffre_expire(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    key     = f"coffre_{chat_id}"
    coffre  = context.bot_data.get(key)
    if not coffre or coffre.get("ouvert"):
        return
    context.bot_data.pop(key, None)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["msg_id"],
            text="⏰ *Le coffre a disparu...* Personne ne l'a ouvert !",
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_coffre_expire] {e}")


async def callback_coffre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    await query.answer()

    key    = f"coffre_{chat_id}"
    coffre = context.bot_data.get(key)

    if not coffre or coffre.get("ouvert"):
        await query.edit_message_text("❌ Trop tard, le coffre est déjà ouvert !", reply_markup=None)
        return

    coffre["ouvert"] = True
    context.bot_data[key] = coffre

    # Annuler le timer d'expiration
    for job in context.job_queue.get_jobs_by_name(f"coffre_expire_{chat_id}"):
        job.schedule_removal()

    recompense = coffre["recompense"]
    m = get_membre_db(user.id, chat_id, user.first_name)
    nouveau_xp = m["xp"] + recompense["xp"]
    update_membre(user.id, chat_id,
                  xp=nouveau_xp,
                  rang=get_rang(nouveau_xp)["rang"],
                  coffres_ouverts=(m.get("coffres_ouverts") or 0) + 1)

    await _verifier_succes(context, user.id, chat_id, m, nouveau_xp)

    await query.edit_message_text(
        f"🎁 *COFFRE OUVERT PAR @{user.username or user.first_name} !*\n\n"
        f"Récompense : *{recompense['label']}*\n"
        f"💰 *+{recompense['xp']} XP* !",
        reply_markup=None, parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 👑 BATTLE ROYALE QUIZ
# ─────────────────────────────────────────

royale_en_cours = {}   # chat_id → royale data

async def cmd_royale(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id

    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Le Battle Royale se joue dans un groupe !")
        return

    if chat_id in royale_en_cours:
        await update.message.reply_text("⚠️ Un Battle Royale est déjà en cours !")
        return
    if chat_id in quiz_en_cours:
        await update.message.reply_text("⚠️ Un quiz est déjà en cours !")
        return

    royale_en_cours[chat_id] = {
        "phase":       "inscription",
        "lanceur_id":  user.id,
        "lanceur_nom": user.username or user.first_name,
        "inscrits":    {user.id: user.username or user.first_name},
        "elimines":    set(),
        "question_idx": 0,
        "scores":      {},
    }

    keyboard = [[InlineKeyboardButton("⚔️ Je participe !", callback_data=f"royale_join_{chat_id}")]]
    msg = await update.message.reply_text(
        f"👑 *BATTLE ROYALE QUIZ !*\n\n"
        f"@{user.username or user.first_name} lance un Battle Royale !\n\n"
        f"• Le dernier à répondre FAUX à chaque question est éliminé\n"
        f"• Le dernier survivant remporte *+200 XP* !\n\n"
        f"👥 Inscrits : *1* — Inscriptions pendant 60s\n\n"
        f"Clique pour rejoindre !",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    royale_en_cours[chat_id]["msg_id"] = msg.message_id

    context.job_queue.run_once(
        _royale_demarrer, 60,
        data={"chat_id": chat_id, "msg_id": msg.message_id},
        name=f"royale_start_{chat_id}"
    )


async def _royale_demarrer(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    if chat_id not in royale_en_cours:
        return
    royale = royale_en_cours[chat_id]
    inscrits = royale["inscrits"]

    if len(inscrits) < 2:
        royale_en_cours.pop(chat_id, None)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=data["msg_id"],
                text="❌ Battle Royale annulé — pas assez de participants (minimum 2).",
                reply_markup=None
            )
        except Exception:
            pass
        return

    royale["phase"] = "jeu"
    royale["scores"] = {uid: 0 for uid in inscrits}
    noms = ", ".join(f"@{n}" for n in inscrits.values())

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["msg_id"],
            text=f"BATTLE ROYALE COMMENCE !\n\n{len(inscrits)} participants\n\nBonne chance !",
            reply_markup=None
        )
    except Exception:
        pass

    await asyncio.sleep(2)
    await _royale_question(context, chat_id)


async def _royale_question(context, chat_id: int):
    if chat_id not in royale_en_cours:
        return
    royale = royale_en_cours[chat_id]
    survivants = {uid: nom for uid, nom in royale["inscrits"].items() if uid not in royale["elimines"]}

    if len(survivants) <= 1:
        await _royale_fin(context, chat_id)
        return

    q = await _get_question(chat_id, "normal")
    royale["question_actuelle"] = q
    royale["reponses_round"] = {}

    keyboard = [
        [InlineKeyboardButton(f"{['🅰️','🅱️','🅲️','🅳️'][i]} {r}", callback_data=f"royale_rep_{i}_{chat_id}")]
        for i, r in enumerate(q["r"])
    ]
    noms_surv = ", ".join(f"@{n}" for n in survivants.values())
    # Nettoyer la question pour eviter les erreurs Markdown
    question_texte = q["q"].replace("**", "").replace("*", "").replace("_", "").replace("`", "")
    msg = await context.bot.send_message(
        chat_id,
        f"ROYALE Q{royale['question_idx']+1}\n\n"
        f"{question_texte}\n\n"
        f"20 secondes !\n"
        f"Survivants : {noms_surv}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    royale["question_msg_id"] = msg.message_id
    royale["question_idx"] += 1

    context.job_queue.run_once(
        _royale_resultat, 20,
        data={"chat_id": chat_id, "msg_id": msg.message_id},
        name=f"royale_res_{chat_id}"
    )


async def _royale_resultat(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    if chat_id not in royale_en_cours:
        return
    royale   = royale_en_cours[chat_id]
    q        = royale.get("question_actuelle", {})
    reponses = royale.get("reponses_round", {})
    survivants = {uid: nom for uid, nom in royale["inscrits"].items() if uid not in royale["elimines"]}
    bonne_rep  = q["r"][q["b"]] if q else "?"

    # Éliminer ceux qui ont mal répondu ou pas répondu
    nouveaux_elimines = []
    for uid in list(survivants.keys()):
        rep = reponses.get(uid)
        if rep is None or rep != q.get("b"):
            royale["elimines"].add(uid)
            nouveaux_elimines.append(survivants[uid])

    # Si tout le monde a mal répondu, personne n'est éliminé ce round
    survivants_apres = {uid: nom for uid, nom in royale["inscrits"].items() if uid not in royale["elimines"]}
    if not survivants_apres:
        # Ressusciter tout le monde sauf ceux qui n'ont pas répondu du tout
        for uid in nouveaux_elimines:
            royale["elimines"].discard(uid)
        survivants_apres = {uid: nom for uid, nom in royale["inscrits"].items() if uid not in royale["elimines"]}

    elim_str = (", ".join(f"@{n}" for n in nouveaux_elimines)) or "personne"
    surv_str = ", ".join(f"@{n}" for n in survivants_apres.values())

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["msg_id"],
            text=f"📊 *Résultat du round*\n\n"
                 f"Bonne réponse : *{bonne_rep}*\n\n"
                 f"❌ Éliminé(s) : {elim_str}\n"
                 f"✅ Survivants : {surv_str}",
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_royale_resultat] {e}")

    await asyncio.sleep(3)
    await _royale_question(context, chat_id)


async def _royale_fin(context, chat_id: int):
    royale = royale_en_cours.pop(chat_id, None)
    if not royale:
        return
    survivants = {uid: nom for uid, nom in royale["inscrits"].items() if uid not in royale["elimines"]}

    if survivants:
        vainqueur_id  = list(survivants.keys())[0]
        vainqueur_nom = list(survivants.values())[0]
        m = get_membre_db(vainqueur_id, chat_id, vainqueur_nom)
        nouveau_xp = m["xp"] + 200
        update_membre(vainqueur_id, chat_id, xp=nouveau_xp, rang=get_rang(nouveau_xp)["rang"])
        await context.bot.send_message(
            chat_id,
            f"👑 *BATTLE ROYALE TERMINÉ !*\n\n"
            f"🥇 *@{vainqueur_nom}* remporte la couronne !\n"
            f"💰 *+200 XP* !",
            parse_mode="Markdown"
        )
    else:
        await context.bot.send_message(chat_id, "🤝 *Battle Royale terminé — Égalité générale !*", parse_mode="Markdown")


async def callback_royale(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    # NE PAS appeler query.answer() ici — on le fait après les checks
    parts = query.data.split("_")
    # royale_join_{chat_id} → parts[1]="join", parts[2]=chat_id
    # royale_rep_{idx}_{chat_id} → parts[1]="rep"
    action = parts[1]

    if action == "join":
        if chat_id not in royale_en_cours:
            await query.answer("Ce Battle Royale a expiré.", show_alert=True)
            return
        royale = royale_en_cours[chat_id]
        if royale["phase"] != "inscription":
            await query.answer("Les inscriptions sont fermées !", show_alert=True)
            return
        if user.id in royale["inscrits"]:
            await query.answer("Tu es déjà inscrit !", show_alert=True)
            return
        royale["inscrits"][user.id] = user.username or user.first_name
        nb = len(royale["inscrits"])
        await query.answer(f"Inscrit ! {nb} participants")
        # Mise a jour sans Markdown pour eviter les erreurs de parsing
        try:
            liste = ", ".join(royale["inscrits"].values())
            keyboard = [[InlineKeyboardButton("⚔️ Je participe !", callback_data=f"royale_join_{chat_id}")]]
            await query.edit_message_text(
                f"BATTLE ROYALE QUIZ\n\n"
                f"Le dernier survivant remporte +200 XP !\n\n"
                f"Inscrits ({nb}) : {liste}\n\n"
                f"Clique pour rejoindre !",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.warning(f"[callback_royale join edit] {e}")

    elif action == "rep":
        if chat_id not in royale_en_cours:
            await query.answer("Battle Royale terminé !", show_alert=True)
            return
        royale = royale_en_cours[chat_id]
        if royale["phase"] != "jeu":
            await query.answer("Pas encore commencé !", show_alert=True)
            return
        if user.id not in royale["inscrits"] or user.id in royale["elimines"]:
            await query.answer("Tu n'es pas dans ce Battle Royale !", show_alert=True)
            return
        if user.id in royale.get("reponses_round", {}):
            await query.answer("⚡ Déjà répondu !", show_alert=True)
            return
        rep = int(parts[2])
        royale["reponses_round"][user.id] = rep
        q = royale.get("question_actuelle", {})
        correct = rep == q.get("b")
        await query.answer("✅ Bonne réponse !" if correct else "❌ Mauvaise réponse...")


# ─────────────────────────────────────────
# 🏟️ TOURNOI
# ─────────────────────────────────────────

async def cmd_tournoi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    try:
        if update.effective_chat.type == "private":
            await update.message.reply_text("Les tournois se creent dans un groupe !")
            return

        with get_db() as con:
            tournoi = _fetchone(con,
                "SELECT * FROM tournois WHERE chat_id=? AND statut IN ('inscription','en_cours')",
                (chat_id,))

        if not tournoi:
            keyboard = [[InlineKeyboardButton("S'inscrire", callback_data=f"tournoi_join_{chat_id}")]]
            msg = await update.message.reply_text(
                f"TOURNOI OTAKU\n\n"
                f"{user.username or user.first_name} cree un tournoi !\n\n"
                f"Regles : duels en {TOURNOI_DUEL_QUESTIONS} questions, bracket eliminatoire\n"
                f"Champion = +500 XP\n\n"
                f"Inscrits : 1\n"
                f"Inscriptions pendant 10 min\n"
                f"Utilise /lancertournoi pour demarrer quand tu es pret !",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            with get_db() as con:
                _execute(con,
                    "INSERT INTO tournois (chat_id, createur_id, statut, participants, bracket, round_actuel, created_at) VALUES (?,?,?,?,?,0,?)",
                    (chat_id, user.id, "inscription",
                     _json_module.dumps([{"uid": user.id, "nom": user.username or user.first_name}]),
                     "[]", datetime.now().isoformat()))
            context.job_queue.run_once(
                _tournoi_fermer_inscriptions, TOURNOI_TIMEOUT_INSCR,
                data={"chat_id": chat_id, "msg_id": msg.message_id},
                name=f"tournoi_inscr_{chat_id}"
            )
        else:
            participants = _json_module.loads(tournoi["participants"])
            inscrits_str = ", ".join(p["nom"] for p in participants)
            statut_str   = "Inscriptions ouvertes" if tournoi["statut"] == "inscription" else "En cours"
            await update.message.reply_text(
                f"TOURNOI EN COURS\n\n"
                f"Statut : {statut_str}\n"
                f"Inscrits ({len(participants)}) : {inscrits_str}"
            )
    except Exception as e:
        logger.error(f"[cmd_tournoi] {e}", exc_info=True)
        await update.message.reply_text("Erreur lors du chargement du tournoi.")


async def _tournoi_fermer_inscriptions(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    chat_id = data["chat_id"]
    with get_db() as con:
        tournoi = _fetchone(con,
            "SELECT * FROM tournois WHERE chat_id=? AND statut='inscription'", (chat_id,))
    if not tournoi:
        return

    participants = _json_module.loads(tournoi["participants"])
    if len(participants) < 2:
        with get_db() as con:
            _execute(con, "UPDATE tournois SET statut='annule' WHERE id=?", (tournoi["id"],))
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=data["msg_id"],
                text="❌ *Tournoi annulé* — pas assez de participants (minimum 2).",
                reply_markup=None, parse_mode="Markdown"
            )
        except Exception:
            pass
        return

    # Compléter le bracket si nombre impair
    random.shuffle(participants)
    if len(participants) % 2 != 0:
        participants.append({"uid": -1, "nom": "BYE"})

    # Créer les matchs du premier round
    matchs = []
    for i in range(0, len(participants), 2):
        matchs.append({
            "j1": participants[i],
            "j2": participants[i+1],
            "gagnant": None,
            "statut": "attente"
        })

    with get_db() as con:
        _execute(con,
            "UPDATE tournois SET statut='en_cours', bracket=?, round_actuel=1 WHERE id=?",
            (_json_module.dumps({"round": 1, "matchs": matchs}), tournoi["id"]))

    noms = ", ".join(f"@{p['nom']}" for p in participants if p["uid"] != -1)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=data["msg_id"],
            text=f"🏟️ *TOURNOI LANCÉ !*\n\n"
                 f"👥 {len([p for p in participants if p['uid'] != -1])} participants : {noms}\n\n"
                 f"🥊 Round 1 — {len(matchs)} match(s) !\n"
                 f"Les duels commencent maintenant !\n\n"
                 f"_Tapez /tournoi pour voir l'état du bracket._",
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception:
        pass

    # Lancer les matchs automatiquement
    await _tournoi_lancer_matchs(context, chat_id, tournoi["id"])


async def _tournoi_lancer_matchs(context, chat_id: int, tournoi_id: int):
    with get_db() as con:
        tournoi = _fetchone(con, "SELECT * FROM tournois WHERE id=?", (tournoi_id,))
    if not tournoi:
        return
    bracket = _json_module.loads(tournoi["bracket"])
    matchs  = bracket.get("matchs", [])

    for i, match in enumerate(matchs):
        if match["statut"] != "attente":
            continue
        j1, j2 = match["j1"], match["j2"]
        # BYE automatique
        if j2["uid"] == -1:
            matchs[i]["gagnant"] = j1
            matchs[i]["statut"] = "terminé"
            await context.bot.send_message(chat_id,
                f"🏟️ *Match automatique :* @{j1['nom']} passe au round suivant (BYE) !",
                parse_mode="Markdown")
            continue
        if j1["uid"] == -1:
            matchs[i]["gagnant"] = j2
            matchs[i]["statut"] = "terminé"
            await context.bot.send_message(chat_id,
                f"🏟️ *Match automatique :* @{j2['nom']} passe au round suivant (BYE) !",
                parse_mode="Markdown")
            continue

        # Vrai match — lancer un mini-duel
        keyboard = [[InlineKeyboardButton(
            f"⚔️ {j1['nom']} vs {j2['nom']} — Commencer !",
            callback_data=f"tournoi_match_{tournoi_id}_{i}_{j1['uid']}_{j2['uid']}"
        )]]
        await context.bot.send_message(
            chat_id,
            f"🥊 *MATCH DU TOURNOI !*\n\n"
            f"🔵 @{j1['nom']} VS 🔴 @{j2['nom']}\n\n"
            f"Clique pour lancer le duel !",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

    # Sauvegarder l'état du bracket
    bracket["matchs"] = matchs
    with get_db() as con:
        _execute(con, "UPDATE tournois SET bracket=? WHERE id=?",
                 (_json_module.dumps(bracket), tournoi_id))

    # Vérifier si le round est terminé
    tous_termines = all(m["statut"] == "terminé" for m in matchs)
    if tous_termines:
        await _tournoi_prochain_round(context, chat_id, tournoi_id)


async def _tournoi_prochain_round(context, chat_id: int, tournoi_id: int):
    with get_db() as con:
        tournoi = _fetchone(con, "SELECT * FROM tournois WHERE id=?", (tournoi_id,))
    if not tournoi:
        return
    bracket   = _json_module.loads(tournoi["bracket"])
    gagnants  = [m["gagnant"] for m in bracket.get("matchs", []) if m.get("gagnant")]

    if len(gagnants) == 1:
        # Champion !
        champion = gagnants[0]
        m = get_membre_db(champion["uid"], chat_id, champion["nom"])
        nouveau_xp = m["xp"] + 500
        update_membre(champion["uid"], chat_id, xp=nouveau_xp, rang=get_rang(nouveau_xp)["rang"])
        with get_db() as con:
            _execute(con, "UPDATE tournois SET statut='termine' WHERE id=?", (tournoi_id,))
        await context.bot.send_message(
            chat_id,
            f"🏆 *TOURNOI TERMINÉ !*\n\n"
            f"👑 *Champion : @{champion['nom']}* !\n"
            f"💰 *+500 XP* !",
            parse_mode="Markdown"
        )
        return

    # Nouveau round
    if len(gagnants) % 2 != 0:
        gagnants.append({"uid": -1, "nom": "BYE"})

    random.shuffle(gagnants)
    nouveaux_matchs = []
    for i in range(0, len(gagnants), 2):
        nouveaux_matchs.append({"j1": gagnants[i], "j2": gagnants[i+1], "gagnant": None, "statut": "attente"})

    round_num = tournoi["round_actuel"] + 1
    nouveau_bracket = {"round": round_num, "matchs": nouveaux_matchs}

    with get_db() as con:
        _execute(con, "UPDATE tournois SET bracket=?, round_actuel=? WHERE id=?",
                 (_json_module.dumps(nouveau_bracket), round_num, tournoi_id))

    noms = " | ".join(f"@{g['nom']}" for g in gagnants if g["uid"] != -1)
    await context.bot.send_message(
        chat_id,
        f"🏟️ *ROUND {round_num} — {len([g for g in gagnants if g['uid'] != -1])} joueurs restants !*\n\n{noms}",
        parse_mode="Markdown"
    )
    await _tournoi_lancer_matchs(context, chat_id, tournoi_id)


async def callback_tournoi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    # Ne pas appeler answer() ici — fait dans chaque branche
    parts  = query.data.split("_")
    action = parts[1]

    if action == "join":
        with get_db() as con:
            tournoi = _fetchone(con,
                "SELECT * FROM tournois WHERE chat_id=? AND statut='inscription'", (chat_id,))
        if not tournoi:
            await query.answer("Les inscriptions sont fermées !", show_alert=True)
            return
        participants = _json_module.loads(tournoi["participants"])
        if any(p["uid"] == user.id for p in participants):
            await query.answer("Tu es déjà inscrit !", show_alert=True)
            return
        participants.append({"uid": user.id, "nom": user.username or user.first_name})
        with get_db() as con:
            _execute(con, "UPDATE tournois SET participants=? WHERE id=?",
                     (_json_module.dumps(participants), tournoi["id"]))
        nb = len(participants)
        await query.answer(f"✅ Inscrit ! ({nb} participants)")
        # Reconstruire le message sans Markdown pour éviter les erreurs
        liste_noms = ", ".join(p["nom"] for p in participants)
        try:
            keyboard = [[InlineKeyboardButton("⚔️ S'inscrire", callback_data=f"tournoi_join_{chat_id}")]]
            await query.edit_message_text(
                f"TOURNOI OTAKU\n\n"
                f"Inscrits ({nb}) : {liste_noms}\n\n"
                f"En attente... /lancertournoi pour demarrer !",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.warning(f"[callback_tournoi join edit] {e}")

    elif action == "match":
        # parts: tournoi_match_{tournoi_id}_{match_idx}_{j1_uid}_{j2_uid}
        tournoi_id = int(parts[2])
        match_idx  = int(parts[3])
        j1_uid     = int(parts[4])
        j2_uid     = int(parts[5])

        if user.id not in (j1_uid, j2_uid):
            await query.answer("Ce n'est pas ton match !", show_alert=True)
            return

        # Lancer un mini-duel dans le groupe
        with get_db() as con:
            tournoi = _fetchone(con, "SELECT * FROM tournois WHERE id=?", (tournoi_id,))
        if not tournoi:
            return

        bracket = _json_module.loads(tournoi["bracket"])
        match   = bracket["matchs"][match_idx]
        if match["statut"] != "attente":
            await query.answer("Ce match est déjà lancé !", show_alert=True)
            return

        match["statut"] = "en_cours"
        bracket["matchs"][match_idx] = match
        with get_db() as con:
            _execute(con, "UPDATE tournois SET bracket=? WHERE id=?",
                     (_json_module.dumps(bracket), tournoi_id))

        # Simuler un résultat rapide (duel en 3 questions dans le groupe)
        j1 = match["j1"]
        j2 = match["j2"]

        # Pour simplifier, on fait un duel en 1 question et le vainqueur passe
        # (le vrai système de duel complet est dans combats_en_cours)
        q = await _get_question(chat_id, "duel")
        keyboard = [
            [InlineKeyboardButton(f"{['🅰️','🅱️','🅲️','🅳️'][i]} {r}",
             callback_data=f"tournoi_rep_{tournoi_id}_{match_idx}_{i}_{j1_uid}_{j2_uid}")]
            for i, r in enumerate(q["r"])
        ]
        try:
            await query.edit_message_text(
                f"🥊 *Match T{tournoi_id} — Question !*\n\n"
                f"🔵 @{j1['nom']} VS 🔴 @{j2['nom']}\n\n"
                f"{q['q']}\n\n"
                f"⏱️ 20 secondes !",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        except Exception:
            pass

        # Stocker la question en mémoire temporaire
        context.bot_data[f"tournoi_match_{tournoi_id}_{match_idx}"] = {
            "q": q, "reponses": {}, "msg_id": query.message.message_id,
            "j1_uid": j1_uid, "j2_uid": j2_uid,
            "tournoi_id": tournoi_id, "match_idx": match_idx,
            "chat_id": chat_id,
        }
        context.job_queue.run_once(
            _tournoi_match_timeout, 20,
            data={"tournoi_id": tournoi_id, "match_idx": match_idx, "chat_id": chat_id,
                  "msg_id": query.message.message_id},
            name=f"tournoi_mt_{tournoi_id}_{match_idx}"
        )

    elif action == "rep":
        # tournoi_rep_{tournoi_id}_{match_idx}_{rep}_{j1_uid}_{j2_uid}
        tournoi_id = int(parts[2])
        match_idx  = int(parts[3])
        rep        = int(parts[4])
        j1_uid     = int(parts[5])
        j2_uid     = int(parts[6])

        if user.id not in (j1_uid, j2_uid):
            await query.answer("Ce n'est pas ton match !", show_alert=True)
            return

        key = f"tournoi_match_{tournoi_id}_{match_idx}"
        match_data = context.bot_data.get(key)
        if not match_data:
            return
        if user.id in match_data["reponses"]:
            await query.answer("⚡ Déjà répondu !", show_alert=True)
            return

        match_data["reponses"][user.id] = rep
        q = match_data["q"]
        correct = rep == q["b"]
        await query.answer("✅ Correct !" if correct else "❌ Faux !")

        # Si les deux ont répondu, traiter le résultat
        if j1_uid in match_data["reponses"] and j2_uid in match_data["reponses"]:
            for job in context.job_queue.get_jobs_by_name(f"tournoi_mt_{tournoi_id}_{match_idx}"):
                job.schedule_removal()
            await _tournoi_traiter_match(context, match_data, q)


async def _tournoi_match_timeout(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    key  = f"tournoi_match_{data['tournoi_id']}_{data['match_idx']}"
    match_data = context.bot_data.get(key)
    if not match_data:
        return
    with get_db() as con:
        tournoi = _fetchone(con, "SELECT * FROM tournois WHERE id=?", (data["tournoi_id"],))
    if not tournoi:
        return
    q = match_data["q"]
    # Ceux qui n'ont pas répondu obtiennent une mauvaise réponse
    for uid in (match_data["j1_uid"], match_data["j2_uid"]):
        if uid not in match_data["reponses"]:
            match_data["reponses"][uid] = -1
    await _tournoi_traiter_match(context, match_data, q)


async def _tournoi_traiter_match(context, match_data: dict, q: dict):
    j1_uid     = match_data["j1_uid"]
    j2_uid     = match_data["j2_uid"]
    tournoi_id = match_data["tournoi_id"]
    match_idx  = match_data["match_idx"]
    chat_id    = match_data["chat_id"]

    j1_correct = match_data["reponses"].get(j1_uid) == q["b"]
    j2_correct = match_data["reponses"].get(j2_uid) == q["b"]

    with get_db() as con:
        tournoi = _fetchone(con, "SELECT * FROM tournois WHERE id=?", (tournoi_id,))
    if not tournoi:
        return

    bracket = _json_module.loads(tournoi["bracket"])
    match   = bracket["matchs"][match_idx]

    # Déterminer le gagnant
    if j1_correct and not j2_correct:
        gagnant = match["j1"]
        perdant = match["j2"]
    elif j2_correct and not j1_correct:
        gagnant = match["j2"]
        perdant = match["j1"]
    else:
        # Égalité — on regarde qui a répondu en premier (on prend j1 par défaut)
        gagnant = match["j1"]
        perdant = match["j2"]

    match["gagnant"] = gagnant
    match["statut"]  = "terminé"
    bracket["matchs"][match_idx] = match

    with get_db() as con:
        _execute(con, "UPDATE tournois SET bracket=? WHERE id=?",
                 (_json_module.dumps(bracket), tournoi_id))

    bonne_rep = q["r"][q["b"]]
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=match_data["msg_id"],
            text=f"🥊 *Résultat du match !*\n\n"
                 f"Bonne réponse : *{bonne_rep}*\n\n"
                 f"🏆 @{gagnant['nom']} passe au prochain round !\n"
                 f"💔 @{perdant['nom']} est éliminé.",
            reply_markup=None, parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[_tournoi_traiter_match] {e}")

    # Nettoyer
    context.bot_data.pop(f"tournoi_match_{tournoi_id}_{match_idx}", None)

    # Vérifier si le round est terminé
    tous_termines = all(m["statut"] == "terminé" for m in bracket["matchs"])
    if tous_termines:
        await asyncio.sleep(2)
        await _tournoi_prochain_round(context, chat_id, tournoi_id)


# ─────────────────────────────────────────
# 🎨 CARTE DE JOUEUR
# ─────────────────────────────────────────

# ─────────────────────────────────────────
# 🎨 GÉNÉRATION CARTE JOUEUR (Pillow)
# ─────────────────────────────────────────

def _find_font(candidates):
    import os
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None

# Priorité 1 : polices embarquées dans le dossier du bot (portables, pour Railway etc.)
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))

_FONT_BOLD = _find_font([
    os.path.join(_BOT_DIR, "DejaVuSans-Bold.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
])

_FONT_REGULAR = _find_font([
    os.path.join(_BOT_DIR, "DejaVuSans.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf",
])

if not _FONT_BOLD or not _FONT_REGULAR:
    import logging as _log
    _log.getLogger(__name__).warning(
        "[carte] Polices TTF introuvables — /carte utilisera le fallback texte. "
        "Installez fonts-dejavu-core ou ajoutez DejaVuSans.ttf dans le dossier du bot."
    )

# ── Couleurs cyberpunk par rang (néon unique par rang) ──────────────────
_RANG_COLORS = {
    "E":      (100, 110, 130),   # gris acier
    "D":      (80,  200, 240),   # cyan froid
    "C":      (0,   255, 180),   # vert néon
    "B":      (60,  130, 255),   # bleu électrique
    "A":      (255,  60,  90),   # rouge plasma
    "S":      (255, 210,  30),   # jaune laser
    "SS":     (255,  80, 220),   # rose néon
    "SSS":    (180,  60, 255),   # violet cyberpunk
    "NATION": (0,   240, 255),   # cyan ultime
}
# Couleur de fond de la barre néon : version sombre de la couleur rang
def _neon_dark(c, factor=0.15):
    return (int(c[0]*factor), int(c[1]*factor), int(c[2]*factor))

def _neon_mid(c, factor=0.3):
    return (int(c[0]*factor), int(c[1]*factor), int(c[2]*factor))

_RANG_ORDER = ["E","D","C","B","A","S","SS","SSS","NATION"]
_XP_SEUILS  = [0,500,2000,5000,10000,30000,70000,100000,250000,500000]

def _draw_rounded_rect(draw, xy, radius, fill, outline=None, outline_width=2):
    draw.rounded_rectangle(list(xy), radius=radius, fill=fill, outline=outline, width=outline_width)

def _draw_neon_line(draw, x1, y1, x2, y2, color, width=1, glow=True):
    """Ligne avec effet néon (glow = ligne épaisse sombre + fine lumineuse)."""
    if glow:
        r,g,b = color
        glow_c = (min(255,r+40), min(255,g+40), min(255,b+40))
        draw.line((x1,y1,x2,y2), fill=_neon_mid(color, 0.4), width=width+2)
    draw.line((x1,y1,x2,y2), fill=color, width=width)

def _generer_carte_image(username, rang, xp_total, quiz, duels, streak,
                          succes_labels, clan, titre, rang_global,
                          nb_succes_total=0, avatar_bytes=None) -> bytes:
    if not _FONT_BOLD or not _FONT_REGULAR:
        raise RuntimeError("Polices TTF absentes — fallback texte requis")

    W, H = 800, 420
    rc   = _RANG_COLORS.get(rang, (150,150,150))
    r,g,b = rc

    # ── Fond cyberpunk dégradé ──────────────────────────────────────────────
    img  = Image.new("RGBA", (W, H), (0,0,0,255))
    draw = ImageDraw.Draw(img)

    # Fond principal très sombre avec légère teinte rang
    bg_color = (max(8, int(r*0.06)), max(8, int(g*0.06)), max(8, int(b*0.06)))
    bg_mid   = (max(12, int(r*0.09)), max(12, int(g*0.09)), max(12, int(b*0.09)))
    draw.rectangle((0,0,W,H), fill=bg_color)
    # Zone header légèrement plus claire
    draw.rectangle((0,0,W,95), fill=bg_mid)

    # Bordure extérieure néon
    _draw_rounded_rect(draw, (0,0,W-1,H-1), 16, None, rc, 2)

    # Bande latérale gauche néon (3 lignes pour effet glow)
    draw.rectangle((0,0,4,H), fill=_neon_dark(rc, 0.4))
    draw.rectangle((0,0,2,H), fill=rc)

    # Ligne décorative horizontale sous header
    _draw_neon_line(draw, 0, 95, W, 95, rc, width=1)

    # Coins décoratifs cyberpunk (angles lumineux)
    corner = 18
    for cx2, cy2, dx, dy in [(0,0,1,1),(W-1,0,-1,1),(0,H-1,1,-1),(W-1,H-1,-1,-1)]:
        draw.line((cx2,cy2,cx2+dx*corner,cy2), fill=rc, width=2)
        draw.line((cx2,cy2,cx2,cy2+dy*corner), fill=rc, width=2)

    # ── Avatar ──────────────────────────────────────────────────────────────
    av_x, av_y, av_r = 58, 47, 40
    # Anneau néon autour de l'avatar
    for offset, alpha_col in [(5, _neon_dark(rc,0.3)), (3, _neon_dark(rc,0.6)), (1, rc)]:
        draw.ellipse((av_x-av_r-offset, av_y-av_r-offset,
                      av_x+av_r+offset, av_y+av_r+offset), outline=alpha_col, width=1)

    # Fond avatar
    draw.ellipse((av_x-av_r, av_y-av_r, av_x+av_r, av_y+av_r), fill=(15,15,25))

    if avatar_bytes:
        try:
            av_img = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA").resize((av_r*2, av_r*2))
            mask   = Image.new("L", (av_r*2, av_r*2), 0)
            ImageDraw.Draw(mask).ellipse((0,0,av_r*2,av_r*2), fill=255)
            img.paste(av_img, (av_x-av_r, av_y-av_r), mask)
        except: pass
    else:
        draw.text((av_x,av_y), username[:2].upper(),
                  font=ImageFont.truetype(_FONT_BOLD,22), fill=rc, anchor="mm")

    # ── Badge rang (sous avatar) ────────────────────────────────────────────
    badge_w = 64
    bx = av_x - badge_w//2
    by = av_y + av_r + 4
    _draw_rounded_rect(draw, (bx,by,bx+badge_w,by+18), 9, (8,8,18), rc, 1)
    draw.text((av_x, by+9), f"RANG  {rang}",
              font=ImageFont.truetype(_FONT_BOLD, 10), fill=rc, anchor="mm")

    # ── Nom + infos header ──────────────────────────────────────────────────
    name_x = av_x + av_r + 22
    # Nom du joueur
    draw.text((name_x, 28), username[:20],
              font=ImageFont.truetype(_FONT_BOLD, 28), fill=(230,230,255), anchor="lm")
    # Titre sous le nom
    if titre and titre != "Aucun":
        draw.text((name_x, 52), f"« {titre[:28]} »",
                  font=ImageFont.truetype(_FONT_REGULAR, 12), fill=rc, anchor="lm")
    # Clan + classement
    draw.text((name_x, 70),
              f"Clan : {clan[:18]}   //   #{rang_global} MONDIAL",
              font=ImageFont.truetype(_FONT_REGULAR, 11),
              fill=(int(r*0.7)+60, int(g*0.7)+60, int(b*0.7)+60), anchor="lm")

    # ── Badge streak (haut droite) ──────────────────────────────────────────
    if streak > 0:
        streak_col = (255, 200, 30)
        _draw_rounded_rect(draw, (W-162, 18, W-18, 44), 10, (20,18,10), streak_col, 1)
        draw.text((W-90, 31), f"★  STREAK  {streak}j",
                  font=ImageFont.truetype(_FONT_BOLD, 12), fill=streak_col, anchor="mm")

    # ── Section XP ─────────────────────────────────────────────────────────
    y_xp = 108

    # Label + XP actuel
    ri_idx  = _RANG_ORDER.index(rang) if rang in _RANG_ORDER else 0
    xp_prev = _XP_SEUILS[ri_idx]
    xp_next = _XP_SEUILS[ri_idx+1] if ri_idx+1 < len(_XP_SEUILS) else xp_total
    xp_reste = max(0, xp_next - xp_total)
    pct      = min(1.0, (xp_total - xp_prev) / max(1, xp_next - xp_prev))

    draw.text((30, y_xp), "PROGRESSION XP",
              font=ImageFont.truetype(_FONT_REGULAR, 11), fill=(100,100,140))
    draw.text((W-30, y_xp), f"{xp_total:,} XP",
              font=ImageFont.truetype(_FONT_BOLD, 11), fill=rc, anchor="ra")

    # Barre XP néon
    bar_y = y_xp + 14
    bar_h = 10
    bw_bar = W - 60
    # Fond barre
    draw.rounded_rectangle((30, bar_y, 30+bw_bar, bar_y+bar_h), radius=5, fill=(18,18,30))
    # Remplissage néon
    fw = max(0, int(bw_bar * pct))
    if fw > 8:
        # Glow sous la barre
        draw.rounded_rectangle((30, bar_y+1, 30+fw, bar_y+bar_h-1),
                                radius=5, fill=_neon_dark(rc, 0.5))
        draw.rounded_rectangle((30, bar_y, 30+fw, bar_y+bar_h),
                                radius=5, fill=rc)
    # Curseur néon
    if fw > 0:
        cx_cur = 30 + fw
        draw.ellipse((cx_cur-6, bar_y-3, cx_cur+6, bar_y+bar_h+3), fill=rc)
        draw.ellipse((cx_cur-3, bar_y, cx_cur+3, bar_y+bar_h), fill=(255,255,255))

    # XP restant jusqu'au prochain rang
    next_rang = _RANG_ORDER[ri_idx+1] if ri_idx+1 < len(_RANG_ORDER) else "MAX"
    draw.text((30, bar_y+bar_h+6),
              f"{'MAX' if xp_reste == 0 else f'{xp_reste:,} XP'} jusqu'au rang {next_rang}",
              font=ImageFont.truetype(_FONT_REGULAR, 10), fill=(70,70,110))

    # ── Stats cards (4 colonnes) ────────────────────────────────────────────
    y_cards = 158
    card_h  = 62
    stats   = [
        (f"{quiz}",        "QUIZ"),
        (f"{duels}",       "DUELS"),
        (f"{xp_total:,}",  "XP TOTAL"),
        (f"{streak}j",     "STREAK"),
    ]
    cw_card = (W - 70) // 4
    for i, (val, label) in enumerate(stats):
        cx = 30 + i * (cw_card + 6)
        # Fond card avec bordure néon subtile
        _draw_rounded_rect(draw, (cx, y_cards, cx+cw_card, y_cards+card_h),
                           8, (10, 10, 20), _neon_dark(rc, 0.6), 1)
        mid = cx + cw_card // 2
        # Ligne décorative haut de la card
        draw.line((cx+12, y_cards+3, cx+cw_card-12, y_cards+3), fill=_neon_dark(rc, 0.8), width=1)
        draw.text((mid, y_cards+22), val,
                  font=ImageFont.truetype(_FONT_BOLD, 18), fill=rc, anchor="mm")
        draw.text((mid, y_cards+44), label,
                  font=ImageFont.truetype(_FONT_REGULAR, 9),
                  fill=(80, 80, 120), anchor="mm")

    # ── Ligne séparatrice néon ──────────────────────────────────────────────
    y_sep = y_cards + card_h + 10
    _draw_neon_line(draw, 30, y_sep, W-30, y_sep, _neon_dark(rc, 0.8), width=1)

    # ── Succès / Badges ─────────────────────────────────────────────────────
    y_badges = y_sep + 10
    nb_succes_possedes = len([s for s in succes_labels if s])

    # Compteur succès
    draw.text((30, y_badges), "SUCCÈS",
              font=ImageFont.truetype(_FONT_REGULAR, 10), fill=(80,80,120))
    draw.text((30 + 58, y_badges),
              f"{nb_succes_possedes}/{nb_succes_total if nb_succes_total else '?'}",
              font=ImageFont.truetype(_FONT_BOLD, 10), fill=rc)

    # Badges inline
    BADGE_NEON = [rc, (255,80,220), (0,255,180), (255,200,30), (60,130,255), (255,80,80)]
    bx2 = 30
    by2 = y_badges + 18
    for i, s in enumerate(succes_labels[:7]):
        if not s: continue
        bc   = BADGE_NEON[i % len(BADGE_NEON)]
        bw2  = max(52, len(s)*6+18)
        if bx2 + bw2 > W - 30: break
        _draw_rounded_rect(draw, (bx2, by2, bx2+bw2, by2+18), 9, _neon_dark(bc, 0.15), bc, 1)
        draw.text((bx2+bw2//2, by2+9), s[:14],
                  font=ImageFont.truetype(_FONT_REGULAR, 9), fill=bc, anchor="mm")
        bx2 += bw2 + 6

    # ── Watermark ───────────────────────────────────────────────────────────
    draw.text((W-20, H-14), "OTAKU CONQUEST",
              font=ImageFont.truetype(_FONT_REGULAR, 9),
              fill=_neon_dark(rc, 0.7), anchor="ra")

    buf = io.BytesIO()
    img.save(buf, "PNG")
    buf.seek(0)
    return buf.read()


async def cmd_carte(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id
    try:
        target_id   = user.id
        target_name = user.username or user.first_name
        target_user = user

        if update.message.reply_to_message:
            ru          = update.message.reply_to_message.from_user
            target_id   = ru.id
            target_name = ru.username or ru.first_name
            target_user = ru
        elif context.args:
            pseudo = context.args[0].lstrip("@").lower()
            with get_db() as con:
                row = _fetchone(con,
                    "SELECT user_id, username FROM membres WHERE LOWER(username)=? AND chat_id=?",
                    (pseudo, chat_id))
                if not row:
                    row = _fetchone(con,
                        "SELECT user_id, username FROM membres WHERE LOWER(username)=? LIMIT 1",
                        (pseudo,))
            if not row:
                await update.message.reply_text(f"Joueur @{pseudo} introuvable.")
                return
            target_id   = row["user_id"]
            target_name = row["username"] or pseudo
            target_user = None

        with get_db() as con:
            rows = _fetchall(con, "SELECT * FROM membres WHERE user_id=?", (target_id,))

        if not rows:
            await update.message.reply_text(f"{target_name} n'est pas encore enregistré.")
            return

        # Stats agrégées
        xp_total       = sum((r.get("xp") or 0) for r in rows)
        quiz_total     = sum((r.get("quiz_gagnes") or 0) for r in rows)
        duels_total    = sum((r.get("combats_gagnes") or 0) for r in rows)
        streak_max     = max((r.get("streak") or 0) for r in rows)
        ri             = get_rang(xp_total)
        rang_key       = ri["rang"]
        best           = max(rows, key=lambda r: r.get("xp") or 0)
        best           = dict(best)
        for k,d in [("titre",""),("succes",""),("clan_id",None)]:
            best.setdefault(k,d)
            if best[k] is None: best[k]=d

        titre    = best.get("titre") or "Aucun"
        possedes = [s for s in (best.get("succes") or "").split(",") if s]

        clan_nom = "Sans clan"
        if best.get("clan_id"):
            with get_db() as con:
                clan_row = _fetchone(con,"SELECT nom FROM clans WHERE clan_id=?",(best["clan_id"],))
            if clan_row: clan_nom = clan_row["nom"]

        with get_db() as con:
            rg_row = _fetchone(con,"""
                SELECT COUNT(*)+1 AS pos FROM (
                    SELECT user_id, SUM(xp) AS total_xp FROM membres GROUP BY user_id
                ) sub WHERE total_xp > ?""",(xp_total,))
        rang_global = rg_row["pos"] if rg_row else 1

        # Labels succès lisibles
        succes_map = {s["id"]: s["label"] for s in SUCCES_LIST}
        succes_labels = [succes_map.get(s, s) for s in possedes]

        # Photo de profil
        avatar_bytes = None
        try:
            photos = await context.bot.get_user_profile_photos(target_id, limit=1)
            if photos.total_count > 0:
                file = await context.bot.get_file(photos.photos[0][0].file_id)
                buf = io.BytesIO()
                await file.download_to_memory(buf)
                avatar_bytes = buf.getvalue()
        except Exception as e:
            logger.warning(f"[cmd_carte] photo profil: {e}")

        # Génération image (Pillow + polices requis, sinon fallback texte)
        image_ok = PILLOW_OK and bool(_FONT_BOLD) and bool(_FONT_REGULAR)
        if image_ok:
            try:
                await update.message.reply_chat_action("upload_photo")
                png = _generer_carte_image(
                    username        = target_name,
                    rang            = rang_key,
                    xp_total        = xp_total,
                    quiz            = quiz_total,
                    duels           = duels_total,
                    streak          = streak_max,
                    succes_labels   = succes_labels,
                    clan            = clan_nom,
                    titre           = titre,
                    rang_global     = rang_global,
                    nb_succes_total = len(SUCCES_LIST),
                    avatar_bytes    = avatar_bytes,
                )
                await update.message.reply_photo(photo=io.BytesIO(png))
                return
            except Exception as img_err:
                logger.warning(f"[cmd_carte] Génération image échouée, fallback texte : {img_err}")
        # Fallback texte si Pillow absent ou polices manquantes
        xp_next, label_next = xp_prochain_rang(xp_total)
        barre   = barre_xp(xp_total, 10)
        texte   = (
            f"{ri['emoji']} *{target_name.upper()}*\n"
            f"Rang : {rang_key} | {xp_total:,} XP\n"
            f"[{barre}]\n"
            f"#{rang_global} mondial | Clan : {clan_nom}\n"
            f"Quiz : {quiz_total} | Duels : {duels_total} | Streak : {streak_max}j\n"
            f"Succès : {len(possedes)}/{len(SUCCES_LIST)}"
        )
        await update.message.reply_text(texte, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"[cmd_carte] {e}", exc_info=True)
        await update.message.reply_text("Erreur chargement carte.")


async def job_notif_streak(context: ContextTypes.DEFAULT_TYPE):
    """Envoie un MP aux joueurs avec streak >= 3 qui n'ont pas fait /daily aujourd'hui."""
    aujourd_hui = date.today().isoformat()
    with get_db() as con:
        # Joueurs avec streak >= 3, qui ont un private_user (ont déjà utilisé le bot en MP)
        # et qui n'ont pas fait /daily aujourd'hui
        rows = _fetchall(con, """
            SELECT DISTINCT m.user_id, MAX(m.username) AS username,
                            MAX(m.streak) AS streak, MAX(m.dernier_daily) AS dernier_daily
            FROM membres m
            INNER JOIN private_users pu ON pu.user_id = m.user_id
            WHERE m.streak >= 3
            GROUP BY m.user_id
        """)

    for row in rows:
        dernier = row.get("dernier_daily")
        if dernier and datetime.fromisoformat(dernier).date() >= date.today():
            continue   # déjà fait aujourd'hui
        try:
            await context.bot.send_message(
                row["user_id"],
                f"🔥 *Ton streak est en danger !*\n\n"
                f"Tu as un streak de *{row['streak']} jour(s)* et tu n'as pas encore fait ton `/daily` aujourd'hui !\n\n"
                f"⏰ Il reste encore du temps — ne laisse pas ton streak tomber !",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"[job_notif_streak user={row['user_id']}] {e}")


# ─────────────────────────────────────────
# /lancertournoi — Force le démarrage immédiat du tournoi (admin)
# ─────────────────────────────────────────

async def cmd_lancer_tournoi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Le createur du tournoi OU un admin peut forcer le lancement."""
    if not update.message:
        return
    user    = update.effective_user
    chat_id = update.effective_chat.id

    with get_db() as con:
        tournoi = _fetchone(con,
            "SELECT * FROM tournois WHERE chat_id=? AND statut='inscription'", (chat_id,))
    if not tournoi:
        await update.message.reply_text("Aucun tournoi en cours d'inscription. Lance d'abord /tournoi.")
        return

    # Autoriser : le createur du tournoi OU admin groupe OU superadmin
    is_createur = tournoi["createur_id"] == user.id
    is_super    = _check_superadmin(update)
    is_admin    = False
    if not is_createur and not is_super:
        try:
            member   = await context.bot.get_chat_member(chat_id, user.id)
            is_admin = member.status in ("administrator", "creator")
        except Exception:
            pass
    if not is_createur and not is_super and not is_admin:
        await update.message.reply_text("Seul le createur du tournoi ou un admin peut forcer le lancement !")
        return

    participants = _json_module.loads(tournoi["participants"])
    if len(participants) < 2:
        await update.message.reply_text(f"Il faut au moins 2 participants ! ({len(participants)} inscrit(s) pour l'instant)")
        return

    # Annuler le timer automatique
    for job in context.job_queue.get_jobs_by_name(f"tournoi_inscr_{chat_id}"):
        job.schedule_removal()

    await update.message.reply_text(
        f"Lancement du tournoi avec {len(participants)} participants !"
    )
    # Lancer via job_queue avec delai 1s
    context.job_queue.run_once(
        _tournoi_fermer_inscriptions, 1,
        data={"chat_id": chat_id, "msg_id": 0},
        name=f"tournoi_inscr_force_{chat_id}"
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"[ERREUR] {context.error}", exc_info=context.error)

# ─────────────────────────────────────────
# 🚀 LANCEMENT
# ─────────────────────────────────────────

def main():
    from datetime import time as dt_time

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        level=logging.INFO
    )
    if BOT_TOKEN == "REMPLACE_MOI":
        logger.error("❌ BOT_TOKEN non configuré !")
        return
    if not ADMIN_IDS:
        logger.warning("⚠️ ADMIN_IDS vide — aucun super-admin défini.")
    if MEMBRES_MINIMUM == 2:
        logger.warning("⚠️ MEMBRES_MINIMUM=2 — penser à augmenter pour la prod.")

    db_type = "PostgreSQL (Supabase)" if USE_POSTGRES else "SQLite (local)"
    logger.info(f"🗄️ Base de données : {db_type}")

    init_db()
    _charger_questions_json()  # v9: chargement questions JSON

    # Nettoyer les tournois bloques en inscription au redemarrage
    try:
        with get_db() as con:
            _execute(con, "UPDATE tournois SET statut='expire' WHERE statut='inscription'")
        logger.info("Tournois expires nettoyes.")
    except Exception as e:
        logger.warning(f"[cleanup tournois] {e}")

    app = Application.builder().token(BOT_TOKEN).build()

    # ── Membres ──
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("rang",         cmd_rang))
    app.add_handler(CommandHandler("profil",       cmd_rang))   # alias
    app.add_handler(CommandHandler("top",          cmd_top))
    app.add_handler(CommandHandler("weekly",       cmd_weekly))
    app.add_handler(CommandHandler("globalrank",   cmd_globalrank))
    app.add_handler(CommandHandler("worldtop",     cmd_worldtop))
    app.add_handler(CommandHandler("quiz",         cmd_quiz))
    app.add_handler(CommandHandler("quizhc",       cmd_quizhc))
    app.add_handler(CommandHandler("mystere",      cmd_mystere))
    app.add_handler(CommandHandler("war",          cmd_war))
    app.add_handler(CommandHandler("daily",        cmd_daily))
    app.add_handler(CommandHandler("give",         cmd_give))
    app.add_handler(CommandHandler("shop",         cmd_shop))
    app.add_handler(CommandHandler("buy",          cmd_buy))
    app.add_handler(CommandHandler("equip",        cmd_equip))
    # v8 commands
    app.add_handler(CommandHandler("roulette",    cmd_roulette))
    app.add_handler(CommandHandler("quetes",      cmd_quetes))
    app.add_handler(CommandHandler("succes",      cmd_succes))
    app.add_handler(CommandHandler("carte",       cmd_carte))
    app.add_handler(CommandHandler("royale",      cmd_royale))
    app.add_handler(CommandHandler("tournoi",     cmd_tournoi))
    app.add_handler(CommandHandler("lancertournoi", cmd_lancer_tournoi))
    # v9: commandes questions
    app.add_handler(CommandHandler("addquestion",  cmd_addquestion))
    app.add_handler(CommandHandler("qstats",           cmd_questions_stats))
    app.add_handler(CommandHandler("genererquestions", cmd_genererquestions))
    app.add_handler(CommandHandler("resetwar",     cmd_resetwar))
    app.add_handler(CommandHandler("checkbot",     cmd_checkbot))
    app.add_handler(CommandHandler("aide",         cmd_aide))
    app.add_handler(CommandHandler("langue",       cmd_langue))
    app.add_handler(CommandHandler("language",     cmd_langue))

    # ── Clan ──
    app.add_handler(CommandHandler("transfert",    cmd_transfert))
    app.add_handler(CommandHandler("proposer",     cmd_proposer))
    app.add_handler(CommandHandler("clan",         cmd_clan))
    app.add_handler(CommandHandler("clanid",       cmd_clanid))
    app.add_handler(CommandHandler("createclan",   cmd_createclan))
    app.add_handler(CommandHandler("joinclan",     cmd_joinclan))
    app.add_handler(CommandHandler("leaveclan",    cmd_leaveclan))
    app.add_handler(CommandHandler("myclan",       cmd_myclan))
    app.add_handler(CommandHandler("clanrank",     cmd_clanrank))
    app.add_handler(CommandHandler("renameclan",   cmd_renameclan))
    app.add_handler(CommandHandler("deleteclan",   cmd_deleteclan))
    app.add_handler(CommandHandler("clanwar",      cmd_clanwar))
    app.add_handler(CommandHandler("warstat",      cmd_warstat))
    app.add_handler(CommandHandler("warhistory",   cmd_warhistory))

    # ── Super-admin ──
    app.add_handler(CommandHandler("broadcast",    cmd_broadcast))
    app.add_handler(CommandHandler("resetclans",   cmd_resetclans))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("listclans",    cmd_listclans))
    app.add_handler(CommandHandler("givexp",       cmd_givexp))
    app.add_handler(CommandHandler("resetxp",      cmd_resetxp))
    app.add_handler(CommandHandler("removemembre", cmd_removemembre))

    # ── Callbacks ──
    app.add_handler(CallbackQueryHandler(callback_lang,              pattern="^lang_"))
    app.add_handler(CallbackQueryHandler(callback_quiz,              pattern="^quiz_"))
    app.add_handler(CallbackQueryHandler(callback_war,               pattern="^war_"))
    app.add_handler(CallbackQueryHandler(callback_war,               pattern="^duel_"))
    app.add_handler(CallbackQueryHandler(callback_transfert_joueur,  pattern="^tj_"))
    app.add_handler(CallbackQueryHandler(callback_give,              pattern="^give_"))
    app.add_handler(CallbackQueryHandler(callback_top,               pattern="^top_"))
    app.add_handler(CallbackQueryHandler(callback_resetxp,           pattern="^resetxp_"))
    # v8 callbacks
    app.add_handler(CallbackQueryHandler(callback_roulette,          pattern="^roulette_"))
    app.add_handler(CallbackQueryHandler(callback_coffre,            pattern="^coffre_"))
    app.add_handler(CallbackQueryHandler(callback_royale,            pattern="^royale_"))
    app.add_handler(CallbackQueryHandler(callback_tournoi,           pattern="^tournoi_"))

    # ── Messages ──
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, xp_auto))
    # FIX AMÉLIORATION: détection quand le bot rejoint un groupe
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, cmd_new_chat_member))

    # ── Jobs ──
    # FIX BUG: reset hebdo le lundi à minuit pile (pas toutes les heures)
    app.job_queue.run_daily(job_reset_weekly, time=dt_time(0, 0, 0), days=(0,))  # 0 = lundi
    app.job_queue.run_repeating(job_mystere_auto, interval=MYSTERE_INTERVAL_SECONDES, first=300)
    # v8: notification streak en danger à 20h chaque jour
    app.job_queue.run_daily(job_notif_streak, time=dt_time(20, 0, 0))

    # ── v10: Groq — génération automatique de questions ──
    if GROQ_API_KEY:
        # Génération initiale au démarrage (60s après le lancement pour laisser le bot se stabiliser)
        app.job_queue.run_once(
            job_groq_init,
            when=60,
            name="groq_init"
        )
        # Sessions planifiées aux heures définies (UTC)
        for heure in GROQ_SCHEDULE_HOURS:
            app.job_queue.run_daily(
                job_groq_generation,
                time=dt_time(heure, 0, 0),
                name=f"groq_daily_{heure}h"
            )
        logger.info(f"[groq] ✅ Génération auto activée — sessions à {GROQ_SCHEDULE_HOURS} UTC")
    else:
        logger.warning("[groq] ⚠️ GROQ_API_KEY manquante — génération auto désactivée. Ajoute-la dans .env !")

    app.add_error_handler(error_handler)

    logger.info(f"🚀 {UNIVERS_NOM} v9 — Bot lancé !")
    logger.info("⚠️  Le bot doit être ADMIN du groupe OU Privacy Mode désactivé via @BotFather.")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query", "chat_member"],
    )


if __name__ == "__main__":
    import time
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        def log_message(self, *args):
            pass

    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health check HTTP sur port {port}")
    import time
    MAX_RETRIES = 10
    retry = 0
    while True:
        try:
            logger.info(f"🔄 Démarrage du bot (tentative {retry + 1})...")
            main()
        except Exception as e:
            retry += 1
            wait = min(30 * retry, 300)
            logger.error(f"💥 Le bot a crashé : {e}")
            if retry >= MAX_RETRIES:
                logger.critical("❌ Trop de crashs. Arrêt définitif.")
                break
            logger.info(f"⏳ Redémarrage dans {wait}s... (tentative {retry}/{MAX_RETRIES})")
            time.sleep(wait)
        else:
            logger.warning("⚠️ Bot arrêté proprement. Redémarrage dans 10s...")
            time.sleep(10)
            retry = 0
