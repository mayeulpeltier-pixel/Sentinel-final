#!/usr/bin/env python3
# db_manager.py — SENTINEL v3.52 — Gestionnaire SQLite WAL
# =============================================================================
# CORRECTIONS v3.40 :
# BUG-DB1   UNIQUE sur reports.date → ON CONFLICT(date) fonctionnel
# BUG-DB2   datetime SQLite valide → purge 90j opérationnelle
# BUG-DB3   DDL exécuté une seule fois → _DB_INITIALIZED + verrou
# BUG-DB4   Table acteurs CRUD manquant → saveacteur / getacteurs
# BUG-DB5   SQL dynamique savemetrics() → whitelist _ALLOWED_METRIC_COLS
# BUG-DB6   LIMIT ≠ filtre temporel → WHERE date >= date('now', ?)
# BUG-DB7   Wildcards LIKE non échappés → ESCAPE '\\'
# BUG-DB8   rawtail jamais exposée → getrecentreports() inclut rawtail
# AMÉLIORATIONS v3.40 :
# PERF-DB1  Connexions thread-locales → _get_thread_conn / closedb()
# PERF-DB2  VACUUM + ANALYZE automatique → maintenance() corrigé
# MAINT-DB1 Archivage JSON post-migration → .json.migrated
# MAINT-DB2 PRAGMA cache_size / mmap → 32 MB cache, 64 MB mmap
# MAINT-DB3 Index sur colonnes actives → idx_tendances_active / alertes
# EXTRA-1   getmetrics() pour dashboard → lecture métriques agrégées
# EXTRA-2   closedb() nettoyage thread → appelé en fin de cron
#
# CORRECTIONS v3.41 :
# DB41-FIX1 maintenance() : closedb() avant _create_connection()
# DB41-FIX2 runmigration() : rename() → replace()
# DB41-FIX3 loadseen() : filtre temporel WHERE dateseen >= date('now', ?)
# DB41-FIX4 backup_db() : imports shutil/time déplacés en tête de fichier
# DB41-FIX5 getdb() : exc_info=True ajouté sur log.error rollback
# DB41-FIX6 ouvrirealerte() : INSERT OR IGNORE → UPSERT sur (texte, active)
#
# MODIFICATIONS v3.48 — Intégration github_days_back :
# DB-48-COL  _ALLOWED_METRIC_COLS : ajout "github_days_back"
# DB-48-DDL  DDL metrics : colonne github_days_back INTEGER DEFAULT 0
# DB-48-MIG  initdb() : ALTER TABLE migration safe (idempotent)
# DB-48-SEL  getmetrics() : COALESCE(github_days_back, 0) dans SELECT
# DB-48-RPT  get_last_n_reports(n) : méthode pour run_monthly_report()
#
# MODIFICATIONS v3.49 — Export CSV analytiques :
# CSV-1 à CSV-8 : export_csv() dans SentinelDB
#
# MODIFICATIONS v3.50 — Double format standard / Excel FR :
# CSV-9 à CSV-12 : double export standard + excel_fr, configs déclaratives
#
# CORRECTIONS v3.51 — Post-audit final :
# DB-51-FIX1  savereport() : rawtail[-2000:] → rawtail[:2000]
# DB-51-FIX2  DDL : INDEX idx_alertes_texte sur alertes(texte)
# DB-51-FIX3  export_csv() : _CSV_MODE relu au runtime
# DB-51-FIX4  Smoke test BOM : b"ï»¿" correct
# DB-51-PERF1 atexit.register(closedb)
# DB-51-ALIAS loadseendays = loadseen
# DB-51-IDX   INDEX idx_metrics_date sur metrics(date DESC)
#
# CORRECTIONS v3.52 — Audit inter-scripts (SENTINEL audit 2026-04) :
#
# [DB-52-PATH]  CHEMINS ABSOLUS — _ROOT = Path(__file__).resolve().parent
#               DBPATH, SEENJSON, MEMJSON, MIGRATION_FLAG, OUTPUT_DIR_DEFAULT
#               utilisaient des chemins RELATIFS au répertoire de travail.
#               En cron (cd / && python sentinel/db_manager.py), la DB était
#               créée dans / au lieu de ~/sentinel/data/.
#               Fix : tous les chemins par défaut sont désormais absolus depuis
#               la position du fichier db_manager.py lui-même.
#               DBPATH conserve la surcharge ENV SENTINEL_DB (absolue ou relative
#               à l'appelant — comportement explicite documenté).
#
# [DB-52-PAT]   MÉTRIQUES BREVETS — ops_patents.py appelle savemetrics() avec
#               nb_patents, nb_patents_new, nb_patents_critical, top_patent_score.
#               Ces 4 colonnes étaient ABSENTES de _ALLOWED_METRIC_COLS ET du DDL
#               metrics → toutes les métriques brevets loggées WARNING et perdues
#               silencieusement. Correction :
#                 - 4 colonnes ajoutées à _ALLOWED_METRIC_COLS
#                 - 4 colonnes ajoutées au DDL metrics
#                 - getmetrics() SELECT étendu (COALESCE → 0 sur lignes anciennes)
#                 - initdb() : ALTER TABLE safe pour les 4 colonnes (idempotent)
#
# [DB-52-GH]    TABLE github_stars + MÉTHODES get/save_github_stars()
#               github_scraper.py v3.45 appelle :
#                 SentinelDB.get_github_stars(owner, repo, days_ago=7)
#                 SentinelDB.save_github_stars(owner, repo, stars, date)
#               Ces méthodes étaient ABSENTES → AttributeError avalé en debug
#               (_check_star_spike : "except Exception: return 0.0").
#               La star spike detection retournait systématiquement 0.0.
#               Correction :
#                 - Table github_stars(owner, repo, stars, date) dans DDL
#                 - UNIQUE(owner, repo, date) + index sur (owner, repo, date DESC)
#                 - get_github_stars() : SELECT stars le plus récent avant J-N
#                 - save_github_stars() : INSERT OR REPLACE idempotent
#                 - purge_old_github_stars() : nettoyage des données > N jours
#
# [DB-52-TEND]  gettendances(actives_only=True) — charts.py v3.40 appelle
#               SentinelDB.gettendances(actives_only=True) [AttributeError].
#               La méthode existante s'appelait gettendancesactives() sans
#               paramètre. Correction : gettendances() unifiée avec paramètre
#               actives_only, rétrocompatible avec gettendancesactives() conservée
#               comme alias.
#               IMPORTANT : les champs retournés restent texte/count/datepremiere/
#               datederniere — charts.py doit être mis à jour (voir notes bas).
#
# [DB-52-NLP]   MÉTRIQUES NLP — nlp_scorer.py crée sa propre table nlp_metrics
#               hors du DDL centralisé. Ajout d'une table nlp_metrics officielle
#               dans le DDL + méthodes save_nlp_metrics() / get_nlp_metrics()
#               pour centraliser la gestion des tables satellites.
#               Rétrocompatible : si la table existait déjà (créée par nlp_scorer),
#               le DDL IF NOT EXISTS ne la touche pas.
#
# [DB-52-TGM]   MÉTRIQUES TELEGRAM — même pattern que NLP.
#               Table telegram_metrics officielle dans le DDL +
#               méthodes save_telegram_metrics() / get_telegram_metrics().
#
# [DB-52-IDX]   INDEX github_stars, nlp_metrics, telegram_metrics ajoutés.
#
# =============================================================================
# IMPACT SUR LES AUTRES SCRIPTS (voir § en bas de fichier) :
#   github_scraper.py  — aucun changement requis (méthodes maintenant présentes)
#   ops_patents.py     — aucun changement requis (colonnes maintenant dans whitelist)
#   charts.py          — CORRECTION REQUISE : champs tendances (voir bas de fichier)
#   nlp_scorer.py      — save_nlp_metrics() peut utiliser SentinelDB directement
#   telegram_scraper.py— save_telegram_metrics() peut utiliser SentinelDB
#   health_check.py    — vérification tables étendue à 9 tables (voir bas de fichier)
# =============================================================================

from __future__ import annotations

import atexit
import csv
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("sentinel.dbmanager")

# ---------------------------------------------------------------------------
# CHEMINS & CONSTANTES
# ---------------------------------------------------------------------------
# [DB-52-PATH] Chemins absolus depuis la position de db_manager.py
# Garantit le bon répertoire en cron, import depuis un autre répertoire, etc.
# DBPATH : surchargeage par ENV SENTINEL_DB conservé (chemin absolu recommandé).
_ROOT          = Path(__file__).resolve().parent
_DEFAULT_DB    = str(_ROOT / "data" / "sentinel.db")

DBPATH         = Path(os.environ.get("SENTINEL_DB", _DEFAULT_DB))
SEENJSON       = _ROOT / "data" / "seenhashes.json"
MEMJSON        = _ROOT / "data" / "sentinel_memory.json"
MIGRATION_FLAG = _ROOT / "data" / ".migrationdonev3.37"
OUTPUT_DIR_DEFAULT = _ROOT / "output"

_DB_INITIALIZED = False
_DB_INIT_LOCK   = threading.Lock()
_thread_local   = threading.local()

# ---------------------------------------------------------------------------
# WHITELIST savemetrics()
# [DB-48-COL]  github_days_back ajouté en v3.48
# [DB-52-PAT]  4 colonnes brevets ajoutées en v3.52 — ops_patents.py
# ---------------------------------------------------------------------------
_ALLOWED_METRIC_COLS = frozenset({
    # Métriques pipeline quotidien
    "indice", "alerte", "nb_articles", "nb_pertinents",
    # Répartition géographique
    "geousa", "geoeurope", "geoasie", "geomo", "georussie",
    # Répartition domaines
    "terrestre", "maritime", "transverse", "contractuel",
    # GitHub
    "github_days_back",
    # [DB-52-PAT] Brevets (ops_patents.py)
    "nb_patents", "nb_patents_new", "nb_patents_critical", "top_patent_score",
})

# ---------------------------------------------------------------------------
# CSV — configuration (DB-51-FIX3 : _CSV_MODE relu au runtime)
# ---------------------------------------------------------------------------
_CSV_DAYS        = int(os.environ.get("SENTINEL_CSV_DAYS",        "30"))
_CSV_ACTEURS_MAX = int(os.environ.get("SENTINEL_CSV_ACTEURS_MAX", "50"))

_CSV_EXPORT_CONFIGS: list[dict] = [
    {
        "key_suffix":  "",
        "file_suffix": "",
        "delimiter":   ",",
        "decimal_sep": ".",
        "encoding":    "utf-8",
        "modes":       {"both", "standard"},
        "label":       "standard",
    },
    {
        "key_suffix":  "_excel",
        "file_suffix": "_excel",
        "delimiter":   ";",
        "decimal_sep": ",",
        "encoding":    "utf-8-sig",
        "modes":       {"both", "excel"},
        "label":       "excel_fr",
    },
]

# ---------------------------------------------------------------------------
# DDL v3.52 — schéma complet 9 tables
# ---------------------------------------------------------------------------
_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA mmap_size = 67108864;
PRAGMA synchronous = NORMAL;

-- Table rapports journaliers / mensuels
CREATE TABLE IF NOT EXISTS reports (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       TEXT NOT NULL UNIQUE,
    indice     REAL DEFAULT 5.0,
    alerte     TEXT DEFAULT 'VERT',
    compressed TEXT,
    rawtail    TEXT
);

-- Table hashes vus (déduplication articles)
CREATE TABLE IF NOT EXISTS seenhashes (
    hash     TEXT NOT NULL PRIMARY KEY,
    dateseen TEXT NOT NULL,
    source   TEXT
);

-- Table tendances actives
CREATE TABLE IF NOT EXISTS tendances (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    texte        TEXT NOT NULL UNIQUE,
    datepremiere TEXT NOT NULL,
    datederniere TEXT NOT NULL,
    count        INTEGER DEFAULT 1,
    active       INTEGER DEFAULT 1
);

-- Table alertes ouvertes/fermées
CREATE TABLE IF NOT EXISTS alertes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    texte         TEXT NOT NULL,
    dateouverture TEXT NOT NULL,
    datecloture   TEXT,
    active        INTEGER DEFAULT 1
);

-- Table acteurs industriels / étatiques
CREATE TABLE IF NOT EXISTS acteurs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    nom              TEXT NOT NULL UNIQUE,
    pays             TEXT,
    scoreactivite    REAL DEFAULT 0.0,
    derniereactivite TEXT,
    dateajout        TEXT NOT NULL
);

-- Table métriques pipeline (1 ligne/jour)
-- [DB-52-PAT] 4 colonnes brevets ajoutées
CREATE TABLE IF NOT EXISTS metrics (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    date                 TEXT NOT NULL UNIQUE,
    indice               REAL,
    alerte               TEXT,
    nb_articles          INTEGER,
    nb_pertinents        INTEGER,
    geousa               REAL,
    geoeurope            REAL,
    geoasie              REAL,
    geomo                REAL,
    georussie            REAL,
    terrestre            REAL,
    maritime             REAL,
    transverse           REAL,
    contractuel          REAL,
    github_days_back     INTEGER DEFAULT 0,
    nb_patents           INTEGER DEFAULT 0,
    nb_patents_new       INTEGER DEFAULT 0,
    nb_patents_critical  INTEGER DEFAULT 0,
    top_patent_score     REAL    DEFAULT 0.0
);

-- [DB-52-GH] Table stars GitHub (star spike detection)
-- UNIQUE(owner, repo, date) → 1 snapshot/repo/jour idempotent
CREATE TABLE IF NOT EXISTS github_stars (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    owner TEXT NOT NULL,
    repo  TEXT NOT NULL,
    stars INTEGER NOT NULL DEFAULT 0,
    date  TEXT NOT NULL,
    UNIQUE(owner, repo, date)
);

-- [DB-52-NLP] Métriques NLP scorer (nlp_scorer.py)
-- Centralisé ici pour cohérence avec health_check et backups.
-- Compatible avec la table éventuellement créée par nlp_scorer.py (IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS nlp_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    nb_articles     INTEGER DEFAULT 0,
    nb_reranked     INTEGER DEFAULT 0,
    nb_deduped      INTEGER DEFAULT 0,
    nb_cross_ref    INTEGER DEFAULT 0,
    avg_nlp_score   REAL    DEFAULT 0.0,
    top_nlp_score   REAL    DEFAULT 0.0,
    corpus_size     INTEGER DEFAULT 0,
    backend         TEXT    DEFAULT 'tfidf'
);

-- [DB-52-TGM] Métriques Telegram scraper (telegram_scraper.py)
-- Compatible avec la table éventuellement créée par telegram_scraper.py.
CREATE TABLE IF NOT EXISTS telegram_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                TEXT NOT NULL UNIQUE,
    nb_channels         INTEGER DEFAULT 0,
    nb_messages_raw     INTEGER DEFAULT 0,
    nb_articles_kept    INTEGER DEFAULT 0,
    nb_filtered_spam    INTEGER DEFAULT 0,
    nb_filtered_hash    INTEGER DEFAULT 0,
    nb_translated       INTEGER DEFAULT 0,
    avg_score           REAL    DEFAULT 0.0,
    top_channel         TEXT    DEFAULT ''
);

-- Index principaux
CREATE INDEX IF NOT EXISTS idx_reports_date
    ON reports(date DESC);
CREATE INDEX IF NOT EXISTS idx_seenhashes_date
    ON seenhashes(dateseen);
CREATE INDEX IF NOT EXISTS idx_tendances_active
    ON tendances(active, count DESC);
CREATE INDEX IF NOT EXISTS idx_alertes_active
    ON alertes(active, dateouverture DESC);
CREATE INDEX IF NOT EXISTS idx_alertes_texte
    ON alertes(texte);
CREATE INDEX IF NOT EXISTS idx_acteurs_score
    ON acteurs(scoreactivite DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_date
    ON metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_github_stars_repo
    ON github_stars(owner, repo, date DESC);
CREATE INDEX IF NOT EXISTS idx_nlp_metrics_date
    ON nlp_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_telegram_metrics_date
    ON telegram_metrics(date DESC);
"""

# ---------------------------------------------------------------------------
# CONNEXION INTERNE
# ---------------------------------------------------------------------------

def _create_connection() -> sqlite3.Connection:
    global _DB_INITIALIZED
    DBPATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DBPATH), timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA cache_size = -32000")
    conn.execute("PRAGMA mmap_size = 67108864")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    if not _DB_INITIALIZED:
        with _DB_INIT_LOCK:
            if not _DB_INITIALIZED:
                conn.executescript(_DDL)
                conn.commit()
                _DB_INITIALIZED = True
                log.info("DB schéma initialisé (v3.52)")
    return conn

def _get_thread_conn() -> sqlite3.Connection:
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = _create_connection()
        _thread_local.conn = conn
    return conn

def closedb() -> None:
    """
    Ferme et libère la connexion du thread courant.
    Appeler en fin de cron (sentinel_main.py : finally: closedb()).
    [DB-51-PERF1] Enregistré via atexit.register() pour garantir la fermeture
    même en cas de crash avant le finally du pipeline principal.
    """
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        try:
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning(f"closedb() : {e}")
        finally:
            _thread_local.conn = None

@contextmanager
def getdb():
    conn = _get_thread_conn()
    try:
        yield conn
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error(f"DB rollback : {exc}", exc_info=True)
        raise

# ---------------------------------------------------------------------------
# HELPERS CSV — module-level
# ---------------------------------------------------------------------------

def _fr_row(row: dict, decimal_sep: str) -> dict:
    """
    [CSV-10] Normalise les valeurs numériques pour un format d'export donné.
    decimal_sep='.' → identité (standard).
    decimal_sep=',' → float écrits avec virgule (Excel FR).
    """
    if decimal_sep == ".":
        return row
    return {
        k: str(v).replace(".", decimal_sep) if isinstance(v, float) else v
        for k, v in row.items()
    }

# ---------------------------------------------------------------------------
# CLASSE PRINCIPALE — API publique
# ---------------------------------------------------------------------------

class SentinelDB:

    # ------------------------------------------------------------------ #
    # REPORTS                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def savereport(
        date:       str,
        indice:     float,
        alerte:     str,
        compressed: str,
        rawtail:    str = "",
    ) -> None:
        """
        Sauvegarde ou met à jour un rapport journalier/mensuel.

        [DB-51-FIX1] safe_rawtail = (rawtail or "")[:2000]
            Conserve les 2000 PREMIERS caractères (titre, indice, alerte).
            La v3.50 utilisait [-2000:] → début du rapport systématiquement perdu.
        """
        safe_rawtail = (rawtail or "")[:2000]
        with getdb() as db:
            db.execute(
                """
                INSERT INTO reports(date, indice, alerte, compressed, rawtail)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    compressed = excluded.compressed,
                    indice     = excluded.indice,
                    alerte     = excluded.alerte,
                    rawtail    = excluded.rawtail
                """,
                (date, indice, alerte, compressed, safe_rawtail),
            )

    @staticmethod
    def getrecentreports(ndays: int = 7) -> list[dict]:
        """Retourne les rapports des N derniers jours (filtre temporel)."""
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, indice, alerte, compressed, rawtail
                FROM reports
                WHERE date >= date('now', ?)
                ORDER BY date DESC
                LIMIT 90
                """,
                (f"-{ndays} days",),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def get_last_n_reports(n: int = 30) -> list[dict]:
        """
        [DB-48-RPT] Retourne les N derniers rapports (filtre cardinal).
        Utilisé par run_monthly_report() — exactement N entrées indépendamment
        des gaps calendaires (jours fériés, pannes cron).
        """
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, indice, alerte, compressed, rawtail
                FROM reports
                ORDER BY date DESC
                LIMIT ?
                """,
                (n,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def tolegacydict() -> dict:
        """Rétrocompatibilité memory_manager.py (DEPRECATED)."""
        reports = SentinelDB.getrecentreports(ndays=90)
        return {
            "compressed_reports": [
                {
                    "date":       r["date"],
                    "indice":     r["indice"],
                    "alerte":     r["alerte"],
                    "resumebrut": r.get("rawtail", ""),
                }
                for r in reports
            ],
            "tendances":      [t["texte"] for t in SentinelDB.gettendancesactives()],
            "alertesactives": [a["texte"] for a in SentinelDB.getalertesactives()],
            "acteurs":        {a["nom"]: a for a in SentinelDB.getacteurs()},
        }

    # ------------------------------------------------------------------ #
    # SEENHASHES                                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def loadseen(days: int = 90) -> set[str]:
        """
        [DB41-FIX3] Charge uniquement les hashes des N derniers jours.
        [DB-51-ALIAS] loadseendays() est un alias strict de cette méthode.
        """
        with getdb() as db:
            rows = db.execute(
                "SELECT hash FROM seenhashes WHERE dateseen >= datetime('now', ?)",
                (f"-{days} days",),
            ).fetchall()
        return {r["hash"] for r in rows}

    @staticmethod
    def saveseen(seen: set[str], source: str = "") -> None:
        if not seen:
            return
        today = datetime.now(timezone.utc).isoformat()
        with getdb() as db:
            db.executemany(
                "INSERT OR IGNORE INTO seenhashes(hash, dateseen, source) VALUES(?,?,?)",
                [(h, today, source) for h in seen],
            )

    @staticmethod
    def purgeseeolderthandays(days: int = 90) -> int:
        with getdb() as db:
            cur = db.execute(
                "DELETE FROM seenhashes WHERE dateseen < datetime('now', ?)",
                (f"-{days} days",),
            )
        deleted = cur.rowcount
        log.info(f"PURGE seenhashes : {deleted} entrée(s) supprimée(s) (>{days}j)")
        return deleted

    # ------------------------------------------------------------------ #
    # TENDANCES                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def savetendance(texte: str, date: str) -> None:
        texte = (texte or "")[:500]
        if not texte.strip():
            return
        with getdb() as db:
            db.execute(
                """
                INSERT INTO tendances(texte, datepremiere, datederniere, count, active)
                VALUES(?, ?, ?, 1, 1)
                ON CONFLICT(texte) DO UPDATE SET
                    datederniere = excluded.datederniere,
                    count        = count + 1,
                    active       = 1
                """,
                (texte, date, date),
            )

    @staticmethod
    def gettendancesactives(limit: int = 50) -> list[dict]:
        """
        Retourne les tendances actives triées par fréquence.
        Alias conservé pour rétrocompatibilité — utiliser gettendances().

        Champs retournés : texte, count, datepremiere, datederniere
        """
        with getdb() as db:
            rows = db.execute(
                """
                SELECT texte, count, datepremiere, datederniere
                FROM tendances
                WHERE active = 1
                ORDER BY count DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def gettendances(actives_only: bool = True, limit: int = 50) -> list[dict]:
        """
        [DB-52-TEND] Méthode unifiée pour les tendances.

        charts.py v3.40 appelle SentinelDB.gettendances(actives_only=True)
        → AttributeError sur la v3.51 (méthode absente).
        Cette méthode corrige l'erreur et unifie l'accès aux tendances.

        actives_only=True  → équivalent à gettendancesactives() (défaut)
        actives_only=False → toutes les tendances (y compris inactives)

        Champs retournés : texte, count, datepremiere, datederniere, active

        IMPORTANT — charts.py doit utiliser ces noms de champs :
            "texte"        (pas "tendance")
            "count"        (pas "score")
            "datepremiere" (pas "date_ouverture")
            Le champ "categorie" n'existe pas dans le schéma.
            → Voir note de compatibilité en bas de ce fichier.
        """
        if actives_only:
            # Réutilise gettendancesactives() pour la cohérence du code
            rows = SentinelDB.gettendancesactives(limit=limit)
            # Ajoute le champ active=1 pour uniformité
            for r in rows:
                r.setdefault("active", 1)
            return rows
        with getdb() as db:
            db_rows = db.execute(
                """
                SELECT texte, count, datepremiere, datederniere, active
                FROM tendances
                ORDER BY count DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in db_rows]

    # ------------------------------------------------------------------ #
    # ALERTES                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def ouvrirealerte(texte: str, date: str) -> None:
        """[DB41-FIX6] UPSERT sur texte : pas de doublon d'alertes actives."""
        texte = (texte or "")[:500]
        if not texte.strip():
            return
        with getdb() as db:
            existing = db.execute(
                "SELECT id FROM alertes WHERE texte = ? AND active = 1",
                (texte,),
            ).fetchone()
            if existing is None:
                db.execute(
                    "INSERT INTO alertes(texte, dateouverture, active) VALUES(?,?,1)",
                    (texte, date),
                )

    @staticmethod
    def closealerte(texte: str, date: str) -> None:
        """
        [DB-51-FIX2] idx_alertes_texte dans le DDL → O(log n).
        [BUG-DB7]    LIKE avec ESCAPE '\\' → wildcards échappés.
        """
        escaped = (
            texte[:40]
            .replace("\\", "\\\\")
            .replace("%",  "\\%")
            .replace("_",  "\\_")
        )
        with getdb() as db:
            db.execute(
                """
                UPDATE alertes
                SET active = 0, datecloture = ?
                WHERE active = 1
                AND texte LIKE ? ESCAPE '\\'
                """,
                (date, f"%{escaped}%"),
            )

    @staticmethod
    def getalertesactives() -> list[dict]:
        with getdb() as db:
            rows = db.execute(
                """
                SELECT texte, dateouverture
                FROM alertes
                WHERE active = 1
                ORDER BY dateouverture DESC
                """,
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # ACTEURS                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def saveacteur(nom: str, pays: str, score: float, date: str) -> None:
        nom  = (nom  or "")[:200]
        pays = (pays or "")[:100]
        if not nom.strip():
            return
        with getdb() as db:
            db.execute(
                """
                INSERT INTO acteurs(nom, pays, scoreactivite, derniereactivite, dateajout)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(nom) DO UPDATE SET
                    scoreactivite    = excluded.scoreactivite,
                    derniereactivite = excluded.derniereactivite,
                    pays             = COALESCE(excluded.pays, acteurs.pays)
                """,
                (nom, pays, score, date, date),
            )

    @staticmethod
    def getacteurs(limit: int = 20) -> list[dict]:
        with getdb() as db:
            rows = db.execute(
                """
                SELECT nom, pays, scoreactivite, derniereactivite
                FROM acteurs
                ORDER BY scoreactivite DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # METRICS                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def savemetrics(date: str, **kwargs) -> None:
        """
        [BUG-DB5]  Whitelist _ALLOWED_METRIC_COLS — SQL dynamique sécurisé.
        [DB-48-COL] github_days_back persisté.
        [DB-52-PAT] 4 colonnes brevets persistées (nb_patents, nb_patents_new,
                    nb_patents_critical, top_patent_score).
        Colonnes inconnues loggées en WARNING et ignorées.
        """
        safe    = {k: v for k, v in kwargs.items() if k in _ALLOWED_METRIC_COLS}
        unknown = set(kwargs) - _ALLOWED_METRIC_COLS
        if unknown:
            log.warning(f"METRICS colonnes inconnues ignorées : {unknown}")
        if not safe:
            return
        cols = ", ".join(safe.keys())
        plhd = ", ".join(["?"] * len(safe))
        updt = ", ".join(f"{c} = excluded.{c}" for c in safe)
        with getdb() as db:
            db.execute(
                f"INSERT INTO metrics(date, {cols}) VALUES(?, {plhd}) "
                f"ON CONFLICT(date) DO UPDATE SET {updt}",
                [date] + list(safe.values()),
            )

    @staticmethod
    def getmetrics(ndays: int = 30) -> list[dict]:
        """
        [DB-48-SEL] COALESCE sur toutes les colonnes avec DEFAULT 0.
        [DB-52-PAT] 4 colonnes brevets dans le SELECT.

        Champs retournés (tous présents, jamais None) :
          date, indice, alerte, nb_articles, nb_pertinents,
          geousa, geoeurope, geoasie, geomo, georussie,
          terrestre, maritime, transverse, contractuel,
          github_days_back,
          nb_patents, nb_patents_new, nb_patents_critical, top_patent_score
        """
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, indice, alerte, nb_articles, nb_pertinents,
                       geousa, geoeurope, geoasie, geomo, georussie,
                       terrestre, maritime, transverse, contractuel,
                       COALESCE(github_days_back,    0)   AS github_days_back,
                       COALESCE(nb_patents,          0)   AS nb_patents,
                       COALESCE(nb_patents_new,      0)   AS nb_patents_new,
                       COALESCE(nb_patents_critical, 0)   AS nb_patents_critical,
                       COALESCE(top_patent_score,    0.0) AS top_patent_score
                FROM metrics
                WHERE date >= date('now', ?)
                ORDER BY date DESC
                """,
                (f"-{ndays} days",),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # GITHUB STARS — [DB-52-GH]                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_github_stars(owner: str, repo: str, days_ago: int = 7) -> int | None:
        """
        [DB-52-GH] Retourne le nombre de stars enregistré N jours auparavant.

        Cherche le snapshot le plus récent antérieur ou égal à date('now', -N days).
        Retourne None si aucune donnée historique n'est disponible pour ce repo
        (premier passage, ou données trop anciennes après purge).

        Utilisé par github_scraper.py._check_star_spike() :
            prev_stars = SentinelDB.get_github_stars(owner, repo, days_ago=7)
        """
        with getdb() as db:
            row = db.execute(
                """
                SELECT stars FROM github_stars
                WHERE owner = ? AND repo = ?
                  AND date <= date('now', ?)
                ORDER BY date DESC
                LIMIT 1
                """,
                (owner, repo, f"-{days_ago} days"),
            ).fetchone()
        return int(row["stars"]) if row else None

    @staticmethod
    def save_github_stars(owner: str, repo: str, stars: int, date: str) -> None:
        """
        [DB-52-GH] Sauvegarde le nombre de stars d'un repo à une date donnée.

        UNIQUE(owner, repo, date) + ON CONFLICT DO UPDATE → idempotent.
        Permet de rappeler la méthode plusieurs fois dans la même journée
        sans créer de doublons (ex: scraping retry).

        Utilisé par github_scraper.py._check_star_spike() :
            SentinelDB.save_github_stars(owner, repo, current_stars, today)
        """
        with getdb() as db:
            db.execute(
                """
                INSERT INTO github_stars(owner, repo, stars, date)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(owner, repo, date) DO UPDATE SET
                    stars = excluded.stars
                """,
                (owner, repo, int(stars), date),
            )

    @staticmethod
    def purge_old_github_stars(days: int = 90) -> int:
        """
        [DB-52-GH] Supprime les snapshots de stars plus anciens que N jours.
        À appeler périodiquement (watchdog ou maintenance mensuelle).
        """
        with getdb() as db:
            cur = db.execute(
                "DELETE FROM github_stars WHERE date < date('now', ?)",
                (f"-{days} days",),
            )
        deleted = cur.rowcount
        if deleted:
            log.info(f"PURGE github_stars : {deleted} snapshot(s) >{days}j supprimé(s)")
        return deleted

    # ------------------------------------------------------------------ #
    # MÉTRIQUES NLP — [DB-52-NLP]                                         #
    # ------------------------------------------------------------------ #

    @staticmethod
    def save_nlp_metrics(date: str, **kwargs) -> None:
        """
        [DB-52-NLP] Sauvegarde les métriques du scorer NLP.

        Champs acceptés (tous optionnels) :
          nb_articles, nb_reranked, nb_deduped, nb_cross_ref,
          avg_nlp_score, top_nlp_score, corpus_size, backend

        Utilisé par nlp_scorer.py.save_nlp_metrics() :
            SentinelDB.save_nlp_metrics(date, nb_articles=n, ...)
        """
        _NLP_COLS = frozenset({
            "nb_articles", "nb_reranked", "nb_deduped", "nb_cross_ref",
            "avg_nlp_score", "top_nlp_score", "corpus_size", "backend",
        })
        safe    = {k: v for k, v in kwargs.items() if k in _NLP_COLS}
        unknown = set(kwargs) - _NLP_COLS
        if unknown:
            log.warning(f"NLP METRICS colonnes inconnues ignorées : {unknown}")
        if not safe:
            return
        cols = ", ".join(safe.keys())
        plhd = ", ".join(["?"] * len(safe))
        updt = ", ".join(f"{c} = excluded.{c}" for c in safe)
        with getdb() as db:
            db.execute(
                f"INSERT INTO nlp_metrics(date, {cols}) VALUES(?, {plhd}) "
                f"ON CONFLICT(date) DO UPDATE SET {updt}",
                [date] + list(safe.values()),
            )

    @staticmethod
    def get_nlp_metrics(ndays: int = 30) -> list[dict]:
        """[DB-52-NLP] Retourne les métriques NLP des N derniers jours."""
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, nb_articles, nb_reranked, nb_deduped, nb_cross_ref,
                       avg_nlp_score, top_nlp_score, corpus_size, backend
                FROM nlp_metrics
                WHERE date >= date('now', ?)
                ORDER BY date DESC
                """,
                (f"-{ndays} days",),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # MÉTRIQUES TELEGRAM — [DB-52-TGM]                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def save_telegram_metrics(date: str, **kwargs) -> None:
        """
        [DB-52-TGM] Sauvegarde les métriques du scraper Telegram.

        Champs acceptés (tous optionnels) :
          nb_channels, nb_messages_raw, nb_articles_kept,
          nb_filtered_spam, nb_filtered_hash, nb_translated,
          avg_score, top_channel

        Utilisé par telegram_scraper.py._save_telegram_metrics() :
            SentinelDB.save_telegram_metrics(date, nb_channels=n, ...)
        """
        _TGM_COLS = frozenset({
            "nb_channels", "nb_messages_raw", "nb_articles_kept",
            "nb_filtered_spam", "nb_filtered_hash", "nb_translated",
            "avg_score", "top_channel",
        })
        safe    = {k: v for k, v in kwargs.items() if k in _TGM_COLS}
        unknown = set(kwargs) - _TGM_COLS
        if unknown:
            log.warning(f"TELEGRAM METRICS colonnes inconnues ignorées : {unknown}")
        if not safe:
            return
        cols = ", ".join(safe.keys())
        plhd = ", ".join(["?"] * len(safe))
        updt = ", ".join(f"{c} = excluded.{c}" for c in safe)
        with getdb() as db:
            db.execute(
                f"INSERT INTO telegram_metrics(date, {cols}) VALUES(?, {plhd}) "
                f"ON CONFLICT(date) DO UPDATE SET {updt}",
                [date] + list(safe.values()),
            )

    @staticmethod
    def get_telegram_metrics(ndays: int = 30) -> list[dict]:
        """[DB-52-TGM] Retourne les métriques Telegram des N derniers jours."""
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, nb_channels, nb_messages_raw, nb_articles_kept,
                       nb_filtered_spam, nb_filtered_hash, nb_translated,
                       avg_score, top_channel
                FROM telegram_metrics
                WHERE date >= date('now', ?)
                ORDER BY date DESC
                """,
                (f"-{ndays} days",),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # EXPORT CSV — CSV-1 à CSV-12                                         #
    # ------------------------------------------------------------------ #

    @staticmethod
    def export_csv(
        ndays:         int  = _CSV_DAYS,
        limit_acteurs: int  = _CSV_ACTEURS_MAX,
        output_dir:    Path = OUTPUT_DIR_DEFAULT,
    ) -> dict[str, Path]:
        """
        [CSV-1] Export des données brutes en CSV pour les analystes.
        [DB-51-FIX3] _CSV_MODE relu au runtime.
        [CSV-9/12]   Double format standard + excel_fr selon SENTINEL_CSV_MODE.

        Génère jusqu'à 6 fichiers selon mode :
          YYYY-MM-DD_sentinel_metrics.csv / _excel.csv
          YYYY-MM-DD_sentinel_acteurs.csv / _excel.csv
          YYYY-MM-DD_sentinel_tendances.csv / _excel.csv

        Retourne dict[str, Path] compatible mailer.py csv_attachments.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        today   = datetime.now().strftime("%Y-%m-%d")
        exports: dict[str, Path] = {}

        csv_mode = os.environ.get("SENTINEL_CSV_MODE", "both")
        active_configs = [
            cfg for cfg in _CSV_EXPORT_CONFIGS
            if csv_mode in cfg["modes"]
        ]
        if not active_configs:
            log.warning(
                f"CSV export : SENTINEL_CSV_MODE='{csv_mode}' invalide "
                f"(valeurs acceptées : both, standard, excel) — export annulé"
            )
            return exports

        datasets: list[dict] = [
            {
                "name":          "metrics",
                "loader":        lambda: SentinelDB.getmetrics(ndays=ndays),
                "empty_warning": f"table vide ou ndays={ndays} trop court",
            },
            {
                "name":          "acteurs",
                "loader":        lambda: SentinelDB.getacteurs(limit=limit_acteurs),
                "empty_warning": "aucun acteur en base",
            },
            {
                "name":          "tendances",
                "loader":        lambda: SentinelDB.gettendancesactives(limit=200),
                "empty_warning": "aucune tendance active en base",
            },
        ]

        total_rows_written = 0

        for dataset in datasets:
            name = dataset["name"]
            try:
                rows = dataset["loader"]()
            except Exception as e:
                log.error(f"CSV {name} chargement échec : {e}", exc_info=True)
                continue

            if not rows:
                log.warning(f"CSV {name} : {dataset['empty_warning']}")
                continue

            fieldnames = list(rows[0].keys())

            for cfg in active_configs:
                key  = f"{name}{cfg['key_suffix']}"
                path = output_dir / f"{today}_sentinel_{name}{cfg['file_suffix']}.csv"
                try:
                    with open(path, "w", newline="", encoding=cfg["encoding"]) as f:
                        writer = csv.DictWriter(
                            f, fieldnames=fieldnames, delimiter=cfg["delimiter"]
                        )
                        writer.writeheader()
                        writer.writerows(
                            _fr_row(r, cfg["decimal_sep"]) for r in rows
                        )
                    size_kb       = path.stat().st_size // 1024
                    exports[key]  = path
                    total_rows_written += len(rows)
                    log.info(
                        f"CSV [{cfg['label']}] {name} : "
                        f"{len(rows)} lignes → {path.name} ({size_kb} Ko)"
                    )
                except Exception as e:
                    log.error(
                        f"CSV [{cfg['label']}] {name} écriture échec : {e}",
                        exc_info=True,
                    )

        if exports:
            total_kb = sum(p.stat().st_size for p in exports.values()) // 1024
            log.info(
                f"CSV export terminé ({csv_mode}) : "
                f"{len(exports)} fichiers, "
                f"{total_rows_written} lignes, "
                f"{total_kb} Ko → {output_dir}/"
            )
        else:
            log.warning(
                "CSV export : aucun fichier généré "
                "(base vide ? lancer quelques cycles de collecte)"
            )
        return exports

    # ------------------------------------------------------------------ #
    # MAINTENANCE                                                          #
    # ------------------------------------------------------------------ #

    @staticmethod
    def maintenance(force: bool = False) -> None:
        """
        VACUUM + ANALYZE mensuels.
        [DB41-FIX1] closedb() avant _create_connection() — conflict WAL évité.
        Purge automatique des github_stars > 90 jours.
        """
        closedb()
        conn = _create_connection()
        try:
            version   = conn.execute("PRAGMA user_version").fetchone()[0]
            now_month = int(datetime.now().strftime("%Y%m"))
            if not force and version >= now_month:
                return

            log.info("MAINTENANCE : WAL checkpoint + VACUUM + ANALYZE...")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()

            old_isolation        = conn.isolation_level
            conn.isolation_level = None
            conn.execute("VACUUM")
            conn.isolation_level = old_isolation

            conn.execute("ANALYZE")
            conn.execute(f"PRAGMA user_version = {now_month}")
            conn.commit()
            log.info(f"MAINTENANCE terminée (user_version={now_month})")

        except Exception as exc:
            log.error(f"MAINTENANCE erreur : {exc}", exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()

        # Purge satellites lors de la maintenance mensuelle
        SentinelDB.purge_old_github_stars(days=90)

    # ------------------------------------------------------------------ #
    # ALIASES — compatibilité multi-scripts                               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_metrics_ndays(n: int = 1) -> list[dict]:
        """Alias de getmetrics() — mailer.py / report_builder.py."""
        return SentinelDB.getmetrics(ndays=n)

    @staticmethod
    def save_metrics(date: str, **kwargs) -> None:
        """Alias de savemetrics() — sentinel_api.py."""
        return SentinelDB.savemetrics(date, **kwargs)

    @staticmethod
    def save_seen(seen: set[str], source: str = "") -> None:
        """Alias de saveseen() — samgov_scraper.py."""
        return SentinelDB.saveseen(seen, source)

    @staticmethod
    def purgeseen_older_than_days(days: int = 90) -> int:
        """Alias de purgeseeolderthandays() — scraper_rss.py."""
        return SentinelDB.purgeseeolderthandays(days)

    @staticmethod
    def loadseendays(days: int = 90) -> set[str]:
        """
        [DB-51-ALIAS] Alias de loadseen() pour scraper_rss.py et telegram_scraper.py.
        Les deux noms sont strictement équivalents.
        """
        return SentinelDB.loadseen(days=days)


# ---------------------------------------------------------------------------
# HEALTHCHECK
# ---------------------------------------------------------------------------

# [DB-52] Tables officielles étendues à 9 (+ github_stars, nlp_metrics, telegram_metrics)
_EXPECTED_TABLES = frozenset({
    "reports", "seenhashes", "tendances", "alertes",
    "acteurs", "metrics", "github_stars", "nlp_metrics", "telegram_metrics",
})

def check_integrity() -> bool:
    """
    Vérifie l'intégrité SQLite et la présence des 9 tables officielles.
    health_check.py doit appeler cette fonction pour un check complet.
    """
    try:
        with getdb() as db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            wal       = db.execute("PRAGMA journal_mode").fetchone()[0]

            existing_tables = {
                row[0] for row in
                db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            missing = _EXPECTED_TABLES - existing_tables

            ok = (integrity == "ok" and wal == "wal" and not missing)
            if missing:
                log.warning(f"HEALTHCHECK DB : tables manquantes → {missing}")
            log.info(
                f"HEALTHCHECK DB : {'OK' if ok else 'FAIL'} "
                f"(integrity={integrity}, wal={wal}, "
                f"tables={len(existing_tables)}/{len(_EXPECTED_TABLES)})"
            )
            return ok
    except Exception as exc:
        log.error(f"HEALTHCHECK DB exception : {exc}", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# INIT & MIGRATION
# ---------------------------------------------------------------------------

def initdb() -> None:
    """
    Point d'entrée appelé par sentinel_main.py au démarrage.
    Crée le schéma (DDL IF NOT EXISTS) et applique les migrations safe
    pour les DBs existantes (ALTER TABLE idempotent).

    Migrations appliquées :
      [DB-48-MIG]  github_days_back dans metrics
      [DB-51-FIX2] idx_alertes_texte
      [DB-51-IDX]  idx_metrics_date
      [DB-52-PAT]  4 colonnes brevets dans metrics
      [DB-52-GH]   table github_stars (DDL IF NOT EXISTS suffit)
      [DB-52-NLP]  table nlp_metrics  (DDL IF NOT EXISTS suffit)
      [DB-52-TGM]  table telegram_metrics (DDL IF NOT EXISTS suffit)
    """
    DBPATH.parent.mkdir(parents=True, exist_ok=True)
    # Crée le schéma complet (nouvelles tables via IF NOT EXISTS)
    with getdb():
        pass

    # ── Migrations métriques colonnes ────────────────────────────────────────
    _metric_migrations = [
        # (colonne, définition SQL)
        ("github_days_back",    "INTEGER DEFAULT 0"),
        ("nb_patents",          "INTEGER DEFAULT 0"),
        ("nb_patents_new",      "INTEGER DEFAULT 0"),
        ("nb_patents_critical", "INTEGER DEFAULT 0"),
        ("top_patent_score",    "REAL    DEFAULT 0.0"),
    ]
    for col, col_def in _metric_migrations:
        try:
            with getdb() as db:
                db.execute(f"ALTER TABLE metrics ADD COLUMN {col} {col_def}")
            log.info(f"DB-MIG : colonne metrics.{col} ajoutée")
        except Exception:
            pass  # Colonne déjà présente — comportement normal

    # ── Migrations index ─────────────────────────────────────────────────────
    _index_migrations = [
        ("idx_alertes_texte",        "CREATE INDEX IF NOT EXISTS idx_alertes_texte ON alertes(texte)"),
        ("idx_metrics_date",         "CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics(date DESC)"),
        ("idx_github_stars_repo",    "CREATE INDEX IF NOT EXISTS idx_github_stars_repo ON github_stars(owner, repo, date DESC)"),
        ("idx_nlp_metrics_date",     "CREATE INDEX IF NOT EXISTS idx_nlp_metrics_date ON nlp_metrics(date DESC)"),
        ("idx_telegram_metrics_date","CREATE INDEX IF NOT EXISTS idx_telegram_metrics_date ON telegram_metrics(date DESC)"),
    ]
    for idx_name, idx_sql in _index_migrations:
        try:
            with getdb() as db:
                db.execute(idx_sql)
            log.debug(f"DB-MIG : index {idx_name} vérifié/créé")
        except Exception as e:
            log.warning(f"DB-MIG : index {idx_name} non créé ({e})")

    # ── Migration JSON → SQLite (première fois) ───────────────────────────────
    if not MIGRATION_FLAG.exists() and (SEENJSON.exists() or MEMJSON.exists()):
        runmigration()


def runmigration() -> None:
    """
    Migre JSON → SQLite (v3.37 → v3.40).
    [DB41-FIX2] rename() → replace() : FileExistsError Windows évitée.
    """
    log.info("MIGRATION JSON → SQLite démarrage...")

    if SEENJSON.exists():
        try:
            raw    = json.loads(SEENJSON.read_text(encoding="utf-8"))
            hashes = raw if isinstance(raw, list) else list(raw)
            today  = datetime.now(timezone.utc).isoformat()
            with getdb() as db:
                db.executemany(
                    "INSERT OR IGNORE INTO seenhashes(hash, dateseen) VALUES(?,?)",
                    [(h, today) for h in hashes],
                )
            SEENJSON.replace(SEENJSON.with_suffix(".json.migrated"))
            log.info(f"MIGRATION seenhashes : {len(hashes)} hash(es) migrés")
        except Exception as exc:
            log.error(f"MIGRATION seenhashes échec : {exc}")

    if MEMJSON.exists():
        try:
            memory = json.loads(MEMJSON.read_text(encoding="utf-8"))
            today  = datetime.now().strftime("%Y-%m-%d")
            for report in (
                memory.get("compressedreports")
                or memory.get("compressed_reports", [])
            ):
                SentinelDB.savereport(
                    date       = report.get("date", today),
                    indice     = float(report.get("indice", 5.0)),
                    alerte     = report.get("alerte", "VERT"),
                    compressed = json.dumps(report, ensure_ascii=False),
                    rawtail    = report.get("resumebrut", ""),
                )
            for texte in memory.get("tendances", []):
                SentinelDB.savetendance(texte, today)
            for texte in memory.get("alertesactives", []):
                SentinelDB.ouvrirealerte(texte, today)
            for nom, data in memory.get("acteurs", {}).items():
                SentinelDB.saveacteur(
                    nom   = nom,
                    pays  = data.get("pays", ""),
                    score = float(data.get("scoreactivite", 0.0)),
                    date  = today,
                )
            MEMJSON.replace(MEMJSON.with_suffix(".json.migrated"))
            log.info("MIGRATION mémoire terminée")
        except Exception as exc:
            log.error(f"MIGRATION mémoire échec : {exc}")

    MIGRATION_FLAG.write_text("v3.52", encoding="utf-8")
    log.info("MIGRATION terminée — flag écrit")
    closedb()


# ---------------------------------------------------------------------------
# BACKUP AUTOMATIQUE SQLite
# ---------------------------------------------------------------------------

def backup_db(dest_dir: str = "backups", keep_days: int = 30) -> bool:
    """
    Sauvegarde atomique sentinel.db → dest_dir/sentinel_YYYYMMDD.db.
    Purge automatique des backups > keep_days jours.
    Le répertoire dest_dir est relatif à _ROOT si non absolu.
    """
    try:
        dest = Path(dest_dir) if Path(dest_dir).is_absolute() else _ROOT / dest_dir
        dest.mkdir(parents=True, exist_ok=True)

        if not DBPATH.exists():
            log.warning("BACKUP sentinel.db absent — rien à sauvegarder")
            return False

        date_str    = datetime.now().strftime("%Y%m%d")
        backup_path = dest / f"sentinel_{date_str}.db"

        with getdb() as db:
            db.execute("PRAGMA wal_checkpoint(FULL)")

        shutil.copy2(str(DBPATH), str(backup_path))
        size_kb = backup_path.stat().st_size // 1024
        log.info(f"BACKUP OK → {backup_path} ({size_kb} Ko)")

        cutoff  = time.time() - keep_days * 86400
        removed = 0
        for f in dest.glob("sentinel_*.db"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        if removed:
            log.info(f"BACKUP purge : {removed} fichier(s) >{keep_days}j supprimé(s)")

        return True

    except Exception as exc:
        log.error(f"BACKUP échec : {exc}", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# ALIASES MODULE-LEVEL
# ---------------------------------------------------------------------------
get_db        = getdb
init_db       = initdb
run_migration = runmigration

# [DB-51-PERF1] Fermeture propre au exit même si finally n'est pas atteint
atexit.register(closedb)


# ---------------------------------------------------------------------------
# SMOKE TESTS v3.52
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Reconfiguration pour le test isolé
        os.environ["SENTINEL_DB"] = f"{tmpdir}/test.db"
        global _DB_INITIALIZED, DBPATH
        _DB_INITIALIZED = False
        DBPATH          = Path(f"{tmpdir}/test.db")

        initdb()
        today = datetime.now().strftime("%Y-%m-%d")

        # ── BUG-DB1 : double insert même date → UPSERT ───────────────────────
        SentinelDB.savereport(today, 7.2, "ORANGE", "Rapport test",  "...brut")
        SentinelDB.savereport(today, 8.0, "ROUGE",  "Rapport MAJ",   "...brut MAJ")
        reports = SentinelDB.getrecentreports(ndays=7)
        assert len(reports) == 1,                        "BUG-DB1 : plus d'1 rapport"
        assert reports[0]["alerte"] == "ROUGE",          "BUG-DB1 : upsert échoué"
        assert reports[0]["rawtail"] == "...brut MAJ",   "BUG-DB8 : rawtail incorrecte"

        # ── DB-51-FIX1 : rawtail[:2000] conserve le début ────────────────────
        long_text = "DEBUT" + ("X" * 3000) + "FIN"
        SentinelDB.savereport("2025-01-01", 5.0, "VERT", "compressed", long_text)
        row_trunc = next(
            r for r in SentinelDB.get_last_n_reports(10)
            if r["date"] == "2025-01-01"
        )
        assert row_trunc["rawtail"].startswith("DEBUT"), \
            "DB-51-FIX1 : rawtail ne commence pas par DEBUT"
        assert not row_trunc["rawtail"].endswith("FIN"), \
            "DB-51-FIX1 : rawtail ne doit pas contenir FIN"

        # ── seenhashes ────────────────────────────────────────────────────────
        SentinelDB.saveseen({"h1", "h2"}, source="Test")
        seen       = SentinelDB.loadseen(days=90)
        seen_alias = SentinelDB.loadseendays(days=90)
        assert "h1" in seen,           "DB41-FIX3 : loadseen filtre KO"
        assert seen == seen_alias,     "DB-51-ALIAS : loadseen ≠ loadseendays"
        assert isinstance(SentinelDB.purgeseeolderthandays(90), int), "BUG-DB2 KO"

        # ── acteurs ───────────────────────────────────────────────────────────
        SentinelDB.saveacteur("Milrem Robotics", "EST", 8.5, today)
        SentinelDB.saveacteur("Milrem Robotics", None,  9.0, today)
        acteurs = SentinelDB.getacteurs()
        assert acteurs[0]["scoreactivite"] == 9.0, "BUG-DB4 : update score KO"
        assert acteurs[0]["pays"] == "EST",        "BUG-DB4 : COALESCE pays KO"

        # ── métriques pipeline ────────────────────────────────────────────────
        SentinelDB.savemetrics(today,
                               indice=7.2, alerte="ORANGE",
                               github_days_back=10, colonneInvalide=99)
        metrics = SentinelDB.getmetrics(ndays=7)
        assert len(metrics) == 1,                    "EXTRA-1 : getmetrics KO"
        assert metrics[0]["github_days_back"] == 10, "DB-48-COL : github_days_back KO"

        # ── [DB-52-PAT] métriques brevets ─────────────────────────────────────
        SentinelDB.savemetrics(today,
                               nb_patents=15, nb_patents_new=3,
                               nb_patents_critical=2, top_patent_score=8.5)
        metrics2 = SentinelDB.getmetrics(ndays=7)
        assert metrics2[0]["nb_patents"]          == 15,  "DB-52-PAT : nb_patents KO"
        assert metrics2[0]["nb_patents_new"]      == 3,   "DB-52-PAT : nb_patents_new KO"
        assert metrics2[0]["nb_patents_critical"] == 2,   "DB-52-PAT : nb_patents_critical KO"
        assert metrics2[0]["top_patent_score"]    == 8.5, "DB-52-PAT : top_patent_score KO"

        # COALESCE → 0 sur lignes sans brevets (anciennes DBs)
        SentinelDB.savemetrics("2000-01-01", indice=5.0, alerte="VERT")
        old_rows = [
            r for r in SentinelDB.getmetrics(ndays=365 * 30)
            if r["date"] == "2000-01-01"
        ]
        assert old_rows[0]["nb_patents"] == 0, "DB-52-PAT : COALESCE NULL→0 KO"

        # ── [DB-52-GH] github_stars ────────────────────────────────────────────
        SentinelDB.save_github_stars("anduril", "lattice-sdk-python", 1200, "2026-04-07")
        SentinelDB.save_github_stars("anduril", "lattice-sdk-python", 1250, today)

        prev = SentinelDB.get_github_stars("anduril", "lattice-sdk-python", days_ago=7)
        assert prev == 1200, f"DB-52-GH : get_github_stars KO (reçu {prev})"

        # Idempotence (même date)
        SentinelDB.save_github_stars("anduril", "lattice-sdk-python", 1260, today)
        curr = SentinelDB.get_github_stars("anduril", "lattice-sdk-python", days_ago=0)
        assert curr == 1260, f"DB-52-GH : idempotence ON CONFLICT KO (reçu {curr})"

        # Repo sans historique → None
        no_hist = SentinelDB.get_github_stars("unknown", "repo", days_ago=7)
        assert no_hist is None, "DB-52-GH : None attendu pour repo sans historique"

        purged = SentinelDB.purge_old_github_stars(days=1)
        assert isinstance(purged, int), "DB-52-GH : purge_old_github_stars KO"

        # ── [DB-52-TEND] gettendances(actives_only=True/False) ─────────────────
        SentinelDB.savetendance("Essaims FPV Ukraine",       today)
        SentinelDB.savetendance("UGV NATO interopérabilité", today)

        tend_active = SentinelDB.gettendances(actives_only=True)
        tend_all    = SentinelDB.gettendances(actives_only=False)
        tend_legacy = SentinelDB.gettendancesactives()
        assert len(tend_active) == len(tend_legacy), \
            "DB-52-TEND : gettendances(actives_only=True) ≠ gettendancesactives()"
        assert len(tend_all) >= len(tend_active),    \
            "DB-52-TEND : gettendances(all) < gettendances(active)"
        assert "texte" in tend_active[0],            \
            "DB-52-TEND : champ 'texte' absent"
        assert "count" in tend_active[0],            \
            "DB-52-TEND : champ 'count' absent (pas 'score')"
        assert "datepremiere" in tend_active[0],     \
            "DB-52-TEND : champ 'datepremiere' absent (pas 'date_ouverture')"

        # ── [DB-52-NLP] save/get_nlp_metrics ──────────────────────────────────
        SentinelDB.save_nlp_metrics(today,
                                    nb_articles=120, nb_reranked=95,
                                    nb_deduped=8, avg_nlp_score=0.72,
                                    backend="tfidf")
        nlp = SentinelDB.get_nlp_metrics(ndays=7)
        assert len(nlp) == 1,                  "DB-52-NLP : get_nlp_metrics KO"
        assert nlp[0]["nb_articles"] == 120,   "DB-52-NLP : nb_articles KO"
        assert nlp[0]["backend"] == "tfidf",   "DB-52-NLP : backend KO"

        # ── [DB-52-TGM] save/get_telegram_metrics ─────────────────────────────
        SentinelDB.save_telegram_metrics(today,
                                         nb_channels=11, nb_messages_raw=340,
                                         nb_articles_kept=42, avg_score=6.8)
        tgm = SentinelDB.get_telegram_metrics(ndays=7)
        assert len(tgm) == 1,                   "DB-52-TGM : get_telegram_metrics KO"
        assert tgm[0]["nb_channels"] == 11,     "DB-52-TGM : nb_channels KO"
        assert tgm[0]["nb_articles_kept"] == 42,"DB-52-TGM : nb_articles_kept KO"

        # ── alertes ───────────────────────────────────────────────────────────
        SentinelDB.ouvrirealerte("Déploiement KARGU 100% confirmé", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel",        today)
        SentinelDB.closealerte("KARGU 100%", today)
        alertes = SentinelDB.getalertesactives()
        assert len(alertes) == 1,               "BUG-DB7 : mauvais nb d'alertes"
        assert "Type_X" in alertes[0]["texte"], "BUG-DB7 : mauvaise alerte fermée"

        # DB41-FIX6 : pas de doublon
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        alertes2 = SentinelDB.getalertesactives()
        assert len(alertes2) == 1, "DB41-FIX6 : doublon alerte détecté"

        # ── healthcheck étendu (9 tables) ─────────────────────────────────────
        assert check_integrity(), "HEALTHCHECK v3.52 FAIL"

        # ── CSV ───────────────────────────────────────────────────────────────
        csv_out = Path(tmpdir) / "csv_test"
        os.environ["SENTINEL_CSV_MODE"] = "both"
        exports_both = SentinelDB.export_csv(ndays=30, limit_acteurs=10, output_dir=csv_out)
        assert "metrics"         in exports_both, "CSV : metrics standard absent"
        assert "metrics_excel"   in exports_both, "CSV : metrics_excel absent"
        assert "tendances"       in exports_both, "CSV : tendances absent"

        # BOM UTF-8
        with open(exports_both["metrics_excel"], "rb") as f:
            bom = f.read(3)
        assert bom == b"ï»¿", f"CSV BOM KO : {bom!r}"

        # mode standard
        os.environ["SENTINEL_CSV_MODE"] = "standard"
        exports_std = SentinelDB.export_csv(ndays=30, limit_acteurs=10, output_dir=csv_out)
        assert "metrics"       in exports_std,     "CSV mode=standard KO"
        assert "metrics_excel" not in exports_std, "CSV mode=standard : excel généré"

        # closedb thread-local
        closedb()
        assert getattr(_thread_local, "conn", None) is None, "PERF-DB1 : leak connexion"

        print("
" + "=" * 65)
        print("✅  Tous les smoke tests v3.52 passent.")
        print(f"    Reports             : {len(reports)}")
        print(f"    rawtail[:2000]      : OK (DEBUT conservé)")
        print(f"    loadseen / alias    : {len(seen)} hashes")
        print(f"    Acteurs             : {len(acteurs)}")
        print(f"    Métriques pipeline  : {len(metrics2)}")
        print(f"    Métriques brevets   : nb_patents={metrics2[0]['nb_patents']} ✅")
        print(f"    GitHub stars        : prev={prev} curr={curr} ✅")
        print(f"    gettendances()      : {len(tend_active)} actives ✅")
        print(f"    NLP metrics         : {len(nlp)} entrée(s) ✅")
        print(f"    Telegram metrics    : {len(tgm)} entrée(s) ✅")
        print(f"    Alertes actives     : {len(alertes2)}")
        print(f"    HealthCheck 9 tables: OK ✅")
        print(f"    CSV mode=both       : {list(exports_both.keys())}")
        print(f"    CSV BOM             : {bom!r} ✅")
        print("=" * 65)


