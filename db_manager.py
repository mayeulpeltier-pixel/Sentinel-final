# db_manager.py --- SENTINEL v3.41 --- Gestionnaire SQLite WAL
# =============================================================================
# CORRECTIONS v3.40 :
# BUG-DB1  UNIQUE sur reports.date → ON CONFLICT(date) fonctionnel
# BUG-DB2  datetime SQLite valide → purge 90j opérationnelle
# BUG-DB3  DDL exécuté une seule fois → _DB_INITIALIZED + verrou
# BUG-DB4  Table acteurs CRUD manquant → saveacteur / getacteurs
# BUG-DB5  SQL dynamique savemetrics() → whitelist _ALLOWED_METRIC_COLS
# BUG-DB6  LIMIT ≠ filtre temporel → WHERE date >= date('now', ?)
# BUG-DB7  Wildcards LIKE non échappés → ESCAPE '\\'
# BUG-DB8  rawtail jamais exposée → getrecentreports() inclut rawtail
# AMÉLIORATIONS v3.40 :
# PERF-DB1 Connexions thread-locales → _get_thread_conn / closedb()
# PERF-DB2 VACUUM + ANALYZE automatique → maintenance() corrigé
# MAINT-DB1 Archivage JSON post-migration → .json.migrated
# MAINT-DB2 PRAGMA cache_size / mmap → 32 MB cache, 64 MB mmap
# MAINT-DB3 Index sur colonnes actives → idx_tendances_active / alertes
# EXTRA-1  getmetrics() pour dashboard → lecture métriques agrégées
# EXTRA-2  closedb() nettoyage thread → appelé en fin de cron
#
# CORRECTIONS v3.41 :
# DB41-FIX1 maintenance() : closedb() avant _create_connection() pour éviter
#           deux connexions simultanées pendant VACUUM (conflict WAL)
# DB41-FIX2 runmigration() : rename() → replace() (FileExistsError sur Windows
#           si .json.migrated existe déjà)
# DB41-FIX3 loadseen() : filtre temporel WHERE dateseen >= date('now', ?)
#           évite de charger des centaines de milliers de hashes en RAM
#           après plusieurs mois de production
# DB41-FIX4 backup_db() : imports shutil/time déplacés en tête de fichier
#           (étaient locaux à la fonction → re-importés à chaque appel)
# DB41-FIX5 getdb() : exc_info=True ajouté sur log.error rollback
#           → stack trace complète dans les logs
# DB41-FIX6 ouvrirealerte() : INSERT OR IGNORE remplacé par un UPSERT
#           sur (texte, active) pour éviter les doublons d'alertes actives
# =============================================================================
# Compatibilité pipeline v3.37 totalement préservée (API SentinelDB inchangée)
# =============================================================================

import json
import logging
import os
import shutil      # DB41-FIX4 — était importé localement dans backup_db()
import sqlite3
import threading
import time        # DB41-FIX4 — était importé localement dans backup_db()
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("dbmanager")

# ---------------------------------------------------------------------------
# CHEMINS & CONSTANTES
# ---------------------------------------------------------------------------
DBPATH         = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
SEENJSON       = Path("data/seenhashes.json")
MEMJSON        = Path("data/sentinel_memory.json")
MIGRATION_FLAG = Path("data/.migrationdonev3.37")

# BUG-DB3 FIX — DDL exécuté une seule fois par process (thread-safe)
_DB_INITIALIZED = False
_DB_INIT_LOCK   = threading.Lock()

# PERF-DB1 FIX — connexion réutilisée dans le même thread
_thread_local = threading.local()

# BUG-DB5 FIX — whitelist colonnes savemetrics()
_ALLOWED_METRIC_COLS = frozenset({
    "indice", "alerte", "nb_articles", "nb_pertinents",
    "geousa", "geoeurope", "geoasie", "geomo", "georussie",
    "terrestre", "maritime", "transverse", "contractuel",
})

# ---------------------------------------------------------------------------
# DDL v3.41 — schéma complet 6 tables
# ---------------------------------------------------------------------------
_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA mmap_size = 67108864;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS reports (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    date     TEXT NOT NULL UNIQUE,
    indice   REAL DEFAULT 5.0,
    alerte   TEXT DEFAULT 'VERT',
    compressed TEXT,
    rawtail  TEXT
);

CREATE TABLE IF NOT EXISTS tendances (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    texte        TEXT NOT NULL UNIQUE,
    datepremiere TEXT NOT NULL,
    datederniere TEXT NOT NULL,
    count        INTEGER DEFAULT 1,
    active       INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS alertes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    texte         TEXT NOT NULL,
    dateouverture TEXT NOT NULL,
    datecloture   TEXT,
    active        INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS acteurs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    nom              TEXT NOT NULL UNIQUE,
    pays             TEXT,
    scoreactivite    REAL DEFAULT 0.0,
    derniereactivite TEXT,
    dateajout        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS seenhashes (
    hash     TEXT NOT NULL PRIMARY KEY,
    dateseen TEXT NOT NULL,
    source   TEXT
);

CREATE TABLE IF NOT EXISTS metrics (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT NOT NULL UNIQUE,
    indice        REAL,
    alerte        TEXT,
    nb_articles   INTEGER,
    nb_pertinents INTEGER,
    geousa        REAL,
    geoeurope     REAL,
    geoasie       REAL,
    geomo         REAL,
    georussie     REAL,
    terrestre     REAL,
    maritime      REAL,
    transverse    REAL,
    contractuel   REAL
);

CREATE INDEX IF NOT EXISTS idx_reports_date
    ON reports(date DESC);
CREATE INDEX IF NOT EXISTS idx_seenhashes_date
    ON seenhashes(dateseen);
CREATE INDEX IF NOT EXISTS idx_tendances_active
    ON tendances(active, count DESC);
CREATE INDEX IF NOT EXISTS idx_alertes_active
    ON alertes(active, dateouverture DESC);
CREATE INDEX IF NOT EXISTS idx_acteurs_score
    ON acteurs(scoreactivite DESC);
"""

# ---------------------------------------------------------------------------
# CONNEXION INTERNE
# ---------------------------------------------------------------------------

def _create_connection() -> sqlite3.Connection:
    global _DB_INITIALIZED
    DBPATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(DBPATH),
        timeout=15,
        check_same_thread=False,
    )
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
                log.info("DB schéma initialisé (v3.41)")
    return conn


def _get_thread_conn() -> sqlite3.Connection:
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = _create_connection()
        _thread_local.conn = conn
    return conn


def closedb() -> None:
    """Ferme et libère la connexion du thread courant. Appeler en fin de cron."""
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
        log.error(f"DB rollback : {exc}", exc_info=True)  # DB41-FIX5
        raise

# ---------------------------------------------------------------------------
# CLASSE PRINCIPALE — API publique compatible v3.37
# ---------------------------------------------------------------------------

class SentinelDB:

    # ------------------------------------------------------------------ #
    # REPORTS                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def savereport(date: str, indice: float, alerte: str,
                   compressed: str, rawtail: str = "") -> None:
        safe_rawtail = (rawtail or "")[-2000:]
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
    def getrecentreports(ndays: int = 7) -> list:
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
    def tolegacydict() -> dict:
        """Rétrocompatibilité memory_manager.py (DEPRECATED — utiliser SentinelDB)."""
        reports = SentinelDB.getrecentreports(ndays=90)
        return {
            "compressed_reports": [
                {
                    "date":      r["date"],
                    "indice":    r["indice"],
                    "alerte":    r["alerte"],
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
    def loadseen(days: int = 90) -> set:
        """
        DB41-FIX3 — Charge uniquement les hashes des N derniers jours.
        Évite de charger toute la table en RAM après plusieurs mois de production.
        Paramètre days synchronisé avec purgeseeolderthandays() (défaut 90j).
        """
        with getdb() as db:
            rows = db.execute(
                "SELECT hash FROM seenhashes WHERE dateseen >= datetime('now', ?)",
                (f"-{days} days",),
            ).fetchall()
        return {r["hash"] for r in rows}

    @staticmethod
    def saveseen(seen: set, source: str = "") -> None:
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
        texte = (texte or "")[:500]  # P4-FIX : tronque les entrées anormalement longues
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
    def gettendancesactives(limit: int = 50) -> list:
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

    # ------------------------------------------------------------------ #
    # ALERTES                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def ouvrirealerte(texte: str, date: str) -> None:
        """
        DB41-FIX6 — Remplace INSERT OR IGNORE par un vrai UPSERT sur texte.
        La table alertes n'a pas de UNIQUE sur texte, donc INSERT OR IGNORE
        n'avait aucun effet protecteur → chaque appel créait un doublon.
        Solution : vérifier l'existence d'une alerte active identique avant insert.
        """
        texte = (texte or "")[:500]  # P4-FIX
        if not texte.strip():
            return
        with getdb() as db:
            # Vérifie si une alerte active identique existe déjà
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
        escaped = texte[:40].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
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
    def getalertesactives() -> list:
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
    # ACTEURS — BUG-DB4 FIX                                               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def saveacteur(nom: str, pays: str, score: float, date: str) -> None:
        nom  = (nom  or "")[:200]  # P4-FIX
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
    def getacteurs(limit: int = 20) -> list:
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
    def getmetrics(ndays: int = 30) -> list:
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, indice, alerte, nb_articles, nb_pertinents,
                       geousa, geoeurope, geoasie, geomo, georussie,
                       terrestre, maritime, transverse, contractuel
                FROM metrics
                WHERE date >= date('now', ?)
                ORDER BY date DESC
                """,
                (f"-{ndays} days",),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # MAINTENANCE — PERF-DB2 FIX + DB41-FIX1                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def maintenance(force: bool = False) -> None:
        """
        VACUUM + ANALYZE mensuels. Appeler le 1er du mois dans sentinel_main.py.

        DB41-FIX1 — closedb() appelé AVANT _create_connection() pour éviter
        deux connexions simultanées sur le même fichier pendant VACUUM.
        VACUUM requiert autocommit exclusif : une transaction ouverte dans
        la connexion thread-locale provoquerait un conflit WAL.
        """
        # Fermer proprement la connexion thread-locale avant d'ouvrir une
        # connexion dédiée au VACUUM (qui requiert autocommit exclusif)
        closedb()  # DB41-FIX1

        conn = _create_connection()
        try:
            version   = conn.execute("PRAGMA user_version").fetchone()[0]
            now_month = int(datetime.now().strftime("%Y%m"))
            if not force and version >= now_month:
                return

            log.info("MAINTENANCE : WAL checkpoint + VACUUM + ANALYZE...")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()

            old_isolation      = conn.isolation_level
            conn.isolation_level = None   # VACUUM requiert autocommit
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
            # Ne pas remettre dans thread_local : laisser se recréer proprement
            # au prochain accès via _get_thread_conn()

    # ------------------------------------------------------------------ #
    # ALIASES snake_case — compatibilité multi-scripts (BUG-3 à BUG-6)   #
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_metrics_ndays(n: int = 1) -> list:
        """Alias de getmetrics() — mailer.py / report_builder.py (BUG-3)."""
        return SentinelDB.getmetrics(ndays=n)

    @staticmethod
    def save_metrics(date: str, **kwargs) -> None:
        """Alias de savemetrics() — sentinel_api.py (BUG-4)."""
        return SentinelDB.savemetrics(date, **kwargs)

    @staticmethod
    def save_seen(seen: set, source: str = "") -> None:
        """Alias de saveseen() — samgov_scraper.py (BUG-5)."""
        return SentinelDB.saveseen(seen, source)

    @staticmethod
    def purgeseen_older_than_days(days: int = 90) -> int:
        """Alias de purgeseeolderthandays() — scraper_rss.py (BUG-6)."""
        return SentinelDB.purgeseeolderthandays(days)

# ---------------------------------------------------------------------------
# HEALTHCHECK
# ---------------------------------------------------------------------------

def check_integrity() -> bool:
    try:
        with getdb() as db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            wal       = db.execute("PRAGMA journal_mode").fetchone()[0]
            ok        = (integrity == "ok" and wal == "wal")
            log.info(f"HEALTHCHECK DB : {'OK' if ok else 'FAIL'} "
                     f"(integrity={integrity}, wal={wal})")
            return ok
    except Exception as exc:
        log.error(f"HEALTHCHECK DB exception : {exc}", exc_info=True)
        return False

# ---------------------------------------------------------------------------
# INIT & MIGRATION
# ---------------------------------------------------------------------------

def initdb() -> None:
    """Point d'entrée appelé par sentinel_main.py au démarrage."""
    DBPATH.parent.mkdir(parents=True, exist_ok=True)
    with getdb():
        pass
    if not MIGRATION_FLAG.exists() and (SEENJSON.exists() or MEMJSON.exists()):
        runmigration()


def runmigration() -> None:
    """
    Migre JSON → SQLite (v3.37 → v3.40).
    Les fichiers JSON sont archivés en .json.migrated après migration.

    DB41-FIX2 — rename() → replace() : évite FileExistsError sur Windows
    si le fichier .json.migrated existe déjà (re-migration accidentelle).
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
            SEENJSON.replace(SEENJSON.with_suffix(".json.migrated"))  # DB41-FIX2
            log.info(f"MIGRATION seenhashes : {len(hashes)} hash(es) migrés")
        except Exception as exc:
            log.error(f"MIGRATION seenhashes échec : {exc}")

    if MEMJSON.exists():
        try:
            memory = json.loads(MEMJSON.read_text(encoding="utf-8"))
            today  = datetime.now().strftime("%Y-%m-%d")
            for report in (memory.get("compressedreports") or memory.get("compressed_reports", [])):
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
            MEMJSON.replace(MEMJSON.with_suffix(".json.migrated"))  # DB41-FIX2
            log.info("MIGRATION mémoire terminée")
        except Exception as exc:
            log.error(f"MIGRATION mémoire échec : {exc}")

    MIGRATION_FLAG.write_text("v3.41", encoding="utf-8")
    log.info("MIGRATION terminée — flag écrit")
    closedb()

# ---------------------------------------------------------------------------
# BACKUP AUTOMATIQUE SQLite (F2/G3)
# ---------------------------------------------------------------------------
# DB41-FIX4 : shutil et time importés en tête de fichier (n'étaient pas ici)

def backup_db(dest_dir: str = "backups", keep_days: int = 30) -> bool:
    """
    Sauvegarde atomique de sentinel.db vers dest_dir/sentinel_YYYYMMDD.db.
    F2-FIX : protège contre la perte totale de l'historique sur corruption disque.
    Garde les N derniers jours de backup (purge automatique).

    Usage cron  : 0 7 * * * python -c "from db_manager import backup_db; backup_db()"
    Usage cloud : après backup_db(), utiliser rclone copy backups/ remote:bucket
    """
    try:
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)

        if not DBPATH.exists():
            log.warning("BACKUP sentinel.db absent — rien à sauvegarder")
            return False

        date_str    = datetime.now().strftime("%Y%m%d")
        backup_path = dest / f"sentinel_{date_str}.db"

        # WAL checkpoint avant backup pour garantir la cohérence
        with getdb() as db:
            db.execute("PRAGMA wal_checkpoint(FULL)")

        shutil.copy2(str(DBPATH), str(backup_path))
        size_kb = backup_path.stat().st_size // 1024
        log.info(f"BACKUP OK → {backup_path} ({size_kb} Ko)")

        # Purge des backups > keep_days jours
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
# SMOKE TESTS
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["SENTINEL_DB"] = f"{tmpdir}/test.db"
        _DB_INITIALIZED = False
        DBPATH          = Path(f"{tmpdir}/test.db")

        initdb()
        today = datetime.now().strftime("%Y-%m-%d")

        # BUG-DB1 : double insert même date → UPSERT
        SentinelDB.savereport(today, 7.2, "ORANGE", "Rapport test",   "...brut")
        SentinelDB.savereport(today, 8.0, "ROUGE",  "Rapport MAJ",    "...brut MAJ")
        reports = SentinelDB.getrecentreports(ndays=7)
        assert len(reports) == 1,                    "BUG-DB1 : plus d'1 rapport"
        assert reports[0]["alerte"] == "ROUGE",      "BUG-DB1 : upsert échoué"
        assert reports[0]["rawtail"] == "...brut MAJ", "BUG-DB8 : rawtail incorrecte"

        # BUG-DB2 : purge syntaxe valide
        SentinelDB.saveseen({"h1", "h2"}, source="Test")
        assert isinstance(SentinelDB.purgeseeolderthandays(90), int), "BUG-DB2 KO"

        # DB41-FIX3 : loadseen avec filtre temporel
        seen = SentinelDB.loadseen(days=90)
        assert "h1" in seen, "DB41-FIX3 : loadseen filtre KO"

        # BUG-DB4 : acteurs CRUD
        SentinelDB.saveacteur("Milrem Robotics", "EST", 8.5, today)
        SentinelDB.saveacteur("Milrem Robotics", None,  9.0, today)
        acteurs = SentinelDB.getacteurs()
        assert acteurs[0]["scoreactivite"] == 9.0, "BUG-DB4 : update score KO"
        assert acteurs[0]["pays"] == "EST",        "BUG-DB4 : COALESCE pays KO"

        # BUG-DB5 : whitelist metrics
        SentinelDB.savemetrics(today, indice=7.2, alerte="ORANGE", colonneInvalide=99)
        metrics = SentinelDB.getmetrics(ndays=7)
        assert len(metrics) == 1, "EXTRA-1 : getmetrics KO"

        # BUG-DB7 : wildcards échappés
        SentinelDB.ouvrirealerte("Déploiement KARGU 100% confirmé", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel",        today)
        SentinelDB.closealerte("KARGU 100%", today)
        alertes = SentinelDB.getalertesactives()
        assert len(alertes) == 1,               "BUG-DB7 : mauvais nb d'alertes fermées"
        assert "Type_X" in alertes[0]["texte"], "BUG-DB7 : mauvaise alerte fermée"

        # DB41-FIX6 : pas de doublon sur ouvrirealerte()
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        alertes2 = SentinelDB.getalertesactives()
        assert len(alertes2) == 1, "DB41-FIX6 : doublon alerte détecté"

        # PERF-DB1 : closedb thread-local
        closedb()
        assert getattr(_thread_local, "conn", None) is None, "PERF-DB1 : leak connexion"

        assert check_integrity(), "HEALTHCHECK FAIL"

        print("
" + "=" * 55)
        print("✅ Tous les smoke tests v3.41 passent.")
        print(f"   Rapports       : {len(reports)}")
        print(f"   Acteurs        : {len(acteurs)}")
        print(f"   Métriques      : {len(metrics)}")
        print(f"   Alertes actives: {len(alertes2)}")
        print(f"   Seenhashes     : {len(seen)}")
        print("=" * 55)

# ---------------------------------------------------------------------------
# ALIASES module-level — compatibilité snake_case (BUG-1 à BUG-9)
# ---------------------------------------------------------------------------
get_db         = getdb          # scraper_rss, nlp_scorer, ops_patents, telegram_scraper
init_db        = initdb         # health_check
run_migration  = runmigration   # health_check
