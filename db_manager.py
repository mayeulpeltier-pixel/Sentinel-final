# db_manager.py --- SENTINEL v3.50 --- Gestionnaire SQLite WAL
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
#            Ajoute la colonne aux DBs existantes sans recréer le schéma.
#            Le try/except absorbe l'erreur "duplicate column" sur les DBs
#            déjà à jour — comportement attendu et documenté.
# DB-48-SEL  getmetrics() : COALESCE(github_days_back, 0) dans SELECT
#            COALESCE garantit 0 même sur les lignes antérieures à v3.48
#            qui ont NULL en base (colonne DEFAULT 0 non rétroactive sur
#            les lignes existantes).
# DB-48-RPT  get_last_n_reports(n) : méthode pour run_monthly_report()
#            Retourne les N derniers rapports (date + rawtail) triés DESC.
#            Distinct de getrecentreports() qui filtre par fenêtre temporelle.
#
# MODIFICATIONS v3.49 — Export CSV analytiques :
# CSV-1  export_csv() dans SentinelDB — séparation de responsabilités.
#        L'export des données brutes appartient à db_manager.py (source
#        de vérité), PAS à charts.py (génération d'images).
# CSV-2  Trois exports : metrics (ndays), acteurs (top N), tendances actives.
# CSV-3  Opt-in via SENTINEL_EXPORT_CSV=1 dans .env — zéro overhead si inactif.
#        Appelé depuis sentinel_main.py après generate_all_charts() :
#            if os.environ.get("SENTINEL_EXPORT_CSV","0") == "1":
#                csv_exports = _timed("csv_export", SentinelDB.export_csv)
# CSV-4  Retourne dict[str, Path] → mailer.py joint les CSV au mail
#        avec la même mécanique que les PNG/PDF.
# CSV-5  Poids garanti minimal : texte pur, typiquement < 50 Ko pour 30j.
# CSV-6  Nommage daté YYYY-MM-DD_sentinel_{nom}.csv — cohérent avec
#        la convention de charts.py (CHK-8) : YYYY-MM-DD_{name}.png.
# CSV-7  Fallback gracieux par export : une erreur sur un dataset n'annule
#        pas les autres. Une erreur sur un format n'annule pas l'autre.
# CSV-8  Bilan consolidé : taille totale et nb fichiers loggés en fin.
#
# MODIFICATIONS v3.50 — Double format standard / Excel FR :
# CSV-9  Double export par dataset : standard (dataviz) + excel_fr (Excel FR).
#        Deux configs déclaratives → une seule boucle d'écriture par dataset.
#        Nommage : YYYY-MM-DD_sentinel_{nom}.csv        (standard)
#                  YYYY-MM-DD_sentinel_{nom}_excel.csv  (Excel FR)
# CSV-10 _fr_row() : float → str avec virgule décimale pour Excel FR.
#        Appliqué uniquement au format excel_fr — le format standard
#        conserve le point décimal natif Python (pandas, numpy, etc.).
# CSV-11 utf-8-sig (BOM) pour excel_fr uniquement → Excel détecte
#        automatiquement l'encodage sans assistant d'importation.
#        Le format standard reste utf-8 pur (BOM parasite sous Linux/pandas).
# CSV-12 SENTINEL_CSV_MODE=[both|standard|excel] dans .env
#        Défaut : both → les deux formats générés à chaque cycle.
#        "standard" → dataviz uniquement (CI, serveurs sans Excel)
#        "excel"    → excel_fr uniquement (poste analyste standalone)
#        Chargement SQL : 1 seule requête par dataset, partagée entre
#        les deux formats — zéro surcharge DB.
# =============================================================================
# Compatibilité pipeline v3.37 totalement préservée (API SentinelDB inchangée)
# =============================================================================

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

log = logging.getLogger("dbmanager")

# ---------------------------------------------------------------------------
# CHEMINS & CONSTANTES
# ---------------------------------------------------------------------------
DBPATH         = Path(os.environ.get("SENTINEL_DB", "data/sentinel.db"))
SEENJSON       = Path("data/seenhashes.json")
MEMJSON        = Path("data/sentinel_memory.json")
MIGRATION_FLAG = Path("data/.migrationdonev3.37")

_DB_INITIALIZED = False
_DB_INIT_LOCK   = threading.Lock()
_thread_local   = threading.local()

# DB-48-COL — whitelist savemetrics()
_ALLOWED_METRIC_COLS = frozenset({
    "indice", "alerte", "nb_articles", "nb_pertinents",
    "geousa", "geoeurope", "geoasie", "geomo", "georussie",
    "terrestre", "maritime", "transverse", "contractuel",
    "github_days_back",
})

# CSV-3 — configuration export (partagée avec sentinel_main.py via .env)
# SENTINEL_EXPORT_CSV=1       active l'export (défaut : désactivé)
# SENTINEL_CSV_DAYS=30        fenêtre temporelle metrics (défaut : 30j)
# SENTINEL_CSV_ACTEURS_MAX=50 top N acteurs exportés (défaut : 50)
# SENTINEL_CSV_MODE=both      both | standard | excel (défaut : both)
_CSV_DAYS        = int(os.environ.get("SENTINEL_CSV_DAYS",        "30"))
_CSV_ACTEURS_MAX = int(os.environ.get("SENTINEL_CSV_ACTEURS_MAX", "50"))
_CSV_MODE        = os.environ.get("SENTINEL_CSV_MODE", "both")  # CSV-12

# CSV-9 — configs déclaratives des formats d'export
# Ajouter un format ici suffit, export_csv() s'adapte automatiquement.
_CSV_EXPORT_CONFIGS = [
    {
        "key_suffix":  "",          # clé retournée : "metrics", "acteurs"...
        "file_suffix": "",          # suffixe fichier : sentinel_metrics.csv
        "delimiter":   ",",
        "decimal_sep": ".",         # float natif Python — pandas/Grafana/numpy
        "encoding":    "utf-8",     # pas de BOM — compatible Linux/macOS/pandas
        "modes":       {"both", "standard"},
        "label":       "standard",
    },
    {
        "key_suffix":  "_excel",
        "file_suffix": "_excel",
        "delimiter":   ";",
        "decimal_sep": ",",         # virgule décimale → nombres calculables Excel FR
        "encoding":    "utf-8-sig", # BOM → Excel détecte UTF-8 sans assistant
        "modes":       {"both", "excel"},
        "label":       "excel_fr",
    },
]

# ---------------------------------------------------------------------------
# DDL v3.48 — schéma complet 6 tables (inchangé v3.50)
# ---------------------------------------------------------------------------
_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA mmap_size = 67108864;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS reports (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       TEXT NOT NULL UNIQUE,
    indice     REAL DEFAULT 5.0,
    alerte     TEXT DEFAULT 'VERT',
    compressed TEXT,
    rawtail    TEXT
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
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL UNIQUE,
    indice           REAL,
    alerte           TEXT,
    nb_articles      INTEGER,
    nb_pertinents    INTEGER,
    geousa           REAL,
    geoeurope        REAL,
    geoasie          REAL,
    geomo            REAL,
    georussie        REAL,
    terrestre        REAL,
    maritime         REAL,
    transverse       REAL,
    contractuel      REAL,
    github_days_back INTEGER DEFAULT 0
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
                log.info("DB schéma initialisé (v3.50)")
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
        log.error(f"DB rollback : {exc}", exc_info=True)
        raise

# ---------------------------------------------------------------------------
# HELPERS CSV — module-level (non méthodes de classe)
# ---------------------------------------------------------------------------

def _fr_row(row: dict, decimal_sep: str) -> dict:
    """
    [CSV-10] Normalise les valeurs numériques pour un format d'export donné.
    decimal_sep="." → identité (standard, pandas/numpy).
    decimal_sep="," → float Python écrits avec virgule (Excel FR).
    Seuls les float sont transformés — int, str, None passent tels quels.
    """
    if decimal_sep == ".":
        return row  # format standard : aucune transformation
    return {
        k: str(v).replace(".", decimal_sep) if isinstance(v, float) else v
        for k, v in row.items()
    }

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
    def get_last_n_reports(n: int = 30) -> list:
        """
        [DB-48-RPT] Retourne les N derniers rapports triés par date DESC.

        Distinct de getrecentreports() :
            getrecentreports() → filtre par fenêtre temporelle (WHERE date >= ?)
            get_last_n_reports() → filtre par nombre exact (LIMIT ?)

        Utilisé par run_monthly_report() dans sentinel_main.py :
            rows = SentinelDB.get_last_n_reports(30)
        Le mensuel veut exactement 30 entrées indépendamment des gaps
        calendaires (jours fériés, pannes) — un filtre temporel sur 30j
        pourrait retourner moins de 30 rapports si des cycles ont été sautés.
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
        """Rétrocompatibilité memory_manager.py (DEPRECATED — utiliser SentinelDB)."""
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
    def loadseen(days: int = 90) -> set:
        """
        DB41-FIX3 — Charge uniquement les hashes des N derniers jours.
        Évite de charger toute la table en RAM après plusieurs mois.
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
        """DB41-FIX6 — UPSERT sur texte : pas de doublon d'alertes actives."""
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
        """
        [DB-48-SEL] COALESCE(github_days_back, 0) garantit 0 sur les lignes
        antérieures à v3.48 (NULL en base, DEFAULT 0 non rétroactif sur
        les lignes existantes).
        """
        with getdb() as db:
            rows = db.execute(
                """
                SELECT date, indice, alerte, nb_articles, nb_pertinents,
                       geousa, geoeurope, geoasie, geomo, georussie,
                       terrestre, maritime, transverse, contractuel,
                       COALESCE(github_days_back, 0) AS github_days_back
                FROM metrics
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
        output_dir:    Path = Path("output"),
    ) -> dict:
        """
        [CSV-1] Export des données brutes en CSV légers pour les analystes.

        Séparation de responsabilités (CSV-1) :
            L'export appartient à db_manager.py (source de vérité).
            charts.py consomme les données — il ne les possède pas et
            ne doit pas les exporter. Toute modification du schéma
            SQLite se répercute automatiquement dans l'export sans
            toucher charts.py.

        Opt-in (CSV-3) : appelé depuis sentinel_main.py uniquement si
            SENTINEL_EXPORT_CSV=1 dans .env — zéro overhead sinon.
            Bloc à insérer dans sentinel_main.py après generate_all_charts() :

                csv_exports: dict = {}
                if os.environ.get("SENTINEL_EXPORT_CSV", "0") == "1":
                    try:
                        csv_exports = _timed("csv_export", SentinelDB.export_csv)
                        logger.info(f"CSV exportés : {list(csv_exports.keys())}")
                    except Exception as e:
                        logger.warning(f"Export CSV non bloquant : {e}")

        Génère jusqu'à 6 fichiers selon SENTINEL_CSV_MODE (CSV-12) :
            YYYY-MM-DD_sentinel_metrics.csv          → pandas, Grafana, Superset
            YYYY-MM-DD_sentinel_metrics_excel.csv    → Excel FR (double-clic)
            YYYY-MM-DD_sentinel_acteurs.csv
            YYYY-MM-DD_sentinel_acteurs_excel.csv
            YYYY-MM-DD_sentinel_tendances.csv
            YYYY-MM-DD_sentinel_tendances_excel.csv

        Différences standard vs excel_fr (CSV-9 à CSV-11) :
            ┌──────────────┬─────────────┬──────────────┐
            │              │  standard   │  excel_fr    │
            ├──────────────┼─────────────┼──────────────┤
            │ délimiteur   │     ,       │     ;        │
            │ décimale     │    7.2      │    7,2       │
            │ encodage     │   utf-8     │  utf-8-sig   │
            │ BOM          │    non      │    oui       │
            └──────────────┴─────────────┴──────────────┘

        Chargement SQL (CSV-12) : 1 seule requête par dataset, partagée
            entre les deux formats — zéro surcharge DB.

        Retourne dict[str, Path] (CSV-4) :
            exports["metrics"]          → Path standard
            exports["metrics_excel"]    → Path excel_fr
            ... etc.

        Fallback par export (CSV-7) : une erreur sur un dataset n'annule
            pas les autres. Une erreur sur un format n'annule pas l'autre.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        today   = datetime.now().strftime("%Y-%m-%d")
        exports: dict = {}

        # Configs actives selon SENTINEL_CSV_MODE
        active_configs = [
            cfg for cfg in _CSV_EXPORT_CONFIGS
            if _CSV_MODE in cfg["modes"]
        ]
        if not active_configs:
            log.warning(
                f"CSV export : SENTINEL_CSV_MODE='{_CSV_MODE}' invalide "
                f"(valeurs acceptées : both, standard, excel) — export annulé"
            )
            return exports

        # ── Datasets à exporter ───────────────────────────────────────────
        datasets = [
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

            # Chargement unique — partagé entre les deux formats (CSV-12)
            try:
                rows = dataset["loader"]()
            except Exception as e:
                log.error(f"CSV {name} chargement échec : {e}", exc_info=True)
                continue  # CSV-7 : on passe au dataset suivant

            if not rows:
                log.warning(f"CSV {name} : {dataset['empty_warning']}")
                continue

            fieldnames = list(rows[0].keys())

            # Écriture dans chaque format actif
            for cfg in active_configs:
                key  = f"{name}{cfg['key_suffix']}"
                path = output_dir / f"{today}_sentinel_{name}{cfg['file_suffix']}.csv"

                try:
                    with open(path, "w", newline="", encoding=cfg["encoding"]) as f:
                        writer = csv.DictWriter(
                            f,
                            fieldnames = fieldnames,
                            delimiter  = cfg["delimiter"],
                        )
                        writer.writeheader()
                        writer.writerows(
                            _fr_row(r, cfg["decimal_sep"]) for r in rows
                        )

                    size_kb = path.stat().st_size // 1024
                    exports[key] = path
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
                    # CSV-7 : l'autre format du même dataset continue

        # ── Bilan consolidé (CSV-8) ───────────────────────────────────────
        if exports:
            total_kb = sum(p.stat().st_size for p in exports.values()) // 1024
            log.info(
                f"CSV export terminé ({_CSV_MODE}) : "
                f"{len(exports)} fichiers, "
                f"{total_rows_written} lignes écrites, "
                f"{total_kb} Ko total → {output_dir}/"
            )
        else:
            log.warning(
                "CSV export : aucun fichier généré "
                "(base vide ? lancer quelques cycles de collecte)"
            )

        return exports

    # ------------------------------------------------------------------ #
    # MAINTENANCE — PERF-DB2 FIX + DB41-FIX1                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def maintenance(force: bool = False) -> None:
        """
        VACUUM + ANALYZE mensuels.
        DB41-FIX1 : closedb() avant _create_connection() — conflict WAL évité.
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

    # ------------------------------------------------------------------ #
    # ALIASES snake_case — compatibilité multi-scripts                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_metrics_ndays(n: int = 1) -> list:
        """Alias de getmetrics() — mailer.py / report_builder.py."""
        return SentinelDB.getmetrics(ndays=n)

    @staticmethod
    def save_metrics(date: str, **kwargs) -> None:
        """Alias de savemetrics() — sentinel_api.py."""
        return SentinelDB.savemetrics(date, **kwargs)

    @staticmethod
    def save_seen(seen: set, source: str = "") -> None:
        """Alias de saveseen() — samgov_scraper.py."""
        return SentinelDB.saveseen(seen, source)

    @staticmethod
    def purgeseen_older_than_days(days: int = 90) -> int:
        """Alias de purgeseeolderthandays() — scraper_rss.py."""
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
    """
    Point d'entrée appelé par sentinel_main.py au démarrage.

    [DB-48-MIG] Migration safe de la colonne github_days_back :
        ALTER TABLE est idempotent grâce au try/except — SQLite lève
        une OperationalError "duplicate column name" si la colonne existe
        déjà. Ce comportement est attendu et silencieux après le premier
        run v3.48. L'approche ALTER TABLE (et non DROP/CREATE) préserve
        toutes les données existantes.
    """
    DBPATH.parent.mkdir(parents=True, exist_ok=True)
    with getdb():
        pass

    # DB-48-MIG — ajout colonne github_days_back aux DBs existantes
    try:
        with getdb() as db:
            db.execute(
                "ALTER TABLE metrics ADD COLUMN github_days_back INTEGER DEFAULT 0"
            )
        log.info("DB-48-MIG : colonne github_days_back ajoutée à metrics")
    except Exception:
        pass  # Colonne déjà présente (DBs v3.48+) — comportement normal

    if not MIGRATION_FLAG.exists() and (SEENJSON.exists() or MEMJSON.exists()):
        runmigration()


def runmigration() -> None:
    """
    Migre JSON → SQLite (v3.37 → v3.40).
    DB41-FIX2 — rename() → replace() : FileExistsError Windows évitée.
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
            MEMJSON.replace(MEMJSON.with_suffix(".json.migrated"))
            log.info("MIGRATION mémoire terminée")
        except Exception as exc:
            log.error(f"MIGRATION mémoire échec : {exc}")

    MIGRATION_FLAG.write_text("v3.41", encoding="utf-8")
    log.info("MIGRATION terminée — flag écrit")
    closedb()

# ---------------------------------------------------------------------------
# BACKUP AUTOMATIQUE SQLite
# ---------------------------------------------------------------------------

def backup_db(dest_dir: str = "backups", keep_days: int = 30) -> bool:
    """
    Sauvegarde atomique sentinel.db → dest_dir/sentinel_YYYYMMDD.db.
    Purge automatique des backups > keep_days jours.
    """
    try:
        dest = Path(dest_dir)
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
# SMOKE TESTS v3.50
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
        SentinelDB.savereport(today, 7.2, "ORANGE", "Rapport test",  "...brut")
        SentinelDB.savereport(today, 8.0, "ROUGE",  "Rapport MAJ",   "...brut MAJ")
        reports = SentinelDB.getrecentreports(ndays=7)
        assert len(reports) == 1,                      "BUG-DB1 : plus d'1 rapport"
        assert reports[0]["alerte"] == "ROUGE",        "BUG-DB1 : upsert échoué"
        assert reports[0]["rawtail"] == "...brut MAJ", "BUG-DB8 : rawtail incorrecte"

        # DB-48-RPT : get_last_n_reports()
        last_n = SentinelDB.get_last_n_reports(5)
        assert len(last_n) == 1,       "DB-48-RPT : get_last_n_reports KO"
        assert "rawtail" in last_n[0], "DB-48-RPT : rawtail absente"
        assert "date"    in last_n[0], "DB-48-RPT : date absente"

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

        # BUG-DB5 : whitelist metrics — colonne invalide ignorée
        SentinelDB.savemetrics(today, indice=7.2, alerte="ORANGE", colonneInvalide=99)
        metrics = SentinelDB.getmetrics(ndays=7)
        assert len(metrics) == 1, "EXTRA-1 : getmetrics KO"

        # DB-48-COL / DB-48-SEL : github_days_back persisté et relu
        SentinelDB.savemetrics(today, github_days_back=10)
        metrics2 = SentinelDB.getmetrics(ndays=7)
        assert metrics2[0]["github_days_back"] == 10, \
            "DB-48-COL/SEL : github_days_back KO"

        # DB-48-SEL : COALESCE → 0 si NULL
        SentinelDB.savemetrics("2000-01-01", indice=5.0, alerte="VERT")
        old_rows = [r for r in SentinelDB.getmetrics(ndays=365 * 30)
                    if r["date"] == "2000-01-01"]
        assert old_rows[0]["github_days_back"] == 0, \
            "DB-48-SEL : COALESCE NULL → 0 KO"

        # BUG-DB7 : wildcards échappés
        SentinelDB.ouvrirealerte("Déploiement KARGU 100% confirmé", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel",        today)
        SentinelDB.closealerte("KARGU 100%", today)
        alertes = SentinelDB.getalertesactives()
        assert len(alertes) == 1,               "BUG-DB7 : mauvais nb d'alertes"
        assert "Type_X" in alertes[0]["texte"], "BUG-DB7 : mauvaise alerte fermée"

        # DB41-FIX6 : pas de doublon sur ouvrirealerte()
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        SentinelDB.ouvrirealerte("Type_X Korea opérationnel", today)
        alertes2 = SentinelDB.getalertesactives()
        assert len(alertes2) == 1, "DB41-FIX6 : doublon alerte détecté"

        # CSV-9 à CSV-12 : double format standard + excel_fr
        SentinelDB.savetendance("Essaims FPV Ukraine", today)
        SentinelDB.savetendance("UGV NATO interopérabilité", today)
        csv_out = Path(tmpdir) / "csv_test"

        # Test mode "both" (défaut)
        os.environ["SENTINEL_CSV_MODE"] = "both"
        exports_both = SentinelDB.export_csv(
            ndays=30, limit_acteurs=10, output_dir=csv_out
        )
        assert "metrics"          in exports_both, "CSV-9 : metrics standard absent"
        assert "metrics_excel"    in exports_both, "CSV-9 : metrics_excel absent"
        assert "acteurs"          in exports_both, "CSV-9 : acteurs standard absent"
        assert "acteurs_excel"    in exports_both, "CSV-9 : acteurs_excel absent"
        assert "tendances"        in exports_both, "CSV-9 : tendances standard absent"
        assert "tendances_excel"  in exports_both, "CSV-9 : tendances_excel absent"

        # Vérification décimales : standard → "." / excel → ","
        with open(exports_both["metrics"],       encoding="utf-8")     as f:
            std_content = f.read()
        with open(exports_both["metrics_excel"], encoding="utf-8-sig") as f:
            xl_content  = f.read()
        assert "7.2" in std_content, "CSV-10 : décimale standard KO"
        assert "7,2" in xl_content,  "CSV-10 : décimale excel_fr KO"

        # Vérification BOM utf-8-sig (CSV-11)
        with open(exports_both["metrics_excel"], "rb") as f:
            bom = f.read(3)
        assert bom == b"ï»¿", "CSV-11 : BOM utf-8-sig absent"

        # Vérification nommage daté (CSV-6)
        assert exports_both["metrics"].name.startswith(today),       "CSV-6 : nommage standard KO"
        assert exports_both["metrics_excel"].name.startswith(today), "CSV-6 : nommage excel KO"

        # Test mode "standard" — uniquement les fichiers sans _excel
        os.environ["SENTINEL_CSV_MODE"] = "standard"
        exports_std = SentinelDB.export_csv(
            ndays=30, limit_acteurs=10, output_dir=csv_out
        )
        assert "metrics"       in exports_std,     "CSV-12 : standard mode KO"
        assert "metrics_excel" not in exports_std, "CSV-12 : excel généré en mode standard"

        # Test mode "excel" — uniquement les fichiers _excel
        os.environ["SENTINEL_CSV_MODE"] = "excel"
        exports_xl = SentinelDB.export_csv(
            ndays=30, limit_acteurs=10, output_dir=csv_out
        )
        assert "metrics_excel" in exports_xl,  "CSV-12 : excel mode KO"
        assert "metrics"       not in exports_xl, "CSV-12 : standard généré en mode excel"

        # PERF-DB1 : closedb thread-local
        closedb()
        assert getattr(_thread_local, "conn", None) is None, "PERF-DB1 : leak connexion"

        assert check_integrity(), "HEALTHCHECK FAIL"

        print("
" + "=" * 60)
        print("✅ Tous les smoke tests v3.50 passent.")
        print(f"   Rapports          : {len(reports)}")
        print(f"   get_last_n(5)     : {len(last_n)}")
        print(f"   Acteurs           : {len(acteurs)}")
        print(f"   Métriques         : {len(metrics)}")
        print(f"   github_days_back  : {metrics2[0]['github_days_back']}")
        print(f"   Alertes actives   : {len(alertes2)}")
        print(f"   Seenhashes        : {len(seen)}")
        print(f"   CSV mode=both     : {list(exports_both.keys())}")
        print(f"   CSV mode=standard : {list(exports_std.keys())}")
        print(f"   CSV mode=excel    : {list(exports_xl.keys())}")
        csv_total_kb = sum(p.stat().st_size for p in exports_both.values()) // 1024
        print(f"   CSV poids total   : {csv_total_kb} Ko")
        print("=" * 60)

# ---------------------------------------------------------------------------
# ALIASES module-level — compatibilité snake_case
# ---------------------------------------------------------------------------
get_db        = getdb
init_db       = initdb
run_migration = runmigration
