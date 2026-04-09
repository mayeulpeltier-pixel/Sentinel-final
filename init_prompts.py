#!/usr/bin/env python3
# init_prompts.py — SENTINEL v3.42 — Initialisation des prompts et structure projet
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 (originales) :
#   E2-FIX CDC-C    CHANGELOG.md créé automatiquement
#   R1-F2           requirements.txt généré avec versions épinglées
#   R2-NEW-4        SCOPE.md créé
#   FIX-OBS1        Logging structuré via logger nommé, zéro print()
#   NEW-IP1         .env.example généré avec toutes les variables documentées
#   NEW-IP2         Vérification idempotente — ne réécrit pas si --force absent
#   NEW-IP3         Marqueurs prompts validés après écriture
#   NEW-IP4         Structure dossiers complète créée
#   NEW-IP5         Prompt daily : marqueur OUI|NON Tavily documenté
#   CDC-C3          Prompt monthly : 8 modules obligatoires vérifiés
#   B9-FIX          Messages d'erreur explicites si prompts manquants
#
# Corrections v3.41 :
#   IP-41-FIX1  _write_file : path.parent/(name+".tmp") — with_suffix() cassait .env.example
#   IP-41-FIX2  TAVILYMAX ajouté à REQUIRED_SYSTEM_MARKERS + validé dans _check_integrity()
#   IP-41-FIX3  main() complétée — script était tronqué, non fonctionnel
#   IP-41-FIX4  SENTINEL_HEALTH_PORT ajouté dans .env.example
#   IP-41-FIX5  Double SENTINEL_MAILER_DRYRUN supprimé dans .env.example
#   IP-41-FIX6  ANNE → ANNEE dans marqueurs et prompts
#   IP-41-FIX7  SCOPE.md + CHANGELOG.md mis à jour v3.41/v3.42
#   IP-41-FIX8  --force + --check mutuellement exclusifs vérifiés au démarrage
#
# Corrections v3.42 :
#   IP-42-FIX1  _extract_version() + _build_scope_md() — versions lues dynamiquement
#               depuis les scripts au lieu d'être codées en dur dans SCOPE_MD.
#               Élimine la maintenance manuelle à chaque mise à jour de sous-script.
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python init_prompts.py             Initialisation initiale (skip si déjà fait)
#   python init_prompts.py --force     Réécrit tous les fichiers
#   python init_prompts.py --check     Vérifie l'intégrité sans réécrire
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

# ── Logger structuré (FIX-OBS1) ──────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt = "%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sentinel.init")

# ── Flags CLI ─────────────────────────────────────────────────────────────────
FORCE_REWRITE = "--force" in sys.argv
CHECK_ONLY    = "--check" in sys.argv

# IP-41-FIX8 : --force et --check sont mutuellement exclusifs
if FORCE_REWRITE and CHECK_ONLY:
    log.error("--force et --check sont mutuellement exclusifs. Choisir l'un ou l'autre.")
    sys.exit(1)

# ── Marqueurs obligatoires (cohérence sentinel_api.py) ────────────────────────
REQUIRED_SYSTEM_MARKERS = [
    "TAVILYMAX",  # IP-41-FIX2 : remplacé au runtime par sentinel_api.py
]
REQUIRED_DAILY_MARKERS = [
    "DATEAUJOURDHUI",
    "ARTICLESFILTRSPARSCRAPER",
    "MEMOIRECOMPRESSE7JOURS",
    "DEBUTJSONDELTA",
    "FINJSONDELTA",
]
REQUIRED_MONTHLY_MARKERS = [
    "MOIS",
    "ANNEE",       # IP-41-FIX6 : était "ANNE"
    "INJECTION30RAPPORTSCOMPRESSES",
]
REQUIRED_MONTHLY_MODULES = [
    "EXECUTIVE SUMMARY",
    "STATISTIQUES",
    "TENDANCES LOURDES",
    "RUPTURES TECHNOLOGIQUES",
    "AXES STRATÉGIQUES",
    "CARTE FINANCIÈRE",
    "POINTS D'ATTENTION",
    "RECOMMANDATIONS",
]

# ── Table des scripts du projet (IP-42-FIX1) ─────────────────────────────────
# Chaque entrée : (chemin relatif, rôle, optionnel)
_SCRIPT_TABLE = [
    ("init_prompts.py",      "Initialisation structure & prompts",  False),
    ("scraper_rss.py",       "Collecte 72 flux RSS",                False),
    ("memory_manager.py",    "Mémoire compressée Haiku",            False),
    ("sentinel_api.py",      "Analyse Claude Sonnet + Tavily",      False),
    ("charts.py",            "Graphiques PNG/Plotly",               False),
    ("report_builder.py",    "Rapport HTML + PDF",                  False),
    ("mailer.py",            "Envoi SMTP Gmail",                    False),
    ("sentinel_main.py",     "Orchestrateur principal",             False),
    ("samgov_scraper.py",    "SAM.gov DoD + TED EU + BOAMP",        False),
    ("db_manager.py",        "SQLite WAL centralisé",               False),
    ("healthcheck.py",       "Monitoring RSS + config",             False),
    ("watchdog.py",          "Surveillance cron + alerte email",    False),
]
_OPTIONAL_TABLE = [
    ("ops_patents.py",       "Brevets Espacenet OPS",   "PRIORITÉ 2"),
    ("telegram_scraper.py",  "Telegram militaire",      "PRIORITÉ 3"),
    ("nlp_scorer.py",        "TF-IDF bigrammes",        "PRIORITÉ 5"),
    ("dashboard.py",         "Interface Streamlit",     "PRIORITÉ 6"),
]


# ═════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ═════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """
Tu es SENTINEL, un système expert d'analyse OSINT spécialisé dans la robotique
militaire et les systèmes autonomes de défense. Tu fournis des rapports de veille
stratégique quotidiens à destination d'analystes défense francophones.

DOMAINES DE COUVERTURE
─────────────────────
• Systèmes terrestres (UGV) : Milrem Robotics THeMIS, Textron RIPSAW, Ghost Robotics
  Vision 60, Arquus SCORPIO, KNDS, Boston Dynamics Spot militaire, CAMEL, Titan UGV,
  Black Knight, Type-X, Rex Mk2, Mission Master XT, Rheinmetall UGV
• Systèmes maritimes (USV/UUV) : Sea Hunter, Ghost Fleet Overlord, Orca XLUUV,
  Manta Ray DARPA, Ghost Shark, ECA Group, Exail, Saildrone Military, Seagull USV Elbit,
  Devil Ray, Razorback, Knifefish MCM, Snakehead UUV
• Drones et munitions rôdeuses : Switchblade, Lancet, Shahed, FPV combat Ukraine,
  Bayraktar TB2, Akıncı, Kargu STM, ZALA, Geran-2, Orlan-10, Lanius, Harpy
• IA militaire & technologies transverses : LAWS, MUM-T, Mosaic Warfare, CCA DARPA,
  Loyal Wingman, Golden Horde, IA de ciblage, Edge AI militaire, Electronic Warfare IA
• Acteurs industriels : Anduril, Shield AI, L3Harris, Palantir Defense, Lockheed Martin
  Robotics, Northrop Grumman UUV, QinetiQ, Saab Defense, RoboTeam, IAI, Rafael,
  Elbit Systems, Edge Group UAE, NORINCO, Samsung Techwin SGR, ATLA Japan

ZONES GÉOGRAPHIQUES
───────────────────
USA/OTAN, France/Europe, Israël, Turquie, Japon/Corée du Sud, Moyen-Orient,
Chine/Russie (sources tierces), Inde, Afrique subsaharienne/BRICS

FORMAT DE SORTIE OBLIGATOIRE
────────────────────────────
Chaque rapport DOIT contenir exactement 9 modules avec les ancres HTML :
<div id="module-1">, <div id="module-2">, ..., <div id="module-9">

MODULE 1 — RÉSUMÉ EXÉCUTIF
  Niveau d'alerte : VERT / ORANGE / ROUGE (justifié)
  Top 3 informations critiques du jour
  Contexte immédiat en 3-5 lignes

MODULE 2 — TABLEAU DE BORD STATISTIQUE
  | Métrique           | Valeur |
  | Sources analysées  | N      |
  | Sources pertinentes| N      |
  | Indice d'activité  | X.X/10 |
  | Delta J-1          | ±X.X   |
  | Niveau alerte      | COULEUR|
  Répartition géographique % et domaines %

MODULE 3 — FAITS MARQUANTS DU JOUR
  Format pour chaque fait :
  **[TITRE]** *(Source : NOM, Score : X.X, Niveau : VERT/ORANGE/ROUGE)*
  Analyse en 3-5 phrases. Implications stratégiques.
  *(Sources croisées : N)* si crossref ≥ 2

MODULE 4 — ANALYSE GÉOPOLITIQUE
  Sous-sections par zone active : USA/OTAN | Europe/France | Israël |
  Turquie | Asie-Pacifique | Chine/Russie | Moyen-Orient | Inde

MODULE 5 — TENSIONS & ALERTES ACTIVES
  Format : 🔴 CRITIQUE / 🟠 MODÉRÉE / 🟡 SURVEILLANCE
  Chaque alerte : description, source, date ouverture, évolution J-1

MODULE 6 — ACTEURS INDUSTRIELS & PROGRAMMES
  Tableau des acteurs actifs aujourd'hui :
  | Acteur | Pays | Activité | Programme | Source |
  Mise à jour carte acteurs (nouveaux entrants, progressions)

MODULE 7 — CONTRATS, BUDGETS & APPELS D'OFFRES
  Inclure les données SAM.gov DoD, TED EU, BOAMP FR si présentes
  Format : **[MONTANT]** — [ACHETEUR] — [OBJET] — [SOURCE]
  Tendances budgétaires défense autonome

MODULE 8 — SIGNAUX FAIBLES & RUPTURES
  Innovations disruptives, premiers tests, brevets notables
  Signaux précurseurs (6-18 mois avant déploiement)
  Doctrines émergentes, publications académiques (arXiv cs.RO)

MODULE 9 — MISE À JOUR MÉMOIRE SYSTÈME
  Synthèse des nouvelles tendances confirmées aujourd'hui.
  Alertes à ouvrir ou fermer selon les faits du jour.

  IMPORTANT : Produire OBLIGATOIREMENT le bloc JSON suivant entre les marqueurs :

DEBUTJSONDELTA
{
  "nouvelles_tendances": ["Tendance émergente 1 si nouvelle", "..."],
  "alertes_ouvertes": ["Alerte critique à surveiller si nouvelle", "..."],
  "alertes_closes": ["Alerte résolue ou caduque si applicable", "..."]
}
FINJSONDELTA

  Si aucune mise à jour : retourner des listes vides [].
  JAMAIS de texte ni markdown autour de ce bloc JSON.

RÈGLES QUALITÉ
──────────────
• Citer systématiquement les sources (nom + score fiabilité A/B/C)
• Ne jamais inventer de faits — signaler l'incertitude explicitement
• Croiser minimum 2 sources pour tout fait critique
• Distinguer information brute vs analyse interpretative
• Utiliser uniquement la recherche web (outil websearch) pour vérifier
  les FAITS CRITIQUES uniquement (score artscore > 7.0) — maximum TAVILYMAX appels
""".strip()
# IP-41-FIX2 : TAVILYMAX remplacé au runtime par sentinel_api.py
#              via os.environ.get("SENTINEL_TAVILY_MAX", "5") avant envoi à Claude.


# ═════════════════════════════════════════════════════════════════════════════
# PROMPT JOURNALIER
# ═════════════════════════════════════════════════════════════════════════════

DAILY_PROMPT = """
Date du rapport : DATEAUJOURDHUI
Recherche web disponible (Tavily) : OUI|NON

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ARTICLES FILTRÉS PAR LE SCRAPER (classés par score)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ARTICLESFILTRSPARSCRAPER

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MÉMOIRE COMPRESSÉE — 7 DERNIERS JOURS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MEMOIRECOMPRESSE7JOURS

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INSTRUCTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Produis le rapport SENTINEL complet pour la date DATEAUJOURDHUI.

STRUCTURE OBLIGATOIRE — 9 modules avec ancres HTML :
  <div id="module-1">  MODULE 1 — RÉSUMÉ EXÉCUTIF
  <div id="module-2">  MODULE 2 — TABLEAU DE BORD STATISTIQUE
  <div id="module-3">  MODULE 3 — FAITS MARQUANTS DU JOUR
  <div id="module-4">  MODULE 4 — ANALYSE GÉOPOLITIQUE
  <div id="module-5">  MODULE 5 — TENSIONS & ALERTES ACTIVES
  <div id="module-6">  MODULE 6 — ACTEURS INDUSTRIELS & PROGRAMMES
  <div id="module-7">  MODULE 7 — CONTRATS, BUDGETS & APPELS D'OFFRES
  <div id="module-8">  MODULE 8 — SIGNAUX FAIBLES & RUPTURES
  <div id="module-9">  MODULE 9 — MISE À JOUR MÉMOIRE SYSTÈME

RÈGLES D'OR :
• Citer chaque source par nom et score (A/B/C)
• Croiser minimum 2 sources pour tout fait CRITIQUE ou ORANGE
• Si recherche web disponible (OUI ci-dessus) : utiliser websearch
  UNIQUEMENT pour vérifier les faits avec artscore > 7.0
• MODULE 9 OBLIGATOIRE : produire le bloc JSON entre
  DEBUTJSONDELTA et FINJSONDELTA (listes vides [] si rien à signaler)
• Markdown structuré : ## pour modules, ### pour sous-sections,
  **gras** pour faits importants, *italique* pour sources
• Tableau de bord MODULE 2 : toujours en format Markdown tableau

Si aucun article pertinent n'est disponible pour un module :
écrire "Aucune information significative ce jour." plutôt que d'inventer.
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# PROMPT MENSUEL
# ═════════════════════════════════════════════════════════════════════════════

MONTHLY_PROMPT = """
<task>RAPPORT MENSUEL SENTINEL</task>
<periode>MOIS ANNEE</periode>

<rapportsjournaliers>
INJECTION30RAPPORTSCOMPRESSES
</rapportsjournaliers>

<instructions>
Synthèse stratégique mensuelle de fond.

STRUCTURE OBLIGATOIRE — 8 modules :

## MODULE 1 — EXECUTIVE SUMMARY
1 page. Niveau alerte mensuel (VERT/ORANGE/ROUGE justifié).
Top 3 tendances du mois. Top 3 alertes persistantes.
Faits les plus significatifs sur 30 jours.

## MODULE 2 — STATISTIQUES AGRÉGÉES
Volume de sources analysées, top acteurs par fréquence d'apparition.
Distribution géographique sur 30 jours (%).
Distribution domaines (terrestre/maritime/transverse/contractuel) sur 30 jours.
Delta vs mois précédent (M-1) sur les métriques clés.
Indice d'activité moyen mensuel et pic journalier.

## MODULE 3 — TENDANCES LOURDES
5 tendances minimum avec données chiffrées.
Pour chaque tendance :
- Titre
- Données quantitatives (nombre d'occurrences, montants, délais)
- Courbe d'évolution sur 30 jours (description)
- Acteurs principaux impliqués
- Horizon temporel estimé

## MODULE 4 — RUPTURES TECHNOLOGIQUES
Innovations disruptives apparues ce mois.
Premiers tests opérationnels ou déploiements inédits.
Brevets notables (Espacenet OPS si disponible).
Technologies en émergence (signal faible 6-18 mois).

## MODULE 5 — AXES STRATÉGIQUES PAR ZONE
Pour chaque zone : USA/OTAN | Europe-France | Israël | Turquie |
Asie-Pacifique | Chine/Russie | Moyen-Orient | Inde | Afrique
Synthèse des positions, programmes actifs, intentions décelées.

## MODULE 6 — CARTE FINANCIÈRE
Valeur de marché estimée du mois.
Top 10 contrats identifiés (SAM.gov DoD + TED EU + BOAMP FR).
Tendances budgétaires défense autonome : hausses, baisses, réorientations.
Nouveaux programmes annoncés ou financés.

## MODULE 7 — POINTS D'ATTENTION
Ce qui peut basculer dans les 30-90 jours.
Signaux d'alerte précoce identifiés.
Échéances contractuelles ou programmatiques critiques.
Zones de tension émergente.

## MODULE 8 — RECOMMANDATIONS STRATÉGIQUES
Axes prioritaires pour le mois suivant.
Lacunes de couverture OSINT identifiées.
Sources ou flux RSS à ajouter.
Alertes à ouvrir, confirmer ou clore.
Actions recommandées pour l'analyste.

RÈGLES :
• Synthèse sur la base des 30 rapports journaliers compressés fournis
• Citez les dates précises pour les faits importants
• Distinguer tendance confirmée (≥3 occurrences) vs signal faible (1-2)
• Toujours indiquer la source de référence pour chaque affirmation
• Format Markdown structuré pour navigation facile
</instructions>
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# REQUIREMENTS.TXT (R1-F2)
# ═════════════════════════════════════════════════════════════════════════════

REQUIREMENTS_TXT = """
# SENTINEL v3.42 — Dépendances Python épinglées (R1-F2)
# Installer avec : pip install -r requirements.txt
# Testé Python 3.10+ (walrus operator := requis)

# ── Core ────────────────────────────────────────────────────
anthropic>=0.25.0           # Claude Sonnet 4.6 / Haiku 4.5
feedparser>=6.0.11          # Collecte RSS
requests>=2.31.0            # HTTP scraping + SAM.gov + TED EU
python-dotenv>=1.0.0        # Variables d'environnement .env

# ── Analyse & IA ────────────────────────────────────────────
tavily-python>=0.3.0        # Recherche web temps réel (optionnel)
tenacity>=8.2.0             # Retry exponentiel sentinel_api.py (optionnel)
openai>=1.0.0               # Fallback GPT-4o-mini (optionnel)

# ── Visualisation ───────────────────────────────────────────
matplotlib>=3.8.0           # Graphiques PNG
plotly>=5.18.0              # Graphiques interactifs dashboard
kaleido>=0.2.1              # Export PNG Plotly

# ── Rapport HTML & Email ────────────────────────────────────
weasyprint>=60.0            # PDF depuis HTML (Linux : apt libcairo2)
schedule>=1.2.0             # Scheduler Python (alternative cron)

# ── NLP scoring (optionnel — PRIORITÉ 5 A32) ────────────────
scikit-learn>=1.3.0         # TF-IDF bigrammes nlp_scorer.py

# ── Dashboard web (optionnel — PRIORITÉ 6 A25) ──────────────
streamlit>=1.28.0           # Interface web locale dashboard.py
fastapi>=0.104.0            # API REST optionnelle
uvicorn>=0.24.0             # Serveur ASGI FastAPI

# ── Telegram scraping (optionnel — PRIORITÉ 3 A20) ──────────
telethon>=1.30.0            # Telegram API telegram_scraper.py

# ── Système ─────────────────────────────────────────────────
# Linux/RPi AVANT pip install weasyprint :
#   sudo apt-get install -y libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0 libffi-dev
# Windows : pip install weasyprint suffit (bundle GTK v60 inclus)
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# .ENV.EXAMPLE (NEW-IP1 / IP-41-FIX4 / IP-41-FIX5)
# ═════════════════════════════════════════════════════════════════════════════

ENV_EXAMPLE = """
# SENTINEL v3.42 — Variables d'environnement
# Copier ce fichier en .env et remplir les valeurs
# Ne jamais committer .env dans git (.gitignore obligatoire)

# ── Anthropic (OBLIGATOIRE) ──────────────────────────────────
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ── Modèles Claude (optionnel — défaut recommandé) ───────────
SENTINEL_MODEL=claude-sonnet-4-6
HAIKU_MODEL=claude-haiku-4-5
SENTINEL_MAX_TOKENS=16000

# ── Tavily web search (FORTEMENT RECOMMANDÉ — gratuit 1000 req/mois)
TAVILY_API_KEY=tvly-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SENTINEL_TAVILY_MAX=5

# ── Email SMTP (OBLIGATOIRE pour envoi rapport) ──────────────
# Gmail : créer un "mot de passe d'application" dans
#         compte Google → Sécurité → Mots de passe d'applications
SMTP_USER=votre.adresse@gmail.com
SMTP_PASS=xxxx xxxx xxxx xxxx
REPORT_EMAIL=destinataire@example.com

# ── SMTP avancé (optionnel — défaut Gmail) ───────────────────
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587

# ── SAM.gov (RECOMMANDÉ — gratuit sur sam.gov/developers) ────
SAM_GOV_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SENTINEL_MIN_CONTRACT_USD=1000000

# ── Base de données SQLite (optionnel — défaut : data/sentinel.db)
SENTINEL_DB=data/sentinel.db

# ── Serveur de santé (optionnel — défaut : 8765) ─────────────
SENTINEL_HEALTH_PORT=8765

# ── Circuit-breaker API (optionnel) ──────────────────────────
SENTINEL_CB_MAX=3

# ── Purge rapports (optionnel — défaut : 30 jours) ───────────
SENTINEL_RETENTION_DAYS=30

# ── Telegram (optionnel — PRIORITÉ 3) ────────────────────────
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_SESSION=data/sentinel_telegram.session

# ── OpenAI fallback (optionnel — PRIORITÉ 4) ─────────────────
OPENAI_API_KEY=

# ── Debug ─────────────────────────────────────────────────────
SENTINEL_MAILER_DRYRUN=0
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# CHANGELOG.MD (E2-FIX CDC-C)
# ═════════════════════════════════════════════════════════════════════════════

CHANGELOG_MD = """
# CHANGELOG.md — SENTINEL — Historique des versions

Format : `vX.Y.Z — AAAA-MM-JJ`
Types : Ajouté | Corrigé | Modifié | Supprimé | Sécurité

---

## v3.42 — 2026-04-09

### Corrigé
- `scraper_rss.py`    : SCR-42-FIX1 keywords TOML, SCR-42-FIX2 Jaccard adaptatif
- `report_builder.py` : RB-42-FIX1 regex <li> re.S → re.M (fusion listes)
- `report_builder.py` : RB-42-FIX2 nettoyage caractères invisibles Unicode
- `report_builder.py` : RB-42-FIX3 orphans/widows CSS @media print

### Ajouté
- `init_prompts.py`   : IP-42-FIX1 _extract_version() + _build_scope_md() dynamiques

---

## v3.41 — 2026-04-09

### Corrigé
- `sentinel_main.py`  : SONNET_MODEL transmis à run_sentinel()
- `sentinel_main.py`  : articles.extend(tg_arts or []) — garde None
- `sentinel_main.py`  : port SENTINEL_HEALTH_PORT protégé try/except ValueError
- `sentinel_main.py`  : spec/loader None guard dans _load_optional_scraper()
- `report_builder.py` : RB-41-FIX1 img_to_svg_or_b64 définie (NameError)
- `report_builder.py` : RB-41-FIX2 signature build_html_report corrigée
- `report_builder.py` : RB-41-FIX3/4/5/6/7/8 regex markdown_to_html corrigées
- `report_builder.py` : RB-41-FIX9/10/11 double guard, import local, SQLite Row
- `init_prompts.py`   : IP-41-FIX1 _write_file atomicité .env.example
- `init_prompts.py`   : IP-41-FIX2 TAVILYMAX validé
- `init_prompts.py`   : IP-41-FIX3 main() complétée
- `init_prompts.py`   : IP-41-FIX4/5/6/7/8 .env.example, ANNEE, versions

---

## v3.40 — 2026-04-09

### Ajouté
- `db_manager.py`     : BUG-DB1 à BUG-DB7 — UNIQUE, DDL, CRUD, whitelist, LIKE
- `sentinel_api.py`   : A07-FIX call_api() hors boucle, R6-NEW-2 validation deltas
- `mailer.py`         : B10 MIME corrects, NEW-M1/M2/M3/M5
- `samgov_scraper.py` : FIX-SAM1/2/3/4 + NEW-SG3 run_all_procurement()
- `init_prompts.py`   : NEW-IP1/2/3/4 .env.example, idempotence, validation
- `report_builder.py` : A13/A14/A23/A24/B3/B10/VIS-3/6/7/VISUAL-R1/R3

### Corrigé
- Circuit-breaker cb_fail/cb_ok/cb_active consolidés
- Logging : zéro print() dans tous les scripts
- Retry SMTP : 3 tentatives backoff 30s/90s

---

## v3.37 — 2026-04-06

### Ajouté
- `watchdog.py` surveillance cron + alerte email (E1-REC1)
- Fallback LLM GPT-4o-mini si Anthropic APIError
- Circuit-breaker budget tokens 30k via SENTINEL_MAX_TOKENS
- 5 flux RSS activés (CSIS, WOTR, VOA Russia, IndiaDefenceReview, 38North)
- `db_manager.py` SQLite WAL centralisé — remplace JSON (E1-5)

### Corrigé
- tmp.rename(MEMORY_FILE) restauré — mémoire sauvegardée atomiquement
- tavilycalls, resp = None restaurés — NameError fatal supprimé
- chartpaths, build_html_report, send_report restaurés — pipeline complet
- Filtre LATAM désormais actif dans scrape_one()

---

## v3.16 — 2026-03-15

### Ajouté
- Sources Israël, Turquie, Inde (PIB Defence, Takshashila)
- BOAMP + TED EU dans samgov_scraper.py
- Dark mode CSS dans report_builder.py
- Badges alerte HTML (VERT/ORANGE/ROUGE)
- 14 nouvelles sources OSINT critiques

---

## v3.12 — 2026-02-01

### Ajouté
- SQLite migration recommandée (E1-5)
- crossreference_articles() détection événements multi-sources
- weight_temporal() décroissance exponentielle λ=0.15

### Corrigé
- scrape_all_feeds() ThreadPoolExecutor — 10 threads max
- seen_list écriture atomique tmp → rename (A08-FIX)
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═════════════════════════════════════════════════════════════════════════════

def _extract_version(script_path: Path) -> str:
    """
    IP-42-FIX1 : lit la version depuis les 10 premières lignes d'un script Python.
    Cherche un pattern vX.YY dans les commentaires d'en-tête.
    Retourne "?" si introuvable ou si le fichier est absent.

    Exemples détectés :
      # sentinel_main.py — SENTINEL v3.41
      VERSION = "3.42"
      # report_builder.py — SENTINEL v3.42
    """
    if not script_path.exists():
        return "?"
    try:
        lines = script_path.read_text(encoding="utf-8").splitlines()[:15]
        for line in lines:
            m = re.search(r'[vV](d+.d+(?:.d+)?)', line)
            if m:
                return f"v{m.group(1)}"
    except OSError:
        pass
    return "?"


def _build_scope_md() -> str:
    """
    IP-42-FIX1 : génère SCOPE.md avec les versions lues dynamiquement
    depuis les scripts présents sur le disque.
    Élimine la maintenance manuelle à chaque mise à jour de sous-script.
    """
    # ── Table scripts obligatoires ────────────────────────────────────────
    rows_required = []
    for script, role, _ in _SCRIPT_TABLE:
        version = _extract_version(Path(script))
        status  = "✅" if Path(script).exists() else "⏳ À créer"
        rows_required.append(f"| `{script}` | {role} | {version} | {status} |")

    # ── Table scripts optionnels ──────────────────────────────────────────
    rows_optional = []
    for script, role, priority in _OPTIONAL_TABLE:
        rows_optional.append(f"| `{script}` | {role} | {priority} |")

    scope = f"""# SENTINEL — Périmètre & Contraintes (SCOPE.md)
# Versions lues dynamiquement depuis les scripts (IP-42-FIX1)

## Périmètre livrable garanti

| Script | Rôle | Version | Statut |
|--------|------|---------|--------|
{chr(10).join(rows_required)}

## Optionnel (hors CDC)

| Script | Rôle | Priorité |
|--------|------|----------|
{chr(10).join(rows_optional)}

## Couverture OSINT

| Zone | Sources primaires | Analytique | Plafond OSINT |
|------|------------------:|:----------:|:-------------:|
| USA/OTAN | 85% | 95% | 90% |
| France/Europe | 80% | 85% | 85% |
| Israël | 70% | 30% | 70% |
| Turquie | 50% | 30% | 55% |
| Japon/Corée | 40% | 50% | 50% |
| Chine | 15% | 55% | 20% |
| Russie | 15% | 45% | 20% |
| Inde | 42% | 40% | 40% |

**Note** : Chine et Russie resteront à 15-20% (programmes classifiés ou filtrés à l'émission).

## Budget mensuel estimé

| Composant | Coût |
|-----------|------|
| Claude Sonnet 4.6 (1M tok) | ~4.80 €/mois |
| Tavily (150 req/j) | 0 € (gratuit) |
| Hébergement Raspberry Pi 4 | 0 € (électricité ~2 €) |
| **TOTAL** | **< 10 €/mois** |

## Contraintes légales

- Scraping RSS : légal (contenu publié volontairement)
- LinkedIn scraping : INTERDIT (arrêt hiQ v. LinkedIn)
- Twitter/X API : payant (100-500 USD/mois minimum)
- Telegram : légal via API officielle Telethon
- SAM.gov, TED EU, BOAMP : APIs publiques officielles, gratuites
""".strip()
    return scope


def _write_file(path: Path, content: str, label: str) -> bool:
    """
    Écrit un fichier de manière atomique (tmp → rename).
    Retourne True si écriture effectuée, False si skippée.

    IP-41-FIX1 : path.parent / (path.name + ".tmp") au lieu de path.with_suffix(".tmp").
    with_suffix() remplaçait le dernier suffixe — transformait
    Path(".env.example") → Path(".env.tmp"), cassant l'atomicité silencieusement.

    NEW-IP2 : idempotent — ne réécrit pas si déjà présent et --force absent.
    """
    if path.exists() and not FORCE_REWRITE:
        log.info(f"SKIP {label} — déjà présent ({path}) — utiliser --force pour réécrire")
        return False

    tmp = path.parent / (path.name + ".tmp")  # IP-41-FIX1
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
        log.info(f"ÉCRIT {label} → {path} ({len(content)} chars)")
        return True
    except OSError as e:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        log.error(f"ERREUR écriture {path} : {e}")
        return False


def _validate_prompt(path: Path, required_markers: list[str], label: str) -> list[str]:
    """
    Vérifie qu'un fichier prompt contient tous les marqueurs requis.
    Retourne la liste des marqueurs manquants (vide = OK).
    NEW-IP3
    """
    if not path.exists():
        return [f"FICHIER ABSENT : {path}"]
    content = path.read_text(encoding="utf-8")
    return [m for m in required_markers if m not in content]


def _create_directories() -> None:
    """
    Crée la structure de dossiers complète du projet.
    Cohérente avec sentinel_main.py (NEW-IP4).
    """
    dirs = [
        Path("data"),
        Path("output"),
        Path("output/charts"),
        Path("logs"),
        Path("prompts"),
        Path("backups"),
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        log.debug(f"DOSSIER {d} ✓")
    log.info(f"DOSSIERS créés/vérifiés : {[str(d) for d in dirs]}")


def _check_integrity() -> dict[str, list[str]]:
    """
    Vérifie l'intégrité de tous les fichiers prompts et de la structure.
    Retourne un dict {fichier: [problèmes]} — vide = tout OK.
    IP-41-FIX2 : system.txt vérifié sur son contenu (TAVILYMAX).
    """
    issues: dict[str, list[str]] = {}

    # System prompt
    sys_issues = _validate_prompt(
        Path("prompts/system.txt"), REQUIRED_SYSTEM_MARKERS, "system"
    )
    if sys_issues:
        issues["prompts/system.txt"] = sys_issues

    # Prompt journalier
    daily_issues = _validate_prompt(
        Path("prompts/daily.txt"), REQUIRED_DAILY_MARKERS, "daily"
    )
    if daily_issues:
        issues["prompts/daily.txt"] = daily_issues

    # Prompt mensuel + 8 modules obligatoires (CDC-C3)
    monthly_issues = _validate_prompt(
        Path("prompts/monthly.txt"), REQUIRED_MONTHLY_MARKERS, "monthly"
    )
    if Path("prompts/monthly.txt").exists():
        content_upper = Path("prompts/monthly.txt").read_text(encoding="utf-8").upper()
        missing_modules = [
            m for m in REQUIRED_MONTHLY_MODULES
            if m.upper() not in content_upper
        ]
        if missing_modules:
            monthly_issues.extend([f"MODULE MANQUANT: {m}" for m in missing_modules])
    if monthly_issues:
        issues["prompts/monthly.txt"] = monthly_issues

    # Fichiers critiques
    for f in ["requirements.txt", "SCOPE.md", ".env.example", "CHANGELOG.md"]:
        if not Path(f).exists():
            issues[f] = ["FICHIER ABSENT"]

    # Dossiers obligatoires
    for d in ["data", "output", "output/charts", "logs", "prompts", "backups"]:
        if not Path(d).is_dir():
            issues[d] = ["DOSSIER ABSENT"]

    return issues


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL (IP-41-FIX3 : main() complétée)
# ═════════════════════════════════════════════════════════════════════════════

def main() -> int:
    """
    Initialise la structure complète du projet SENTINEL.
    Retourne 0 (succès) ou 1 (erreur détectée).
    """
    log.info("─" * 60)
    log.info("SENTINEL v3.42 — Initialisation des prompts & structure projet")
    if FORCE_REWRITE:
        log.info("MODE --force : tous les fichiers seront réécrits")
    if CHECK_ONLY:
        log.info("MODE --check : vérification sans réécriture")
    log.info("─" * 60)

    # ── Mode check-only ───────────────────────────────────────────────────
    if CHECK_ONLY:
        issues = _check_integrity()
        if not issues:
            log.info("✓ Intégrité OK — tous les fichiers et dossiers sont en place")
            return 0
        else:
            log.warning(f"✗ {len(issues)} problème(s) détecté(s) :")
            for path, problems in issues.items():
                for p in problems:
                    log.warning(f"  [{path}] {p}")
            return 1

    # ── Création de la structure de dossiers ──────────────────────────────
    _create_directories()

    # ── Construction dynamique de SCOPE.md (IP-42-FIX1) ─────────────────
    # Appelé ici pour capturer les versions des scripts au moment de l'init,
    # pas au moment de l'import du module.
    scope_md_content = _build_scope_md()

    # ── Écriture des fichiers ─────────────────────────────────────────────
    files_written = 0
    files_skipped = 0

    write_targets = [
        (Path("prompts/system.txt"),  SYSTEM_PROMPT,    "system prompt"),
        (Path("prompts/daily.txt"),   DAILY_PROMPT,     "prompt journalier"),
        (Path("prompts/monthly.txt"), MONTHLY_PROMPT,   "prompt mensuel"),
        (Path("requirements.txt"),    REQUIREMENTS_TXT, "requirements.txt"),
        (Path(".env.example"),        ENV_EXAMPLE,      ".env.example"),
        (Path("SCOPE.md"),            scope_md_content, "SCOPE.md"),  # IP-42-FIX1
        (Path("CHANGELOG.md"),        CHANGELOG_MD,     "CHANGELOG.md"),
    ]

    for path, content, label in write_targets:
        written = _write_file(path, content, label)
        if written:
            files_written += 1
        else:
            files_skipped += 1

    # ── Validation post-écriture (NEW-IP3) ────────────────────────────────
    log.info("─" * 60)
    log.info("Validation des marqueurs après écriture...")

    issues = _check_integrity()

    if not issues:
        log.info("✓ Tous les marqueurs validés — prompts cohérents avec sentinel_api.py")
    else:
        log.error(f"✗ {len(issues)} problème(s) de validation :")
        for path, problems in issues.items():
            for p in problems:
                log.error(f"  [{path}] {p}")

    # ── Résumé final ──────────────────────────────────────────────────────
    log.info("─" * 60)
    log.info(f"RÉSUMÉ : {files_written} fichier(s) écrit(s), {files_skipped} ignoré(s)")

    if not Path(".env").exists():
        log.warning(
            "ATTENTION : fichier .env absent. "
            "Copier .env.example → .env et remplir ANTHROPIC_API_KEY et SMTP_PASS."
        )

    if issues:
        log.error("Initialisation terminée avec erreurs. Voir logs ci-dessus.")
        return 1

    log.info("✓ Initialisation SENTINEL v3.42 terminée avec succès.")
    log.info("  Prochaine étape : python sentinel_main.py")
    return 0


# ═════════════════════════════════════════════════════════════════════════════
# ENTRÉE
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    sys.exit(main())
