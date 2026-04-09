# init_prompts.py — SENTINEL v3.40 — Initialisation des prompts et structure projet
# ─────────────────────────────────────────────────────────────────────────────
# Corrections v3.40 appliquées :
#   E2-FIX CDC-C    CHANGELOG.md créé automatiquement
#   R1-F2           requirements.txt généré avec versions épinglées
#   R2-NEW-4        SCOPE.md créé (testscope_md_exists)
#   FIX-OBS1        Logging structuré via logger nommé, zéro print()
#   NEW-IP1         .env.example généré avec toutes les variables documentées
#   NEW-IP2         Vérification idempotente — ne réécrit pas si --force absent
#   NEW-IP3         Marqueurs prompts validés après écriture (cohérence sentinelapi.py)
#   NEW-IP4         Structure dossiers complète créée (data/output/logs/prompts/
#                   output/charts) cohérente avec sentinelmain.py
#   NEW-IP5         Prompt daily : marqueur OUI|NON Tavily documenté (OSINT-6)
#   CDC-C3          Prompt monthly : 8 modules obligatoires vérifiés
#   B9-FIX          Messages d'erreur explicites si prompts manquants
# ─────────────────────────────────────────────────────────────────────────────
# Usage :
#   python init_prompts.py             Initialisation initiale (skip si déjà fait)
#   python init_prompts.py --force     Réécrit tous les fichiers
#   python init_prompts.py --check     Vérifie l'intégrité sans réécrire
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
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

# ── Marqueurs obligatoires (cohérence sentinelapi.py) ────────────────────────
REQUIRED_DAILY_MARKERS = [
    "DATEAUJOURDHUI",
    "ARTICLESFILTRSPARSCRAPER",
    "MEMOIRECOMPRESSE7JOURS",
    "DEBUTJSONDELTA",
    "FINJSONDELTA",
]
REQUIRED_MONTHLY_MARKERS = [
    "MOIS",
    "ANNE",
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
<periode>MOIS ANNE</periode>

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
# REQUIREMENTS.TXT (R1-F2 — versions épinglées)
# ═════════════════════════════════════════════════════════════════════════════

REQUIREMENTS_TXT = """
# SENTINEL v3.40 — Dépendances Python épinglées (R1-F2)
# Installer avec : pip install -r requirements.txt
# Testé Python 3.10+ (walrus operator := requis)

# ── Core ────────────────────────────────────────────────────
anthropic>=0.25.0           # Claude Sonnet 4.6 / Haiku 4.5
feedparser>=6.0.11          # Collecte RSS
requests>=2.31.0            # HTTP scraping + SAM.gov + TED EU
python-dotenv>=1.0.0        # Variables d'environnement .env

# ── Analyse & IA ────────────────────────────────────────────
tavily-python>=0.3.0        # Recherche web temps réel (optionnel)
tenacity>=8.2.0             # Retry exponentiel sentinelapi.py (optionnel)
openai>=1.0.0               # Fallback GPT-4o-mini (optionnel)

# ── Visualisation ───────────────────────────────────────────
matplotlib>=3.8.0           # Graphiques PNG (R6A3-NEW-5)
plotly>=5.18.0              # Graphiques interactifs dashboard
kaleido>=0.2.1              # Export PNG Plotly

# ── Rapport HTML & Email ────────────────────────────────────
weasyprint>=60.0            # PDF depuis HTML (Linux : apt libcairo2)
schedule>=1.2.0             # Scheduler Python (alternative cron)

# ── NLP scoring (optionnel — PRIORITÉ 5 A32) ────────────────
scikit-learn>=1.3.0         # TF-IDF bigrammes nlpscorer.py

# ── Dashboard web (optionnel — PRIORITÉ 6 A25) ──────────────
streamlit>=1.28.0           # Interface web locale dashboard.py
fastapi>=0.104.0            # API REST optionnelle
uvicorn>=0.24.0             # Serveur ASGI FastAPI

# ── Telegram scraping (optionnel — PRIORITÉ 3 A20) ──────────
telethon>=1.30.0            # Telegram API telegramscraper.py

# ── Système ─────────────────────────────────────────────────
# Linux/RPi AVANT pip install weasyprint :
#   sudo apt-get install -y libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0 libffi-dev
# Windows : pip install weasyprint suffit (bundle GTK v60 inclus)
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# .ENV.EXAMPLE (NEW-IP1)
# ═════════════════════════════════════════════════════════════════════════════

ENV_EXAMPLE = """
# SENTINEL v3.40 — Variables d'environnement
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
SENTINEL_MAILER_DRYRUN=0

# ── SAM.gov (RECOMMANDÉ — gratuit sur sam.gov/developers) ────
SAM_GOV_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SENTINEL_MIN_CONTRACT_USD=1000000

# ── Base de données SQLite (optionnel — défaut : data/sentinel.db)
SENTINEL_DB=data/sentinel.db

# ── Circuit-breaker API (optionnel) ──────────────────────────
SENTINEL_CB_MAX=3

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
# SCOPE.MD (R2-NEW-4)
# ═════════════════════════════════════════════════════════════════════════════

SCOPE_MD = """
# SENTINEL v3.40 — Périmètre & Contraintes (SCOPE.md)

## Périmètre livrable garanti (CDC v3.40)

| Script | Rôle | Statut |
|--------|------|--------|
| `init_prompts.py` | Initialisation structure & prompts | ✅ |
| `scraper_rss.py` | Collecte 72 flux RSS | ✅ |
| `memory_manager.py` | Mémoire compressée Haiku | ✅ |
| `sentinel_api.py` | Analyse Claude Sonnet + Tavily | ✅ |
| `charts.py` | Graphiques PNG/Plotly | ✅ |
| `report_builder.py` | Rapport HTML + PDF | ✅ |
| `mailer.py` | Envoi SMTP Gmail | ✅ |
| `sentinel_main.py` | Orchestrateur principal | ✅ |
| `samgov_scraper.py` | SAM.gov DoD + TED EU + BOAMP | ✅ |
| `db_manager.py` | SQLite WAL centralisé | ✅ |
| `healthcheck.py` | Monitoring RSS + config | ✅ |
| `watchdog.py` | Surveillance cron + alerte email | ✅ |

## Optionnel (hors CDC)

| Script | Rôle | Priorité |
|--------|------|----------|
| `ops_patents.py` | Brevets Espacenet OPS | PRIORITÉ 2 |
| `telegram_scraper.py` | Telegram militaire | PRIORITÉ 3 |
| `nlp_scorer.py` | TF-IDF bigrammes | PRIORITÉ 5 |
| `dashboard.py` | Interface Streamlit | PRIORITÉ 6 |

## Couverture OSINT v3.40

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


# ═════════════════════════════════════════════════════════════════════════════
# CHANGELOG.MD (E2-FIX CDC-C)
# ═════════════════════════════════════════════════════════════════════════════

CHANGELOG_MD = """
# CHANGELOG.md — SENTINEL — Historique des versions

Format : `vX.Y.Z — AAAA-MM-JJ`
Types : Ajouté | Corrigé | Modifié | Supprimé | Sécurité

---

## v3.40 — 2026-04-09

### Ajouté
- `db_manager.py` : BUG-DB1 contrainte UNIQUE sur `reports.date` (ON CONFLICT UPSERT)
- `db_manager.py` : BUG-DB2 syntaxe SQLite correcte dans `purge_seen_older_than_days()`
- `db_manager.py` : BUG-DB3 DDL exécuté une seule fois via flag `_DB_INITIALIZED`
- `db_manager.py` : BUG-DB4 méthodes CRUD `acteurs` implémentées
- `db_manager.py` : BUG-DB5 whitelist `ALLOWED_COLS` dans `save_metrics()`
- `db_manager.py` : BUG-DB6 `get_recent_reports()` filtre temporel réel (non LIMIT)
- `db_manager.py` : BUG-DB7 wildcards LIKE échappés dans `close_alerte()`
- `sentinel_api.py` : A07-FIX `call_api()` définie une seule fois hors boucle agentique
- `sentinel_api.py` : R6-NEW-2 validation schéma deltas — listes garanties
- `sentinel_api.py` : VISUAL-R1 `extract_metrics_from_report()` → `SentinelDB.save_metrics()`
- `mailer.py` : B10 MIME text/html + application/pdf corrects
- `mailer.py` : NEW-M1 destinataires multiples `REPORT_EMAIL=a@b.com,c@d.com`
- `mailer.py` : NEW-M2 mode dry-run `SENTINEL_MAILER_DRYRUN=1`
- `mailer.py` : NEW-M3 validation adresses email avant envoi
- `mailer.py` : NEW-M5 support TLS direct (port 465)
- `samgov_scraper.py` : FIX-SAM1 `deptname="DEPT OF DEFENSE"` + `ptype="o,p,k"`
- `samgov_scraper.py` : FIX-SAM2 TED API v3 POST JSON + CPV défense corrects
- `samgov_scraper.py` : FIX-SAM3 `seen_ids` set global — zéro doublon multi-keyword
- `samgov_scraper.py` : FIX-SAM4 `noticeId` (champ v2 correct) + `artscore=8.5`
- `samgov_scraper.py` : NEW-SG3 `run_all_procurement()` — point d'entrée unique
- `init_prompts.py` : NEW-IP1 `.env.example` généré avec toutes les variables
- `init_prompts.py` : NEW-IP2 vérification idempotente + `--force` / `--check`
- `init_prompts.py` : NEW-IP3 marqueurs prompts validés après écriture
- `init_prompts.py` : NEW-IP4 structure dossiers complète créée

### Corrigé
- Circuit-breaker : `cb_fail()` / `cb_ok()` / `cb_active()` consolidés
- Logging : zéro `print()` dans tous les scripts — remplacés par `log.*`
- Retry SMTP : 3 tentatives backoff 30s/90s (K-6 / R3A3-NEW-5)

---

## v3.37 — 2026-04-06

### Ajouté
- `watchdog.py` surveillance cron automatique + alerte email (E1-REC1)
- Fallback LLM GPT-4o-mini si Anthropic `APIError` (E1-Réserve2)
- Circuit-breaker budget tokens 30k via `SENTINEL_MAX_TOKENS` (E1-Réserve1)
- 5 flux RSS activés (CSIS, WOTR, VOA Russia, IndiaDefenceReview, 38North)
- `db_manager.py` SQLite WAL centralisé — remplace JSON (E1-5)

### Corrigé
- `tmp.rename(MEMORY_FILE)` restauré — mémoire sauvegardée atomiquement
- `tavilycalls`, `resp = None` restaurés — NameError fatal supprimé
- `chartpaths`, `build_html_report`, `send_report` restaurés — pipeline complet
- Filtre LATAM désormais actif dans `scrape_one()` — bug corrigé

---

## v3.16 — 2026-03-15

### Ajouté
- Sources Israël, Turquie, Inde (PIB Defence, Takshashila)
- BOAMP + TED EU dans `samgov_scraper.py`
- Dark mode CSS dans `report_builder.py`
- Badges alerte HTML (VERT/ORANGE/ROUGE)
- 14 nouvelles sources OSINT critiques (USNI, C4ISRNET, Army Technology...)

---

## v3.12 — 2026-02-01

### Ajouté
- SQLite migration recommandée (E1-5)
- `crossreference_articles()` détection événements multi-sources
- `weight_temporal()` décroissance exponentielle λ=0.15

### Corrigé
- `scrape_all_feeds()` ThreadPoolExecutor — 10 threads max (R1A3-NEW-2)
- `seen_list` écriture atomique `tmp → rename` (A08-FIX)
""".strip()


# ═════════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═════════════════════════════════════════════════════════════════════════════

def _write_file(path: Path, content: str, label: str) -> bool:
    """
    Écrit un fichier de manière atomique (tmp → rename).
    Retourne True si écriture effectuée, False si skippée.
    NEW-IP2 : idempotent — ne réécrit pas si déjà présent et --force absent.
    """
    if path.exists() and not FORCE_REWRITE:
        log.info(f"SKIP {label} — déjà présent ({path}) — utiliser --force pour réécrire")
        return False

    tmp = path.with_suffix(".tmp")
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
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        log.debug(f"DOSSIER {d} ✓")
    log.info(f"DOSSIERS créés : {[str(d) for d in dirs]}")


def _check_integrity() -> dict[str, list[str]]:
    """
    Vérifie l'intégrité de tous les fichiers prompts.
    Retourne un dict {fichier: [marqueurs_manquants]}.
    """
    issues: dict[str, list[str]] = {}

    # Prompts
    daily_issues = _validate_prompt(
        Path("prompts/daily.txt"), REQUIRED_DAILY_MARKERS, "daily"
    )
    if daily_issues:
        issues["prompts/daily.txt"] = daily_issues

    monthly_issues = _validate_prompt(
        Path("prompts/monthly.txt"), REQUIRED_MONTHLY_MARKERS, "monthly"
    )
    # Vérifier les 8 modules obligatoires (CDC-C3)
    if Path("prompts/monthly.txt").exists():
        content = Path("prompts/monthly.txt").read_text(encoding="utf-8").upper()
        missing_modules = [
            m for m in REQUIRED_MONTHLY_MODULES
            if m.upper() not in content
        ]
        if missing_modules:
            monthly_issues.extend([f"MODULE MANQUANT: {m}" for m in missing_modules])
    if monthly_issues:
        issues["prompts/monthly.txt"] = monthly_issues

    # Fichiers critiques
    for f in ["prompts/system.txt", "requirements.txt", "SCOPE.md", ".env.example"]:
        if not Path(f).exists():
            issues[f] = ["FICHIER ABSENT"]

    # Dossiers
    for d in ["data", "output", "output/charts", "logs", "prompts"]:
        if not Path(d).is_dir():
            issues[d] = ["DOSSIER ABSENT"]

    return issues


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def main() -> int:
    """
    Initialise la structure complète du projet SENTINEL.
    Retourne 0 (succès) ou 1 (erreur).
    """
    log.info("─" * 60)
    log.info("SENTINEL v3.40 — Initialisation des prompts & structure projet")
    if FORCE_REWRITE:
        log.info("MODE --force : tous les fichiers seront réécrits")
    if CHECK_ONLY:
        log.info("MODE --check : vérification sans réécriture")
    log.info("─" * 60)

    # ── Mode check-only ───────────────────────────────────────────────────────\n    if CHECK_ONLY:\n        issues = _check_integrity()\n        if not issues:\n            log.info("✓ Intégrité OK — tous