# Script — Vidéo technique rapide (5-6 min, un seul enregistrement continu)

Pas de slides, pas de captures séparées à préparer : un seul screen recording,
tu cliques et tu commentes en 5-10 mots par étape. L'exécution Step Functions
tourne en arrière-plan pendant que tu montres le reste (elle prend ~5 min,
donc elle finit pile quand tu as fini de tout montrer).

## Avant de lancer l'enregistrement

1. Ouvre ces onglets à l'avance : Step Functions (`nyc-pipeline`), Glue (ETL jobs),
   Athena, GitHub Actions (`nyc-taxi-pipeline` → Actions), Swagger
   (`http://127.0.0.1:8000/docs`), Dashboard (`http://127.0.0.1:8501`).
2. Vérifie que l'API et le dashboard tournent encore (`lsof -nP -iTCP:8000 -iTCP:8501 -sTCP:LISTEN`).
   S'ils sont morts, relance avec `PIPELINE_ENV=aws` (cf. session précédente).
3. Lance l'enregistrement d'écran (Cmd+Shift+5 sur macOS).

## Déroulé (clique, dis une phrase, passe à la suite)

**0:00 — Step Functions**
- Va sur `nyc-pipeline` → **Start execution** → confirme.
- 1 phrase : "Je lance une exécution réelle de la pipeline sur AWS, orchestrée par Step Functions."
- Laisse cet onglet ouvert, tu y reviendras à la fin.

**0:20 — Glue**
- Onglet Glue → ETL jobs : montre les 8 jobs `nyc-pipeline-*`.
- 1 phrase : "8 jobs Glue Spark : Bronze, Silver, qualité, Gold, ML — c'est ce que Step Functions vient de déclencher."

**0:50 — Athena**
- Requête déjà prête : `SELECT * FROM fact_tips LIMIT 20;` → Run.
- 1 phrase : "Les résultats gold sont interrogeables en SQL via Athena, sur le Glue Data Catalog."

**1:20 — API**
- Swagger `/docs` → déplie `/forecast` → **Try it out** → `borough=Manhattan` → Execute.
- 1 phrase : "Les mêmes données sont exposées en API REST, ici la prévision de demande du modèle ML."

**1:50 — Dashboard**
- Scroll rapide sur les 4 onglets (demande / météo / pourboires / ML) — 10-15s chacun, pas d'explication longue, juste montrer que ça marche et pointer un chiffre clé par onglet.
- Phrases courtes : "pic de demande 16h-19h" / "la neige fait -15% de courses" / "pourboire Manhattan très supérieur au Bronx" / "prédiction vs réel du modèle, +25% mieux qu'une baseline".

**3:00 — CI/CD**
- Onglet GitHub Actions → dernier run vert (2 jobs Success).
- 1 phrase : "CI/CD sur GitHub Actions : tests + déploiement automatique sur push, validé en conditions réelles."

**3:30 — Retour Step Functions**
- Regarde l'état du graphe (probablement encore en cours ou déjà vert).
- Si terminé : montre le graphe tout vert + ouvre l'email SNS reçu.
- Si pas terminé : dis "l'exécution continue en arrière-plan, le graphe montre Bronze en parallèle puis Silver, qualité, Gold et ML" et coupe le montage ici, ou attends 30-60s de plus.
- 1 phrase de clôture : "Pipeline bout en bout, orchestrée, monitorée et déployée automatiquement."

**Fin ~4:30-5:00.** Pas besoin d'aller plus loin — technique montrée, c'est l'objectif.

## Si tu dois couper encore plus court (3 min)

Garde uniquement : Step Functions (lancer + montrer graphe final) → Athena (1 requête) →
API Swagger (1 appel) → Dashboard (2 onglets max) → GitHub Actions (1 capture d'écran du run vert).
Coupe Glue et le dashboard complet.
