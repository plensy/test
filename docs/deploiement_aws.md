# Guide de déploiement AWS — pas à pas

Ce guide couvre le déploiement de la pipeline validée en local vers AWS,
région **us-east-1** (co-localisation avec les sources publiques `nyc-tlc`
et `noaa-ghcn-pds` — zéro coût d'egress).

**Fais chaque phase dans l'ordre, vérifie le résultat avant de passer à la
suivante.** Colle-moi les messages d'erreur si quelque chose bloque.

Coût estimé de bout en bout : **~11-13 $** (voir rapport §7.2), largement
couvert par des crédits AWS Educate.

---

## Phase 0 — Prérequis

```bash
aws configure
# AWS Access Key ID / Secret Access Key : depuis la console AWS Educate (Vocareum) ou IAM
# Default region name : us-east-1
# Default output format : json

# Vérifier que ça fonctionne :
aws sts get-caller-identity
```

Tu dois voir ton `Account`, `UserId`, `Arn` s'afficher (pas d'erreur).

---

## Phase 1 — Fondations (S3 + IAM)

### 1.1 Bucket S3 (nom globalement unique — ajoute tes initiales/un nombre)

```bash
export BUCKET="nyc-pipeline-datalake-TONNOM"   # remplace TONNOM, garde ce nom pour la suite
aws s3 mb s3://$BUCKET --region us-east-1
aws s3api put-bucket-versioning --bucket $BUCKET \
  --versioning-configuration Status=Enabled

# Chiffrement au repos (SSE-S3 par défaut — cf. rapport §8.3)
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration '{
  "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
}'
```

### 1.2 Lifecycle policy (bronze/silver → Glacier après 90j, cf. rapport §7.5)

```bash
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration '{
  "Rules": [{
    "ID": "archive-bronze-silver",
    "Filter": {"Prefix": "bronze/"},
    "Status": "Enabled",
    "Transitions": [{"Days": 90, "StorageClass": "GLACIER_IR"}]
  }]
}'
```

### 1.3 Rôle IAM pour les jobs Glue

```bash
aws iam create-role --role-name nyc-pipeline-glue-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }'

aws iam attach-role-policy --role-name nyc-pipeline-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Accès S3 scopé à notre bucket uniquement
aws iam put-role-policy --role-name nyc-pipeline-glue-role \
  --policy-name nyc-pipeline-s3-access \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\",\"s3:DeleteObject\"], \"Resource\": [\"arn:aws:s3:::$BUCKET\", \"arn:aws:s3:::$BUCKET/*\"]}]
  }"
```

**✅ Vérification** : `aws s3 ls` doit lister ton bucket. `aws iam get-role --role-name nyc-pipeline-glue-role` doit renvoyer le rôle.

---

## Phase 2 — Empaqueter et uploader le code

```bash
cd "/Users/selmas/Documents/PROJET DATA PIPELINE"

# Zip de config/ + src/ (les modules réutilisés par tous les jobs Glue)
zip -r deps.zip config src -x "*__pycache__*" "*.pyc"

aws s3 cp deps.zip s3://$BUCKET/glue-artifacts/deps.zip
aws s3 cp orchestration/glue_jobs/glue_entrypoint.py s3://$BUCKET/glue-artifacts/glue_entrypoint.py
```

**✅ Vérification** : `aws s3 ls s3://$BUCKET/glue-artifacts/` doit lister les 2 fichiers.

---

## Phase 3 — Créer les 8 jobs Glue

Chaque job utilise le **même script** (`glue_entrypoint.py`) avec des
paramètres par défaut différents. Copie-colle ce bloc pour chacun des 8 jobs
(remplace `JOB_NAME`, `MODULE`, `JOB_ARGS` selon le tableau) :

| JOB_NAME | MODULE | JOB_ARGS |
|---|---|---|
| nyc-pipeline-bronze-taxi | src.bronze.ingest | --dataset taxi |
| nyc-pipeline-bronze-zones | src.bronze.ingest | --dataset zones |
| nyc-pipeline-bronze-weather | src.bronze.ingest | --dataset weather |
| nyc-pipeline-silver-weather-daily | src.silver.build_weather_daily | (vide) |
| nyc-pipeline-silver-trips | src.silver.build_trips | (vide) |
| nyc-pipeline-quality-checks | src.silver.quality_checks | (vide) |
| nyc-pipeline-gold-datamarts | src.gold.build_datamarts | (vide) |
| nyc-pipeline-ml-demand-forecast | src.ml.train_demand_forecast | (vide) |

Commande générique (adapter les 3 valeurs) :

```bash
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws glue create-job \
  --name "JOB_NAME" \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/nyc-pipeline-glue-role" \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/glue-artifacts/glue_entrypoint.py,PythonVersion=3" \
  --default-arguments "{
    \"--S3_BUCKET\": \"$BUCKET\",
    \"--job_module\": \"MODULE\",
    \"--job_args\": \"JOB_ARGS\",
    \"--extra-py-files\": \"s3://$BUCKET/glue-artifacts/deps.zip\",
    \"--additional-python-modules\": \"scikit-learn==1.4.2,mlflow>=2.12,great-expectations==0.18.19,pyarrow==21.0.0\"
  }" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --timeout 30
```

**✅ Vérification** : `aws glue get-jobs --query "Jobs[].Name"` doit lister les 8 noms.

---

## Phase 4 — Tester le premier job (le plus petit : zones)

```bash
aws glue start-job-run --job-name nyc-pipeline-bronze-zones
# Note le "JobRunId" retourné, puis suis le statut :
aws glue get-job-run --job-name nyc-pipeline-bronze-zones --run-id <JobRunId> --query "JobRun.JobRunState"
```

Attends `SUCCEEDED` (ou `FAILED` — dans ce cas, récupère les logs) :

```bash
aws logs tail /aws-glue/jobs/output --since 10m
```

**✅ Vérification finale** :

```bash
aws s3 ls s3://$BUCKET/bronze/zones/
```

Dis-moi le résultat (succès ou message d'erreur) avant de lancer les jobs suivants — on valide un par un comme en local.

---

## Phase 5 — Lancer les jobs restants (dans l'ordre des dépendances)

```bash
aws glue start-job-run --job-name nyc-pipeline-bronze-taxi
aws glue start-job-run --job-name nyc-pipeline-bronze-weather
# attendre SUCCEEDED sur les deux avant de continuer

aws glue start-job-run --job-name nyc-pipeline-silver-weather-daily
# attendre SUCCEEDED

aws glue start-job-run --job-name nyc-pipeline-silver-trips
# attendre SUCCEEDED

aws glue start-job-run --job-name nyc-pipeline-quality-checks
# attendre SUCCEEDED (sinon : les données silver ont un problème, ne pas continuer)

aws glue start-job-run --job-name nyc-pipeline-gold-datamarts
aws glue start-job-run --job-name nyc-pipeline-ml-demand-forecast
```

**✅ Vérification** :

```bash
aws s3 ls s3://$BUCKET/gold/ --recursive
```

Tu dois voir les 4 datamarts (fact_demand_by_zone, fact_weather_impact, fact_tips, fact_demand_forecast).

---

## Phase 6 — Glue Data Catalog + Athena

```bash
aws glue create-database --database-input '{"Name": "nyc_pipeline_gold"}'

# Crawler pour découvrir automatiquement le schéma des 4 datamarts
aws glue create-crawler \
  --name nyc-pipeline-gold-crawler \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/nyc-pipeline-glue-role" \
  --database-name nyc_pipeline_gold \
  --targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET/gold/\"}]}"

aws glue start-crawler --name nyc-pipeline-gold-crawler
# Attendre ~2-3 min, puis vérifier :
aws glue get-tables --database-name nyc_pipeline_gold --query "TableList[].Name"
```

**✅ Vérification** — requête Athena de test (nécessite un bucket de résultats) :

```bash
aws s3 mb s3://$BUCKET-athena-results
aws athena start-query-execution \
  --query-string "SELECT * FROM nyc_pipeline_gold.fact_tips LIMIT 5" \
  --result-configuration "OutputLocation=s3://$BUCKET-athena-results/"
```

---

## Phase 7 — SNS (alertes)

```bash
aws sns create-topic --name nyc-pipeline-alerts
# Note l'TopicArn retourné

aws sns subscribe --topic-arn <TopicArn> --protocol email --notification-endpoint TON_EMAIL
# Confirme l'abonnement depuis ta boîte mail
```

---

## Phase 8 — Step Functions

```bash
# Remplace ${AlertsTopicArn} dans le fichier par le vrai ARN SNS de la phase 7
sed "s|\${AlertsTopicArn}|<TopicArn>|g" orchestration/state_machine.asl.json > /tmp/state_machine.json

aws iam create-role --role-name nyc-pipeline-stepfunctions-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }'
aws iam attach-role-policy --role-name nyc-pipeline-stepfunctions-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueConsoleFullAccess
aws iam attach-role-policy --role-name nyc-pipeline-stepfunctions-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonSNSFullAccess

aws stepfunctions create-state-machine \
  --name nyc-pipeline \
  --definition file:///tmp/state_machine.json \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/nyc-pipeline-stepfunctions-role"

# Test manuel (équivalent de python -m orchestration.local_dag) :
aws stepfunctions start-execution --state-machine-arn <StateMachineArn>
```

**✅ Vérification** : console AWS Step Functions → visualisation graphique de l'exécution (utile pour la vidéo de présentation).

---

## Phase 9 — EventBridge Scheduler (déclenchement mensuel)

```bash
aws scheduler create-schedule \
  --name nyc-pipeline-monthly \
  --schedule-expression "cron(0 6 5 * ? *)" \
  --target "{\"Arn\": \"<StateMachineArn>\", \"RoleArn\": \"arn:aws:iam::${ACCOUNT_ID}:role/nyc-pipeline-stepfunctions-role\"}" \
  --flexible-time-window '{"Mode": "OFF"}'
```

(5 du mois, 6h UTC — aligné sur la publication mensuelle TLC.)

---

## Phase 10 — API + Dashboard (à faire en dernier, coûte le plus si laissé actif en continu)

Pour la démo/vidéo, le plus simple et le moins coûteux : lancer l'API et le
dashboard **en local pointés sur les données AWS** plutôt que de déployer
Lambda/API Gateway/EC2 tout de suite (cf. rapport §7.5 — optimisation "à la
demande plutôt que 24/7") :

```bash
export PIPELINE_ENV=aws
export S3_BUCKET=$BUCKET
uvicorn api.main:app --reload
streamlit run dashboard/app.py
```

Si tu veux vraiment déployer Lambda + API Gateway (pour que l'API soit
accessible sans ta machine allumée), dis-le-moi et je prépare cette phase en
détail séparément — c'est plus long à mettre en place.

---

## Ordre recommandé pour aujourd'hui

Fais les **Phases 0 à 5** d'abord (fondations + toutes les données jusqu'à
gold) et montre-moi les résultats. On enchaînera sur Athena/SNS/Step
Functions/EventBridge (Phases 6-9) ensuite, une fois les données confirmées
en place.
