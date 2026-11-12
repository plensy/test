"""Point d'entrée générique pour tous les jobs AWS Glue de la pipeline.

Un seul script (ScriptLocation) est réutilisé par les 8 jobs Glue
(nyc-pipeline-bronze-taxi, nyc-pipeline-silver-trips, etc.) — seuls les
paramètres de job diffèrent d'un job à l'autre. Exécute exactement le même
code que `python -m <module>` en local (config/ et src/ sont fournis en
--extra-py-files), garantissant l'identité du code entre local et AWS.

Paramètres de job Glue attendus :
  --S3_BUCKET    nom du bucket data lake (ex: nyc-pipeline-datalake-xxxx)
  --job_module   module à exécuter, ex: src.bronze.ingest
  --job_args     arguments du module, ex: "--dataset taxi" (chaîne vide si aucun)
"""
import os
import runpy
import sys

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["S3_BUCKET", "job_module", "job_args"])

os.environ["PIPELINE_ENV"] = "aws"
os.environ["S3_BUCKET"] = args["S3_BUCKET"]

# Reconstruit sys.argv pour que argparse (dans les modules src/*) fonctionne
# exactement comme en local : `python -m <module> <job_args...>`.
sys.argv = [args["job_module"], *args["job_args"].split()]

try:
    runpy.run_module(args["job_module"], run_name="__main__")
except SystemExit as e:
    # Nos modules se terminent par sys.exit(main()), correct en local. Mais
    # Glue traite TOUT SystemExit (même code 0 = succès) comme un échec du
    # job. On absorbe donc les sorties propres et on ne laisse remonter que
    # les vrais échecs (ex: gate qualité qui renvoie 1) pour que Step
    # Functions les détecte correctement via Catch.
    code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    if code != 0:
        raise
