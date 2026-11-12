"""Fixture de test : simule un job qui échoue systématiquement.
Utilisée par tests/test_orchestration.py pour vérifier le comportement
retry/backoff/court-circuit de l'orchestrateur sans dépendre de la vraie
pipeline (pas de Spark, pas de données).
"""
import sys

print("simulation d'un échec (job factice)", file=sys.stderr)
sys.exit(1)
