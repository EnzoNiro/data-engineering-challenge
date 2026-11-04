"""Executa etapas do pipeline fora do Airflow: `python -m etl.cli <etapa> [argumento]`.

Etapas: landing | quality | answers | samples | validate | counts <arquivo.json> | compare <arquivo.json>
"""
import logging
import sys


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not argv:
        print(__doc__)
        return 2
    step, args = argv[0], argv[1:]
    if step == "landing":
        from etl.landing import prepare_landing
        prepare_landing()
    elif step == "quality":
        from etl.quality import run_quality_checks
        run_quality_checks()
    elif step == "answers":
        from etl.answers import run_answers
        run_answers()
    elif step == "samples":
        from etl.export_samples import export_samples
        export_samples()
    elif step == "validate":
        from etl.validate import run_validation
        run_validation()
    elif step == "counts" and args:
        from etl.validate import snapshot_counts
        snapshot_counts(args[0])
    elif step == "compare" and args:
        from etl.validate import compare_counts
        compare_counts(args[0])
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
