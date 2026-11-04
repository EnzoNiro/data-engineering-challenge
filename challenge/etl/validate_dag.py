"""Valida a DAG sem executá-la: importa sem erros e tem a estrutura (ordem das tarefas) esperada."""
import os
import sys
from pathlib import Path

DAG_ID = "wecogno_nyc_taxi_etl"
EXPECTED_ORDER = [
    "prepare_landing", "load_raw", "load_trusted", "load_refined",
    "quality_checks", "generate_answers", "export_samples", "validate_results",
]


def main() -> int:
    from airflow.models import DagBag

    dags_dir = os.getenv("DAGS_DIR", str(Path(__file__).resolve().parents[1] / "dags"))
    bag = DagBag(dag_folder=dags_dir, include_examples=False)
    problems = [f"erro de import em {f}: {e}" for f, e in bag.import_errors.items()]
    dag = bag.dags.get(DAG_ID)
    if dag is None:
        problems.append(f"DAG {DAG_ID} não encontrada")
    else:
        ids = set(dag.task_ids)
        if ids != set(EXPECTED_ORDER):
            problems.append(f"tarefas diferentes do esperado: {sorted(ids)}")
        else:
            for up, down in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:]):
                if down not in dag.get_task(up).downstream_task_ids:
                    problems.append(f"falta a dependência {up} >> {down}")
            if dag.has_cycle() if hasattr(dag, "has_cycle") else False:
                problems.append("a DAG tem ciclo")
    if problems:
        print("DAG INVÁLIDA:\n- " + "\n- ".join(problems))
        return 1
    print(f"DAG OK: {DAG_ID} -> " + " >> ".join(EXPECTED_ORDER))
    return 0


if __name__ == "__main__":
    sys.exit(main())
