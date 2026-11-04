"""Renderização simples de resultados SQL como tabelas Markdown."""
from decimal import Decimal


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value).replace("|", "\\|")


def to_md_table(columns: list[str], rows: list[tuple]) -> str:
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(_fmt(v) for v in row) + " |" for row in rows]
    return "\n".join([header, separator, *body])
