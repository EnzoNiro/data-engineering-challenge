"""Etapa de landing: garante que os 12 arquivos mensais existam e normaliza os nomes.

- Os arquivos originais são lidos de SOURCE_DIR (no fork: nyc-tlc-data/, na raiz).
- Se faltar algum mês, é baixado do repositório do desafio (idempotente).
- Aceita variações de nome: yellow_tripdata_2022-01.parquet.gz, ..._parquet.gz, .parquet
- Cria data/landing/yellow_tripdata_YYYY-MM.parquet (link simbólico, ou cópia se o
  sistema de arquivos não permitir link).
"""
import logging
import re
import shutil
import urllib.request
from pathlib import Path

from etl.config import LANDING_DIR, SOURCE_BASE_URL, SOURCE_DIR, YEAR

log = logging.getLogger(__name__)

FILE_RE = re.compile(r"^yellow_tripdata_(\d{4})-(\d{2})[._]parquet(?:\.gz)?$")


def discover_source_files() -> dict[int, Path]:
    """Mapeia mês -> arquivo para os arquivos do ano configurado que já estão em SOURCE_DIR."""
    found: dict[int, Path] = {}
    for path in sorted(SOURCE_DIR.glob("yellow_tripdata_*")):
        match = FILE_RE.match(path.name)
        if match and int(match.group(1)) == YEAR:
            found[int(match.group(2))] = path
    return found


def download_month(month: int) -> Path:
    name = f"yellow_tripdata_{YEAR}-{month:02d}.parquet.gz"
    url = f"{SOURCE_BASE_URL}/{name}"
    target = SOURCE_DIR / name
    partial = target.with_suffix(target.suffix + ".part")
    log.info("Baixando %s", url)
    request = urllib.request.Request(url, headers={"User-Agent": "wecogno-etl"})
    with urllib.request.urlopen(request, timeout=120) as response, open(partial, "wb") as out:
        shutil.copyfileobj(response, out)
    partial.rename(target)
    return target


def _link_or_copy(source: Path, target: Path) -> None:
    try:
        target.symlink_to(source.resolve())
    except OSError:
        log.warning("Link simbólico indisponível; copiando %s", source.name)
        shutil.copy2(source, target)


def prepare_landing() -> list[str]:
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    files = discover_source_files()
    for month in range(1, 13):
        if month not in files:
            files[month] = download_month(month)

    for old in LANDING_DIR.glob("*.parquet"):
        old.unlink()

    landed = []
    for month in range(1, 13):
        target = LANDING_DIR / f"yellow_tripdata_{YEAR}-{month:02d}.parquet"
        _link_or_copy(files[month], target)
        landed.append(str(target))

    log.info("Landing pronto com %d arquivos", len(landed))
    return landed
