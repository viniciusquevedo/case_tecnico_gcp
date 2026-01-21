# -*- coding: utf-8 -*-
# Simulamos a ingestão diária do raw_sfmc_email_logs.csv com Quality Gate de JSON:
# Lê o CSV bruto
# Valida o campo message_details
# Remove linhas com JSON malformado
# Gera um CSV limpo e um log das linhas rejeitadas

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Tuple, Optional


def funcao_logger() -> logging.Logger:
    logger = logging.getLogger("ingest_email_logs")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        h = logging.StreamHandler()
        h.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)

    return logger


def valida_json(raw: str) -> Tuple[bool, Optional[str]]:
    """Valida se uma string é JSON válido. Retorna (ok, erro)."""
    if raw is None:
        return False, "message_details é nulo"

    raw = str(raw).strip()
    if raw == "" or raw.lower() == "nan":
        return False, "message_details é vazio"

    try:
        json.loads(raw)
        return True, None
    except json.JSONDecodeError as e:
        return False, f"JSONDecodeError: {e.msg} (pos {e.pos})"
    except Exception as e:
        return False, f"UnexpectedError: {type(e).__name__}: {e}"


def execucao(input_path: Path, output_path: Path, bad_log_path: Path) -> int:
    logger = funcao_logger()

    if not input_path.exists():
        logger.error("Arquivo de entrada não encontrado: %s", input_path)
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)
    bad_log_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    ok_rows = 0
    bad_rows = 0

    logger.info("Lendo: %s", input_path)

    with input_path.open("r", encoding="utf-8-sig", newline="") as f_in, \
         output_path.open("w", encoding="utf-8", newline="") as f_out, \
         bad_log_path.open("w", encoding="utf-8") as f_bad:

        reader = csv.DictReader(f_in)
        if not reader.fieldnames:
            logger.error("CSV sem header: %s", input_path)
            return 2

        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()

        # Log de rejeitados
        f_bad.write("row_number\tevent_id\treason\n")

        for row_number, row in enumerate(reader, start=2):
            total += 1
            ok, err = valida_json(row.get("message_details"))

            if ok:
                writer.writerow(row)
                ok_rows += 1
            else:
                bad_rows += 1
                event_id = (row.get("event_id") or "").strip()
                f_bad.write(f"{row_number}\t{event_id}\t{err}\n")

    logger.info("Total lidas: %s", total)
    logger.info("Aprovadas (JSON ok): %s", ok_rows)
    logger.info("Rejeitadas (JSON ruim): %s", bad_rows)
    logger.info("Saída limpa: %s", output_path)
    logger.info("Log rejeitadas: %s", bad_log_path)

    return 0 if ok_rows > 0 else 3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Caminho do CSV bruto (raw_sfmc_email_logs.csv)")
    p.add_argument("--output", required=True, help="Caminho do CSV limpo de saída")
    p.add_argument("--bad-log", default="logs/bad_json_rows.log", help="Arquivo de log das linhas rejeitadas")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    code = execucao(
        input_path=Path(args.input),
        output_path=Path(args.output),
        bad_log_path=Path(args.bad_log),
    )
    raise SystemExit(code)