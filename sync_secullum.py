import asyncio
import os
import re
import tempfile
import json
import requests
import websockets
from datetime import datetime, timedelta, timezone, time

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
SECULLUM_USER  = os.getenv("SECULLUM_USER")
SECULLUM_PASS  = os.getenv("SECULLUM_PASS")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

COMPANY_ID = None

DEPARTAMENTOS_ALVO = {
    "Téc. Enfermagem R1 Dia",
    "Téc. Enfermagem R1 Noite",
    "Téc. Enfermagem R2 Dia",
    "Téc. Enfermagem R2 Noite",
}

def get_company_id() -> str:
    global COMPANY_ID
    if not COMPANY_ID:
        res = supabase.table("companies").select("id").eq("slug", "utn").single().execute()
        COMPANY_ID = res.data["id"]
    return COMPANY_ID


def timedelta_to_time(td) -> time | None:
    if not isinstance(td, timedelta):
        return None
    total_sec = int(td.total_seconds())
    if total_sec < 0:
        return None
    h = (total_sec // 3600) % 24
    m = (total_sec % 3600) // 60
    return time(h, m)


def timedelta_to_minutes(td) -> int:
    if not isinstance(td, timedelta):
        return 0
    total = int(td.total_seconds())
    return max(0, total // 60)


def is_texto(val, *keywords) -> bool:
    if not isinstance(val, str):
        return False
    v = val.strip().upper()
    return any(v == kw.upper() for kw in keywords)


# ─── autenticação ────────────────────────────────────────────────────────────

def get_token() -> tuple[str, str]:
    """Faz login e retorna (bearer_token, banco_id)."""
    print("  Fazendo login no Secullum...")

    # Step 1: login no autenticador
    resp = requests.post(
        "https://autenticador.secullum.com.br/Token",
        data={
            "grant_type": "password",
            "username": SECULLUM_USER,
            "password": SECULLUM_PASS,
            "client_id": "3001",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    auth_data = resp.json()
    token = auth_data["access_token"]
    print("  Token obtido.")

    # Step 2: busca o banco (empresa) disponível
    resp2 = requests.get(
        "https://pontoweb.secullum.com.br/Configuracoes",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp2.raise_for_status()
    config = resp2.json()

    # pega o primeiro banco disponível
    banco_id = None
    if isinstance(config, list) and len(config) > 0:
        banco_id = config[0].get("id") or config[0].get("bancoDados")
    elif isinstance(config, dict):
        banco_id = config.get("id") or config.get("bancoDados")

    if not banco_id:
        # tenta buscar em /Funcionarios para pegar o header
        resp3 = requests.get(
            "https://pontoweb.secullum.com.br/Funcionarios",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        banco_id = resp3.headers.get("Secullumbancoselecioando", "")

    print(f"  Banco ID: {banco_id}")
    return token, str(banco_id)


# ─── download via API ────────────────────────────────────────────────────────

async def download_excel(download_dir: str) -> str | None:
    hoje = datetime.today()
    inicio = hoje.replace(day=1).strftime("%Y-%m-%d")
    fim = hoje.strftime("%Y-%m-%d")
    inicio_fmt = hoje.replace(day=1).strftime("%d/%m/%Y")
    fim_fmt = hoje.strftime("%d/%m/%Y")

    print(f"  Período: {inicio_fmt} → {fim_fmt}")

    token, banco_id = get_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Secullumbancoselecioando": banco_id,
        "Content-Type": "application/json",
        "Origin": "https://pontoweb.secullum.com.br",
        "Referer": "https://pontoweb.secullum.com.br/",
    }

    # Step 3: busca ImpressaoCalculo para obter configuração
    print("  Buscando configuração de impressão...")
    resp = requests.get(
        "https://pontoweb.secullum.com.br/ImpressaoCalculo",
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    impressao_config = resp.json()
    print(f"  Config obtida: {str(impressao_config)[:100]}")

    # Step 4: solicita geração do relatório via POST
    print("  Solicitando geração do relatório...")
    payload = {
        "dataInicio": inicio,
        "dataFim": fim,
        "tipoRelatorio": 0,        # Cartão Ponto
        "formato": 6,              # Excel - Layout Simplificado
        "listaCampos": "Lista Padrão",
        "todosOsFuncionarios": True,
        "totaisNoRodape": True,
    }

    resp_post = requests.post(
        "https://pontoweb.secullum.com.br/ImpressaoCalculo",
        headers=headers,
        json=payload,
        timeout=60,
    )

    if resp_post.status_code not in (200, 201, 202):
        print(f"  Resposta: {resp_post.status_code} - {resp_post.text[:200]}")
        raise RuntimeError(f"Erro ao solicitar relatório: {resp_post.status_code}")

    result = resp_post.json()
    print(f"  Resultado: {str(result)[:200]}")

    # pega a URL ou ID do arquivo gerado
    file_url = result.get("url") or result.get("arquivo") or result.get("link")

    if file_url:
        print(f"  URL do arquivo: {file_url}")
        resp_file = requests.get(file_url, headers=headers, timeout=60)
        resp_file.raise_for_status()
        dest = os.path.join(download_dir, "secullum.xlsx")
        with open(dest, "wb") as f:
            f.write(resp_file.content)
        print(f"  Arquivo salvo: {dest}")
        return dest

    # se não tiver URL direta, tenta WebSocket
    report_id = result.get("id") or result.get("relatorioId")
    if report_id:
        print(f"  ID do relatório: {report_id}. Aguardando via WebSocket...")
        dest = await aguardar_via_websocket(report_id, token, banco_id, download_dir)
        return dest

    raise RuntimeError(f"Formato de resposta inesperado: {result}")


async def aguardar_via_websocket(report_id, token, banco_id, download_dir):
    """Aguarda o relatório via WebSocket e baixa o arquivo."""
    ws_url = f"wss://pontowebrelatorios.secullum.com.br/?token={token}&banco={banco_id}"

    dest = os.path.join(download_dir, "secullum.xlsx")

    async with websockets.connect(
        ws_url,
        extra_headers={
            "Origin": "https://pontoweb.secullum.com.br",
        },
        ping_interval=20,
        ping_timeout=60,
    ) as ws:
        print("  WebSocket conectado. Aguardando arquivo...")
        async for message in ws:
            try:
                data = json.loads(message)
                print(f"  WS mensagem: {str(data)[:150]}")

                # verifica se é o relatório pronto
                url = data.get("url") or data.get("arquivo") or data.get("link")
                if url:
                    print(f"  Arquivo pronto: {url}")
                    resp = requests.get(url, timeout=60)
                    resp.raise_for_status()
                    with open(dest, "wb") as f:
                        f.write(resp.content)
                    print(f"  Arquivo salvo: {dest}")
                    return dest

            except json.JSONDecodeError:
                # pode ser binário (o próprio arquivo)
                if isinstance(message, bytes) and len(message) > 100:
                    with open(dest, "wb") as f:
                        f.write(message)
                    print(f"  Arquivo binário recebido via WS: {dest}")
                    return dest

    raise RuntimeError("WebSocket fechou sem entregar o arquivo.")


# ─── parse do Excel ──────────────────────────────────────────────────────────

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}")


def parse_excel(path: str) -> list[dict]:
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    registros = []

    i = 0
    while i < len(rows):
        row = rows[i]
        nome = None
        matricula = None
        funcao = None
        departamento = None

        for cell in row:
            if isinstance(cell, str):
                c = cell.strip()
                if c.startswith("Nome"):
                    partes = c.split(":", 1)
                    if len(partes) == 2 and partes[1].strip():
                        nome = partes[1].strip()

        j = i + 1
        while j < len(rows) and j < i + 10:
            r2 = rows[j]
            line_text = " ".join(str(c) for c in r2 if c is not None)

            if "Nome" in line_text and nome is None:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and "Nome" in cell:
                        partes = cell.split(":", 1)
                        nome = partes[1].strip() if len(partes) == 2 else None
                        if not nome and idx + 1 < len(r2):
                            nome = str(r2[idx + 1]).strip() if r2[idx + 1] else None
                        break

            if "Identificador" in line_text or "Matrícula" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and ("Identificador" in cell or "Matrícula" in cell):
                        partes = cell.split(":", 1)
                        matricula = partes[1].strip() if len(partes) == 2 else None
                        if not matricula and idx + 1 < len(r2):
                            matricula = str(r2[idx + 1]).strip() if r2[idx + 1] else None
                        break

            if "Função" in line_text or "Funcao" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and "Fun" in cell:
                        partes = cell.split(":", 1)
                        funcao = partes[1].strip() if len(partes) == 2 else None
                        if not funcao and idx + 1 < len(r2):
                            funcao = str(r2[idx + 1]).strip() if r2[idx + 1] else None
                        break

            if "Departamento" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and "Departamento" in cell:
                        partes = cell.split(":", 1)
                        departamento = partes[1].strip() if len(partes) == 2 else None
                        if not departamento and idx + 1 < len(r2):
                            departamento = str(r2[idx + 1]).strip() if r2[idx + 1] else None
                        break

            if r2[0] and isinstance(r2[0], str) and DATE_RE.match(str(r2[0]).strip()):
                break
            j += 1

        if nome and departamento and departamento in DEPARTAMENTOS_ALVO:
            print(f"    Técnico: {nome} | {departamento}")
            k = j
            while k < len(rows):
                dr = rows[k]
                col0 = str(dr[0]).strip() if dr[0] else ""
                if not DATE_RE.match(col0):
                    break

                data_str = col0[:10]
                try:
                    data_date = datetime.strptime(data_str, "%d/%m/%Y").date()
                except ValueError:
                    k += 1
                    continue

                ent1 = dr[1] if len(dr) > 1 else None
                sai1 = dr[2] if len(dr) > 2 else None
                sai2 = dr[4] if len(dr) > 4 else None
                sai3 = dr[6] if len(dr) > 6 else None
                ex50  = dr[8]  if len(dr) > 8  else None
                ex100 = dr[9]  if len(dr) > 9  else None
                exnot = dr[10] if len(dr) > 10 else None

                if is_texto(ent1, "FOLGA"):
                    status = "folga"
                elif is_texto(ent1, "FALTA"):
                    status = "falta"
                elif is_texto(ent1, "FÉRIAS", "FERIAS"):
                    status = "ferias"
                elif is_texto(ent1, "INSS", "AFASTADO"):
                    status = "afastado"
                elif isinstance(ent1, timedelta):
                    status = "presente"
                else:
                    status = "ausente"

                entrada_t = timedelta_to_time(ent1) if status == "presente" else None
                saida_t = None
                for sai in (sai3, sai2, sai1):
                    t = timedelta_to_time(sai)
                    if t is not None:
                        saida_t = t
                        break

                horas_min = None
                if status == "presente" and entrada_t and saida_t:
                    ent_min = entrada_t.hour * 60 + entrada_t.minute
                    sai_min = saida_t.hour * 60 + saida_t.minute
                    horas_min = max(0, sai_min - ent_min - 60)

                extra_min = (
                    timedelta_to_minutes(ex50) +
                    timedelta_to_minutes(ex100) +
                    timedelta_to_minutes(exnot)
                )

                shift = "noite" if (entrada_t and entrada_t.hour >= 18) else (
                    "noite" if "Noite" in (departamento or "") else "dia"
                )

                registros.append({
                    "nome": nome, "matricula": matricula, "funcao": funcao,
                    "departamento": departamento, "date": data_date.isoformat(),
                    "status": status,
                    "entrada": entrada_t.strftime("%H:%M") if entrada_t else None,
                    "saida": saida_t.strftime("%H:%M") if saida_t else None,
                    "horas_trabalhadas_min": horas_min,
                    "extra_min": extra_min if extra_min > 0 else None,
                    "shift": shift,
                })
                k += 1
            i = k
        else:
            i = j + 1

    print(f"  {len(registros)} registros de presença parseados.")
    return registros


_technician_cache: dict[str, str] = {}

def upsert_technician(nome: str, company_id: str) -> str:
    if nome in _technician_cache:
        return _technician_cache[nome]
    res = (
        supabase.table("technicians")
        .upsert({"company_id": company_id, "name": nome}, on_conflict="company_id,name")
        .select("id").execute()
    )
    tech_id = res.data[0]["id"]
    _technician_cache[nome] = tech_id
    return tech_id


def upsert_presenca(registros: list[dict]) -> dict:
    company_id = get_company_id()
    inserted = updated = errors = 0
    batch = []

    for r in registros:
        try:
            tech_id = upsert_technician(r["nome"], company_id)
            batch.append({
                "technician_id": tech_id, "company_id": company_id,
                "date": r["date"], "shift": r["shift"], "status": r["status"],
                "entrada": r["entrada"], "saida": r["saida"],
                "horas_trabalhadas_min": r["horas_trabalhadas_min"],
                "extra_min": r["extra_min"], "matricula": r["matricula"],
                "departamento": r["departamento"], "fonte": "secullum",
                "registered_by": None,
            })
        except Exception as e:
            print(f"  Erro ao preparar {r['nome']}: {e}")
            errors += 1

    if batch:
        existing_ids = set()
        try:
            tech_ids = list({b["technician_id"] for b in batch})
            dates = list({b["date"] for b in batch})
            res = (
                supabase.table("daily_presence")
                .select("technician_id,date,shift")
                .in_("technician_id", tech_ids).in_("date", dates).execute()
            )
            for row in res.data:
                existing_ids.add((row["technician_id"], row["date"], row["shift"]))
        except Exception as e:
            print(f"  Aviso ao buscar existentes: {e}")

        for b in batch:
            if (b["technician_id"], b["date"], b["shift"]) in existing_ids:
                updated += 1
            else:
                inserted += 1

        for i in range(0, len(batch), 200):
            lote = batch[i:i + 200]
            try:
                supabase.table("daily_presence").upsert(
                    lote, on_conflict="technician_id,date,shift"
                ).execute()
            except Exception as e:
                print(f"  Erro no upsert lote {i}: {e}")
                errors += len(lote)

    return {"inserted": inserted, "updated": updated, "errors": errors}


def registrar_log(status: str, fetched: int, stats: dict, error: str = None):
    company_id = get_company_id()
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("import_logs").insert({
        "company_id": company_id, "source": "secullum_ponto",
        "started_at": now, "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status, "records_fetched": fetched,
        "records_inserted": stats.get("inserted", 0),
        "records_updated": stats.get("updated", 0),
        "records_unchanged": 0, "error_detail": error,
    }).execute()


async def main():
    print(f"\n{'='*50}")
    print(f"AllocAI Sync — Secullum Ponto — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*50}")

    stats = {"inserted": 0, "updated": 0, "errors": 0}
    fetched = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            print("\n[1/3] Baixando cartão ponto do Secullum...")
            xlsx_path = await download_excel(tmpdir)
            if not xlsx_path or not os.path.exists(xlsx_path):
                raise RuntimeError("Download falhou — arquivo não encontrado.")
            print(f"  ✓ Arquivo: {xlsx_path}")

            print("\n[2/3] Parseando Excel...")
            registros = parse_excel(xlsx_path)
            fetched = len(registros)
            print(f"  ✓ {fetched} registros de presença")

            print("\n[3/3] Upserting no Supabase...")
            stats = upsert_presenca(registros)
            print(f"  ✓ Inseridos: {stats['inserted']}")
            print(f"  ✓ Atualizados: {stats['updated']}")
            if stats["errors"]:
                print(f"  ⚠ Erros: {stats['errors']}")

            registrar_log("success", fetched, stats)
            print(f"\n✅ Sync Secullum concluído em {datetime.now().strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"\n❌ Erro crítico: {e}")
            registrar_log("error", fetched, stats, error=str(e))
            raise


asyncio.run(main())
