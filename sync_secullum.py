import asyncio
import os
import re
import tempfile
import json
import requests
from datetime import datetime, timedelta, timezone, time

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
SECULLUM_USER = os.getenv("SECULLUM_USER")
SECULLUM_PASS = os.getenv("SECULLUM_PASS")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
COMPANY_ID = None

DEPARTAMENTOS_ALVO = {
    "Téc. Enfermagem R1 Dia",
    "Téc. Enfermagem R1 Noite",
    "Téc. Enfermagem R2 Dia",
    "Téc. Enfermagem R2 Noite",
}

def get_company_id():
    global COMPANY_ID
    if not COMPANY_ID:
        res = supabase.table("companies").select("id").eq("slug", "utn").single().execute()
        COMPANY_ID = res.data["id"]
    return COMPANY_ID

def timedelta_to_time(td):
    if not isinstance(td, timedelta): return None
    total_sec = int(td.total_seconds())
    if total_sec < 0: return None
    return time((total_sec // 3600) % 24, (total_sec % 3600) // 60)

def timedelta_to_minutes(td):
    if not isinstance(td, timedelta): return 0
    return max(0, int(td.total_seconds()) // 60)

def is_texto(val, *keywords):
    if not isinstance(val, str): return False
    return val.strip().upper() in [k.upper() for k in keywords]

# ─── autenticação ─────────────────────────────────────────────────────────────

def get_token():
    print("  Fazendo login...")
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
    token = resp.json()["access_token"]
    print("  Token obtido.")
    return token

def get_banco_id(token):
    """Descobre o ID do banco via header da resposta."""
    resp = requests.get(
        "https://pontoweb.secullum.com.br/Funcionarios",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    banco_id = resp.headers.get("Secullumbancoselecioando", "")
    print(f"  Banco ID: {banco_id}")
    return banco_id

# ─── SignalR negotiate ────────────────────────────────────────────────────────

def negotiate_signalr(token, banco_id):
    """Faz o negotiate do SignalR para obter connection token."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Secullumbancoselecioando": banco_id,
        "Origin": "https://pontoweb.secullum.com.br",
        "Referer": "https://pontoweb.secullum.com.br/",
    }
    resp = requests.post(
        "https://pontowebrelatorios.secullum.com.br/signalr/negotiate",
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        # tenta GET
        resp = requests.get(
            "https://pontowebrelatorios.secullum.com.br/signalr/negotiate",
            headers=headers,
            timeout=30,
        )
    print(f"  Negotiate status: {resp.status_code}")
    print(f"  Negotiate response: {resp.text[:200]}")
    return resp.json() if resp.ok else {}

# ─── download via SignalR ─────────────────────────────────────────────────────

async def download_excel(download_dir):
    import websockets

    hoje = datetime.today()
    inicio = hoje.replace(day=1).strftime("%d/%m/%Y")
    fim = hoje.strftime("%d/%m/%Y")
    print(f"  Período: {inicio} → {fim}")

    token = get_token()
    banco_id = get_banco_id(token)

    headers_http = {
        "Authorization": f"Bearer {token}",
        "Secullumbancoselecioando": banco_id,
        "Origin": "https://pontoweb.secullum.com.br",
        "Referer": "https://pontoweb.secullum.com.br/",
    }

    # busca lista de campos para obter ID da Lista Padrão
    resp = requests.get(
        "https://pontoweb.secullum.com.br/ImpressaoCalculo",
        headers=headers_http,
        timeout=30,
    )
    listas = resp.json()
    lista_padrao_id = next((l["Id"] for l in listas if "Padrão" in l["NomeLista"] or "Padrao" in l["NomeLista"]), 1)
    print(f"  Lista Padrão ID: {lista_padrao_id}")

    # monta a query string do relatório
    inicio_api = hoje.replace(day=1).strftime("%Y-%m-%d")
    fim_api = hoje.strftime("%Y-%m-%d")

    # monta URL do WebSocket SignalR
    # o token e banco vão como query params
    import urllib.parse
    qs = urllib.parse.urlencode({
        "token": token,
        "banco": banco_id,
    })
    ws_url = f"wss://pontowebrelatorios.secullum.com.br/signalr/connect?{qs}"

    # tenta negotiate primeiro
    neg = negotiate_signalr(token, banco_id)
    conn_token = neg.get("ConnectionToken", "")
    if conn_token:
        qs2 = urllib.parse.urlencode({
            "transport": "webSockets",
            "connectionToken": conn_token,
            "token": token,
            "banco": banco_id,
        })
        ws_url = f"wss://pontowebrelatorios.secullum.com.br/signalr/connect?{qs2}"

    print(f"  Conectando WebSocket: {ws_url[:80]}...")

    dest = os.path.join(download_dir, "secullum.xlsx")

    # payload SignalR para solicitar o relatório
    payload = {
        "H": "RelatorioCartaoPontoExcelSimplificado",
        "M": "GerarRelatorio",
        "A": [{
            "DataInicio": inicio_api,
            "DataFim": fim_api,
            "ListaImpressaoCalculoId": lista_padrao_id,
            "TipoRelatorio": 0,
            "TotaisNoRodape": True,
            "TodosOsFuncionarios": True,
            "Formato": 6,
        }],
        "I": 0,
    }

    async with websockets.connect(
        ws_url,
        additional_headers={
            "Origin": "https://pontoweb.secullum.com.br",
        },
        ping_interval=20,
    ) as ws:
        print("  WebSocket conectado!")

        # envia inicialização SignalR
        await ws.send(json.dumps({"protocol": "json", "version": 1}))
        await asyncio.sleep(1)

        # envia solicitação do relatório
        await ws.send(json.dumps(payload))
        print("  Solicitação enviada. Aguardando resposta...")

        # aguarda resposta por até 3 minutos
        deadline = asyncio.get_event_loop().time() + 180
        while asyncio.get_event_loop().time() < deadline:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                print(f"  WS recv: {str(msg)[:200]}")

                # arquivo binário
                if isinstance(msg, bytes) and len(msg) > 1000:
                    with open(dest, "wb") as f:
                        f.write(msg)
                    print(f"  Arquivo binário salvo: {dest}")
                    return dest

                # JSON com URL ou base64
                try:
                    data = json.loads(msg)
                    url = data.get("url") or data.get("R", {}).get("url") if isinstance(data.get("R"), dict) else None
                    if url:
                        r = requests.get(url, headers=headers_http, timeout=60)
                        with open(dest, "wb") as f:
                            f.write(r.content)
                        print(f"  Arquivo baixado via URL: {dest}")
                        return dest

                    b64 = data.get("b64") or data.get("R", {}).get("b64") if isinstance(data.get("R"), dict) else None
                    if b64:
                        import base64
                        with open(dest, "wb") as f:
                            f.write(base64.b64decode(b64))
                        print(f"  Arquivo salvo via base64: {dest}")
                        return dest

                except json.JSONDecodeError:
                    pass

            except asyncio.TimeoutError:
                continue

    raise RuntimeError("WebSocket não entregou o arquivo em 3 minutos.")


# ─── parse do Excel ──────────────────────────────────────────────────────────

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}")

def parse_excel(path):
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    registros = []
    i = 0
    while i < len(rows):
        row = rows[i]
        nome = matricula = funcao = departamento = None
        for cell in row:
            if isinstance(cell, str) and cell.strip().startswith("Nome"):
                partes = cell.split(":", 1)
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
                        nome = partes[1].strip() if len(partes) == 2 else (str(r2[idx+1]).strip() if idx+1 < len(r2) and r2[idx+1] else None)
                        break
            if "Identificador" in line_text or "Matrícula" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and ("Identificador" in cell or "Matrícula" in cell):
                        partes = cell.split(":", 1)
                        matricula = partes[1].strip() if len(partes) == 2 else (str(r2[idx+1]).strip() if idx+1 < len(r2) and r2[idx+1] else None)
                        break
            if "Fun" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and "Fun" in cell:
                        partes = cell.split(":", 1)
                        funcao = partes[1].strip() if len(partes) == 2 else (str(r2[idx+1]).strip() if idx+1 < len(r2) and r2[idx+1] else None)
                        break
            if "Departamento" in line_text:
                for idx, cell in enumerate(r2):
                    if isinstance(cell, str) and "Departamento" in cell:
                        partes = cell.split(":", 1)
                        departamento = partes[1].strip() if len(partes) == 2 else (str(r2[idx+1]).strip() if idx+1 < len(r2) and r2[idx+1] else None)
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
                if not DATE_RE.match(col0): break
                try:
                    data_date = datetime.strptime(col0[:10], "%d/%m/%Y").date()
                except ValueError:
                    k += 1; continue
                ent1 = dr[1] if len(dr) > 1 else None
                sai1 = dr[2] if len(dr) > 2 else None
                sai2 = dr[4] if len(dr) > 4 else None
                sai3 = dr[6] if len(dr) > 6 else None
                ex50  = dr[8]  if len(dr) > 8  else None
                ex100 = dr[9]  if len(dr) > 9  else None
                exnot = dr[10] if len(dr) > 10 else None
                if is_texto(ent1, "FOLGA"): status = "folga"
                elif is_texto(ent1, "FALTA"): status = "falta"
                elif is_texto(ent1, "FÉRIAS", "FERIAS"): status = "ferias"
                elif is_texto(ent1, "INSS", "AFASTADO"): status = "afastado"
                elif isinstance(ent1, timedelta): status = "presente"
                else: status = "ausente"
                entrada_t = timedelta_to_time(ent1) if status == "presente" else None
                saida_t = next((timedelta_to_time(s) for s in (sai3, sai2, sai1) if timedelta_to_time(s)), None)
                horas_min = None
                if status == "presente" and entrada_t and saida_t:
                    horas_min = max(0, (saida_t.hour*60+saida_t.minute) - (entrada_t.hour*60+entrada_t.minute) - 60)
                extra_min = timedelta_to_minutes(ex50) + timedelta_to_minutes(ex100) + timedelta_to_minutes(exnot)
                shift = "noite" if (entrada_t and entrada_t.hour >= 18) else ("noite" if "Noite" in (departamento or "") else "dia")
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
    print(f"  {len(registros)} registros parseados.")
    return registros

_technician_cache = {}

def upsert_technician(nome, company_id):
    if nome in _technician_cache: return _technician_cache[nome]
    res = supabase.table("technicians").upsert({"company_id": company_id, "name": nome}, on_conflict="company_id,name").select("id").execute()
    tech_id = res.data[0]["id"]
    _technician_cache[nome] = tech_id
    return tech_id

def upsert_presenca(registros):
    company_id = get_company_id()
    inserted = updated = errors = 0
    batch = []
    for r in registros:
        try:
            tech_id = upsert_technician(r["nome"], company_id)
            batch.append({"technician_id": tech_id, "company_id": company_id, "date": r["date"], "shift": r["shift"], "status": r["status"], "entrada": r["entrada"], "saida": r["saida"], "horas_trabalhadas_min": r["horas_trabalhadas_min"], "extra_min": r["extra_min"], "matricula": r["matricula"], "departamento": r["departamento"], "fonte": "secullum", "registered_by": None})
        except Exception as e:
            print(f"  Erro {r['nome']}: {e}"); errors += 1
    if batch:
        existing_ids = set()
        try:
            res = supabase.table("daily_presence").select("technician_id,date,shift").in_("technician_id", list({b["technician_id"] for b in batch})).in_("date", list({b["date"] for b in batch})).execute()
            for row in res.data: existing_ids.add((row["technician_id"], row["date"], row["shift"]))
        except: pass
        for b in batch:
            if (b["technician_id"], b["date"], b["shift"]) in existing_ids: updated += 1
            else: inserted += 1
        for i in range(0, len(batch), 200):
            try: supabase.table("daily_presence").upsert(batch[i:i+200], on_conflict="technician_id,date,shift").execute()
            except Exception as e: print(f"  Erro upsert: {e}"); errors += len(batch[i:i+200])
    return {"inserted": inserted, "updated": updated, "errors": errors}

def registrar_log(status, fetched, stats, error=None):
    company_id = get_company_id()
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("import_logs").insert({"company_id": company_id, "source": "secullum_ponto", "started_at": now, "finished_at": datetime.now(timezone.utc).isoformat(), "status": status, "records_fetched": fetched, "records_inserted": stats.get("inserted", 0), "records_updated": stats.get("updated", 0), "records_unchanged": 0, "error_detail": error}).execute()

async def main():
    print(f"\n{'='*50}")
    print(f"AllocAI Sync — Secullum Ponto — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*50}")
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    fetched = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            print("\n[1/3] Baixando cartão ponto...")
            xlsx_path = await download_excel(tmpdir)
            if not xlsx_path or not os.path.exists(xlsx_path):
                raise RuntimeError("Download falhou.")
            print(f"  ✓ {xlsx_path}")
            print("\n[2/3] Parseando Excel...")
            registros = parse_excel(xlsx_path)
            fetched = len(registros)
            print(f"  ✓ {fetched} registros")
            print("\n[3/3] Upserting no Supabase...")
            stats = upsert_presenca(registros)
            print(f"  ✓ Inseridos: {stats['inserted']} | Atualizados: {stats['updated']}")
            registrar_log("success", fetched, stats)
            print(f"\n✅ Concluído em {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"\n❌ Erro: {e}")
            registrar_log("error", fetched, stats, error=str(e))
            raise

asyncio.run(main())
