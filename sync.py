import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
from playwright.async_api import async_playwright
from supabase import create_client

load_dotenv()

SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
CIDNEFRO_URL  = os.getenv("CIDNEFRO_URL")
CIDNEFRO_USER = os.getenv("CIDNEFRO_USER")
CIDNEFRO_PASS = os.getenv("CIDNEFRO_PASS")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
COMPANY_ID = None

def get_company_id():
    global COMPANY_ID
    if not COMPANY_ID:
        res = supabase.table("companies").select("id").eq("slug", "utn").single().execute()
        COMPANY_ID = res.data["id"]
    return COMPANY_ID

def get_datas():
    hoje = datetime.today()
    inicio = hoje.replace(day=1).strftime("%Y-%m-%d")
    fim = (hoje + timedelta(days=2)).strftime("%Y-%m-%d")
    return inicio, fim

def converter_data(valor) -> str:
    if not valor:
        return None
    try:
        valor = str(valor).strip()
        if "/" in valor:
            partes = valor.split("/")
            if len(partes) == 3:
                return f"{partes[2]}-{partes[1]}-{partes[0]}"
        return valor
    except:
        return None

def converter_datetime(valor) -> str:
    if not valor:
        return None
    try:
        valor = str(valor).strip()
        if "/" in valor:
            partes = valor.split(" ")
            data = partes[0].split("/")
            hora = partes[1] if len(partes) > 1 else "00:00"
            return f"{data[2]}-{data[1]}-{data[0]} {hora}:00"
        return valor
    except:
        return None

def make_hash(record: dict) -> str:
    campos = ["situation","technician1","technician2","technician3",
              "technician4","technician5","technician6","technician7",
              "technician8","bed","localization","start","end"]
    valores = {k: str(record.get(k, "")) for k in campos}
    return hashlib.md5(json.dumps(valores, sort_keys=True).encode()).hexdigest()

def mapear_procedimento(rec: dict, company_id: str) -> dict:
    classification = rec.get("classification", "")
    if isinstance(classification, dict):
        classification = classification.get("name", "")

    work_shift = rec.get("work_shift", "")
    if not work_shift:
        start = rec.get("start", "") or rec.get("estimated_start", "") or ""
        if start:
            try:
                hora = int(str(start).split(":")[0])
                work_shift = "dia" if 7 <= hora < 19 else "noite"
            except:
                work_shift = None

    return {
        "company_id":        company_id,
        "external_id":       str(rec.get("id", "")),
        "external_sheet":    str(rec.get("sheet", "")),
        "hospital_name":     rec.get("hospital", ""),
        "doctor":            rec.get("doctor", ""),
        "technician1":       rec.get("technician1", ""),
        "technician2":       rec.get("technician2", ""),
        "technician3":       rec.get("technician3", ""),
        "technician4":       rec.get("technician4", ""),
        "technician5":       rec.get("technician5", ""),
        "technician6":       rec.get("technician6", ""),
        "technician7":       rec.get("technician7", ""),
        "technician8":       rec.get("technician8", ""),
        "patient_name":      rec.get("patient", ""),
        "patient_new":       bool(rec.get("new_patient", False)),
        "procedure_date":    converter_data(rec.get("date", "")),
        "prescription_date": converter_datetime(rec.get("prescription_date", None)),
        "classification":    classification,
        "procedure_type":    rec.get("proc", ""),
        "localization":      rec.get("localization", ""),
        "bed":               str(rec.get("bed", "")),
        "access_type":       rec.get("access", ""),
        "work_shift":        work_shift,
        "start_time":        rec.get("start", ""),
        "end_time":          rec.get("end", ""),
        "estimated_start":   rec.get("estimated_start", ""),
        "status":            rec.get("situation", ""),
        "suspended_reason":  rec.get("suspended_reason", ""),
        "interrupted":       rec.get("interrupted", ""),
        "agreement":         rec.get("agreement", ""),
        "validated":         bool(rec.get("validated", False)),
        "sync_hash":         make_hash(rec),
        "raw_data":          rec,
    }

def upsert_procedimentos(registros: list, import_id: str) -> dict:
    company_id = get_company_id()
    inserted = 0
    updated = 0
    unchanged = 0
    errors = 0

    external_ids = [str(r.get("id", "")) for r in registros]
    existing = {}
    try:
        for i in range(0, len(external_ids), 1000):
            lote_ids = external_ids[i:i+1000]
            res = supabase.table("procedures")\
                .select("external_id,sync_hash,id,status")\
                .eq("company_id", company_id)\
                .in_("external_id", lote_ids)\
                .execute()
            for row in res.data:
                existing[row["external_id"]] = row
        print(f"  Existentes no banco: {len(existing)}")
    except Exception as e:
        print(f"  Erro ao buscar existentes: {e}")

    staging_batch = []
    for rec in registros:
        staging_batch.append({
            "company_id": company_id,
            "import_id":  import_id,
            "raw_data":   rec,
            "processed":  False,
        })

    batch_size = 100
    for i in range(0, len(registros), batch_size):
        lote = registros[i:i+batch_size]
        to_upsert_new = []
        to_upsert_changed = []

        for rec in lote:
            try:
                mapped = mapear_procedimento(rec, company_id)
                ext_id = mapped["external_id"]
                if ext_id not in existing:
                    to_upsert_new.append(mapped)
                    inserted += 1
                elif existing[ext_id]["sync_hash"] != mapped["sync_hash"]:
                    to_upsert_changed.append(mapped)
                    updated += 1
                else:
                    unchanged += 1
            except Exception as e:
                print(f"  Erro ao mapear: {e}")
                errors += 1

        if to_upsert_new:
            try:
                supabase.table("procedures").upsert(
                    to_upsert_new,
                    on_conflict="company_id,external_id"
                ).execute()
            except Exception as e:
                print(f"  Erro ao inserir lote: {e}")
                errors += len(to_upsert_new)
                inserted -= len(to_upsert_new)

        if to_upsert_changed:
            try:
                supabase.table("procedures").upsert(
                    to_upsert_changed,
                    on_conflict="company_id,external_id"
                ).execute()
            except Exception as e:
                print(f"  Erro ao atualizar lote: {e}")
                errors += len(to_upsert_changed)
                updated -= len(to_upsert_changed)

    for i in range(0, len(staging_batch), 500):
        try:
            supabase.table("staging_nefrocloud")\
                .insert(staging_batch[i:i+500])\
                .execute()
        except Exception as e:
            print(f"  Erro staging: {e}")

    return {"inserted": inserted, "updated": updated,
            "unchanged": unchanged, "errors": errors}

async def coletar_cidnefro() -> list:
    data_inicio, data_fim = get_datas()
    print(f"  Período: {data_inicio} → {data_fim}")

    todos = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(CIDNEFRO_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        await page.fill('input[name="username"], input[name="usuario"]', CIDNEFRO_USER)
        await page.fill('input[type="password"]', CIDNEFRO_PASS)
        await page.click('button[type="submit"]')
        await page.wait_for_timeout(4000)

        page_num = 1
        length = 500
        total = None

        while True:
            response = await page.evaluate(f"""
                async () => {{
                    const resp = await fetch('/api/grade/atendimento/cadastro', {{
                        method: 'POST',
                        headers: {{'Content-Type': 'application/json',
                                  'X-Requested-With': 'XMLHttpRequest'}},
                        body: JSON.stringify({{
                            orderBy: {{id: "date", desc: "Y"}},
                            length: {length},
                            page: {page_num},
                            from: "{data_inicio}",
                            to: "{data_fim}"
                        }})
                    }});
                    return await resp.json();
                }}
            """)

            if not isinstance(response, dict):
                break

            dados = response.get("data", [])
            if total is None:
                total = response.get("total", 0)
                print(f"  Total na API: {total} registros")

            if not dados:
                break

            todos.extend(dados)
            print(f"  Coletados: {len(todos)}/{total}")

            if len(todos) >= total or len(dados) < length:
                break

            page_num += 1

        await browser.close()

    return todos

def registrar_import(status: str, fetched: int, stats: dict,
                     error: str = None, import_id: str = None):
    company_id = get_company_id()
    now = datetime.now(timezone.utc).isoformat()
    hora_atual = datetime.now().hour
    minutos = 15 if 7 <= hora_atual < 19 else 60
    proxima = (datetime.now(timezone.utc) + timedelta(minutes=minutos)).isoformat()

    supabase.table("import_logs").insert({
        "company_id":        company_id,
        "source":            "cidnefro_api",
        "started_at":        now,
        "finished_at":       datetime.now(timezone.utc).isoformat(),
        "status":            status,
        "records_fetched":   fetched,
        "records_inserted":  stats.get("inserted", 0),
        "records_updated":   stats.get("updated", 0),
        "records_unchanged": stats.get("unchanged", 0),
        "error_detail":      error,
        "expected_next_run": proxima,
    }).execute()

async def main():
    print(f"\n{'='*50}")
    print(f"AllocAI Sync — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*50}")

    import_id = str(uuid.uuid4())
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
    fetched = 0

    try:
        print("\n[1/3] Coletando dados do CidNefro...")
        registros = await coletar_cidnefro()
        fetched = len(registros)
        print(f"  ✓ {fetched} registros coletados")

        print("\n[2/3] Fazendo upsert no Supabase...")
        stats = upsert_procedimentos(registros, import_id)
        print(f"  ✓ Inseridos: {stats['inserted']}")
        print(f"  ✓ Atualizados: {stats['updated']}")
        print(f"  ✓ Sem mudança: {stats['unchanged']}")
        if stats['errors']:
            print(f"  ⚠ Erros: {stats['errors']}")

        print("\n[3/3] Registrando log...")
        registrar_import("success", fetched, stats, import_id=import_id)
        print(f"  ✓ Log salvo")

        print(f"\n✅ Sync concluído em {datetime.now().strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")
        registrar_import("error", fetched, stats, error=str(e), import_id=import_id)

asyncio.run(main())
