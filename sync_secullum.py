import asyncio
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone, time

from dotenv import load_dotenv
from playwright.async_api import async_playwright
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


async def download_excel(download_dir: str) -> str | None:
    hoje = datetime.today()
    inicio = hoje.replace(day=1).strftime("%d/%m/%Y")
    fim    = hoje.strftime("%d/%m/%Y")
    print(f"  Período: {inicio} → {fim}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )
        context = await browser.new_context(
            accept_downloads=True,
        )
        page    = await context.new_page()

        print("  Acessando Secullum (SSO)...")
        await page.goto(
            "https://autenticador.secullum.com.br/Authorization"
            "?response_type=code&client_id=3001"
            "&redirect_uri=https://pontoweb.secullum.com.br/Auth"
        )
        await page.wait_for_load_state("load", timeout=30_000)
        await page.fill("#Email", SECULLUM_USER)
        await page.fill("input[type='password']", SECULLUM_PASS)
        await page.click("button:has-text('Entrar')")
        await page.wait_for_load_state("load", timeout=30_000)
        print("  Login efetuado.")

        try:
            close_btn = page.locator("button:has-text('Fechar')").first
            if await close_btn.is_visible(timeout=3000):
                await close_btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        await page.goto("https://pontoweb.secullum.com.br/#/calculos")
        await page.wait_for_load_state("load", timeout=30_000)

        try:
            btn = page.locator("button:has-text('Fechar')")
            if await btn.is_visible(timeout=3000):
                await btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        try:
            btn = page.locator("button:has-text('OK')")
            if await btn.is_visible(timeout=3000):
                await btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        await page.wait_for_load_state("load", timeout=10_000)

        # preenche período
        date_inputs = await page.query_selector_all('input[type="text"][class*="data"], input[placeholder*="/"], input[ng-model*="data"]')
        if len(date_inputs) >= 2:
            await date_inputs[0].fill(inicio)
            await date_inputs[1].fill(fim)
        else:
            inputs = await page.query_selector_all('input[type="text"]')
            for i, inp in enumerate(inputs[:2]):
                await inp.fill(inicio if i == 0 else fim)

        await page.wait_for_timeout(1000)

        # aguarda botão Imprimir estar habilitado
        await page.wait_for_selector("#btnImprimir:not([disabled])", timeout=15_000)
        await page.wait_for_timeout(500)

        # abre modal de impressão
        await page.get_by_title("Imprimir").click()
        await page.wait_for_timeout(1500)

        # fecha sub-modal "Relatório Cartão Ponto" se aparecer
        try:
            fechar = page.get_by_label("Relatório Cartão Ponto").get_by_title("Fechar")
            if await fechar.is_visible(timeout=3000):
                await fechar.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

        # seleciona "Imprimir todos funcionários"
        await page.locator("label").filter(has_text="Imprimir todos funcionários").click()
        await page.wait_for_timeout(500)

        # Lista de campos → Lista Padrão
        await page.locator("#CampoListaCampos > div > .divSelectDireita > .Select > .Select-control > .Select-arrow-zone > .Select-arrow").click()
        await page.get_by_role("option", name="Lista Padrão").click()
        await page.wait_for_timeout(500)

        # Formato → Excel - Layout Simplificado
        await page.locator("#formatoImpressao").select_option("6")
        await page.wait_for_timeout(500)

        # clica Imprimir
        print("  Gerando relatório...")
        await page.get_by_role("button", name="Imprimir").click()

        # aguarda conclusão
        print("  Aguardando geração (pode demorar ~2 min)...")
        await page.wait_for_selector("text=Relatório gerado com êxito", timeout=180_000)
        print("  Relatório gerado! Abrindo arquivo...")
        await page.wait_for_timeout(500)

        # captura download
        dest = os.path.join(download_dir, "secullum.xlsx")
        
        async with context.expect_event("page") as new_page_info:
            await page.get_by_role("button", name="Abrir").click()
        
        new_page = await new_page_info.value
        await new_page.wait_for_timeout(5000)
        
        # tenta capturar o download via CDP direto no browser
        cdp = await context.new_cdp_session(new_page)
        await cdp.send("Browser.setDownloadBehavior", {
            "behavior": "allowAndName",
            "downloadPath": download_dir,
            "eventsEnabled": True
        })
        
        # aguarda o arquivo aparecer na pasta
        import glob
        for _ in range(30):
            await new_page.wait_for_timeout(1000)
            arquivos = glob.glob(os.path.join(download_dir, "*"))
            arquivos = [f for f in arquivos if os.path.isfile(f)]
            if arquivos:
                arquivo = sorted(arquivos, key=os.path.getmtime, reverse=True)[0]
                dest = arquivo
                print(f"  Download salvo: {dest}")
                break
        else:
            raise RuntimeError("Arquivo não capturado.")
        
        await new_page.close()

        await browser.close()
        return dest


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
                    elif i < len(rows) - 1:
                        for nc in row:
                            if nc and nc != cell:
                                nome = str(nc).strip()
                                break

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
