import asyncio
import os
import re
import requests
import tempfile
from datetime import datetime, timedelta, timezone, time

from dotenv import load_dotenv
from playwright.async_api import async_playwright
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


async def download_excel(download_dir: str) -> str | None:
    hoje = datetime.today()
    inicio_dia = "1"
    inicio_mes = hoje.strftime("%b")[:3]  # ex: Mai
    fim_dia = str(hoje.day)
    fim_mes = hoje.strftime("%b")[:3]

    # Nomes dos meses em português como aparecem no Secullum
    meses_pt = {
        "Jan": "Jan", "Feb": "Fev", "Mar": "Mar", "Apr": "Abr",
        "May": "Mai", "Jun": "Jun", "Jul": "Jul", "Aug": "Ago",
        "Sep": "Set", "Oct": "Out", "Nov": "Nov", "Dec": "Dez"
    }
    mes_atual_pt = meses_pt.get(hoje.strftime("%b"), hoje.strftime("%b"))

    print(f"  Período: 01/{hoje.month:02d} → {hoje.day:02d}/{hoje.month:02d}/{hoje.year}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        # LOGIN — fluxo exato do codegen
        print("  Abrindo Secullum...")
        await page.goto("http://pontoweb.secullum.com.br/")
        await page.wait_for_timeout(3000)

        print("  Fazendo login...")
        await page.get_by_text("Entrar").click()
        await page.wait_for_timeout(2000)

        # preenche email se necessário
        try:
            email_field = page.get_by_role("textbox", name="Email")
            await email_field.fill(SECULLUM_USER, timeout=5000)
        except:
            pass

        await page.get_by_role("textbox", name="Senha").click()
        await page.get_by_role("textbox", name="Senha").fill(SECULLUM_PASS)
        await page.get_by_role("button", name="Entrar").click()
        await page.wait_for_timeout(4000)
        print(f"  URL após login: {page.url}")

        # Fecha modal de aviso UTN
        try:
            await page.get_by_role("button", name="Fechar").click(timeout=5000)
            print("  Modal fechado")
            await page.wait_for_timeout(1000)
        except:
            pass

        # Navega para Cálculos
        print("  Navegando para Cálculos...")
        await page.get_by_role("link", name=" Relatórios ").click()
        await page.wait_for_timeout(1000)
        await page.get_by_role("link", name="Cálculos").click()
        await page.wait_for_timeout(3000)

        # Fecha modal "Sim" se aparecer
        try:
            await page.get_by_role("button", name="Sim").click(timeout=3000)
            await page.wait_for_timeout(1000)
        except:
            pass

        # Configura período: dia 1 do mês atual até hoje
        print("  Configurando período...")
        data_inicio = hoje.replace(day=1).strftime("%d/%m/%Y")
        data_fim = hoje.strftime("%d/%m/%Y")
        try:
            await page.get_by_role("textbox", name="Período").click()
            await page.get_by_role("textbox", name="Período").fill(data_inicio)
            await page.wait_for_timeout(500)
            await page.locator("#dataFim").click()
            await page.locator("#dataFim").fill(data_fim)
            await page.wait_for_timeout(500)
            await page.get_by_role("button", name="OK").click()
            await page.wait_for_timeout(1000)
            print(f"  Período configurado: {data_inicio} → {data_fim}")
        except Exception as e:
            print(f"  Aviso configuração período: {e}")

        # Abre modal de impressão
        print("  Abrindo modal de impressão...")
        await page.get_by_title("Imprimir").click()
        await page.wait_for_timeout(2000)

        # Configura modal
        await page.get_by_text("Imprimir todos funcionários").click()
        await page.wait_for_timeout(500)

        await page.locator("#CampoListaCampos > div > .divSelectDireita > .Select > .Select-control > .Select-arrow-zone").click()
        await page.wait_for_timeout(500)
        await page.get_by_role("option", name="Lista Padrão").click()
        await page.wait_for_timeout(500)

        await page.locator("#formatoImpressao").select_option("6")
        await page.wait_for_timeout(500)

        dest = os.path.join(download_dir, "secullum.xlsx")

        # Clica Imprimir
        print("  Gerando relatório (aguarde até 5 min)...")
        await page.get_by_role("button", name="Imprimir").click()

        # aguarda relatório terminar
        await page.wait_for_selector("text=Relatório gerado com êxito", timeout=300000)
        print("  Relatório gerado! Capturando arquivo...")

        # captura o download — abre nova aba que dispara o download
        async with context.expect_page() as popup_info:
            await page.get_by_role("button", name="Abrir").click()

        popup = await popup_info.value
        print(f"  Popup URL: {popup.url}")

        # aguarda o download dentro do popup
        try:
            async with popup.expect_download(timeout=30000) as dl_info:
                pass  # o download já foi disparado ao abrir a página
            download = await dl_info.value
            await download.save_as(dest)
            print(f"  ✓ Download via popup: {dest}")
        except Exception as e:
            print(f"  Popup sem download direto: {e}")
            # tenta via URL do popup com cookies
            await popup.wait_for_load_state("networkidle", timeout=15000)
            file_url = popup.url
            if file_url and file_url != "about:blank":
                cookies = await context.cookies()
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                resp = requests.get(file_url, cookies=cookie_dict, timeout=120)
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    f.write(resp.content)
                print(f"  ✓ Download via URL: {dest}")
            else:
                raise RuntimeError("Não foi possível capturar o arquivo")

        await browser.close()
        return dest


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
        nome = None
        for cell in row:
            if isinstance(cell, str) and "Nome" in cell and cell.strip() != "Nome":
                partes = cell.split(":", 1)
                if len(partes) == 2 and partes[1].strip():
                    nome = partes[1].strip()
                    break
            elif isinstance(cell, str) and cell.strip() == "Nome":
                idx_cell = list(row).index(cell)
                if idx_cell + 1 < len(row) and row[idx_cell + 1]:
                    nome = str(row[idx_cell + 1]).strip()
                    break
        if not nome:
            i += 1
            continue
        departamento = None
        data_start = None
        for k in range(i+1, min(i+15, len(rows))):
            r = rows[k]
            for idx_c, cell in enumerate(r):
                if isinstance(cell, str) and "Departamento" in cell:
                    partes = cell.split(":", 1)
                    if len(partes) == 2 and partes[1].strip():
                        departamento = partes[1].strip()
                    elif idx_c + 1 < len(r) and r[idx_c + 1]:
                        departamento = str(r[idx_c + 1]).strip()
                    break
            if r[0] and isinstance(r[0], str) and DATE_RE.match(str(r[0]).strip()):
                data_start = k
                break
        if not departamento or not data_start:
            i += 1
            continue
        departamento = departamento.strip()
        if departamento not in DEPARTAMENTOS_ALVO:
            i = data_start + 1
            while i < len(rows):
                if rows[i][0] and isinstance(rows[i][0], str) and DATE_RE.match(str(rows[i][0]).strip()):
                    i += 1
                else:
                    break
            continue
        print(f"    Técnico: {nome} | {departamento}")
        k = data_start
        while k < len(rows):
            dr = rows[k]
            col0 = str(dr[0]).strip() if dr[0] else ""
            if not DATE_RE.match(col0): break
            try:
                data_date = datetime.strptime(col0[:10], "%d/%m/%Y").date()
            except ValueError:
                k += 1; continue
            ent1  = dr[1]  if len(dr) > 1  else None
            sai1  = dr[2]  if len(dr) > 2  else None
            sai2  = dr[4]  if len(dr) > 4  else None
            sai3  = dr[6]  if len(dr) > 6  else None
            ex50  = dr[8]  if len(dr) > 8  else None
            ex100 = dr[9]  if len(dr) > 9  else None
            exnot = dr[10] if len(dr) > 10 else None
            if is_texto(ent1, "FOLGA"):               status = "folga"
            elif is_texto(ent1, "FALTA"):             status = "falta"
            elif is_texto(ent1, "FÉRIAS", "FERIAS"):  status = "ferias"
            elif is_texto(ent1, "INSS", "AFASTADO"):  status = "afastado"
            elif isinstance(ent1, timedelta):         status = "presente"
            else:                                     status = "ausente"
            entrada_t = timedelta_to_time(ent1) if status == "presente" else None
            saida_t = next((timedelta_to_time(s) for s in (sai3, sai2, sai1) if timedelta_to_time(s)), None)
            horas_min = None
            if status == "presente" and entrada_t and saida_t:
                horas_min = max(0, (saida_t.hour*60+saida_t.minute) - (entrada_t.hour*60+entrada_t.minute) - 60)
            extra_min = timedelta_to_minutes(ex50) + timedelta_to_minutes(ex100) + timedelta_to_minutes(exnot)
            shift = "noite" if (entrada_t and entrada_t.hour >= 18) else ("noite" if "Noite" in departamento else "dia")
            registros.append({
                "nome": nome, "matricula": None, "funcao": None,
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
    print(f"  {len(registros)} registros parseados.")
    return registros
_technician_cache = {}

def upsert_technician(nome, company_id):
    if nome in _technician_cache: return _technician_cache[nome]
    # busca primeiro, cria se não existir
    res = supabase.table("technicians").select("id").eq("company_id", company_id).eq("name", nome).execute()
    if res.data:
        tech_id = res.data[0]["id"]
    else:
        res2 = supabase.table("technicians").insert({"company_id": company_id, "name": nome}).execute()
        tech_id = res2.data[0]["id"]
    _technician_cache[nome] = tech_id
    return tech_id

def upsert_presenca(registros):
    company_id = get_company_id()
    inserted = updated = errors = 0
    batch = []
    for r in registros:
        try:
            tech_id = upsert_technician(r["nome"], company_id)
            batch.append({
                "technician_id": tech_id,
                "company_id": company_id,
                "date": r["date"],
                "shift": r["shift"],
                "status": r["status"],
                "entrada": r["entrada"],
                "saida": r["saida"],
                "horas_trabalhadas_min": r["horas_trabalhadas_min"],
                "extra_min": r["extra_min"],
                "matricula": r["matricula"],
                "departamento": r["departamento"],
                "fonte": "secullum",
                "registered_by": None,
            })
        except Exception as e:
            print(f"  Erro {r['nome']}: {e}"); errors += 1
    if batch:
        existing_ids = set()
        try:
            res = supabase.table("daily_presence")\
                .select("technician_id,date,shift")\
                .in_("technician_id", list({b["technician_id"] for b in batch}))\
                .in_("date", list({b["date"] for b in batch}))\
                .execute()
            for row in res.data:
                existing_ids.add((row["technician_id"], row["date"], row["shift"]))
        except: pass
        for b in batch:
            if (b["technician_id"], b["date"], b["shift"]) in existing_ids: updated += 1
            else: inserted += 1
        for i in range(0, len(batch), 200):
            try:
                supabase.table("daily_presence")\
                    .upsert(batch[i:i+200], on_conflict="technician_id,date,shift")\
                    .execute()
            except Exception as e:
                print(f"  Erro upsert: {e}"); errors += len(batch[i:i+200])
    return {"inserted": inserted, "updated": updated, "errors": errors}

def registrar_log(status, fetched, stats, error=None):
    company_id = get_company_id()
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("import_logs").insert({
        "company_id": company_id,
        "source": "secullum_ponto",
        "started_at": now,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "records_fetched": fetched,
        "records_inserted": stats.get("inserted", 0),
        "records_updated": stats.get("updated", 0),
        "records_unchanged": 0,
        "error_detail": error,
    }).execute()

async def main():
    print(f"\n{'='*50}")
    print(f"AllocAI Sync — Secullum Ponto — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*50}")
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    fetched = 0
    tmpdir = r"C:\allocai\allocai-sync\downloads"
    os.makedirs(tmpdir, exist_ok=True)
    if True:
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