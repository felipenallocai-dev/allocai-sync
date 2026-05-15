"""
AllocAI - Indicadores de Produtividade
Executa as 14 queries via supabase.rpc() ou supabase.table() conforme disponibilidade.
"""

import os, re, json
from dotenv import load_dotenv
from supabase import create_client

# Forca UTF-8 no terminal Windows
import sys
sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Corrige erros de tipo TIME ────────────────────────────────────────────────
# TIME::TIMESTAMP e TIME::INTERVAL sao invalidos; TIME - TIME = INTERVAL,
# DATE + TIME = TIMESTAMP ja sao suportados nativamente pelo PostgreSQL.
RAW_SQL = open(
    os.path.join(os.path.dirname(__file__), "allocai_indicadores.sql"),
    encoding="utf-8"
).read()

FIXED_SQL = RAW_SQL

# start_time/end_time sao TEXT no banco, nao TIME — precisa cast explicito
FIXED_SQL = FIXED_SQL.replace("p.end_time::TIMESTAMP",   "p.end_time::TIME")
FIXED_SQL = FIXED_SQL.replace("p.start_time::TIMESTAMP", "p.start_time::TIME")
FIXED_SQL = FIXED_SQL.replace("p.start_time::INTERVAL",  "p.start_time::TIME")

# em procedures a coluna e work_shift, nao shift
# Usa lookbehind para evitar substituir dp.shift (daily_presence usa "shift")
FIXED_SQL = re.sub(r'(?<![a-zA-Z])p\.shift', 'p.work_shift', FIXED_SQL)

# Q9: a CTE exporta p.work_shift mas o outer SELECT usa o nome "shift"
# Adiciona alias so nesse contexto especifico (linha seguida de technician1)
FIXED_SQL = FIXED_SQL.replace(
    "p.work_shift,\n        p.technician1",
    "p.work_shift AS shift,\n        p.technician1"
)

# Q10: CASE de normalizacao de turnos ja esta no SQL com work_shift diretamente
# (SD->dia, SN->noite); nenhuma transformacao adicional necessaria aqui

# prescription_datetime nao existe; a coluna e prescription_date
FIXED_SQL = FIXED_SQL.replace("p.prescription_datetime", "p.prescription_date")

# motivo de suspensao e suspended_reason, nao suspension_reason
FIXED_SQL = FIXED_SQL.replace("p.suspension_reason", "p.suspended_reason")

# ── Divide em 14 queries usando os blocos numerados como delimitador ─────────
# Cada bloco comeca com "-- ===....\n-- N. TITULO"
pattern = re.compile(
    r'--\s*={20,}.*?--\s*(\d+[a-z]?\.\s+[^\n]+)\n'
    r'(?:--[^\n]*\n)*'         # linhas de comentario extras (subtitulo, etc)
    r'(.*?)(?=\n--\s*={20,}|\Z)',
    re.DOTALL
)

queries = []
for m in pattern.finditer(FIXED_SQL):
    num_title = m.group(1).strip()
    body      = m.group(2).strip()
    if body:
        queries.append((num_title, body))

# Trata query 12 que tem dois sub-blocos (12a e 12b) unidos por UNION ALL
# O regex ja os captura juntos como um so bloco se a query 12 nao tiver separador interno
print(f"AllocAI - Indicadores de Produtividade")
print(f"URL: {SUPABASE_URL}")
print(f"Queries detectadas: {len(queries)}")

# ── Execucao via supabase.rpc ─────────────────────────────────────────────────
# Tenta chamar funcao exec_sql(query text) no banco. Se nao existir, reporta erro
# claro e mostra alternativas via supabase.table() onde possivel.
def run_via_rpc(sql: str):
    """Tenta exec_sql(query) — funcao customizada no banco do projeto."""
    # Remove ponto-e-virgula final: EXECUTE nao aceita ";" dentro do wrapper
    sql_clean = sql.rstrip().rstrip(";").rstrip()
    result = sb.rpc("exec_sql", {"query": sql_clean}).execute()
    return result.data

def run_via_table(table: str, select: str = "*", filters: dict = None,
                  limit: int = 500):
    """Fallback para queries simples usando supabase.table()."""
    q = sb.table(table).select(select)
    if filters:
        for col, val in filters.items():
            q = q.eq(col, val)
    return q.limit(limit).execute().data

# ── Display ───────────────────────────────────────────────────────────────────
def display(idx: int, title: str, rows):
    bar = "=" * 72
    print(f"\n{bar}")
    print(f"  [{idx}] {title}")
    print(bar)
    if not rows:
        print("  (sem resultados)")
        return
    if not isinstance(rows, list):
        print(f"  {rows}")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(str(c)), max(len(str(r.get(c, "") or "")) for r in rows))
              for c in cols}
    # limita largura maxima de cada coluna a 30 chars
    widths = {c: min(w, 30) for c, w in widths.items()}
    header = "  " + "  ".join(str(c).ljust(widths[c])[:widths[c]] for c in cols)
    sep    = "  " + "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows:
        line = "  " + "  ".join(str(row.get(c, "") or "").ljust(widths[c])[:widths[c]]
                                  for c in cols)
        print(line)
    print(f"\n  ({len(rows)} linha(s))")

# ── Descobre se exec_sql existe no banco ──────────────────────────────────────
EXEC_SQL_AVAILABLE = False
print("\nVerificando funcao exec_sql no banco...", end=" ")
try:
    sb.rpc("exec_sql", {"query": "SELECT 1 AS ok"}).execute()
    EXEC_SQL_AVAILABLE = True
    print("OK - usando exec_sql()")
except Exception as e:
    msg = str(e)
    if "Could not find the function" in msg or "PGRST202" in msg:
        print("NAO ENCONTRADA")
        print("\nA funcao exec_sql(query text) nao existe neste projeto Supabase.")
        print("Para habilitar, execute no SQL Editor do Supabase:")
        print("""
  CREATE OR REPLACE FUNCTION exec_sql(query text)
  RETURNS json
  LANGUAGE plpgsql
  SECURITY DEFINER
  AS $$
  DECLARE
    result json;
  BEGIN
    EXECUTE 'SELECT json_agg(row_to_json(t)) FROM (' || query || ') t'
    INTO result;
    RETURN COALESCE(result, '[]'::json);
  END;
  $$;
""")
        print("Rodando o que e possivel via supabase.table()...\n")
    else:
        print(f"ERRO: {e}")

# ── Fallback de unaccent para Q12 ────────────────────────────────────────────
# Verifica se unaccent pode ser criada (requer superuser na maioria dos casos)
def try_create_unaccent():
    try:
        run_via_rpc("CREATE EXTENSION IF NOT EXISTS unaccent SCHEMA public")
        return True
    except Exception:
        return False

# Fallback: query Q12 reescrita com CTEs que pre-computam nomes normalizados
# (regexp_replace executado UMA vez por nome distinto, nao por comparacao)
Q12_REGEXP_FALLBACK = """
WITH
norm AS (
    -- funcao inline: remove acentos PT-BR e coloca em minusculo
    -- aplicada UMA vez por nome distinto em cada tabela
    SELECT x, lower(
        translate(x,
            'áàâãäÁÀÂÃÄéèêëÉÈÊËíìîïÍÌÎÏóòôõöÓÒÔÕÖúùûüÚÙÛÜçÇñÑ',
            'aaaaaaaaaaaeeeeeeeeiiiiiiiioooooooooooouuuuuuuuccnn'
        )
    ) AS xn FROM (
        SELECT DISTINCT technician1 AS x FROM procedures WHERE technician1 IS NOT NULL
        UNION
        SELECT DISTINCT technician2 FROM procedures WHERE technician2 IS NOT NULL
        UNION
        SELECT DISTINCT name FROM technicians
    ) s
),
procs_norm AS (
    SELECT DISTINCT p.technician1 AS nome_orig, n.xn AS nome_norm
    FROM procedures p
    JOIN norm n ON n.x = p.technician1
    WHERE p.technician1 IS NOT NULL
),
techs_norm AS (
    SELECT t.name AS nome_orig, n.xn AS nome_norm
    FROM technicians t
    JOIN norm n ON n.x = t.name
)
SELECT pn.nome_orig AS tecnico,
       'So NefroCloud' AS situacao,
       'procedimento sem ponto registrado' AS descricao
FROM   procs_norm pn
WHERE  NOT EXISTS (SELECT 1 FROM techs_norm tn WHERE tn.nome_norm = pn.nome_norm)
UNION ALL
SELECT tn.nome_orig AS tecnico,
       'So Secullum' AS situacao,
       'ponto sem procedimento vinculado' AS descricao
FROM   techs_norm tn
WHERE  NOT EXISTS (SELECT 1 FROM procs_norm pn WHERE pn.nome_norm = tn.nome_norm)
ORDER BY situacao, tecnico
"""

# ── Executa cada query ────────────────────────────────────────────────────────
for i, (title, sql) in enumerate(queries, 1):
    label = f"{i}/{len(queries)}"
    print(f"\n[{label}] {title}...", end=" ", flush=True)

    if EXEC_SQL_AVAILABLE:
        try:
            rows = run_via_rpc(sql)
            if isinstance(rows, str):
                rows = json.loads(rows)
            print("OK")
            display(i, title, rows)
        except Exception as e:
            err = str(e)
            # Q12: se unaccent nao existe, tenta fallback com regexp_replace
            is_unaccent_err = (
                "unaccent" in err.lower() or
                ("42883" in err and "unaccent" in err.lower())
            )
            if is_unaccent_err:
                # Tenta criar a extensao unaccent primeiro
                if try_create_unaccent():
                    print("RETRY (unaccent criada)...", end=" ", flush=True)
                    sql_retry = sql
                else:
                    print("RETRY (translate fallback)...", end=" ", flush=True)
                    sql_retry = Q12_REGEXP_FALLBACK
                try:
                    rows = run_via_rpc(sql_retry)
                    if isinstance(rows, str):
                        rows = json.loads(rows)
                    print("OK")
                    display(i, title, rows)
                except Exception as e2:
                    print(f"ERRO: {e2}")
            else:
                print(f"ERRO")
                print(f"  Detalhe: {e}")
    else:
        # Fallbacks via supabase.table() para as queries mais simples
        try:
            if "2." in title and "PRESENCA" in title.upper():
                rows = run_via_table("daily_presence", "technician_id,date,shift,departamento,status")
            elif "3." in title and "SUSPENSAO" in title.upper() and "HOSPITAL" in title.upper():
                rows = run_via_table("procedures", "hospital_id,status")
            elif "7." in title:
                rows = run_via_table("procedures", "procedure_type,status")
            elif "11." in title:
                rows = run_via_table("daily_presence", "departamento,shift,status,date")
            else:
                print("PULADA (requer exec_sql ou funcao customizada)")
                continue
            print(f"OK (dados brutos via table — sem agregacao)")
            display(i, title + " [DADOS BRUTOS]", rows[:20])
        except Exception as e2:
            print(f"ERRO: {e2}")
