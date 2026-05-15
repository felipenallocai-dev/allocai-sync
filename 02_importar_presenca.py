"""
Importa presença do Secullum para o Supabase.
Coloca este arquivo na pasta C:\allocai\allocai-sync\ junto com o presenca_secullum.csv
e roda: python 02_importar_presenca.py
"""
import csv, os, sys
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COMPANY_ID   = "b7a7639e-9692-440e-bf75-d4b159a2ab76"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# 1. carrega técnicos do Supabase
print("Carregando técnicos do Supabase...")
res = sb.table("technicians").select("id,name").eq("company_id", COMPANY_ID).execute()
tech_map = {r["name"].strip(): r["id"] for r in res.data}
print(f"  {len(tech_map)} técnicos carregados.")

# 2. lê CSV de presença
csv_path = os.path.join(os.path.dirname(__file__), "presenca_secullum.csv")
if not os.path.exists(csv_path):
    print(f"ERRO: arquivo não encontrado: {csv_path}")
    sys.exit(1)

presenca = []
with open(csv_path, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        presenca.append(row)

print(f"  {len(presenca)} registros no CSV.")

# 3. monta batch
sem_match = set()
batch = []
for r in presenca:
    nome = r["nome"].strip()
    tech_id = tech_map.get(nome)
    if not tech_id:
        sem_match.add(nome)
        continue

    batch.append({
        "technician_id":        tech_id,
        "company_id":           COMPANY_ID,
        "date":                 r["date"],
        "shift":                r["shift"],
        "status":               r["status"],
        "entrada":              r["entrada"] or None,
        "saida":                r["saida"] or None,
        "horas_trabalhadas_min": int(r["horas_trabalhadas_min"]) if r["horas_trabalhadas_min"] else None,
        "extra_min":            int(r["extra_min"]) if r["extra_min"] else None,
        "matricula":            r["matricula"] or None,
        "departamento":         r["departamento"] or None,
        "fonte":                "secullum",
        "registered_by":        None,
    })

print(f"  {len(batch)} registros com técnico encontrado.")
if sem_match:
    print(f"  ⚠️  {len(sem_match)} técnicos sem match (ignorados):")
    for n in sorted(sem_match):
        print(f"    - {n}")

# 4. upsert em lotes de 200
print("\nImportando...")
inserted = updated = errors = 0
lote_size = 200
total_lotes = (len(batch) + lote_size - 1) // lote_size

for i in range(0, len(batch), lote_size):
    lote = batch[i:i+lote_size]
    lote_num = i // lote_size + 1
    try:
        sb.table("daily_presence").upsert(
            lote,
            on_conflict="technician_id,date,shift"
        ).execute()
        inserted += len(lote)
        print(f"  Lote {lote_num}/{total_lotes} ✓ ({len(lote)} registros)")
    except Exception as e:
        print(f"  Lote {lote_num}/{total_lotes} ❌ erro: {e}")
        errors += len(lote)

print(f"\n✅ Concluído!")
print(f"  Inseridos/atualizados: {inserted}")
print(f"  Erros: {errors}")
