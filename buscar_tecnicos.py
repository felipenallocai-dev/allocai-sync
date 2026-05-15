from supabase import create_client
import os, json
from dotenv import load_dotenv

load_dotenv()

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
res = sb.table("technicians").select("id,name").execute()
print(json.dumps(res.data[:5], ensure_ascii=False))
print(f"Total: {len(res.data)}")
with open("tecnicos.json", "w", encoding="utf-8") as f:
    json.dump(res.data, f, ensure_ascii=False)
print("Salvo em tecnicos.json")
