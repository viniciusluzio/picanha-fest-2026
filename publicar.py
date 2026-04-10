"""
Picanha Fest 2026 — Publicador Instagram
Roda via GitHub Actions a cada 5 minutos.
Lê planilha, publica posts pendentes, atualiza status.
"""
import warnings; warnings.filterwarnings("ignore")
import os, json, time, requests
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ─────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1Eqzy-Gvp8BbI33vrWMvnMfFHxMRPansmzdJDCRa4Er4"
IG_ACCOUNT_ID  = "17841413527665117"
META_TOKEN     = os.environ["META_TOKEN"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]

# ── Auth Google ────────────────────────────────────────────────────────────────
creds_info = json.loads(GOOGLE_SA_JSON)
creds  = service_account.Credentials.from_service_account_info(
    creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets = build("sheets", "v4", credentials=creds)

# ── Hora atual Brasília ────────────────────────────────────────────────────────
br_now      = datetime.now(timezone.utc) - timedelta(hours=3)
today       = br_now.strftime("%Y-%m-%d")
current_min = br_now.hour * 60 + br_now.minute

print(f"[{br_now.strftime('%Y-%m-%d %H:%M')} BRT] Verificando posts...")

# ── Ler planilha ───────────────────────────────────────────────────────────────
rows = sheets.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range="Página1!A:I"
).execute().get("values", [])

# ── Filtrar posts a publicar ───────────────────────────────────────────────────
# Janela: agendado há 0–10 min (GitHub Actions roda a cada 5 min)
posts = []
for i, row in enumerate(rows[1:], start=2):
    while len(row) < 9:
        row.append("")
    id_, arquivo, tipo, data, hora, legenda, hashtags, url_arquivo, status = row
    if status != "pendente":
        continue
    if not url_arquivo or not legenda:
        continue
    if data != today:
        continue
    try:
        h, m = map(int, hora.split(":"))
    except Exception:
        continue
    diff = current_min - (h * 60 + m)
    if 0 <= diff < 10:
        posts.append({
            "row": i, "tipo": tipo, "legenda": legenda,
            "hashtags": hashtags, "url": url_arquivo
        })

print(f"Posts encontrados: {len(posts)}")
if not posts:
    print("Nada a publicar agora.")
    exit(0)

# ── Helpers ────────────────────────────────────────────────────────────────────
def atualizar_status(row_index, status):
    sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"Página1!I{row_index}",
        valueInputOption="RAW",
        body={"values": [[status]]}
    ).execute()

def caption_completa(post):
    return post["legenda"] + "\n\n" + post["hashtags"]

def checar_erro(r, contexto=""):
    if "error" in r:
        raise Exception(f"{contexto}: {r['error'].get('message', str(r['error']))}")

# ── Publicar estático ──────────────────────────────────────────────────────────
def publicar_estatico(post):
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "image_url":    post["url"],
            "caption":      caption_completa(post),
            "access_token": META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container estático")
    container_id = r["id"]
    time.sleep(3)
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "publicar estático")
    return r2["id"]

# ── Publicar reel ──────────────────────────────────────────────────────────────
def publicar_reel(post):
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":   "REELS",
            "video_url":    post["url"],
            "caption":      caption_completa(post),
            "share_to_feed": "true",
            "access_token": META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container reel")
    container_id = r["id"]
    # Polling até FINISHED (máx 3 min)
    print("  Aguardando processamento do reel...")
    for tentativa in range(18):
        time.sleep(10)
        s = requests.get(
            f"https://graph.facebook.com/v19.0/{container_id}",
            params={"fields": "status_code", "access_token": META_TOKEN}
        ).json()
        sc = s.get("status_code", "")
        print(f"  status_code: {sc}")
        if sc == "FINISHED":
            break
        if sc == "ERROR":
            raise Exception("Processamento do reel falhou (ERROR)")
    else:
        raise Exception("Timeout aguardando processamento do reel")
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "publicar reel")
    return r2["id"]

# ── Publicar carrossel ─────────────────────────────────────────────────────────
def publicar_carrossel(post):
    urls = post["url"].split("|")
    child_ids = []
    for url in urls:
        r = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
            params={
                "image_url":        url,
                "is_carousel_item": "true",
                "access_token":     META_TOKEN
            }
        ).json()
        checar_erro(r, f"criar slide {url[:40]}")
        child_ids.append(r["id"])
        time.sleep(1)
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":   "CAROUSEL",
            "children":     ",".join(child_ids),
            "caption":      caption_completa(post),
            "access_token": META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container carrossel")
    container_id = r["id"]
    time.sleep(3)
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "publicar carrossel")
    return r2["id"]

# ── Loop de publicação ─────────────────────────────────────────────────────────
for post in posts:
    print(f"\n→ [{post['tipo']}] linha {post['row']}")
    try:
        if post["tipo"] == "estatico":
            media_id = publicar_estatico(post)
        elif post["tipo"] == "reel":
            media_id = publicar_reel(post)
        elif post["tipo"] == "carrossel":
            media_id = publicar_carrossel(post)
        else:
            raise Exception(f"Tipo desconhecido: {post['tipo']}")
        atualizar_status(post["row"], "publicado")
        print(f"✅ Publicado! media_id={media_id}")
    except Exception as e:
        msg = str(e)[:80]
        atualizar_status(post["row"], f"erro: {msg}")
        print(f"❌ Erro: {e}")
