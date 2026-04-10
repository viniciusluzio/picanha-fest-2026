"""
Picanha Fest 2026 — Agendador Instagram
Roda uma vez para agendar todos os posts com conteúdo.
Usa agendamento nativo da Meta API (published=false + scheduled_publish_time).
Instagram publica automaticamente na hora certa — sem cron de 5 em 5 min.

Fluxo:
  1. Lê planilha, filtra linhas com conteúdo e status "aguardando"
  2. Converte data+hora (BRT) para Unix timestamp
  3. Cria container na Meta com published=false + scheduled_publish_time
  4. Para reels: aguarda FINISHED antes de media_publish
  5. Atualiza status para "agendado" na planilha

Restrições Meta API:
  - Mínimo: 10 minutos no futuro
  - Máximo: 75 dias no futuro
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
BRT            = timezone(timedelta(hours=-3))

# ── Auth Google ────────────────────────────────────────────────────────────────
_sa_raw    = GOOGLE_SA_JSON.strip()
creds_info = json.loads(_sa_raw[:_sa_raw.rfind('}')+1])
creds      = service_account.Credentials.from_service_account_info(
    creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets = build("sheets", "v4", credentials=creds)

# ── Hora atual ─────────────────────────────────────────────────────────────────
now_utc   = datetime.now(timezone.utc)
now_brt   = now_utc.astimezone(BRT)
now_unix  = int(now_utc.timestamp())
MIN_AHEAD = 10 * 60   # 10 minutos em segundos
MAX_AHEAD = 75 * 24 * 3600  # 75 dias em segundos

print(f"[{now_brt.strftime('%Y-%m-%d %H:%M')} BRT] Iniciando agendamento...")

# ── Ler planilha ───────────────────────────────────────────────────────────────
rows = sheets.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range="Página1!A:I"
).execute().get("values", [])

# ── Filtrar posts para agendar ─────────────────────────────────────────────────
posts = []
for i, row in enumerate(rows[1:], start=2):
    while len(row) < 9:
        row.append("")
    id_, arquivo, tipo, data, hora, legenda, hashtags, url_arquivo, status = row

    if status != "aguardando":
        continue
    if not url_arquivo or not legenda:
        continue

    # Converter data+hora BRT → Unix timestamp
    try:
        dt_brt   = datetime.strptime(f"{data} {hora}", "%Y-%m-%d %H:%M").replace(tzinfo=BRT)
        ts_unix  = int(dt_brt.timestamp())
    except Exception:
        continue

    diff = ts_unix - now_unix
    if diff < MIN_AHEAD:
        print(f"  ⚠️  L{i} [{tipo}] {data} {hora} — muito cedo (diff={diff}s < 10min), pulando")
        continue
    if diff > MAX_AHEAD:
        print(f"  ⚠️  L{i} [{tipo}] {data} {hora} — muito longe (diff={diff//86400}d > 75d), pulando")
        continue

    posts.append({
        "row": i, "tipo": tipo, "legenda": legenda,
        "hashtags": hashtags, "url": url_arquivo,
        "ts": ts_unix, "hora_brt": f"{data} {hora}"
    })

print(f"Posts para agendar: {len(posts)}")
if not posts:
    print("Nada a agendar.")
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

def aguardar_container(container_id, max_tentativas=40, intervalo=5):
    """Aguarda container ficar FINISHED (necessário para reels antes de media_publish)."""
    for t in range(max_tentativas):
        time.sleep(intervalo)
        s  = requests.get(
            f"https://graph.facebook.com/v19.0/{container_id}",
            params={"fields": "status_code", "access_token": META_TOKEN}
        ).json()
        sc = s.get("status_code", "")
        print(f"    status_code ({t+1}/{max_tentativas}): {sc}")
        if sc == "FINISHED":
            return
        if sc == "ERROR":
            raise Exception(f"Container {container_id} falhou (ERROR)")
    raise Exception("Timeout aguardando processamento do container")

def media_publish(container_id):
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r, "media_publish")
    return r["id"]

# ── Agendar estático ───────────────────────────────────────────────────────────
def agendar_estatico(post):
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "image_url":              post["url"],
            "caption":                caption_completa(post),
            "published":              "false",
            "scheduled_publish_time": post["ts"],
            "access_token":           META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container estático agendado")
    container_id = r["id"]
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "registrar agendamento estático")
    return r2["id"]

# ── Agendar reel ───────────────────────────────────────────────────────────────
def agendar_reel(post):
    video_url = post["url"].replace("export=view", "export=download")
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":             "REELS",
            "video_url":              video_url,
            "caption":                caption_completa(post),
            "published":              "false",
            "scheduled_publish_time": post["ts"],
            "share_to_feed":          "true",
            "access_token":           META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container reel agendado")
    container_id = r["id"]
    print(f"    Aguardando processamento do vídeo...")
    aguardar_container(container_id)
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "registrar agendamento reel")
    return r2["id"]

# ── Agendar carrossel ──────────────────────────────────────────────────────────
def agendar_carrossel(post):
    urls = post["url"].split("|")
    child_ids = []
    for url in urls:
        r = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
            params={
                "image_url":        url.strip(),
                "is_carousel_item": "true",
                "access_token":     META_TOKEN
            }
        ).json()
        checar_erro(r, f"criar slide {url.strip()[:40]}")
        child_ids.append(r["id"])
        time.sleep(1)

    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":             "CAROUSEL",
            "children":               ",".join(child_ids),
            "caption":                caption_completa(post),
            "published":              "false",
            "scheduled_publish_time": post["ts"],
            "access_token":           META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container carrossel agendado")
    container_id = r["id"]
    r2 = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media_publish",
        params={"creation_id": container_id, "access_token": META_TOKEN}
    ).json()
    checar_erro(r2, "registrar agendamento carrossel")
    return r2["id"]

# ── Loop de agendamento ────────────────────────────────────────────────────────
ok = 0
erros = 0
for post in posts:
    print(f"\n→ [{post['tipo']}] L{post['row']} — {post['hora_brt']}")
    try:
        if post["tipo"] == "estatico":
            media_id = agendar_estatico(post)
        elif post["tipo"] == "reel":
            media_id = agendar_reel(post)
        elif post["tipo"] == "carrossel":
            media_id = agendar_carrossel(post)
        else:
            raise Exception(f"Tipo desconhecido: {post['tipo']}")
        atualizar_status(post["row"], "agendado")
        print(f"  ✅ Agendado! creation_id={media_id}")
        ok += 1
    except Exception as e:
        msg = str(e)[:80]
        atualizar_status(post["row"], f"erro: {msg}")
        print(f"  ❌ Erro: {e}")
        erros += 1

print(f"\n{'='*50}")
print(f"Concluído: {ok} agendados, {erros} erros")
