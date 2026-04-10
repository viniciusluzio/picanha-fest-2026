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
# Extrai só o JSON (ignora lixo após o fechamento do objeto)
_sa_raw = GOOGLE_SA_JSON.strip()
creds_info = json.loads(_sa_raw[:_sa_raw.rfind('}')+1])
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
def drive_url(url):
    """Garante que URLs do Drive usam export=download (servido direto, sem HTML)."""
    return url.replace("export=view", "export=download")

def publicar_estatico(post):
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "image_url":    drive_url(post["url"]),
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

# ── Preparar vídeo ─────────────────────────────────────────────────────────────
def preparar_video_reel(url_drive):
    """
    Baixa vídeo do Drive, re-encoda com specs corretos para Meta Reels
    (H.264 CFR 30fps, CRF 23, AAC 128k) e sobe num host temporário público.
    Necessário porque bitrate alto / B-frames ausentes causam ERROR no processador da Meta.
    """
    import subprocess, tempfile, os

    # 1. Download do Drive
    print("  Baixando vídeo do Drive...")
    tmp_in = tempfile.NamedTemporaryFile(suffix="_orig.mp4", delete=False)
    with requests.get(url_drive, stream=True, timeout=120) as resp:
        for chunk in resp.iter_content(chunk_size=65536):
            tmp_in.write(chunk)
    tmp_in.close()

    # 2. Re-encoda: CFR 30fps, CRF 23, sem B-frames implícitos, AAC 128k
    tmp_out = tmp_in.name.replace("_orig.mp4", "_enc.mp4")
    print("  Re-encodando para Meta Reels (H.264 CFR 30fps, AAC 128k)...")
    subprocess.run([
        "ffmpeg", "-y", "-i", tmp_in.name,
        "-c:v", "libx264", "-profile:v", "high",
        "-crf", "23", "-preset", "medium",
        "-r", "30", "-g", "60", "-keyint_min", "60", "-sc_threshold", "0",
        "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
        "-movflags", "+faststart",
        tmp_out
    ], check=True, capture_output=True)

    # 3. Sobe no 0x0.st (host temporário, sem auth, URL direta)
    print("  Subindo vídeo re-encodado para host público...")
    with open(tmp_out, "rb") as f:
        resp = requests.post("https://0x0.st", files={"file": f}, timeout=120)
    resp.raise_for_status()
    url_final = resp.text.strip()
    print(f"  URL pública: {url_final}")

    # Limpa temporários
    os.unlink(tmp_in.name)
    os.unlink(tmp_out)

    return url_final

# ── Publicar reel ──────────────────────────────────────────────────────────────
def publicar_reel(post):
    video_url = preparar_video_reel(drive_url(post["url"]))
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":   "REELS",
            "video_url":    video_url,
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
                "image_url":        drive_url(url.strip()),
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
