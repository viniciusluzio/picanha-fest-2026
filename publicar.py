"""
Picanha Fest 2026 — Publicador Instagram
Roda via GitHub Actions a cada 5 minutos.
Lê planilha, publica posts pendentes, atualiza status.

Drive URLs (drive.google.com / drive.usercontent.google.com) são bloqueadas
pela Meta API. Todos os arquivos são baixados e reupados no catbox.moe antes
de enviar para a Meta. Reels também são re-encodados com ffmpeg.
"""
import warnings; warnings.filterwarnings("ignore")
import os, json, time, tempfile, subprocess, requests
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ─────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1Eqzy-Gvp8BbI33vrWMvnMfFHxMRPansmzdJDCRa4Er4"
IG_ACCOUNT_ID  = "17841413527665117"
META_TOKEN     = os.environ["META_TOKEN"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]

# ── Auth Google ────────────────────────────────────────────────────────────────
_sa_raw    = GOOGLE_SA_JSON.strip()
creds_info = json.loads(_sa_raw[:_sa_raw.rfind('}')+1])
creds      = service_account.Credentials.from_service_account_info(
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
    # Publica qualquer post do dia que já passou o horário (janela = dia inteiro)
    # Garante que posts não sejam perdidos mesmo quando o GitHub Actions atrasa horas
    if diff >= 0:
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
    legenda   = post["legenda"].replace("\\n", "\n")
    hashtags  = post["hashtags"].replace("\\n", "\n")
    return legenda + "\n\n" + hashtags

def checar_erro(r, contexto=""):
    if "error" in r:
        raise Exception(f"{contexto}: {r['error'].get('message', str(r['error']))}")

def extrair_file_id(url):
    """Extrai o file ID de uma URL do Google Drive."""
    import re
    m = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', url)
    return m.group(1) if m else None

MANAGEMENT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2MTJlNmQzMmVlMjRjZDFjYWE3Yzg2MTEiLCJtb2R1bGUiOiJtYW5hZ2VtZW50IiwiaWF0IjoxNzU4MTE0ODI5LCJleHAiOjE4MjEyMzAwMjl9.CgI4UQRwoiME8qKyxnCxGeOzNUcFWYMYYhhEAGsKxcw"

def s3_upload(filepath, filename="file", mimetype="application/octet-stream"):
    """Sobe arquivo na management API do Trend → S3 público. Retorna URL direta."""
    print(f"  Subindo {filename} para S3...")
    with open(filepath, "rb") as f:
        resp = requests.post(
            "https://api.clubedapicanhatrend.com.br/v2/management/files/others",
            headers={"Authorization": f"Bearer {MANAGEMENT_TOKEN}"},
            files={"file": (filename, f, mimetype)},
            timeout=180
        )
    resp.raise_for_status()
    url = resp.json()["url"]
    print(f"  URL S3: {url}")
    return url

def baixar_drive(url, suffix):
    """Baixa arquivo do Drive usando drive.usercontent.google.com com confirm=t.
    Este endpoint pula o aviso de vírus para arquivos grandes sem precisar de scraping."""
    file_id = extrair_file_id(url)
    if not file_id:
        raise Exception(f"Não foi possível extrair file_id da URL: {url}")

    # Usar o endpoint usercontent que aceita confirm=t direto (sem scraping de HTML)
    dl_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t&authuser=0"
    print(f"  Baixando do Drive (id={file_id})...")

    session = requests.Session()
    resp = session.get(dl_url, stream=True, timeout=300, allow_redirects=True)
    resp.raise_for_status()

    # Verificar se realmente veio um arquivo e não HTML
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" in content_type:
        raise Exception(f"Drive retornou HTML em vez do arquivo (file_id={file_id}). Verifique permissões.")

    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    for chunk in resp.iter_content(chunk_size=65536):
        tmp.write(chunk)
    tmp.close()
    size_mb = os.path.getsize(tmp.name) // 1024 // 1024
    print(f"  Download OK: {size_mb}MB")
    if size_mb == 0:
        raise Exception("Arquivo baixado está vazio — possível erro de permissão no Drive")
    return tmp.name

# ── Preparar imagem ────────────────────────────────────────────────────────────
def preparar_imagem(url_drive):
    """Baixa imagem do Drive e sobe no catbox.moe (Drive bloqueado pela Meta)."""
    tmp = baixar_drive(url_drive, "_img.jpg")
    try:
        return s3_upload(tmp, "image.jpg", "image/jpeg")
    finally:
        os.unlink(tmp)

# ── Preparar vídeo reel ────────────────────────────────────────────────────────
def preparar_video_reel(url_drive):
    """
    Baixa vídeo do Drive, re-encoda (H.264 CFR 30fps, CRF 23, AAC 128k)
    e sobe no catbox.moe. Necessário porque:
    - Drive URLs bloqueadas pela Meta
    - Bitrate alto / ausência de B-frames causam ERROR no processador da Meta
    """
    tmp_in = baixar_drive(url_drive, "_orig.mp4")
    tmp_out = tmp_in.replace("_orig.mp4", "_enc.mp4")
    try:
        print("  Re-encodando (H.264 CFR 30fps, AAC 128k)...")
        subprocess.run([
            "ffmpeg", "-y", "-i", tmp_in,
            "-c:v", "libx264", "-profile:v", "high",
            "-crf", "23", "-preset", "medium",
            "-r", "30", "-g", "60", "-keyint_min", "60", "-sc_threshold", "0",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-movflags", "+faststart",
            tmp_out
        ], check=True, capture_output=True)
        return s3_upload(tmp_out, "reel.mp4", "video/mp4")
    finally:
        os.unlink(tmp_in)
        if os.path.exists(tmp_out):
            os.unlink(tmp_out)

# ── Publicar estático ──────────────────────────────────────────────────────────
def publicar_estatico(post):
    image_url = preparar_imagem(post["url"])
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "image_url":    image_url,
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
    video_url = preparar_video_reel(post["url"])
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
        params={
            "media_type":    "REELS",
            "video_url":     video_url,
            "caption":       caption_completa(post),
            "share_to_feed": "true",
            "access_token":  META_TOKEN
        }
    ).json()
    checar_erro(r, "criar container reel")
    container_id = r["id"]
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
        image_url = preparar_imagem(url.strip())
        r = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_ACCOUNT_ID}/media",
            params={
                "image_url":        image_url,
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
