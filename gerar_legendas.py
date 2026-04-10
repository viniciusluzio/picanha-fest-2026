"""
Picanha Fest 2026 — Gerador de Legendas
Roda uma vez quando os arquivos estiverem no Drive.
Lê cada criativo, gera legenda via Claude Vision e preenche a planilha.
"""
import warnings; warnings.filterwarnings("ignore")
import os, io, json, time, base64, requests
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ─────────────────────────────────────────────────────────────────────
SPREADSHEET_ID  = "1Eqzy-Gvp8BbI33vrWMvnMfFHxMRPansmzdJDCRa4Er4"
DRIVE_FOLDER_ID = "1BFgBo20Jp-Nhqe6JBFrVuKBpXvlIjekD"
CLAUDE_API_KEY  = os.environ["CLAUDE_API_KEY"]
CREDENTIALS     = json.loads(os.environ["GOOGLE_SA_JSON"])

CONTEXT = """Você é o redator oficial do Instagram do Picanha Fest 2026 (@picanhafestoficial).

EVENTO: Picanha Fest — 6ª edição | 25 de abril de 2026, 14h às 22h | Paioça do Caboclo, Joaquim Egídio, Campinas-SP
Open food + open beer | +2 toneladas de carne | 14 estações de churrasco
Shows: Otávio & Raphael, Junior Freitas, Sem Tempo, Resenha do Marcílio, DJs
Ingresso 2º lote: R$ 350 | trendsuperapp.com.br/loja/picanha-fest

ESTILO DA CONTA (baseado nos últimos 40 posts reais):
- Tom: animado, informal, entusiasta. Fala diretamente com o seguidor ("você", "vocês", "quem aí")
- 1ª frase sempre é um gancho forte com emoji. Ex: "Bora curtir a vida no Picanha Fest?! 🔥👀"
- Emojis entremeados no texto, não só no final
- Frases curtas, exclamações frequentes, às vezes "!!" ou "!!!"
- Perguntas retóricas ao público: "E aí? Bora?!", "Quem aí também está contando os dias?", "Estão preparados?"
- Superlativos e hipérboles: "inesquecível", "incomparável", "de respeito", "de milhões", "absurdo de sabor"
- "Bora" aparece muito. Também: "né?!", "hem?!", "galera"
- CTA OBRIGATÓRIO ao final: "🎟️ Ingressos através do link na bio!" (sempre esta frase exata)
- Comprimento: posts de evento = 3-6 linhas. Posts de patrocinadores = até 8 linhas
- NÃO usar hashtags no corpo — vão em campo separado

EXEMPLOS REAIS DE LEGENDAS DA CONTA:
- "Bora curtir a vida no Picanha Fest?! 🔥👀 Tá chegaaaando, hem?! 😏
8 horas de festa 💃🏻 Open beer 🍻 Open food com mais de 14 estações de churrasco 🥩 Música ao vivo 🎶 Em meio à natureza 🍃
Tá esperando o que para garantir o seu ingresso? 🤔
🎟️ Ingressos através do link na bio!"
- "FALTA 1 MÊS!!🔥🔥🔥
O Picanha Fest está chegando e essa 6ª edição promete ser inesquecível 🚀 Você vem, né?! 😏
🎟️ Ingressos voando através do link na bio."
- "Experiência única que só o Picanha Fest proporciona para você! 🔥🚀
⏰ 8 horas de festa com churrasco e cerveja à vontade 🥩🍻 Pagode, sertanejo e DJs 🎶 Ambiente em meio à natureza 🍃
🗓️ 25 de abril | 📍Paioça do Caboclo, Joaquim Egídio
🎟️ Ingressos voando através do link na bio 💨"
"""

HASHTAGS = "#PicanhaFest2026 #PicanhaFest #Churrasco #Campinas #OpenFood #OpenBeer #ChurrascoDeRespeito #Picanha #FestaCampinas #Churrasqueiro #VidaBoaChurrasco #EventoCampinas"

# ── Google APIs ────────────────────────────────────────────────────────────────
scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]
creds   = service_account.Credentials.from_service_account_info(CREDENTIALS, scopes=scopes)
drive   = build("drive", "v3", credentials=creds)
sheets  = build("sheets", "v4", credentials=creds)

# ── Funções auxiliares ─────────────────────────────────────────────────────────

def listar_arquivos_pasta(folder_id):
    """Retorna lista de (nome, id, mimeType) na pasta."""
    result = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        orderBy="name"
    ).execute()
    return result.get("files", [])

def url_publica(file_id):
    """URL direta do Drive para o arquivo."""
    return f"https://drive.google.com/uc?export=view&id={file_id}"

def baixar_imagem_bytes(file_id):
    """Baixa imagem do Drive e retorna bytes brutos."""
    from googleapiclient.http import MediaIoBaseDownload
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue()

# Dimensões padrão Instagram feed
CROP_DIMS = {
    "estatico":  (1080, 1350),  # 4:5 portrait
    "carrossel": (1080, 1080),  # 1:1 square (todos os slides mesma proporção)
}

def crop_instagram(dados, tipo):
    """Center-crop e redimensiona para padrão Instagram. Retorna bytes JPEG."""
    alvo_w, alvo_h = CROP_DIMS.get(tipo, (1080, 1350))
    img = Image.open(io.BytesIO(dados)).convert("RGB")
    w, h = img.size
    alvo_ratio = alvo_w / alvo_h
    img_ratio  = w / h

    # Já está no tamanho certo (tolerância 1%)
    if abs(img_ratio - alvo_ratio) < 0.01 and w == alvo_w and h == alvo_h:
        saida = io.BytesIO()
        img.save(saida, format="JPEG", quality=92, optimize=True)
        return saida.getvalue()

    # Crop centrado
    if img_ratio > alvo_ratio:
        # Imagem mais larga → cortar laterais
        new_w = int(h * alvo_ratio)
        left = (w - new_w) // 2
        img = img.crop((left, 0, left + new_w, h))
    else:
        # Imagem mais alta → cortar topo/baixo
        new_h = int(w / alvo_ratio)
        top = (h - new_h) // 2
        img = img.crop((0, top, w, top + new_h))

    img = img.resize((alvo_w, alvo_h), Image.LANCZOS)
    saida = io.BytesIO()
    img.save(saida, format="JPEG", quality=92, optimize=True)
    return saida.getvalue()

def atualizar_arquivo_drive(file_id, dados_bytes):
    """Atualiza conteúdo de um arquivo existente no Drive (não cria novo — evita quota de service account).
    O arquivo permanece com o mesmo ID/URL, dono e permissões."""
    from googleapiclient.http import MediaIoBaseUpload
    media = MediaIoBaseUpload(io.BytesIO(dados_bytes), mimetype="image/jpeg", resumable=False)
    drive.files().update(fileId=file_id, media_body=media).execute()
    return file_id

def processar_imagem(file_id, parent_id, nome, tipo):
    """Baixa, verifica dimensões, aplica crop se necessário e retorna (url, nome_final, dados_para_claude).
    Se a imagem precisar de crop, atualiza o arquivo existente no Drive (mesmo ID/URL)."""
    dados = baixar_imagem_bytes(file_id)
    alvo_w, alvo_h = CROP_DIMS.get(tipo, (1080, 1350))

    img = Image.open(io.BytesIO(dados))
    w, h = img.size
    precisa_crop = not (w == alvo_w and h == alvo_h)

    if precisa_crop:
        dados_crop = crop_instagram(dados, tipo)
        atualizar_arquivo_drive(file_id, dados_crop)
        print(f"    📐 Crop: {w}x{h} → {alvo_w}x{alvo_h} (arquivo atualizado no Drive)")
        return url_publica(file_id), nome, dados_crop
    else:
        return url_publica(file_id), nome, dados

def baixar_imagem_base64(file_id):
    """Baixa imagem do Drive, comprime se necessário (<5MB) e retorna base64."""
    from googleapiclient.http import MediaIoBaseDownload
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    dados = buf.getvalue()

    # Se maior que 4MB, redimensiona com Pillow
    if len(dados) > 4 * 1024 * 1024:
        img = Image.open(io.BytesIO(dados))
        img = img.convert("RGB")
        # Reduz resolução mantendo proporção até caber em 4MB
        saida = io.BytesIO()
        qualidade = 85
        max_dim = 1920
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        img.save(saida, format="JPEG", quality=qualidade, optimize=True)
        # Se ainda grande, reduz mais
        while saida.tell() > 4 * 1024 * 1024 and qualidade > 40:
            qualidade -= 10
            saida = io.BytesIO()
            img.save(saida, format="JPEG", quality=qualidade, optimize=True)
        dados = saida.getvalue()

    return base64.standard_b64encode(dados).decode("utf-8")

def gerar_legenda_imagem(file_id, nome, tipo, dados_bytes=None):
    """Chama Claude Vision para gerar legenda baseada na imagem.
    Se dados_bytes fornecido, usa diretamente (evita re-download)."""
    if dados_bytes is not None:
        # Comprime se necessário
        if len(dados_bytes) > 4 * 1024 * 1024:
            img = Image.open(io.BytesIO(dados_bytes)).convert("RGB")
            saida = io.BytesIO()
            qualidade = 85
            img.thumbnail((1920, 1920), Image.LANCZOS)
            img.save(saida, format="JPEG", quality=qualidade, optimize=True)
            while saida.tell() > 4 * 1024 * 1024 and qualidade > 40:
                qualidade -= 10
                saida = io.BytesIO()
                img.save(saida, format="JPEG", quality=qualidade, optimize=True)
            dados_bytes = saida.getvalue()
        img_b64 = base64.standard_b64encode(dados_bytes).decode("utf-8")
    else:
        img_b64 = baixar_imagem_base64(file_id)
    ext = nome.split(".")[-1].lower()
    mime = "image/png" if ext == "png" else "image/jpeg"

    prompt = (
        f"Você é o redator do @picanhafestoficial. Analise este criativo (tipo: {tipo}) e gere uma legenda no estilo real da conta.\n\n"
        f"{CONTEXT}\n\n"
        "Responda EXATAMENTE neste formato (sem mais nada):\n"
        "LEGENDA: [legenda completa aqui, com quebras de linha naturais, no estilo real da conta]\n"
        "HASHTAGS: [hashtags separadas por espaço, começando com #PicanhaFest2026]\n\n"
        "IMPORTANTE: A legenda deve soar 100% humana, no estilo real da conta. Sempre termine com o CTA."
    )

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": "claude-opus-4-6",
            "max_tokens": 500,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": mime, "data": img_b64}},
                    {"type": "text", "text": prompt}
                ]
            }]
        }
    )
    text = response.json()["content"][0]["text"]
    legenda, hashtags = "", HASHTAGS
    for line in text.split("\n"):
        if line.startswith("LEGENDA:"):
            legenda = line.replace("LEGENDA:", "").strip()
        elif line.startswith("HASHTAGS:"):
            hashtags = line.replace("HASHTAGS:", "").strip()
    return legenda, hashtags

def gerar_legenda_video(nome):
    """Gera legenda para vídeo/reel baseado no nome do arquivo."""
    titulo = nome.replace(".mp4","").replace(".mov","").replace(".MP4","").replace("-"," ").replace("_"," ")
    prompt = (
        f"Você é o redator do @picanhafestoficial. Gere uma legenda para um Reel no estilo real da conta.\n\n"
        f"{CONTEXT}\n\n"
        f"Tema do vídeo (baseado no nome do arquivo): {titulo}\n\n"
        "Responda EXATAMENTE neste formato (sem mais nada):\n"
        "LEGENDA: [legenda completa aqui, com quebras de linha naturais, no estilo real da conta]\n"
        "HASHTAGS: [hashtags separadas por espaço, começando com #PicanhaFest2026]\n\n"
        "IMPORTANTE: A legenda deve soar 100% humana, no estilo real da conta. Sempre termine com o CTA."
    )
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": "claude-opus-4-6",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}]
        }
    )
    text = response.json()["content"][0]["text"]
    legenda, hashtags = "", HASHTAGS
    for line in text.split("\n"):
        if line.startswith("LEGENDA:"):
            legenda = line.replace("LEGENDA:", "").strip()
        elif line.startswith("HASHTAGS:"):
            hashtags = line.replace("HASHTAGS:", "").strip()
    return legenda, hashtags

# ── Ler planilha e montar mapa de slots vagos por tipo ────────────────────────

print("Lendo planilha...")
result = sheets.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range="Página1!A:I"
).execute()
rows = result.get("values", [])
header = rows[0]  # id, arquivo, tipo, data, hora, legenda, hashtags, url_arquivo, status

# Índices de slots vagos por tipo — guarda também tipo/data/hora para reescrever corretamente
slots_vagos = {"estatico": [], "reel": [], "carrossel": []}
for i, row in enumerate(rows[1:], start=2):
    while len(row) < 9:
        row.append("")
    id_, arquivo, tipo, data, hora, legenda, hashtags, url_arquivo, status = row
    if status == "aguardando" and not arquivo:
        if tipo in slots_vagos:
            slots_vagos[tipo].append({
                "row": i, "tipo": tipo, "data": data, "hora": hora
            })

print(f"Slots vagos — estático: {len(slots_vagos['estatico'])} | reel: {len(slots_vagos['reel'])} | carrossel: {len(slots_vagos['carrossel'])}")


# ── Listar subpastas do Drive ─────────────────────────────────────────────────
print("\nListando arquivos no Drive...")

def encontrar_pasta(nome_busca, parent_id):
    """Encontra ID de subpasta pelo nome (case-insensitive)."""
    itens = listar_arquivos_pasta(parent_id)
    for item in itens:
        if item["mimeType"] == "application/vnd.google-apps.folder" and item["name"].upper() == nome_busca.upper():
            return item["id"]
    return None

# Encontrar as 3 subpastas principais
id_estaticos  = encontrar_pasta("ESTATICOS",  DRIVE_FOLDER_ID)
id_reels      = encontrar_pasta("REELS",      DRIVE_FOLDER_ID)
id_carrosseis = encontrar_pasta("CARROSSEIS", DRIVE_FOLDER_ID)

# Listar arquivos dentro de cada subpasta
estaticos = []
if id_estaticos:
    tudo = listar_arquivos_pasta(id_estaticos)
    estaticos = [f for f in tudo if f["mimeType"].startswith("image/")]

reels = []
if id_reels:
    tudo = listar_arquivos_pasta(id_reels)
    reels = [f for f in tudo if f["mimeType"].startswith("video/")]

# Carrosséis: cada subpasta dentro de CARROSSEIS é um carrossel
carrosseis = []
if id_carrosseis:
    subpastas = [f for f in listar_arquivos_pasta(id_carrosseis)
                 if f["mimeType"] == "application/vnd.google-apps.folder"]
    for pasta in subpastas:
        slides = [s for s in listar_arquivos_pasta(pasta["id"]) if s["mimeType"].startswith("image/")]
        if slides:
            carrosseis.append({"nome": pasta["name"], "id": pasta["id"], "slides": slides})

print(f"Encontrados — estáticos: {len(estaticos)} | reels: {len(reels)} | carrosséis: {len(carrosseis)}")

# ── Processar e preencher planilha ────────────────────────────────────────────
updates = []

def registrar_update(slot, nome, legenda, hashtags, url):
    # Escreve todas as 8 colunas (B a I) sem deslocar nada
    # B=arquivo | C=tipo | D=data | E=hora | F=legenda | G=hashtags | H=url_arquivo | I=status
    row_index = slot["row"]
    updates.append({
        "range": f"Página1!B{row_index}:I{row_index}",
        "values": [[
            nome,           # B — arquivo
            slot["tipo"],   # C — tipo (preserva o original)
            slot["data"],   # D — data (preserva o original)
            slot["hora"],   # E — hora (preserva o original)
            legenda,        # F — legenda
            hashtags,       # G — hashtags
            url,            # H — url_arquivo
            "pendente"      # I — status
        ]]
    })
    print(f"  ✅ [{slot['tipo']}] linha {row_index} ({slot['data']} {slot['hora']}): {nome}")

# Estáticos
print(f"\nProcessando {len(estaticos)} estáticos...")
for i, arq in enumerate(estaticos):
    if i >= len(slots_vagos["estatico"]):
        print(f"  ⚠️  Sem slots vagos para estático: {arq['name']}")
        break
    print(f"  Processando {arq['name']}...")
    try:
        url, nome_final, dados = processar_imagem(arq["id"], id_estaticos, arq["name"], "estatico")
        legenda, hashtags = gerar_legenda_imagem(arq["id"], nome_final, "post estático", dados_bytes=dados)
        registrar_update(slots_vagos["estatico"][i], nome_final, legenda, hashtags, url)
        time.sleep(1)
    except Exception as e:
        print(f"  ❌ Erro: {e}")

# Reels
print(f"\nProcessando {len(reels)} reels...")
for i, arq in enumerate(reels):
    if i >= len(slots_vagos["reel"]):
        print(f"  ⚠️  Sem slots vagos para reel: {arq['name']}")
        break
    print(f"  Gerando legenda para {arq['name']}...")
    try:
        legenda, hashtags = gerar_legenda_video(arq["name"])
        url = url_publica(arq["id"])
        registrar_update(slots_vagos["reel"][i], arq["name"], legenda, hashtags, url)
        time.sleep(1)
    except Exception as e:
        print(f"  ❌ Erro: {e}")

# Carrosséis
print(f"\nProcessando {len(carrosseis)} carrosséis...")
for i, car in enumerate(carrosseis):
    if i >= len(slots_vagos["carrossel"]):
        print(f"  ⚠️  Sem slots vagos para carrossel: {car['nome']}")
        break
    print(f"  Processando carrossel {car['nome']}...")
    try:
        # Crop cada slide e coletar URLs
        urls_slides = []
        dados_primeiro = None
        nome_primeiro  = None
        for j, slide in enumerate(car["slides"]):
            url_slide, nome_slide, dados_slide = processar_imagem(
                slide["id"], car["id"], slide["name"], "carrossel"
            )
            urls_slides.append(url_slide)
            if j == 0:
                dados_primeiro = dados_slide
                nome_primeiro  = nome_slide

        legenda, hashtags = gerar_legenda_imagem(
            car["slides"][0]["id"], nome_primeiro, "carrossel", dados_bytes=dados_primeiro
        )
        urls = "|".join(urls_slides)
        registrar_update(slots_vagos["carrossel"][i], car["nome"], legenda, hashtags, urls)
        time.sleep(1)
    except Exception as e:
        print(f"  ❌ Erro: {e}")

# ── Enviar updates em batch ───────────────────────────────────────────────────
if updates:
    print(f"\nSalvando {len(updates)} linhas na planilha...")
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": updates}
    ).execute()
    print(f"✅ Planilha atualizada com {len(updates)} posts prontos para publicar.")
else:
    print("\n⚠️  Nenhum arquivo encontrado no Drive. Suba os arquivos nas pastas corretas e rode novamente.")

print("\nDone.")
