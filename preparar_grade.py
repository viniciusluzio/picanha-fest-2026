"""
Picanha Fest 2026 — Preparar Grade de Publicação
Cria as 396 linhas na planilha com datas/horários/tipos.
Roda UMA vez antes do gerar_legendas.py.

Grade: 13/04 a 24/04 | 08h a 18h | 3 posts/slot (reel, carrossel, estatico)
"""
import warnings; warnings.filterwarnings("ignore")
import os, json
from datetime import date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1Eqzy-Gvp8BbI33vrWMvnMfFHxMRPansmzdJDCRa4Er4"
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]

_sa_raw    = GOOGLE_SA_JSON.strip()
creds_info = json.loads(_sa_raw[:_sa_raw.rfind('}')+1])
creds      = service_account.Credentials.from_service_account_info(
    creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets = build("sheets", "v4", credentials=creds)

# ── Grade ──────────────────────────────────────────────────────────────────────
INICIO  = date(2026, 4, 13)
FIM     = date(2026, 4, 24)
HORAS   = list(range(8, 19))          # 8, 9, ..., 18  →  11 slots/dia
MINUTOS = {"reel": "00", "carrossel": "02", "estatico": "04"}

linhas = [["id", "arquivo", "tipo", "data", "hora", "legenda", "hashtags", "url_arquivo", "status"]]

seq = 1
dia = INICIO
while dia <= FIM:
    data_str = dia.strftime("%Y-%m-%d")
    for hora in HORAS:
        for tipo, minuto in MINUTOS.items():
            linhas.append([
                f"{seq:03d}",       # A — id
                "",                 # B — arquivo  (gerar_legendas vai preencher)
                tipo,               # C — tipo
                data_str,           # D — data
                f"{hora:02d}:{minuto}",  # E — hora
                "",                 # F — legenda  (idem)
                "",                 # G — hashtags (idem)
                "",                 # H — url_arquivo (idem)
                "aguardando"        # I — status
            ])
            seq += 1
    dia += timedelta(days=1)

total = len(linhas) - 1  # sem header
print(f"Grade gerada: {total} posts ({INICIO} a {FIM})")
print(f"  Reels:      {total // 3}")
print(f"  Carrosséis: {total // 3}")
print(f"  Estáticos:  {total // 3}")

# ── Limpar planilha e escrever tudo ───────────────────────────────────────────
print("\nLimpando planilha e escrevendo grade...")

# Limpa a aba inteira
sheets.spreadsheets().values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range="Página1"
).execute()

# Escreve header + 396 linhas
sheets.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    range="Página1!A1",
    valueInputOption="RAW",
    body={"values": linhas}
).execute()

print(f"✅ Planilha pronta com {total} slots aguardando conteúdo.")
print("\nPróximo passo: rode gerar_legendas.py para preencher arquivos e legendas.")
