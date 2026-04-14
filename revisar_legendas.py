"""
Picanha Fest 2026 — Revisão de Legendas
Relê todas as 144 linhas da planilha, reescreve cada legenda com prompt
melhorado (sem @, mais elaboradas, infos corretas de patrocinadores)
e atualiza a planilha em batch.
"""
import warnings; warnings.filterwarnings("ignore")
import os, json, re, time, requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1Eqzy-Gvp8BbI33vrWMvnMfFHxMRPansmzdJDCRa4Er4"
CLAUDE_API_KEY  = os.environ["CLAUDE_API_KEY"]
CREDENTIALS     = json.loads(os.environ["GOOGLE_SA_JSON"])

# ── Google Sheets ───────────────────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_info(
    CREDENTIALS, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets = build("sheets", "v4", credentials=creds)

# ── Contexto base da conta ──────────────────────────────────────────────────────
CONTEXT = """Você é o redator oficial do Instagram do Picanha Fest 2026.

EVENTO: Picanha Fest — 6ª edição | 25 de abril de 2026, 14h às 22h | Paioça do Caboclo, Joaquim Egídio, Campinas-SP
Open food + open beer | +2 toneladas de carne | 14+ estações de churrasco
Shows: Otávio & Raphael, Junior Freitas, Sem Tempo, Resenha do Marcílio, DJs
Ingresso: trendsuperapp.com.br/loja/picanha-fest

ESTILO DA CONTA:
- Tom: animado, informal, entusiasta. Fala diretamente com o seguidor ("você", "vocês", "quem aí")
- 1ª frase sempre é gancho forte com emoji
- Emojis entremeados no texto, não só no final
- Frases curtas, exclamações, perguntas retóricas ao público
- Superlativos: "inesquecível", "incomparável", "de respeito", "absurdo de sabor"
- "Bora" aparece muito. Também: "né?!", "hem?!", "galera"
- CTA OBRIGATÓRIO ao final: "🎟️ Ingressos através do link na bio!"
- NÃO usar hashtags no corpo — vão em campo separado
- NUNCA usar @ (arroba) em NENHUMA menção — nem de marcas, nem de pessoas

EXEMPLOS REAIS PUBLICADOS:
- "Bora curtir a vida no Picanha Fest?! 🔥👀 Tá chegaaaando, hem?! 😏\\n8 horas de festa 💃🏻 Open beer 🍻 Open food com mais de 14 estações de churrasco 🥩 Música ao vivo 🎶 Em meio à natureza 🍃\\nTá esperando o que para garantir o seu ingresso? 🤔\\n🎟️ Ingressos através do link na bio!"
- "FALTA 1 MÊS!!🔥🔥🔥\\nO Picanha Fest está chegando e essa 6ª edição promete ser inesquecível 🚀 Você vem, né?! 😏\\n🎟️ Ingressos voando através do link na bio."
- "Experiência única que só o Picanha Fest proporciona para você! 🔥🚀\\n⏰ 8 horas de festa com churrasco e cerveja à vontade 🥩🍻 Pagode, sertanejo e DJs 🎶 Ambiente em meio à natureza 🍃\\n🗓️ 25 de abril | 📍Paioça do Caboclo, Joaquim Egídio\\n🎟️ Ingressos voando através do link na bio 💨"
"""

HASHTAGS = "#PicanhaFest2026 #PicanhaFest #Churrasco #Campinas #OpenFood #OpenBeer #ChurrascoDeRespeito #Picanha #FestaCampinas #Churrasqueiro #VidaBoaChurrasco #EventoCampinas"

# ── Informações dos patrocinadores (baseadas nos posts já publicados) ───────────
# Cada entrada: (padrões de arquivo, nome da marca, descrição, legenda de referência)
SPONSORS = [
    (
        ["CRIATIVO_", "IMPERIO", "IMPERIO_V", "IMPERIO"],
        "Cerveja Império",
        "Puro malte e autenticidade em uma cerveja pilsen com ingredientes importados. Quatro opções para as 8 horas de festa: Helles, Gold, Ultra e Zero. Parceria de sucesso para quem busca uma cerveja legítima.",
        'Produzida por quem realmente entende de cerveja, a Império trará ainda mais sabor para a 6ª edição do Picanha Fest 🍻 😎\n\n🍺 A combinação perfeita entre o puro malte e a autenticidade da cerveja pilsen com ingredientes importados em quatro opções disponíveis durante as 8 horas de festa: Helles, Gold, Ultra e Zero 😍\n\n💥 Uma parceria de sucesso para entregar tudo o que você procura em uma cerveja legítima! 🚀\n\nBora?! 👀 Ingressos disponíveis através do link na bio 🎟️'
    ),
    (
        ["SPECIALLI", "PF_SPECIALLI"],
        "Specialli (Charcutaria)",
        "Charcutaria artesanal com quase 20 anos de tradição e técnica. Embutidos produzidos com carnes selecionadas e condimentação única. Após marcar presença na última edição, retornam com ainda mais inovação e exclusividade em cada detalhe.",
        'Com a tradição milenar da charcutaria e um compromisso inegociável com a qualidade, a Specialli está de volta ao Picanha Fest 2026 🌭🔥\n\nApós marcar presença na última edição, eles retornam trazendo toda a sua técnica, inovação e exclusividade em cada detalhe dos seus embutidos 😋\n\nDesenvolvida por quem vive a charcutaria há quase 20 anos, a Specialli combina carnes selecionadas com uma condimentação única, elevando o sabor a outro nível 🥩✨\n\n🐷 Tradição, técnica e paixão em cada preparo. Isso é Specialli no Picanha Fest!\n\n🎟️ Garanta seu ingresso no link da bio e venha viver essa experiência com a gente 😉'
    ),
    (
        ["_51", "PICANHA FEST_51"],
        "Cachaça 51",
        "Referência em tradição, qualidade e sabor brasileiro. A marca mais brasileira das cachaças, feita para ser apreciada em doses ou drinks. A opção perfeita para acompanhar um bom churrasco.",
        'Sempre uma boa ideia! 😏 Referência em tradição, qualidade e sabor brasileiro, a 51 estará com a gente em mais uma edição do Picanha Fest 💥😉\n\n🍹 Feita para ser apreciada em doses ou drinks, a marca mais brasileira das cachaças é a opção perfeita para acompanhar um bom churrasco 🥩🥓\n\n🥃 Um brinde a essa parceria incrível!! 🥰'
    ),
    (
        ["BONFA", "BONFÁ"],
        "Bonfá Pães Artesanais",
        "Desde 2018 produzindo pães artesanais de alta qualidade com ingredientes selecionados. Chegam ao Picanha Fest 2026 para elevar ainda mais o nível do cardápio.",
        'Desde 2018 produzindo pães artesanais de alta qualidade, a Bonfá Pães Artesanais chega ao Picanha Fest 2026 para elevar ainda mais o nível do nosso cardápio 🍔😋\n\n👊 Parceria de sucesso com pães extremamente saborosos, produzidos com ingredientes selecionados 💛\n\nEstão preparados para provar? 😉 Garanta seu ingresso através do link na bio 🎟️'
    ),
    (
        ["CAROLINA BLACK"],
        "Carolina Black",
        "Carnes desenvolvidas nos mínimos detalhes para surpreender com uma experiência inesquecível. Cortes macios e muito saborosos, refletindo comprometimento com qualidade e padronização do cardápio.",
        'Com carnes desenvolvidas nos mínimos detalhes para surpreender e oferecer uma experiência inesquecível aos clientes, a Carolina Black garantirá cortes macios e muito saborosos ao público do Picanha Fest! 🥩🔥😋\n\n😍 Uma parceria de sucesso que reflete nosso comprometimento com a qualidade e a padronização do cardápio!\n\n🎟️ Acesse o link na bio e garanta o seu lugar!'
    ),
    (
        ["CASTAS"],
        "Castas (Vinhos)",
        "Desde 2018 no mercado com a missão de descomplicar o consumo de vinhos no Brasil. Venderá rótulos selecionados na 6ª edição, trazendo mais opções ao público do evento.",
        'Para os amantes de vinhos, contaremos com a parceria da Castas, que venderá rótulos selecionados na 6ª edição do Picanha Fest 🍷🥩\n\n🍇 Desde 2018 no mercado, a empresa nasceu com a missão de descomplicar o consumo de vinhos no país e chega para trazer ainda mais opções ao público do nosso evento 🥰\n\nBora?! 😎 Acesse o link na bio e garanta seu ingresso! 🎟️'
    ),
    (
        ["COMESUL"],
        "Comesul",
        "Reconhecida em todo o país pela alta qualidade na criação de bovinos. Parceria de milhões para os amantes de um bom churrasco, com produtos de qualidade confirmada.",
        'Reconhecida em todo o país devido à sua alta qualidade na criação de bovinos, a Comesul não poderia ficar de fora da 6ª edição do Picanha Fest 🥩🔥\n\n👀 Estão preparados para saborear os deliciosos produtos de qualidade da marca no dia 25 de abril? 🤤🫶\n\nParceria de milhões para os amantes de um bom churrasco! 😍🚀'
    ),
    (
        ["EXXUTO"],
        "Exxuto",
        "Parceiro oficial do Picanha Fest 2026, chegando com tudo para somar ainda mais qualidade e experiência ao nosso evento.",
        None  # sem referência publicada — usar apenas a descrição
    ),
    (
        ["FARTURA"],
        "Hortifruti Fartura",
        "Referência em qualidade, variedade e frescor dos produtos. Com seleção cuidadosa, garantem ingredientes à altura de um dos maiores festivais de churrasco do Brasil. Do campo à mesa, com qualidade e confiança.",
        'Reconhecido pela qualidade, variedade e frescor dos seus produtos, o Hortifruti Fartura chega junto ao Picanha Fest 2026 para elevar ainda mais o nível da experiência 🥬🍅🔥\n\nCom seleção cuidadosa e aquele padrão que faz diferença em cada detalhe, eles garantem ingredientes à altura de um dos maiores festivais de churrasco do Brasil 😋\n\n🌿 Do campo à mesa, com qualidade e confiança. Isso também é Picanha Fest!\n\n🎟️ Garanta seu ingresso no link da bio e venha viver essa experiência com a gente 😉'
    ),
    (
        ["FLYING HORSE"],
        "Flying Horse",
        "Energia e presença marcante. De volta ao Picanha Fest 2026 após agitar a edição de 2025. Energético para acompanhar 8 horas de Open Food e Open Beer sem perder o ritmo.",
        'Conhecida por sua energia e presença marcante, a Flying Horse está de volta ao Picanha Fest 2026 ⚡🔥\n\nDepois de agitar a edição de 2025, a marca retorna para trazer ainda mais intensidade e experiência para quem quer viver cada momento da festa no máximo 🚀\n\n🐎 Energia, atitude e performance para acompanhar 8 horas de Open Food e Open Beer sem perder o ritmo 😎\n\nIsso é Flying Horse no Picanha Fest… e quem viveu, sabe!\n\n🎟️ Garanta seu ingresso no link da bio 😉'
    ),
    (
        ["FRINNI"],
        "Frinni",
        "Referência na comercialização de carcaças e cortes suínos desde 1999. Sabor e qualidade que superam o crescente nível de exigência do mercado. Retorna à nova edição do Picanha Fest.",
        'O sabor que a gente prova e comprova! 😋\n\n🐽 Referência na comercialização de carcaças e cortes suínos desde 1999, a Frinni estará novamente com a gente na próxima edição do Picanha Fest 🔥\n\n✅ Sabor e qualidade que superam o crescente nível de exigência do mercado! Estão preparados para viver essa experiência? 😎\n\n🎟️ Ingressos através do link na bio 😉'
    ),
    (
        ["GOODBOM"],
        "Good Bom",
        "Desde 1987 na RMC, crescendo de forma gradativa e sustentável. Atualmente 13 lojas em 7 cidades: Campinas, Sumaré, Hortolândia, Indaiatuba, Mogi Mirim, Monte Mor e Mogi Guaçu. Busca sempre garantir qualidade aos clientes com trabalho em equipe e brilho nos olhos.",
        'Assim como nós, o Good Bom busca garantir qualidade aos clientes, sempre com trabalho em equipe e brilho nos olhos 🤩\n\n✔️ Desde 1987 na RMC, sem perder a tradição, a marca cresce de forma gradativa e sustentável e, atualmente, possui 13 lojas, distribuídas em 7 cidades: Campinas, Sumaré, Hortolândia, Indaiatuba, Mogi Mirim, Monte Mor e Mogi Guaçu 👏🚀\n\nO match perfeito com o Picanha Fest! ✨😍'
    ),
    (
        ["GUIDARA"],
        "Guidara Meat & Co",
        "Com o propósito de ser a melhor empresa de carnes nobres do Brasil. De volta ao Picanha Fest 2026 com o Hambúrguer Guidara como grande destaque: carne selecionada, suculência no ponto certo, da fazenda ao prato.",
        'Com o propósito de ser a melhor empresa de carnes nobres do Brasil, a Guidara Meat & Co está de volta ao Picanha Fest 2026 🥩🔥\n\nEles chegam com um dos grandes destaques do evento: o Hambúrguer Guidara 🍔😋\n\nPrepare-se para uma experiência absurda de sabor, com carne selecionada, suculência no ponto certo e aquele padrão que só quem viveu o Picanha Fest conhece…\n\n🐂 Da fazenda ao prato, agora também no burger. Isso é nível Guidara, isso é Picanha Fest!\n\n🎟️ Garanta seu ingresso no link da bio e vem provar isso de perto 😉'
    ),
    (
        ["INTENTION"],
        "Intencion (Vodka & Gin)",
        "Chegando pela primeira vez ao Picanha Fest. Vodka pura com sabor único, perfeita para transformar qualquer momento em algo especial. Também traz o Intencion London Dry Gin, com versatilidade e personalidade — clássico gin tônica ou combinações criativas.",
        'Chegando pela primeira vez ao Picanha Fest, a Intencion já entra elevando o nível da experiência 🍸🔥\n\nCriada para quem valoriza bebidas de qualidade, a Intencion traz uma vodka pura, com sabor único, perfeita para transformar qualquer momento em algo especial ❄️\n\nE para os amantes de drinks, o Intencion London Dry Gin chega com versatilidade e personalidade, seja no clássico gin tônica ou em combinações criativas 🍋✨\n\n🍸 Cada drink, uma experiência. Cada gole, um momento.\n\n🎟️ Garanta seu ingresso no link da bio e venha viver isso com a gente 😉'
    ),
    (
        ["IRMAOS VICENTE", "IRMÃOS VICENTE"],
        "Irmãos Vicente",
        "Carnes premium de altíssima qualidade. Chegam ao Picanha Fest com o admirado Steak de Wagyu, uma das iguarias mais nobres do mundo. Sabor, maciez e suculência para proporcionar ainda mais excelência ao evento.",
        'Com carnes premium de altíssima qualidade, os Irmãos Vicente chegam ao Picanha Fest com o admirado Steak de Wagyu, uma das iguarias mais nobres do mundo 🥩😋\n\nSabor, maciez e suculência para proporcionar ainda mais excelência ao nosso evento 🚀😍\n\nAgradecemos pela parceria! 🔝\n\n🎟️ Garanta seu ingresso através do link na bio 😎'
    ),
    (
        ["NUAGE"],
        "Nuage IT's Everywhere",
        "Time de especialistas que leva soluções inovadoras no setor de TI e Cloud services. Patrocinadora oficial do Picanha Fest 2026.",
        '🌐 Com um time de especialistas que leva soluções inovadoras no setor de TI e Cloud services, a Nuage IT\'s Everywhere é patrocinadora oficial do Picanha Fest 2026 💻😉\n\nAgradecemos pela confiança e parceria! 🙏🚀'
    ),
    (
        ["QUALITY BEEF"],
        "Quality Beef",
        "Parceiro em destaque no mercado pelo sal de parrilla e pela qualidade dos produtos, que deixam a marca registrada em todos os que provam.",
        'Muito mais sabor para o nosso churrasco com o sal de parrilla Quality Beef 🧂🥩\n\n👏 Parceiro em destaque no mercado pela qualidade dos produtos, que deixam a marca registrada em todos os que provam! 😋\n\n⏳ Quem aí também está contando os dias para provar essas delícias na 6ª edição do Picanha Fest? 🔥'
    ),
    (
        ["QUEIJOS BANDEIRA"],
        "Queijos Bandeira",
        "Produção de queijos autênticos e deliciosos. Tradição que preserva o sabor genuíno do alimento. Retorna à nova edição do Picanha Fest com todo o seu cuidado artesanal.",
        'Responsável por uma cuidadosa produção de queijos autênticos e deliciosos, a Queijos Bandeira estará novamente conosco na 6ª edição do Picanha Fest 🧀😋\n\nTradição que se saboreia a cada pedaço e que preserva o sabor genuíno do alimento que nos acompanha há gerações!! 😍 Mais uma parceria incrível para o nosso evento! 🙌\n\n📲 Acesse o link na bio e garanta o seu ingresso 😎'
    ),
    (
        ["RAM_"],
        "RAM (Caminhonetes)",
        "Referência em caminhonetes que combinam força, capacidade, luxo e tecnologia. Parceria de alto nível para uma edição extraordinária.",
        'Reconhecida por elevar o patamar do mercado brasileiro com caminhonetes que combinam força, capacidade, luxo e tecnologia, a RAM se une ao Picanha Fest para garantir ainda mais exclusividade ao evento 🚘🚀\n\n✅ Parceria de alto nível para uma edição extraordinária! 🔝\n\n🎟️ Ingressos disponíveis através do link na bio 😉'
    ),
    (
        ["SANTA VERENA"],
        "Empório Santa Verena",
        "Sinônimo de qualidade e variedade. Desde 2003 atendendo o exigente público de Campinas com departamentos diversos: queijos, pães, frios e muito mais.",
        'Sinônimo de qualidade e variedade, o Empório Santa Verena conta com departamentos diversos para atender, desde 2003, o exigente público de Campinas 🧀🤗🥖\n\nUma parceria incrível que chega ao Picanha Fest 2026 para proporcionar ainda mais sabor aos nossos clientes 😋🥰\n\nBora?! 😏 Ingressos através do link na bio 👉🎟️'
    ),
    (
        ["SERTAO CORDEIRO", "SERTÃO CORDEIRO"],
        "Sertão Cordeiro",
        "Dedicada à produção de carne de alta qualidade de ovinos e caprinos. Uma experiência gastronômica incomparável, onde cada mordida é uma celebração da tradição da marca.",
        'Do campo, para o Picanha Fest! 🌾🔥\n\nDedicada à produção de carne de alta qualidade de ovinos e caprinos, a Sertão Cordeiro promete muito sabor e excelência em mais uma edição da nossa festa! 😋❤️\n\nUma experiência gastronômica incomparável, onde cada mordida é uma celebração da tradição da marca 😉 Bora?! 😎\n\n👉🎟️ Garanta o seu lugar através do link na bio.'
    ),
    (
        ["UPD8", "UPDT8"],
        ":upd8",
        "Há 10 anos conectando tecnologia e negócios. Parceira oficial do Picanha Fest 2026. Reconhecida por transformar empresas com soluções de computação em nuvem, análise de dados, inteligência artificial e desenvolvimento de sistemas personalizados.",
        'Há 10 anos conectando tecnologia e negócios, a :upd8 é parceira oficial do Picanha Fest 2026 🚀🚀🚀\n\n💻 Reconhecida por transformar empresas com soluções de computação em nuvem, análise de dados, inteligência artificial e desenvolvimento de sistemas personalizados, a marca chega a Campinas para a 6ª edição de uma das festas mais admiradas do interior paulista 😉\n\nBora?! 🥩🎶🍻\n\n🎟️ Ingressos através do link na bio.'
    ),
]


def identificar_patrocinador(arquivo):
    """Retorna (nome, descrição, referência) se for post de patrocinador, ou None."""
    arq_upper = arquivo.upper()
    for padroes, nome, desc, ref in SPONSORS:
        for padrao in padroes:
            if padrao.upper() in arq_upper:
                return nome, desc, ref
    return None


def remover_arroba(texto):
    """Remove qualquer @menção do texto."""
    return re.sub(r'@\w+', '', texto).strip()


CTA_VARIACOES = [
    "🎟️ Garanta seu ingresso através do link na bio!",
    "🎟️ Compre seu ingresso no link da bio!",
    "🎟️ Ingressos através do link na bio!",
    "🎟️ Garanta já o seu ingresso pelo link na bio!",
    "🎟️ Ingressos voando pelo link na bio — não vacila!",
    "🎟️ Acesse o link na bio e garanta seu ingresso!",
    "🎟️ Link na bio para garantir o seu lugar!",
    "🎟️ Compre agora pelo link na bio e garanta sua vaga!",
    "🎟️ Bora?! Ingressos no link da bio!",
    "🎟️ Garanta seu lugar através do link na bio 😎",
]

_cta_idx = [0]  # índice rotativo para variar CTAs

def proximo_cta():
    cta = CTA_VARIACOES[_cta_idx[0] % len(CTA_VARIACOES)]
    _cta_idx[0] += 1
    return cta


def gerar_legenda_revisada(arquivo, tipo, legenda_atual, sponsor_info=None):
    """Chama Claude para reescrever a legenda. Retorna (nova_legenda, hashtags)."""

    cta = proximo_cta()

    if sponsor_info:
        nome_marca, desc_marca, ref_publicada = sponsor_info
        ref_text = f"\nLEGENDA JÁ PUBLICADA COMO REFERÊNCIA DE ESTILO:\n{ref_publicada}\n" if ref_publicada else ""
        instrucao_tipo = (
            f"Este é um {tipo.upper()} de PATROCINADOR ({nome_marca}).\n"
            f"INFORMAÇÕES DA MARCA: {desc_marca}\n"
            f"{ref_text}\n"
            "REGRAS PARA PATROCINADOR:\n"
            "- Mencione o nome da marca naturalmente (SEM usar @)\n"
            "- Explique o que a marca traz ao evento com entusiasmo\n"
            "- Use 5-8 linhas com parágrafos separados por linha em branco\n"
            "- Mostre empolgação pela parceria\n"
            f"- Termine com EXATAMENTE este CTA na última linha: {cta}\n"
        )
    else:
        instrucao_tipo = (
            f"Este é um {tipo.upper()} de conteúdo geral do evento (não é de patrocinador).\n"
            "REGRAS:\n"
            "- Crie uma legenda elaborada e variada, não genérica\n"
            "- Use 3-6 linhas com boa variação de tom\n"
            "- Pode mencionar detalhes do evento (data 25/04, local Paioça do Caboclo, open food, open beer, shows)\n"
            f"- Termine com EXATAMENTE este CTA na última linha: {cta}\n"
        )

    prompt = (
        f"{CONTEXT}\n\n"
        f"{instrucao_tipo}\n"
        f"LEGENDA ATUAL (use como referência de tema, mas MELHORE bastante):\n{legenda_atual}\n\n"
        "REGRAS ABSOLUTAS:\n"
        "- NUNCA use @ em nenhuma menção (nem de marcas, nem de pessoas, nunca)\n"
        "- Não copie a legenda atual — reescreva com muito mais personalidade e elaboração\n"
        "- Não use hashtags no corpo da legenda\n"
        f"- A ÚLTIMA linha da legenda deve ser: {cta}\n\n"
        "Responda EXATAMENTE neste formato (a legenda pode ter várias linhas):\n"
        "LEGENDA:\n"
        "[linha 1 da legenda]\n"
        "[linha 2]\n"
        "[...]\n"
        f"[{cta}]\n"
        "HASHTAGS: [hashtags separadas por espaço, iniciando com #PicanhaFest2026]\n"
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": "claude-opus-4-6",
            "max_tokens": 700,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=60
    )
    text = resp.json()["content"][0]["text"]

    # Parser multi-linha: captura tudo entre LEGENDA: e HASHTAGS:
    legenda_parts = []
    hashtags = HASHTAGS
    in_legenda = False

    for line in text.split("\n"):
        if line.strip() == "LEGENDA:" or line.startswith("LEGENDA:\n"):
            in_legenda = True
        elif line.startswith("HASHTAGS:"):
            in_legenda = False
            hashtags = line.replace("HASHTAGS:", "").strip()
        elif line.startswith("LEGENDA:"):
            # "LEGENDA: texto na mesma linha"
            in_legenda = True
            first = line.replace("LEGENDA:", "").strip()
            if first:
                legenda_parts.append(first)
        elif in_legenda:
            legenda_parts.append(line)

    # Remove linhas vazias do final
    while legenda_parts and not legenda_parts[-1].strip():
        legenda_parts.pop()

    legenda = "\n".join(legenda_parts)

    # Se ficou vazio, fallback para captura simples
    if not legenda:
        for line in text.split("\n"):
            if line.startswith("LEGENDA:"):
                legenda = line.replace("LEGENDA:", "").strip()

    # Garantir CTA no final
    if not legenda.endswith("!") and not any(c in legenda[-50:] for c in ["bio", "ingresso"]):
        legenda = legenda.rstrip() + "\n" + cta

    # Segurança: remover qualquer @ que escapar
    legenda = remover_arroba(legenda)

    return legenda, hashtags


# ── Ler planilha ────────────────────────────────────────────────────────────────
print("Lendo planilha...")
result = sheets.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range="Página1!A:I"
).execute()
rows = result.get("values", [])
print(f"Total de linhas: {len(rows) - 1}")

# ── Processar todas as linhas ───────────────────────────────────────────────────
updates = []
erros   = []

for i, row in enumerate(rows[1:], start=2):
    while len(row) < 9:
        row.append("")
    id_, arquivo, tipo, data, hora, legenda_atual, hashtags_atual, url, status = row

    # Pular linhas sem conteúdo ou já publicadas
    if not arquivo or status == "publicado":
        continue

    sponsor_info = identificar_patrocinador(arquivo) if tipo in ("reel", "estatico") else None
    sponsor_label = f" [{sponsor_info[0]}]" if sponsor_info else ""

    print(f"  {id_} [{tipo}]{sponsor_label} {arquivo[:50]}...")

    try:
        nova_legenda, novos_hashtags = gerar_legenda_revisada(
            arquivo, tipo, legenda_atual, sponsor_info
        )
        updates.append({
            "range": f"Página1!F{i}:G{i}",
            "values": [[nova_legenda, novos_hashtags]]
        })
        print(f"    ✅ legenda gerada ({len(nova_legenda)} chars)")
        time.sleep(0.5)
    except Exception as e:
        print(f"    ❌ Erro: {e}")
        erros.append((id_, str(e)))

# ── Salvar em batch ─────────────────────────────────────────────────────────────
if updates:
    print(f"\nSalvando {len(updates)} legendas na planilha...")
    # Processar em lotes de 50 (limite do Sheets API)
    BATCH = 50
    for start in range(0, len(updates), BATCH):
        lote = updates[start:start+BATCH]
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": lote}
        ).execute()
        print(f"  Lote {start//BATCH + 1}: {len(lote)} linhas salvas")
    print(f"\n✅ {len(updates)} legendas atualizadas!")
else:
    print("\n⚠️ Nenhuma legenda para atualizar.")

if erros:
    print(f"\n⚠️ {len(erros)} erros:")
    for id_, msg in erros:
        print(f"  linha {id_}: {msg}")

print("\nDone.")
