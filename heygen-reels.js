/**
 * Picanha Fest 2026 — Gerador de Reels HeyGen
 * Avatar: Felipe Roselli
 *
 * Uso: HEYGEN_API_KEY=sk_... node heygen-reels.js
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const API_KEY = process.env.HEYGEN_API_KEY || 'sk_V2_hgu_kiERdYpZ5F7_PpL23cRfzskQkGhPJGa4vio8NFnWJSpJ';
const HEYGEN_BASE = 'https://api.heygen.com';
const OUTPUT_DIR = path.join(__dirname, 'heygen-output');

const AVATAR_ID = 'd86dab1f3b5b4baa9d44e82ba753fea9';  // Felipe Roselli
const VOICE_ID  = 'b889fc7ab81f437bab5711e58a95de5f';  // Felipe Roselli (voz clone)

const REELS = [
  {
    id: 'reel-01-menos10dias',
    title: 'Picanha Fest — Faltam menos de 10 dias',
    outputFile: 'reel-01-menos10dias.mp4',
    // Números por extenso · sem traço em · sem "open food/beer" (TTS lê literalmente)
    script: 'Gente, faltam menos de dez dias pro Picanha Fest! A sexta edição acontece no dia vinte e cinco de abril, na Paioça do Caboclo, em Campinas. São oito horas de festa. Comida e chope à vontade, mais de duas toneladas de carne na brasa. Otávio e Raphael, Sem Tempo, Resenha do Marcílio e muito mais. Os ingressos estão voando. Já passamos do segundo lote. Corre no link da bio e garante o seu antes que esgote!'
  },
  {
    id: 'reel-02-menos1semana',
    title: 'Picanha Fest — Falta menos de uma semana',
    outputFile: 'reel-02-menos1semana.mp4',
    script: 'Falta menos de uma semana pro Picanha Fest! No dia vinte e cinco de abril a Paioça do Caboclo vai pegar fogo. Quatorze estações de churrasco, mais de duas toneladas de carne na brasa, chope Império à vontade e shows incríveis. Os ingressos estão acabando mesmo. Se você ainda não garantiu o seu, corre agora no link da bio. Não deixa essa passar!'
  },
  {
    id: 'reel-03-este-sabado',
    title: 'Picanha Fest — É neste sábado!',
    outputFile: 'reel-03-este-sabado.mp4',
    script: 'É neste sábado! O Picanha Fest acontece no dia vinte e cinco de abril, na Paioça do Caboclo, em Campinas. A festa começa às quatorze horas e vai até as vinte e duas. Comida e chope à vontade, quatorze estações de churrasco e shows ao vivo. Ainda tem ingresso, mas tá acabando rápido. Pega o link na bio e chega junto com a gente nessa festa!'
  },
  {
    id: 'reel-04-amanha',
    title: 'Picanha Fest — É amanhã!',
    outputFile: 'reel-04-amanha.mp4',
    script: 'É amanhã! O Picanha Fest acontece amanhã, vinte e cinco de abril, na Paioça do Caboclo, em Campinas. Das quatorze às vinte e duas horas. Comida e chope à vontade, shows ao vivo e churrasco sem parar. Se você ainda não garantiu seu ingresso, essa é a última chance. Corre agora no link da bio. Te vejo amanhã!'
  }
];

async function apiPost(endpoint, body) {
  const res = await fetch(`${HEYGEN_BASE}${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
    body: JSON.stringify(body)
  });
  if (!res.ok) throw new Error(`POST ${endpoint} → ${res.status}: ${await res.text()}`);
  return res.json();
}

async function apiGet(endpoint) {
  const res = await fetch(`${HEYGEN_BASE}${endpoint}`, {
    headers: { 'X-Api-Key': API_KEY }
  });
  if (!res.ok) throw new Error(`GET ${endpoint} → ${res.status}: ${await res.text()}`);
  return res.json();
}

async function gerarVideo(reel) {
  console.log(`\n▶ Gerando: ${reel.title}`);
  console.log(`  Script (${reel.script.length} chars): "${reel.script.substring(0, 80)}..."`);

  const data = await apiPost('/v2/video/generate', {
    video_inputs: [{
      character: {
        type: 'avatar',
        avatar_id: AVATAR_ID,
        avatar_style: 'closeUp'   // melhor lip sync que "normal"
      },
      voice: {
        type: 'text',
        input_text: reel.script,
        voice_id: VOICE_ID,
        speed: 0.95               // levemente mais lento = fala mais natural
      },
      background: {
        type: 'color',
        value: '#111111'
      }
    }],
    dimension: { width: 720, height: 1280 },
    caption: false,
    title: reel.title
  });

  const videoId = data.data?.video_id;
  if (!videoId) throw new Error(`Resposta inesperada: ${JSON.stringify(data)}`);
  console.log(`  video_id: ${videoId}`);
  return videoId;
}

async function aguardarVideo(videoId) {
  const start = Date.now();
  console.log(`  Aguardando processamento...`);
  while (true) {
    if (Date.now() - start > 600000) throw new Error('Timeout 10 min');
    await new Promise(r => setTimeout(r, 10000));
    const d = await apiGet(`/v1/video_status.get?video_id=${videoId}`);
    const status = d.data?.status;
    const elapsed = Math.round((Date.now() - start) / 1000);
    console.log(`  Status: ${status} (${elapsed}s)`);
    if (status === 'completed') {
      const url = d.data?.video_url;
      if (!url) throw new Error('completed mas sem video_url');
      return url;
    }
    if (status === 'failed') throw new Error(`HeyGen falhou: ${d.data?.error || '?'}`);
  }
}

async function baixarVideo(videoUrl, outputPath) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(path.dirname(outputPath))) {
      fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    }
    console.log(`  Baixando...`);
    const file = fs.createWriteStream(outputPath);
    const handleResp = (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        https.get(res.headers.location, handleResp).on('error', reject);
        return;
      }
      if (res.statusCode !== 200) { reject(new Error(`Download falhou: ${res.statusCode}`)); return; }
      res.pipe(file);
      file.on('finish', () => {
        file.close();
        const mb = (fs.statSync(outputPath).size / 1024 / 1024).toFixed(2);
        console.log(`  Salvo: ${outputPath} (${mb} MB)`);
        resolve();
      });
    };
    https.get(videoUrl, handleResp).on('error', err => { fs.unlink(outputPath, ()=>{}); reject(err); });
  });
}

async function main() {
  console.log('═'.repeat(60));
  console.log('  Picanha Fest 2026 — Reels HeyGen — Felipe Roselli');
  console.log('═'.repeat(60));

  const log = [];

  for (const reel of REELS) {
    try {
      const videoId = await gerarVideo(reel);
      const videoUrl = await aguardarVideo(videoId);
      const outputPath = path.join(OUTPUT_DIR, reel.outputFile);
      await baixarVideo(videoUrl, outputPath);
      log.push({ id: reel.id, videoId, status: 'ok', path: outputPath });
      console.log(`  ✅ ${reel.id}`);
    } catch (err) {
      console.error(`  ❌ ${reel.id}: ${err.message}`);
      log.push({ id: reel.id, status: 'erro', error: err.message });
    }
  }

  const logPath = path.join(OUTPUT_DIR, 'generation-log.json');
  fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
  console.log(`\nLog salvo em: ${logPath}`);
  console.log('\nConcluído!');
}

main().catch(err => { console.error('Erro fatal:', err); process.exit(1); });
