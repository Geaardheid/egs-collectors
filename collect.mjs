/* EGS FiveM collector
   Haalt elk halfuur de volledige FiveM-serverlijst op (dezelfde bron als de FiveM-client:
   Cfx.re frontend, protobuf-stream), telt spelers en stuurt de servers met spelers naar
   EveryGameStat. Spelersnamen worden niet gelezen of opgeslagen. Aantallen zijn zoals de
   servers ze zelf melden. */
const SRC = "https://frontend.cfx-services.net/api/servers/streamRedir/";
const SB_URL = "https://wcsgosrevyyafnerrhge.supabase.co";
const SB_KEY = "sb_publishable_mric3P9h3YsHU_t5Jc5wWw_qulO3z8I";
const TOKEN = process.env.EGS_INGEST_TOKEN;
if (!TOKEN) { console.error("EGS_INGEST_TOKEN ontbreekt"); process.exit(1); }

/* protobuf-varint; int32-velden met een negatieve waarde komen als 64-bit binnen, dus via BigInt terug naar int64 */
const varint = (b, i) => {
  let r = 0, s = 0, c;
  do { c = b[i++]; if (s < 49) r += (c & 0x7f) * 2 ** s; else { let big = BigInt(r), sh = BigInt(s); for (;;) { big |= BigInt(c & 0x7f) << sh; sh += 7n; if (!(c & 0x80)) break; c = b[i++]; } return [Number(BigInt.asIntN(64, big)), i]; } s += 7; } while (c & 0x80);
  return [r, i];
};
function* fields(b) {
  let i = 0;
  while (i < b.length) {
    let k; [k, i] = varint(b, i);
    const f = Math.floor(k / 8), t = k & 7;
    if (t === 0) { let v; [v, i] = varint(b, i); yield [f, v]; }
    else if (t === 2) { let L; [L, i] = varint(b, i); yield [f, b.subarray(i, i + L)]; i += L; }
    else if (t === 5) { i += 4; } else if (t === 1) { i += 8; }
    else throw new Error("wire type " + t);
  }
}
const dec = new TextDecoder();
const ctrl = s => s.replace(/[\u0000-\u001f\u007f\ud800-\udfff]/g, "");
const clean = s => ctrl(s).replace(/\^[0-9]/g, "").replace(/~[a-zA-Z]~/g, "").replace(/\s+/g, " ").trim().slice(0, 140);
const slugify = s => s.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60);

const res = await fetch(SRC, { headers: { "User-Agent": "EveryGameStat/1.0 (+https://everygamestat.com)" }, redirect: "follow" });
if (!res.ok) { console.error("bron", res.status); process.exit(1); }
const buf = new Uint8Array(await res.arrayBuffer());
const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
let i = 0, servers = 0, players = 0; const rows = [];
while (i + 4 <= buf.length) {
  const L = dv.getUint32(i, true); const rec = buf.subarray(i + 4, i + 4 + L); i += 4 + L;
  let code = "", data = null;
  for (const [f, v] of fields(rec)) { if (f === 1) code = dec.decode(v); else if (f === 2) data = v; }
  if (!data) continue;
  const s = { vars: {} };
  for (const [f, v] of fields(data)) {
    if (f === 1) s.max = v; else if (f === 2) s.clients = v; else if (f === 4) s.host = dec.decode(v);
    else if (f === 5) s.gametype = dec.decode(v); else if (f === 6) s.mapname = dec.decode(v);
    else if (f === 11) s.icon = v; else if (f === 17) s.upvote = v;
    else if (f === 12) { let k = "", val = ""; for (const [ff, vv] of fields(v)) { if (ff === 1) k = dec.decode(vv); else if (ff === 2) val = dec.decode(vv); } s.vars[k] = val; }
  }
  if ((s.vars.gamename || "gta5") !== "gta5") continue;   /* alleen FiveM, geen RedM */
  servers++;
  const c = s.clients || 0; players += c;
  if (c <= 0) continue;
  const name = clean(s.vars.sv_projectName || s.host || code) || code;
  const disc = s.vars["discord.gg"] || s.vars.discord || "";
  rows.push({ code, name, slug: slugify(name) || code.toLowerCase(), raw: ctrl(s.host || "").slice(0, 300),
    gametype: clean(s.gametype || "").slice(0, 60), mapname: clean(s.mapname || "").slice(0, 60), locale: ctrl(s.vars.locale || "").slice(0, 10),
    tags: [...new Set((s.vars.tags || "").split(",").map(t => ctrl(t).trim().toLowerCase().slice(0, 40)).filter(Boolean))].slice(0, 12),
    clients: c, max: s.max || null, banner: /^https:\/\//.test(s.vars.banner_detail || "") ? ctrl(s.vars.banner_detail).slice(0, 400) : null,
    icon: s.icon || null, discord: disc ? ctrl(disc).replace(/^https?:\/\//, "").slice(0, 120) : null,
    peak: /^\d+$/.test(s.vars.peak_players || "") ? Number(s.vars.peak_players) : null, upvote: s.upvote || 0,
    onesync: s.vars.onesync_enabled === "true" });
}
rows.sort((a, b) => b.clients - a.clients).forEach((r, k) => { r.rank = k + 1; });
/* Okt 2026: de ingest draait onder de publieke rol met een limiet van 3 s per aanroep. Daarom chunks van 500,
   één tijdstip (run_at) voor alle chunks, en alleen de laatste chunk (final) zet ontbrekende servers op 0 en
   schrijft het totaal. Dagcijfers en opruimen doet de database zelf (pg_cron egs-fivem-maintain).
   Een chunk die faalt (time-out, 5xx) wordt nog twee keer geprobeerd. */
const CHUNK = 500;
const total = { servers, active: rows.length, players, run_at: new Date().toISOString() };
console.log(`servers ${servers}, active ${rows.length}, players ${players}`);
const sleep = ms => new Promise(r => setTimeout(r, ms));
for (let k = 0; k < rows.length; k += CHUNK) {
  const final = k + CHUNK >= rows.length;
  const body = JSON.stringify({ p_token: TOKEN, p_rows: rows.slice(k, k + CHUNK), p_total: { ...total, final } });
  let ok = false, last = "";
  for (let attempt = 1; attempt <= 3 && !ok; attempt++) {
    const r = await fetch(SB_URL + "/rest/v1/rpc/egs_fivem_ingest", { method: "POST", headers: { apikey: SB_KEY, "Content-Type": "application/json" }, body });
    last = `${r.status} ${(await r.text()).slice(0, 300)}`;
    if (r.ok) ok = true;
    else if (r.status === 403 || r.status === 400) break;   /* verkeerde token of data: opnieuw proberen helpt niet */
    else await sleep(4000 * attempt);
  }
  if (!ok) { console.error("ingest chunk", k, last); process.exit(1); }
  console.log("chunk", k, final ? "(final)" : "", last);
}
