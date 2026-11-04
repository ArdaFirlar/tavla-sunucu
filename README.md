# Tavla sunucusu

Tavla oyununun internet üzerinden çok oyunculu sunucusu (Cloudflare Workers + Durable Objects, ücretsiz plan).

- `GET /` → durum yazısı
- `GET /ws` → oyun bağlantısı (WebSocket, JSON mesajlar; oyundaki `src/net.rs` ile aynı protokol)

Odalar ve maç kayıtları Durable Object deposunda saklanır; 14 gün oynanmayan oda silinir.

Yerel deneme: `npm install && npx wrangler dev` → oyunu `TAVLA_SUNUCU=ws://127.0.0.1:8787/ws` ile başlat.
Yayınlama: Cloudflare panelinde bu depo bağlıysa her `git push` sonrası otomatik; elle: `npx wrangler deploy`.
