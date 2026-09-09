# Ürün Dedektifi

Yapay Zekâ Destekli Ürün ve Pazar Analiz Platformu.

Temel felsefe:

> Çok satanı değil, bizim satabileceğimiz çok satanı bul.

## Proje yapısı

```text
apps/
  mobile/       Android uygulama
  web/          Web/admin paneli, sonraki aşama
backend/
  api/          Railway API
  workers/      Autopilot/queue worker
  adapters/     Trendyol, Shopify, Ads, Alibaba adapterları
database/
  migrations/   PostgreSQL/Supabase şeması
docs/
shared/
```

## Milestone 1

İlk çalışan hedef:

```text
Telefon → AI Oda → Backend → OpenAI → Sonuç → Karar Merkezi
```

## Railway

İlk deploy için Railway root directory:

```text
backend/api
```

Variables:

```text
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-5-mini
```

Test:

```text
GET /
GET /ai-room?message=katlanabilir%20düzenleyici%20Türkiye%20pazarı
POST /ai-room
POST /
```

## Güvenlik

AI API key'leri APK içine konmaz. Sadece backend ortam değişkenlerinde tutulur.
