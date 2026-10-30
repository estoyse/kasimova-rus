# Kasimova Rus — Telegram Business AI Sales Manager

Zaynab Kasimovaning «po-russki gaplashamiz» online rus tili kursi uchun rasmiy AI savdo menejeri va to'lov nazoratchisi.

## 📁 Loyiha Strukturasi

```text
├── COURSE_DETAILS.md           # ⚡️ Dinamik sozlamalar (narx, sana, kartalar) - RESTARTSIZ ISHLAYDI
├── docker-compose.yml          # Konteynerlar: hermes (AI) + adapter (Telegram bot)
├── adapter/
│   ├── Dockerfile              # Python 3.13 konteyner muhiti
│   ├── main.py                 # Telegram Business adapteri, CRM ledger, Vision tahlilchi
│   └── requirements.txt        # aiogram 3, httpx, pillow, pyjwt
├── data/
│   ├── SOUL.md                 # 5 bosqichli insoniy savdo voronkasi va qoidalari
│   ├── mem0.json               # Uzoq muddatli xotira (Mem0) sozlamalari
│   ├── skills/
│   │   └── productivity/
│   │       └── russian-course-sales/
│   │           └── SKILL.md    # Kurs savdosi maxsus mahorati (Hermes skill)
│   └── rules_backup_20261030/  # Eski qoidalar va skriptlarning xavfsiz zaxirasi
```

---

## ⚡️ Kurs Sozlamalarini O'zgartirish (Zero-Restart)

Narx, dars boshlanish sanasi yoki karta raqamlari o'zgarsa, dasturni **restart qilish shart emas**!
Shunchaki `COURSE_DETAILS.md` faylini ochib, kerakli qatorni tahrirlang va saqlang:

```markdown
- **Asl narxi:** 1 490 000 so'm
- **Predzapis narxi (to'liq kurs):** 980 000 so'm
- **Joy band qilish (minimal to'lov):** 100 000 so'm
- **Darslar boshlanish sanasi:** 10-noyabr
```

Adapter fayl o'zgarganini soniya ichida avtomatik o'qiydi va yangi xabarlardan boshlab yangi narxlarda ishlaydi.

---

## 🚀 Ishga Tushirish va Boshqarish

### 1. Ishga tushirish:
```bash
docker compose up -d
```

### 2. Loglarni kuzatish:
```bash
# Telegram adapter loglari
docker logs -f hermes-kasimova-rus-adapter

# Hermes AI miyasi loglari
docker logs -f hermes-kasimova-rus
```

### 3. Konteynerni qayta qurish (faqat python kod o'zgarganda):
```bash
docker compose build adapter && docker compose up -d adapter
```

---

## 🛡 Xavfsizlik va Ma'lumotlar Bazasi
- Mijoz profillari va kumulyativ to'lovlar `data/adapter/adapter.sqlite3` da saqlanadi (`lead_profiles`).
- Sirli kalitlar va xavfsizlik fayllari git reponing `.gitignore` ro'yxatida xavfsiz saqlanadi.
