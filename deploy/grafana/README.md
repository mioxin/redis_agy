# Инструкция по мониторингу и дашбордам Grafana

Данная директория содержит полный набор артефактов для сквозного мониторинга микросервиса **Courier Location Service**, кэш-движка **Redis** и **Go Runtime / pprof** с использованием **Victoria Metrics** и **Grafana**.

---

## 📁 Структура директорий

```
deploy/
├── prometheus/
│   └── prometheus.yml          # Конфигурация сбора метрик для Victoria Metrics
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── datasource.yml  # Авто-подключение Victoria Metrics (http://victoria:8428)
    │   └── dashboards/
    │       └── dashboards.yml  # Авто-импорт JSON-дашбордов из /var/lib/grafana/dashboards
    └── dashboards/
        ├── courier-service-sla.json    # Дашборд 1: Бизнес-метрики, SLA (<100мс), HTTP & Poller
        ├── redis-monitoring.json       # Дашборд 2: Инфраструктура и производительность Redis
        └── go-runtime-pprof.json       # Дашборд 3: Go Runtime, GC, Memory + 1-Click pprof дампы
```

---

## 📊 Описание дашбордов

### 1. `Courier Location Service — SLA & Business Metrics` (`courier-service-sla`)
* **SLA Compliance Rate (< 100ms)**: процент входящих запросов, уложившихся в норматив SLA.
* **SLA Breaches Total**: общее количество нарушений времени отклика.
* **Cache Hit Ratio (%)**: процент попаданий в кэш Redis при чтении координат.
* **Active Tracked Orders**: количество заказов, находящихся на активном мониторинге.
* **Poller Sync Lag**: задержка с момента последней успешной итерации фонового поллера.
* **HTTP Requests RPS**: разбивка по путям и статус-кодам (200 OK, 202 Accepted, 500 Error).
* **Latency Percentiles**: точные перцентили времени обработки (p50, p90, p99, p99.9) с горизонтальной контрольной линией 100 мс.
* **Circuit Breaker Trips**: срабатывания защитных автоматов Order Service и Courier Service.

### 2. `Redis Infrastructure & Cache Engine` (`redis-monitoring`)
* **Статус инстанса**: `redis_up`.
* **Потребление памяти**: `Used Memory`, `RSS Memory`, `Max Memory Limit`.
* **Keyspace & TTLs**: количество ключей в базе данных и количество ключей с активным TTL.
* **Native Hit Ratio**: эффективность кэширования на уровне самого Redis.
* **Throughput & Network**: количество обработанных команд в секунду и сетевой трафик (In/Out).
* **Evictions & Expirations**: частота вытеснения ключей по OOM и естественного истечения TTL.

### 3. `Go Runtime & Diagnostic Profiling (pprof)` (`go-runtime-pprof`)
* **Интерактивный центр pprof (1-Click Actions)**:
  * 🌐 Открыть веб-индекс `/debug/pprof/`
  * ⏱️ Скачать 30-секундный CPU-профиль
  * 💾 Скачать снимок памяти кучи (Heap Profile)
  * 🧵 Просмотреть стек-дамп горутин
  * 🔒 Скачать профиль блокировок мьютексов (Mutex Contention)
  * 🔍 Скачать трассировку рантайма (Execution Trace)
* **Память и аллокации**: Heap In-Use, Heap Alloc, Stack In-Use, Heap Sys, скорость аллокаций.
* **Сборщик мусора (GC)**: перцентили пауз GC (p50, p75, Max Pause) и частота циклов сборки.
* **Горутины и треды ОС**: мониторинг динамики горутин (`go_goroutines`) и системных тредов (`go_threads`).

---

## 🚀 Запуск и проверка

При запуске через Docker Compose:

```bash
docker compose up -d victoria grafana redis redis-exporter
```

- **Grafana доступна по адресу**: [http://localhost:4000](http://localhost:4000) (логин/пароль по умолчанию: `admin` / `admin`).
- Дашборды автоматически появятся в папке **"Courier Service"**.
- При необходимости ручного импорта дашбордов файлы JSON можно загрузить через меню Grafana: **Dashboards -> New -> Import**.
