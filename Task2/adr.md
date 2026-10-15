### **Название задачи:**

Высокоуровневое видение решений (C2) платформы мониторинга свиноферм

### **Автор:**

Вячеслав Душин

### **Дата:**

2026-10-06

### **Функциональные требования**

См. [Task1/adr_mine.md#функциональные-требования](../Task1/adr_mine.md#функциональные-требования)

### **Нефункциональные требования**

См. [Task1/adr_mine.md#нефункциональные-требования](../Task1/adr_mine.md#нефункциональные-требования)

### Ограниченные контексты

| Контекст | FR | Обоснование |
| --- | --- | --- |
| Incident detection | F1+F2 | Выделено из общей аналитики, так как тут более жесткий SLA (миллисекунды). Это потребует отдельного решения, не зависящего от остальной аналитики |
| Herd management                  | F4+F6   | Тот же источник, что и для контекста выше, но менее жесткий SLA (секунды) |
| Food and inventory forecasting | F7 | Задача планирования и прогноза запасов |
| Identity and access | F13 | Для аутентификации и авторизации |
| Edge sync and offline resilience | F12+F9 | Каждой ферме нужен механизм локальной буферизации и синхронизации при отключении интернета |
| Metrics collection and export | F10+F11 | Пользовательские и встроенный метрики должны собираться в одном контексте, так как механизм обработки и сбора одинаковый. Единственное отличие пользовательских -- что их добавляют юзеры |
| External API | F14 | Для интеграции со внешними клиентами |
| Farm equipment integration       | F5+F3   | Кормушки, поилки, фильтрация воды -- это все мультивендорная интеграция оборудования |

### Решение

#### Описание решения

Контейнеры на агенте фермы: Camera Gateway, Incident Detection Service, Herd Analytics Service, Equipment Adapter, Feed Sensor Adapter, Local Event Outbox (SQLite), Sync Client, Local Notification Service, Local Auth Cache.

Контейнеры в центре: Farm Sync Gateway, Event Bus (Kafka), Incident Alerting & Escalation, Herd Analytics Aggregator, Feed Inventory & Forecasting, Equipment Config Service, Metrics Export Service, Identity & Access Service, API Gateway, Operational DB (PG SQL), Timeseries DB (Timescale DB)



#### Обоснование выбора решений

Event bus -- Apache Kafka

Преимущества:

1. Consumer groups позволяют нескольким инструментам  (Alerting, Herd Aggregator, Feed Forecasting etc) читать один и тот же поток независимо
2. Держит неограниченное количество ферм
3. Есть replay событий по offset -- можно использовать при восстановлении после сбоя.

Ограничения:

1. Больше операционная сложность, чем у простого брокера очередей: партицирование, балансировка и т.д.



Local Event Outbox: -- встраиваемая BD SQLite

Преимущества

1. Не требует отдельного процесса/сервиса на оконечном устройстве.

Ограничения:

1. Не рассчитана на конкурентную запись. WAL снимает блокировку читателя писателем, но конкурентная запись все равно невозможна.

   

Канал передачи инцидентов на центральный сервер: MQTT

Преимущества: 

1. Низкая задержка: событие не только пишется в outbox, но и сразу пересылается через MQTT.
2. Легкий протокол для constrained-устройств и нестабильных каналов связи (ферма может сидеть на слабом/дорогом канале): минимальный overhead кадра (заголовок MQTT может быть 2 байта против заметно большего у Kafka/AMQP), легкий клиент (работает даже на микроконтроллере) и сессии, спроектированные под частые разрывы/переподключения.

Ограничение:

1. Работает быстро (миллисекунды) только при наличии связи.



#### Интеграции

|  №   | Откуда -> Куда                                               | Sync/Async         | Протокол                                               | Гарантия доставки                                            |
| :--: | ------------------------------------------------------------ | ------------------ | ------------------------------------------------------ | ------------------------------------------------------------ |
|  1   | Cameras -> Camera Gateway                                    | Sync               | RTSP, ONVIF, другой видео-протокол                     | best-effort. Не под нашим контролем                          |
|  2   | Camera Gateway -> Incident detection                         | Sync               | внутри агента                                          | at most once. пара пропущенных кадров не сделает погоды      |
|  3   | Camera Gateway -> Herd Analytics                             | Async              | внутренняя очередь агента                              | At-least-once.                                               |
|  4   | Incident Detection -> Local Notification                     | Sync               | внутренний вызов                                       | At least once, idempotent                                    |
|  5   | Incident Detection -> Local Event Outbox                     | Async              | internal write                                         | At least once, durable                                       |
|  6   | Incident Detection -> Farm Sync Gateway (fast-path)          | Async, low-latency | MQTT (не Kafka/RabbitMQ (AMQP) -- см. обоснование выше) | At least once, idempotent                                    |
|  7   | Equipment Adapter <-> feeders, waterers                      | Sync               | Modbus/MQTT                                            | At least once, idempotent                                    |
|  8   | Water Filtration -> Equipment Adapter                        | Async              | Modbus/MQTT                                            | Best-effort. не под нашим контролем                          |
|  9   | Sync Client -> Local Event outbox                            | Sync               | internal                                               | at least once, idempotent                                    |
|  10  | Sync Client -> Farm Sync Gateway                             | Async              | gRPC/HTTPS                                             | At least once, idempotent (event id)                         |
|  11  | Farm Sync Gateway -> Event Bus                               | Async              | Kafka producer                                         | At least once (idempotent producer)                          |
|  12  | Event Bus -> Alerting, Herd Aggregator,Feed Forecasting,Metrics | Async              | Kafka consumer                                         | At least once, idempotent (incident id) -- иначе возможна двойная запись при одновременной доставке через fast-path и batch |
|  13  | Equipment Config Service -> Sync Client                      | Async              | gRPC/HTTPS                                             | At least once, idempotent                                    |
|  14  | Metrics Export -> Corporate Platform                         | Async              | REST/Kafka                                             | At least once, idempotent (event id)                         |
|  15  | API Gateway -> Mobile/Web apps                               | Sync               | REST, GraphQL                                          | Request/response                                             |
|  16  | API Gateway -> Identity and Access                           | Sync               | OAuth2, OIDC                                           | Request/response                                             |
|  17  | Identity and access <-> External Identity Provider           | Sync               | OAuth2, OIDC                                           | Request/response                                             |



Диаграмма: [C2_option-A_direct-integration.puml](./C2_option-A_direct-integration.puml)

### Альтернативы

#### Описание решения

|  №   | Откуда -> Куда                              | Sync/Async | Протокол                | Гарантия доставки                               |
| :--: | ------------------------------------------- | ---------- | ----------------------- | ----------------------------------------------- |
|  1   | Camera Cloud -> Camera Gateway              | Async      | HTTPS, Vendor Cloud API | Best-effort, at least once (зависит от вендора) |
|  7   | Equipment Adapter -> Feeders/waterers cloud | Async      | HTTPS, Vendor Cloud API | At-least-once                                   |

Остальные пункты идентичны основному варианту.

Диаграмма: [C2_option-B_vendor-cloud-integration.puml](./C2_option-B_vendor-cloud-integration.puml)

### Недостатки, ограничения, риски

1. Невозможность конкурентной записи в SQLite. Переход на MySQL/PGSQL/Redis решит эту проблему, но вернет необходимость держать отдельный процесс БД на каждом агенте -- то есть именно то, от чего мы уходили, выбирая SQLite (F9: неограниченное число ферм, каждая добавляет свой сервер БД = рост эксплуатационных расходов пропорционально числу ферм). Более дешевая альтернатива, не меняющая саму БД -- сериализовать запись на уровне приложения: один writer-процесс владеет файлом, остальные контейнеры пишут через него.
2. Local Event Outbox на встроенной БД ограничен емкостью диска агента -- при очень длинном оффлайне может быть переполнение диска
3. Один и тот же инцидент может дойти до Incident Alerting & Escalation дважды: сразу через fast-path (MQTT) и повторно через обычную батч-синхронизацию (Local Event Outbox -> Sync Client -> Event Bus), так как событие пишется в outbox независимо от успеха fast-path. Без дедупликации по incident id на стороне Alerting это может привести к двойной эскалации одного и того же инцидента.
