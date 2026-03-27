# План реализации фичи Task Orchestration

## Статус документа

- Статус: design and implementation plan
- Область: новая feature area поверх текущего `codex-switch`
- Цель: production-grade scheduler для независимых задач с account-aware placement, thread continuity, follow-up routing и гарантированным отсутствием коллизий по identity occupancy

## 1. Контекст и проблема

Сейчас проект умеет:

- держать несколько изолированных Codex identities;
- выбирать лучший identity для новой работы по quota/health;
- продолжать существующий thread на другом identity;
- делать safe handoff и checkpoint fallback;
- retry запуск через другой identity при retryable auth/rate-limit ошибках.

Этого недостаточно для сценария:

- есть несколько проектов;
- внутри проектов есть несколько независимых задач;
- задачи можно запускать параллельно;
- желательно раскладывать новые задачи по разным аккаунтам, чтобы лимиты расходовались независимо;
- follow-up по существующей задаче должен по возможности продолжать тот же thread и тот же identity;
- если прежний identity занят, unhealthy или по квоте плохой, follow-up должен корректно уйти на другой identity через handoff или checkpoint fallback.

Ключевой gap текущей архитектуры:

- проект хранит только `current selection`, а не реальную занятость пула identities;
- проект не хранит самостоятельную модель `task`;
- проект не умеет диспетчеризовать несколько заданий без коллизий;
- project workspace isolation для параллельных задач отсутствует;
- scheduler-managed execution нельзя надёжно строить на blocking `codex exec`, потому что для follow-up критично знать `thread_id`, видеть события turn lifecycle и переживать обрывы клиентской CLI-сессии.

## 2. Цели

Новая feature должна обеспечить:

1. Надёжную постановку независимых задач в очередь.
2. Автоматическое распределение новых задач по свободным identities.
3. Жёсткий контроль занятости identities.
4. Follow-up по той же задаче с приоритетом на тот же identity и тот же thread.
5. Controlled handoff на другой identity, если прежний занят или недоступен.
6. Worktree isolation для параллельной работы в одном и том же проекте.
7. Durable state, переживающий падение CLI-клиента и restart scheduler daemon.
8. Детальную explainability: почему задача ушла именно на этот identity.
9. Полноценные тесты, retries, reconciliation и observability.

## 3. Не-цели

В первую production реализацию не входят:

- distributed multi-host scheduler;
- remote cluster execution;
- multi-tenant server mode;
- arbitrary user-defined scheduling DSL;
- semantic analysis prompt content для угадывания "какая задача тяжелее";
- динамический autoscaling identities;
- совместное выполнение нескольких активных задач на одном identity по умолчанию.

## 4. Ключевые архитектурные решения

### 4.1 Новый bounded context

Task Orchestration должен быть отдельным bounded context, а не расширением текущего `selection-state`.

Причина:

- текущий selection отвечает на вопрос "какой identity сейчас выбран";
- scheduler должен отвечать на вопрос "какие tasks активны, кто их владелец, какие identities заняты, какой task lineage имеет приоритет, какой run можно переassignить, какой worktree уже захвачен".

Это разные модели и разные consistency requirements.

### 4.2 App Server как основной runtime для scheduler-managed tasks

Для scheduler-managed tasks использовать App Server как primary runtime, а не blocking `codex exec`.

Обоснование:

- OpenAI описывает App Server как first-class integration surface для editor/IDE clients и систем, которым нужен устойчивый event stream;
- OpenAI описывает `Codex Exec` как lightweight one-off mode для CI, scripting и единичных automation scenarios;
- scheduler должен получать `thread_id`, `turn_id`, status changes, completion events, interruption support и переживать потерю клиентской CLI-сессии.

Это осознанная инженерная inference на основе официальных материалов OpenAI.

### 4.3 SQLite WAL как source of truth для orchestration state

Для orchestration state нужен транзакционный storage backend. Набор JSON-файлов для этой фичи недостаточен.

Решение:

- ввести `scheduler.db` на SQLite;
- включить WAL mode;
- использовать `rusqlite` с `bundled` SQLite;
- зафиксировать минимум SQLite `3.51.3` или новее.

Почему SQLite:

- инструмент остаётся single-host локальным orchestrator;
- официальная SQLite WAL-модель хорошо подходит для same-host concurrency;
- хранение leases, queue state и dispatch decisions требует транзакций и коротких atomic updates;
- это сильно проще и надёжнее, чем ad-hoc coordination через десятки JSON-файлов.

Почему не продолжать на JSON:

- нельзя безопасно claim-ить task и account в одной транзакции;
- трудно обеспечивать уникальность, FIFO, recovery и reconciliation;
- explainability и status inspection превращаются в разрозненные файлы без индексов;
- усложняется crash recovery.

## 5. Жёсткие инварианты системы

Эти инварианты обязательны и должны быть защищены кодом, базой и тестами.

1. В каждый момент времени у task lineage может быть не более одного `running` run.
2. По умолчанию у identity может быть не более одного `running` task run.
3. Follow-up не запускается параллельно с активным run той же задачи.
4. Account occupancy определяется только account lease, а не `selection-state`.
5. Потеря CLI-клиента не должна убивать scheduler-managed task автоматически.
6. Потеря scheduler daemon не должна терять tasks; после restart обязателен reconciliation pass.
7. Worktree не может одновременно принадлежать двум активным task runs.
8. Любое scheduler decision должно быть explainable и сохранено в durable event log.
9. Любой cross-identity follow-up должен идти либо через safe same-thread continuity, либо через checkpoint fallback; скрытого "просто начни новый thread без следа" быть не должно.
10. Retry не должен создавать два конкурентных владельца одной и той же задачи.

## 6. Доменная модель

### 6.1 Project

`Project` описывает рабочий контекст.

Поля:

- `project_id`
- `name`
- `repo_root`
- `execution_mode` (`git_worktree` | `copy_workspace`)
- `default_codex_args`
- `default_model_or_profile`
- `env_allowlist`
- `cleanup_policy`
- `created_at`
- `updated_at`

### 6.2 Task

`Task` это логическая единица работы, которая может жить дольше одного запуска.

Поля:

- `task_id`
- `project_id`
- `title`
- `status`
- `priority`
- `labels`
- `created_by`
- `created_at`
- `updated_at`
- `current_lineage_thread_id`
- `preferred_identity_id`
- `last_identity_id`
- `last_checkpoint_id`
- `last_completed_run_id`
- `pending_followup_count`

### 6.3 TaskRun

`TaskRun` это конкретная попытка выполнить initial task или follow-up.

Поля:

- `run_id`
- `task_id`
- `sequence_no`
- `run_kind` (`initial` | `follow_up` | `retry`)
- `status`
- `input_artifact_path`
- `requested_at`
- `assigned_identity_id`
- `assigned_worktree_id`
- `assigned_thread_id`
- `launch_mode` (`new_thread` | `resume_same_identity` | `resume_handoff` | `resume_checkpoint`)
- `retry_count`
- `started_at`
- `finished_at`
- `exit_code`
- `failure_kind`
- `failure_message`

### 6.4 AccountRuntime

`AccountRuntime` отвечает за текущее operational состояние identity внутри scheduler-а.

Поля:

- `identity_id`
- `state` (`free` | `reserved` | `launching` | `running` | `draining` | `offline`)
- `active_run_id`
- `active_count`
- `last_dispatch_at`
- `last_success_at`
- `last_failure_at`
- `updated_at`

### 6.5 AccountLease

`AccountLease` это authoritative occupancy record.

Поля:

- `identity_id`
- `lease_owner_id`
- `run_id`
- `lease_started_at`
- `heartbeat_at`
- `expires_at`
- `updated_at`

### 6.6 WorktreeLease

`WorktreeLease` защищает execution workspace от конкурентного использования.

Поля:

- `worktree_id`
- `project_id`
- `path`
- `state`
- `leased_run_id`
- `created_at`
- `heartbeat_at`
- `expires_at`

### 6.7 DispatchDecision

Каждое scheduler decision должно быть сохранено.

Поля:

- `decision_id`
- `run_id`
- `decision_kind`
- `selected_identity_id`
- `selected_worktree_id`
- `lineage_mode`
- `reason`
- `candidates_json`
- `policy_snapshot_json`
- `created_at`

## 7. Состояния и state machines

### 7.1 Task state machine

`queued -> running -> awaiting_followup -> completed`

Дополнительные переходы:

- `queued -> canceled`
- `running -> failed_retryable`
- `running -> failed_terminal`
- `running -> orphaned`
- `failed_retryable -> queued`
- `awaiting_followup -> running` при новом follow-up
- `orphaned -> queued` после reconciliation

### 7.2 TaskRun state machine

`pending_assignment -> assigned -> launching -> running -> completed`

Ошибочные и промежуточные состояния:

- `pending_assignment -> canceled`
- `assigned -> abandoned`
- `launching -> failed`
- `running -> timed_out`
- `running -> failed`
- `running -> handoff_pending`
- `handoff_pending -> running`
- `running -> orphaned`

### 7.3 AccountRuntime state machine

`free -> reserved -> launching -> running -> free`

Дополнительно:

- `free -> draining`
- `running -> draining`
- `any -> offline`
- `offline -> free` только через явную recovery path

## 8. Scheduler algorithm

### 8.1 Общий подход

Делать lexicographic scheduling, а не непрозрачный "магический score".

Порядок:

1. Hard eligibility filters.
2. Lineage affinity.
3. Occupancy / free-vs-busy.
4. Quota and health ranking.
5. Identity priority.
6. Deterministic tie-break.

### 8.2 Hard eligibility filters

Кандидат исключается полностью, если:

- identity disabled;
- identity manually disabled;
- penalty active;
- unauthenticated;
- quota отсутствует и policy требует свежего состояния;
- quota превышает hard-stop;
- scheduler policy запрещает oversubscribe, а account already leased;
- project execution workspace unavailable и нельзя выделить новый worktree.

### 8.3 Выбор для новых независимых задач

Для `initial` task runs:

- сначала выбирать свободные identities;
- внутри свободных prefer identities с нулевым active count;
- затем prefer больше headroom по quota;
- затем prefer identities, которые меньше всего недавно использовались;
- затем identity priority;
- затем deterministic tie-break by `task_id`.

Если свободных identities нет:

- либо оставлять задачу в queue;
- либо по policy `allow_oversubscribe_when_pool_full` запускать на лучшем занятом identity.

### 8.4 Выбор для follow-up

Для follow-up действует другой приоритет:

1. Если исходный identity свободен и lineage thread resumable там же, продолжать на том же identity.
2. Если исходный identity занят, но задача той же lineage уже выполняется, follow-up не стартует и становится pending behind the same task.
3. Если исходный identity unhealthy, draining или quota-bad, выбирать другой свободный identity и выполнять `resume_handoff`.
4. Если same-thread handoff unsafe, использовать checkpoint fallback и явно фиксировать это в decision log.

### 8.5 Explainability

Для каждого кандидата сохранять:

- `eligible`
- `rejection_reason`
- `occupancy_state`
- `active_count`
- `same_task_affinity`
- `same_identity_affinity`
- `quota_bucket`
- `remaining_headroom_percent`
- `priority`
- `selected`

Команда `tasks explain <task-id>` обязана печатать эти поля.

## 9. Worktree isolation

Параллельные задачи не должны писать в один и тот же рабочий каталог.

Правило:

- для git-проектов всегда создавать отдельный worktree на каждый active run;
- для follow-up той же задачи можно переиспользовать прежний worktree, если run lineage serial и worktree не очищен;
- если worktree утерян, удалён или повреждён, follow-up должен либо восстановить workspace policy-compliant способом, либо перевести run в failed/orphaned.

Путь по умолчанию:

- `shared/task-worktrees/<project-id>/<task-id>/<run-id>/`

Cleanup:

- immediate cleanup только для terminal failed runs без follow-up потребности;
- TTL cleanup для completed runs;
- отдельная команда `scheduler gc`.

## 10. Storage design

### 10.1 Физическое размещение

Новые пути:

- `shared/scheduler/scheduler.db`
- `shared/task-artifacts/`
- `shared/task-worktrees/`
- `shared/scheduler-events/` опционально, если оставить filesystem mirror для отладки

### 10.2 Таблицы SQLite

Минимально нужны:

- `projects`
- `tasks`
- `task_runs`
- `task_run_inputs`
- `account_runtime`
- `account_leases`
- `worktrees`
- `worktree_leases`
- `dispatch_decisions`
- `scheduler_events`

### 10.3 Индексы

Обязательные индексы:

- `tasks(project_id, status, priority, created_at)`
- `task_runs(task_id, sequence_no)`
- `task_runs(status, requested_at)`
- `account_runtime(state, updated_at)`
- `account_leases(identity_id)`
- `worktrees(project_id, state)`
- `dispatch_decisions(run_id)`

### 10.4 Транзакционные правила

Одной транзакцией должны происходить:

- claim queued run;
- claim free identity;
- claim or create worktree;
- persist dispatch decision;
- transition run state в `assigned`.

Это критично для real no-collision scheduling.

## 11. Runtime architecture

### 11.1 Scheduler daemon

Нужен отдельный долгоживущий process:

- `dispatcher loop`
- `supervisor monitor loop`
- `reconciler loop`
- `quota refresh loop`
- `cleanup/gc loop`

### 11.2 Supervisor model

Каждый active run должен иметь local supervisor.

Supervisor:

- стартует task runtime;
- держит process group;
- пишет heartbeat в `task_run_lease`;
- обновляет `account_lease`;
- сохраняет `thread_id` и `turn_id`;
- capture-ит stdout/stderr/events/artifacts;
- умеет interrupt, timeout и cancel;
- при graceful finish освобождает leases и обновляет terminal state.

### 11.3 Crash recovery

При старте daemon обязан:

- открыть SQLite;
- проверить lock singleton;
- просканировать `assigned/launching/running` runs;
- проверить lease freshness;
- проверить existence child PIDs и artifact markers;
- перевести zombie runs в `orphaned`;
- по policy requeue orphans.

## 12. Интеграция с текущими модулями

### 12.1 Переиспользовать существующее

Оставить и использовать:

- `src/identity_selector/mod.rs` как quota/health ranker;
- `src/automatic_handoff/mod.rs` как high-level automatic handoff service;
- `src/continuation/mod.rs` как safe same-thread vs checkpoint fallback logic;
- `src/thread_leases/mod.rs` и `src/storage/thread_lease_store.rs` только для thread ownership, не для account occupancy;
- `src/decision_log/mod.rs` как шаблон explainability/event logging.

### 12.2 Расширить `codex_rpc`

Нужно расширить `src/codex_rpc/mod.rs`.

Новый runtime client должен уметь:

- инициализацию App Server session;
- start new thread/message;
- subscribe to thread/turn/item events;
- interrupt/cancel;
- read thread after completion;
- persist canonical `thread_id`.

### 12.3 Не использовать `selection-state` как runtime occupancy

`selection-state` остаётся operator-facing state, а не scheduler truth.

Это принципиальное решение.

## 13. CLI surface

Новые команды:

- `codex-switch projects add`
- `codex-switch projects list`
- `codex-switch projects show <project>`
- `codex-switch tasks submit`
- `codex-switch tasks follow-up <task-id>`
- `codex-switch tasks list`
- `codex-switch tasks status <task-id>`
- `codex-switch tasks show <task-id>`
- `codex-switch tasks logs <task-id>`
- `codex-switch tasks explain <task-id>`
- `codex-switch tasks cancel <task-id>`
- `codex-switch tasks retry <task-id>`
- `codex-switch scheduler run`
- `codex-switch scheduler tick --once`
- `codex-switch scheduler health`
- `codex-switch scheduler gc`

Основные флаги `tasks submit`:

- `--project`
- `--title`
- `--prompt-file` или `--prompt`
- `--priority`
- `--labels`
- `--max-runtime`
- `--queue-if-busy`
- `--allow-oversubscribe`
- `--affinity spread|prefer_same_identity|prefer_project_locality`

## 14. Retry, timeout, idempotency

Правила:

- scheduler retry-ит только retryable failures;
- использовать exponential backoff с jitter;
- cap on retries;
- long-running tasks обязаны иметь supervisor heartbeat;
- все side-effecting transitions должны быть idempotent;
- повторный dispatch того же run не должен создавать новый logical run, только новый attempt record или retry transition внутри того же run.

## 15. Observability

Нужны:

- append-only `scheduler_events`;
- per-run event timeline;
- `dispatch_decisions`;
- queue depth metrics;
- active/free identities metrics;
- handoff rate;
- checkpoint fallback rate;
- lease renewal latency;
- orphan recovery count;
- worktree cleanup lag.

CLI должен давать:

- `tasks show` с lineage summary;
- `tasks explain` с candidate breakdown;
- `scheduler health` с summary по queue, leases и stale runs.

## 16. Testing strategy

### 16.1 Unit tests

- ranking logic;
- affinity selection;
- lease expiration;
- reconciliation decisions;
- retry policy;
- state transitions;
- deterministic tie-break.

### 16.2 Integration tests

С fake App Server и test SQLite:

- 5 независимых задач распределяются по 5 свободным identities;
- если свободных 3, остальные 2 либо queue, либо oversubscribe по policy;
- follow-up уходит на тот же identity при свободном owner;
- follow-up queue-ится, если lineage уже running;
- follow-up handoff-ится на другой identity, если исходный занят или unhealthy;
- checkpoint fallback корректно создаётся и логируется;
- scheduler daemon restart не теряет running tasks;
- stale account lease корректно истекает;
- worktree cleanup не удаляет workspace активного run.

### 16.3 Property tests

Инварианты:

- не более одного running run на task;
- не более `max_active_runs_per_identity` active runs на identity;
- активный worktree не принадлежит двум tasks одновременно;
- requeue не создаёт duplicate account ownership.

## 17. План реализации по этапам

### Этап A. Foundations

- добавить новые domain модели;
- добавить новые ошибки;
- добавить новые paths;
- добавить SQLite backend и migrations;
- добавить scheduler config model.

### Этап B. Storage and leases

- реализовать project/task/run stores;
- реализовать account/worktree leases;
- реализовать transactional claim API;
- реализовать scheduler event store.

### Этап C. Runtime client

- расширить `codex_rpc` до long-lived App Server client;
- реализовать event subscription;
- реализовать thread ID capture;
- реализовать graceful cancel/interrupt.

### Этап D. Scheduler daemon

- singleton process lock;
- dispatcher loop;
- supervisor registry;
- reconciler loop;
- quota refresh integration;
- cleanup loop.

### Этап E. Worktree management

- git worktree allocator;
- reuse policy for same task lineage;
- cleanup policy;
- recovery on corrupted/missing worktree.

### Этап F. CLI

- projects commands;
- tasks commands;
- scheduler commands;
- explain/status rendering.

### Этап G. Reliability and observability

- metrics surface;
- decision logs;
- operator diagnostics;
- crash recovery tests;
- long-run soak tests.

## 18. Migration and compatibility

Backward compatibility rules:

- текущие команды `select`, `exec`, `continue`, `threads` не ломать;
- scheduler вводить как новый mode;
- legacy JSON stores пока не удалять;
- `selection-state` не переопределять поведением scheduler-а;
- feature flag `scheduler_v1` держать до завершения dogfooding и soak testing.

## 19. Открытые инженерные вопросы

1. Точный набор App Server methods и notifications в установленном `codex-cli 0.115.0` нужно зафиксировать контрактным тестом.
2. Нужно решить, будет ли `TaskRun` иметь отдельные `attempts`, или retry будет моделироваться повторным переходом того же run.
3. Нужно решить, допускается ли configurable `max_active_runs_per_identity > 1` в будущем.
4. Нужно решить, будет ли queue policy по умолчанию `queue-if-busy=true` или `allow-oversubscribe-when-pool-full=true`.
5. Нужно решить, будут ли project-level defaults хранить model/profile/sandbox policy.

## 20. Использованные внешние источники

- OpenAI App Server architecture and event-driven Codex integration:
  - https://openai.com/index/unlocking-the-codex-harness/
- OpenAI Codex launch article with explicit parallel task assignment framing:
  - https://openai.com/ru-RU/index/introducing-codex/
- SQLite WAL official documentation:
  - https://sqlite.org/wal.html
- SQLite release history:
  - https://www.sqlite.org/changes.html
- Kubernetes Lease concept:
  - https://kubernetes.io/docs/concepts/architecture/leases/
- Celery task execution guidance:
  - https://docs.celeryq.dev/en/v5.1.2/userguide/tasks.html
- GitHub Actions concurrency groups:
  - https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency
- Git worktree official documentation:
  - https://git-scm.com/docs/git-worktree.html

## 21. Итоговая рекомендация

Эта фича должна быть реализована как production-grade local scheduler с durable state, account leases, worktree isolation, App Server runtime и explainable dispatch decisions.

Правильная ментальная модель:

- `codex-switch` больше не просто переключает identities;
- `codex-switch` становится orchestrator-ом локального пула Codex workers;
- identity selection остаётся важной, но становится только одним из этапов scheduler decision;
- follow-up routing становится не ad-hoc действием оператора, а частью формальной task lineage model.
