# Запуск OBAuditor как systemd-сервис

## Что меняется по сравнению со старой schedule-моделью

Раньше бинарник запускался по cron / Task Scheduler один раз в минуту,
отрабатывал и выходил. Теперь — это долгоживущий процесс с тремя
рабочими горутинами:

- **logs** (`logsInterval`, 60s по умолчанию) — парсер observer.log / obproxy.log
- **ddl** (`ddlDclInterval`, 20s) — сбор DDL/DCL из GV$OB_SQL_AUDIT
- **rsyslog** (`rsyslogInterval`, 10s) — пересылка событий в syslog

Cleanup `sessions` и `ddl_dcl_audit_log` встроен в соответствующие
горутины и срабатывает каждые `cleanupEveryNCycles` циклов.

Watchdog следит, чтобы ни одна горутина не зависла дольше
`watchdogThreshold` (120s по умолчанию). При зависании процесс
вылетает с кодом 1, и systemd поднимает его заново.

## Установка

```bash
# Юзер и каталог
sudo useradd -r -s /bin/false -d /opt/obauditor obauditor
sudo mkdir -p /opt/obauditor
sudo cp obauditor /opt/obauditor/
sudo cp config.yaml /opt/obauditor/
sudo chown -R obauditor:obauditor /opt/obauditor
sudo chmod 600 /opt/obauditor/config.yaml   # пароль БД

# Unit
sudo cp obauditor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now obauditor
```

Если каталоги логов OB монтируются через SMB/NFS — добавь в unit:

```ini
[Unit]
RequiresMountsFor=/mnt/oceanbase_log /mnt/obproxy_log
```

## Эксплуатация

```bash
# Статус
sudo systemctl status obauditor

# Живой лог
sudo journalctl -u obauditor -f

# Лог за последний час
sudo journalctl -u obauditor --since "1 hour ago"

# Только ошибки
sudo journalctl -u obauditor -p err

# Перезапуск (после правки config.yaml)
sudo systemctl restart obauditor

# Остановка / запуск
sudo systemctl stop obauditor
sudo systemctl start obauditor

# Выключить автозапуск
sudo systemctl disable obauditor
```

## Мастер / резерв

На основном хосте — `ddlDclAuditMode: 1`. На резервном — `ddlDclAuditMode: 2`.
Резерв запустит Collect только если основной не обновлял
`ddl_dcl_audit_checkpoint.updated_at` дольше чем
`daemon.ddlDclStaleThreshold` (60s).

`logs` и `rsyslog` запускаются на обоих хостах независимо: они
парсят логи со своих машин и шлют свои события.

## Что в журнале

При `logLevel: INFO` — финальная строка каждого тика **только если
было что-то полезное** (вставили / отправили / почистили). Иначе
тишина (но мониторинг видит, что процесс жив через `systemctl`).

При `logLevel: DEBUG` — финальная строка каждого тика всегда + куча
деталей внутри (парсинг строк, SQL-запросы, состояние БД).

## Что делать если

**Сервис в `failed` после нескольких рестартов.** Превышен
`StartLimitBurst=5` в окне 5 минут. Глянь `journalctl`, разберись
с причиной, потом `sudo systemctl reset-failed obauditor` и
`sudo systemctl start obauditor`.

**Watchdog сработал.** В логе строка `[Watchdog] FATAL: goroutine "X"
heartbeat age ...`. Имя горутины подскажет где зависло. Обычно — БД
не отвечает дольше 2 минут.

**Хочется поменять интервалы.** Правишь `config.yaml`, делаешь
`sudo systemctl restart obauditor`. Hot-reload конфига не реализован.
