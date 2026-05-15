# Лабораторная работа №3 — dbt + DuckDB

**Цель:** научиться автоматизировать работу с хранилищами данных с использованием фреймворка **dbt (build data tools)**.

**Задачи:**
- развернуть dbt-приложение,
- разработать скрипты автоматизированной работы с данными (витрины),
- поставить работу скриптов на расписание.

---

## 1. Описание стека

| Компонент       | Выбор                                           | Обоснование                                                                                          |
| --------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Фреймворк       | **dbt-core 1.8** + адаптер **dbt-duckdb 1.8**   | dbt — стандарт индустрии для построения витрин в SQL.                                                |
| Хранилище       | **DuckDB** (файл `jaffle_shop.duckdb`)          | Не требует сервера, работает на Windows, отлично подходит для лабораторной.                          |
| Источник данных | **Jaffle Shop** (заказы кафе) в виде CSV-seeds  | Канонический учебный датасет dbt с тремя сущностями: клиенты, заказы, платежи.                       |
| Планировщик     | **Windows Task Scheduler**                      | Нативный планировщик ОС, не требует сторонних оркестраторов.                                         |

---

## 2. Структура проекта

```
Masha_laba/
├── venv/                              # Python 3.12 virtualenv с dbt
├── requirements.txt                   # зависимости (dbt-core, dbt-duckdb)
├── run_dbt.bat                        # обёртка для планировщика
├── dbt_run.log                        # лог прогонов (создаётся автоматически)
├── jaffle_shop.duckdb                 # файловая БД (создаётся dbt seed)
├── README.md                          # этот файл
└── jaffle_shop/                       # dbt-проект
    ├── dbt_project.yml
    ├── profiles.yml                   # подключение DuckDB
    ├── seeds/
    │   ├── raw_customers.csv          # 15 клиентов
    │   ├── raw_orders.csv             # 30 заказов
    │   └── raw_payments.csv           # 40 платежей
    └── models/
        ├── staging/                   # слой 1: очистка/нормализация
        │   ├── stg_customers.sql      # view
        │   ├── stg_orders.sql         # view
        │   ├── stg_payments.sql       # view
        │   └── schema.yml             # тесты unique / not_null / relationships / accepted_values
        └── marts/                     # слой 2: бизнес-витрины
            ├── customers.sql          # table — итоговая витрина клиентов
            └── schema.yml             # тесты витрины
```

### Логика витрины `customers`

Витрина агрегирует данные клиента:
- `customer_id`, `first_name`, `last_name` — атрибуты клиента;
- `first_order`, `most_recent_order` — дата первого/последнего заказа;
- `number_of_orders` — количество заказов;
- `customer_lifetime_value` — суммарная выручка по клиенту (в долларах, перевод из центов).

Линидж: `raw_customers / raw_orders / raw_payments` → `stg_customers / stg_orders / stg_payments` → `customers`.

---

## 3. Развёртывание

```powershell
# 1. Создать виртуальное окружение
py -3.12 -m venv venv

# 2. Установить зависимости
venv\Scripts\activate
pip install -r requirements.txt

# 3. Проверить установку
dbt --version
```

---

## 4. Ручной запуск пайплайна

```powershell
cd jaffle_shop

dbt seed --profiles-dir .   # загрузить CSV в DuckDB как таблицы main.raw_*
dbt run  --profiles-dir .   # построить 3 staging-view + 1 mart-table
dbt test --profiles-dir .   # проверить 14 schema-тестов
```

Ожидаемый результат:
- `dbt seed` → `Done. PASS=3`,
- `dbt run` → `Done. PASS=4`,
- `dbt test` → `Done. PASS=14`.

### Просмотр витрины

```powershell
venv\Scripts\python.exe -c "import duckdb; con=duckdb.connect('jaffle_shop.duckdb'); print(con.execute('SELECT * FROM main.customers ORDER BY customer_id').fetchall())"
```

### Документация и линидж (бонус)

```powershell
cd jaffle_shop
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Откроется браузер с интерактивным графом моделей.

---

## 5. Постановка на расписание (Windows Task Scheduler)

Файл `run_dbt.bat` в корне проекта активирует venv и запускает `dbt seed → run → test`, дописывая вывод в `dbt_run.log`.

### Вариант А — через CLI (`schtasks`)

Запустить **обычный** PowerShell (без админа):

```powershell
schtasks /Create `
  /SC DAILY `
  /TN "dbt_jaffle_shop" `
  /TR "c:\Users\Michael\python_projects\Masha_laba\run_dbt.bat" `
  /ST 09:00
```

Проверить:

```powershell
schtasks /Query /TN "dbt_jaffle_shop"
```

Запустить вручную (без ожидания расписания):

```powershell
schtasks /Run /TN "dbt_jaffle_shop"
```

Удалить:

```powershell
schtasks /Delete /TN "dbt_jaffle_shop" /F
```

### Вариант Б — через GUI (Планировщик заданий)

1. **Win+R** → `taskschd.msc`.
2. **Действие → Создать задачу…**
3. Вкладка **Общие**: имя `dbt_jaffle_shop`.
4. Вкладка **Триггеры → Создать**: ежедневно, время 09:00.
5. Вкладка **Действия → Создать**: «Запуск программы»,
   - Программа: `c:\Users\Michael\python_projects\Masha_laba\run_dbt.bat`
6. **OK** → задача появится в списке.

После регистрации можно правой кнопкой → **Выполнить** для проверки. Результат пишется в `dbt_run.log`.

---

## 6. Подтверждение работоспособности

После запуска пайплайна (вручную или через планировщик) `dbt_run.log` содержит:

```
====== dbt run started: <дата время> ======
... Done. PASS=3   (seed)
... Done. PASS=4   (run)
... Done. PASS=14  (test)
====== dbt run finished: <дата время> ======
```

Все три этапа `Completed successfully`, ошибок и FAIL-тестов нет — лаба сдана.
