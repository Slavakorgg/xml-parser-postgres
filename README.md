# xml-parser-postgres

Небольшой сервис на Java, который:

- по HTTPS читает XML выгрузку каталога  
  `https://expro.ru/bitrix/catalog_export/export_Sai.xml`
- парсит её с помощью `groovy.xml.XmlSlurper`
- обновляет таблицы в PostgreSQL через JDBC

## Основные методы `XmlParserService`

Класс `parser.XmlParserService` реализует функции:

- **`ArrayList<String> getTableNames()`**  
  Читает XML и возвращает список логических имён таблиц:  
  `["currency", "categories", "offers"]`.

- **`String getTableDDL(String tableName)`**  
  Анализирует структуру соответствующей секции (`currencies`, `categories`, `offers`)  
  и строит SQL `CREATE TABLE IF NOT EXISTS "<tableName>" (...)` с колонками,
  соответствующими атрибутам в XML.  
  Для `offers` первичный ключ — `vendorCode`.

- **`void update()`**  
  Обновляет все таблицы, которые есть в XML:
  - при отсутствии таблицы в БД — создаёт её по `getTableDDL(...)`.
  - сверяет список колонок XML ↔ БД.
  - выполняет `INSERT ... ON CONFLICT (...) DO UPDATE` для строк.

- **`void update(String tableName)`**  
  Обновляет только одну таблицу (`currency`, `categories` или `offers`) по тем же правилам.

При изменении структуры методы update выбрасывают исключение.

### Уникальность `offers.vendorCode`

Cтолбец `offers.vendorCode` считается уникальным.  
В таблице `offers` он используется как первичный ключ

```sql
ON CONFLICT ("vendorCode") DO UPDATE ...
```

Поэтому:

- если в XML встречается несколько `<offer>` с одинаковым `vendorCode`,
  в таблице остаётся одна строка.
- при конфликте по `vendorCode` в таблице оказывается последняя запись из XML.

## Консольный интерфейс

Класс `parser.Main` реализует простой консольный интерфейс:

- при запуске показывает меню:

  ```text
  1) getTableNames  2) getTableDDL(table)  3) update()  4) update(table)  0) exit
  ```

- позволяет:
  - вывести список таблиц
  - сгенерировать DDL для указанной таблицы
  - обновить все таблицы
  - обновить одну выбранную таблицу

При ошибках (несуществующая таблица, проблемы с XML или БД)  
программа не завершается, а показывает сообщение и возвращается в главное меню.

## Обработка ошибок

Для централизованной обработки исключений используется класс ErrorHandler:

- переводит разные типы ошибок в понятные сообщения

