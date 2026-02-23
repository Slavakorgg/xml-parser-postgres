package errorHandler;

import java.io.IOException;
import java.sql.SQLException;

public final class ErrorHandler {

    private ErrorHandler() {
    }

    public static String toUserMessage(Throwable t) {
        if (t == null) return "Неизвестная ошибка.";
        String msg = t.getMessage();
        if (t instanceof IllegalArgumentException) {
            if (msg != null && msg.contains("Table not in XML"))
                return "Таблица не существует в XML. Доступные таблицы: currency, categories, offers.";
            return msg != null && !msg.isEmpty() ? msg : "Некорректный аргумент.";
        }
        if (t instanceof IllegalStateException) {
            if (msg != null && msg.startsWith("Schema changed for "))
                return "Изменилась структура таблицы. Пересоздайте таблицу (DROP TABLE) или приведите XML к текущей схеме.";
            if (msg != null && msg.startsWith("No rows in "))
                return "В секции нет данных (пустая таблица в XML).";
            return msg != null && !msg.isEmpty() ? msg : "Недопустимое состояние.";
        }
        if (t instanceof SQLException) {
            return "Ошибка БД: " + (msg != null ? msg : t.getClass().getSimpleName());
        }
        if (t instanceof IOException) {
            return "Ошибка чтения/сети (XML или ресурс): " + (msg != null ? msg : "проверьте URL и доступность.");
        }
        if (t instanceof org.xml.sax.SAXException) {
            return "Ошибка разбора XML: " + (msg != null ? msg : "некорректный документ.");
        }
        if (t instanceof javax.xml.parsers.ParserConfigurationException) {
            return "Ошибка настройки парсера XML: " + (msg != null ? msg : "");
        }
        return msg != null && !msg.isEmpty() ? msg : t.getClass().getSimpleName() + ".";
    }

    public static void print(Throwable t, boolean printStackTrace) {
        System.err.println("Ошибка: " + toUserMessage(t));
        if (printStackTrace) t.printStackTrace(System.err);
    }
}

