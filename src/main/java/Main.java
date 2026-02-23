import errorHandler.ErrorHandler;
import parser.XmlParserService;

import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        String jdbc = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/xml-parser");
        String user = System.getenv().getOrDefault("DB_USER", "postgres");
        String pass = System.getenv().getOrDefault("DB_PASSWORD", "password");
        String xmlUri = System.getenv().getOrDefault("XML_URI", "https://expro.ru/bitrix/catalog_export/export_Sai.xml");

        try (XmlParserService svc = new XmlParserService(jdbc, user, pass, xmlUri)) {
            if (args.length > 0) {
                runArgs(svc, args);
            } else {
                runMenu(svc);
            }
        } catch (Exception e) {
            ErrorHandler.print(e, true);
        }
    }

    private static void runArgs(XmlParserService svc, String[] args) {
        String cmd = args[0].toLowerCase();
        try {
            if ("--tables".equals(cmd) || "-t".equals(cmd)) {
                ArrayList<String> names = svc.getTableNames();
                System.out.println("Tables: " + String.join(", ", names));
            } else if ("--ddl".equals(cmd) || "-d".equals(cmd)) {
                if (args.length < 2) {
                    System.out.println("Usage: --ddl <tableName>");
                    return;
                }
                System.out.println(svc.getTableDDL(args[1]));
            } else if ("--update".equals(cmd) || "-u".equals(cmd)) {
                if (args.length >= 2) svc.update(args[1]);
                else svc.update();
                System.out.println("Update done.");
            } else {
                System.out.println("Usage: [--tables] [--ddl <table>] [--update [table]]");
            }
        } catch (Exception e) {
            System.err.println(ErrorHandler.toUserMessage(e));
        }
    }

    private static void runMenu(XmlParserService svc) {
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.println("1) getTableNames  2) getTableDDL(table)  3) update()  4) update(table)  0) exit");
            String line = in.nextLine().trim();
            if ("0".equals(line)) break;
            switch (line) {
                case "1" -> runMenuAction(() -> System.out.println("Tables: " + svc.getTableNames()));
                case "2" -> runMenuAction(() -> {
                    System.out.print("Table name: ");
                    String t = in.nextLine().trim();
                    if (!t.isEmpty()) System.out.println(svc.getTableDDL(t));
                });
                case "3" -> runMenuAction(() -> {
                    svc.update();
                    System.out.println("Обновлены все таблицы.");
                });
                case "4" -> runMenuAction(() -> {
                    System.out.print("Table name: ");
                    String t = in.nextLine().trim();
                    if (!t.isEmpty()) {
                        svc.update(t);
                        System.out.println("Обновлена таблица: " + t);
                    }
                });
                default -> System.out.println("Неизвестный пункт.");
            }
        }
    }

    @FunctionalInterface
    private interface MenuAction {
        void run() throws Exception;
    }

    private static void runMenuAction(MenuAction action) {
        try {
            action.run();
        } catch (Exception e) {
            System.err.println(ErrorHandler.toUserMessage(e));
        }
    }
}
