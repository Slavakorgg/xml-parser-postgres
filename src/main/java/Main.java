

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import parser.XmlParserService;

import java.io.IOException;
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
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runArgs(XmlParserService svc, String[] args) throws Exception {
        String cmd = args[0].toLowerCase();
        if ("--tables".equals(cmd) || "-t".equals(cmd)) {
            ArrayList<String> names = svc.getTableNames();
            System.out.println("Tables: " + String.join(", ", names));
        } else if ("--ddl".equals(cmd) || "-d".equals(cmd)) {
            if (args.length < 2) { System.out.println("Usage: --ddl <tableName>"); return; }
            System.out.println(svc.getTableDDL(args[1]));
        } else if ("--update".equals(cmd) || "-u".equals(cmd)) {
            if (args.length >= 2) svc.update(args[1]); else svc.update();
            System.out.println("Update done.");
        } else {
            System.out.println("Usage: [--tables] [--ddl <table>] [--update [table]]");
        }
    }

    private static void runMenu(XmlParserService svc) throws Exception {
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.println("1) getTableNames  2) getTableDDL(table)  3) update()  4) update(table)  0) exit");
            String line = in.nextLine().trim();
            if ("0".equals(line)) break;
            switch (line) {
                case "1" -> System.out.println("Tables: " + svc.getTableNames());
                case "2" -> {
                    System.out.print("Table name: ");
                    String t = in.nextLine().trim();
                    if (!t.isEmpty()) System.out.println(svc.getTableDDL(t));
                }
                case "3" -> { svc.update(); System.out.println("Updated all."); }
                case "4" -> {
                    System.out.print("Table name: ");
                    String t = in.nextLine().trim();
                    if (!t.isEmpty()) { svc.update(t); System.out.println("Updated " + t); }
                }
                default -> System.out.println("Unknown option.");
            }
        }
    }
}
