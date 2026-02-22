package parser;

import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.NodeChild;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class XmlParserService implements AutoCloseable {

    private final Connection conn;
    private final XmlSlurper slurper;
    private final String uri;

    public XmlParserService(String jdbcUrl, String user, String password, String xmlUri)
            throws SQLException, SAXException, ParserConfigurationException {
        this.conn = DriverManager.getConnection(jdbcUrl, user, password);
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
        SAXParser saxParser = factory.newSAXParser();
        this.slurper = new XmlSlurper(saxParser);
        this.uri = xmlUri;
    }

    private GPathResult parse() throws SAXException, IOException {
        return (GPathResult) slurper.parse(uri);
    }

    // Возвращает названия таблиц из XML: currency, categories, offers
    public ArrayList<String> getTableNames() throws ParserConfigurationException, SAXException, IOException {
        GPathResult xml = parse();
        ArrayList<String> out = new ArrayList<>();
        for (Object c : (Iterable<?>) xml.children().children()) {
            GPathResult node = (GPathResult) c;
            String tag = node.name();
            if ("currencies".equals(tag)) out.add("currency");
            else if ("categories".equals(tag)) out.add("categories");
            else if ("offers".equals(tag)) out.add("offers");
        }
        return out;
    }

    // Создаёт SQL для создания таблицы по структуре из XML
    public String getTableDDL(String tableName) throws SAXException, IOException {
        GPathResult xml = parse();
        String tag = tableTag(tableName);
        GPathResult section = null;
        for (Object c : (Iterable<?>) xml.children().children()) {
            GPathResult node = (GPathResult) c;
            if (tag.equals(node.name())) {
                section = node;
                break;
            }
        }
        if (section == null) throw new IllegalArgumentException("Table not in XML: " + tableName);

        String expectedRowTag = rowTagName(tableName);
        GPathResult first = null;
        for (Object c : (Iterable<?>) section.children()) {
            GPathResult node = (GPathResult) c;
            String n = node.name();
            if (n != null && !n.isEmpty() && (expectedRowTag == null || expectedRowTag.equalsIgnoreCase(n))) {
                first = node;
                break;
            }
        }
        if (first == null) throw new IllegalStateException("No rows in: " + tableName);

        Map<String, String> cols = new LinkedHashMap<>();
        for (Object c : (Iterable<?>) section.children()) {
            GPathResult node = (GPathResult) c;
            String n = node.name();
            if (n == null || n.isEmpty() || (expectedRowTag != null && !expectedRowTag.equalsIgnoreCase(n))) continue;
            Map<String, String> attrs = node instanceof NodeChild ? ((NodeChild) node).attributes() : null;
            if (attrs != null) for (Map.Entry<String, String> e : attrs.entrySet()) {
                String col = safe(e.getKey());
                if (!cols.containsKey(col)) cols.put(col, type(e.getValue()));
            }
            for (Object ch : (Iterable<?>) node.children()) {
                GPathResult child = (GPathResult) ch;
                String cn = child.name();
                if (cn != null && !cn.isEmpty()) {
                    String col = safe(cn);
                    if (!cols.containsKey(col)) cols.put(col, "TEXT");
                }
            }
            String text = node.text().trim();
            if (!text.isEmpty() && !cols.containsKey("name")) cols.put("name", "TEXT");
        }

        String pk = "offers".equals(tableName) ? "vendorCode" : "id";
        if (!cols.containsKey(pk)) pk = cols.keySet().iterator().next();

        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (");
        List<String> defs = new ArrayList<>();
        for (Map.Entry<String, String> e : cols.entrySet()) defs.add("\"" + e.getKey() + "\" " + e.getValue());
        ddl.append(String.join(", ", defs)).append(", PRIMARY KEY (\"").append(pk).append("\"))");
        return ddl.toString();
    }

    // Обновляет все таблицы. При смене структуры — exception
    public void update() throws Exception {
        for (String t : getTableNames()) update(t);
    }

    /// Обновляет одну таблицу. При смене структуры — exception
    public void update(String tableName) throws Exception {
        GPathResult xml = parse();
        String tag = tableTag(tableName);
        GPathResult section = null;
        for (Object c : (Iterable<?>) xml.children().children()) {
            GPathResult node = (GPathResult) c;
            if (tag.equals(node.name())) { section = node; break; }
        }
        if (section == null) throw new IllegalArgumentException("Table not in XML: " + tableName);

        String expectedRowTag = rowTagName(tableName);
        List<Map<String, String>> rows = new ArrayList<>();
        for (Object c : (Iterable<?>) section.children()) {
            GPathResult node = (GPathResult) c;
            String nodeName = node.name();
            if (nodeName == null || nodeName.isEmpty()) continue;
            if (expectedRowTag != null && !expectedRowTag.equalsIgnoreCase(nodeName)) continue;
            Map<String, String> row = rowToMap(node);
            if ("offers".equals(tableName)) {
                String vendorCode = row.get("vendorCode");
                if (vendorCode == null || vendorCode.isBlank()) continue;
            }
            if (!row.isEmpty()) rows.add(row);
        }
        if (rows.isEmpty()) return;

        Set<String> xmlCols = new java.util.LinkedHashSet<>(rows.get(0).keySet());
        for (int i = 1; i < rows.size(); i++) xmlCols.addAll(rows.get(i).keySet());
        String pk = "offers".equals(tableName) ? "vendorCode" : "id";
        if (!xmlCols.contains(pk)) {
            for (String k : xmlCols) if ("id".equalsIgnoreCase(k) || "vendorcode".equalsIgnoreCase(k)) { pk = k; break; }
        }

        Set<String> dbCols = getColumns(tableName);
        if (dbCols.isEmpty()) {
            conn.createStatement().execute(getTableDDL(tableName));
            dbCols = getColumns(tableName);
        }
        if (!dbCols.containsAll(xmlCols) || !xmlCols.containsAll(dbCols))
            throw new IllegalStateException("Schema changed for " + tableName);

        final String pkColumn = pk;
        List<String> colList = new ArrayList<>(xmlCols);
        String cols = colList.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "));
        String vals = colList.stream().map(s -> "?").collect(Collectors.joining(", "));
        String onConflict = "\"" + pkColumn + "\"";
        String doUpdate = colList.stream().filter(s -> !s.equalsIgnoreCase(pkColumn))
                .map(s -> "\"" + s + "\" = EXCLUDED.\"" + s + "\"").collect(Collectors.joining(", "));
        String sql = "INSERT INTO \"" + tableName + "\" (" + cols + ") VALUES (" + vals + ")"
                + " ON CONFLICT (" + onConflict + ") DO UPDATE SET " + doUpdate;

        final int batchChunk = 500;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int r = 0; r < rows.size(); r++) {
                Map<String, String> row = rows.get(r);
                for (int i = 0; i < colList.size(); i++)
                    setTypedParam(ps, i + 1, row.get(colList.get(i)));
                ps.addBatch();
                if ((r + 1) % batchChunk == 0 || r == rows.size() - 1) {
                    ps.executeBatch();
                }
            }
        }
    }

    private String tableTag(String tableName) {
        return "currency".equals(tableName) ? "currencies" : tableName;
    }

    private String rowTagName(String tableName) {
        if ("offers".equals(tableName)) return "offer";
        if ("currency".equals(tableName)) return "currency";
        if ("categories".equals(tableName)) return "category";
        return null;
    }

    private static String safe(String s) {
        return s == null ? "" : s.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private static String type(String v) {
        if (v == null || v.isEmpty()) return "TEXT";
        if (v.matches("-?\\d+")) return "BIGINT";
        if (v.matches("-?\\d+(\\.\\d+)?")) return "NUMERIC(20,6)";
        if ("true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v)) return "BOOLEAN";
        return "TEXT";
    }

    private static void setTypedParam(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setObject(index, null);
            return;
        }
        if ("true".equalsIgnoreCase(value)) {
            ps.setBoolean(index, true);
            return;
        }
        if ("false".equalsIgnoreCase(value)) {
            ps.setBoolean(index, false);
            return;
        }
        if (value.matches("-?\\d+")) {
            ps.setLong(index, Long.parseLong(value));
            return;
        }
        if (value.matches("-?\\d+(\\.\\d+)?")) {
            ps.setBigDecimal(index, new java.math.BigDecimal(value));
            return;
        }
        ps.setString(index, value);
    }

    private Map<String, String> rowToMap(GPathResult row) {
        Map<String, String> m = new LinkedHashMap<>();
        Map<String, String> a = row instanceof NodeChild ? ((NodeChild) row).attributes() : null;
        if (a != null) for (Map.Entry<String, String> e : a.entrySet())
            m.put(safe(e.getKey()), e.getValue());
        for (Object c : (Iterable<?>) row.children()) {
            GPathResult ch = (GPathResult) c;
            String n = ch.name();
            if (n != null && !n.isEmpty()) {
                String col = safe(n);
                if (!m.containsKey(col)) m.put(col, ch.text().trim());
            }
        }
        String text = row.text().trim();
        if (!text.isEmpty()) m.put("name", text);
        return m;
    }

    private Set<String> getColumns(String tableName) throws SQLException {
        Set<String> set = new java.util.HashSet<>();
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) set.add(rs.getString("COLUMN_NAME"));
        }
        return set;
    }

    public void close() throws SQLException {
        conn.close();
    }
}
