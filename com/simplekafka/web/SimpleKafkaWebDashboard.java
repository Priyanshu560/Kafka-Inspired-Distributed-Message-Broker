package com.simplekafka.web;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.client.SimpleKafkaClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Modern Web Dashboard for SimpleKafka Cluster
 */
public class SimpleKafkaWebDashboard {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaWebDashboard.class.getName());
    private static final int PORT = 8080;
    private static final String BOOTSTRAP_HOST = "localhost";
    private static final int BOOTSTRAP_PORT = 9091;

    private final SimpleKafkaClient client;

    public SimpleKafkaWebDashboard() {
        this.client = new SimpleKafkaClient(BOOTSTRAP_HOST, BOOTSTRAP_PORT);
    }

    public void start() throws IOException {
        try {
            client.initialize();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Initial metadata fetch warning: " + e.getMessage());
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/", new DashboardHtmlHandler());
        server.createContext("/api/cluster", new ClusterApiHandler());
        server.createContext("/api/produce", new ProduceApiHandler());
        server.createContext("/api/consume", new ConsumeApiHandler());
        server.createContext("/api/create-topic", new CreateTopicApiHandler());

        server.start();
        System.out.println("==================================================================");
        System.out.println("  SimpleKafka Web Dashboard is running!");
        System.out.println("  Local Host URL: http://localhost:" + PORT);
        System.out.println("==================================================================");
    }

    public static void main(String[] args) {
        try {
            new SimpleKafkaWebDashboard().start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start Web Dashboard", e);
        }
    }

    private class ClusterApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendCors(exchange);
                return;
            }

            try {
                client.refreshMetadata();
                Map<Integer, BrokerInfo> brokers = client.getBrokers();
                Map<String, SimpleKafkaClient.TopicMetadata> topics = client.getTopicMetadata();

                StringBuilder json = new StringBuilder("{");
                json.append("\"success\":true,");
                json.append("\"brokers\":[");
                int bIndex = 0;
                for (BrokerInfo b : brokers.values()) {
                    if (bIndex++ > 0) json.append(",");
                    json.append(String.format("{\"id\":%d,\"host\":\"%s\",\"port\":%d}",
                            b.getId(), escapeJson(b.getHost()), b.getPort()));
                }
                json.append("],\"topics\":[");
                int tIndex = 0;
                for (SimpleKafkaClient.TopicMetadata t : topics.values()) {
                    if (tIndex++ > 0) json.append(",");
                    json.append("{\"name\":\"").append(escapeJson(t.getName())).append("\",\"partitions\":[");
                    int pIndex = 0;
                    for (SimpleKafkaClient.PartitionInfo p : t.getPartitions()) {
                        if (pIndex++ > 0) json.append(",");
                        json.append(String.format("{\"id\":%d,\"leader\":%d,\"followers\":%s}",
                                p.getId(), p.getLeader(), p.getFollowers().toString()));
                    }
                    json.append("]}");
                }
                json.append("]}");

                sendJsonResponse(exchange, 200, json.toString());
            } catch (Exception e) {
                String errorJson = String.format("{\"success\":false,\"error\":\"%s\"}", escapeJson(e.getMessage()));
                sendJsonResponse(exchange, 500, errorJson);
            }
        }
    }

    private class ProduceApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendCors(exchange);
                return;
            }
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = readStream(exchange.getRequestBody());
                Map<String, String> params = parseSimpleJson(body);

                String topic = params.get("topic");
                int partition = Integer.parseInt(params.getOrDefault("partition", "0"));
                String message = params.getOrDefault("message", "");

                if (topic == null || topic.trim().isEmpty() || message.isEmpty()) {
                    sendJsonResponse(exchange, 400, "{\"success\":false,\"error\":\"Topic and message are required\"}");
                    return;
                }

                long offset = client.send(topic, partition, message.getBytes(StandardCharsets.UTF_8));
                String res = String.format("{\"success\":true,\"topic\":\"%s\",\"partition\":%d,\"offset\":%d}",
                        escapeJson(topic), partition, offset);
                sendJsonResponse(exchange, 200, res);
            } catch (Exception e) {
                String errorJson = String.format("{\"success\":false,\"error\":\"%s\"}", escapeJson(e.getMessage()));
                sendJsonResponse(exchange, 500, errorJson);
            }
        }
    }

    private class ConsumeApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendCors(exchange);
                return;
            }

            try {
                String query = exchange.getRequestURI().getQuery();
                Map<String, String> params = parseQueryParams(query);

                String topic = params.get("topic");
                int partition = Integer.parseInt(params.getOrDefault("partition", "0"));
                long offset = Long.parseLong(params.getOrDefault("offset", "0"));
                int maxBytes = 1024 * 1024;

                if (topic == null || topic.trim().isEmpty()) {
                    sendJsonResponse(exchange, 400, "{\"success\":false,\"error\":\"Topic is required\"}");
                    return;
                }

                List<byte[]> rawMessages = client.fetch(topic, partition, offset, maxBytes);
                StringBuilder json = new StringBuilder("{");
                json.append("\"success\":true,");
                json.append("\"topic\":\"").append(escapeJson(topic)).append("\",");
                json.append("\"partition\":").append(partition).append(",");
                json.append("\"nextOffset\":").append(offset + rawMessages.size()).append(",");
                json.append("\"messages\":[");
                for (int i = 0; i < rawMessages.size(); i++) {
                    if (i > 0) json.append(",");
                    String msgStr = new String(rawMessages.get(i), StandardCharsets.UTF_8);
                    json.append(String.format("{\"offset\":%d,\"content\":\"%s\"}",
                            offset + i, escapeJson(msgStr)));
                }
                json.append("]}");

                sendJsonResponse(exchange, 200, json.toString());
            } catch (Exception e) {
                String errorJson = String.format("{\"success\":false,\"error\":\"%s\"}", escapeJson(e.getMessage()));
                sendJsonResponse(exchange, 500, errorJson);
            }
        }
    }

    private class CreateTopicApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendCors(exchange);
                return;
            }
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = readStream(exchange.getRequestBody());
                Map<String, String> params = parseSimpleJson(body);

                String topic = params.get("topic");
                int partitions = Integer.parseInt(params.getOrDefault("partitions", "3"));
                short replication = Short.parseShort(params.getOrDefault("replication", "2"));

                if (topic == null || topic.trim().isEmpty()) {
                    sendJsonResponse(exchange, 400, "{\"success\":false,\"error\":\"Topic name is required\"}");
                    return;
                }

                boolean ok = client.createTopic(topic.trim(), partitions, replication);
                if (ok) {
                    // Small delay to let follower brokers load topic partitions
                    Thread.sleep(500);
                    sendJsonResponse(exchange, 200, "{\"success\":true,\"message\":\"Topic created successfully\"}");
                } else {
                    sendJsonResponse(exchange, 500, "{\"success\":false,\"error\":\"Failed to create topic on broker\"}");
                }
            } catch (Exception e) {
                String errorJson = String.format("{\"success\":false,\"error\":\"%s\"}", escapeJson(e.getMessage()));
                sendJsonResponse(exchange, 500, errorJson);
            }
        }
    }

    private class DashboardHtmlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] htmlBytes = DASHBOARD_HTML.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, htmlBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(htmlBytes);
            }
        }
    }

    private static void sendJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sendCors(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
        exchange.sendResponseHeaders(204, -1);
    }

    private static String readStream(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[1024];
        int nRead;
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toString(StandardCharsets.UTF_8.name());
    }

    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isEmpty()) return map;
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                map.put(pair.substring(0, idx), pair.substring(idx + 1));
            }
        }
        return map;
    }

    private static Map<String, String> parseSimpleJson(String json) {
        Map<String, String> map = new HashMap<>();
        if (json == null || json.trim().isEmpty()) return map;
        String clean = json.trim();
        if (clean.startsWith("{")) clean = clean.substring(1);
        if (clean.endsWith("}")) clean = clean.substring(0, clean.length() - 1);

        String[] parts = clean.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        for (String part : parts) {
            String[] kv = part.split(":(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (kv.length == 2) {
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                String value = kv[1].trim().replaceAll("^\"|\"$", "");
                map.put(key, value);
            }
        }
        return map;
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static final String DASHBOARD_HTML = "<!DOCTYPE html>\n" +
            "<html lang=\"en\">\n" +
            "<head>\n" +
            "  <meta charset=\"UTF-8\" />\n" +
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />\n" +
            "  <title>SimpleKafka Cluster Dashboard</title>\n" +
            "  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n" +
            "  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\n" +
            "  <link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap\" rel=\"stylesheet\">\n" +
            "  <style>\n" +
            "    :root {\n" +
            "      --bg: #090d16;\n" +
            "      --card-bg: rgba(18, 24, 38, 0.75);\n" +
            "      --card-border: rgba(255, 255, 255, 0.08);\n" +
            "      --accent: #6366f1;\n" +
            "      --accent-glow: rgba(99, 102, 241, 0.3);\n" +
            "      --accent-hover: #4f46e5;\n" +
            "      --success: #10b981;\n" +
            "      --success-glow: rgba(16, 185, 129, 0.2);\n" +
            "      --warning: #f59e0b;\n" +
            "      --text: #f3f4f6;\n" +
            "      --text-muted: #9ca3af;\n" +
            "      --mono-font: 'JetBrains Mono', monospace;\n" +
            "    }\n" +
            "    * { box-sizing: border-box; margin: 0; padding: 0; }\n" +
            "    body {\n" +
            "      background: radial-gradient(circle at 50% -20%, #1e1b4b, #090d16 70%);\n" +
            "      color: var(--text);\n" +
            "      font-family: 'Plus Jakarta Sans', sans-serif;\n" +
            "      min-height: 100vh;\n" +
            "      padding: 2rem;\n" +
            "    }\n" +
            "    .container {\n" +
            "      max-width: 1300px;\n" +
            "      margin: 0 auto;\n" +
            "    }\n" +
            "    header {\n" +
            "      display: flex;\n" +
            "      justify-content: space-between;\n" +
            "      align-items: center;\n" +
            "      padding-bottom: 2rem;\n" +
            "      border-bottom: 1px solid var(--card-border);\n" +
            "      margin-bottom: 2rem;\n" +
            "    }\n" +
            "    .logo-group {\n" +
            "      display: flex;\n" +
            "      align-items: center;\n" +
            "      gap: 1rem;\n" +
            "    }\n" +
            "    .logo-badge {\n" +
            "      background: linear-gradient(135deg, #6366f1, #a855f7);\n" +
            "      width: 44px;\n" +
            "      height: 44px;\n" +
            "      border-radius: 12px;\n" +
            "      display: flex;\n" +
            "      align-items: center;\n" +
            "      justify-content: center;\n" +
            "      font-weight: 800;\n" +
            "      font-size: 1.4rem;\n" +
            "      box-shadow: 0 0 25px var(--accent-glow);\n" +
            "    }\n" +
            "    h1 { font-size: 1.6rem; font-weight: 800; letter-spacing: -0.5px; }\n" +
            "    .subtitle { color: var(--text-muted); font-size: 0.85rem; margin-top: 2px; }\n" +
            "    .status-badge {\n" +
            "      display: flex;\n" +
            "      align-items: center;\n" +
            "      gap: 8px;\n" +
            "      padding: 6px 14px;\n" +
            "      background: var(--success-glow);\n" +
            "      border: 1px solid var(--success);\n" +
            "      color: #34d399;\n" +
            "      border-radius: 9999px;\n" +
            "      font-size: 0.85rem;\n" +
            "      font-weight: 600;\n" +
            "    }\n" +
            "    .pulse-dot {\n" +
            "      width: 8px;\n" +
            "      height: 8px;\n" +
            "      border-radius: 50%;\n" +
            "      background: #10b981;\n" +
            "      box-shadow: 0 0 8px #10b981;\n" +
            "      animation: pulse 2s infinite;\n" +
            "    }\n" +
            "    @keyframes pulse {\n" +
            "      0% { transform: scale(0.95); opacity: 0.8; }\n" +
            "      50% { transform: scale(1.4); opacity: 1; }\n" +
            "      100% { transform: scale(0.95); opacity: 0.8; }\n" +
            "    }\n" +
            "    .grid-metrics {\n" +
            "      display: grid;\n" +
            "      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));\n" +
            "      gap: 1.2rem;\n" +
            "      margin-bottom: 2rem;\n" +
            "    }\n" +
            "    .metric-card {\n" +
            "      background: var(--card-bg);\n" +
            "      backdrop-filter: blur(16px);\n" +
            "      border: 1px solid var(--card-border);\n" +
            "      border-radius: 16px;\n" +
            "      padding: 1.4rem;\n" +
            "      transition: transform 0.2s, border-color 0.2s;\n" +
            "    }\n" +
            "    .metric-card:hover { border-color: rgba(99, 102, 241, 0.4); transform: translateY(-2px); }\n" +
            "    .metric-label { font-size: 0.8rem; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.5px; }\n" +
            "    .metric-val { font-size: 2rem; font-weight: 800; margin-top: 6px; }\n" +
            "    .grid-2 {\n" +
            "      display: grid;\n" +
            "      grid-template-columns: 1fr 1fr;\n" +
            "      gap: 1.5rem;\n" +
            "      margin-bottom: 2rem;\n" +
            "    }\n" +
            "    @media (max-width: 900px) { .grid-2 { grid-template-columns: 1fr; } }\n" +
            "    .panel {\n" +
            "      background: var(--card-bg);\n" +
            "      backdrop-filter: blur(16px);\n" +
            "      border: 1px solid var(--card-border);\n" +
            "      border-radius: 16px;\n" +
            "      padding: 1.5rem;\n" +
            "      display: flex;\n" +
            "      flex-direction: column;\n" +
            "    }\n" +
            "    .panel-header {\n" +
            "      display: flex;\n" +
            "      justify-content: space-between;\n" +
            "      align-items: center;\n" +
            "      margin-bottom: 1.2rem;\n" +
            "    }\n" +
            "    .panel-title { font-size: 1.1rem; font-weight: 700; }\n" +
            "    .broker-badge {\n" +
            "      display: inline-flex;\n" +
            "      align-items: center;\n" +
            "      gap: 6px;\n" +
            "      padding: 6px 12px;\n" +
            "      background: rgba(255, 255, 255, 0.05);\n" +
            "      border-radius: 8px;\n" +
            "      margin: 4px;\n" +
            "      font-family: var(--mono-font);\n" +
            "      font-size: 0.85rem;\n" +
            "    }\n" +
            "    .tag { font-size: 0.75rem; padding: 2px 6px; border-radius: 4px; }\n" +
            "    .tag-controller { background: #818cf8; color: #1e1b4b; font-weight: 700; }\n" +
            "    .form-group {\n" +
            "      margin-bottom: 1rem;\n" +
            "    }\n" +
            "    label {\n" +
            "      display: block;\n" +
            "      font-size: 0.8rem;\n" +
            "      color: var(--text-muted);\n" +
            "      margin-bottom: 6px;\n" +
            "      font-weight: 600;\n" +
            "    }\n" +
            "    input, select, textarea {\n" +
            "      width: 100%;\n" +
            "      background: rgba(0, 0, 0, 0.4);\n" +
            "      border: 1px solid var(--card-border);\n" +
            "      border-radius: 8px;\n" +
            "      padding: 10px 12px;\n" +
            "      color: var(--text);\n" +
            "      font-family: inherit;\n" +
            "      font-size: 0.9rem;\n" +
            "      outline: none;\n" +
            "      transition: border-color 0.2s;\n" +
            "    }\n" +
            "    input:focus, select:focus, textarea:focus {\n" +
            "      border-color: var(--accent);\n" +
            "    }\n" +
            "    button {\n" +
            "      background: var(--accent);\n" +
            "      color: white;\n" +
            "      border: none;\n" +
            "      border-radius: 8px;\n" +
            "      padding: 10px 18px;\n" +
            "      font-weight: 600;\n" +
            "      cursor: pointer;\n" +
            "      display: inline-flex;\n" +
            "      align-items: center;\n" +
            "      gap: 8px;\n" +
            "      transition: all 0.2s;\n" +
            "    }\n" +
            "    button:hover {\n" +
            "      background: var(--accent-hover);\n" +
            "      box-shadow: 0 0 15px var(--accent-glow);\n" +
            "    }\n" +
            "    button.btn-secondary {\n" +
            "      background: rgba(255, 255, 255, 0.1);\n" +
            "    }\n" +
            "    button.btn-secondary:hover {\n" +
            "      background: rgba(255, 255, 255, 0.15);\n" +
            "    }\n" +
            "    .message-stream {\n" +
            "      background: #040711;\n" +
            "      border-radius: 8px;\n" +
            "      border: 1px solid var(--card-border);\n" +
            "      padding: 1rem;\n" +
            "      height: 280px;\n" +
            "      overflow-y: auto;\n" +
            "      font-family: var(--mono-font);\n" +
            "      font-size: 0.85rem;\n" +
            "    }\n" +
            "    .msg-item {\n" +
            "      padding: 6px 8px;\n" +
            "      border-bottom: 1px solid rgba(255, 255, 255, 0.04);\n" +
            "      display: flex;\n" +
            "      justify-content: space-between;\n" +
            "      gap: 12px;\n" +
            "    }\n" +
            "    .msg-offset { color: #818cf8; font-weight: 700; min-width: 80px; }\n" +
            "    .msg-content { color: #e5e7eb; word-break: break-all; flex-grow: 1; }\n" +
            "    .table-container {\n" +
            "      overflow-x: auto;\n" +
            "      margin-top: 1rem;\n" +
            "    }\n" +
            "    table {\n" +
            "      width: 100%;\n" +
            "      border-collapse: collapse;\n" +
            "      font-size: 0.88rem;\n" +
            "    }\n" +
            "    th, td {\n" +
            "      padding: 10px 14px;\n" +
            "      text-align: left;\n" +
            "      border-bottom: 1px solid var(--card-border);\n" +
            "    }\n" +
            "    th { color: var(--text-muted); font-size: 0.78rem; text-transform: uppercase; font-weight: 700; }\n" +
            "  </style>\n" +
            "</head>\n" +
            "<body>\n" +
            "  <div class=\"container\">\n" +
            "    <header>\n" +
            "      <div class=\"logo-group\">\n" +
            "        <div class=\"logo-badge\">K</div>\n" +
            "        <div>\n" +
            "          <h1>SimpleKafka Cluster</h1>\n" +
            "          <div class=\"subtitle\">Distributed Messaging System &bull; Java NIO + ZooKeeper</div>\n" +
            "        </div>\n" +
            "      </div>\n" +
            "      <div class=\"status-badge\">\n" +
            "        <span class=\"pulse-dot\"></span>\n" +
            "        Cluster Healthy\n" +
            "      </div>\n" +
            "    </header>\n" +
            "\n" +
            "    <div class=\"grid-metrics\">\n" +
            "      <div class=\"metric-card\">\n" +
            "        <div class=\"metric-label\">Live Brokers</div>\n" +
            "        <div class=\"metric-val\" id=\"brokerCount\">3</div>\n" +
            "      </div>\n" +
            "      <div class=\"metric-card\">\n" +
            "        <div class=\"metric-label\">ZooKeeper Port</div>\n" +
            "        <div class=\"metric-val\" style=\"color:#38bdf8;\">2181</div>\n" +
            "      </div>\n" +
            "      <div class=\"metric-card\">\n" +
            "        <div class=\"metric-label\">Topics Registered</div>\n" +
            "        <div class=\"metric-val\" id=\"topicCount\">-</div>\n" +
            "      </div>\n" +
            "      <div class=\"metric-card\">\n" +
            "        <div class=\"metric-label\">Active Controller</div>\n" +
            "        <div class=\"metric-val\" id=\"controllerId\" style=\"color:#a855f7;\">Broker 1</div>\n" +
            "      </div>\n" +
            "    </div>\n" +
            "\n" +
            "    <div class=\"panel\" style=\"margin-bottom: 1.5rem;\">\n" +
            "      <div class=\"panel-header\">\n" +
            "        <div class=\"panel-title\">Cluster Brokers & Partition Distribution</div>\n" +
            "        <button class=\"btn-secondary\" onclick=\"fetchCluster()\">&#8635; Refresh Metadata</button>\n" +
            "      </div>\n" +
            "      <div id=\"brokersList\" style=\"display:flex; flex-wrap:wrap; gap:8px; margin-bottom:1rem;\"></div>\n" +
            "      <div class=\"table-container\">\n" +
            "        <table>\n" +
            "          <thead>\n" +
            "            <tr>\n" +
            "              <th>Topic</th>\n" +
            "              <th>Partition ID</th>\n" +
            "              <th>Leader Broker</th>\n" +
            "              <th>Follower Replicas</th>\n" +
            "            </tr>\n" +
            "          </thead>\n" +
            "          <tbody id=\"topicsTableBody\">\n" +
            "            <tr><td colspan=\"4\" style=\"text-align:center; color:var(--text-muted);\">Loading cluster state...</td></tr>\n" +
            "          </tbody>\n" +
            "        </table>\n" +
            "      </div>\n" +
            "    </div>\n" +
            "\n" +
            "    <div class=\"grid-2\">\n" +
            "      <!-- Produce Form -->\n" +
            "      <div class=\"panel\">\n" +
            "        <div class=\"panel-header\">\n" +
            "          <div class=\"panel-title\">Produce Message</div>\n" +
            "        </div>\n" +
            "        <div class=\"form-group\">\n" +
            "          <label>Topic</label>\n" +
            "          <select id=\"produceTopicSelect\" onchange=\"updatePartitions()\"></select>\n" +
            "        </div>\n" +
            "        <div class=\"form-group\">\n" +
            "          <label>Partition</label>\n" +
            "          <select id=\"producePartitionSelect\"></select>\n" +
            "        </div>\n" +
            "        <div class=\"form-group\">\n" +
            "          <label>Message Content</label>\n" +
            "          <textarea id=\"produceMessage\" rows=\"3\" placeholder=\"Type payload...\">Hello SimpleKafka from Web Dashboard!</textarea>\n" +
            "        </div>\n" +
            "        <div style=\"display:flex; justify-content:space-between; align-items:center;\">\n" +
            "          <button onclick=\"produceMessage()\">Send to Broker</button>\n" +
            "          <span id=\"produceStatus\" style=\"font-size:0.85rem; font-family:var(--mono-font);\"></span>\n" +
            "        </div>\n" +
            "      </div>\n" +
            "\n" +
            "      <!-- Consume Stream -->\n" +
            "      <div class=\"panel\">\n" +
            "        <div class=\"panel-header\">\n" +
            "          <div class=\"panel-title\">Live Consumer Stream</div>\n" +
            "          <div style=\"display:flex; gap:6px;\">\n" +
            "            <button class=\"btn-secondary\" style=\"padding:6px 12px;\" onclick=\"fetchMessages()\">Poll Now</button>\n" +
            "          </div>\n" +
            "        </div>\n" +
            "        <div style=\"display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:10px;\">\n" +
            "          <div>\n" +
            "            <label>Topic</label>\n" +
            "            <select id=\"consumeTopicSelect\" onchange=\"updateConsumePartitions()\"></select>\n" +
            "          </div>\n" +
            "          <div>\n" +
            "            <label>Partition</label>\n" +
            "            <select id=\"consumePartitionSelect\" onchange=\"resetAndFetch()\"></select>\n" +
            "          </div>\n" +
            "        </div>\n" +
            "        <div class=\"message-stream\" id=\"messageStream\">\n" +
            "          <div style=\"color:var(--text-muted);\">Polling messages...</div>\n" +
            "        </div>\n" +
            "      </div>\n" +
            "    </div>\n" +
            "\n" +
            "    <!-- Create Topic Panel -->\n" +
            "    <div class=\"panel\">\n" +
            "      <div class=\"panel-header\">\n" +
            "        <div class=\"panel-title\">Create New Topic</div>\n" +
            "      </div>\n" +
            "      <div style=\"display:grid; grid-template-columns:2fr 1fr 1fr auto; gap:12px; align-items:flex-end;\">\n" +
            "        <div>\n" +
            "          <label>Topic Name</label>\n" +
            "          <input id=\"newTopicName\" type=\"text\" placeholder=\"e.g. orders-stream\" />\n" +
            "        </div>\n" +
            "        <div>\n" +
            "          <label>Partitions</label>\n" +
            "          <input id=\"newTopicPartitions\" type=\"number\" value=\"3\" min=\"1\" max=\"10\" />\n" +
            "        </div>\n" +
            "        <div>\n" +
            "          <label>Replication Factor</label>\n" +
            "          <input id=\"newTopicReplication\" type=\"number\" value=\"2\" min=\"1\" max=\"3\" />\n" +
            "        </div>\n" +
            "        <button onclick=\"createTopic()\">Create Topic</button>\n" +
            "      </div>\n" +
            "      <div id=\"createTopicStatus\" style=\"margin-top:10px; font-size:0.85rem;\"></div>\n" +
            "    </div>\n" +
            "  </div>\n" +
            "\n" +
            "  <script>\n" +
            "    let clusterData = null;\n" +
            "    let currentOffset = 0;\n" +
            "    let pollTimer = null;\n" +
            "\n" +
            "    async function fetchCluster() {\n" +
            "      try {\n" +
            "        const res = await fetch('/api/cluster');\n" +
            "        clusterData = await res.json();\n" +
            "        if (clusterData.success) {\n" +
            "          renderCluster();\n" +
            "        }\n" +
            "      } catch (e) {\n" +
            "        console.error(e);\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    function renderCluster() {\n" +
            "      document.getElementById('brokerCount').innerText = clusterData.brokers.length;\n" +
            "      document.getElementById('topicCount').innerText = clusterData.topics.length;\n" +
            "      \n" +
            "      const bList = document.getElementById('brokersList');\n" +
            "      bList.innerHTML = clusterData.brokers.map(b => `\n" +
            "        <div class=\"broker-badge\">\n" +
            "          <span style=\"color:#34d399;\">&#9679;</span>\n" +
            "          <strong>Broker ${b.id}</strong> &bull; ${b.host}:${b.port}\n" +
            "          ${b.id === 1 ? '<span class=\"tag tag-controller\">Controller</span>' : ''}\n" +
            "        </div>\n" +
            "      `).join('');\n" +
            "\n" +
            "      const tBody = document.getElementById('topicsTableBody');\n" +
            "      if (clusterData.topics.length === 0) {\n" +
            "        tBody.innerHTML = '<tr><td colspan=\"4\" style=\"text-align:center; color:var(--text-muted);\">No topics created yet</td></tr>';\n" +
            "      } else {\n" +
            "        let rows = '';\n" +
            "        clusterData.topics.forEach(t => {\n" +
            "          t.partitions.forEach(p => {\n" +
            "            rows += `<tr>\n" +
            "              <td><strong>${t.name}</strong></td>\n" +
            "              <td>Partition #${p.id}</td>\n" +
            "              <td><span style=\"color:#818cf8; font-weight:700;\">Broker ${p.leader}</span></td>\n" +
            "              <td>Brokers ${p.followers.join(', ')}</td>\n" +
            "            </tr>`;\n" +
            "          });\n" +
            "        });\n" +
            "        tBody.innerHTML = rows;\n" +
            "      }\n" +
            "\n" +
            "      updateDropdowns();\n" +
            "    }\n" +
            "\n" +
            "    function updateDropdowns() {\n" +
            "      const pTopic = document.getElementById('produceTopicSelect');\n" +
            "      const cTopic = document.getElementById('consumeTopicSelect');\n" +
            "      const currentPTopic = pTopic.value;\n" +
            "      const currentCTopic = cTopic.value;\n" +
            "\n" +
            "      const options = clusterData.topics.map(t => `<option value=\"${t.name}\">${t.name}</option>`).join('');\n" +
            "      pTopic.innerHTML = options;\n" +
            "      cTopic.innerHTML = options;\n" +
            "\n" +
            "      if (currentPTopic) pTopic.value = currentPTopic;\n" +
            "      if (currentCTopic) cTopic.value = currentCTopic;\n" +
            "\n" +
            "      updatePartitions();\n" +
            "      updateConsumePartitions();\n" +
            "    }\n" +
            "\n" +
            "    function updatePartitions() {\n" +
            "      const topic = document.getElementById('produceTopicSelect').value;\n" +
            "      const tObj = clusterData?.topics.find(t => t.name === topic);\n" +
            "      const pSelect = document.getElementById('producePartitionSelect');\n" +
            "      if (tObj) {\n" +
            "        pSelect.innerHTML = tObj.partitions.map(p => `<option value=\"${p.id}\">Partition ${p.id} (Leader: Broker ${p.leader})</option>`).join('');\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    function updateConsumePartitions() {\n" +
            "      const topic = document.getElementById('consumeTopicSelect').value;\n" +
            "      const tObj = clusterData?.topics.find(t => t.name === topic);\n" +
            "      const cSelect = document.getElementById('consumePartitionSelect');\n" +
            "      if (tObj) {\n" +
            "        cSelect.innerHTML = tObj.partitions.map(p => `<option value=\"${p.id}\">Partition ${p.id}</option>`).join('');\n" +
            "        resetAndFetch();\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    function resetAndFetch() {\n" +
            "      currentOffset = 0;\n" +
            "      document.getElementById('messageStream').innerHTML = '';\n" +
            "      fetchMessages();\n" +
            "    }\n" +
            "\n" +
            "    async function produceMessage() {\n" +
            "      const topic = document.getElementById('produceTopicSelect').value;\n" +
            "      const partition = document.getElementById('producePartitionSelect').value;\n" +
            "      const message = document.getElementById('produceMessage').value;\n" +
            "      const status = document.getElementById('produceStatus');\n" +
            "      if (!topic || !message) return;\n" +
            "\n" +
            "      status.innerText = 'Publishing...';\n" +
            "      status.style.color = '#f59e0b';\n" +
            "\n" +
            "      try {\n" +
            "        const res = await fetch('/api/produce', {\n" +
            "          method: 'POST',\n" +
            "          headers: { 'Content-Type': 'application/json' },\n" +
            "          body: JSON.stringify({ topic, partition, message })\n" +
            "        });\n" +
            "        const data = await res.json();\n" +
            "        if (data.success) {\n" +
            "          status.innerText = `Ack: Partition ${data.partition} @ Offset ${data.offset}`;\n" +
            "          status.style.color = '#10b981';\n" +
            "          fetchMessages();\n" +
            "        } else {\n" +
            "          status.innerText = 'Error: ' + data.error;\n" +
            "          status.style.color = '#ef4444';\n" +
            "        }\n" +
            "      } catch (e) {\n" +
            "        status.innerText = 'Network Error';\n" +
            "        status.style.color = '#ef4444';\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    async function fetchMessages() {\n" +
            "      const topic = document.getElementById('consumeTopicSelect').value;\n" +
            "      const partition = document.getElementById('consumePartitionSelect').value;\n" +
            "      if (!topic) return;\n" +
            "\n" +
            "      try {\n" +
            "        const res = await fetch(`/api/consume?topic=${encodeURIComponent(topic)}&partition=${partition}&offset=${currentOffset}`);\n" +
            "        const data = await res.json();\n" +
            "        if (data.success && data.messages.length > 0) {\n" +
            "          const stream = document.getElementById('messageStream');\n" +
            "          data.messages.forEach(m => {\n" +
            "            const row = document.createElement('div');\n" +
            "            row.className = 'msg-item';\n" +
            "            row.innerHTML = `<span class=\"msg-offset\">Offset ${m.offset}</span><span class=\"msg-content\">${m.content}</span>`;\n" +
            "            stream.appendChild(row);\n" +
            "          });\n" +
            "          currentOffset = data.nextOffset;\n" +
            "          stream.scrollTop = stream.scrollHeight;\n" +
            "        }\n" +
            "      } catch (e) {\n" +
            "        console.error(e);\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    async function createTopic() {\n" +
            "      const name = document.getElementById('newTopicName').value.trim();\n" +
            "      const partitions = document.getElementById('newTopicPartitions').value;\n" +
            "      const replication = document.getElementById('newTopicReplication').value;\n" +
            "      const status = document.getElementById('createTopicStatus');\n" +
            "      if (!name) return;\n" +
            "\n" +
            "      status.innerText = 'Creating topic...';\n" +
            "      status.style.color = '#f59e0b';\n" +
            "\n" +
            "      try {\n" +
            "        const res = await fetch('/api/create-topic', {\n" +
            "          method: 'POST',\n" +
            "          headers: { 'Content-Type': 'application/json' },\n" +
            "          body: JSON.stringify({ topic: name, partitions, replication })\n" +
            "        });\n" +
            "        const data = await res.json();\n" +
            "        if (data.success) {\n" +
            "          status.innerText = `Topic \"${name}\" created!`;\n" +
            "          status.style.color = '#10b981';\n" +
            "          document.getElementById('newTopicName').value = '';\n" +
            "          await fetchCluster();\n" +
            "        } else {\n" +
            "          status.innerText = 'Error: ' + data.error;\n" +
            "          status.style.color = '#ef4444';\n" +
            "        }\n" +
            "      } catch (e) {\n" +
            "        status.innerText = 'Network error';\n" +
            "        status.style.color = '#ef4444';\n" +
            "      }\n" +
            "    }\n" +
            "\n" +
            "    // Initial load and periodic refresh\n" +
            "    fetchCluster();\n" +
            "    setInterval(fetchCluster, 4000);\n" +
            "    setInterval(fetchMessages, 2000);\n" +
            "  </script>\n" +
            "</body>\n" +
            "</html>\n";
}
