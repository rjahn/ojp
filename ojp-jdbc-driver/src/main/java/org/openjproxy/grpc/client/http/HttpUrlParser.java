package org.openjproxy.grpc.client.http;

/**
 * Parser for the HTTP(S) transport URL, e.g. jdbc:ojp[https://host[:port]/[context]]_actual_jdbc_url
 */
public final class HttpUrlParser {
    private static final String PREFIX_OJP   = "jdbc:ojp[";

    private static final String PREFIX_HTTP  = PREFIX_OJP + "http://";
    private static final String PREFIX_HTTPS = PREFIX_OJP + "https://";

    private HttpUrlParser() {
    }

    public static boolean isHttpUrl(String url) {
        if (url != null) {
            String urlLower = url.toLowerCase();

            return urlLower.startsWith(PREFIX_HTTP) || urlLower.startsWith(PREFIX_HTTPS);
        }

        return false;
    }

    public static Result parse(String url) {
        if (!isHttpUrl(url)) {
            throw new IllegalArgumentException("Not an OJP HTTP URL: " + url);
        }

        int end = url.indexOf("]_");
        if (end < 0) {
            throw new IllegalArgumentException(
                    "Invalid OJP HTTP URL. Expected: jdbc:ojp-http[https://host:port/ojp]_actual_jdbc_url");
        }

        String endpoint = url.substring(PREFIX_OJP.length(), end).trim();
        String jdbcUrl = url.substring(end + 2);

        // Match the legacy OJP URL semantics: the database part is written
        // without the jdbc: prefix (e.g. _oracle:thin:@...), while the
        // actual JDBC URL sent to the server must retain jdbc:.
        if (!jdbcUrl.startsWith("jdbc:")) {
            jdbcUrl = "jdbc:" + jdbcUrl;
        }

        if (endpoint.isEmpty() || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("OJP HTTP endpoint and database JDBC URL are required");
        }

        return new Result(endpoint, jdbcUrl);
    }

    public static final class Result {
        private final String endpoint;
        private final String jdbcUrl;

        private Result(String endpoint, String jdbcUrl) {
            this.endpoint = endpoint;
            this.jdbcUrl = jdbcUrl;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }
    }
}
