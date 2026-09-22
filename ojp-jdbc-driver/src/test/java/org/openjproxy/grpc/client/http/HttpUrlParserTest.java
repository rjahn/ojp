package org.openjproxy.grpc.client.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpUrlParserTest {

    @Test
    void acceptsLegacyOjpDatabaseUrlWithoutJdbcPrefix() {
        String url = "jdbc:ojp[https://proxy.example/ojp]_oracle:thin:@localhost:1521/XE";

        HttpUrlParser.Result result = HttpUrlParser.parse(url);

        assertEquals("https://proxy.example/ojp", result.getEndpoint());
        assertEquals("jdbc:oracle:thin:@localhost:1521/XE", result.getJdbcUrl());
    }

    @Test
    void keepsJdbcPrefixWhenAlreadyPresent() {
        String url = "jdbc:ojp[https://proxy.example/ojp]_jdbc:oracle:thin:@localhost:1521/XE";

        HttpUrlParser.Result result = HttpUrlParser.parse(url);

        assertEquals("jdbc:oracle:thin:@localhost:1521/XE", result.getJdbcUrl());
    }

    @Test
    void recognizesHttpUrl() {
        assertTrue(HttpUrlParser.isHttpUrl("jdbc:ojp[https://proxy.example/ojp]_oracle:thin:@localhost:1521/XE"));
    }
}
