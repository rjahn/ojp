package org.openjproxy.http;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public final class OjpHttpServlet extends HttpServlet {
    private OjpHttpService service;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        service = OjpHttpService.create(config.getServletContext());
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        try {
            service.handle(request, response);
        } catch (Exception e) {
            if (!response.isCommitted()) {
                response.reset();
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain;charset=UTF-8");
            }
            if (!response.isCommitted()) {
                response.getWriter().write(e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }
    }

    @Override
    protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Allow", "POST, OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    public void destroy() {
        if (service != null) {
            service.close();
        }
        super.destroy();
    }
}
