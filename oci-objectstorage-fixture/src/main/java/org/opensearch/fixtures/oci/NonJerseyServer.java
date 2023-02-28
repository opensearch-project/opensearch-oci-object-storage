package org.opensearch.fixtures.oci;

import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonJerseyServer implements Closeable {
    public static final String DEFAULT_BASE_URI = "http://localhost:8080/";
    private final static int DEFAULT_PORT = 8080;
    private final static String URL = "localhost";
    private final HttpServer server;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final int port;

    public NonJerseyServer() throws IOException {
        this(DEFAULT_PORT);
    }

    public NonJerseyServer(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(URL), port), 0);
        this.server.createContext("/", new OciHttpHandler());
        this.port = port;
    }

    public void start() {
        executorService.submit(
                () -> {
                    try {
                        server.start();
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        executorService.shutdownNow();
    }

    public String getUrl() {
        return String.format("http://localhost:%s/", port);
    }
}
