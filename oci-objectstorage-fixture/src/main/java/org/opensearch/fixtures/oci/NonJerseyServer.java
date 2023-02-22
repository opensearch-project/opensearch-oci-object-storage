package org.opensearch.fixtures.oci;

import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonJerseyServer implements Closeable {
    public static final String BASE_URI = "http://localhost:8080/";
    private static int PORT = 8080;
    private static String URL = "localhost";
    private final HttpServer server;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public NonJerseyServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(URL), PORT), 0);
        this.server.createContext("/", new OciHttpHandler());
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
}
