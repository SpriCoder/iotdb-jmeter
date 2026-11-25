package org.apache.iotdb.jmeter.test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;

/** Validates the endpoint before the legacy RPC client opens its chain-verified TLS socket. */
public final class TlsVerifier {
  private TlsVerifier() {}

  public static void verify(String host, int port, String trustStore, String password, int timeoutMs)
      throws Exception {
    KeyStore store = KeyStore.getInstance("JKS");
    try (InputStream input = Files.newInputStream(Paths.get(trustStore))) {
      store.load(input, password.toCharArray());
    }
    TrustManagerFactory managers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    managers.init(store);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, managers.getTrustManagers(), null);
    try (Socket connection = new Socket()) {
      connection.connect(new InetSocketAddress(host, port), timeoutMs);
      try (SSLSocket socket = (SSLSocket) context.getSocketFactory()
          .createSocket(connection, host, port, true)) {
        socket.setSoTimeout(timeoutMs);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
        socket.startHandshake();
      }
    }
  }
}
