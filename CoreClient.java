package com.example.demo;


import org.springframework.stereotype.Component;

@Component
public class CoreClient {

  /**
   * Simula una transacción al core bancario.
   * Reemplázalo por tu invocación real (HTTP/MQ/etc).
   */
  public String execute(String accountId, String requestId) {
    try {
      // Simulación del tiempo de respuesta promedio del core
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "INTERRUPTED";
    }
    return "OK";
  }
}
