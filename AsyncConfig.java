package com.example.demo;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AsyncConfig {

  /**
   * Executor dedicado para:
   * - esperar turno en Redis
   * - ejecutar la llamada al core bancario
   *
   * IMPORTANTE:
   * - NO son threads de Tomcat
   * - Permite que el API escale en espera sin colapsar
   */
  @Bean(name = "gateExecutor")
  public ThreadPoolTaskExecutor gateExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(80);
    executor.setMaxPoolSize(200);
    executor.setQueueCapacity(1000);
    executor.setThreadNamePrefix("gate-");
    executor.initialize();
    return executor;
  }
}
