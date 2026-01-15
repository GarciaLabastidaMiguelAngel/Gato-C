package com.example.demo;



import java.time.Duration;
import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/demo")
public class ChargeController {
  private static final Logger log = LoggerFactory.getLogger(ChargeController.class);
  private final AccountPriorityGate gate;
  private final CoreClient core;
  private final Executor executor;

  // Este timeout es el límite del canal HTTP (alinear con gateway).
  private final long asyncTimeoutMs;

  public ChargeController(
      AccountPriorityGate gate,
      CoreClient core,
      @Qualifier("gateExecutor") Executor executor,
      @Value("${spring.mvc.async.request-timeout}") long asyncTimeoutMs
  ) {
    this.gate = gate;
    this.core = core;
    this.executor = executor;
    this.asyncTimeoutMs = asyncTimeoutMs;
  }

  @PostMapping("/charge")
  public DeferredResult<ResponseEntity<String>> charge( @RequestBody ChargeRequest req) {

    DeferredResult<ResponseEntity<String>> dr = new DeferredResult<>(asyncTimeoutMs);

    // Holder para poder cancelar si el canal expira
    Holder holder = new Holder(req.accountId());

    // Si expira el canal, se hace cleanup best-effort
    dr.onTimeout(() -> {
      if (holder.requestId != null) {
        gate.cancel(holder.accountId, holder.requestId);
      }
      dr.setErrorResult(ResponseEntity.status(504).body("GATEWAY_TIMEOUT"));
    });

    // Si termina (ok/error/timeout), aquí podrías meter métricas
    dr.onCompletion(() -> {
      // noop
    });

    executor.execute(() -> {
      try {
        AccountPriorityGate.Priority prio = AccountPriorityGate.Priority.valueOf(req.type());

        // Deadline alineado al canal (con margen)
        Duration channelDeadline = Duration.ofMillis(Math.max(1000, asyncTimeoutMs - 500));

        try (AccountPriorityGate.Permit permit = gate.acquire(req.accountId(), prio, channelDeadline)) {
          holder.requestId = permit.requestId();

          // Invocación al core (simulada en CoreClient)
          String result = core.execute(req.accountId(), permit.requestId());

          // Respuesta síncrona para el cliente
          dr.setResult(ResponseEntity.ok(
              "{\"status\":\"" + result + "\",\"accountId\":\"" + req.accountId() + "\",\"requestId\":\"" + permit.requestId() + "\"}"
          ));
        }

      } catch (IllegalArgumentException e) {
        dr.setErrorResult(ResponseEntity.badRequest().body("Invalid type. Use ONLINE or BATCH"));
      } catch (Exception e) {
    	  log.error("",e);
        dr.setErrorResult(ResponseEntity.status(500).body("ERROR"));
      }
    });

    return dr;
  }

  private static final class Holder {
    final String accountId;
    volatile String requestId;

    Holder(String accountId) {
      this.accountId = accountId;
    }
  }
}
