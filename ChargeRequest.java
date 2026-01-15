package com.example.demo;

public record ChargeRequest(
	    String accountId,
	    String type // "ONLINE" o "BATCH"
	) {}
