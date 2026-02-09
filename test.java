    @Bean
    public RelyingPartyRegistrationRepository relyingPartyRegistrationRepository(
            SamlProperties samlProperties, 
            ResourceLoader resourceLoader) {
        
        try {
            // Load SAML keystore
            Resource keystoreResource = resourceLoader.getResource(samlProperties.getKeystore().getLocation());
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            
            try (InputStream is = keystoreResource.getInputStream()) {
                keyStore.load(is, samlProperties.getKeystore().getPassword().toCharArray());
            }
            
            // Get SP signing key (optional, commented out for minimal setup)
            RSAPrivateKey privateKey = (RSAPrivateKey) keyStore.getKey(
                samlProperties.getKeystore().getAlias(),
                samlProperties.getKeystore().getKeyPassword().toCharArray()
            );
            X509Certificate spCertificate = (X509Certificate) keyStore.getCertificate(
                samlProperties.getKeystore().getAlias()
            );
            
            // Get IdP verification certificate from PEM file
            X509Certificate idpCertificate;
            String certLocation = samlProperties.getIdp().getVerificationCertLocation();
            
            if (certLocation != null && !certLocation.isEmpty()) {
                // Load from PEM file
                Resource certResource = resourceLoader.getResource(certLocation);
                try (InputStream certStream = certResource.getInputStream()) {
                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    idpCertificate = (X509Certificate) cf.generateCertificate(certStream);
                }
            } else {
                // Fallback: Load from keystore (legacy)
                Certificate idpCert = keyStore.getCertificate(samlProperties.getIdp().getVerificationCertAlias());
                if (idpCert == null) {
                    throw new RuntimeException("IdP verification certificate not found");
                }
                idpCertificate = (X509Certificate) idpCert;
            }
            
            // Build RelyingPartyRegistration with AuthnRequest signing enabled
            RelyingPartyRegistration registration = RelyingPartyRegistration.withRegistrationId("bet")
                .entityId("{baseUrl}/saml2/service-provider-metadata/bet")
                .assertionConsumerServiceLocation("{baseUrl}/login/saml2/sso/bet")
                .assertionConsumerServiceBinding(Saml2MessageBinding.POST)
                .singleLogoutServiceLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceResponseLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceBinding(Saml2MessageBinding.POST)
                // SP signing credential for AuthnRequest signature
                .signingX509Credentials(c -> c.add(
                    org.springframework.security.saml2.core.Saml2X509Credential.signing(privateKey, spCertificate)
                ))
                .assertingPartyDetails(party -> party
                    .entityId(samlProperties.getIdp().getEntityId())
                    .singleSignOnServiceLocation(samlProperties.getIdp().getSsoUrl())
                    .singleSignOnServiceBinding(Saml2MessageBinding.REDIRECT)
                    // IdP verification credential for Response/Assertion signature verification
                    .verificationX509Credentials(c -> c.add(
                        org.springframework.security.saml2.core.Saml2X509Credential.verification(idpCertificate)
                    ))
                    // Request IdP to expect signed AuthnRequest
                    .wantAuthnRequestsSigned(true)
                )
                .build();
            
            System.out.println("[SAML2 CONFIG] SP signing credential loaded: " + spCertificate.getSubjectX500Principal());
            System.out.println("[SAML2 CONFIG] AuthnRequest signing ENABLED for registration: bet");
            System.out.println("[SAML2 CONFIG] IdP EntityId: " + samlProperties.getIdp().getEntityId());
            System.out.println("[SAML2 CONFIG] IdP SSO URL: " + samlProperties.getIdp().getSsoUrl());
            
            return new InMemoryRelyingPartyRegistrationRepository(registration);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure SAML2 SP", e);
        }
    }
