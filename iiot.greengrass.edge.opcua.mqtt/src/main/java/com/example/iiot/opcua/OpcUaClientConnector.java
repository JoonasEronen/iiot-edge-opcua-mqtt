package com.example.iiot.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.SessionActivityListener;
import org.eclipse.milo.opcua.sdk.client.api.UaSession;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.client.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.iiot.config.AwsSecretReader;
import com.example.iiot.config.GreengrassConfigReader;
import com.example.iiot.config.ProgramSettings;

import software.amazon.awssdk.regions.Region;


/**
 * Handles OPC UA client creation, connection, authentication and automatic reconnect logic.
 * <p>
 * This class manages the entire lifecycle of an OPC UA client. It connects to the server using
 * certificate-based security and user credentials (fetched securely from AWS Secrets Manager),
 * and starts the subscription logic defined in {@link ManagedOpcUASubscription}.
 */
public class OpcUaClientConnector {
    static {
    	// Register BouncyCastle as a security provider
        Security.addProvider(new BouncyCastleProvider());
    }

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    /**
     * CompletableFuture that represents the current OPC UA connection state.
     * Used to synchronize connection and subscriptions.
     */
    private CompletableFuture<OpcUaClient> future = new CompletableFuture<>();
     
    private final ManagedOpcUASubscription opcUaSubscription;
    private OpcUaClient client = null;
    private final ProgramSettings programSettings;
    private final Logger logger = LoggerFactory.getLogger(getClass());    
    private int reconnectAttempt = 0; // Incremented on each failure


    /**
     * Constructs a new OPC UA client connector.
     *
     * @param opcUaSubscription subscription logic to run once connected
     * @param configReader configuration provider (including AWS secrets and file paths)
     */
    public OpcUaClientConnector(ManagedOpcUASubscription opcUaSubscription, GreengrassConfigReader configReader) {
    	this.opcUaSubscription = opcUaSubscription;
    	this.programSettings = configReader.getProgramSettings();
    }

    
    /**
     * Starts the OPC UA connection process.
     * Initializes the client and begins subscription after successful connection.
     */
    public void run() {
        future = new CompletableFuture<>();
        tryConnect();
    }

    
    /**
     * Attempts to create and connect a new OPC UA client session.
     * <p>
     * If an existing client session is active, it is properly disconnected
     * before initializing a new one. This prevents multiple open sessions
     * on the OPC UA server which could lead to session limits being exceeded
     * (e.g., Bad_TooManySessions or Bad_TooManyOperations).
     * <p>
     * Upon successful connection, the client session is marked active and
     * the subscription logic is started. If connection fails, a reconnect
     * attempt is scheduled with randomized delay.
     */
    private void tryConnect() {
        try {
            logger.info("Attempting to connect to OPC UA server...");

            // Disconnect previous client session if already initialized
            if (this.client != null) {
                try {
                    logger.info("Disconnecting previous OPC UA client...");
                    this.client.disconnect().get(); // Gracefully close the session
                } catch (Exception ex) {
                    logger.warn("Failed to disconnect previous client cleanly: {}", ex.getMessage(), ex);
                }
            }

            // Create and configure a new OPC UA client
            this.client = createClient();
            
            // Reset reconnect attempt counter on successful connection
            reconnectAttempt = 0;

            // Mark session active and complete the future
            future.complete(client);

            // Register session activity listeners
            client.addSessionActivityListener(new SessionActivityListener() {
                @Override
                public void onSessionActive(UaSession session) {
                    logger.info("OPC UA session is active.");
                }

                @Override
                public void onSessionInactive(UaSession session) {
                    logger.warn("OPC UA session is inactive.");
                    // Optional: trigger reconnect here if needed
                    // reconnect();
                }
            });

            // Start the subscription logic
            opcUaSubscription.run(client, future);

            // Wait for future completion
            future.get();

        } catch (Exception e) {
        	logger.error("OPC UA connection failed", e);
            future.completeExceptionally(e);

            // Schedule reconnect attempt
            reconnect();
        }
    }

    
    /**
     * Schedules a reconnect attempt with exponential backoff and random jitter.
     * <p>
     * This logic reduces server load and prevents mass reconnect storms by
     * gradually increasing the delay after consecutive failures and adding
     * randomness to reconnect timing for improved cybersecurity.
     */
    private void reconnect() {
        // Increase reconnect attempt counter (used to calculate backoff)
        reconnectAttempt++;

        // Base delay grows exponentially: 2s, 4s, 8s, ..., capped at 60s
        int baseDelay = Math.min(60, (int) Math.pow(2, reconnectAttempt));

        // Add small random jitter (0–4 seconds) for desynchronization
        int jitter = ThreadLocalRandom.current().nextInt(0, 5);

        // Total delay before next reconnect
        int totalDelay = baseDelay + jitter;

        logger.info("Reconnecting in {} seconds (base: {}, jitter: {}, attempt #{})...",
            totalDelay, baseDelay, jitter, reconnectAttempt);

        // Schedule reconnect after computed delay
        scheduler.schedule(this::tryConnect, totalDelay, TimeUnit.SECONDS);
    }

    
    /**
     * Creates and configures a new OPC UA client using the provided certificates and credentials.
     * Uses secure authentication (X.509 + username/password) and fetches secrets from AWS.
     *
     * @return a configured OPC UA client instance
     * @throws Exception if certificate or credential loading fails, or if endpoint is not suitable
     */
    private OpcUaClient createClient() throws Exception {
        logger.info("Creating OPC UA client...");

        String clientCertPath = programSettings.getOpcuaClientCertPath();
        String clientKeyPath = programSettings.getOpcuaClientKeyPath();
        String trustListPath = programSettings.getOpcuaTrustListPath();
        String awsRegionString = programSettings.getAwsRegion();
        
        if (awsRegionString == null || awsRegionString.isBlank()) {
            throw new IllegalStateException("AWS region is missing (programSettings.awsRegion).");
        }

        Region awsRegion;
        try {
            awsRegion = Region.of(awsRegionString.trim());
        } catch (Exception e) {
            throw new IllegalStateException("Invalid AWS region: '" + awsRegionString + "'", e);
        }      
             
        DefaultClientCertificateValidator certValidator =
            new DefaultClientCertificateValidator(new DefaultTrustListManager(new File(trustListPath)));

        // Load keystore password from AWS Secrets Manager
        Map<String, char[]> keySecret = AwsSecretReader.getSecret(programSettings.getOpcuaKeystoreSecretName(), awsRegion);
        
        if (keySecret == null || keySecret.isEmpty()) {
            throw new SecurityException("Keystore secret is missing or empty");
        }
        
        char[] keyPassword = keySecret.get("opcuaKeystorePassword");
        
        if (keyPassword == null || keyPassword.length == 0) {
            throw new SecurityException("opcuaKeystorePassword missing from keystore secret");
        }

        X509Certificate clientCert = loadCertificate(clientCertPath);
        PrivateKey privateKey = loadPrivateKey(clientKeyPath, keyPassword);

        // Clear password from memory
        Arrays.fill(keyPassword, '\0');
        keySecret.clear();

        if (clientCert == null || privateKey == null) {
            throw new SecurityException("Missing client certificate or private key!");
        }

        // Discover server endpoints
        List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(programSettings.getOpcuaServerEndpoint()).get(10, TimeUnit.SECONDS);        

        Optional<EndpointDescription> endpoint = endpoints.stream()
            .filter(e -> e.getSecurityPolicyUri().equals(SecurityPolicy.Basic256Sha256.getUri()) &&
                         e.getSecurityMode() == MessageSecurityMode.SignAndEncrypt)
            .findFirst();

        if (!endpoint.isPresent()) {
            throw new Exception("No suitable OPC UA endpoint found!");
        }
   
        // Load username/password credentials
        Map<String, char[]> credentials = AwsSecretReader.getSecret(programSettings.getOpcuaCredentialsSecretName(), awsRegion);
       
        if (credentials == null || credentials.isEmpty()) {
            throw new SecurityException("OPC UA credentials secret is missing or empty");
        }
        
        char[] usernameChars = credentials.get("username");
        char[] passwordChars = credentials.get("password");

        if (usernameChars == null || usernameChars.length == 0) {
            throw new SecurityException("username missing from OPC UA credentials secret");
        }
        if (passwordChars == null || passwordChars.length == 0) {
            throw new SecurityException("password missing from OPC UA credentials secret");
        }
        
        //“Milo UsernameProvider requires String credentials; char[] is used for transport and cleared immediately afterwards.”
        UsernameProvider identityProvider = new UsernameProvider(new String(usernameChars), new String(passwordChars));
     	Arrays.fill(usernameChars, '\0');
       	Arrays.fill(passwordChars, '\0');
       	credentials.clear();
       
        OpcUaClientConfig config = OpcUaClientConfig.builder()
            .setApplicationName(LocalizedText.english(programSettings.getOpcuaApplicationName()))
            .setApplicationUri(programSettings.getOpcuaApplicationUri())
            .setCertificate(clientCert)
            .setKeyPair(new KeyPair(clientCert.getPublicKey(), privateKey))
            .setEndpoint(endpoint.get())
            .setIdentityProvider(identityProvider)
            .setCertificateValidator(certValidator)
            .setRequestTimeout(uint(50000))
            .setSessionTimeout(uint(60000))
            .setKeepAliveInterval(uint(10000))
            .setKeepAliveFailuresAllowed(uint(100))
            .build();

        OpcUaClient client = OpcUaClient.create(config);
        logger.info("OPC UA client created.");
        return client;
    }


    /**
     * Loads an X.509 certificate from the given file path.
     *
     * @param certPath the full file path to the certificate file
     * @return the parsed X509Certificate
     * @throws Exception if the certificate cannot be read or parsed
     */
    private X509Certificate loadCertificate(String certPath) throws Exception {
        try (FileInputStream fis = new FileInputStream(certPath)) {
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(fis);
        }
    }

    
    /**
     * Loads a private key from a PEM-formatted file. Supports encrypted and unencrypted keys.
     *
     * @param pemPath the path to the PEM private key file
     * @param password the password used to decrypt the key (if encrypted)
     * @return the loaded PrivateKey
     * @throws Exception if the key is in an invalid format or cannot be decrypted
     */
    private PrivateKey loadPrivateKey(String pemPath, char[] password) throws Exception {
        try (PEMParser pemParser = new PEMParser(new FileReader(pemPath))) {
            Object object = pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
            
            if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
                return converter.getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) object)
                        .decryptPrivateKeyInfo(new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password)));
            }

            if (object instanceof PrivateKeyInfo) {
                return converter.getPrivateKey((PrivateKeyInfo) object);
            }
        }
        throw new IllegalArgumentException("Invalid private key format");
    }
    
    
    /**
     * Returns the currently connected OPC UA client instance.
     * <p>
     * This can be used for low-level access or injecting the client into
     * external components such as publishers.
     *
     * @return the active {@link OpcUaClient} instance, or {@code null} if not connected yet
     */
    public OpcUaClient getClient() {
        return client;
    }

}
