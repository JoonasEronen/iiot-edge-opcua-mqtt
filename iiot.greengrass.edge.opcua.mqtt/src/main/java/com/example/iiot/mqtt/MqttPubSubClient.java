package com.example.iiot.mqtt;

import com.example.iiot.config.ProgramSettings;
import com.example.iiot.model.CommandMessage;
import com.example.iiot.opcua.OpcUaPublisher;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import java.io.FileReader;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;


/**
 * MQTT client for publishing telemetry data and receiving commands.
 * <p>
 * Connects to an MQTT broker (e.g. AWS IoT Core) using mutual TLS, handles
 * reconnections, publishes messages, and listens for commands (e.g. on CMD/#).
 */
public class MqttPubSubClient {

    private String endpoint;
    private String clientId;
    private String caFilePath;
    private String certFilePath;
    private String keyFilePath;   
    
    private MqttClient client;
    private final OpcUaPublisher opcUaPublisher;
    private final String mqttTopicDCMD;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MemoryPersistence persistence;
    private final ScheduledExecutorService reconnectScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isConnecting = new AtomicBoolean(false);
    
    private int reconnectDelay = 5;
    private final int maxReconnectDelay = 60;
    
    
    /**
     * Constructor
     *
     * @param programSettings     application-level configuration (including certs and broker endpoint)
     * @param opcUaPublisher      OPC UA publisher for writing received CMD messages to server
     */
    public MqttPubSubClient(ProgramSettings programSettings, OpcUaPublisher opcUaPublisher) {
        this.endpoint = programSettings.getMqttBrokerEndpoint();
        this.clientId = programSettings.getMqttBrokerClientId();
        this.caFilePath = programSettings.getRootCAFilePath();
        this.certFilePath = programSettings.getThingCertFilePath();
        this.keyFilePath = programSettings.getPrivKeyFilePath();
        this.persistence = new MemoryPersistence();
        this.opcUaPublisher = opcUaPublisher;
        this.mqttTopicDCMD = programSettings.getMqttTopicDCMD();
    }


    /**
     * Establishes connection to MQTT broker with secure settings and sets callback handlers.
     */
    public void connect() {
        if (isConnecting.get()) {
            logger.info("Already trying to reconnect. Skipping...");
            return;
        }
        
        isConnecting.set(true);        
        logger.info("Attempting MQTT connect now...");
        
        try {
            client = new MqttClient(this.endpoint, this.clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);
            connOpts.setSocketFactory(getSocketFactory(this.caFilePath, this.certFilePath, this.keyFilePath));

            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectionLost(Throwable cause) {
                    logger.error("MQTT connection lost: {}", cause.getMessage());
                    scheduleReconnect();
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    logger.info("Message received on topic {}: {}", topic, new String(message.getPayload()));
                    handleIncomingCommand(topic, message.getPayload());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    logger.info("Message delivered successfully.");
                }

                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    logger.info("Connected to MQTT broker at {} (reconnect: {})", serverURI, reconnect);
                    isConnecting.set(false);
                    
                    subscribeToCommandTopics();
                }
            });

            client.connect(connOpts);
            logger.info("Connected to MQTT broker at {}", endpoint);
            isConnecting.set(false);
        } catch (MqttException e) {
            logger.error("MQTT connection failed: {}", e.getMessage());
            scheduleReconnect();
        } catch (Exception e) {
            logger.error("Error setting up SSL connection: {}", e.getMessage());
            isConnecting.set(false);
        }
    }

    
    /**
     * Disconnects from MQTT broker gracefully.
     */
    public void disconnect() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
                logger.info("Disconnected from MQTT broker");
            }
        } catch (MqttException e) {
            logger.error("Disconnection from MQTT broker failed: {}", e.getMessage(), e);
        }
    }

    private String lastPublishedMessage = null;

    
    /**
     * Publishes a message to the given topic. Prevents duplicate publishing after reconnect.
     *
     * @param payloadBytes message content
     * @param mqttTopic    target topic
     */
    public void mqttPublish(byte[] payloadBytes, String mqttTopic) {
        try {
            String messageStr = new String(payloadBytes);

            if (!client.isConnected()) {
                logger.warn("Client is not connected. Attempting to reconnect...");
                connect();
            }

            // Prevent sending the same message twice after reconnecting
            if (messageStr.equals(lastPublishedMessage)) {
                logger.warn("Skipping duplicate message for topic {}", mqttTopic);
                return;
            }

            MqttMessage message = new MqttMessage(payloadBytes);
            message.setQos(0);

            client.publish(mqttTopic, message);
            lastPublishedMessage = messageStr;

            logger.info("Message published to topic: {}", mqttTopic);
        } catch (MqttException e) {
            logger.error("Publishing message to MQTT broker failed: {}", e.getMessage(), e);
        }
    }
    
    
    /**
     * Subscribes to command topic. Supports both new format (payload includes mappedName)
     * and old format (topic includes mappedName as suffix).
     */
    private void subscribeToCommandTopics() {
        try {
            // Subscribe to base command topic (now shared for all tags)
            String cmdTopic = mqttTopicDCMD;
            client.subscribe(cmdTopic, 1);

            // Optionally support old-style subtopics: mqtt/.../DCMD/TagName
            client.subscribe(cmdTopic + "/#", 1);

            logger.info("Subscribed to MQTT command topics: {}, {}/*", cmdTopic, cmdTopic);
        } catch (MqttException e) {
            logger.error("Failed to subscribe to command topics: {}", e.getMessage(), e);
        }
    }


    
    /**
     * Handles incoming command messages received over MQTT and writes them to the OPC UA server.
     * Supports two formats:
     * 1. Modern: payload contains "mappedName" and "value"
     * 2. Legacy: topic includes mappedName, payload has only "value"
     *
     * @param topic        the MQTT topic
     * @param payloadBytes the message payload
     */
    public void handleIncomingCommand(String topic, byte[] payloadBytes) {
        String payload = new String(payloadBytes);

        try {
            CommandMessage cmd = new Gson().fromJson(payload, CommandMessage.class);

            // If mappedName is missing in payload, extract from topic (legacy fallback)
            if (cmd.mappedName == null || cmd.mappedName.isEmpty()) {
                String[] parts = topic.split("/");
                if (parts.length >= 1) {
                    cmd.mappedName = parts[parts.length - 1]; // assume last part is tag name
                    logger.warn("mappedName not in payload, extracted from topic: {}", cmd.mappedName);
                } else {
                    logger.warn("Unable to extract mappedName from topic: {}", topic);
                    return;
                }
            }

            if (cmd.value == null) {
                logger.warn("No value found in payload: {}", payload);
                return;
            }

            opcUaPublisher.publishValueToOpcUa(cmd);
            logger.info("CMD received and published: {} = {}", cmd.mappedName, cmd.value);

        } catch (Exception e) {
            logger.error("Invalid CMD payload for topic {}: {}", topic, payload, e);
        }
    }

      
    /**
     * Schedules a reconnection attempt with exponential backoff.
     */
    private void scheduleReconnect() {
        if (isConnecting.get()) {
            logger.info("Reconnect already scheduled. Skipping duplicate attempt.");
            return;
        }

        logger.info("Attempting MQTT reconnect in {} seconds...", reconnectDelay);
        reconnectScheduler.schedule(() -> {
            connect();
            reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
        }, reconnectDelay, TimeUnit.SECONDS);
    }


    /**
     * @return true if the MQTT client is connected
     */
    public boolean isConnected() {
        return client != null && client.isConnected();
    }    
  

    /**
     * Loads CA/client certificates and keys to create an SSL socket factory for TLS connection.
     *
     * @param caCrtFile path to CA certificate
     * @param crtFile   path to client certificate
     * @param keyFile   path to client private key
     * @return configured SSLSocketFactory
     * @throws Exception if any certificate or key fails to load
     */
    private SSLSocketFactory getSocketFactory(final String caCrtFile, final String crtFile, final String keyFile) throws Exception {
    	// Download CA certificate
        X509Certificate caCert;
        try (PEMParser reader = new PEMParser(new FileReader(caCrtFile))) {
            caCert = (X509Certificate) new JcaX509CertificateConverter().setProvider("BC").getCertificate((X509CertificateHolder) reader.readObject());
            logger.info("Loaded CA certificate from {}", caCrtFile);
        }

        // Load client certificate
        X509Certificate cert;
        try (PEMParser reader = new PEMParser(new FileReader(crtFile))) {
            cert = (X509Certificate) new JcaX509CertificateConverter().setProvider("BC").getCertificate((X509CertificateHolder) reader.readObject());
            logger.info("Loaded client certificate from {}", crtFile);
        }

        // Load the client key
        PrivateKey key;
        try (PEMParser reader = new PEMParser(new FileReader(keyFile))) {
            Object keyObject = reader.readObject();
            if (keyObject instanceof PEMKeyPair) {
                key = new JcaPEMKeyConverter().setProvider("BC").getPrivateKey(((PEMKeyPair) keyObject).getPrivateKeyInfo());
            } else if (keyObject instanceof PrivateKeyInfo) {
                key = new JcaPEMKeyConverter().setProvider("BC").getPrivateKey((PrivateKeyInfo) keyObject);
            } else {
                throw new IllegalArgumentException("Invalid key format");
            }
            logger.info("Loaded private key from {}", keyFile);
        }

        // CA certificate is used to authenticate the server
        KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
        caKs.load(null, null);
        caKs.setCertificateEntry("ca-certificate", caCert);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(caKs);

        // The client key and certificates are sent to the server so it can authenticate us
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setCertificateEntry("certificate", cert);
        ks.setKeyEntry("private-key", key, "password".toCharArray(), new java.security.cert.Certificate[]{cert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "password".toCharArray());

        // Create SSL socket factory
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return context.getSocketFactory();
    }
}