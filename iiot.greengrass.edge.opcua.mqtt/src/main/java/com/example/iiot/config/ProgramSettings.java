package com.example.iiot.config;

import com.fasterxml.jackson.annotation.JsonSetter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ProgramSettings holds configuration values loaded from the AWS Greengrass component
 * or local configuration file. These settings include OPC UA and MQTT parameters,
 * security credentials, tag list path, and UNS identifiers.
 */
public class ProgramSettings {

	// === OPC UA / UNS IDENTIFIERS ===

    /** Unique ID for the enterprise level in the UNS hierarchy. */
    private String unsEnterpriseId;

    /** Unique ID for the site level in the UNS hierarchy. */
    private String unsSiteId;

    /** Unique ID for the area level in the UNS hierarchy. */
    private String unsAreaId;

    /** Unique ID for the production unit in the UNS hierarchy. */
    private String unsProductionUnitId;

    /** Unique ID for the unit level in the UNS hierarchy. */
    private String unsUnitId;

    /** OPC UA server endpoint URI (e.g., opc.tcp://<HOST>:<PORT>). */
    private String opcuaServerEndpoint;

    /** Security policy used in OPC UA communication (e.g., Basic256Sha256). */
    private String opcuaSecurityPolicy;

    /** Security mode used in OPC UA (e.g., SignAndEncrypt). */
    private String opcuaSecurityMode;

    /** Path to OPC UA server certificate file. */
    private String opcuaCertFilePath;

    /** Path to keystore containing the private key and certificate. */
    private String opcuaKeystorePath;

    /** Path to truststore containing trusted certificates. */
    private String opcuaTruststorePath;

    /** Path to OPC UA client certificate file. */
    private String opcuaClientCertPath;

    /** Path to OPC UA client private key file. */
    private String opcuaClientKeyPath;

    /** Path to the folder containing trusted OPC UA certificates. */
    private String opcuaTrustListPath;    

    /** OPC UA username used for authentication. */
    private String opcuaUsername;
    
    /** AWS Secrets Manager secret name for OPC UA credentials. */
    private String opcuaKeystoreSecretName;
    
    /** AWS Secrets Manager secret name for OPC UA user credentials. */
    private String opcuaCredentialsSecretName;

    /** Application name for OPC UA client registration. */
    private String opcuaApplicationName;

    /** Application URI for OPC UA client. */
    private String opcuaApplicationUri;
    
    /**
     * Interval in seconds for forcing OPC UA tag value polling,
     * even if the value has not changed.
     * <p>
     * This is used to ensure periodic data reporting (e.g. once per hour)
     * for static or rarely changing values.
     *
     * Example use case: some metrics must be reported every hour regardless of changes.
     */
    private long opcuaForcePollIntervalSeconds;
    

    // === MQTT / AWS ===
    
    /** AWS region used for cloud service integrations (e.g. Secrets Manager, IoT). */
   private String awsRegion;

   /** AWS Greengrass Core Thing name used in IoT Core. */
    private String awsGreengrassCoreThingName;
    
    /** MQTT broker endpoint used for publishing/subscribing. */
    private String mqttBrokerEndpoint;

    /** MQTT client ID used by this component. */
    private String mqttBrokerClientId;

    /** MQTT topic for publishing DDATA messages. */
    private String mqttTopicDDATA;

    /** MQTT topic for subscribing to DCMD messages. */
    private String mqttTopicDCMD;

    /** MQTT topic for sending device STATE. */
    private String mqttTopicSTATE;

    /** Path to AWS root CA file. */
    private String rootCAFilePath;

    /** Path to AWS IoT Thing certificate file. */
    private String thingCertFilePath;

    /** Path to AWS IoT private key file. */
    private String privKeyFilePath;

    /** Path to tag list JSON configuration file. */
    private String tagListFilePath;

    /** Maximum number of MQTT metrics sent in a single payload. */
    private int mqttPayloadBatchSize;

    /** Logger instance. */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    // === Getters ===    
    public String getUnsEnterpriseId() { return unsEnterpriseId; }
    public String getUnsSiteId() { return unsSiteId; }
    public String getUnsAreaId() { return unsAreaId; }
    public String getUnsProductionUnitId() { return unsProductionUnitId; }
    public String getUnsUnitId() { return unsUnitId; }
    public String getOpcuaServerEndpoint() { return opcuaServerEndpoint; }
    public String getOpcuaSecurityPolicy() { return opcuaSecurityPolicy; }
    public String getOpcuaSecurityMode() { return opcuaSecurityMode; }
    public String getOpcuaCertFilePath() { return opcuaCertFilePath; }
    public String getOpcuaKeystorePath() { return opcuaKeystorePath; }
    public String getOpcuaTruststorePath() { return opcuaTruststorePath; }
    
    public String getOpcuaClientCertPath() { return opcuaClientCertPath; }
    public String getOpcuaClientKeyPath() { return opcuaClientKeyPath; }
    public String getOpcuaTrustListPath() { return opcuaTrustListPath; }
        
    public String getOpcuaUsername() { return opcuaUsername; }   
    public String getOpcuaKeystoreSecretName() { return opcuaKeystoreSecretName; }
    public String getOpcuaCredentialsSecretName() { return opcuaCredentialsSecretName; }
    public String getOpcuaApplicationName() { return opcuaApplicationName; }
    public String getOpcuaApplicationUri() { return opcuaApplicationUri; }
    
    public long getOpcuaForcePollIntervalSeconds() { return opcuaForcePollIntervalSeconds; }
    
    public String getAwsRegion() { return this.awsRegion; }
    public String getAwsGreengrassCoreThingName() { return awsGreengrassCoreThingName; }
    public String getMqttBrokerEndpoint() { return mqttBrokerEndpoint; }
    public String getMqttBrokerClientId() { return mqttBrokerClientId; }    
    public String getMqttTopicDDATA() { return mqttTopicDDATA; }
    public String getMqttTopicDCMD() { return mqttTopicDCMD; }
    public String getMqttTopicSTATE() { return mqttTopicSTATE; }    
    public String getRootCAFilePath() { return rootCAFilePath; }
    public String getThingCertFilePath() { return thingCertFilePath; }
    public String getPrivKeyFilePath() { return privKeyFilePath; }      
    public String getTagListFilePath() { return tagListFilePath; }      
    public int getMqttPayloadBatchSize() { return mqttPayloadBatchSize; }

    // === Setters ===
    public void setUnsEnterpriseId(String value) { this.unsEnterpriseId = value; }
    public void setUnsSiteId(String value) { this.unsSiteId = value; }
    public void setUnsAreaId(String value) { this.unsAreaId = value; }
    public void setUnsProductionUnitId(String value) { this.unsProductionUnitId = value; }
    public void setUnsUnitId(String value) { this.unsUnitId = value; }
    public void setOpcuaServerEndpoint(String value) { this.opcuaServerEndpoint = value; }
    public void setOpcuaSecurityPolicy(String value) { this.opcuaSecurityPolicy = value; }
    public void setOpcuaSecurityMode(String value) { this.opcuaSecurityMode = value; }
    public void setOpcuaCertFilePath(String value) { this.opcuaCertFilePath = value; }
    public void setOpcuaKeystorePath(String value) { this.opcuaKeystorePath = value; }	
    public void setOpcuaTruststorePath(String value) { this.opcuaTruststorePath = value; }
    
    public void setOpcuaClientCertPath(String value) { this.opcuaClientCertPath = value; }
    public void setOpcuaClientKeyPath(String value) { this.opcuaClientKeyPath = value; }
    public void setOpcuaTrustListPath(String value) { this.opcuaTrustListPath = value; }
                
    public void setOpcuaUsername(String value) { this.opcuaUsername = value; }
    public void setOpcuaKeystoreSecretName(String value) { this.opcuaKeystoreSecretName = value; }
    public void setOpcuaCredentialsSecretName(String value) { this.opcuaCredentialsSecretName = value; }
    public void setOpcuaApplicationName(String value) { this.opcuaApplicationName = value; }    
    public void setOpcuaApplicationUri(String value) { this.opcuaApplicationUri = value; }     
       
    public void setAwsRegion(String awsRegion) { this.awsRegion = awsRegion; }
    public void setAwsGreengrassCoreThingName(String value) { this.awsGreengrassCoreThingName = value; }
    public void setMqttBrokerEndpoint(String value) { this.mqttBrokerEndpoint = value; }
    public void setMqttBrokerClientId(String value) { this.mqttBrokerClientId = value; }   
    public void setMqttTopicDDATA(String value) { this.mqttTopicDDATA = value; }
    public void setMqttTopicDCMD(String value) { this.mqttTopicDCMD = value; }
    public void setMqttTopicSTATE(String value) { this.mqttTopicSTATE = value; }    
    public void setRootCAFilePath(String value) { this.rootCAFilePath = value; }
    public void setThingCertFilePath(String value) { this.thingCertFilePath = value; }
    public void setPrivKeyFilePath(String value) { this.privKeyFilePath = value; }
    public void setTagListFilePath(String value) { this.tagListFilePath = value; }

    
    /**
     * Parses mqttPayloadBatchSize from String input for JSON compatibility.
     * If parsing fails, default value 1 is used.
     *
     * @param value the string value from configuration
     */   
    @JsonSetter("mqttPayloadBatchSize")
    public void setMqttPayloadBatchSize(String value) {
        try {
            if (value != null) {
                this.mqttPayloadBatchSize = Integer.parseInt(value.trim());
                logger.info("Parsed mqttPayloadBatchSize from String '{}' as: {}", value, this.mqttPayloadBatchSize);
            } else {
                throw new NumberFormatException("Value is null");
            }
        } catch (NumberFormatException e) {
            logger.error("Invalid mqttPayloadBatchSize '{}', setting default 1", value);
            this.mqttPayloadBatchSize = 1;
        }
    }  
    
    
    /**
     * Parses opcuaForcePollIntervalSeconds from String input for JSON compatibility.
     * If parsing fails, default value 3600 is used (1 hour).
     *
     * @param value the string value from configuration
     */
    @JsonSetter("opcuaForcePollIntervalSeconds")
    public void setOpcuaForcePollIntervalSeconds(String value) {
        try {
            if (value != null) {
                this.opcuaForcePollIntervalSeconds = Long.parseLong(value.trim());
                logger.info("Parsed opcuaForcePollIntervalSeconds from String '{}' as: {}", value, this.opcuaForcePollIntervalSeconds);
            } else {
                throw new NumberFormatException("Value is null");
            }
        } catch (NumberFormatException e) {
            this.opcuaForcePollIntervalSeconds = 3600L; // fallback: 1 hour
            logger.error("Invalid opcuaForcePollIntervalSeconds '{}', setting default 3600", value);
        }
    }

}
