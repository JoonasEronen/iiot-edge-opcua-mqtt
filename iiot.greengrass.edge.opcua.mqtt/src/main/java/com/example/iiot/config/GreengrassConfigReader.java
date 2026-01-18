package com.example.iiot.config;

import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationRequest;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.security.Security;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GreengrassConfigReader is responsible for fetching and parsing
 * the component's runtime configuration from AWS Greengrass.
 */
public class GreengrassConfigReader {
    
	private final GreengrassCoreIPCClientV2 ipcClient;
    private ProgramSettings programSettings;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    /**
     * Constructs a new GreengrassConfigReader and immediately loads the configuration.
     *
     * @throws IOException if the IPC client cannot be initialized
     */
    public GreengrassConfigReader() throws IOException {
        this.ipcClient = GreengrassCoreIPCClientV2.builder().build();       
        loadConfiguration();
    }

    
    /**
     * Loads and parses the component's runtime configuration from Greengrass IPC.
     * Populates {@link ProgramSettings} with values from the JSON config structure.
     */
    private void loadConfiguration() {
        try {
        	
        	Security.setProperty("ocsp.enable", "true");
        	Security.setProperty("com.sun.security.ocsp.timeout", "10");

            
            GetConfigurationRequest request = new GetConfigurationRequest();            
            CompletableFuture<GetConfigurationResponse> responseFuture = ipcClient.getConfigurationAsync(request);
            GetConfigurationResponse response = responseFuture.get(10, TimeUnit.SECONDS);
            
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> configMap = response.getValue();            
        
            JsonNode configJson = objectMapper.valueToTree(configMap);            
         
            if (configJson.has("ComponentConfiguration")) {
                configJson = configJson.get("ComponentConfiguration");
            }
            if (configJson.has("DefaultConfiguration")) {
                configJson = configJson.get("DefaultConfiguration");
            }                       
            
            if (configJson.has("programSettings")) {
                JsonNode settingsNode = configJson.get("programSettings");

                if (settingsNode != null && !settingsNode.isMissingNode()) {
                    ProgramSettings settings = new ProgramSettings();

                    // AWS/MQTT       
                    settings.setAwsRegion(getJsonValue(settingsNode,"awsRegion"));
                    settings.setAwsGreengrassCoreThingName(getJsonValue(settingsNode, "awsGreengrassCoreThingName"));
                    settings.setMqttBrokerEndpoint(getJsonValue(settingsNode, "mqttBrokerEndpoint"));
                    settings.setMqttBrokerClientId(getJsonValue(settingsNode, "mqttBrokerClientId"));
                    settings.setMqttTopicDDATA(getJsonValue(settingsNode, "mqttTopicDDATA"));
                    settings.setMqttTopicDCMD(getJsonValue(settingsNode, "mqttTopicDCMD"));
                    settings.setMqttTopicSTATE(getJsonValue(settingsNode, "mqttTopicSTATE"));
                    settings.setRootCAFilePath(getJsonValue(settingsNode, "rootCAFilePath"));
                    settings.setThingCertFilePath(getJsonValue(settingsNode, "thingCertFilePath"));
                    settings.setPrivKeyFilePath(getJsonValue(settingsNode, "privKeyFilePath"));
                    settings.setTagListFilePath(getJsonValue(settingsNode, "tagListFilePath"));                 
                    settings.setMqttPayloadBatchSize(getJsonValue(settingsNode, "mqttPayloadBatchSize"));

                    // UNS
                    settings.setUnsEnterpriseId(getJsonValue(settingsNode, "unsEnterpriseId"));
                    settings.setUnsSiteId(getJsonValue(settingsNode, "unsSiteId"));
                    settings.setUnsAreaId(getJsonValue(settingsNode, "unsAreaId"));
                    settings.setUnsProductionUnitId(getJsonValue(settingsNode, "unsProductionUnitId"));
                    settings.setUnsUnitId(getJsonValue(settingsNode, "unsUnitId"));

                    // OPC UA
                    settings.setOpcuaServerEndpoint(getJsonValue(settingsNode, "opcuaServerEndpoint"));
                    settings.setOpcuaSecurityPolicy(getJsonValue(settingsNode, "opcuaSecurityPolicy"));
                    settings.setOpcuaSecurityMode(getJsonValue(settingsNode, "opcuaSecurityMode"));
                    settings.setOpcuaCertFilePath(getJsonValue(settingsNode, "opcuaCertFilePath"));                    
                    settings.setOpcuaTruststorePath(getJsonValue(settingsNode, "opcuaTruststorePath"));
                    settings.setOpcuaKeystorePath(getJsonValue(settingsNode, "opcuaKeystorePath"));                    
                    settings.setOpcuaClientCertPath(getJsonValue(settingsNode, "opcuaClientCertPath"));
                    settings.setOpcuaClientKeyPath(getJsonValue(settingsNode, "opcuaClientKeyPath"));
                    settings.setOpcuaTrustListPath(getJsonValue(settingsNode, "opcuaTrustListPath"));                                                        
                    settings.setOpcuaUsername(getJsonValue(settingsNode, "opcuaUsername"));     
                    settings.setOpcuaKeystoreSecretName(getJsonValue(settingsNode, "opcuaKeystoreSecretName"));
                    settings.setOpcuaCredentialsSecretName(getJsonValue(settingsNode, "opcuaCredentialsSecretName"));
                    settings.setOpcuaApplicationName(getJsonValue(settingsNode, "opcuaApplicationName"));
                    settings.setOpcuaApplicationUri(getJsonValue(settingsNode, "opcuaApplicationUri"));
                    settings.setOpcuaForcePollIntervalSeconds(getJsonValue(settingsNode, "opcuaForcePollIntervalSeconds"));
                                       
                    this.programSettings = settings;   
                    
                    logger.info("Greengrass configuration loaded successfully.");
                   
                } else {
                	logger.error("ERROR: ProgramSettings JSON structure is missing or incorrect!");
                }
            } else {
            	logger.error("ProgramSettings not found in Greengrass configuration.");
            }

        } catch (Exception e) {
        	logger.error("Error reading Greengrass configuration: " + e.getMessage());
            e.printStackTrace();
        }               
    }

    
    /**
     * Returns the loaded {@link ProgramSettings} configuration object.
     *
     * @return ProgramSettings, or null if configuration failed
     */
    public ProgramSettings getProgramSettings() {
        return programSettings;
    }


    /**
     * Safely extracts a value from a JSON node by key.
     *
     * @param node the parent JSON node
     * @param key  the key to extract
     * @return the string value, or empty string if missing
     */
    private String getJsonValue(JsonNode node, String key) {
        if (node.has(key)) {
            JsonNode valueNode = node.get(key);
            if (!valueNode.isNull()) {
                return valueNode.asText();
            } else {
                logger.warn("WARNING: Key '{}' is null in JSON.", key);
                return null;
            }
        } else {
            logger.error("ERROR: Key '{}' missing from JSON.", key);
            return "";
        }
    }

}
