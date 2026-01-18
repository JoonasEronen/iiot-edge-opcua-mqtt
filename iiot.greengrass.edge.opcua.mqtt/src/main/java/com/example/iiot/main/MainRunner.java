package com.example.iiot.main;

import java.io.IOException;
import java.security.Security;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.example.iiot.config.GreengrassConfigReader;
import com.example.iiot.config.ProgramSettings;
import com.example.iiot.config.TagReader;
import com.example.iiot.model.Tag;
import com.example.iiot.mqtt.MqttMessageHandler;
import com.example.iiot.mqtt.MqttPubSubClient;
import com.example.iiot.opcua.ManagedOpcUASubscription;
import com.example.iiot.opcua.OpcUaClientConnector;
import com.example.iiot.opcua.OpcUaPublisher;


/**
 * Entry point for the IIoT application.
 * <p>
 * Initializes OPC UA and MQTT components, reads configuration and tag list,
 * and handles bidirectional data flow between OPC UA server and MQTT broker.
 */
public class MainRunner {
	
	/**
	 * Disables verbose and sensitive AWS/HTTP logging that could expose secrets in production.
	 * This method should be called at application startup, before any AWS SDK or HTTP clients are initialized.
	 */
	private static void disableAwsAndApacheLogging() {
	    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
	    context.getLogger("org.apache.http.wire").setLevel(Level.OFF);
	    context.getLogger("org.apache.http.headers").setLevel(Level.OFF);
	    context.getLogger("org.apache.http").setLevel(Level.OFF);
	    context.getLogger("software.amazon.awssdk").setLevel(Level.OFF);
	    context.getLogger("software.amazon.awssdk.request").setLevel(Level.OFF);
	    context.getLogger("software.amazon.awssdk.response").setLevel(Level.OFF);
	    context.getLogger("software.amazon.awssdk.http.apache").setLevel(Level.OFF);
	    context.getLogger("software.amazon.awssdk.services.secretsmanager").setLevel(Level.OFF);
	}

	
	  /**
     * Main method that bootstraps the entire application.
     *
     * @param args command-line arguments (not used)
     * @throws InterruptedException if thread is interrupted
     * @throws IOException          if configuration reading fails
     */
    public static void main(String[] args) throws InterruptedException, IOException {    
    	
    	disableAwsAndApacheLogging();    	
        org.slf4j.Logger appLogger = LoggerFactory.getLogger(MainRunner.class);       

    	
        // Add BouncyCastle provider (needed for TLS/SSL parsing etc.)
        Security.addProvider(new BouncyCastleProvider());

        // Synchronization latch to wait for MQTT connection before proceeding
        CountDownLatch latch = new CountDownLatch(1);

        // Container for MQTT client instance
        MqttPubSubClient[] mqttClient = new MqttPubSubClient[1];

        // Read configuration from Greengrass and validate
        GreengrassConfigReader configReader = new GreengrassConfigReader();
        ProgramSettings programSettings = configReader.getProgramSettings();
        if (programSettings == null) {
            appLogger.error("ERROR: Failed to read program settings.");
            return;
        }
        
        // Read tag definitions from local file
        List<Tag> tagList = TagReader.getTagList(programSettings.getTagListFilePath());
        if (tagList == null) {
        	appLogger.error("ERROR: Failed to read tag list.");
            return;
        }

        // Create OPC UA writer for CMD-messages (MQTT → OPC UA)
        // OPC UA client is injected later by ManagedOpcUASubscription
        OpcUaPublisher opcUaPublisher = new OpcUaPublisher(null, tagList);
        
        // Create MQTT client for bidirectional communication (includes CMD handling)
        mqttClient[0] = new MqttPubSubClient(programSettings, opcUaPublisher);

        // Create MQTT message handler (for OPC UA - MQTT publishing)
        MqttMessageHandler mqttMessageHandler = new MqttMessageHandler(mqttClient[0], latch, programSettings);

        // Create OPC UA subscriber (handles subscription updates and polling)
        ManagedOpcUASubscription opcUaSubscription = new ManagedOpcUASubscription(tagList, programSettings, mqttMessageHandler);      
       
        // Create OPC UA client connector, integrates subscriptions and CMD write logic
        OpcUaClientConnector opcUaClientConnector = new OpcUaClientConnector(opcUaSubscription, configReader);

        // Connect to MQTT in a separate thread to avoid blocking the main thread
        new Thread(() -> {
            try {
                mqttClient[0].connect();
                if (mqttClient[0].isConnected()) {
                    latch.countDown(); // Continue once MQTT is connected
                } else {
                	appLogger.error("ERROR: Failed to connect to MQTT broker.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Wait for MQTT to be ready before continuing
        latch.await();
                

        try {                                                                                  
        	// Start OPC UA client and begin subscriptions
            opcUaClientConnector.run();
            
            // // Gets the active client and injects it to the publisher
            opcUaPublisher.setClient(opcUaClientConnector.getClient());

            // Add shutdown hook to disconnect cleanly
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    mqttClient[0].disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));

            // Keep main thread alive (application continues running)
            new CountDownLatch(1).await();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}