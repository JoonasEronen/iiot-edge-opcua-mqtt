package com.example.iiot.mqtt;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import com.example.iiot.config.ProgramSettings;


/**
 * Handles formatting and sending MQTT messages to the broker.
 * <p>
 * Supports batching of process metrics and immediate sending of state metrics.
 * Also prevents duplicate message sending using a time-based key cache.
 */
public class MqttMessageHandler {

    private MqttPubSubClient mqttPublisher = null;
    private int batchSize;    
    private String mqttTopicDDATA;
    private String mqttTopicSTATE;
    
    /**
     * Stores metric payloads until the batch size is reached.
     */
  //  private final Map<String, Object> processMetrics = new LinkedHashMap<>();    
    //
    
    /**
     * Stores metric payloads until the batch size is reached.
     * Automatically removes oldest entries once size exceeds 1000 to prevent memory overflow.
     */
    private final Map<String, Object> processMetrics = Collections.synchronizedMap(new LinkedHashMap<String, Object>(1000, 0.75f, true) {
        private static final long serialVersionUID = 1L;
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
            return size() > 1000; // Evict oldest when over limit
        }
    });
    
    
    
    /**
     * Keeps track of recently sent messages to prevent duplicates.
     * Automatically removes oldest entries once size exceeds 1000.
     */
    private final Map<String, Boolean> sentMessages = Collections.synchronizedMap(new LinkedHashMap<String, Boolean>(1000, 0.75f, true) {
       
		private static final long serialVersionUID = 1L;
		protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > 1000; // Only keeps the last 1000 messages in memory
        }
    });

    private final Logger logger = LoggerFactory.getLogger(getClass());

    
    /**
     * Constructor
     *
     * @param mqttPublisher   the MQTT client used for publishing
     * @param latch           optional latch for startup synchronization (currently unused)
     * @param programSettings config settings (topics, batch size etc.)
     */
    public MqttMessageHandler(MqttPubSubClient mqttPublisher, CountDownLatch latch, ProgramSettings programSettings) {
        this.mqttPublisher = mqttPublisher;
        this.batchSize = programSettings.getMqttPayloadBatchSize();        
        this.mqttTopicDDATA = programSettings.getMqttTopicDDATA();
        this.mqttTopicSTATE = programSettings.getMqttTopicSTATE();
        logger.info("DEBUG: MqttMessageHandler initialized with batchSize = {}", this.batchSize);
    }

    
    /**
     * Adds a metric to be published. Supports batching for process data and immediate send for state data.
     *
     * @param name           metric name (e.g., "enterprise/site/PLC_Heartbeat")
     * @param value          measured value
     * @param timeStamp      timestamp of the value
     * @param lastValue      previous value for comparison
     * @param threshold      true if threshold logic triggered sending
     * @param isState        true if the message is a state (not process) metric
     */
    public synchronized void addMetric(String name, Object value, Date timeStamp, Object lastValue, boolean threshold, boolean isState) {
        Map<String, Object> metricData = new LinkedHashMap<>();
        metricData.put("name", name);
        metricData.put("value", value);
        metricData.put("timestamp", timeStamp.getTime());
        metricData.put("lastValue", lastValue);
        metricData.put("thresholdExceeded", threshold);

        String uniqueKey = name + "_ts_" + timeStamp.getTime();
        String payloadHash = uniqueKey + ":" + value.toString(); // Unique hash to prevent duplication

        if (sentMessages.putIfAbsent(payloadHash, true) != null) { // Preventing duplications
            logger.warn("Skipping duplicate message: {}", uniqueKey);
            return;
        }

        if (isState) {
            // // STATE metrics are sent immediately
            Map<String, Object> statePayload = new LinkedHashMap<>();
            statePayload.put(uniqueKey, metricData);
            sendPayloadToMqttBroker(createPayload(statePayload), mqttTopicSTATE);
        } else {
            // PROCESS metrics are sent in batches
            processMetrics.put(uniqueKey, metricData);
            if (processMetrics.size() >= batchSize) {
                if (sendPayloadToMqttBroker(createPayload(processMetrics), mqttTopicDDATA)) {
                    processMetrics.clear();  // Only cleared when the message has been sent
                }
            }
        }
    }

    
    /**
     * Converts the metric map into a JSON string payload.
     *
     * @param metrics map of metrics
     * @return JSON-formatted payload
     */
    private String createPayload(Map<String, Object> metrics) {
        return new Gson().toJson(metrics);
    }

    
    /**
     * Sends payload to MQTT topic with up to 3 retry attempts.
     *
     * @param payload   the JSON-formatted message
     * @param mqttTopic the topic to publish to
     * @return true if send was successful
     */
    private boolean sendPayloadToMqttBroker(String payload, String mqttTopic) {
        for (int i = 0; i < 3; i++) { // Maximum 3 attempts
            if (mqttPublisher.isConnected()) {
                mqttPublisher.mqttPublish(payload.getBytes(), mqttTopic);
                logger.info("Payload sent to topic: {}", mqttTopic);
                return true; // The transmission was successful
            }
            logger.warn("MQTT client not connected, retrying {}/3...", i + 1);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.error("Failed to send MQTT message, discarding: {}", payload);
        return false;
    }    
}
