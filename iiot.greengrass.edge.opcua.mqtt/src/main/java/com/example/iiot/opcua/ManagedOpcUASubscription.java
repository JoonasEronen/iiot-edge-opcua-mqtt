package com.example.iiot.opcua;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedDataItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.iiot.config.ProgramSettings;
import com.example.iiot.model.Tag;
import com.example.iiot.mqtt.MqttMessageHandler;



/**
 * Manages OPC UA subscriptions using Milo SDK and sends data to MQTT via MqttMessageHandler.
 * <p>
 * Subscribes to configured OPC UA tags, listens for changes, applies threshold rules, and sends
 * filtered data to AWS IoT Core or other MQTT targets.
 */
public class ManagedOpcUASubscription {
	
	// OPC UA tag list and metadata
    private final List<Tag> tagList;
    private final String enterpriseId;
    private final String siteId;
    private final String areaId;
    private final String productionUnitId;
    private final String unitId;   
    
    // Full metric name builder (e.g., "enterprise/site/unit")
    private final StringBuilder metricName;   
    
    // Handler that pushes data to MQTT
    private final MqttMessageHandler mqttMessageHandler;
    private final ExecutorService dataProcessingExecutor;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    // Store last values per tag to apply threshold logic
    private final ConcurrentMap<String, Tag> previousValues = new ConcurrentHashMap<>();
        
    // Define tags considered as system STATE info
    private static final Set<String> STATE_METRICS = Set.of("PLC_Heartbeat", "OPCUA_ServerState");
    
    /**
     * Scheduler for periodically polling values of tags that have not changed recently.
     */
    private final ScheduledExecutorService periodicPollScheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * Time interval (in seconds) after which tag values are forcefully re-read from the OPC UA server
     * even if the value has not changed.
     * <p>
     * This ensures that data from static or rarely changing OPC UA tags is periodically sent to
     * downstream systems (e.g., via MQTT) to maintain data freshness and completeness.
     * 
     * Default: 3600 seconds (1 hour), configurable via Greengrass component configuration.
     */
    private final long forcePollIntervalSeconds;

      
    /**
     * Constructor
     *
     * @param tagList the list of monitored OPC UA tags
     * @param programSettings application configuration including UNS hierarchy
     * @param mqttMessageHandler the MQTT message handler for data publishing
     */
    public ManagedOpcUASubscription(List<Tag> tagList, ProgramSettings programSettings, MqttMessageHandler mqttMessageHandler) {
        this.tagList = tagList;
        this.enterpriseId = programSettings.getUnsEnterpriseId();
        this.siteId = programSettings.getUnsSiteId();
        this.areaId = programSettings.getUnsAreaId();
        this.productionUnitId = programSettings.getUnsProductionUnitId();
        this.unitId = programSettings.getUnsUnitId();        
        this.mqttMessageHandler = mqttMessageHandler;       
             
        // Safe and backpressure-enabled thread pool executor:
        // - Single thread processes OPC UA updates (ensures order)
        // - Bounded queue prevents unbounded memory usage
        // - CallerRunsPolicy applies backpressure if queue is full (no data loss)
        this.dataProcessingExecutor = new ThreadPoolExecutor(
         1, 1, // core and max threads: single-threaded execution
         60, TimeUnit.SECONDS, // keep-alive time for idle thread (not used here)
         new LinkedBlockingQueue<>(1000), // bounded queue with max size of 1000 tasks
         new ThreadPoolExecutor.CallerRunsPolicy() // slow down producer instead of dropping tasks
     );
        
        this.forcePollIntervalSeconds = programSettings.getOpcuaForcePollIntervalSeconds();
        // Build metric name
        metricName = new StringBuilder();
        if (enterpriseId != null) metricName.append(enterpriseId).append("/");
        if (siteId != null) metricName.append(siteId).append("/");
        if (areaId != null) metricName.append(areaId).append("/");
        if (productionUnitId != null) metricName.append(productionUnitId).append("/");
        if (unitId != null) metricName.append(unitId).append("/");        
    }

    
    /**
     * Starts the OPC UA subscription and reconnects if needed.
     *
     * @param client connected OPC UA client
     * @param future future representing client state
     */
    public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {        
        try {
            client.connect().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore suspended state
            logger.error("Initial connection interrupted: {}", e.getMessage(), e);
            return; // Terminate if interrupted
        } catch (ExecutionException e) {
            logger.error("Initial connection failed: {}", e.getMessage(), e);
            Thread.sleep(5000); // Wait before retrying
        }    
              
        
        client.getSubscriptionManager().addSubscriptionListener(new UaSubscriptionManager.SubscriptionListener() {
            @Override
            public void onSubscriptionTransferFailed(UaSubscription subscription, StatusCode statusCode) {
                logger.warn("Subscription transfer failed: " + statusCode);
                try {
                    recreateSubscriptions(client, tagList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        ManagedSubscription subscription = ManagedSubscription.create(client, 1000);//, opcuaSubscriptionCycle_ms);        
        createSubscriptions(client, subscription, tagList);       
        
        startPeriodicPolling(client);
    }

    
    /**
     * Polls OPC UA values for the given list of tags.
     * Can be used both at startup and during scheduled polling.
     *
     * @param client connected OPC UA client
     * @param tags   list of tags to poll values for
     */
    private void pollValues(OpcUaClient client, List<Tag> tags) {
        try {
            List<NodeId> nodeIds = tags.stream()
                .map(tag -> {
                    Object nodeId = tag.getNodeId();
                    if (nodeId instanceof Integer) {
                        return new NodeId(tag.getNamespaceIndex(), (Integer) nodeId);
                    } else if (nodeId instanceof String) {
                        return new NodeId(tag.getNamespaceIndex(), (String) nodeId);
                    } else {
                        throw new IllegalArgumentException("Unsupported NodeId type: " + nodeId);
                    }
                })
                .collect(Collectors.toList());

            logger.debug("Polling OPC UA values for {} tags...", nodeIds.size());

            client.readValues(0, TimestampsToReturn.Both, nodeIds)
                .thenAccept(values -> {
                    for (int i = 0; i < tags.size(); i++) {
                        logger.debug("Polled value for NodeId {}: {}", nodeIds.get(i), values.get(i));
                        processPolledData(tags.get(i), values.get(i), true);
                    }
                    logger.info("Values polled successfully.");
                })
                .exceptionally(e -> {
                    logger.error("Failed to poll values: {}", e.getMessage(), e);
                    return null;
                });

        } catch (Exception e) {
            logger.error("Failed to poll values: {}", e.getMessage(), e);
        }
    }



    /**
     * Processes a polled data value and checks if it should be sent to MQTT.
     * Prevents duplicate sends if value and timestamp match previous value.
     *
     * @param tag   the tag whose value is being processed
     * @param value the OPC UA DataValue
     */
    private void processPolledData(Tag tag, DataValue value, boolean isForcedPoll) {
        if (value == null || value.getValue() == null) {
            logger.warn("Polled value is null for tag: {}", tag.getMappedName());
            return;
        }

        Object rawValue = value.getValue().getValue();
        Date sourceTime = new Date(value.getSourceTime().getJavaTime());
        Long sourceTimeUnix = sourceTime.getTime() / 1000;

        Tag previousTag = previousValues.get(tag.getMappedName());

        boolean isDuplicate = false;
        Object previousValue = null;

        if (rawValue instanceof String) {
            String strValue = (String) rawValue;
            if (previousTag != null && strValue.equals(previousTag.getStringValue())
                    && sourceTimeUnix.equals(previousTag.getTimestampUnix())) {
                isDuplicate = true;
            } else {
                previousTag = new Tag();
                previousTag.setStringValue(strValue);
                previousTag.setTimestampUnix(sourceTimeUnix);
                previousValues.put(tag.getMappedName(), previousTag);
                previousValue = previousTag.getStringValue();
            }
        } else if (rawValue instanceof Boolean) {
            double tagValue = ((Boolean) rawValue) ? 1.0 : 0.0;
            if (previousTag != null && tagValue == previousTag.getValue()
                    && sourceTimeUnix.equals(previousTag.getTimestampUnix())) {
                isDuplicate = true;
            } else {
                previousTag = new Tag();
                previousTag.setValue(tagValue);
                previousTag.setTimestampUnix(sourceTimeUnix);
                previousValues.put(tag.getMappedName(), previousTag);
                previousValue = previousTag.getValue();
            }
        } else if (rawValue instanceof Number) {
            double tagValue = ((Number) rawValue).doubleValue();
            if (previousTag != null && tagValue == previousTag.getValue()
                    && sourceTimeUnix.equals(previousTag.getTimestampUnix())) {
                isDuplicate = true;
            } else {
                previousTag = new Tag();
                previousTag.setValue(tagValue);
                previousTag.setTimestampUnix(sourceTimeUnix);
                previousValues.put(tag.getMappedName(), previousTag);
                previousValue = previousTag.getValue();
            }
        } else {
            logger.warn("Unsupported data type: {} for tag: {}", rawValue.getClass().getSimpleName(), tag.getMappedName());
            return;
        }

        if (isDuplicate) {
            logger.debug("Duplicate value detected, skipping MQTT send: {} = {}", tag.getMappedName(), rawValue);
            return;
        }        
        
        if (isForcedPoll && previousTag != null) {
            long now = System.currentTimeMillis() / 1000;
            long timeSinceLastSend = now - previousTag.getTimestampUnix();
            if (timeSinceLastSend < forcePollIntervalSeconds) {
                logger.debug("Skipping forced poll: recently sent ({} s ago): {}", timeSinceLastSend, tag.getMappedName());
                return;
            }
        }
                
        logger.info("Polled value stored and sent: {} = {}", tag.getMappedName(), rawValue);
        sendMQTTMessage(tag, rawValue, sourceTime, previousValue, false);
    }

    
    /**
     * Starts a scheduled polling task that runs at fixed intervals.
     * <p>
     * This task checks all monitored tags and forces a read for those
     * whose last update timestamp is older than the defined threshold.
     * 
     * @param client the connected OPC UA client used for reading values
     */
    private void startPeriodicPolling(OpcUaClient client) {
        periodicPollScheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis() / 1000; // Unix time in seconds

                List<Tag> tagsToPoll = tagList.stream()
                    .filter(tag -> {
                        Tag previous = previousValues.get(tag.getMappedName());
                        if (previous == null || previous.getTimestampUnix() == null) return true;
                        return (now - previous.getTimestampUnix()) >= forcePollIntervalSeconds;
                    })
                    .collect(Collectors.toList());

                if (!tagsToPoll.isEmpty()) {
                    logger.info("Force polling {} tags due to time interval exceeded.", tagsToPoll.size());
                    pollValues(client, tagsToPoll);
                }

            } catch (Exception e) {
                logger.error("Error in periodic polling: {}", e.getMessage(), e);
            }
        }, forcePollIntervalSeconds, forcePollIntervalSeconds, TimeUnit.SECONDS);
    }


    

    /**
     * Creates ManagedDataItems (subscriptions) and sets listeners for value changes.
     *
     * @param client       connected OPC UA client
     * @param subscription the ManagedSubscription instance
     * @param tagList      the list of OPC UA tags to subscribe to
     * @throws Exception if creation fails for any of the tags
     */
    private void createSubscriptions(OpcUaClient client, ManagedSubscription subscription, List<Tag> tagList) throws Exception {
        List<Tag> tags = tagList;

        this.pollValues(client, tagList);
        
        // Create ManagedDataItems
        List<ManagedDataItem> dataItems = subscription.createDataItems(
        	    tags.stream().map(tag -> {
        	        Object nodeId = tag.getNodeId();
        	        if (nodeId instanceof Integer) {
        	            return new NodeId(tag.getNamespaceIndex(), (Integer) nodeId);
        	        } else if (nodeId instanceof String) {
        	            return new NodeId(tag.getNamespaceIndex(), (String) nodeId);
        	        } else {
        	            throw new IllegalArgumentException("Unsupported NodeId type: " + nodeId);
        	        }
        	    }).collect(Collectors.toList())
        	);

        

        // Handle data updates
        for (ManagedDataItem dataItem : dataItems) {
            if (dataItem.getStatusCode().isGood()) {
                dataItem.addDataValueListener((item, value) -> {
                    dataProcessingExecutor.submit(() -> handleDataUpdate(tags, item, value));
                });
            } else {
                throw new RuntimeException("ManagedDataItem failed. Status code: " + dataItem.getStatusCode());
            }
        }
    }
    
    
    /**
     * Handles data updates from OPC UA server and maps incoming values to configured tags.
     *
     * @param tags  list of all known tags
     * @param item  the OPC UA data item triggering the update
     * @param value the new DataValue from OPC UA
     */
    private void handleDataUpdate(List<Tag> tags, ManagedDataItem item, DataValue value) {
        Object identifier = item.getNodeId().getIdentifier(); // Tämä voi olla Integer tai String

        Tag tag = tags.stream()
            .filter(t -> isMatchingNodeId(t.getNodeId(), identifier)) // Huom. tarkista getNodeId()!
            .findFirst()
            .orElse(null);

        if (tag != null) {
            processTagUpdate(tag, value);
        }
    }

    
    /**
     * Matches an OPC UA NodeId with a configured tag node identifier.
     * Accepts both String and Integer node identifiers.
     *
     * @param nodeID     the configured tag NodeId (String or Integer)
     * @param identifier the identifier from the OPC UA event
     * @return true if the IDs match, false otherwise
     */
    private boolean isMatchingNodeId(Object nodeID, Object identifier) {
        if (nodeID == null || identifier == null) {
            return false;
        }

        if (identifier instanceof Integer) {
            try {
                return Integer.parseInt(nodeID.toString()) == (Integer) identifier;
            } catch (NumberFormatException e) {
                return false; // nodeID ei ole numero
            }
        } else {
            return nodeID.toString().equals(identifier.toString());
        }
    }

    
    /**
     * Processes a new OPC UA tag value, applies threshold and/or time-based logic,
     * and sends the value to MQTT if sending criteria are met.
     *
     * Supports data types: String, Boolean, Integer, Double.
     *
     * @param tag   the updated OPC UA tag
     * @param value the new DataValue received from OPC UA server
     */
    private void processTagUpdate(Tag tag, DataValue value) {
        if (value == null || value.getValue() == null) {
            logger.warn("Received null value for tag: {}", tag.getMappedName());
            return;
        }

        Object rawValue = value.getValue().getValue();
        Date sourceTime = new Date(value.getSourceTime().getJavaTime());
        Long sourceTimeUnix = sourceTime.getTime() / 1000;
        String tagName = tag.getMappedName();
        Tag previousTag = previousValues.get(tagName);

        boolean useThreshold = tag.getUseThreshold();
        boolean useThresholdSeconds = tag.getUseThresholdSeconds();
        String thresholdType = tag.getThresholdType() != null ? tag.getThresholdType() : "";
        Double threshold = tag.getThreshold() != null ? tag.getThreshold() : 0.0;

        Object previousValue = null;
        boolean thresholdExceeded = false;
        boolean timeExceeded = false;

        if (previousTag != null) {
            previousValue = (rawValue instanceof String) ? previousTag.getStringValue() : previousTag.getValue();
        }

        // Apply threshold and time logic
        thresholdExceeded = checkThreshold(useThreshold, previousTag, rawValue, thresholdType, threshold);
        timeExceeded = checkTimeThreshold(useThresholdSeconds, previousTag, sourceTimeUnix, tag.getThresholdSeconds());

        boolean sendToMQTT = decideIfSendToMQTT(useThreshold, useThresholdSeconds, thresholdExceeded, timeExceeded);

        if (sendToMQTT) {
            logger.info("MQTT sending triggered for tag: {}", tagName);

            if (previousTag == null) {
                previousTag = new Tag();
            }

            if (rawValue instanceof String) {
                previousTag.setStringValue((String) rawValue);
            } else if (rawValue instanceof Boolean) {
                previousTag.setValue(((Boolean) rawValue) ? 1.0 : 0.0);
            } else if (rawValue instanceof Number) {
                previousTag.setValue(((Number) rawValue).doubleValue());
            }

            previousTag.setTimestampUnix(sourceTimeUnix);
            previousValues.put(tagName, previousTag);

            sendMQTTMessage(tag, rawValue, sourceTime, previousValue, thresholdExceeded);
        }
    }
       
   
    /**
     * Checks whether the tag's value change exceeds its configured threshold.
     * Supports numeric thresholds (absolute or percentage) and string changes.
     *
     * @param useThreshold   whether threshold checking is enabled
     * @param previousTag    previous tag state (value or string)
     * @param currentValue   current tag value (Double, Boolean, or String)
     * @param thresholdType  "absolute", "percentage", or default for string
     * @param threshold      numeric threshold (ignored for string)
     * @return true if change exceeds threshold
     */
    private boolean checkThreshold(boolean useThreshold, Tag previousTag, Object currentValue, String thresholdType, Double threshold) {
        if (!useThreshold || previousTag == null || currentValue == null) return false;

        if (currentValue instanceof String) {
            String prevStr = previousTag.getStringValue();
            return !currentValue.equals(prevStr);
        }

        if (currentValue instanceof Boolean) {
            currentValue = ((Boolean) currentValue) ? 1.0 : 0.0;
        }

        if (currentValue instanceof Number) {
            Double current = ((Number) currentValue).doubleValue();
            Double previous = previousTag.getValue();
            if (previous == null) return false;

            if ("absolute".equalsIgnoreCase(thresholdType)) {
                return Math.abs(current - previous) >= threshold;
            } else if ("percentage".equalsIgnoreCase(thresholdType)) {
                return previous != 0.0 && Math.abs(current - previous) / previous * 100 > threshold;
            }
        }

        return false;
    }


    
    /**
     * Checks whether the time since the last update exceeds the configured threshold.
     *
     * @param useThresholdSeconds  whether time threshold is enabled
     * @param previousTag          previously recorded tag data (with timestamp)
     * @param currentUnixTime      current value timestamp (in seconds)
     * @param thresholdSeconds     number of seconds before update is considered due
     * @return true if time threshold is exceeded
     */
    private boolean checkTimeThreshold(boolean useThresholdSeconds, Tag previousTag, Long currentUnixTime, Double thresholdSeconds) {
        if (!useThresholdSeconds || previousTag == null || previousTag.getTimestampUnix() == null) return false;
        return Math.abs(currentUnixTime - previousTag.getTimestampUnix()) >= thresholdSeconds;
    }

    
    /**
     * Determines whether the tag value should be sent to MQTT based on threshold conditions.
     *
     * @param useThreshold        whether value change threshold is enabled
     * @param useThresholdSeconds whether time threshold is enabled
     * @param thresholdExceeded   whether value change exceeded threshold
     * @param timeExceeded        whether time threshold was exceeded
     * @return true if message should be sent
     */
    private boolean decideIfSendToMQTT(boolean useThreshold, boolean useThresholdSeconds, boolean thresholdExceeded, boolean timeExceeded) {
        return (useThreshold && !useThresholdSeconds && thresholdExceeded) ||
               (!useThreshold && useThresholdSeconds && timeExceeded) ||
               (useThreshold && useThresholdSeconds && thresholdExceeded && timeExceeded) ||
               (!useThreshold && !useThresholdSeconds);
    }
    
    
    /**
     * Sends the given tag value to the MQTT message handler with all required metadata.
     *
     * @param tag               the tag object
     * @param tagValue          the current value (numeric, boolean, or string)
     * @param sourceTime        the timestamp of the value
     * @param previousValue     the last known value
     * @param thresholdExceeded whether threshold condition was triggered
     */
    private void sendMQTTMessage(Tag tag, Object tagValue, Date sourceTime, Object previousValue, boolean thresholdExceeded) {        
        String mappedName = tag.getMappedName();
        String fullMetricName = metricName + mappedName;
        
        boolean isState = STATE_METRICS.contains(mappedName); // Check the Set to see if it is part of the state data.        
             
        mqttMessageHandler.addMetric(fullMetricName, (tagValue instanceof Number) ? ((Number) tagValue).doubleValue() : tagValue,
                sourceTime, previousValue, thresholdExceeded, isState);
    }
  
    
    /**
     * Recreates the subscriptions (e.g. after connection loss or transfer failure).
     *
     * @param client   connected OPC UA client
     * @param tagList  list of tags to re-subscribe to
     * @throws Exception if subscription creation fails
     */
    private void recreateSubscriptions(OpcUaClient client, List<Tag> tagList) throws Exception {     
        ManagedSubscription subscription = ManagedSubscription.create(client);        
        this.createSubscriptions(client, subscription, tagList);
       
    }       
}