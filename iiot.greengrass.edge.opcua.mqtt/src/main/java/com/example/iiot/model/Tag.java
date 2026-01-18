package com.example.iiot.model;

import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * Represents a monitored OPC UA tag with its configuration and current state.
 * <p>
 * Contains both metadata (from config) and runtime values (from subscription).
 */
public class Tag {

    // --- Configuration parameters loaded from AWS Greengrass or JSON ---

    /** Raw NodeId value as string (can be integer or string format) */
    private String nodeID;

    /** Logical mapped name for UNS or MQTT topic (e.g., "PLC_Start") */
    private String mappedName;

    /** OPC UA namespace index (used with NodeId) */
    private int namespaceIndex;
    
    /**
     * Expected data type of the tag value, e.g., "Double", "Boolean", "String".
     * Used to guide how the tag value should be interpreted or processed.
     */
    private String dataType;

    /** Enables threshold-based change detection */
    private boolean useThreshold;

    /** Threshold value (absolute or percentage) */
    private Double threshold;

    /** Type of threshold: "absolute", "percentage", etc. */
    private String thresholdType;

    /** Enables time-based publishing even if value hasn’t changed */
    private boolean useThresholdSeconds;

    /** Time threshold (in seconds) for forced publishing */
    private Double thresholdSeconds;


    // --- Runtime values updated from OPC UA subscription ---

    /** Latest value received from OPC UA server */
    private double value;

    /** Previous value (used for threshold comparisons) */
    private double previousValue;
    
    /** Latest string value received from OPC UA server or AWS */
    private String stringValue;

    /** OPC UA timestamp (source timestamp) */
    private DateTime timestamp;

    /** Unix timestamp (epoch seconds) */
    private Long timestampUnix;

    /** Previous value timestamp (epoch seconds) */
    private Long previousTimestampUnix;

    /** Previous full OPC UA timestamp */
    private DateTime previousTimestamp;


    /** Logger for internal error reporting */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    // --- GETTERS ---

    /**
     * Converts and returns NodeId as Integer or String depending on format.
     * @return parsed NodeId object
     */
    public Object getNodeId() {
        if (nodeID == null) return null;
        try {
            return Integer.parseInt(this.nodeID);
        } catch (NumberFormatException e) {
            return this.nodeID;
        }
    }

    public String getMappedName() { return mappedName; }
    public int getNamespaceIndex() { return namespaceIndex; }
    public String getDataType() { return dataType; }
    public boolean getUseThreshold() { return useThreshold; }
    public Double getThreshold() { return threshold; }
    public String getThresholdType() { return thresholdType; }
    public boolean getUseThresholdSeconds() { return useThresholdSeconds; }
    public Double getThresholdSeconds() { return thresholdSeconds; }
    public double getValue() { return value; }
    public double getPreviousValue() { return previousValue; }
    public String getStringValue() { return stringValue; }
    public DateTime getTimestamp() { return timestamp; }
    public Long getTimestampUnix() { return timestampUnix; }
    public Long getPreviousTimestampUnix() { return previousTimestampUnix; }
    public DateTime getPreviousTimestamp() { return previousTimestamp; }

    // --- SETTERS (many mapped via JSON) ---

    @JsonSetter
    public void setNodeID(String name) { this.nodeID = name; }

    @JsonSetter
    public void setMappedName(String mappedName) { this.mappedName = mappedName; }

    @JsonSetter
    public void setNamespaceIndex(Object value) { this.namespaceIndex = convertToInteger(value); }

    @JsonSetter
    public void setDataType(String dataType) { this.dataType = dataType; }
    
    @JsonSetter
    public void setUseThreshold(boolean useThreshold) { this.useThreshold = useThreshold; }

    @JsonSetter
    public void setThreshold(Double threshold) { this.threshold = threshold; }

    @JsonSetter
    public void setThresholdType(String thresholdType) { this.thresholdType = thresholdType; }

    @JsonSetter
    public void setUseThresholdSeconds(boolean useThresholdSeconds) { this.useThresholdSeconds = useThresholdSeconds; }

    @JsonSetter
    public void setThresholdSeconds(Double thresholdSeconds) { this.thresholdSeconds = thresholdSeconds; }

    public void setValue(double value) { this.value = value; }

    public void setPreviousValue(double previousValue) { this.previousValue = previousValue; }
    
    public void setStringValue(String stringValue) { this.stringValue = stringValue; }

    public void setTimestamp(DateTime timestamp) { this.timestamp = timestamp; }

    public void setTimestampUnix(Long timestampUnix) { this.timestampUnix = timestampUnix; }

    public void setPreviousTimestampUnix(Long previousTimestampUnix) { this.previousTimestampUnix = previousTimestampUnix; }

    public void setPreviousTimestamp(DateTime previousTimestamp) { this.previousTimestamp = previousTimestamp; }

    // --- Internal utility method ---

    /**
     * Converts JSON value to integer (e.g., namespace index).
     * @param value Number or String type
     * @return parsed int or 0 on failure
     */
    private int convertToInteger(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                logger.error("Could not parse namespaceIndex: '{}'", value);
            }
        }
        return 0;
    }
}
