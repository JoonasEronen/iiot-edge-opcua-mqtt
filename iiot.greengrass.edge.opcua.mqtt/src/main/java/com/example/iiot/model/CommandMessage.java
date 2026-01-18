package com.example.iiot.model;

/**
 * Represents a command message received from MQTT.
 * <p>
 * This message contains a logical name (mappedName) that identifies the target OPC UA tag,
 * and a value to be written (e.g. boolean, number, or string).
 * <p>
 * Example:
 * <pre>
 * {
 *   "mappedName": "PLC_Start",
 *   "value": true
 * }
 * </pre>
 */
public class CommandMessage {

    /**
     * The mapped name of the OPC UA tag (e.g. "PLC_Start").
     * This should match the `mappedName` field in your tag configuration.
     */
    public String mappedName;

    /**
     * The value to be written to the OPC UA tag.
     * Can be a boolean (true/false), number (e.g., 123.45), or string.
     */
    public Object value;
}
