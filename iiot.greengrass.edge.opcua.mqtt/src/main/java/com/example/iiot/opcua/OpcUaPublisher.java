package com.example.iiot.opcua;

import com.example.iiot.model.Tag;
import com.example.iiot.model.CommandMessage;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Publishes values received via MQTT to the corresponding OPC UA tags
 * using Eclipse Milo client.
 *
 * This class resolves mapped tag names to NodeIds and converts values to proper
 * OPC UA compatible data types before writing them to the PLC.
 */
public class OpcUaPublisher {

    private OpcUaClient client;
    private final Map<String, Tag> tagMap;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Constructs a new OpcUaPublisher with the provided OPC UA client and tag configuration.
     *
     * @param client   the connected OPC UA client
     * @param tagList  the list of tags containing mapping and metadata
     */
    public OpcUaPublisher(OpcUaClient client, List<Tag> tagList) {
        this.client = client;
        this.tagMap = tagList.stream().collect(Collectors.toMap(Tag::getMappedName, Function.identity()));
    }

    /**
     * Resolves the NodeId and data type from the tag mapping and writes the converted value
     * to the OPC UA server.
     *
     * @param cmd the command message containing mappedName and value
     */
    public void publishValueToOpcUa(CommandMessage cmd) {
        Tag tag = tagMap.get(cmd.mappedName);
        if (tag == null) {
            logger.error("Tag not found for mappedName: {}", cmd.mappedName);
            return;
        }

        String identifier = tag.getNodeId().toString();
        int namespaceIndex = tag.getNamespaceIndex();
        NodeId nodeId = new NodeId(namespaceIndex, identifier);

        logger.info("Writing to NodeId: {}", nodeId);

        String dataType = tag.getDataType() != null ? tag.getDataType().toLowerCase() : "string";
        Object convertedValue;
        try {
            convertedValue = convertToDataType(cmd.value, dataType);
        } catch (Exception e) {
            logger.error("Failed to convert value '{}' to type '{}'", cmd.value, dataType, e);
            return;
        }

        writeValues(nodeId, convertedValue, cmd.mappedName);
    }

    /**
     * Writes the value to the OPC UA server using Milo's writeValues method.
     *
     * @param nodeId     the OPC UA NodeId to write to
     * @param value      the value to write
     * @param mappedName the mapped tag name (used in logs)
     */
    private void writeValues(NodeId nodeId, Object value, String mappedName) {
        try {
            DataValue dataValue = new DataValue(new Variant(value), null, null);
            List<StatusCode> results = client.writeValues(List.of(nodeId), List.of(dataValue)).get();
            StatusCode status = results.get(0);

            if (status.isGood()) {
                logger.info("Milo-style write succeeded: {} = {}", mappedName, value);
            } else {
                logger.warn("Milo-style write failed for {}: {}", mappedName, status);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during write to {}: {}", mappedName, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.error("Execution error during write to {}: {}", mappedName, e.getMessage(), e);
        }
    }

    /**
     * Converts the value from the message to the OPC UA data type defined in the tag configuration.
     *
     * @param value    the raw value from the message
     * @param dataType the expected target data type ("boolean", "int", "double", "string")
     * @return the correctly typed Java object
     */
    private Object convertToDataType(Object value, String dataType) {
        if (value == null) return null;

        return switch (dataType) {
            case "boolean" -> {
                if (value instanceof Boolean b) yield b;
                yield Boolean.parseBoolean(value.toString());
            }
            case "int", "integer" -> {
                if (value instanceof Integer i) yield i;
                yield Integer.parseInt(value.toString());
            }
            case "double", "float" -> {
                if (value instanceof Double d) yield d;
                yield Double.parseDouble(value.toString());
            }
            case "string" -> value.toString();
            default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
        };
    }

    /**
     * Sets the active OPC UA client instance for this publisher.
     *
     * @param client the connected OPC UA client to use for writing values
     */
    public void setClient(OpcUaClient client) {
        this.client = client;
    }
}
