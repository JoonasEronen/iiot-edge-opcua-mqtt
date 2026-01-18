package com.example.iiot.config;

import com.example.iiot.model.Tag;

import java.io.IOException;
import java.io.File;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;



/**
 * Utility class responsible for reading tag configuration from a JSON file.
 * The tag list defines which OPC UA variables are monitored and how they are mapped.
 */
public class TagReader {

	private final static Logger logger = LoggerFactory.getLogger(TagReader.class);
	
	 /**
     * Wrapper class for reading a list of tags from a JSON structure.
     * The JSON must contain a root element like: { "tagList": [ {...}, {...} ] }
     */
    public static class TagsWrapper {
        private List<Tag> tagList;
      
        /**
         * Gets the list of tags defined in the configuration.
         *
         * @return list of {@link Tag} objects
         */
        public List<Tag> getTagList() {
            return tagList;
        }

        /**
         * Sets the list of tags from the configuration.
         *
         * @param tagList list of {@link Tag} objects to assign
         */
        public void setTagList(List<Tag> tagList) {
            this.tagList = tagList;
        }
    }

    
    /**
     * Reads the tag list from the given JSON file path.
     * Automatically ignores unknown properties and logs any issues.
     *
     * @param filePath path to the tag configuration JSON file
     * @return list of {@link Tag} objects, or empty list on error
     */
    public static List<Tag> getTagList(String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // Ignore unknown fields
        File file = new File(filePath);

        if (!file.exists()) {
            logger.error("Tag configuration file not found: {}", filePath);
            return Collections.emptyList();
        }

        try {
            TagsWrapper wrapper = mapper.readValue(file, TagsWrapper.class);
            logger.info("Successfully loaded {} tags from {}", wrapper.getTagList().size(), filePath);
            return wrapper.getTagList();
        } catch (IOException e) {
            logger.error("Error reading tags from {}: {}", filePath, e.getMessage(), e);
            return Collections.emptyList();
        }
    }  
}