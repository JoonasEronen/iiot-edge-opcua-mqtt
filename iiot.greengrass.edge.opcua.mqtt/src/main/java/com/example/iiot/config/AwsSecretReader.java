package com.example.iiot.config;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/**
 * AwsSecretReader provides utility to securely retrieve secrets from AWS Secrets Manager.
 * It returns secrets as a Map where values are stored as {@code char[]} to reduce memory exposure.
 */
public class AwsSecretReader {

	/**
     * Fetches a secret from AWS Secrets Manager and parses it as a key-value map.
     * Each value is stored as {@code char[]} to improve security by avoiding String reuse in memory.
     *
     * @param secretName the name or ARN of the secret in Secrets Manager
     * @param region the AWS region where the secret is stored
     * @return a {@code Map<String, char[]>} of the parsed secret values
     * @throws RuntimeException if parsing fails
     */
    public static Map<String, char[]> getSecret(String secretName, software.amazon.awssdk.regions.Region region) {    	
    	disableAwsAndApacheLogging(); // Disable HTTP-level debug logging before any sensitive data is fetched
    	
    	ObjectMapper mapper = new ObjectMapper();
        Map<String, char[]> secureMap = new HashMap<>();
    	
    	try (SecretsManagerClient client = SecretsManagerClient.builder()
    		    .region(region)
    		    .credentialsProvider(DefaultCredentialsProvider.create())
    		    .build()) {
    		
    		GetSecretValueResponse response = client.getSecretValue(
    				GetSecretValueRequest.builder() 	
    				.secretId(secretName)
    				.build()
    	);  		
    		String secretString = response.secretString();
    		if (secretString == null || secretString.isBlank()) {
    		    throw new IllegalStateException("SecretString is empty/null (binary secrets not supported by this reader).");
    		}
    		
            Map<String, String> rawMap = mapper.readValue(secretString, new TypeReference<Map<String, String>>() {});
            
            for (Map.Entry<String, String> entry : rawMap.entrySet()) {
                String value = entry.getValue();
                if (value != null) {
                    secureMap.put(entry.getKey(), value.toCharArray());                    
                    entry.setValue(null); // Nullify the original String ASAP
                }
            }

            // Clean up memory references
            secretString = null;
            rawMap.clear();

           
        } catch (Exception e) {        	
            throw new RuntimeException("Unable to parse secret JSON", e);
        }

        return secureMap;
    }
    
    
    /**
     * Disables Apache HTTP wire-level logging to prevent accidental exposure of
     * sensitive AWS Secrets Manager responses such as SecretString (which may contain passwords).
     * <p>
     * This should be called before any SecretsManagerClient operations are performed.
     * Affected loggers:
     * <ul>
     *     <li>org.apache.http.wire</li>
     *     <li>org.apache.http.headers</li>
     *     <li>org.apache.http</li>
     * </ul>
     */
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

}
