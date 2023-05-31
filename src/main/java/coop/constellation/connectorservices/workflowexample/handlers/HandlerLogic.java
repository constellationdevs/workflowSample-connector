package coop.constellation.connectorservices.workflowexample.handlers;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import com.xtensifi.dspco.ConnectorMessage;

/**
 * Interface for the custom logic to generate a response
 */

 @FunctionalInterface
 public interface HandlerLogic {
     String generateResponse(final Map<String, String> parms, final String userId, final ConnectorMessage connectorMessage) throws IOException, ParseException;
 }