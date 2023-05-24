package coop.constellation.connectorservices.workflowexample.controller;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.cufx.CustomData;
import com.xtensifi.cufx.ValuePair;
import com.xtensifi.dspco.ConnectorMessage;
import com.xtensifi.dspco.ConnectorParametersResponse;
import com.xtensifi.dspco.ExternalServicePayload;
import org.springframework.stereotype.Component;
import lombok.Getter;

@Component
public class ConnectorControllerBase {

    @Getter
    private final ObjectMapper objectMapper;

    private static ConnectorLogging clog = new ConnectorLogging();

    public ConnectorControllerBase() {
        objectMapper = new ObjectMapper();
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    /**
     * Get all the value pairs out of the connector message. NOTE: if a name occurs
     * more than once, only the first occurrance is returned.
     * 
     * @param connectorMessage the request connector message
     * @return a Map of the value pairs
     */
    static public Map<String, String> getAllParams(final ConnectorMessage connectorMessage) {
        final Map<String, String> allParams = new HashMap<>();

        final ExternalServicePayload externalServicePayload = connectorMessage.getExternalServicePayload();
        final ConnectorParametersResponse connectorParametersResponse = connectorMessage
                .getConnectorParametersResponse();

        clog.info(connectorMessage, "BEGIN getAllParams");

        if (externalServicePayload != null) {

            final CustomData methodParams = externalServicePayload.getPayload();
            if (methodParams != null)
                for (ValuePair valuePair : methodParams.getValuePair()) {
                    allParams.putIfAbsent(valuePair.getName(), valuePair.getValue());
                }
        }

        if (connectorParametersResponse != null) {

            final CustomData otherParams = connectorParametersResponse.getParameters();
            if (otherParams != null) {
                for (ValuePair valuePair : otherParams.getValuePair()) {
                    allParams.putIfAbsent(valuePair.getName(), valuePair.getValue());
                }
            }

            final CustomData methodParams = connectorParametersResponse.getMethod().getParameters();
            if (methodParams != null) {
                for (ValuePair valuePair : methodParams.getValuePair()) {
                    allParams.putIfAbsent(valuePair.getName(), valuePair.getValue());
                }
            }

        }

        return allParams;
    }

}
