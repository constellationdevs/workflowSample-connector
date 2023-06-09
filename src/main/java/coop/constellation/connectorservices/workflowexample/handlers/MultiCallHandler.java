package coop.constellation.connectorservices.workflowexample.handlers;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MultiCallHandler extends HandlerBase implements WorkflowHandlerLogic {

    private final ConnectorLogging logger;

    @Override
    public String generateResponse(final Map<String, String> parms, ConnectorState connectorState)
            throws IOException, ParseException {
        List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

        // This is how you capture the response
        String resp = "{\"response\": 1}";
        for (ConnectorResponse connectorResponse : connectorResponseList) {

            // This is how you retrieve the name of the connector
            String name = connectorResponse.getConnectorRequestData().getConnectorName();
            logger.info(connectorState.getConnectorMessage(), name);

            // This is how you capture the response
            String data = connectorResponse.getResponse();

            // Parse the response how ever you see fit
            resp = "{\"response\": " + data + "}";
            logger.info(connectorState.getConnectorMessage(), resp);
        }

        // This is required, and is how you set the response for a workflow method
        connectorState.setResponse(resp);
        return resp;
    }

    public Function<ConnectorState, ConnectorState> getMultiCallParams() {
        return connectorState -> {
            ConnectorMessage connectorMessage = connectorState.getConnectorMessage();
            List<ConnectorResponse> responseList = connectorState.getConnectorResponseList().getResponses();
            // This example is only expecting 1 response
            if (responseList.size() == 1) {
                logger.info(connectorState.getConnectorMessage(), "Start Parsing");

                String response = responseList.get(0).getResponse();
                logger.info(connectorState.getConnectorMessage(), response);

                ObjectMapper jsonMap = new ObjectMapper();
                String accountID = "";
                try {
                    JsonNode component = jsonMap.readTree(response);
                    JsonNode depositArray = component.at("/accountContainer/depositMessage/depositList/deposit");
                    logger.info(connectorState.getConnectorMessage(), depositArray.toString());
                    accountID = depositArray.get(1).get("accountId").asText();
                } catch (Exception e) {
                    logger.error(connectorMessage, "failed to get accountid");
                }
                // add accountId as param for the getTransactions call
                connectorState.getConnectorRequestParams().addNameValue("accountId", accountID);
            }
            return connectorState;
        };
    }

    @Override
    public String generateResponse(Map<String, String> parms, String userId, ConnectorMessage connectorMessage)
            throws IOException, ParseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'generateResponse'");
    }
}