package coop.constellation.connectorservices.workflowexample.handlers;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.stereotype.Service;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RetrieveTransactionListHandler extends HandlerBase implements WorkflowHandlerLogic {

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

    public Function<ConnectorState, ConnectorState> processRetrieveTransactionList() {
        return connectorState -> {
            logger.info(connectorState.getConnectorMessage(), "processRetrieveTransactionList");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one response
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                logger.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                logger.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
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