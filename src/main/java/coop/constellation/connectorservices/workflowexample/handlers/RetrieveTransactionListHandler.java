package coop.constellation.connectorservices.workflowexample.handlers;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestParams;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;
import coop.constellation.connectorservices.workflowexample.controller.BaseParamsSupplier;
import coop.constellation.connectorservices.workflowexample.controller.ConnectorControllerBase;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RetrieveTransactionListHandler extends HandlerBase implements WorkflowHandlerLogic {

    private final BaseParamsSupplier baseParamsSupplier;
    private final ConnectorLogging logger;
    private final ObjectMapper mapper;

    public JdbcTemplate createJdbc(ConnectorMessage connectorMessage) {
        Map<String, String> parms = ConnectorControllerBase.getAllParams(connectorMessage, baseParamsSupplier.get());

        return createJdbcTemplate(parms, logger, connectorMessage);
    }

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

    /* Get the accountID and transaction filters passed in */
    public Function<ConnectorRequestParams, ConnectorRequestParams> retrieveTransactionParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            // Gets a list of all paramters passed into your connector call
            final Map<String, String> allParams = ConnectorControllerBase.getAllParams(connectorMessage,
                    baseParamsSupplier.get());

            logger.info(connectorMessage, "all params GC: " + allParams);
            // Finds the value of the accountId parameter passed in from the tile, if not
            // found returns an empty string
            String accountID = allParams.getOrDefault("accountId", "");

            if (!accountID.isEmpty() || accountID != null) {
                // Add the accountId parameter to our list of parameters to be returned
                connectorRequestParams.addNameValue("accountId", accountID);
            }

            // Finding the value of the filters parameter passed from the tile
            String strFilter = allParams.getOrDefault("filters", "");

            if (!strFilter.equals("")) {
                try {
                    Map<String, String> filterMap = mapper.readValue(strFilter, new TypeReference<>() {
                    });
                    filterMap.entrySet().stream()
                            .forEach((entry) -> connectorRequestParams.addNameValue(entry.getKey(), entry.getValue()));

                } catch (Exception e) {
                    logger.error(connectorMessage, "Could not get filters: " + e.getMessage());
                }

            }

            // Returns our list of parameters to pass into the kivapublic call
            return connectorRequestParams;
        };
    }

    @Override
    public String generateResponse(Map<String, String> parms, String userId, ConnectorMessage connectorMessage)
            throws IOException, ParseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'generateResponse'");
    }
}