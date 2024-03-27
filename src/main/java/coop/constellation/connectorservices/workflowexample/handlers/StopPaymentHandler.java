package coop.constellation.connectorservices.workflowexample.handlers;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.stereotype.Service;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestParams;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;
import com.xtensifi.connectorservices.common.events.RealtimeEventService;
import coop.constellation.connectorservices.workflowexample.helpers.RealtimeEvents;

import coop.constellation.connectorservices.workflowexample.controller.BaseParamsSupplier;
import coop.constellation.connectorservices.workflowexample.controller.ConnectorControllerBase;
import static coop.constellation.connectorservices.workflowexample.helpers.Constants.*;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class StopPaymentHandler extends HandlerBase implements WorkflowHandlerLogic {

    private final BaseParamsSupplier baseParamsSupplier;
    private final ConnectorLogging logger;
    private final RealtimeEventService realtimeEventService;
    private final RealtimeEvents realtimeEvents;

    @Override
    public String generateResponse(final Map<String, String> parms, ConnectorState connectorState)
            throws IOException, ParseException {
        // Gather the list of responses, when only making 1 kiva call there should only
        // be one response
        List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

        // This is a placeholder incase no responses are returned, this should be
        // updated with a default message when responses are found
        String resp = "{\"response\": 1}";
        for (ConnectorResponse connectorResponse : connectorResponseList) {
            ConnectorMessage connectorMessage = connectorState.getConnectorMessage();

            // This is how you retrieve the name of the connector
            String name = connectorResponse.getConnectorRequestData().getConnectorName();
            logger.info(connectorMessage, name);

            // This is how you capture the response
            String data = connectorResponse.getResponse();

            // check for a successful transfer
            // TODO

            // if successful, send an event

            List<String> affectedItems = getFromAndToAccount(connectorMessage);
            try {
                realtimeEvents.send(CDP_SOURCE, PLATFORM_ACCOUNT_TRANSACTION_ADDED, affectedItems, connectorMessage,
                        logger, realtimeEventService);
                logger.info(connectorMessage, "realtime event for " + PLATFORM_ACCOUNT_TRANSACTION_ADDED + " sent. ");
            } catch (Exception e) {
                logger.error(connectorMessage, "error sending realtime event ");
                e.printStackTrace();

            }

            // Parse the response however you see fit
            resp = "{\"response\": " + data + "}";
            logger.info(connectorMessage, resp);
        }

        // This is required, and is how you set the response for a workflow method
        connectorState.setResponse(resp);
        return resp;
    }

    public Function<ConnectorRequestParams, ConnectorRequestParams> getStopPaymentParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = ConnectorControllerBase.getAllParams(connectorMessage,
                    baseParamsSupplier.get());

            logger.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountId", "holdDescription", "holdAmount", "checkNumber",
                    "feeAccountId", "feeAmount", "feeAccountType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }

    protected List<String> getFromAndToAccount(ConnectorMessage connectorMessage) {
        Map<String, String> parms = ConnectorControllerBase.getAllParams(connectorMessage, baseParamsSupplier.get());
        String fromAccount = parms.getOrDefault(FROM_ACCOUNT, "");
        String toAccount = parms.getOrDefault(TO_ACCOUNT, "");
        return List.of(fromAccount, toAccount);
    }

    @Override
    public String generateResponse(Map<String, String> parms, String userId, ConnectorMessage connectorMessage)
            throws IOException, ParseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'generateResponse'");
    }
}