package coop.constellation.connectorservices.workflowexample.controller;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.events.RealtimeEventService;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubService;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestData;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestParams;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.dspco.ConnectorMessage;
import com.xtensifi.dspco.UserData;

import coop.constellation.connectorservices.workflowexample.helpers.RealtimeEvents;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static coop.constellation.connectorservices.workflowexample.helpers.Constants.*;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@CrossOrigin
@Controller
@RequiredArgsConstructor
@RequestMapping("/externalConnector/workflowMethodExample/1.0")
public class WorkflowExampleController extends ConnectorControllerBase {

    @Autowired
    // ConnectorHubService is required for workflow methods
    private ConnectorHubService connectorHubService;

    private ConnectorLogging clog = new ConnectorLogging();

    private final ObjectMapper mapper = new ObjectMapper();
    private final RealtimeEventService realtimeEventService;
    private final RealtimeEvents realtimeEvents;



    /**
     * This method is required in order for your controller to pass health checks.
     * If the server cannot call awsping and get the expected response your app will
     * not be active.
     *
     * @return the required ping-pong string
     */
    @GetMapping("/awsping")
    public String getAWSPing() {
        return "{ping: 'pong'}";
    }

    // region retrieveAccountList

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/retrieveAccountListRefresh", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveAccountListRefresh(@RequestBody final ConnectorMessage connectorMessage) {
        clog.info(connectorMessage, connectorMessage.toString());
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccountsRefresh"))
                .thenApply(this.processRetrieveAccountList()).thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveAccountList: " + exception.getMessage()));

        return responseEntity.build();

    }

    // This is an example of how to process the kivapublic response
    private Function<ConnectorState, ConnectorState> processRetrieveAccountList() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processRetrieveAccountList");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one response
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is how you capture the response
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response how ever you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);

            return connectorState;
        };
    }


    @CrossOrigin
    @PostMapping(path = "/retrieveAccountList", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveAccountList(@RequestBody final ConnectorMessage connectorMessage) {
        clog.info(connectorMessage, connectorMessage.toString());
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService

                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                .thenApply(this.retrieveFilterAcctParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.processRetrieveAccountList()).thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveAccountList: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> retrieveFilterAcctParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            // Gets a list of all paramters passed into your connector call
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            // Finding the value of the filters parameter passed from the tile
            String strFilter = allParams.getOrDefault("filters", "");

            if (!strFilter.equals("")) {
                connectorRequestParams.addNameValue("accountFilter", strFilter);
            }

            // Returns our list of parameters to pass into the kivapublic call
            return connectorRequestParams;
        };
    }
    // endregion

    // region retrieveTransactionList

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/retrieveTransactionList", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveTransactionList(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactions"))
                .thenApply(this.retrieveTransactionParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.processRetrieveTransactionList()).thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveTransactionList: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processRetrieveTransactionList() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processRetrieveTransactionList");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one response
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    /* Get the accountID and transaction filters passed in */
    private Function<ConnectorRequestParams, ConnectorRequestParams> retrieveTransactionParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            // Gets a list of all paramters passed into your connector call
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);
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
                    Map<String, String> filterMap = mapper.readValue(strFilter, new TypeReference<>(){});
                    filterMap.entrySet().stream()
                            .forEach((entry) -> connectorRequestParams.addNameValue(entry.getKey(), entry.getValue()));

                } catch (Exception e) {
                    clog.error(connectorMessage, "Could not get filters: " + e.getMessage());
                }

            }

            // Returns our list of parameters to pass into the kivapublic call
            return connectorRequestParams;
        };
    }
    // endregion

    // region retrieveUserBySocial

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/retrieveUserBySocial", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveUserBySocial(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getPartyBySSN"))
                .thenApply(this.retrieveUserBySocialParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.processRetrieveUserBySocial()).thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveUserBySocial: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processRetrieveUserBySocial() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processRetrieveUserBySocial");

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    /* Get the ssn passed in */
    private Function<ConnectorRequestParams, ConnectorRequestParams> retrieveUserBySocialParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);
            String SSN = allParams.getOrDefault("ssn", "");

            if (!SSN.equals("")) {
                connectorRequestParams.addNameValue("ssn", SSN);
            }

            return connectorRequestParams;
        };
    }
    // endregion

    // region retrieveUserById

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/retrieveUserById", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveUserById(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyById"))
                .thenApply(this.processRetrieveUserById()).thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveUserById: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processRetrieveUserById() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processRetrieveUserById");

            // Retrieve and log userId
            UserData userData = connectorState.getConnectorMessage().getExternalServicePayload().getUserData();
            String userId = userData.getUserId();
            clog.info(connectorState.getConnectorMessage(), userId);

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }
    // endregion

    // region retrieveTransactionCategories

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/retrieveTransactionCategories", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveTransactionCategories(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactionCategories"))
                .thenApply(this.processRetrieveTransactionCategories())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveTransactionCategories: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processRetrieveTransactionCategories() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processRetrieveTransactionCategories");

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }
    // endregion

    // region editTransaction

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/editTransaction", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> editTransaction(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "updateTransaction"))
                .thenApply(this.getEditTransactionParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse()).thenApply(this.processEditTransaction())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running editTransaction: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processEditTransaction() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processEditTransaction");

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getEditTransactionParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountId", "transactionId", "endUserTransCategory", "endUserTransNote",
                    "endUserTransDescription");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }
    // endregion


    // region startTransfer

    // Workflow methods return a ResponseEntity
    @CrossOrigin
    @PostMapping(path = "/startTransfer", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> startTransfer(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "createInternalTransfer"))
                .thenApply(this.getStartTransferParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.processStartTransfer())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running startTransfer: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processStartTransfer() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processStartTransfer");

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
                clog.info(connectorMessage, name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();
                Map<String, String> params = getAllParams(connectorMessage);


                // check for a successful transfer
                // TODO

                // if successful, send an event

                List<String> affectedItems = getFromAndToAccount(connectorMessage);
                try {
                    realtimeEvents.send(CDP_SOURCE, PLATFORM_ACCOUNT_TRANSACTION_ADDED, affectedItems, connectorMessage, clog, realtimeEventService);
                    clog.info(connectorMessage, "realtime event for " + PLATFORM_ACCOUNT_TRANSACTION_ADDED + " sent. ");
                }
                catch(Exception e){
                    clog.error(connectorMessage, "error sending realtime event ");
                    e.printStackTrace();

                }

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorMessage, resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getStartTransferParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountFrom", "accountTo", "transferAmount", "transferMemo",
                    "occurrenceFromAccountType", "occurrenceToAccountType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }
    // endregion

    // region p2pTransfer

    @CrossOrigin
    @PostMapping(path = "/p2pTransfer", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> p2pTransfer(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "personToPersonTransfer"))
                .thenApply(this.getP2pTransferParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.processP2pTransfer())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running p2pTransfer: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processP2pTransfer() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processP2pTransfer");

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {
                ConnectorMessage connectorMessage = connectorState.getConnectorMessage();

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorMessage, name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // check for a successful transfer
                // TODO

                // if successful, send an event
                List<String> affectedItems = getFromAndToAccount(connectorMessage);
                try {
                    realtimeEvents.send(CDP_SOURCE, PLATFORM_ACCOUNT_TRANSACTION_ADDED, affectedItems,connectorMessage, clog, realtimeEventService);
                    clog.info(connectorMessage, "realtime event for " + PLATFORM_ACCOUNT_TRANSACTION_ADDED + " sent. ");
                }
                catch(Exception e){
                    clog.error(connectorMessage, "error sending realtime event " + e.getMessage());

                }

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorMessage, resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getP2pTransferParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountFrom", "accountTo", "transferAmount", "transferMemo",
                    "occurrenceFromAccountType", "occurrenceToAccountType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }

    // endregion

    // region stopPayment

    @CrossOrigin
    @PostMapping(path = "/stopPayment", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> stopPayment(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "createStopPayment"))
                .thenApply(this.getStopPaymentParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse()).thenApply(this.processStopPayment())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running stopPayment: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorState, ConnectorState> processStopPayment() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processStopPayment");

            // Gather the list of responses, when only making 1 kiva call ther should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getStopPaymentParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountId", "holdDescription", "holdAmount", "checkNumber", "feeAccountId", "feeAmount", "feeAccountType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }

    // endregion

    // region multiCall
    // This call demonstrates an example of making two calls with logic in between
    @CrossOrigin
    @PostMapping(path = "/multiCall", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> multiCall(@RequestBody final ConnectorMessage connectorMessage) {
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                // .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApplyAsync(connectorState -> connectorHubService.prepareNextConnector(
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactions"), connectorState))
                .thenApply(this.getMultiCallParams()).thenApplyAsync(connectorHubService.callConnectorUsingState())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse()).thenApply(this.processMultiCall())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running multiCall: " + exception.getMessage()));

        return responseEntity.build();
    }

    private Function<ConnectorState, ConnectorState> processMultiCall() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processMultiCall");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorState, ConnectorState> getMultiCallParams() {
        return connectorState -> {
            ConnectorMessage connectorMessage = connectorState.getConnectorMessage();
            List<ConnectorResponse> responseList = connectorState.getConnectorResponseList().getResponses();
            // This example is only expecting 1 response
            if (responseList.size() == 1) {
                clog.info(connectorState.getConnectorMessage(), "Start Parsing");

                String response = responseList.get(0).getResponse();
                clog.info(connectorState.getConnectorMessage(), response);

                ObjectMapper jsonMap = new ObjectMapper();
                String accountID = "";
                try {
                    JsonNode component = jsonMap.readTree(response);
                    JsonNode depositArray = component.at("/accountContainer/depositMessage/depositList/deposit");
                    clog.info(connectorState.getConnectorMessage(), depositArray.toString());
                    accountID = depositArray.get(1).get("accountId").asText();
                } catch (Exception e) {
                    clog.error(connectorMessage, "failed to get accountid");
                }
                // add accountId as param for the getTransactions call
                connectorState.getConnectorRequestParams().addNameValue("accountId", accountID);
            }
            return connectorState;
        };
    }
    // endregion

    // region Validate Member Account Info

    @CrossOrigin
    @PostMapping(path = "/validateMemberAccountInfo", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> validateMemberAccountInfo(@RequestBody final ConnectorMessage connectorMessage)
            throws JsonProcessingException {
        clog.info(connectorMessage, mapper.writeValueAsString(connectorMessage));
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "validateMemberAccountInfo"))
                .thenApply(this.getValidateMemberAccountInfoParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse()).thenApply(this.processValidateMember())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveAccountList: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getValidateMemberAccountInfoParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            List<String> paramList = List.of("memberId", "accountId", "firstThreeOfLastName");

            for (String paramName : paramList) {
                String paramValue = allParams.getOrDefault(paramName, "");
                connectorRequestParams.addNameValue(paramName, paramValue);
            }

            try {
                clog.info(connectorMessage, "all params GC: " + mapper.writeValueAsString(allParams));
                clog.info(connectorMessage, "connector request params for validate member account info: "
                        + mapper.writeValueAsString(connectorRequestParams));
                clog.info(connectorMessage,
                        "this is the connector message: " + mapper.writeValueAsString(connectorMessage));
            } catch (JsonProcessingException e) {
                // do nothing
            }

            return connectorRequestParams;
        };
    }

    private Function<ConnectorState, ConnectorState> processValidateMember() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processValidateMember");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one response
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    // End region Validate Member Account Info

    @CrossOrigin
    @PostMapping(path = "/sendRealtimeEvent", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ConnectorMessage sendRealtimeEvent(@RequestBody final String connectorJson) throws IOException {
        ConnectorMessage connectorMessage = mapper.readValue(connectorJson, ConnectorMessage.class);
        final Map<String, String> parms = getAllParams(connectorMessage);
        String eventName = parms.get(EVENT_NAME);
        String accounts = parms.get(ACCOUNTS);
        boolean success = true;
        String message = "";
        if(eventName!=null && !eventName.isEmpty() && accounts!=null && !accounts.isEmpty()) {
            List<String> affectedItems = mapper.readValue(accounts, new TypeReference<>() {});
            String accountIdList = "";
            for(String id : affectedItems){
                accountIdList = accountIdList + id + ",";
            }
            try {
                realtimeEvents.send(CDP_SOURCE, eventName, affectedItems, connectorMessage, clog, realtimeEventService);
                message = String.format("Realtime event sent successfully for %s, affected items %s", eventName, accountIdList);
                clog.info(connectorMessage, message);
            }
            catch(Exception e){
                message = String.format("Realtime event was unsuccessful for %s, affected items %s ", eventName, accountIdList) + e.getMessage();
                clog.error(connectorMessage, message);
            }
        }
        else{
            message = String.format("Something is missing when sending event for event name: %s, affected items: %s ", eventName);
            clog.error(connectorMessage, message);
        }
        String response = "{\"response\":{\"success\":"+success+", \"message\": \""+message+"\"}}";
        clog.info(connectorMessage, "this is the response " + response);
        connectorMessage.setResponse(response);
        return connectorMessage;
    }

    private ConnectorRequestParams createConnectorRequestParams(ConnectorRequestParams connectorRequestParams,
            Map<String, String> allParams, List<String> paramNames) {
        for (String name : paramNames) {
            String param = allParams.getOrDefault(name, "");
            if (!param.isEmpty()) {
                connectorRequestParams.addNameValue(name, param);
            }
        }
        return connectorRequestParams;
    }

    private List<String> getFromAndToAccount(ConnectorMessage connectorMessage){
        Map<String,String> parms = getAllParams(connectorMessage);
        String fromAccount = parms.getOrDefault(FROM_ACCOUNT, "");
        String toAccount =parms.getOrDefault(TO_ACCOUNT, "");
        return List.of(fromAccount, toAccount);
    }


}