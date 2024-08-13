package coop.constellation.connectorservices.workflowexample.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.events.RealtimeEventService;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubService;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestData;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestParams;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;

import coop.constellation.connectorservices.workflowexample.handlers.EditTransactionHandler;
import coop.constellation.connectorservices.workflowexample.handlers.MultiCallHandler;
import coop.constellation.connectorservices.workflowexample.handlers.P2pTransferHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveAccountListHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveAccountListRefreshHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveTransactionCategoriesHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveTransactionListHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveUserByIdHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveUserBySocialHandler;
import coop.constellation.connectorservices.workflowexample.handlers.StartTransferHandler;
import coop.constellation.connectorservices.workflowexample.handlers.StopPaymentHandler;
import coop.constellation.connectorservices.workflowexample.handlers.ValidateMemberAccountInfoHandler;
import coop.constellation.connectorservices.workflowexample.helpers.RealtimeEvents;
import lombok.RequiredArgsConstructor;

import org.apache.commons.lang3.exception.ExceptionUtils;
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

    private final ConnectorLogging clog;
    private final ObjectMapper mapper;
    private final RealtimeEventService realtimeEventService;
    private final RealtimeEvents realtimeEvents;

    private final RetrieveAccountListRefreshHandler retrieveAccountListRefreshHandler;
    private final RetrieveAccountListHandler retrieveAccountListHandler;
    private final RetrieveTransactionListHandler retrieveTransactionListHandler;
    private final RetrieveUserBySocialHandler retrieveUserBySocialHandler;
    private final RetrieveUserByIdHandler retrieveUserByIdHandler;
    private final RetrieveTransactionCategoriesHandler retrieveTransactionCategoriesHandler;
    private final EditTransactionHandler editTransactionsHandler;
    private final StartTransferHandler startTransferHandler;
    private final P2pTransferHandler p2pTransferHandler;
    private final StopPaymentHandler stopPaymentHandler;
    private final ValidateMemberAccountInfoHandler validateMemberAccountInfoHandler;
    private final MultiCallHandler multiCallHandler;

    /**
     * This method is required in order for your controller to pass health checks.
     * If the server cannot call awsping and get the expected response your app will
     * not be active.
     *
     * @return the required ping-pong string
     */
    @CrossOrigin
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
                .thenApply(this.handleResponseEntity(retrieveAccountListRefreshHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveAccountList: " + exception.getMessage()));

        return responseEntity.build();

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
                .thenApply(this.handleResponseEntity(retrieveAccountListHandler))
                .thenApplyAsync(connectorHubService.completeAsync())

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
                .thenApply(this.handleResponseEntity(retrieveTransactionListHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveTransactionList: " + exception.getMessage()));

        return responseEntity.build();

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
                    Map<String, String> filterMap = mapper.readValue(strFilter, new TypeReference<>() {
                    });
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
                .thenApply(this.handleResponseEntity(retrieveUserBySocialHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveUserBySocial: " + exception.getMessage()));

        return responseEntity.build();

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

    // // endregion

    // // region retrieveUserById

    // // Workflow methods return a ResponseEntity

    @CrossOrigin
    @PostMapping(path = "/retrieveUserById", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveUserById(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyById"))
                .thenApply(this.handleResponseEntity(retrieveUserByIdHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveUserById: " + exception.getMessage()));

        return responseEntity.build();

    }
    // // endregion

    // // region retrieveTransactionCategories

    // // Workflow methods return a ResponseEntity

    @CrossOrigin
    @PostMapping(path = "/retrieveTransactionCategories", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> retrieveTransactionCategories(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactionCategories"))
                .thenApply(this.handleResponseEntity(retrieveTransactionCategoriesHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running retrieveTransactionCategories: " + exception.getMessage()));

        return responseEntity.build();

    }
    // // endregion

    // // region editTransaction

    // // Workflow methods return a ResponseEntity

    @CrossOrigin
    @PostMapping(path = "/editTransaction", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> editTransaction(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "updateTransaction"))
                .thenApply(this.getEditTransactionParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.handleResponseEntity(editTransactionsHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running editTransaction: " + exception.getMessage()));

        return responseEntity.build();

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
    // // endregion

    // // region startTransfer

    // // Workflow methods return a ResponseEntity

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
                .thenApply(this.handleResponseEntity(startTransferHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running startTransfer: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getStartTransferParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountFrom", "accountTo", "transferAmount", "transferMemo",
                    "occurrenceFromAccountType", "occurrenceToAccountType", "paymentType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }
    // // endregion

    // // region p2pTransfer
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
                .thenApply(this.handleResponseEntity(p2pTransferHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running p2pTransfer: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getP2pTransferParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountFrom", "accountTo", "transferAmount", "transferMemo",
                    "occurrenceFromAccountType", "occurrenceToAccountType", "paymentType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }
    // // endregion

    // // region stopPayment
    @CrossOrigin
    @PostMapping(path = "/stopPayment", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> stopPayment(@RequestBody final ConnectorMessage connectorMessage) {

        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .initAsyncConnectorRequest(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "createStopPayment"))
                .thenApply(this.getStopPaymentParams(connectorMessage))
                .thenApply(connectorHubService.callConnectorAsync())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.handleResponseEntity(stopPaymentHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running stopPayment: " + exception.getMessage()));

        return responseEntity.build();

    }

    private Function<ConnectorRequestParams, ConnectorRequestParams> getStopPaymentParams(
            ConnectorMessage connectorMessage) {

        return connectorRequestParams -> {
            final Map<String, String> allParams = getAllParams(connectorMessage);

            clog.info(connectorMessage, "all params GC: " + allParams);

            List<String> paramNames = List.of("accountId", "holdDescription", "holdAmount", "checkNumber",
                    "startCheckNumber", "endCheckNumber", "feeAccountId", "feeAmount", "feeAccountType");

            return createConnectorRequestParams(connectorRequestParams, allParams, paramNames);
        };
    }
    // // endregion

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
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.handleResponseEntity(validateMemberAccountInfoHandler))
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

    // // endregion

    @CrossOrigin
    @PostMapping(path = "/multiCall", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> multiCall(@RequestBody final ConnectorMessage connectorMessage) {
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                .thenApplyAsync(connectorState -> connectorHubService.prepareNextConnector(
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactions"), connectorState))
                .thenApply(this.getMultiCallParams())
                .thenApplyAsync(connectorHubService.callConnectorUsingState())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApply(this.handleResponseEntity(multiCallHandler))
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running multiCall: " + exception.getMessage()));

        return responseEntity.build();
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

    /**
     * Returns a new completable future that waits for a list of connector state
     * futures to complete and then creates
     * a new connector state future that contains all of their responses.
     */
    public CompletableFuture<ConnectorState> invokeCompletableFutures(
            List<CompletableFuture<ConnectorState>> completableFutures, ConnectorMessage connectorMessage) {
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))

                // Since we can't work with CompletableFuture<Void> we need to map the response
                // data to a list of ConnectorStates
                .thenApplyAsync((future) -> completableFutures.stream().map(completableFuture -> {
                    ConnectorState cs;
                    try {
                        cs = completableFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        clog.error(connectorMessage,
                                "error getting the completable future " + ExceptionUtils.getStackTrace(e));
                        throw new RuntimeException("Error getting the completable future");
                    }
                    return cs;
                }).collect(Collectors.toList()))

                // And then we have to get the connector response lists from the connector
                // states
                .thenApplyAsync((connectorStates) -> {
                    List<List<ConnectorResponse>> responsesLists = connectorStates.stream().map(cs -> {
                        try {
                            clog.info(connectorMessage, "this is the connector state " + mapper.writeValueAsString(cs));
                        } catch (JsonProcessingException e) {
                        }
                        List<ConnectorResponse> cmResponses = cs.getConnectorResponseList().getResponses();

                        return cmResponses;
                    }).collect(Collectors.toList());

                    ConnectorState cs = connectorHubService.createConnectorState(
                            ConnectorRequestData.fromConnectorMessage(connectorMessage), connectorMessage);
                    // each connector message _could_ have multiple responses. Unpack each list of
                    // responses into a flat list on this connector state.
                    responsesLists.forEach(responsesList -> responsesList.forEach(cs::addResponse));
                    return cs;
                });
    }
    // endregion

    @CrossOrigin
    @PostMapping(path = "/sendRealtimeEvent", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ConnectorMessage sendRealtimeEvent(@RequestBody final String connectorJson) throws IOException {
        ConnectorMessage connectorMessage = mapper.readValue(connectorJson, ConnectorMessage.class);
        Map<String, String> parms = ConnectorControllerBase.getAllParams(connectorMessage);
        String eventName = parms.get(EVENT_NAME);
        String accounts = parms.get(ACCOUNTS);
        boolean success = true;
        String message = "";
        if (eventName != null && !eventName.isEmpty() && accounts != null && !accounts.isEmpty()) {
            List<String> affectedItems = mapper.readValue(accounts, new TypeReference<>() {
            });
            String accountIdList = "";
            for (String id : affectedItems) {
                accountIdList = accountIdList + id + ",";
            }
            try {
                realtimeEvents.send(CDP_SOURCE, eventName, affectedItems, connectorMessage, clog, realtimeEventService);
                message = String.format("Realtime event sent successfully for %s, affected items %s", eventName,
                        accountIdList);
                clog.info(connectorMessage, message);
            } catch (Exception e) {
                message = String.format("Realtime event was unsuccessful for %s, affected items %s ", eventName,
                        accountIdList) + e.getMessage();
                clog.error(connectorMessage, message);
            }
        } else {
            message = String.format("Something is missing when sending event for event name: %s, affected items: %s ",
                    eventName);
            clog.error(connectorMessage, message);
        }
        String response = "{\"response\":{\"success\":" + success + ", \"message\": \"" + message + "\"}}";
        clog.info(connectorMessage, "this is the response " + response);
        connectorMessage.setResponse(response);
        return connectorMessage;
    }

    //endregion


    private ConnectorRequestParams createConnectorRequestParams(ConnectorRequestParams connectorRequestParams,
            Map<String, String> allParams, List<String> paramNames){
        for (String name : paramNames) {
            String param = allParams.getOrDefault(name, "");
            if (!param.isEmpty()) {
                connectorRequestParams.addNameValue(name, param);
            }
        }
        return connectorRequestParams;
    }    

}