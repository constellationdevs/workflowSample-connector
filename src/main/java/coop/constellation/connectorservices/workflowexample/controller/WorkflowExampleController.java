package coop.constellation.connectorservices.workflowexample.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.events.RealtimeEventService;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubService;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestData;
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
import coop.constellation.connectorservices.workflowexample.helpers.ConnectorResponseEntityBuilder;
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
@RequestMapping("/externalConnector/WorkflowSampleConnector/1.0")
public class WorkflowExampleController extends ConnectorControllerBase {

    @Autowired
    // ConnectorHubService is required for workflow methods
    private ConnectorHubService connectorHubService;

    private final ConnectorLogging clog;
    private final ObjectMapper mapper;
    private final ConnectorResponseEntityBuilder responseEntityBuilder;
    private final RealtimeEventService realtimeEventService;
    private final RealtimeEvents realtimeEvents;
    private final BaseParamsSupplier baseParamsSupplier;

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
    @PostMapping(path = "/retrieveAccountListRefresh", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveAccountListRefresh(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccountsRefresh"))
                .thenApply(this.handleResponseEntity(retrieveAccountListRefreshHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }

    @PostMapping(path = "/retrieveAccountList", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveAccountList(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                .thenApply(this.handleResponseEntity(retrieveAccountListHandler))
                .thenApply(this.handleResponseEntity(retrieveAccountListRefreshHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);
    }
    // endregion

    // region retrieveTransactionList

    // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveTransactionList", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveTransactionList(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getTransactions"))
                .thenApply(this.handleResponseEntity(retrieveTransactionListHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);
    }
    // endregion

    // region retrieveUserBySocial

    // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveUserBySocial", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveUserBySocial(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyBySSN"))
                .thenApply(this.handleResponseEntity(retrieveUserBySocialHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);
    }

    // // endregion

    // // region retrieveUserById

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveUserById", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveUserById(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyById"))
                .thenApply(this.handleResponseEntity(retrieveUserByIdHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // // region retrieveTransactionCategories

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveTransactionCategories", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveTransactionCategories(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactionCategories"))
                .thenApply(this.handleResponseEntity(retrieveTransactionCategoriesHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // // region editTransaction

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/editTransactions", produces = "application/json", consumes = "application/json")
    public ResponseEntity editTransactions(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "updateTransactions"))
                .thenApply(this.handleResponseEntity(editTransactionsHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // // region startTransfer

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/startTransfer", produces = "application/json", consumes = "application/json")
    public ResponseEntity startTransfer(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "createInternalTransfer"))
                .thenApply(this.handleResponseEntity(startTransferHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // // region p2pTransfer
    @PostMapping(path = "/p2pTransfer", produces = "application/json", consumes = "application/json")
    public ResponseEntity p2pTransfer(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "personToPersonTransfer"))
                .thenApply(this.handleResponseEntity(p2pTransferHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // // region stopPayment
    @PostMapping(path = "/stopPayment", produces = "application/json", consumes = "application/json")
    public ResponseEntity stopPayment(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "createStopPayment"))
                .thenApply(this.handleResponseEntity(stopPaymentHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }
    // // endregion

    // region Validate Member Account Info
    @PostMapping(path = "/validateMemberAccountInfo", produces = "application/json", consumes = "application/json")
    public ResponseEntity validateMemberAccountInfo(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorMessage> future = connectorHubService
                .executeConnector(connectorMessage,
                        new ConnectorRequestData("kivapublic", "1.0", "validateMemberAccountInfo"))
                .thenApply(this.handleResponseEntity(validateMemberAccountInfoHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running submitApplication future: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, future);

    }

    @PostMapping(path = "/multiCall", produces = "application/json", consumes = "application/json")
    public ResponseEntity multiCall(@RequestBody final ConnectorMessage connectorMessage) {

        CompletableFuture<ConnectorState> getAccountsFuture = connectorHubService.initAsyncConnectorRequest(
                connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                .thenApplyAsync(connectorHubService.callConnectorAsync())
                .thenApply(connectorHubService.waitForConnectorResponse());

        CompletableFuture<ConnectorState> getTransactionsFuture = connectorHubService.initAsyncConnectorRequest(
                connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getTransactions"))
                .thenApplyAsync(connectorHubService.callConnectorAsync())
                .thenApply(connectorHubService.waitForConnectorResponse());

        CompletableFuture<ConnectorMessage> resultFuture = invokeCompletableFutures(
                List.of(getAccountsFuture, getTransactionsFuture), connectorMessage)
                .thenApply(this.handleResponseEntity(multiCallHandler))
                .thenApply(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running getSearchPage: " + exception.getMessage()));

        return responseEntityBuilder.build(HttpStatus.OK, resultFuture);
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
        Map<String, String> parms = ConnectorControllerBase.getAllParams(connectorMessage, baseParamsSupplier.get());
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

}