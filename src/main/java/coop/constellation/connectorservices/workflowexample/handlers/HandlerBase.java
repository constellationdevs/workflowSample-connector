package coop.constellation.connectorservices.workflowexample.handlers;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestParams;
import com.xtensifi.dspco.ConnectorMessage;
import com.xtensifi.dspco.UserData;

@Service
public abstract class HandlerBase implements HandlerLogic {

    Boolean isAauthenticated(UserData userData) {

        String memberID = userData.getUserId();
        try {
            if (memberID == null || memberID == "") {
                return false;
            } else {
                return true;
            }

        } catch (Exception e) {
            return false;

        }
    }

    public ConnectorRequestParams createConnectorRequestParams(ConnectorRequestParams connectorRequestParams,
            Map<String, String> allParams, List<String> paramNames) {
        for (String name : paramNames) {
            String param = allParams.getOrDefault(name, "");
            if (!param.isEmpty()) {
                connectorRequestParams.addNameValue(name, param);
            }
        }
        return connectorRequestParams;
    }

}