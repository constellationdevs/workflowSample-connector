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

import coop.constellation.connectorservices.workflowexample.helpers.MultiLazyDataSourcePool;

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

    /**
     * Creates a JdbcTemplate using the lazy pooling datasource.
     * 
     * @param parms The parms passed to the request
     * @return
     */
    protected JdbcTemplate createJdbcTemplate(final Map<String, String> parms, ConnectorLogging clog,
            ConnectorMessage cm) {
        return new JdbcTemplate(MultiLazyDataSourcePool.getDataSource(parms, () -> parms.get("org"), clog, cm));
    }

    /**
     * Creates a NamedParameterJdbcTemplate using the lazy pooling datasource.
     * 
     * @param parms The parms passed to the request
     * @return
     */
    NamedParameterJdbcTemplate createNamedParameterJdbcTemplate(final Map<String, String> parms, ConnectorLogging clog,
            ConnectorMessage cm) {
        return new NamedParameterJdbcTemplate(
                MultiLazyDataSourcePool.getDataSource(parms, () -> parms.get("org"), clog, cm));
    }

    DataSource getDataSource(final Map<String, String> params, ConnectorLogging clog, ConnectorMessage cm) {
        return MultiLazyDataSourcePool.getDataSource(params, () -> params.get("org"), clog, cm);
    }

}