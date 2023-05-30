package coop.constellation.connectorservices.workflowexample.service;

import com.xtensifi.connectorservices.common.events.RealtimeEventService;
import com.xtensifi.connectorservices.common.events.RealtimeEventServiceImpl;
import com.xtensifi.connectorservices.common.workflow.ConnectorConfig;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubService;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubServiceImpl;
import coop.constellation.connectorservices.workflowexample.helpers.RealtimeEvents;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeansConfig {

    /**
     * Used for sending realtime events
     * @return
     */
    @Bean
    RealtimeEventService realtimeEventService(){
        return new RealtimeEventServiceImpl();
    }

    @Bean
    RealtimeEvents realtimeEvents(){
        return new RealtimeEventsImpl();
    }


    @Bean
    ConnectorHubService connectorHubService() {
        return new ConnectorHubServiceImpl();
    }

    @Bean
    ConnectorConfig connectorConfig() {
        return new ConnectorConfig();
    }
}
