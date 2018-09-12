package win.leizhang.demo.es.demoes.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.net.InetAddress;
import java.util.Properties;

@Component
public class ESTransportClient implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(ESTransportClient.class);
    //private String clusterNodes = "10.0.53.68:9300,10.0.53.69:9300,10.0.53.70:9300";
    //private String clusterName = "hrt-points-es";
    private String clusterName = "elasticsearch";
    private String clusterNodes = "10.0.55.27:9300";
    private Boolean clientTransportSniff = true;
    private Boolean clientIgnoreClusterName = Boolean.FALSE;
    private String clientPingTimeout = "60s";
    private String clientNodesSamplerInterval = "5s";
    private TransportClient client;
    private Properties properties = new Properties();
    private boolean isNormalWork;
    static final String COLON = ":";
    static final String COMMA = ",";

    @Override
    public void destroy() throws Exception {
        try {
            logger.info("Closing elasticSearch  client");
            if (client != null) {
                client.close();
            }
        } catch (final Exception e) {
            logger.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public TransportClient getObject() throws Exception {
        return client;
    }

    @Override
    public Class<TransportClient> getObjectType() {
        return TransportClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        buildClient();
    }

    public void buildClient() throws Exception {
        //client = TransportClient.builder().settings(settings()).build();
        client = new PreBuiltTransportClient(settings());
        Assert.hasText(clusterNodes, "[Assertion failed] clusterNodes settings missing.");
        for (String clusterNode : clusterNodes.split(COMMA)) {
            String hostName = substringBeforeLast(clusterNode, COLON);
            String port = substringAfterLast(clusterNode, COLON);
            Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
            Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
            logger.info("adding transport node : " + clusterNode);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
        }
        client.connectedNodes();
    }

    private Settings settings() {
        return Settings.builder()
                .put("cluster.name",
                        properties.getProperty("cluster.name") == null ? clusterName
                                : properties.getProperty("cluster.name"))
//				.put("client.transport.sniff",
//						properties.getProperty("client.transport.sniff") == null ? clientTransportSniff
//								: properties.getProperty("client.transport.sniff"))
                .put("client.transport.ignore_cluster_name",
                        properties.getProperty("client.transport.ignore_cluster_name") == null ? clientIgnoreClusterName
                                : properties.getProperty("client.transport.ignore_cluster_name"))
                .put("client.transport.ping_timeout",
                        properties.getProperty("client.transport.ping_timeout") == null ? clientPingTimeout
                                : properties.getProperty("client.transport.ping_timeout"))
                .put("client.transport.nodes_sampler_interval",
                        properties.getProperty("client.transport.nodes_sampler_interval") == null
                                ? clientNodesSamplerInterval
                                : properties.getProperty("client.transport.nodes_sampler_interval"))
                //.put("xpack.security.user", "elastic:changeme")
                .build();
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setClientTransportSniff(Boolean clientTransportSniff) {
        this.clientTransportSniff = clientTransportSniff;
    }

    public String getClientNodesSamplerInterval() {
        return clientNodesSamplerInterval;
    }

    public void setClientNodesSamplerInterval(String clientNodesSamplerInterval) {
        this.clientNodesSamplerInterval = clientNodesSamplerInterval;
    }

    public String getClientPingTimeout() {
        return clientPingTimeout;
    }

    public void setClientPingTimeout(String clientPingTimeout) {
        this.clientPingTimeout = clientPingTimeout;
    }

    public Boolean getClientIgnoreClusterName() {
        return clientIgnoreClusterName;
    }

    public void setClientIgnoreClusterName(Boolean clientIgnoreClusterName) {
        this.clientIgnoreClusterName = clientIgnoreClusterName;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean isNormalWork() {
        return isNormalWork;
    }

    public void setIsNormalWork(boolean isNormalWork) {
        this.isNormalWork = isNormalWork;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public String getClusterName() {
        return clusterName;
    }


    private static String substringBeforeLast(String str, String formater) {

        return str.substring(0, str.indexOf(formater));
    }

    private static String substringAfterLast(String str, String formater) {
        return str.substring(str.indexOf(formater) + 1, str.length());
    }

}
