package win.leizhang.demo.es.demoes.utils;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * es初始化工具类
 */
@Component
public class EsInitUtil {

    private final static Logger log = LoggerFactory.getLogger(EsInitUtil.class);

    @Value("${es.transport.sniff.enable}")
    private boolean sniffEnable;
    @Value("${es.cluster.name}")
    private String clusterName;//集群名称
    @Value("${es.cluster-node}")
    private String clusterNode;//集群主机的master节点

    private Settings sts = Settings.builder()
            .put("client.transport.sniff", sniffEnable)
            .put("cluster.name", clusterName)
            .build();
    private static volatile TransportClient client;

    /**
     * 全局客户端
     *
     * @return client
     * @throws UnknownHostException
     */
    public TransportClient getClient() {
        if (client == null) {
            // 同步xxx代码块的作用和[synchronized static]方法作用一样, 对当前对应的*.class进行持锁, static方法和.class一样都是锁的该类本身,同一个监听器.
            synchronized (TransportClient.class) {
                String node[] = clusterNode.split(":");
                try {
                    client = new PreBuiltTransportClient(sts)
                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node[0]), Integer.parseInt(node[1])));
                } catch (UnknownHostException e) {
                    client = null;
                    log.error("[es初始化] 无效的主机异常！配置信息==>{}, 节点==>{}", sts.toString(), clusterName);
                    e.printStackTrace();
                }
            }
            clusterInfo();
            allIndex();
        }
        return client;
    }

    // 查看集群信息
    private void clusterInfo() {
        List<DiscoveryNode> nodes = client.connectedNodes();
        for (DiscoveryNode node : nodes) {
            log.info("[es集群信息] hostId={}, hostName={}, address={}", node.getHostAddress(), node.getHostName(), node.getAddress());
        }
    }

    // 获取索引管理的client对象
    private IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    // 获取所有索引
    private void allIndex() {
        ClusterStateResponse response = getClient().admin()
                .cluster()
                .prepareState()
                .get();

        // 所有
        String[] ids = response.getState().getMetaData().getConcreteAllIndices();
        log.info("[es集群信息] index总数={}", ids.length);
        for (String index : ids) {
            log.info("[es集群信息] 获取的index={}", index);
        }
    }

    // 判定索引是否存在
    public boolean isExists(String index) {
        IndicesExistsResponse response = getAdminClient().prepareExists(index).get();
        return response.isExists() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param index 索引名
     */
    public boolean createIndex(String index) {
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(index.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名
     * @param shards    分片数
     * @param replicas  副本数
     */
    public boolean createIndex(String indexName, int shards, int replicas) {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 为索引indexName设置mapping
     *
     * @param indexName 索引名
     * @param typeName
     * @param mapping
     */
    public void setMapping(String indexName, String typeName, String mapping) {
        getAdminClient().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();
    }

    /**
     * 删除索引
     *
     * @param index 索引名
     */
    public boolean deleteIndex(String index) {
        DeleteIndexResponse deleteResponse = getAdminClient()
                .prepareDelete(index.toLowerCase())
                .get();
        return deleteResponse.isAcknowledged() ? true : false;
    }
}
