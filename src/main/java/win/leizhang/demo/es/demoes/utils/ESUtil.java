package win.leizhang.demo.es.demoes.utils;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 索引工具类
 */
@Component
public class ESUtil {

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
                    e.printStackTrace();
                }
            }
        }
        return client;
    }

    // 获取索引管理的client对象
    private IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    // 判定索引是否存在
    public boolean isExists(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名
     * @return
     */
    public boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名
     * @param shards    分片数
     * @param replicas  副本数
     * @return
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
     * @param indexName 索引名
     * @return
     */
    public boolean deleteIndex(String indexName) {
        DeleteIndexResponse deleteResponse = getAdminClient()
                .prepareDelete(indexName.toLowerCase())
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged() ? true : false;
    }
}
