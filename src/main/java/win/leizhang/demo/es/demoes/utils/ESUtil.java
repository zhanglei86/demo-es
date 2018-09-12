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
    //private static final String CLUSTER_NAME = "hrt-points-es";
    //private static final String HOSTNAME = "10.0.53.68";

    private static final String CLUSTER_NAME = "elasticsearch";
    private static final String HOSTNAME = "10.0.55.27";
    private static final int TCP_PORT = 9300;

    @Value("${es.cluster.name}")
    private String clusterName;
    @Value("${es.cluster-nodes}")
    private String clusterNodes;

    //构建Settings对象
    private Settings settings = Settings.builder().put("cluster.name", clusterName).build();
    //TransportClient对象，用于连接ES集群
    //private static volatile TransportClient client;
    private static TransportClient client;

    /**
     * 同步synchronized(*.class)代码块的作用和synchronized static方法作用一样,
     * 对当前对应的*.class进行持锁,static方法和.class一样都是锁的该类本身,同一个监听器
     *
     * @return
     * @throws UnknownHostException
     */
    public TransportClient getClient() {
        if (client == null) {
            synchronized (TransportClient.class) {

                String node[] = clusterNodes.split(":");
                try {
                    client = new PreBuiltTransportClient(settings)
                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node[0]), Integer.parseInt(node[1])));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.connectedNodes();
        return client;
    }

    /**
     * 获取索引管理的IndicesAdminClient
     */
    public IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    /**
     * 判定索引是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isExists(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName
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
     * @param indexName
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
     * @param indexName
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
