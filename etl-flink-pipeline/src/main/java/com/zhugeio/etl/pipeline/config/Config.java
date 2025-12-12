package com.zhugeio.etl.pipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理类
 * 
 * 配置优先级: 系统属性 > 环境变量 > 配置文件
 */
public class Config {
    
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);
    
    // ============ 原有常量 (保持不变) ============
    public static final String FRONT_DB_FILE = "front_db.properties";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_SOURCE_TOPIC = "kafka.sourceTopic";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String STATIS_TOPIC = "statisTopic";
    public static final String QUALITY_TOPIC = "qualityTopic";
    public static final String JEDIS_FILE = "data_quality_redis.properties";
    public static final String DEBUG_TOPIC = "debugTopic";
    public static final String PRIVATE_KEY = "privateKey";
    public static final String ACESS_DATA_TIMEOUT_SECONDS = "acess_data_timeout_seconds";
    public static final String ACESS_DATA_TIME_SECONDS = "acess_data_time_seconds";
    public static final String ENCRYPTION_SECRET_PATH = "encryption_secret_path";
    public static final String ROOT_SECRET_PATH = "root_secret_path";
    public static final String WORKER_SECRET_PATH = "worker_secret_path";
    public static final String PRIVATE_KEY_PATH = "private_key_path";
    public static final String SM2_PRIKEY = "sm2_priKey";
    public static final String SM4_PRIKEY_PATH = "sm4_priKey_path";
    public static final String ENCRYPTION_TYPE = "encryption_type";
    public static final String SPARK_UI_ENABLED = "spark.ui.enabled";
    public static final String APPKEYS_AND_PARTITIONS = "appkeys.and.partitions";
    
    // ============ DwJob 新增常量 ============
    // 调试
    public static final String DEBUG_ENABLED = "debug.enabled";
    
    // Flink
    public static final String FLINK_PARALLELISM = "flink.parallelism";
    public static final String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    public static final String FLINK_CHECKPOINT_TIMEOUT = "flink.checkpoint.timeout";
    public static final String FLINK_CHECKPOINT_PATH = "flink.checkpoint.path";
    
    // KVRocks
    public static final String KVROCKS_HOST = "kvrocks.host";
    public static final String KVROCKS_PORT = "kvrocks.port";
    public static final String KVROCKS_PASSWORD = "kvrocks.password";
    public static final String KVROCKS_CLUSTER = "kvrocks.cluster";
    public static final String KVROCKS_LOCAL_CACHE_SIZE = "kvrocks.local.cache.size";
    public static final String KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES = "kvrocks.local.cache.expire.minutes";
    
    // Redis (百度关键词缓存)
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_CLUSTER = "redis.cluster";
    
    // Doris
    public static final String DORIS_HOST = "doris.host";
    public static final String DORIS_HTTP_PORT = "doris.http.port";
    public static final String DORIS_DB = "doris.db";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    public static final String DORIS_FE_NODES = "doris.fe.nodes";
    public static final String DORIS_USERNAME = "doris.username";
    public static final String DORIS_DATABASE = "doris.database";
    
    // IP 解析
    public static final String IP_FILE_DIR = "ip.file.dir";
    public static final String IPV6_FILE_DIR = "ipv6.file.dir";
    public static final String IPV6_LOAD = "ipv6.load";
    public static final String RELOAD_IP_FILE = "reload.ip.file";
    public static final String RELOAD_IPV6_FILE = "reload.ipv6.file";
    public static final String RELOAD_RATE_SECOND = "reload.rate.second";
    public static final String FLAG_HA = "flag.ha";
    public static final String IP_DATABASE_PATH = "ip.database.path";
    public static final String IP_DATABASE_RELOAD_ENABLED = "ip.database.reload.enabled";
    public static final String IP_DATABASE_RELOAD_INTERVAL_SECONDS = "ip.database.reload.interval.seconds";
    
    // HDFS
    public static final String FS_DEFAULT_FS = "fs.defaultFS";
    public static final String DFS_NAMESERVICES = "dfs.nameservices";
    public static final String DFS_HA_NAMESPACE = "dfs.ha.namenodes.namespace";
    public static final String RPC_Z1 = "dfs.namenode.rpc-address.namespace.z1";
    public static final String RPC_Z2 = "dfs.namenode.rpc-address.namespace.z2";
    public static final String HDFS_HA_ENABLED = "hdfs.ha.enabled";
    
    // 百度 API
    public static final String BAIDU_URL = "baidu.url";
    public static final String BAIDU_ID = "baidu_id";
    public static final String BAIDU_KEY = "baidu_key";
    
    // HTTP
    public static final String REQUEST_SOCKET_TIMEOUT = "requestSocketTimeout";
    public static final String REQUEST_CONNECT_TIMEOUT = "requestConnectTimeout";
    public static final String REQUEST_TIMEOUT = "requestTimeout";
    public static final String MAX_RETRY_NUM = "maxRetryNum";
    public static final String BATCH_SIZE = "batchSize";
    
    // 时间
    public static final String TIME_EXPIRE_SUBDAYS = "subtime";
    public static final String TIME_EXPIRE_ADDDAYS = "addtime";
    
    // 应用
    public static final String BLACK_APPIDS = "blackAppIds";
    public static final String WHITE_APPID = "white_appid";
    public static final String EVENT_ATTR_LENGTH_LIMIT = "event_attr_length_limit";
    public static final String WRITE_EVENT_ALL_FLAG = "write.event.all.flag";
    public static final String WRITE_EVENT_ATTR_EID_PARTITION = "write.event.attr.eid.partition";
    
    // 数据库类型
    public static final String DB_TYPE = "dbtype";  // 1=Kudu, 2=Doris
    
    // 写入
    public static final String WRITE_POOL = "write.pool";
    public static final String BATCH_DATA_NUM = "batch.dataNum";
    public static final String MUTATION_BUFFER = "mutation.buffer";
    
    // 算子配置
    public static final String OPERATOR_KEYWORD_TIMEOUT_MS = "operator.keyword.timeout.ms";
    public static final String OPERATOR_KEYWORD_CAPACITY = "operator.keyword.capacity";
    public static final String OPERATOR_IP_TIMEOUT_MS = "operator.ip.timeout.ms";
    public static final String OPERATOR_IP_CAPACITY = "operator.ip.capacity";
    public static final String OPERATOR_UA_TIMEOUT_MS = "operator.ua.timeout.ms";
    public static final String OPERATOR_UA_CAPACITY = "operator.ua.capacity";
    public static final String OPERATOR_UA_CACHE_SIZE = "operator.ua.cache.size";
    public static final String OPERATOR_UA_CACHE_EXPIRE_MINUTES = "operator.ua.cache.expire.minutes";
    
    private static final Properties properties = new Properties();
    
    // 静态初始化块加载配置 (保持原有逻辑)
    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input != null) {
                properties.load(input);
            } else {
                LOG.warn("无法找到 config.properties 文件，使用默认值");
            }
        } catch (IOException e) {
            LOG.error("加载配置文件失败", e);
        }
    }
    
    // ============ 原有方法 (保持不变) ============
    
    public static String getProp(String key) {
        return properties.getProperty(key);
    }
    
    public static String getProp(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
    
    public static int getInt(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("配置项 " + key + " 不存在");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("配置项 " + key + " 不是有效的整数: " + value, e);
        }
    }
    
    public static String readResource(String filename) {
        try (BufferedReader ir = new BufferedReader(new InputStreamReader(
                Config.class.getClassLoader().getResourceAsStream(filename)))) {
            return ir.readLine();
        } catch (IOException e) {
            throw new RuntimeException("读取资源文件失败: " + filename, e);
        }
    }
    
    public static String readFile(String filename) {
        try (BufferedReader ir = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            return ir.readLine();
        } catch (IOException e) {
            throw new RuntimeException("读取文件失败: " + filename, e);
        }
    }
    
    // ============ DwJob 新增方法 (带默认值版本) ============
    
    /**
     * 获取字符串配置 (支持默认值)
     * 优先级: 系统属性 > 环境变量 > 配置文件 > 默认值
     */
    public static String getString(String key, String defaultValue) {
        // 1. 系统属性
        String value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 2. 环境变量 (将 . 替换为 _)
        String envKey = key.replace('.', '_').toUpperCase();
        value = System.getenv(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 3. 配置文件
        value = properties.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 4. 默认值
        return defaultValue;
    }
    
    /**
     * 获取字符串配置 (无默认值)
     */
    public static String getString(String key) {
        return getString(key, null);
    }
    
    /**
     * 获取整数配置 (支持默认值)
     */
    public static int getInt(String key, int defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }
    
    /**
     * 获取长整数配置
     */
    public static long getLong(String key, long defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid long value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }
    
    /**
     * 获取布尔配置
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            return "true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim());
        }
        return defaultValue;
    }
    
    /**
     * 获取字符串列表
     */
    public static String[] getStringArray(String key, String delimiter) {
        String value = getString(key, "");
        if (value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }
    
    /**
     * 是否是 Doris 模式
     */
    public static boolean isDorisMode() {
        return getInt(DB_TYPE, 1) == 2;
    }
    
    /**
     * 是否是 Kudu 模式
     */
    public static boolean isKuduMode() {
        return getInt(DB_TYPE, 1) == 1;
    }
    
    /**
     * 打印配置摘要 (调试用)
     */
    public static void printConfig() {
        LOG.info("╔══════════════════════════════════════════════════════╗");
        LOG.info("║               Configuration Summary                  ║");
        LOG.info("╠══════════════════════════════════════════════════════╣");
        LOG.info("║ [Kafka]");
        LOG.info("║   brokers: {}", getString(KAFKA_BROKERS, "N/A"));
        LOG.info("║   sourceTopic: {}", getString(KAFKA_SOURCE_TOPIC, "N/A"));
        LOG.info("║   groupId: {}", getString(KAFKA_GROUP_ID, "N/A"));
        LOG.info("║ [Flink]");
        LOG.info("║   parallelism: {}", getInt(FLINK_PARALLELISM, 16));
        LOG.info("║   checkpoint.interval: {}", getLong(FLINK_CHECKPOINT_INTERVAL, 60000L));
        LOG.info("║ [KVRocks]");
        LOG.info("║   host: {}", getString(KVROCKS_HOST, "N/A"));
        LOG.info("║   port: {}", getInt(KVROCKS_PORT, 6379));
        LOG.info("║ [Doris]");
        LOG.info("║   fe.nodes: {}", getString(DORIS_FE_NODES, getString(DORIS_HOST, "N/A")));
        LOG.info("║   database: {}", getString(DORIS_DATABASE, getString(DORIS_DB, "N/A")));
        LOG.info("╚══════════════════════════════════════════════════════╝");
    }
    
    /**
     * 获取所有配置
     */
    public static Properties getAllProperties() {
        Properties all = new Properties();
        all.putAll(properties);
        return all;
    }
}
