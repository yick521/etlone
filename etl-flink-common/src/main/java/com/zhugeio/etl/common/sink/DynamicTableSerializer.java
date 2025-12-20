package com.zhugeio.etl.common.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * 动态表名序列化器
 * 
 * 支持根据记录内容动态路由到不同的 Doris 表
 * 
 * 使用示例:
 * <pre>
 * // UserRow 写入 b_user_{appId}
 * DynamicTableSerializer<UserRow> serializer = new DynamicTableSerializer<>(
 *     "dwd", "b_user", UserRow::getAppId
 * );
 * 
 * // EventAttrRow 写入 b_user_event_attr_{appId}
 * DynamicTableSerializer<EventAttrRow> serializer = new DynamicTableSerializer<>(
 *     "dwd", "b_user_event_attr", EventAttrRow::getAppId
 * );
 * </pre>
 */
public class DynamicTableSerializer<T> implements DorisRecordSerializer<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableSerializer.class);

    private transient ObjectMapper objectMapper;
    
    private final String database;
    private final String tablePrefix;
    private final Function<T, Integer> appIdExtractor;
    private final JsonInclude.Include includeStrategy;

    /**
     * 构造函数 (默认过滤 null 值)
     * 
     * @param database 数据库名，如 "dwd"
     * @param tablePrefix 表前缀，如 "b_user"，最终表名为 b_user_{appId}
     * @param appIdExtractor 从记录中提取 appId 的函数
     */
    public DynamicTableSerializer(String database, String tablePrefix, 
                                   Function<T, Integer> appIdExtractor) {
        this(database, tablePrefix, appIdExtractor, JsonInclude.Include.NON_NULL);
    }

    /**
     * 构造函数 (自定义 JSON 序列化策略)
     * 
     * @param database 数据库名
     * @param tablePrefix 表前缀
     * @param appIdExtractor 从记录中提取 appId 的函数
     * @param includeStrategy JSON 序列化包含策略
     */
    public DynamicTableSerializer(String database, String tablePrefix,
                                   Function<T, Integer> appIdExtractor,
                                   JsonInclude.Include includeStrategy) {
        this.database = database;
        this.tablePrefix = tablePrefix;
        this.appIdExtractor = appIdExtractor;
        this.includeStrategy = includeStrategy;
    }

    private void initObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(includeStrategy);
        }
    }

    @Override
    public DorisRecord serialize(T record) throws IOException {
        if (record == null) {
            return null;
        }
        
        initObjectMapper();
        
        // 1. 提取 appId
        Integer appId = appIdExtractor.apply(record);
        if (appId == null) {
            LOG.warn("Record appId is null, skipping: {}", record);
            return null;
        }
        
        // 2. 构建动态表名: database.tablePrefix_appId
        String tableName = database + "." + tablePrefix + "_" + appId;
        
        // 3. 序列化数据为 JSON
        String json = objectMapper.writeValueAsString(record);
        
        // 4. 使用 DorisRecord.of(tableName, data) 指定目标表
        return DorisRecord.of(tableName, json.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void initial() {
        // 初始化 ObjectMapper
        initObjectMapper();
    }

    @Override
    public DorisRecord flush() {
        // 无需刷新
        return null;
    }

    /**
     * 获取数据库名
     */
    public String getDatabase() {
        return database;
    }

    /**
     * 获取表前缀
     */
    public String getTablePrefix() {
        return tablePrefix;
    }
}
