package com.zhugeio.etl.common.sink;

import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.function.Function;

/**
 * 动态分表 Doris Sink 构建器
 * 
 * 根据 appId 自动路由到对应的表 (如 b_user_123, b_device_456)
 * 
 * 使用示例:
 * <pre>
 * // 方式1: 使用预定义的快捷方法
 * DynamicDorisSinkBuilder.addUserSink(userStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addDeviceSink(deviceStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addUserPropertySink(propStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addEventAttrSink(eventStream, feNodes, database, username, password);
 * 
 * // 方式2: 自定义构建
 * DynamicDorisSinkBuilder.&lt;MyRow&gt;builder()
 *     .feNodes("doris-fe:8030")
 *     .database("dwd")
 *     .tablePrefix("my_table")
 *     .appIdExtractor(MyRow::getAppId)
 *     .username("root")
 *     .password("")
 *     .partialUpdate()
 *     .addTo(stream);
 * </pre>
 */
public class DynamicDorisSinkBuilder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDorisSinkBuilder.class);

    // 连接参数
    private String feNodes;
    private String database;
    private String tablePrefix;
    private String username = "root";
    private String password = "";

    // appId 提取器
    private Function<T, Integer> appIdExtractor;

    // 批量参数
    private String labelPrefix = "flink_doris";
    private int batchSize = 10000;
    private long batchIntervalMs = 5000;
    private int maxRetries = 3;

    // Stream Load 参数
    private String format = "json";
    private boolean partialUpdate = false;
    private boolean stripOuterArray = true;

    // 回调
    private CommitSuccessCallback callback;

    private DynamicDorisSinkBuilder() {}

    public static <T> DynamicDorisSinkBuilder<T> builder() {
        return new DynamicDorisSinkBuilder<>();
    }

    // ========== 连接参数 ==========

    public DynamicDorisSinkBuilder<T> feNodes(String feNodes) {
        this.feNodes = feNodes;
        return this;
    }

    public DynamicDorisSinkBuilder<T> database(String database) {
        this.database = database;
        return this;
    }

    public DynamicDorisSinkBuilder<T> tablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DynamicDorisSinkBuilder<T> username(String username) {
        this.username = username;
        return this;
    }

    public DynamicDorisSinkBuilder<T> password(String password) {
        this.password = password;
        return this;
    }

    public DynamicDorisSinkBuilder<T> appIdExtractor(Function<T, Integer> appIdExtractor) {
        this.appIdExtractor = appIdExtractor;
        return this;
    }

    // ========== 批量参数 ==========

    public DynamicDorisSinkBuilder<T> labelPrefix(String labelPrefix) {
        this.labelPrefix = labelPrefix;
        return this;
    }

    public DynamicDorisSinkBuilder<T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public DynamicDorisSinkBuilder<T> batchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
        return this;
    }

    public DynamicDorisSinkBuilder<T> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    // ========== Stream Load 参数 ==========

    public DynamicDorisSinkBuilder<T> partialUpdate() {
        this.partialUpdate = true;
        return this;
    }

    public DynamicDorisSinkBuilder<T> partialUpdate(boolean enable) {
        this.partialUpdate = enable;
        return this;
    }

    // ========== 模式预设 ==========

    public DynamicDorisSinkBuilder<T> highThroughput() {
        this.batchSize = 50000;
        this.batchIntervalMs = 10000;
        return this;
    }

    public DynamicDorisSinkBuilder<T> lowLatency() {
        this.batchSize = 5000;
        this.batchIntervalMs = 2000;
        return this;
    }

    // ========== 回调 ==========

    public DynamicDorisSinkBuilder<T> onSuccess(CommitSuccessCallback callback) {
        this.callback = callback;
        return this;
    }

    // ========== 构建 ==========

    public DorisSink<T> build() {
        validate();

        LOG.info("构建动态分表 Doris Sink: {}.{}_*, batchSize={}, partialUpdate={}",
                database, tablePrefix, batchSize, partialUpdate);

        DynamicTableSerializer<T> serializer = new DynamicTableSerializer<>(
                database, tablePrefix, appIdExtractor);

        return DorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .build();
    }

    public void addTo(DataStream<T> stream) {
        addTo(stream, "DorisSink-" + tablePrefix, "doris-sink-" + tablePrefix.replace("_", "-"));
    }

    public void addTo(DataStream<T> stream, String name, String uid) {
        if (callback != null) {
            // 使用带回调的 Sink
            CallbackDorisSink<T> sink = buildWithCallback();
            stream.sinkTo(sink).name(name).uid(uid);
        } else {
            stream.sinkTo(build()).name(name).uid(uid);
        }
        LOG.info("  ✓ {} 动态分表 Sink 已添加", tablePrefix);
    }

    public CallbackDorisSink<T> buildWithCallback() {
        validate();

        if (callback == null) {
            throw new IllegalArgumentException("callback is required for buildWithCallback()");
        }

        DynamicTableSerializer<T> serializer = new DynamicTableSerializer<>(
                database, tablePrefix, appIdExtractor);

        return CallbackDorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .setTableName(database + "." + tablePrefix + "_*")
                .setCommitCallback(callback)
                .build();
    }

    private void validate() {
        if (feNodes == null || feNodes.isEmpty()) {
            throw new IllegalArgumentException("feNodes is required");
        }
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("database is required");
        }
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            throw new IllegalArgumentException("tablePrefix is required");
        }
        if (appIdExtractor == null) {
            throw new IllegalArgumentException("appIdExtractor is required");
        }
    }

    private DorisOptions buildDorisOptions() {
        // 注意: 动态表名模式下，这里的 tableIdentifier 只是占位符
        // 实际表名由 DynamicTableSerializer 决定
        return DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(database + "." + tablePrefix + "_0")
                .setUsername(username)
                .setPassword(password)
                .build();
    }

    private DorisExecutionOptions buildExecutionOptions() {
        Properties props = new Properties();
        props.setProperty("format", format);
        props.setProperty("strip_outer_array", String.valueOf(stripOuterArray));
        if (partialUpdate) {
            props.setProperty("partial_columns", "true");
        }

        return DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferCount(batchSize)
                .setBufferFlushIntervalMs(batchIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props)
                .enable2PC()
                .build();
    }

    // ========== 静态快捷方法 ==========

    /**
     * 添加 UserRow 动态分表 Sink
     * 表名: b_user_{appId}
     */
    public static void addUserSink(DataStream<UserRow> stream,
                                    String feNodes, String database,
                                    String username, String password,
                                    CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<UserRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user")
                .appIdExtractor(UserRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 DeviceRow 动态分表 Sink
     * 表名: b_device_{appId}
     */
    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                      String feNodes, String database,
                                      String username, String password,
                                      CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<DeviceRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_device")
                .appIdExtractor(DeviceRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 UserPropertyRow 动态分表 Sink
     * 表名: b_user_property_{appId}
     */
    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                            String feNodes, String database,
                                            String username, String password,
                                            CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<UserPropertyRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_property")
                .appIdExtractor(UserPropertyRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink
     * 表名: b_user_event_attr_{appId}
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                         String feNodes, String database,
                                         String username, String password,
                                         CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<EventAttrRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_event_attr")
                .appIdExtractor(EventAttrRow::getAppId)
                .username(username)
                .password(password)
                .highThroughput()
                .onSuccess(callback)
                .addTo(stream);
    }
}
