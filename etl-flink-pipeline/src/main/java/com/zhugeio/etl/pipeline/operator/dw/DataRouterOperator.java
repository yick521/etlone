package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.model.output.*;
import com.zhugeio.etl.pipeline.config.Config;
import com.zhugeio.etl.pipeline.example.ZGMessage;
import com.zhugeio.etl.pipeline.service.BaiduKeywordService;
import com.zhugeio.etl.pipeline.service.EventAttrColumnService;
import com.zhugeio.etl.pipeline.transfer.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据路由算子
 * 
 * 对应 Scala: MsgTransfer.transfer
 * 
 * 输入: 已富化的 ZGMessage (包含 IP/UA 解析结果)
 * 输出: 根据 dt 类型路由到不同的侧输出
 * 
 * 路由规则:
 * - zgid -> UserRow
 * - pl -> DeviceRow
 * - usr -> UserPropertyRow (可能多条)
 * - evt/vtl/mkt/ss/se/abp -> EventAttrRow
 * 
 * 注意: 不再输出 EventAllRow (Scala 工程中可配置关闭)
 */
public class DataRouterOperator extends ProcessFunction<ZGMessage, EventAttrRow> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouterOperator.class);
    
    // 侧输出标签
    public static final OutputTag<UserRow> USER_OUTPUT = 
            new OutputTag<UserRow>("user-output") {};
    public static final OutputTag<DeviceRow> DEVICE_OUTPUT = 
            new OutputTag<DeviceRow>("device-output") {};
    public static final OutputTag<UserPropertyRow> USER_PROPERTY_OUTPUT = 
            new OutputTag<UserPropertyRow>("user-property-output") {};
    
    // 黑名单应用
    private Set<String> blackAppIds;
    
    // 白名单应用 (用于百度关键词)
    private Set<String> whiteAppIds;
    
    // CDP 应用
    private Set<Integer> cdpAppIds;
    
    // 转换器
    private transient UserTransfer userTransfer;
    private transient DeviceTransfer deviceTransfer;
    private transient UserPropertyTransfer userPropertyTransfer;
    private transient EventAttrTransfer eventAttrTransfer;
    
    // 服务
    private transient EventAttrColumnService columnService;
    private transient BaiduKeywordService keywordService;
    
    // 统计
    private final AtomicLong userCount = new AtomicLong(0);
    private final AtomicLong deviceCount = new AtomicLong(0);
    private final AtomicLong userPropertyCount = new AtomicLong(0);
    private final AtomicLong eventAttrCount = new AtomicLong(0);
    private final AtomicLong unknownCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong blacklistedCount = new AtomicLong(0);
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 加载黑名单
        String blackAppsStr = Config.getString(Config.BLACK_APPIDS, "-1");
        blackAppIds = new HashSet<>(Arrays.asList(blackAppsStr.split(",")));
        
        // 加载白名单 (用于百度关键词)
        String whiteAppsStr = Config.getString(Config.WHITE_APPID, "");
        whiteAppIds = new HashSet<>();
        if (!whiteAppsStr.isEmpty()) {
            whiteAppIds.addAll(Arrays.asList(whiteAppsStr.split(",")));
        }
        
        // CDP 应用 (TODO: 从配置或数据库加载)
        cdpAppIds = new HashSet<>();
        
        // 初始化列映射服务
        columnService = new EventAttrColumnService(
                Config.getString(Config.KVROCKS_HOST, "localhost"),
                Config.getInt(Config.KVROCKS_PORT, 6379),
                Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 10000),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 60)
        );
        columnService.init();
        
        // 初始化百度关键词服务
        keywordService = new BaiduKeywordService(
                Config.getString(Config.BAIDU_URL, "http://referer.bj.baidubce.com/v1/eqid"),
                Config.getString(Config.BAIDU_ID, ""),
                Config.getString(Config.BAIDU_KEY, ""),
                Config.getString(Config.REDIS_HOST, "localhost"),
                Config.getInt(Config.REDIS_PORT, 6379),
                Config.getBoolean(Config.REDIS_CLUSTER, false),
                Config.getInt(Config.REQUEST_TIMEOUT, 5)
        );
        keywordService.init();
        
        // 初始化转换器
        userTransfer = new UserTransfer();
        deviceTransfer = new DeviceTransfer();
        
        userPropertyTransfer = new UserPropertyTransfer();
        userPropertyTransfer.setCdpAppIds(cdpAppIds);
        
        int expireSubDays = Config.getInt(Config.TIME_EXPIRE_SUBDAYS, 7);
        int expireAddDays = Config.getInt(Config.TIME_EXPIRE_ADDDAYS, 1);
        int eventAttrLengthLimit = Config.getInt(Config.EVENT_ATTR_LENGTH_LIMIT, 256);
        eventAttrTransfer = new EventAttrTransfer(columnService, expireSubDays, expireAddDays, eventAttrLengthLimit);
        
        LOG.info("DataRouterOperator initialized: blackAppIds={}, whiteAppIds={}", 
                blackAppIds.size(), whiteAppIds.size());
    }
    
    @Override
    public void processElement(ZGMessage message, Context ctx, Collector<EventAttrRow> out) throws Exception {
        try {
            // 直接从 ZGMessage 获取数据
            Map<String, Object> msgData = message.getData();
            if (msgData == null) {
                errorCount.incrementAndGet();
                return;
            }
            
            // 从 ZGMessage 获取基础信息
            Integer appId = message.getAppId();
            String business = message.getBusiness();
            
            if (appId == null || appId == 0) {
                errorCount.incrementAndGet();
                return;
            }
            
            // 黑名单检查
            if (blackAppIds.contains(String.valueOf(appId))) {
                blacklistedCount.incrementAndGet();
                return;
            }
            
            // 从 data map 获取其他字段
            Integer platform = getIntValue(msgData, "plat", 0);
            String ua = getStringValue(msgData, "ua");
            String ip = getStringValue(msgData, "ip");
            
            // 获取消息级别的 st 时间戳 (用于 DeviceTransfer)
            Long msgSt = getLongValue(msgData, "st", null);
            
            // 获取设备 MD5 (来自 usr.did)
            String deviceMd5 = null;
            Object usrObj = msgData.get("usr");
            if (usrObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> usr = (Map<String, Object>) usrObj;
                deviceMd5 = getStringValue(usr, "did");
            }
            
            // 获取富化数据 (由上游算子填充)
            String[] ipResult = extractIpResultFromMap(msgData);
            Map<String, String> uaResult = extractUaResultFromMap(msgData);
            if (business == null || business.isEmpty()) {
                business = getStringValue(msgData, "business", "\\N");
            }
            
            // 获取 data 数组
            Object dataObj = msgData.get("data");
            if (dataObj == null) {
                return;
            }
            
            List<?> dataList = (List<?>) dataObj;
            if (dataList.isEmpty()) {
                return;
            }
            
            // 预先提取所有 eqid 用于批量查询关键词
            Map<String, String> eqidKeywords = preloadKeywordsFromList(appId, dataList);
            
            // 处理每个数据项
            for (Object item : dataList) {
                if (item == null) {
                    continue;
                }
                
                @SuppressWarnings("unchecked")
                Map<String, Object> dataItem = (Map<String, Object>) item;
                
                String dt = getStringValue(dataItem, "dt");
                @SuppressWarnings("unchecked")
                Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
                
                if (dt == null || pr == null) {
                    continue;
                }
                
                // 根据 dt 类型路由
                routeByDtFromMap(ctx, out, dt, appId, platform, pr, ip, ipResult, ua, uaResult, 
                          business, eqidKeywords, msgSt, deviceMd5);
            }
            
        } catch (Exception e) {
            errorCount.incrementAndGet();
            LOG.error("Error processing message", e);
        }
    }
    
    // ============ Map 工具方法 ============
    
    private String getStringValue(Map<String, Object> map, String key) {
        return getStringValue(map, key, null);
    }
    
    private String getStringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        return String.valueOf(value);
    }
    
    private Integer getIntValue(Map<String, Object> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    private Long getLongValue(Map<String, Object> map, String key, Long defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    /**
     * 根据 dt 类型路由数据 (使用 Map)
     */
    private void routeByDtFromMap(Context ctx, Collector<EventAttrRow> out, String dt,
                           Integer appId, Integer platform, Map<String, Object> pr,
                           String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                           String business, Map<String, String> eqidKeywords,
                           Long msgSt, String deviceMd5) {
        
        switch (dt) {
            case "zgid":
                // 用户映射表
                UserRow userRow = userTransfer.transferFromMap(appId, platform, pr);
                if (userRow != null) {
                    ctx.output(USER_OUTPUT, userRow);
                    userCount.incrementAndGet();
                }
                break;
                
            case "pl":
                // 设备表
                DeviceRow deviceRow = deviceTransfer.transferFromMap(appId, platform, pr, msgSt, deviceMd5);
                if (deviceRow != null) {
                    ctx.output(DEVICE_OUTPUT, deviceRow);
                    deviceCount.incrementAndGet();
                }
                break;
                
            case "usr":
                // 用户属性表 (可能产生多条)
                List<UserPropertyRow> propRows = userPropertyTransfer.transferFromMap(appId, platform, pr);
                for (UserPropertyRow propRow : propRows) {
                    ctx.output(USER_PROPERTY_OUTPUT, propRow);
                    userPropertyCount.incrementAndGet();
                }
                break;
                
            case "evt":
            case "vtl":
            case "mkt":
            case "ss":
            case "se":
            case "abp":
                // 事件属性表
                EventAttrRow eventRow = eventAttrTransfer.transferFromMap(
                        appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords);
                if (eventRow != null) {
                    out.collect(eventRow);
                    eventAttrCount.incrementAndGet();
                }
                break;
                
            default:
                unknownCount.incrementAndGet();
                break;
        }
    }
    
    /**
     * 预加载百度关键词 (使用 List)
     */
    private Map<String, String> preloadKeywordsFromList(Integer appId, List<?> dataList) {
        Map<String, String> result = new HashMap<>();
        
        // 只对白名单应用启用
        if (!whiteAppIds.contains(String.valueOf(appId))) {
            return result;
        }
        
        Set<String> eqids = new HashSet<>();
        
        for (Object item : dataList) {
            if (item == null) {
                continue;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> dataItem = (Map<String, Object>) item;
            
            String dt = getStringValue(dataItem, "dt");
            if (dt == null || !keywordService.shouldExtractKeyword(dt)) {
                continue;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
            if (pr == null) {
                continue;
            }
            
            // 检查是否已有 utm_term
            String utmTerm = getStringValue(pr, "$utm_term");
            if (utmTerm != null && !utmTerm.isEmpty() && !"\\N".equals(utmTerm)) {
                continue;
            }
            
            // 提取 eqid
            String ref = getStringValue(pr, "$ref");
            String eqid = keywordService.extractEqid(ref);
            if (eqid != null) {
                eqids.add(eqid);
                // 回写 eqid 到 pr
                pr.put("$eqid", eqid);
            }
        }
        
        // 批量查询
        if (!eqids.isEmpty()) {
            try {
                result = keywordService.preloadKeywordsAsync(eqids).get();
            } catch (Exception e) {
                LOG.warn("Failed to preload keywords", e);
            }
        }
        
        return result;
    }
    
    /**
     * 从 Map 提取 IP 解析结果
     */
    @SuppressWarnings("unchecked")
    private String[] extractIpResultFromMap(Map<String, Object> msgData) {
        Object enrichedObj = msgData.get("_enriched");
        if (enrichedObj == null) {
            return new String[]{"\\N", "\\N", "\\N"};
        }
        
        Map<String, Object> enriched = (Map<String, Object>) enrichedObj;
        Object ipInfoObj = enriched.get("ip");
        if (ipInfoObj == null) {
            return new String[]{"\\N", "\\N", "\\N"};
        }
        
        Map<String, Object> ipInfo = (Map<String, Object>) ipInfoObj;
        return new String[]{
                getStringValue(ipInfo, "country", "\\N"),
                getStringValue(ipInfo, "province", "\\N"),
                getStringValue(ipInfo, "city", "\\N")
        };
    }
    
    /**
     * 从 Map 提取 UA 解析结果
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> extractUaResultFromMap(Map<String, Object> msgData) {
        Map<String, String> result = new HashMap<>();
        
        Object enrichedObj = msgData.get("_enriched");
        if (enrichedObj == null) {
            return result;
        }
        
        Map<String, Object> enriched = (Map<String, Object>) enrichedObj;
        Object uaInfoObj = enriched.get("ua");
        if (uaInfoObj == null) {
            return result;
        }
        
        Map<String, Object> uaInfo = (Map<String, Object>) uaInfoObj;
        result.put("os", getStringValue(uaInfo, "os"));
        result.put("os_version", getStringValue(uaInfo, "os_version"));
        result.put("browser", getStringValue(uaInfo, "browser"));
        result.put("browser_version", getStringValue(uaInfo, "browser_version"));
        
        return result;
    }
    
    @Override
    public void close() throws Exception {
        LOG.info("Closing DataRouterOperator - Stats: user={}, device={}, userProperty={}, " +
                 "eventAttr={}, unknown={}, error={}, blacklisted={}",
                userCount.get(), deviceCount.get(), userPropertyCount.get(),
                eventAttrCount.get(), unknownCount.get(), errorCount.get(), blacklistedCount.get());
        
        if (columnService != null) {
            columnService.close();
        }
        
        if (keywordService != null) {
            keywordService.close();
        }
        
        super.close();
    }
    
    // Getters for stats
    public long getUserCount() { return userCount.get(); }
    public long getDeviceCount() { return deviceCount.get(); }
    public long getUserPropertyCount() { return userPropertyCount.get(); }
    public long getEventAttrCount() { return eventAttrCount.get(); }
    public long getUnknownCount() { return unknownCount.get(); }
    public long getErrorCount() { return errorCount.get(); }
}
