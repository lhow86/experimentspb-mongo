package com.example.sys.db;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

public class QueryFactory {

    private static final Log log = LogFactory.getLog(QueryFactory.class);

    public static Query createCriteriaQuery(JSONObject queryMap) {
        Query query = new Query();
        if (null != queryMap.get("selectKeys")) {
            String[] keys = queryMap.remove("selectKeys").toString().split(",");
            for (String key : keys) {
                query.fields().include(key);
            }
        }
        if (null != queryMap.get("orderBy")) {
            query.with(createSortList((String)queryMap.remove("orderBy")));
        }
        Map<String, Criteria> criteriaMap = createCriteriaMap(queryMap);
        for (Criteria criteria : criteriaMap.values()) {
            query.addCriteria(criteria);
        }
        return query;
    }

    /**
     * 创建查询所用的Criteria集合（Map存储）
     * @param queryMap
     * @return
     */
    public static Map<String, Criteria> createCriteriaMap(JSONObject queryMap) {
        Map<String, Criteria> criteriaMap = new HashMap();
        List<String> caseIgnoreList = new ArrayList<>();
        if (null != queryMap.get("caseIgnore")) {
            caseIgnoreList = Arrays.asList(queryMap.getString("caseIgnore").split(","));
        }
        for(String key: queryMap.keySet()) {
            String[] word = key.split("_");
            String fieldName = QueryUtils.fieldNameSplicing(word);
        }
        return criteriaMap;
    }

    /**
     * 创建排序对象
     */
    public static Sort createSortList(String orderBy) {
        String[] orderStrSplit = orderBy.split(",");
        List<Sort.Order> orders = new ArrayList<>();
        for (int i = 0; i < orderStrSplit.length; i = i + 2) {
            Sort.Direction direction = "desc".equals(orderStrSplit[i + 1]) ? Sort.Direction.DESC : Sort.Direction.ASC;
            Sort.Order order = new Sort.Order(direction, orderStrSplit[i]);
            orders.add(order);
        }
        return Sort.by(orders);
    }

    /**
     * 创建排序对象
     *
     * @param orderBy
     * @param baseField
     */
    public static Sort createSortList(String orderBy, String baseField) {
        String[] orderStrSplit = orderBy.split(",");
        List<Sort.Order> orders = new ArrayList<>();
        for (int i = 0; i < orderStrSplit.length; i = i + 2) {
            Sort.Direction direction = "desc".equals(orderStrSplit[i + 1]) ? Sort.Direction.DESC : Sort.Direction.ASC;
            Sort.Order order = new Sort.Order(direction, baseField + orderStrSplit[i]);
            orders.add(order);
        }
        return Sort.by(orders);
    }

}
