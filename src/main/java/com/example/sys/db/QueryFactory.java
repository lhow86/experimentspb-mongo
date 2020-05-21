package com.example.sys.db;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

import static com.mongodb.client.model.Projections.*;

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
            query.with(createSortList((String) queryMap.remove("orderBy")));
        }
        Map<String, Criteria> criteriaMap = createCriteriaMap(queryMap);
        for (Criteria criteria : criteriaMap.values()) {
            query.addCriteria(criteria);
        }
        return query;
    }

    /**
     * 创建查询所用的Criteria集合（Map存储）
     *
     * @param queryMap
     * @return
     */
    public static Map<String, Criteria> createCriteriaMap(JSONObject queryMap) {
        Map<String, Criteria> criteriaMap = new HashMap();
        List<String> caseIgnoreList = new ArrayList<>();
        if (null != queryMap.get("caseIgnore")) {
            caseIgnoreList = Arrays.asList(queryMap.getString("caseIgnore").split(","));
        }
        for (String key : queryMap.keySet()) {
            String[] word = key.split("_");
            String fieldName = QueryUtils.fieldNameSplicing(word);
            Boolean isCaseIgnore = caseIgnoreList.contains(fieldName);
            Criteria criteria = criteriaMap.containsKey(fieldName) ?
                    criteriaMap.get(fieldName) : Criteria.where(fieldName);
            switch (word[0]) {
                case "eq":
                    if (isCaseIgnore) {
                        criteria.regex("^" + queryMap.get(key) + "$", "i");
                    } else {
                        criteria.is(queryMap.get(key));
                    }
                    break;
                case "isNull":
                    List nullList = new ArrayList();
                    nullList.add("");
                    nullList.add(null);
                    criteria.in(nullList);
                    break;
                case "isNotNull":
                    List notNullList = new ArrayList();
                    notNullList.add("");
                    notNullList.add(null);
                    criteria.nin(notNullList);
                    break;
                case "not":
                    criteria.not();
                    break;
                case "notEq":
                    criteria.ne(queryMap.get(key));
                    break;
                case "in":
                    if (isCaseIgnore) {
                        criteria.regex("^(" + QueryUtils.filterList2RegexString(queryMap.getJSONArray(key)) + ")$", "i");
                    } else {
                        criteria.in(queryMap.getJSONArray(key));
                    }
                    break;
                case "notIn":
                    criteria.nin(queryMap.getJSONArray(key));
                    break;
                case "like":
                    QueryUtils.regexCaseIgnore(criteria, "^" + queryMap.get(key) + "$", isCaseIgnore);
                    break;
                case "psLike":
                    QueryUtils.regexCaseIgnore(criteria, ".*" + queryMap.get(key) + ".*", isCaseIgnore);
                    break;
                case "preLike":
                    QueryUtils.regexCaseIgnore(criteria, ".*" + queryMap.get(key) + "$", isCaseIgnore);
                    break;
                case "suffLike":
                    QueryUtils.regexCaseIgnore(criteria, "^" + queryMap.get(key) + ".*", isCaseIgnore);
                    break;
                case "psLikeIn":
                    QueryUtils.regexCaseIgnore(criteria,
                            ".*(" + QueryUtils.filterList2RegexString(queryMap.getJSONArray(key)) + ").*", isCaseIgnore);
                    break;
                case "preLikeIn":
                    QueryUtils.regexCaseIgnore(criteria,
                            ".*(" + QueryUtils.filterList2RegexString(queryMap.getJSONArray(key)) + ")$", isCaseIgnore);
                    break;
                case "suffLikeIn":
                    QueryUtils.regexCaseIgnore(criteria,
                            "^(" + QueryUtils.filterList2RegexString(queryMap.getJSONArray(key)) + ").*", isCaseIgnore);
                    break;
                case "le":
                    criteria.lte(QueryUtils.changeStringToDate(queryMap.get(key), 0));
                    break;
                case "lt":
                    criteria.lt(QueryUtils.changeStringToDate(queryMap.get(key), 0));
                    break;
                case "ge":
                    criteria.gte(QueryUtils.changeStringToDate(queryMap.get(key), 0));
                    break;
                case "gt":
                    criteria.gt(QueryUtils.changeStringToDate(queryMap.get(key), 0));
                    break;
                case "between":
                    String[] splits = queryMap.getString(key).split(",");
                    String startValue = splits[0];
                    String endValue = splits[1];
                    criteria.gte(QueryUtils.changeStringToDate(startValue, 0)).lte(QueryUtils.changeStringToDate(endValue, 1));
                    break;
                default:
                    break;
            }
            criteriaMap.put(fieldName, criteria);
        }
        return criteriaMap;
    }

    /**
     * 将CriteriaMap转化为CriteriaList  Group By时聚合使用
     *
     * @param criteriaMap
     * @return
     */
    public static Criteria[] createCriteriaArray(Map<String, Criteria> criteriaMap) {
        Criteria[] criteriaArray = new Criteria[criteriaMap.size()];
        int i = 0;
        for (Criteria criteria : criteriaMap.values()) {
            criteriaArray[i++] = criteria;
        }
        return criteriaArray;
    }

    /**
     * 原生批量删除Query
     *
     * @param deleteList
     * @param key
     * @return
     */
    public static Query originDeleteMapHandler(JSONArray deleteList, String key) {
        JSONObject queryMap = new JSONObject();
        if ("_id".equals(key)) {
            List<ObjectId> objectIds = new ArrayList<>();
            for (Object id : deleteList) {
                objectIds.add(new ObjectId((String) id));
            }
            queryMap.put("in__id", objectIds);
        } else {
            queryMap.put("in_" + key, deleteList);
        }
        Query query = QueryFactory.createCriteriaQuery(queryMap);
        return query;
    }

    /**
     * 原生过滤Bson创建
     *
     * @param queryMap
     * @return
     */
    public static BasicDBObject createFilterList(JSONObject queryMap) {
        List<BasicDBObject> filterList = new ArrayList<>();
        for (String key : queryMap.keySet()) {
            String[] word = key.split("_");
            String fieldName = QueryUtils.fieldNameSplicing(word);
            switch (word[0]) {
                case "eq":
                    filterList.add(new BasicDBObject(fieldName, queryMap.get(key)));
                    break;
                case "isNull":
                    List nullList = new ArrayList();
                    nullList.add("");
                    nullList.add(null);
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$in", nullList)));
                    break;
                case "isNotNull":
                    List notNullList = new ArrayList();
                    notNullList.add("");
                    notNullList.add(null);
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$nin", notNullList)));
                    break;
                case "not":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$not", queryMap.get(key))));
                    break;
                case "notEq":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$ne", queryMap.get(key))));
                    break;
                case "notIn":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$nin", queryMap.get(key))));
                    break;
                case "like":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$regex", "^" + queryMap.get(key) + "$")));
                    break;
                case "psLike":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$regex", ".*" + queryMap.get(key) + ".*")));
                    break;
                case "preLike":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$regex", ".*" + queryMap.get(key) + "$")));
                    break;
                case "suffLike":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$regex", "^" + queryMap.get(key) + ".*")));
                    break;
                case "le":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$lte", QueryUtils.changeStringToDate(queryMap.get(key), 0))));
                    break;
                case "lt":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$lt", QueryUtils.changeStringToDate(queryMap.get(key), 0))));
                    break;
                case "ge":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$gte", QueryUtils.changeStringToDate(queryMap.get(key), 0))));
                    break;
                case "gt":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$gt", QueryUtils.changeStringToDate(queryMap.get(key), 0))));
                    break;
                case "in":
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$in", queryMap.get(key))));
                    break;
                case "between":
                    String[] splits = queryMap.getString(key).split(",");
                    String startValue = splits[0];
                    String endValue = splits[1];
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$gte", QueryUtils.changeStringToDate(startValue, 0))));
                    filterList.add(new BasicDBObject(fieldName, new BasicDBObject("$lte", QueryUtils.changeStringToDate(endValue, 1))));
                    break;
                default:
                    break;
            }
        }
        BasicDBObject filter = new BasicDBObject();
        if (filterList.size() > 0) {
            filter.append("$and", filterList);
        }
        return filter;
    }

    /**
     * 原生FindIterable创建 类似于Query
     *
     * @param mongoTemplate
     * @param queryMap
     * @param collectionName
     * @return
     */
    public static FindIterable<Document> createFindIterable(MongoTemplate mongoTemplate, JSONObject queryMap, String collectionName) {
        FindIterable<Document> findIterable;
        if (null != queryMap.get("selectKeys")) {
            String[] keys = queryMap.remove("selectKeys").toString().split(",");
            findIterable = mongoTemplate.getCollection(collectionName).find().projection(fields(include(keys), excludeId()));
        } else {
            findIterable = mongoTemplate.getCollection(collectionName).find();
        }
        if (null != queryMap.get("orderBy")) {
            String[] orderStrSplit = ((String) queryMap.remove("orderBy")).split(",");
            BasicDBObject sortMap = new BasicDBObject();
            for (int i = 0; i < orderStrSplit.length; i = i + 2) {
                Integer direction = "desc".equals(orderStrSplit[i + 1]) ? -1 : 1;
                String fieldName = orderStrSplit[i];
                sortMap.append(fieldName, direction);
            }
            findIterable.sort(sortMap);
        }
        BasicDBObject filter = QueryFactory.createFilterList(queryMap);
        findIterable.filter(filter);
        return findIterable;
    }

    /**
     * 原生AggregationOperation创建 适用于GroupBy聚合
     *
     * @param queryMap
     * @return
     */
    public static List<AggregationOperation> createAggregationOperations(JSONObject queryMap) {
        List<AggregationOperation> operations = new ArrayList<>();
        //加上字符串为了解决单字段聚合显示为_id
        String[] groupKeys = (queryMap.remove("groupBy") + ",AAAA0B1F993697F1").split(",");
        Map<String, Criteria> criteriaMap = QueryFactory.createCriteriaMap(queryMap);
        for (Criteria criteria : criteriaMap.values()) {
            operations.add(Aggregation.match(criteria));
        }
        operations.add(Aggregation.group(groupKeys).count().as("count"));
        return operations;
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
