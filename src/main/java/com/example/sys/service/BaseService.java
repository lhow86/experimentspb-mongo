package com.example.sys.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.business.constant.BusinessConstants;
import com.example.sys.db.QueryFactory;
import com.example.sys.db.QueryUtils;
import com.example.sys.exception.EpMongoModuleException;
import com.example.sys.util.JsonUtils;
import com.example.sys.util.ReflectUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

public abstract class BaseService {

    protected final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    MongoTemplate mongoTemplate;

    /**
     * 新增
     */
    public Object createBasicHandler(JSONObject dataMap, Class entityClass) throws EpMongoModuleException {
        Object entity = JsonUtils.beanToBean(dataMap, entityClass);
        mongoTemplate.insert(entity);
        return entity;
    }

    /**
     * Update
     */
    public Object updateBasicHandler(JSONObject dataMap, Class entityClass) throws EpMongoModuleException {
        Object entity;
        if ("class org.bson.types.ObjectId".equals(ReflectUtils.getFieldTypeByFieldName(entityClass, "_id"))) {
            String id = (String) dataMap.remove("_id");
            entity = JsonUtils.beanToBean(dataMap, entityClass);
            ReflectUtils.setFieldValueByFieldName("_id", entity, new ObjectId(id));
        } else {
            entity = JsonUtils.beanToBean(dataMap, entityClass);
        }
        mongoTemplate.save(entity);
        return entity;
    }

    /**
     * Delete
     */
    public Object deleteBasicHandler(JSONArray idList, Class entityClass) throws EpMongoModuleException {
        JSONObject queryMap = new JSONObject();
        queryMap.put("in_id", idList);
        Query query = QueryFactory.createCriteriaQuery(queryMap);
        Object deleteList = mongoTemplate.find(query, entityClass);
        mongoTemplate.remove(query, entityClass);
        return deleteList;
    }

    /**
     * 通用增删改操作
     */
    public JSONObject cudBasicHandler(JSONObject queryMap, Class entityClass) throws EpMongoModuleException {
        if (null == queryMap) {
            queryMap = new JSONObject();
        }
        JSONObject dataMap = new JSONObject();
        try {
            JSONArray insertList = null != queryMap.get("insert") ? queryMap.getJSONArray("insert") : new JSONArray();
            insertList.stream().forEach(insertObj -> {
                createBasicHandler((JSONObject) insertObj, entityClass);
            });
            JSONArray updateList = null != queryMap.get("update") ? queryMap.getJSONArray("update") : new JSONArray();
            updateList.stream().forEach(updateObj -> {
                updateBasicHandler((JSONObject) updateObj, entityClass);
            });
            JSON delete = (JSON) queryMap.get("delete");
            if (null != delete) {
                if (delete instanceof JSONArray) {
                    JSONObject deleteMap = new JSONObject();
                    deleteMap.put("in_id", delete);
                    Query query = QueryFactory.createCriteriaQuery(deleteMap);
                    mongoTemplate.remove(query, entityClass);
                } else if (delete instanceof JSONObject) {
                    ((JSONObject) delete).keySet().stream().forEach(key -> {
                        JSONArray deleteList = ((JSONObject) delete).getJSONArray(key);
                        JSONObject deleteMap = new JSONObject();
                        deleteMap.put("in_" + key, deleteList);
                        Query query = QueryFactory.createCriteriaQuery(deleteMap);
                        mongoTemplate.remove(query, entityClass);
                    });
                }
            }
            dataMap.put("success", true);
            return dataMap;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
    }

    /**
     * 通用单页查询操作
     */
    public JSONObject queryBasicPage(JSONObject requestMap, Class entityClass) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            int offset = requestMap.get("offset") != null ? (int) requestMap.remove("offset") : 1;
            int limit = requestMap.get("limit") != null ? (int) requestMap.remove("limit") : 10;
            Query query = QueryFactory.createCriteriaQuery(requestMap);
            //设置起始数
            query.skip((long) (offset - 1) * (long) limit);
            //设置查询条数
            query.limit(limit);
            List<Object> entityList = mongoTemplate.find(query, entityClass);
            long total = mongoTemplate.count(query, entityClass);
            result.put("total", total);
            result.put("rows", entityList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 通用列表查询操作
     */
    public JSONObject queryBasicList(JSONObject queryMap, Class entityClass) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            List<Object> entityList;
            if (null == queryMap.get("groupBy")) {
                Query query = QueryFactory.createCriteriaQuery(queryMap);
                entityList = mongoTemplate.find(query, entityClass);
            } else {
                Map<String, Criteria> criteriaMap = QueryFactory.createCriteriaMap(queryMap);
                Criteria[] criteriaArray = QueryFactory.createCriteriaArray(criteriaMap);
                String[] groupKeys = queryMap.getString("groupBy").split(",");
                String collectionName = ((org.springframework.data.mongodb.core.mapping.Document) entityClass.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class)).collection();
                GroupByResults<Document> groupByResults = mongoTemplate.group(
                        criteriaArray.length > 0 ? new Criteria().andOperator(criteriaArray) : null,
                        collectionName,
                        GroupBy.key(groupKeys).initialDocument("{ count: 0 }").reduceFunction("function(doc, prev) { prev.count += 1 }"),
                        Document.class);
                entityList = (List) groupByResults.getRawResults().get("retval");
            }
            result.put("total", entityList.size());
            result.put("rows", entityList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 通过单条查询操作
     */
    public JSONObject getBasic(JSONObject queryMap, Class entityClass) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            if (queryMap.keySet().size() == 0) {
                throw new EpMongoModuleException(BusinessConstants.ERROR_CODE101);
            }
            Query query = new Query();
            for (String key : queryMap.keySet()) {
                Criteria criteria = Criteria.where(key).is(queryMap.get(key));
                query.addCriteria(criteria);
            }
            Object entityObject = mongoTemplate.findOne(query, entityClass);
            result.put("entity", entityObject);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 基础查询记录
     *
     * @column 查询行
     * @value 过滤值
     */
    public JSONObject getBasicFilter(String column, Object value, Class entityClass) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            if (null == value) {
                throw new EpMongoModuleException(BusinessConstants.ERROR_CODE101);
            }
            Query query = new Query();
            Criteria criteria = Criteria.where(column).is(value);
            query.addCriteria(criteria);
            Object entityObject = mongoTemplate.find(query, entityClass);
            result.put("rows", entityObject);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 查询获取单条记录
     *
     * @param id
     * @param entityClass
     * @param <T>
     * @return
     * @throws EpMongoModuleException
     */
    public <T> T getBasicEntity(Object id, Class<T> entityClass) throws EpMongoModuleException {
        return getBasicEntity(id, "id", entityClass);
    }

    /**
     * 查询获取单条记录
     *
     * @param value       过滤值
     * @param column      查询行
     * @param entityClass
     * @param <T>
     * @return
     * @throws EpMongoModuleException
     */
    public <T> T getBasicEntity(Object value, String column, Class<T> entityClass) throws EpMongoModuleException {
        Query query = new Query();
        try {
            if (null == value) {
                throw new EpMongoModuleException(BusinessConstants.ERROR_CODE101);
            }
            Criteria criteria = Criteria.where(column).is(value);
            query.addCriteria(criteria);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return mongoTemplate.findOne(query, entityClass);
    }

    /**
     * 通用增删改操作（Mongo原生操作）
     *
     * @param queryMap
     * @param collectionName
     * @return
     * @throws EpMongoModuleException
     */
    public JSONObject cudBasicHandlerMongoOrigin(JSONObject queryMap, String collectionName) throws EpMongoModuleException {
        if (null == queryMap) {
            queryMap = new JSONObject();
        }
        JSONObject dataMap = new JSONObject();
        try {
            JSONArray insertList = null != queryMap.get("insert") ? queryMap.getJSONArray("insert") : new JSONArray();
            insertList.stream().forEach(insertObj -> {
                ((JSONObject) insertObj).remove("_id");
                mongoTemplate.insert(insertObj, collectionName);
            });
            JSON delete = (JSON) queryMap.get("delete");
            if (null != delete) {
                if (delete instanceof JSONArray) {
                    Query query = QueryFactory.originDeleteMapHandler((JSONArray) delete, "_id");
                    mongoTemplate.remove(query, collectionName);
                } else if (delete instanceof JSONObject) {
                    ((JSONObject) delete).keySet().stream().forEach(key -> {
                        Query query = QueryFactory.originDeleteMapHandler(((JSONObject) delete).getJSONArray(key), key);
                        mongoTemplate.remove(query, collectionName);
                    });
                }
            }
            JSONArray updateList = null != queryMap.get("update") ? queryMap.getJSONArray("update") : new JSONArray();
            JSONObject finalQueryMap = queryMap;
            updateList.stream().forEach(updateObj -> {
                if (null != ((JSONObject) updateObj).get("_id") && !"String".equals(finalQueryMap.getString("IdExpand"))) {
                    ((JSONObject) updateObj).put("_id", new ObjectId((String) ((JSONObject) updateObj).remove("_id")));
                }
                mongoTemplate.save(updateObj, collectionName);
            });
            dataMap.put("success", true);
            return dataMap;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
    }

    /**
     * 通用单页查询操作（Mongo原生操作）
     *
     * @param queryMap
     * @param collectionName
     * @return
     * @throws EpMongoModuleException
     */
    public JSONObject queryBasicPageMongoOrigin(JSONObject queryMap, String collectionName) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            int offset = queryMap.get("offset") != null ? (int) queryMap.remove("offset") : 1;
            int limit = queryMap.get("limit") != null ? (int) queryMap.remove("limit") : 10;
            List entityList;
            if (null == queryMap.get("groupBy")) {
                FindIterable<Document> findIterable = QueryFactory.createFindIterable(mongoTemplate, queryMap, collectionName);
                Query query = QueryFactory.createCriteriaQuery(queryMap);
                Long count = mongoTemplate.count(query, collectionName);
                findIterable.skip((offset - 1) * limit);
                findIterable.limit(limit);
                entityList = QueryUtils.findIterable2List(findIterable);
                result.put("total", count);
            } else {
                List<AggregationOperation> operations = QueryFactory.createAggregationOperations(queryMap);
                operations.add(Aggregation.skip((long) (offset - 1) * (long) limit));
                operations.add(Aggregation.limit((long) limit));
                AggregationResults<Document> aggregationResults = mongoTemplate.aggregate(Aggregation.newAggregation(operations), collectionName, Document.class);
                entityList = aggregationResults.getMappedResults();
            }
            result.put("rows", entityList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 通用列表查询操作（Mongo原生操作）
     *
     * @param queryMap
     * @param collectionName
     * @return
     * @throws EpMongoModuleException
     */
    public JSONObject queryBasicListMongoOrigin(JSONObject queryMap, String collectionName) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            List entityList;
            if (null == queryMap.get("groupBy")) {
                FindIterable<Document> findIterable = QueryFactory.createFindIterable(mongoTemplate, queryMap, collectionName);
                entityList = QueryUtils.findIterable2List(findIterable);
            } else {
                List<AggregationOperation> operations = QueryFactory.createAggregationOperations(queryMap);
                AggregationResults<Document> aggregationResults = mongoTemplate.aggregate(Aggregation.newAggregation(operations), collectionName, Document.class);
                entityList = aggregationResults.getMappedResults();
            }
            result.put("total", entityList.size());
            result.put("rows", entityList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

    /**
     * 通用单条查询操作（Mongo原生操作）
     *
     * @param queryMap
     * @param collectionName
     * @return
     * @throws EpMongoModuleException
     */
    public JSONObject getBasicMongoOrigin(JSONObject queryMap, String collectionName) throws EpMongoModuleException {
        JSONObject result = new JSONObject();
        try {
            if (null == queryMap.get("_id")) {
                throw new EpMongoModuleException(BusinessConstants.ERROR_CODE101);
            }
            FindIterable<Document> findIterable = mongoTemplate.getCollection(collectionName).find();
            findIterable.filter(new BasicDBObject("_id", new ObjectId(queryMap.getString("_id"))));
            findIterable.limit(1);
            result.put("entity", findIterable.iterator().next());
        } catch (EpMongoModuleException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new EpMongoModuleException(BusinessConstants.ERROR_CODE001, e.getMessage());
        }
        return result;
    }

}
