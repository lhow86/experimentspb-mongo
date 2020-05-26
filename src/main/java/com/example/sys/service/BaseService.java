package com.example.sys.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sys.db.QueryFactory;
import com.example.sys.util.JsonUtils;
import com.example.sys.util.ReflectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
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
    public Object createBasicHandler(JSONObject dataMap, Class entityClass) throws Exception {
        Object entity = JsonUtils.beanToBean(dataMap, entityClass);
        mongoTemplate.insert(entity);
        return entity;
    }

    /**
     * Update
     */
    public Object updateBasicHandler(JSONObject dataMap, Class entityClass) throws Exception {
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
    public Object deleteBasicHandler(JSONArray idList, Class entityClass) throws Exception {
        JSONObject queryMap = new JSONObject();
        queryMap.put("in_id", idList);
        Query query = QueryFactory.createCriteriaQuery(queryMap);
        Object deleteList = mongoTemplate.find(query, entityClass);
        mongoTemplate.remove(query, entityClass);
        return deleteList;
    }

    /**
     * 通用增删改操作 todo
     */
    public JSONObject cudBasicHandler(JSONObject queryMap, Class entityClass) throws Exception{
        if (null == queryMap) {
            queryMap = new JSONObject();
        }
        JSONObject dataMap = new JSONObject();
        try {
            JSONArray insertList = null != queryMap.get("insert") ? queryMap.getJSONArray("insert") : new JSONArray();
            insertList.stream().forEach(insertObj -> {
//                createBasicHandler((JSONObject) insertObj, entityClass);
            });
            JSONArray updateList = null != queryMap.get("update") ? queryMap.getJSONArray("update") : new JSONArray();
            updateList.stream().forEach(updateObj -> {
//                updateBasicHandler((JSONObject) updateObj, entityClass);
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
            throw new Exception(e);
        }
    }

    /**
     * 通用单页查询操作
     */
    public JSONObject queryBasicPage(JSONObject requestMap, Class entityClass) throws Exception {
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
            throw new Exception(e);
        }
        return result;
    }

    /**
     * 通用列表查询操作
     */
    public JSONObject queryBasicList(JSONObject queryMap, Class entityClass) throws Exception {
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
            throw new Exception(e);
        }
        return result;
    }

    /**
     * 通过单条查询操作
     */
    public JSONObject getBasic(JSONObject queryMap, Class entityClass) throws Exception {
        JSONObject result = new JSONObject();
        try {
            if (queryMap.keySet().size() == 0) {

            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new Exception(e);
        }
        return null;
    }

}
