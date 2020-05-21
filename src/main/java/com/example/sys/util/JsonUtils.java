package com.example.sys.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonUtils {

    private static Log log = LogFactory.getLog(JsonUtils.class);

    private final static String JSON_SEPERATOR = "%JS%";

    /**
     * 序列化的时候的特性
     *   DisableCircularReferenceDetect 消除对同一对象循环引用的问题，默认为false
     *   SortField 按字段名称排序后输出。默认为false
     *   WriteDateUseDateFormat 全局修改日期格式,默认为false。
     */
    private static SerializerFeature[] serializerFeatures = {
            SerializerFeature.DisableCircularReferenceDetect,
            SerializerFeature.WriteDateUseDateFormat
    };

    /**
     * 反序列化的时候的特性
     *   OrderedField 属性保持原来的顺序
     */
    private static Feature[] features = {
            Feature.OrderedField
    };

    public static SerializerFeature[] getSerializerFeatures() {
        return serializerFeatures;
    }

    public static void setSerializerFeatures(SerializerFeature[] serializerFeatures) {
        JsonUtils.serializerFeatures = serializerFeatures;
    }

    public static Feature[] getFeatures() {
        return features;
    }

    public static void setFeatures(Feature[] features) {
        JsonUtils.features = features;
    }

    public static JSONObject jsonToBean(String json) {
        try {
            return JSON.parseObject(json, features);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> T jsonToBean(String json, Class<T> clazz) {
        try {
            return JSON.parseObject(json, clazz, features);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> String beanToJson(T entity) {
        try {
            return JSON.toJSONString(entity, serializerFeatures);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static JSONObject beanToBean(Object entity) {
        try {
            return jsonToBean(beanToJson(entity));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> T beanToBean(Object entity, Class<T> clazz) {
        try {
            return jsonToBean(beanToJson(entity) , clazz);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static JSONArray jsonToArray(String json) {
        try {
            return JSON.parseArray(json);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> List<T> jsonToArray(String json, Class<T> clazz) {
        try {
            return JSON.parseArray(json , clazz);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static JSONArray beanToArray(Object entity){
        try{
            String jsonString = beanToJson(entity);
            return jsonToArray(jsonString);
        }catch (Exception e){
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> List<T> beanToArray(Object entity, Class<T> clazz){
        try{
            String jsonString = beanToJson(entity);
            return jsonToArray(jsonString , clazz);
        }catch (Exception e){
            log.error(e.getMessage(), e);
            return null;
        }
    }



    public static <T> T jsonToBeanWithException(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz, features);
    }

    public static <T> String beanToJsonWithNullValue(T entity) {
        try {
            List<SerializerFeature> serializerFeatures = new ArrayList<>(Arrays.asList(JsonUtils.serializerFeatures));
            serializerFeatures.add(SerializerFeature.WriteMapNullValue);
            String jsonString = JSON.toJSONString(entity, serializerFeatures.toArray(new SerializerFeature[]{}));
            return "null".equals(jsonString) ? null : jsonString;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static JSON filePath2JSON(String filePath){
        try(InputStream inputStream = JsonUtils.class.getClassLoader().getResourceAsStream(filePath)){
            JSON jsonExpectedValue = (JSON) JSON.parse(IOUtils.toString(inputStream,"utf-8"));
            return jsonExpectedValue;
        }catch (Exception e){
            log.error(e.getMessage() , e);
            return null;
        }
    }

    public static Object searchNode(JSON json , String nodeName){
        if(json instanceof JSONArray){
            if(((JSONArray)json).size() > 0){
                Object target = ((JSONArray)json).get(0);
                if(target instanceof JSON){
                    return searchNode((JSON) target, nodeName);
                }else {
                    return null;
                }
            }else {
                return null;
            }
        }else if(json instanceof JSONObject){
            if(null == json){
                return null;
            }
            List<String> nodes = new ArrayList<>(Arrays.asList(nodeName.split(JSON_SEPERATOR)));
            String realNodeName = nodes.remove(0);
            Object target = ((JSONObject)json).get(realNodeName);
            if(target instanceof JSON && nodes.size() > 0){
                return searchNode((JSON) target, StringUtils.join(nodes , JSON_SEPERATOR));
            }else {
                return target;
            }
        }else {
            return null;
        }
    }

}
