package com.example.sys.service;

import com.alibaba.fastjson.JSONObject;
import com.example.sys.db.SeqUtils;
import com.example.sys.entity.SeqCharMongo;
import com.example.sys.entity.SeqMongo;
import com.example.sys.util.JsonUtils;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.util.Date;

@Component
public class SaveMongoEventListener extends AbstractMongoEventListener<Object> {

    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void onBeforeConvert(BeforeConvertEvent<Object> event) {
        Object source = event.getSource();
        if (source != null) {
            ReflectionUtils.doWithFields(source.getClass(),
                    (Field field) -> {
                        ReflectionUtils.makeAccessible(field);

                        JSONObject jsonObject = JsonUtils.jsonToBean(JsonUtils.beanToJson(source), JSONObject.class);
                        if (field.isAnnotationPresent(Id.class)) {
                            // 设置自增ID 必须有Id注解
                            Object fieldValue = jsonObject.get(field.getName());
                            if (null == fieldValue || "0".equals(fieldValue.toString())) {
                                // Id字段为空或者0 判断为新增 使用Seq
                                if (field.isAnnotationPresent(SeqMongo.class)) {
                                    // 普通Seq
                                    field.set(source, SeqUtils.getNextId(source.getClass().getSimpleName(), mongoTemplate));
                                } else if (field.isAnnotationPresent(SeqCharMongo.class)) {
                                    // 字符串Seq
                                    SeqCharMongo seqCharMongo = field.getAnnotation(SeqCharMongo.class);
                                    field.set(source, SeqUtils.getNextIdChar(source.getClass().getSimpleName(),
                                            seqCharMongo.head(), seqCharMongo.min(), mongoTemplate));
                                }
                            }
                        }
                        if (field.isAnnotationPresent(CreatedDate.class)) {
                            if (null == jsonObject.get(field.getName())) {
                                field.set(source, new Date());
                            }
                        }
                    }
            );
        }
    }

}
