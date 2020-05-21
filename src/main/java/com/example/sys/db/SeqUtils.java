package com.example.sys.db;

import com.example.sys.entity.SequenceId;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

public class SeqUtils {

    /**
     * 获取下一个自增ID
     *
     * @param collName
     * @param mongoTemplate
     * @return
     */
    public static Long getNextId(String collName, MongoTemplate mongoTemplate) {
        Query query = new Query(Criteria.where("collName").is(collName));
        Update update = new Update();
        update.inc("seqId", 1);
        FindAndModifyOptions options = new FindAndModifyOptions();
        options.upsert(true);
        options.returnNew(true);
        SequenceId seqId = mongoTemplate.findAndModify(query, update, options, SequenceId.class);
        return seqId.getSeqId();
    }

    public static String getNextIdChar(String collName, String head, Integer min, MongoTemplate mongoTemplate) {
        StringBuilder rlsId = new StringBuilder(String.valueOf(getNextId(collName, mongoTemplate)));
        Integer len = rlsId.length();
        if (len < min) {
            for (int i = 0; i < min - len; i++) {
                rlsId.insert(0, "0");
            }
        }
        return rlsId.insert(0, head).toString();
    }

    /**
     * 获取当前自增ID
     * @param collName
     * @param mongoTemplate
     * @return
     */
    public static Long getNowId(String collName, MongoTemplate mongoTemplate) {
        Query query = new Query(Criteria.where("collName").is(collName));
        SequenceId seqId = mongoTemplate.findOne(query, SequenceId.class);
        return seqId.getSeqId();
    }

}
