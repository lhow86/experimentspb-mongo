package com.example.sys.db;

import com.alibaba.fastjson.JSONArray;
import com.example.sys.util.DateUtils;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Query工厂类工具类
 */
public class QueryUtils {

    private static final Log log = LogFactory.getLog(QueryUtils.class);

    /**
     * 驼峰字符串换成下划线字符串
     * @param s
     * @return
     */
    public static String replaceWithLine(String s) {
        String[] strings = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
        for (String string : strings) {
            s = s.replace(string, "_" + string);
        }
        return s.toUpperCase();
    }

    /**
     * 字符串转时间对象 用于between
     * @param o
     * @param dayOfMonthOffset
     * @return
     */
    public static Object changeStringToDate(Object o, int dayOfMonthOffset) {
        Calendar c = Calendar.getInstance();
        if (o instanceof String) {
            try {
                String format = DateUtils.stringToFormat(o.toString());
                if ("yyyy-MM-dd HH:mm:ss".equals(format)) {
                    return new SimpleDateFormat(format).parse(o.toString());
                } else if ("yyyy-MM-dd".equals(format)) {
                    Date sDate = new SimpleDateFormat(format).parse(o.toString());
                    c.setTime(sDate);
                    c.add(Calendar.DAY_OF_MONTH, dayOfMonthOffset);
                    return c.getTime();
                } else {
                    return o;
                }
            } catch (ParseException pe) {
                log.error("日期转换失败");
                return null;
            }
        } else {
            return o;
        }
    }

    public static Object[] objectArrayChange(Object o) {
        if (o instanceof int[]) {
            int[] ints = (int[]) o;
            Object[] objs = new Object[ints.length];
            for (int i = 0; i < ints.length; i++) {
                objs[i] = ints[i];
            }
            return objs;
        } else if (o instanceof String[]) {
            String[] strings = (String[]) o;
            Object[] objs = new Object[strings.length];
            for (int i = 0; i < strings.length; i++) {
                objs[i] = "'" + strings[i] + "'";
            }
            return objs;
        } else if (o instanceof List) {
            List list2 = new ArrayList();
            Iterator it = ((List) o).iterator();
            while (it.hasNext()) {
                Object obj = it.next();
                if (obj instanceof String) {
                    list2.add("'" + obj + "'");
                }
            }
            return list2.toArray();
        }
        return ((List) o).toArray();
    }

    public static JSONArray findIterable2List(FindIterable<Document> findIterable) {
        JSONArray queryForList = new JSONArray();
        findIterable.forEach((Block<? super Document>) (final Document document) -> {
            queryForList.add(document);
        });
        return queryForList;
    }

    public static Document findIterable2Document(FindIterable<Document> findIterable) {
        Document document = new Document();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            document = mongoCursor.next();
        }
        return document;
    }

    public static String fieldNameSplicing(String[] word) {
        String fieldName = "";
        for (int i = 1; i < word.length; i++) {
            if ("".equals(word[i])) {
                //有空说明条件字段中含有下划线（默认处理为前下划线、中间的下划线和后面的下划线用转义符号代替）
                fieldName += "_";
            } else {
                if (i == word.length - 1) {
                    fieldName += word[i];
                } else {
                    fieldName += word[i] + ".";
                }
                if (fieldName.contains("%underline%")) {
                    //下划线的转义符
                    fieldName = fieldName.replace("%underline%", "_");
                }
            }
        }
        return fieldName;
    }

    // 过滤项转正则
    public static String filterList2RegexString(JSONArray filterList) {
        StringBuffer regex = new StringBuffer();
        filterList.stream().forEach(filter -> {
            regex.append(filter + "|");
        });
        return regex.substring(0, regex.length() - 1);
    }

    // 大小写正则
    public static void regexCaseIgnore(Criteria criteria, String regexValue, Boolean isIgnore) {
        if (isIgnore) {
            criteria.regex(regexValue, "i");
        } else {
            criteria.regex(regexValue);
        }
    }

}
