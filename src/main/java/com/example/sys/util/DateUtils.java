package com.example.sys.util;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class DateUtils extends org.apache.commons.lang3.time.DateUtils {

    private static Log log = LogFactory.getLog(DateUtils.class);
    public static final String PATTERN_FULL_TIME = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_MIDUM_TIME = "yyyy-MM-dd";
    public static final String SQL_FULL_TIME = "yyyy-MM-dd hh24:mi:ss";

    public DateUtils() {
    }

    public static Date datePlus(Date oldDate , int i){
        Calendar c = Calendar.getInstance();
        c.setTime(oldDate);
        // 今天+i天
        c.add(Calendar.DAY_OF_MONTH, i);

        Date newDate = c.getTime();
        return newDate;
    }

    /**
     * 获得当前时间指定格式的字符串
     */
    public static String getString(String format) {
        return dateToString(new Date(),format);
    }

    /**
     * 获得指定时间指定格式的字符串
     */
    public static String getString(Date d , String format) {
        return dateToString(d,format);
    }

    /**
     * 获得当前时间默认格式(yyyy-MM-dd)的字符串
     */
    public static String getDateString() {
        return dateToString(new Date(),PATTERN_MIDUM_TIME);
    }

    /**
     * 获得指定时间默认格式(yyyy-MM-dd)的字符串
     */
    public static String getDateString(Date d) {
        return dateToString(d,PATTERN_MIDUM_TIME);
    }

    /**
     * 获得当前时间默认格式(yyyy-MM-dd HH:mm:ss)的字符串
     */
    public static String getTimeString() {
        return dateToString(new Date(),PATTERN_FULL_TIME);
    }
    /**
     * 获得指定时间默认格式(yyyy-MM-dd HH:mm:ss)的字符串
     */
    public static String getTimeString(Date d) {
        return dateToString(d,PATTERN_FULL_TIME);
    }

    /**
     * 获取今天的起始日期
     */
    public static Date getStartTimeToday () {
        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.SECOND, 0);
        todayStart.set(Calendar.MILLISECOND, 0);
        return todayStart.getTime();
    }

    /**
     * 将指定的日期字符串转换为日期
     */
    public static Date stringToDate(String dateStr){
        try {
            return parseDate(dateStr, new String[]{PATTERN_FULL_TIME,PATTERN_MIDUM_TIME});
        } catch (ParseException e) {
            log.error(e.getMessage(),e);
        }
        return null;
    }

    /**
     * 将指定的日期字符串转换为日期
     */
    public static Date stringToDate(String dateStr , String[] parsePatterns){
        try {
            return parseDate(dateStr, parsePatterns);
        } catch (ParseException e) {
            log.error(e.getMessage(),e);
        }
        return null;
    }

    /**
     * 获得指定时间指定格式的字符串
     */
    public static String dateToString(Date d,String format){
        return FastDateFormat.getInstance(format).format(d);
    }

    /**
     * 转换为网页显示格式
     */
    public static String toSimpleDateString(Date d) {
        if (d == null) {
            return null;
        }
        long old = d.getTime();
        long now = System.currentTimeMillis();
        int m = (int) ((now - old) / org.apache.commons.lang3.time.DateUtils.MILLIS_PER_MINUTE);
        if (m < 60) {
            return (m == 0 ? 1 : m) + "分钟前";
        }
        int h = (int) ((now - old) / org.apache.commons.lang3.time.DateUtils.MILLIS_PER_HOUR);
        if (h < 24) {
            return h + "小时前";
        }
        int day = (int) ((now - old) / org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY);
        if (day <= 3) {
            return day + "天前";
        }
        return dateToString(d,"yyyy-MM-dd HH:mm");
    }

    /**
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws ParseException{
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN_FULL_TIME);
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

    /**
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN_FULL_TIME);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    /**
     * 把传入的字符串日期转换成 yyyy-MM-dd HH:mm:ss
     */
    public static String formatDate(String dateStr) {
        Map<String, String> dateRegFormat = new HashMap<>();
        //2014年3月12日 13时5分34秒，2014-03-12 12:05:34，2014/3/12 12:5:34
        dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$", "yyyy-MM-dd-HH-mm-ss");
        //2014-03-12 12:05
        dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd-HH-mm");
        //2014-03-12 12
        dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd-HH");
        //2014-03-12
        dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd");
        //2014-03
        dateRegFormat.put("^\\d{4}\\D+\\d{2}$", "yyyy-MM");
        //2014
        dateRegFormat.put("^\\d{4}$", "yyyy");
        //20140312120534
        dateRegFormat.put("^\\d{14}$", "yyyyMMddHHmmss");
        //201403121205
        dateRegFormat.put("^\\d{12}$", "yyyyMMddHHmm");
        //2014031212
        dateRegFormat.put("^\\d{10}$", "yyyyMMddHH");
        //20140312
        dateRegFormat.put("^\\d{8}$", "yyyyMMdd");
        //201403
        dateRegFormat.put("^\\d{6}$", "yyyyMM");
        //13:05:34 拼接当前日期
        dateRegFormat.put("^\\d{2}\\s*:\\s*\\d{2}\\s*:\\s*\\d{2}$", "yyyy-MM-dd-HH-mm-ss");
        //13:05 拼接当前日期
        dateRegFormat.put("^\\d{2}\\s*:\\s*\\d{2}$", "yyyy-MM-dd-HH-mm");
        //14.10.18(年.月.日)
        dateRegFormat.put("^\\d{2}\\D+\\d{1,2}\\D+\\d{1,2}$", "yy-MM-dd");
        //30.12(日.月) 拼接当前年份
        dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}$", "yyyy-dd-MM");
        //12.21.2013(日.月.年)
        dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{4}$", "dd-MM-yyyy");
        String curDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        DateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat formatter2;
        String dateReplace;
        String strSuccess = "";
        try {
            for (String key : dateRegFormat.keySet()) {
                if (Pattern.compile(key).matcher(dateStr).matches()) {
                    formatter2 = new SimpleDateFormat(dateRegFormat.get(key));
                    if ("^\\d{2}\\s*:\\s*\\d{2}\\s*:\\s*\\d{2}$".equals(key)
                            || "^\\d{2}\\s*:\\s*\\d{2}$".equals(key)) {
                        //13:05:34 或 13:05 拼接当前日期
                        dateStr = curDate + "-" + dateStr;
                    } else if ("^\\d{1,2}\\D+\\d{1,2}$".equals(key)) {
                        //21.1 (日.月) 拼接当前年份
                        dateStr = curDate.substring(0, 4) + "-" + dateStr;
                    }
                    dateReplace = dateStr.replaceAll("\\D+", "-");
                    strSuccess = formatter1.format(formatter2.parse(dateReplace));
                    break;
                }
            }
        } catch (Exception e) {
            log.error("-----------------日期格式无效:" + dateStr);
        }
        return strSuccess;
    }

    /**
     *  判断传入的日期字符串的 DateFormat 格式
     */
    public static String stringToFormat(String dateStr) {
        Map<String, String> dateRegFormat = new HashMap<>();
        //2014-03-12 12:05:34
        dateRegFormat.put("^\\d{4}\\-+\\d{1,2}\\-+\\d{1,2}\\D+\\d{1,2}\\:+\\d{1,2}\\:+\\d{1,2}\\:*$","yyyy-MM-dd HH:mm:ss");
        //2014-03-12
        dateRegFormat.put("^\\d{4}\\-+\\d{1,2}\\-+\\d{1,2}","yyyy-MM-dd");
        String format = "";
        try {
            for (String key : dateRegFormat.keySet()) {
                if (Pattern.compile(key).matcher(dateStr).matches()) {
                    format = dateRegFormat.get(key);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("-----------------日期格式无效:" + dateStr);
        }
        return format;
    }

}
