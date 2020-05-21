package com.example.sys.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils extends org.apache.commons.lang3.StringUtils {

    private static Log log = LogFactory.getLog(StringUtils.class);

    private static final String[] CHARSET_ARRAY = {
            "utf-8", "gbk", "gb2312", "iso-8859-1"
    };

    public static final String BLANK = "";

    public static final String DEFAULT_CHARSET = "utf-8";

    public static final String DOT = ".";

    public static String firstCharUpperCase(String src) {
        if (Character.isUpperCase(src.charAt(0)))
            return src;
        else {
            StringBuilder sb = new StringBuilder();
            sb.append(Character.toUpperCase(src.charAt(0))).append(
                    src.substring(1));
            return sb.toString();
        }
    }

    public static String[] match(String src, String reg) {
        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(src);
        List<String> l = new ArrayList<String>();
        while (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                l.add(m.group(i));
            }
        }
        return l.toArray(new String[]{});
    }

    public static String[] matchAll(String src, String reg) {
        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(src);
        List<String> l = new ArrayList<String>();
        while (m.find()) {
            l.add(m.group());
        }
        return l.toArray(new String[]{});
    }

    public static String stringtify(Reader in) {
        try {
            StringBuilder sb = new StringBuilder();
            String line = null;
            BufferedReader br = new BufferedReader(in);
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            return e.getMessage();
        }
    }

    public static String stringtify(InputStream in) {
        return stringtify(in, DEFAULT_CHARSET);
    }

    public static String stringtify(InputStream in, String charset) {
        try {
            InputStreamReader isr = new InputStreamReader(in, charset);
            return stringtify(isr);
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * 字符串前补零
     *
     * @param src       源
     * @param padLength 字符格式化位数
     * @return 返回补零后的结果
     */
    public static String lpad(String src, Integer padLength) {
        int length = padLength - src.length();
        for (int i = 0; i < length; i++) {
            src = "0" + src;
        }
        return src;
    }

    /**
     * 比较是否含有给定字符串
     *
     * @param src 源
     * @param reg 正则
     * @return
     */
    public static boolean matches(String src, String reg) {
        return Pattern.compile(reg).matcher(src).find();
    }

    //随机生成6位验证码
    public static String genRandomPsd(int length) {
        SecureRandom rad = new SecureRandom();
        String result = rad.nextInt(1000000) + "";
        if (result.length() != length) {
            return genRandomPsd(length);
        }
        return result;
    }

    public static String valueOf(Object obj) {
        if (obj != null) {
            return obj.toString();
        }
        return "";
    }

    public static boolean isEmpty(Object str) {
        if (str == null || valueOf(str).length() == 0) {
            return true;
        } else {
            return false;
        }
    }


    public static void chartSetHint(String oldStr) {
        try {
            for (String chartSetLeft : CHARSET_ARRAY) {
                for (String chartSetRight : CHARSET_ARRAY) {
                    log.info("new String(oldStr.getBytes(\"" + chartSetLeft + "\"),\"" + chartSetRight + "\")");
                    log.info(new String(oldStr.getBytes(chartSetLeft), chartSetRight));
                }
                log.info("new String(oldStr.getBytes(\"" + chartSetLeft + "\"))");
                log.info(new String(oldStr.getBytes(chartSetLeft)));
                log.info("new String(oldStr.getBytes(),\"" + chartSetLeft + "\")");
                log.info(new String(oldStr.getBytes(), chartSetLeft));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
