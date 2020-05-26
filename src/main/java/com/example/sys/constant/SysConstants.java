package com.example.sys.constant;

import java.lang.reflect.Field;

public class SysConstants {

    // Redis 命名空间
    public static final String REDIS_USERID_NAMESPACE = "userId:";
    public static final String REDIS_RT_NAMESPACE = "rt:";
    public static final String REDIS_CAPTCHA_NAMESPACE = "captcha:";

    public static final int REDIS_OUT_TIME_5 = 5 * 60;	// 5分钟
    public static final int REDIS_OUT_TIME_30 = 1800;	// 30分钟
    public static final int REDIS_OUT_TIME = 1296000;
    public static final int REDIS_TIMEOUT = 864000;		//10天

    public static final String FLAG_T = "T";
    public static final String FLAG_F = "F";

    public static final String ERROR_CODE_EXAMPLE = "EXAMPLE";
    public static final String ERROR_INFO_EXAMPLE = "错误代码样例";

    public static final String OAUTH_USER_INFO_EXAMPLE = "{\"flag\": \"T\",\"info\": {\"attrs\": {\"UserloginId\": \"U_3111985C2X74x\",\"Usermobile\": \"15821936270\",\"OrgorgCode\": \"SHISC0001\",\"Systemid\": 125022,\"Orgid\": 105703,\"Usertele\": \"64824627\",\"Userid\": 1501989,\"UserloginDate\": \"2019-09-18 11:22:22\",\"UserlastIp\": \"192.168.12.99\",\"SystemsubSystemCode\": \"SWGD\",\"Usermail\": \"625083130@qq.com\",\"Username\": \"jxren\",\"Orgname\": \"国航网站标准用户\",},\"roles\": [{\"subsystemId\": 125022,\"flag\": \"0\",\"roleCode\": \"Default-App-User-SWGD\",\"name\": \"海关瘦客户端默认角色\",\"towinxportal\": \"Y\",\"id\": 237463}],\"permits\": [{\"subsysId\": 125022,\"code\": \"SWGD_QUERY\",\"flag\": \"0\",\"name\": \"信天翁查询\",\"meno\": null,\"id\": 539985,\"url\": \"/**\"},{\"subsysId\": 125022,\"code\": \"SWGD_testPerm\",\"flag\": \"0\",\"name\": \"信天翁oauth测试用权限\",\"meno\": \"信天翁oauth测试用权限\",\"id\": 539822,\"url\": \"/*\"}]}}";

    /**
     * 手机号码正则表达式
     */
    public static final String MOBILE_VERIFY_REGEX = "^1[\\d]{10}$";


    public static String showInfo(String code) {
        String info = null;
        try{
            Class clazz = Class.forName("com.example.sys.constant.SysConstants");
            Field field = clazz.getField("ERROR_INFO_"+code);
            info = (String) field.get(clazz);
        }catch (Exception e){
        }
        return info;
    }

}

