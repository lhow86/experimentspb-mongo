package com.example.business.constant;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

@Slf4j
public class BusinessConstants {

    public static final String FLAG_T = "T";
    public static final String FLAG_F = "F";

    public static String showInfo(String code) {
        String info = null;
        try{
            Class clazz = Class.forName("com.example.business.constant.BusinessConstants");
            Field field = clazz.getField("ERROR_INFO_"+code);
            info = (String) field.get(clazz);
        }catch (Exception e){
            log.error(e.getMessage() , e);
        }
        return info;
    }
}
