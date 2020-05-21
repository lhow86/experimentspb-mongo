package com.example.sys.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ReflectUtils {

    private static final Log log = LogFactory.getLog(ReflectUtils.class);

    public static Object excutePrivateFunc(Object obj, String funcName, Class[] paramClasses, Object[] paramValues) throws Exception {
        Class cls = obj.getClass();
        Method method = cls.getDeclaredMethod(funcName, paramClasses);
        //设置私有方法可以被访问
        method.setAccessible(true);
        //调用私有方法
        return method.invoke(obj, paramValues);
    }

    public static Object getPrivateParam(Object obj, String paramName) throws Exception {
        Class cls = obj.getClass();
        Field field = cls.getDeclaredField(paramName);
        field.setAccessible(true);
        return field.get(obj);
    }

    public static void setPrivateParam(Object obj, String paramName, Object paramValue) throws Exception {
        Class cls = obj.getClass();
        Field field = cls.getDeclaredField(paramName);
        field.setAccessible(true);
        field.set(obj, paramValue);
    }

    public static String getFieldTypeByFieldName(Class clazz, String fieldName) {
        // 获取实体类的所有属性，返回Field数组
        Field[] field = clazz.getDeclaredFields();
        String fieldType = null;
        // 遍历所有属性
        for (int j = 0; j < field.length; j++) {
            // 获取属性的名字
            String name = field[j].getName();
            // 获取属性的类型
            String type = field[j].getGenericType().toString();
            if (fieldName.equals(name)) {
                fieldType = type;
                break;
            }
        }
        return fieldType;
    }

    /**
     * 根据属性名获取属性值
     *
     * @param fieldName
     * @param object
     * @return
     */
    public static String getFieldValueByFieldName(String fieldName, Object object) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            //设置对象的访问权限，保证对private的属性的访问
            field.setAccessible(true);
            return (String) field.get(object);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 根据属性名设置属性值
     *
     * @param fieldName
     * @param object
     * @return
     */
    public static void setFieldValueByFieldName(String fieldName, Object object, Object value) {
        try {
            // 获取obj类的字节文件对象
            Class c = object.getClass();
            // 获取该类的成员变量
            Field f = c.getDeclaredField(fieldName);
            // 取消语言访问检查
            f.setAccessible(true);
            // 给变量赋值
            f.set(object, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
