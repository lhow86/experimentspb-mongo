package com.example.sys.constant;

import com.example.sys.dto.ErrorCodeI;

public enum SysErrorCode implements ErrorCodeI {

    /**
     * 未知错误，请稍后再试
     */
    ERROR_CODE001("001" , "未知错误，请稍后再试"),

    /**
     *请求参数有误或缺失
     */
    ERROR_CODE101("101" , "请求参数有误或缺失"),

    /**
     *图形验证码未输入
     */
    ERROR_CODE201("201" , "图形验证码未输入"),

    /**
     *图形验证码输入有误或已失效
     */
    ERROR_CODE202("202" , "图形验证码输入有误或已失效"),

    /**
     *验签参数未传入
     */
    ERROR_CODE203("203" , "验签参数未传入"),

    /**
     *验签失败
     */
    ERROR_CODE204("204" , "验签失败"),

    /**
     *ip白名单验证失败
     */
    ERROR_CODE205("205" , "ip白名单验证失败"),

    /**
     *登录凭据为空
     */
    ERROR_CODE301("301" , "登录凭据为空"),

    /**
     *用户登录凭据有误或失效，请重新登录
     */
    ERROR_CODE302("302" , "用户登录凭据有误或失效，请重新登录"),

    /**
     *未获得网关中的用户信息
     */
    ERROR_CODE303("303" , "未获得网关中的用户信息"),

    /**
     *未配置该接口权限
     */
    ERROR_CODE304("304" , "未配置该接口权限"),

    /**
     *OAUTH的异常信息
     */
    ERROR_CODE305("305" , "OAUTH的异常信息"),

    /**
     *限流
     */
    ERROR_CODE401("401" , "流量超过限制，请稍后再试"),

    /**
     *熔断
     */
    ERROR_CODE402("402" , "请求被熔断，请稍后再试"),

    /**
     *网络异常，请稍后再试
     */
    ERROR_CODE901("901" , "网络异常，请稍后再试"),

    /**
     *服务器异常
     */
    ERROR_CODE902("902" , "服务器异常"),

    /**
     *数据库异常
     */
    ERROR_CODE903("903" , "数据库异常"),

    /**
     *999
     */
    ERROR_CODE999("999" , "999"),

    /**
     *未注册到指令中心
     */
    ERROR_CODE_CommandHubRegistError("CommandHubRegistError" , "未注册到指令中心"),

    /**
     *未注册到事件中心
     */
    ERROR_CODE_EventHubRegistError("EventHubRegistError" , "未注册到事件中心"),

    /**
     *找不到扩展
     */
    ERROR_CODE_ExtensionNotFound("ExtensionNotFound" , "找不到扩展"),

    /**
     *业务场景为空
     */
    ERROR_CODE_BizScenarioIsNull("BizScenarioIsNull" , "业务场景为空"),

    /**
     *Spring组件未找到
     */
    ERROR_CODE_SpringComponentNotFound("SpringComponentNotFound" , "Spring组件未找到"),

    /**
     *执行者未找到
     */
    ERROR_CODE_ExecutorNotFound("ExecutorNotFound" , "执行者未找到"),


    /**
     *执行方法至少需要一个参数
     */
    ERROR_CODE_ExecuteMethodHasNoParameter("ExecuteMethodHasNoParameter" , "执行方法至少需要一个参数"),

    /**
     *执行者未继承Command
     */
    ERROR_CODE_ExecutorNotInheritCommand("ExecutorNotInheritCommand" , "执行者未继承Command"),

    /**
     *执行者未继承Event
     */
    ERROR_CODE_ExecutorNotInheritEvent("ExecutorNotInheritEvent" , "执行者未继承Event"),

    /**
     *未指定扫描路径
     */
    ERROR_CODE_CommandPackagesNotSpecified("CommandPackagesNotSpecified" , "未指定扫描路径"),

    /**
     *未找到该事件的执行方法
     */
    ERROR_CODE_EventExecuteMethodNotFound("EventExecuteMethodNotFound" , "未找到该事件的执行方法"),

    /**
     *扩展坐标不能重复
     */
    ERROR_CODE_ExtensionCoordinateDuplicateError("ExtensionCoordinateDuplicateError" , "扩展坐标不能重复"),

    /**
     *扩展类未实现接口
     */
    ERROR_CODE_ExtensionImplementNoInterface("ExtensionPointImplementNoInterface" , "扩展类未实现扩展接口"),

    /**
     *扩展类名有误
     */
    ERROR_CODE_ExtensionPointNameNotValid("ExtensionPointNameNotValid" , "扩展类名有误"),

    ;

    private String errorCode;
    private String errorInfo;

    SysErrorCode(String errorCode , String errorInfo) {
        this.errorCode = errorCode;
        this.errorInfo = errorInfo;
    }

    @Override
    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String getErrorInfo() {
        return errorInfo;
    }

}

