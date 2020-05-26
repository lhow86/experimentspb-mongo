package com.example.sys.model;

import com.example.sys.constant.SysConstants;
import com.example.sys.dto.ErrorCodeI;
import com.example.sys.dto.Response;

public class ApiResult extends Response {

    private String sign;
    private Object data;

    public ApiResult(){}
    private ApiResult(String flag, String errorCode, String errorInfo, String sign, Object data) {
        this.flag = flag;
        this.errorCode = errorCode;
        this.errorInfo = errorInfo;
        this.sign = sign;
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    public static ApiResult T(){
        return newInstance(SysConstants.FLAG_T , "", "", null);
    }

    public static ApiResult T(Object data){
        return newInstance(SysConstants.FLAG_T , "", "", data);
    }

    public static ApiResult T(String sign, Object data){
        return newInstance(SysConstants.FLAG_T , "", "", sign, data);
    }

    public static ApiResult F(){
        return newInstance(SysConstants.FLAG_F, "", "", null);
    }

    public static ApiResult F(String errorCode, String errorInfo){
        return newInstance(SysConstants.FLAG_F, errorCode, errorInfo, null);
    }

    public static ApiResult F(ErrorCodeI errorCodeI, String errorInfo){
        return newInstance(SysConstants.FLAG_F, errorCodeI.getErrorCode(), errorInfo, null);
    }

    public static ApiResult F(ErrorCodeI errorCodeI){
        return newInstance(SysConstants.FLAG_F, errorCodeI.getErrorCode(), errorCodeI.getErrorInfo(), null);
    }

    public static ApiResult newInstance(String flag, ErrorCodeI errorCodeI){
        return newInstance(flag, errorCodeI.getErrorCode(), errorCodeI.getErrorInfo(), null);
    }

    public static ApiResult newInstance(String flag, String errorCode, String errorInfo){
        return newInstance(flag, errorCode, errorInfo, null);
    }

    public static ApiResult newInstance(String flag, String errorCode, String errorInfo, Object data){
        ApiResult apiResult = new ApiResult(flag, errorCode, errorInfo, "", data );
        return apiResult;
    }

    public static ApiResult newInstance(String flag, String errorCode, String errorInfo, String sign, Object data){
        ApiResult apiResult = new ApiResult(flag, errorCode, errorInfo, sign, data );
        return apiResult;
    }

}
