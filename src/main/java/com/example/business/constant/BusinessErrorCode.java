package com.example.business.constant;

import com.example.sys.dto.ErrorCodeI;

public enum BusinessErrorCode implements ErrorCodeI {

    ERROR_CODE001("001" , "未知错误，请稍后再试");

    private String errorCode;
    private String errorInfo;

    BusinessErrorCode(String errorCode , String errorInfo) {
        this.errorCode = errorCode;
        this.errorInfo = errorInfo;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }
}
