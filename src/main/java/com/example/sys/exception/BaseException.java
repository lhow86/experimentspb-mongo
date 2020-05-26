package com.example.sys.exception;

import com.example.sys.dto.ErrorCodeI;

public abstract class BaseException extends RuntimeException {

    /**
     * <p>
     * Field code: 异常代码
     * </p>
     */
    protected String errorCode;

    /**
     *
     * @param code    异常代码
     * @param message 异常信息
     */
    public BaseException(String code, String message) {
        super(message);
        this.errorCode = code;
    }

    public BaseException(String code, String message, Throwable throwble) {
        super(message, throwble);
        this.errorCode = code;
    }

    public BaseException(ErrorCodeI errorCodeI) {
        super(errorCodeI.getErrorInfo());
        this.errorCode = errorCodeI.getErrorCode();
    }

    public BaseException(ErrorCodeI errorCodeI, String message) {
        super(message);
        this.errorCode = errorCodeI.getErrorCode();
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. " , this.errorCode , this.getMessage());
    }
}
