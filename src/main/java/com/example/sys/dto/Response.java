package com.example.sys.dto;

import com.example.sys.constant.SysConstants;

/**
 * Response to caller
 */
public class Response extends AbstractDto{

    private static final long serialVersionUID = 1L;

    protected String flag;
    protected String errorCode;
    protected String errorInfo;

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
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

    @Override
    public String toString() {
        return "Response{" +
                "flag='" + flag + '\'' +
                ", errorCode='" + errorCode + '\'' +
                ", errorInfo='" + errorInfo + '\'' +
                '}';
    }

    public static Response buildFailure(String errCode, String errMessage) {
        Response response = new Response();
        response.setFlag(SysConstants.FLAG_F);
        response.setErrorCode(errCode);
        response.setErrorInfo(errMessage);
        return response;
    }

    public static Response buildSuccess(){
        Response response = new Response();
        response.setFlag(SysConstants.FLAG_T);
        return response;
    }

}
