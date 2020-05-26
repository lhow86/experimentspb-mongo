package com.example.sys.config;

import com.example.sys.constant.SysConstants;
import com.example.sys.constant.SysErrorCode;
import com.example.sys.exception.BaseException;
import com.example.sys.model.ApiResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice(annotations = {RestController.class , Controller.class})
@ResponseBody
@Slf4j
public class GlobalRestExceptionHandler {

    @Autowired
    HttpServletRequest request;



    @ExceptionHandler(value = com.example.sys.exception.BaseException.class)
    public ApiResult easiBaseSysExceptionHandler(Exception e) {
        ApiResult result = ApiResult.newInstance(SysConstants.FLAG_F , ((BaseException) e).getErrorCode() , e.getMessage());
        return result;
    }


    @ExceptionHandler(value = org.springframework.web.client.RestClientException.class)
    public ApiResult restClientExceptionHandler(Exception e) {
        ApiResult result = ApiResult.newInstance(SysConstants.FLAG_F, SysErrorCode.ERROR_CODE902);
        return result;
    }

    @ExceptionHandler(value = Exception.class)
    public ApiResult exceptionHandler(Exception e) {
        log.error(e.getMessage() , e);
        ApiResult result = ApiResult.newInstance(SysConstants.FLAG_F, SysErrorCode.ERROR_CODE999.getErrorCode(), e.getMessage());
        return result;
    }

}
