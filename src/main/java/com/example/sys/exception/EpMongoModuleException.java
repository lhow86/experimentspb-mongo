package com.example.sys.exception;

import com.example.business.constant.BusinessConstants;
import com.example.sys.dto.ErrorCodeI;

public class EpMongoModuleException extends BaseException {

    public EpMongoModuleException(String code, String message) {
        super(code , message);
    }

    public EpMongoModuleException(String code) {
        super(code , BusinessConstants.showInfo(code));
    }

    public EpMongoModuleException(String code, String message, Throwable throwble) {
        super(code , message, throwble);
    }

    public EpMongoModuleException(ErrorCodeI dbErrorCode) {
        super(dbErrorCode);
    }

}
