package com.example.sys.exception;

import com.example.sys.dto.ErrorCodeI;

public class EasiBusinessException extends BaseException {


    public EasiBusinessException(String code, String message) {
        super(code, message);
    }

    public EasiBusinessException(String code, String message, Throwable throwble) {
        super(code, message, throwble);
    }

    public EasiBusinessException(ErrorCodeI errorCodeI) {
        super(errorCodeI);
    }

    public EasiBusinessException(ErrorCodeI errorCodeI, String message) {
        super(errorCodeI, message);
    }

}
