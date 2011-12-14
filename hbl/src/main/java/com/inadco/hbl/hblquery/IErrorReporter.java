package com.inadco.hbl.hblquery;

public interface IErrorReporter {
    void reportError(String error);
    void reset();
}
