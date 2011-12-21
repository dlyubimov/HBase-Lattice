package com.inadco.hbl.hblquery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * collect all the errors to report at a later point
 * @author dmitriy
 *
 */
public final class ErrorAccumulator implements IErrorReporter {
    
    private List<String> errors = new ArrayList<String>();
    private List<String> unmodifiableErrors = Collections.unmodifiableList(errors);

    public ErrorAccumulator() {
        super();
    }

    @Override
    public void reportError(String error) {
        errors.add(error);
    }
    
    @Override
    public void reset() {
        errors.clear();
    }

    public List<String> getErrors() { 
        return unmodifiableErrors;
    }
    
    public String formatErrors () { 
        StringBuffer sbAllErrors = new StringBuffer("Syntax errors present in hbl query.:\n");
        for (String errStr : errors )
            sbAllErrors.append(errStr + "\n");
        return sbAllErrors.toString();

    }

}
