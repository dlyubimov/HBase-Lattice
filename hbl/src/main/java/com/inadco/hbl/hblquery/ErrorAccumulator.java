/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
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
