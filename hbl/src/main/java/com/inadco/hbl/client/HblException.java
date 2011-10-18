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
package com.inadco.hbl.client;
/**
 * Hbl specific checked exceptions.
 * 
 * @author dmitriy
 *
 */
public class HblException extends Exception {

    private static final long serialVersionUID = 1L;

    public HblException() {
    }

    public HblException(String message) {
        super(message);
    }

    public HblException(Throwable cause) {
        super(cause);
    }

    public HblException(String message, Throwable cause) {
        super(message, cause);
    }

}
