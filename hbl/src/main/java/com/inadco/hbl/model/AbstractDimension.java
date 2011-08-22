package com.inadco.hbl.model;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.Dimension;

public abstract class AbstractDimension implements Dimension {

    protected String name;

    public AbstractDimension(String name) {
        super();
        Validate.notNull(name);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

}
