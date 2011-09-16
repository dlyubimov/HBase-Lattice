package com.inadco.hbl.client.impl;

public class Slice {
    private Object leftBound;
    private Object rightBound;
    private boolean leftOpen;
    private boolean rightOpen;
    
    public Slice(Object leftBound, boolean leftOpen, Object rightBound, boolean rightOpen) {
        super();
        this.leftBound = leftBound;
        this.leftOpen = leftOpen;
        this.rightBound = rightBound;
        this.rightOpen = rightOpen;
    }

    public Object getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(Object leftBound) {
        this.leftBound = leftBound;
    }

    public Object getRightBound() {
        return rightBound;
    }

    public void setRightBound(Object rightBound) {
        this.rightBound = rightBound;
    }

    public boolean isLeftOpen() {
        return leftOpen;
    }

    public void setLeftOpen(boolean leftOpen) {
        this.leftOpen = leftOpen;
    }

    public boolean isRightOpen() {
        return rightOpen;
    }

    public void setRightOpen(boolean rightOpen) {
        this.rightOpen = rightOpen;
    }
    
    
    
    

}
