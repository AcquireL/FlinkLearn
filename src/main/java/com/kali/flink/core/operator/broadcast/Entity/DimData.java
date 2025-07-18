package com.kali.flink.core.operator.broadcast.Entity;

public class DimData {
    private String key;
    private String value;

    public DimData(){}

    public DimData(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DimData{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
