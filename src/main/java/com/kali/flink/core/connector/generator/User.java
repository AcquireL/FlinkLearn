package com.kali.flink.core.connector.generator;

public class User {
    private String userId;
    private String opt;
    private Long ts;

    public User(String userId, String opt, Long ts) {
        this.userId = userId;
        this.opt = opt;
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOpt() {
        return opt;
    }

    public void setOpt(String opt) {
        this.opt = opt;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", opt='" + opt + '\'' +
                ", ts=" + ts +
                '}';
    }
}
