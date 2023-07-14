package com.example.flinkdemo.pojo;

import java.io.Serializable;
import java.sql.Timestamp;

public class UserBehavior implements Serializable {
    private static final long serialVersionUID = 1L;
    private long userId;
    private long itemId;
    private Integer categoryId;
    private String behavior;
    private long ts;

    public UserBehavior() {
    }

    public UserBehavior(long userId, long itemId, Integer categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.ts = timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long timestamp) {
        this.ts = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + ts +
                '}';
    }
}
