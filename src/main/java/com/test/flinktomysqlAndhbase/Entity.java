package com.test.flinktomysqlAndhbase;
/**
 * 实体类封装
 * create by xiax.xpu on @Date 2019/4/13 12:14
 */
public class Entity {
    public String phoneName;
    public String os;
    public String city;
    public String loginTime;

    public Entity() {
    }

    public Entity(String phoneName, String os, String city, String loginTime) {
        this.phoneName = phoneName;
        this.os = os;
        this.city = city;
        this.loginTime = loginTime;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "phoneName='" + phoneName + '\'' +
                ", os='" + os + '\'' +
                ", city='" + city + '\'' +
                ", loginTime='" + loginTime + '\'' +
                '}';
    }

    public String getPhoneName() {
        return phoneName;
    }

    public void setPhoneName(String phoneName) {
        this.phoneName = phoneName;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(String loginTime) {
        this.loginTime = loginTime;
    }
}