package com.kali.flink.core.operator.window.Entity;

public class WordCountCart {
    private int sen;
    private Integer cardNum;

    public WordCountCart(){}

    public WordCountCart(int sen, Integer cardNum) {
        this.sen = sen;
        this.cardNum = cardNum;
    }

    public int getSen() {
        return sen;
    }

    public void setSen(int sen) {
        this.sen = sen;
    }

    public Integer getCardNum() {
        return cardNum;
    }

    public void setCardNum(Integer cardNum) {
        this.cardNum = cardNum;
    }

    @Override
    public String toString() {
        return "WordCountCart{" +
                "sen=" + sen +
                ", cardNum=" + cardNum +
                '}';
    }
}
