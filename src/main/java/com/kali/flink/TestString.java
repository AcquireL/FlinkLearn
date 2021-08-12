package com.kali.flink;

public class TestString {
    public static void main(String[] args) {
        String s = "abc";
        String s1 = new String("abc");
        String s2 = "abc";
        String s3 = null;
        String s4 = new String();
        String s5 =new String("abc");
        String s6 = null;
        System.out.println(s.equals(s1));
        System.out.println(s==s1);
        System.out.println(s==s2);
        System.out.println(s3);
        System.out.println(s4);
        System.out.println(s4.equals(s3));
        System.out.println(s3==s4);
        System.out.println(s1.equals(s5));
        System.out.println(s1==s5);
        System.out.println(s3==s6);
        System.out.println(s3==null);
        System.out.println(s4.equals(""));
        System.out.println("".equals(s3));
    }
}
