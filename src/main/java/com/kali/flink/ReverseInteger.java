package com.kali.flink;


public class ReverseInteger {
    public static void main(String[] agrs) {

        System.out.println(getRes(-123));
    }

    /**
     * @param tergetInt 需反转的整数
     * @return 1.tergetInt%10后剩下的就是个位数上的值此时，此时加上res*10一起赋值给res
     * 2.将terget/10后返回给terget，相当于去掉最后一位。
     * 3.一直循环直到tergetInt为0
     * 4.对比返回的res值是否在int区间内 -2147483648～2147483647，在则返回，不再则返回0
     */
    public static long getRes(int tergetInt) {
        long res = 0;
        while (tergetInt != 0) {
            res = res * 10 + tergetInt % 10;
            printA();
            tergetInt /= 10;

        }
        return (res > 2147483647 || res < -2147483648) ? 0 : res;

    }

    public static void  printA(){
        System.out.println("Hello");
    }

}

