package storm;

public class Studet {
    public static void main(String[] args) {
        Long a=100000l;
        Long b=100L;
        int c=100;
        int d=100000;
        System.out.println(b==c);
        System.out.println(a==d);
        System.out.println(a.equals(d));
        System.out.println(b.equals(c));
    }
}