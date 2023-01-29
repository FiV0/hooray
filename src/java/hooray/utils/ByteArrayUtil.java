package hooray.utils;

public class ByteArrayUtil {

    public static byte[] increment(byte[] a) {
        for (int i = a.length - 1; i >= 0; --i) {
            if (++a[i] != 0) {
                return a;
            }
        }
        throw new IllegalStateException("Byte Array overflow!!!");
    }

    public static String toString (byte [] a) {
        String res = "";
        for (int i = 0; i < a.length; i++) {
            res = (a [i] == 0 ? "0": "1") + res;
        }
        return res;
    }
}
