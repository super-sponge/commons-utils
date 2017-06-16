package commons;

import java.util.Random;

/**
 * Created by sponge on 2017/6/14.
 */
public class Utils {
    private static final Random random = new Random();

    public static  int randomInt(int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }

    public static String randString(int length) {
        String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        int charLength = base.length();
        StringBuilder sb = new StringBuilder();
        for(int i =0; i < length; i++){
            sb.append(base.charAt(random.nextInt(charLength)));
        }
        return sb.toString();
    }
}
