package com.mysql.jdbc.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author hjx
 */
public class SecurityUtil {
    /**
     * 使用seed加密密码
     * @param password 密码
     * @param seed seed
     * @return byte
     */
    public static byte[] scramble411(String password, String seed,String encoding) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] passwordHashStage1 = md.digest(password.getBytes(StandardCharsets.UTF_8));
        md.reset();
        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();
        byte[] seedAsBytes = seed.getBytes(encoding);
        md.update(seedAsBytes);
        md.update(passwordHashStage2);
        byte[] result = md.digest();
        int numToXor = result.length;
        for (int i = 0; i < numToXor; i++) {
            result[i] = (byte) (result[i] ^ passwordHashStage1[i]);
        }
        return result;
    }

    /**
     * 使用mysql协议中323协议加密密码
     * @param password
     * @param seed
     * @return
     */
    public static String newCrypt(String password, String seed) {
        byte b;
        double d;

        if ((password == null) || (password.length() == 0)) {
            return password;
        }

        long[] pw = newHash(seed);
        long[] msg = newHash(password);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);

        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }

        return new String(chars);
    }

    static long[] newHash(String password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (int i = 0; i < password.length(); ++i) {
            if ((password.charAt(i) == ' ') || (password.charAt(i) == '\t')) {
                continue; // skip spaces
            }

            tmp = (0xff & password.charAt(i));
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
    }
}
