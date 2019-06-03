package com.bigdata.dis.sdk.demo.other;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * 输入分区总数，输出每个分区对应的PartitionKey值
 */
public class PartitionKeyMap {
    private static String ALL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // 总分区数
    private static int totalPartitionCount = 20;

    // PartitionKey，用于计算分区ID
    private static String partitionKey = "n";

    public static void main(String[] args) {
        printPartitonId(partitionKey, totalPartitionCount);
//        printPartitionKey(totalPartitionCount);
//        printExplicitHashKey(totalPartitionCount);
    }

    public static void printPartitonId(String partitionKey, int totalPartitionCount) {
        long hash = getHash(partitionKey);
        System.out.println(getPartition(totalPartitionCount, hash));
    }

    public static void printPartitionKey(int totalPartitionCount) {
        for (int i = 1; i <= totalPartitionCount; i++) {
            String[] s = new String[i];
            int prefixIndex = 0;
            String key = null;
            int findCount = 0;
            Integer partition = null;
            while (findCount < i) {
                for (int j = 0; j < ALL.length(); j++) {
                    key = (prefixIndex == 0 ? "" : String.valueOf(ALL.charAt(prefixIndex))) + ALL.charAt(j);
                    long hash = getHash(key);
                    partition = getPartition(i, hash);
                    if (partition < i && s[partition] == null) {
                        s[partition] = key;
                        findCount++;
                    }
                }

                if (findCount == i) {
                    break;
                }

                if (prefixIndex < ALL.length() - 1) {
                    prefixIndex++;
                } else {
                    throw new RuntimeException("Error to find key");
                }
            }
            System.out.println(String.join(",", s));
        }
    }


    public static int getPartition(int total, Long hashKey) {
        long avg = Long.MAX_VALUE / total;

        int activePartitionIndex = (int) (hashKey / avg);
        long mod = hashKey % avg;

        if (activePartitionIndex > 0 && mod == 0) {
            activePartitionIndex = activePartitionIndex - 1;
        }

        return activePartitionIndex;
    }


    public static void printExplicitHashKey(int totalPartitionCount) {
        for (int i = 1; i <= totalPartitionCount; i++) {
            List<String> hashRanges = hashRangeSplitter(0, Long.MAX_VALUE, i);

            StringBuilder sb = new StringBuilder();
            for (String hashRange : hashRanges) {
                sb.append(hashRange).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            System.out.println(sb.toString());
        }
    }

    private static int getRandom(int count) {
        return (int) Math.round(Math.random() * (count));
    }

    private static String getRandomString(int length) {
        StringBuilder sb = new StringBuilder();
        int len = ALL.length();
        for (int i = 0; i < length; i++) {
            sb.append(ALL.charAt(getRandom(len - 1)));
        }
        return sb.toString();
    }

    public static long getHash(String value) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return -1;
        }

        md.update(value.getBytes());
        byte byteData[] = md.digest();

        return getLong(byteData) & Long.MAX_VALUE;
    }

    public static final List<String> hashRangeSplitter(long startHash, long endHash, int numberOfRangeToSplit) {
        List<String> hashRanges = new ArrayList<>();
        long partitonbucketSize = (endHash - startHash) / numberOfRangeToSplit;
        for (int i = 0; i < numberOfRangeToSplit; i++) {
            long startHashCode = startHash + i * partitonbucketSize;
            if (i > 0) {
                startHashCode++;
            }

            long endHashCode = (i < numberOfRangeToSplit - 1) ? startHashCode + partitonbucketSize : endHash;

            hashRanges.add("[" + startHashCode + " : " + endHashCode + "]");
        }

        return hashRanges;
    }

    protected static final long getLong(final byte[] array) {
        final int SHIFTBITS = 8;
        final int NUMBER_HASH_BYTES = 8;

        int totalBytesToConvert = (array.length > NUMBER_HASH_BYTES) ? NUMBER_HASH_BYTES : array.length;

        long value = 0;
        for (int i = 0; i < totalBytesToConvert; i++) {
            value = ((value << SHIFTBITS) | (array[i] & 0xFF));
        }
        return value;
    }
}
