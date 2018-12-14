package com.bigdata.dis.sdk.demo.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {

    public static String compress(String str) {

        if (str == null || str.isEmpty()) {

            return str;

        }

        try {

            ByteArrayOutputStream bos = null;

            GZIPOutputStream os = null;

            byte[] bs = null;

            try {

                bos = new ByteArrayOutputStream();

                os = new GZIPOutputStream(bos);

                os.write(str.getBytes());

                os.close();

                bos.close();

                bs = bos.toByteArray();

                return new String(bs, "ISO-8859-1");

            } finally {

                bs = null;

                bos = null;

                os = null;

            }

        } catch (Exception ex) {

            return str;

        }

    }

    public static String decompress(String str) {

        if (str == null || str.isEmpty()) {

            return str;

        }

        ByteArrayInputStream bis = null;

        ByteArrayOutputStream bos = null;

        GZIPInputStream is = null;

        byte[] buf = null;

        try {

            bis = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));

            bos = new ByteArrayOutputStream();

            is = new GZIPInputStream(bis);

            buf = new byte[1024];

            int len = 0;

            while ((len = is.read(buf)) != -1) {

                bos.write(buf, 0, len);

            }

            is.close();

            bis.close();

            bos.close();

            return new String(bos.toByteArray());

        } catch (Exception ex) {
            ex.printStackTrace();
            return str;

        } finally {

            bis = null;

            bos = null;

            is = null;

            buf = null;

        }

    }
}
