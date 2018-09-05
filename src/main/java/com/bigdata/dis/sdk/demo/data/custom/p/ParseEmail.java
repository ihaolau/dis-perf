package com.bigdata.dis.sdk.demo.data.custom.p;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseEmail implements IData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseEmail.class);

    //定义的全局变量
    public static List allEmailPath = new ArrayList<String>();
    public static List emailListGlobal = new ArrayList<String>();
    public static List subjectListGlobal = new ArrayList<String>();
    public static List emailBodyListGlobal = new ArrayList<String>();

    private static String FILE_DIR = Constants.DIS_CONFIG.get("producer_data_dir", "data/");

    public ParseEmail() {
        init();
    }

    public static void init() {
        long start = System.currentTimeMillis();
        emailListGlobal = jsonToList(FILE_DIR + "emailsJson.json", "key");
        emailBodyListGlobal = jsonToList(FILE_DIR + "bodysJson.json", "key");
        subjectListGlobal = jsonToList(FILE_DIR + "subjectsJson.json", "key");
        LOGGER.info("{} init cost {}ms", ParseEmail.class.getName(), (System.currentTimeMillis() - start));
    }

    public static void main(String args[]) throws Exception {
        Date start = new Date();
        SimpleDateFormat matter = new SimpleDateFormat("现在时间:yyyy年MM月dd日E HH时mm分ss秒");
        System.out.println(matter.format(start));

//        String originalPath = "D:\\enron_mail_20150507.tar\\maildir";
//        getAllEmailPath(originalPath);
//        getAllEmailAddressesSubContent(allEmailPath)

        init();
        long time = System.currentTimeMillis();
        System.out.println("Start Time in Milliseconds: " + time);

        for (int i = 0; i < 100000; i++) {
            createEmail();
        }
        System.out.println("Cost: " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        System.out.println("Start Time in Milliseconds: " + time);

        for (int i = 0; i < 100000; i++) {
            createEmail();
        }
        System.out.println("Cost: " + (System.currentTimeMillis() - time));
    }


    //获取所有Email文件的详细地址
    public static void getAllEmailPath(String path) {
        File file = new File(path);        //获取其file对象
        File[] fs = file.listFiles();    //遍历path下的文件和目录，放在File数组中
        for (File f : fs) {                    //遍历File[]数组
            if (f.isDirectory())        //若非目录(即文件)，则打印
                getAllEmailPath(f.getPath());
            else if (!f.getPath().endsWith(".")) {
                allEmailPath.add(f.getAbsolutePath());
            }
        }
    }

    //匹配Email地址
    public static void fillEmailsHashSet(String line, HashSet<String> container) {
        Pattern p = Pattern.compile("([\\w\\-]([\\.\\w])+[\\w]+@([\\w\\-]+\\.)+[A-Za-z]{2,4})");
        Matcher m = p.matcher(line);
        while (m.find()) {
            container.add(m.group(1));
        }
    }

    //匹配主题
    public static void fillSubjectsHashSet(String line, HashSet<String> container) {
        Pattern p = Pattern.compile(Pattern.quote("Subject:") + "[\\s\\S]*" + Pattern.quote("Mime-Version"));
        Matcher m = p.matcher(line);
        String content;
        while (m.find()) {
            content = m.group();
            content = content.replace("Subject:", "");
            content = content.replace("Mime-Version", "");
            content = content.replace("\r\n", "");
            container.add(content);
        }
    }

    //匹配邮件内容
    public static void fillEmailBodysHashSet(String line, HashSet<String> container) {
        String regEx = "X-FileName[\\s\\S]*\r\n";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(line);
        while (m.find()) {
            String regExIn = "\r\n[\\s\\S]*";
            Pattern pIn = Pattern.compile(regExIn);
            Matcher mIn = pIn.matcher(m.group(0));
            while (mIn.find()) {
                container.add(mIn.group(0));
            }
        }
    }

    //把本地json文件一次加载进内存，转成list以供后续程序使用
    public static ArrayList<String> jsonToList(String subjectsJsonPath, String prefix) {
        /*JSONArray subjectsJa = new JSONArray();
        subjectsJa.put(subjectsMap);
        String subjectsJson = subjectsJsonPath;
        BufferedWriter subjectsOut = new BufferedWriter(new FileWriter(subjectsJson));
        subjectsOut.write(subjectsJa.toString());
        subjectsOut.close();*/
        //将磁盘上的json数据一次加载进内存，然后保存进list以供后面使用
        String subjectsStr = null;
        try {
            subjectsStr = new String(Files.readAllBytes(Paths.get(subjectsJsonPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HashMap<String, String> hashMap = JsonUtils.jsonToObj(subjectsStr.replace("[", "").replace("]", ""), HashMap.class);
        Collection<String> values = hashMap.values();
        return new ArrayList<>(values);
    }

    //把map转成json存到本地磁盘
    public static void mapToJsonToDisk(Map<String, String> subjectsMap, String subjectsJsonPath) throws Exception {
//        JSONArray subjectsJa = new JSONArray();
//        subjectsJa.put(subjectsMap);
//        String subjectsJson = subjectsJsonPath;
//        BufferedWriter subjectsOut = new BufferedWriter(new FileWriter(subjectsJson));
//        subjectsOut.write(subjectsJa.toString());
//        subjectsOut.close();
    }

    //解析每个email文件获取获取所有email地址，主题，内容，形成资源池
    public static void getAllEmailAddressesSubContent(List<String> emailPaths) throws Exception {
        List<String> emailList = new ArrayList<>();
        List<String> subjectList = new ArrayList<>();
        List<String> emailBodyList = new ArrayList<>();
        String emailPath;
        for (int i = 0; i < emailPaths.size(); i++) {
            emailPath = emailPaths.get(i);
            HashSet<String> hs = new HashSet<>();
            HashSet<String> hsSubjects = new HashSet<>();
            HashSet<String> hsEmailBody = new HashSet<>();
            String content = new String(Files.readAllBytes(Paths.get(emailPath)));

            fillEmailBodysHashSet(content, hsEmailBody);
            fillSubjectsHashSet(content, hsSubjects);
            fillEmailsHashSet(content, hs);

            for (String string : hs) {
                if (!emailList.contains(string)) {
                    emailList.add(string);
                }
            }

            for (String string : hsSubjects) {
                if (!subjectList.contains(string)) {
                    subjectList.add(string);
                }
            }

            for (String string : hsEmailBody) {
                if (!emailBodyList.contains(string)) {
                    emailBodyList.add(string);
                }
            }
        }

        //将enron中所有的subject保存在一个list里面，然后转换成Map，最终以json的形式保存在磁盘上。
        Map<String, String> subjectsMap = new HashMap<>();
        for (int i = 0; i < subjectList.size(); i++) {
            subjectsMap.put("key" + i, subjectList.get(i));
        }

        mapToJsonToDisk(subjectsMap, "D:\\enron_mail_20150507.tar\\subjectsJson.json");
        //subjectListGlobal = jsonToList("D:\\enron_mail_20150507.tar\\subjectsJson.json","key");

        Map<String, String> emailBodysMap = new HashMap<>();
        for (int i = 0; i < emailBodyList.size(); i++) {
            emailBodysMap.put("key" + i, emailBodyList.get(i));
        }

        mapToJsonToDisk(emailBodysMap, "D:\\enron_mail_20150507.tar\\bodysJson.json");
        //emailBodyListGlobal = jsonToList("D:\\enron_mail_20150507.tar\\bodysJson.json","key");

        Map<String, String> emailsMap = new HashMap<>();
        for (int i = 0; i < emailList.size(); i++) {
            emailsMap.put("key" + i, emailList.get(i));
        }
        mapToJsonToDisk(emailsMap, "D:\\enron_mail_20150507.tar\\emailsJson.json");
        //emailListGlobal = jsonToList("D:\\enron_mail_20150507.tar\\emailsJson.json","key");
    }

    //获取message_id
    public static String getMsgId() {
        long first = Math.abs(ThreadLocalRandom.current().nextLong());
        long second = Math.abs(ThreadLocalRandom.current().nextLong());
//        return first + "." + second + ".JavaMail.evans@thyme";

        StringBuilder sb = sbs.get();
        sb.setLength(0);
        sb.append(first).append(".").append(second).append(".JavaMail.evans@thyme");
        return sb.toString();
    }

    //获取邮件发送时间
    public static String getEmailSendTime(String start, String end) {
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date start_p = dateFormat.parse(start);
//        Date end_p = dateFormat.parse(end);
//        long delta_mills = end_p.getTime() - start_p.getTime();
//        float delta = new Random().nextFloat();
//        long date = start_p.getTime() + (long) (delta_mills * delta);
//        String dateString = new Date(date).toString();
//        return dateString;
        // promote 1s
        return new Date().toString();
    }

    //随机获取list中的值
    public static String getRandomList(List<String> list) {
        int index = Math.abs(ThreadLocalRandom.current().nextInt(list.size()));
        return list.get(index);
    }

    static ThreadLocal<StringBuilder> sbs = new ThreadLocal<StringBuilder>() {
        protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    //随机获取list中N个值
    public static String getNRecodes(List<String> list, int n) {
//        String result = "";
//        for (int i = 0; i < n; i++) {
//            result = result + "," + getRandomList(list);
//        }
//        return result.substring(1, result.length());
        if (n == 1) {
            return getRandomList(list);
        }
        String[] temp = new String[n];
        for (int i = 0; i < n; i++) {
            temp[i] = getRandomList(list);
        }
        return String.join(",", temp);
    }

    //生成最终的Email文件
    public static String createEmail() {
        //邮箱地址
        String message_id = getMsgId();
        String send_time = getEmailSendTime("2017-09-09 09:09:09", "2018-09-09 09:09:09");
        String from_address = getNRecodes(emailListGlobal, 1);
        String to_addresses = getNRecodes(emailListGlobal, 5);
        String subject = getNRecodes(subjectListGlobal, 1);
        String cc_addresses = getNRecodes(emailListGlobal, 5);
        String bcc_addresses = getNRecodes(emailListGlobal, 5);
        String email_content = getNRecodes(emailBodyListGlobal, 1);

        StringBuilder sb = sbs.get();
        sb.setLength(0);
        sb.append("Message-ID: <").append(message_id).append(">\r\nDate: ").append(send_time)
                .append("\r\nFrom: ").append(from_address)
                .append("\r\nTo: ").append(to_addresses)
                .append("\r\nSubject: ").append(subject)
                .append("\r\nCc: ").append(cc_addresses)
                .append("\r\nMime-Version: 1.0\r\nContent-Type: text/plain; charset=us-ascii\r\nContent-Transfer-Encoding: 7bit\r\nBcc: ").append(bcc_addresses)
                .append("\r\nX-From: Alyse Herasimchuk\r\nX-To: ").append(to_addresses)
                .append("\r\nX-cc: ").append(cc_addresses)
                .append("\r\nX-bcc: ").append(bcc_addresses)
                .append("\r\nX-Folder: \\Phillip_Allen_June2001\\Notes Folders\\Notes inbox\r\nX-Origin: ").append(from_address)
                .append("\r\nX-FileName: pallen.nsf\r\n").append(email_content);

        String email_file = sb.toString();


//        String email_file = "Message-ID: <" + message_id + ">\r\nDate: " +
//                send_time + "\r\nFrom: " + from_address +
//                "\r\nTo: " + to_addresses + "\r\nSubject: " +
//                subject + "\r\nCc: " +
//                cc_addresses + "\r\nMime-Version: 1.0" +
//                "\r\nContent-Type: text/plain; charset=us-ascii" +
//                "\r\nContent-Transfer-Encoding: 7bit" +
//                "\r\nBcc: " +
//                bcc_addresses + "\r\nX-From: Alyse Herasimchuk" +
//                "\r\nX-To: " +
//                to_addresses + "\r\nX-cc: " +
//                cc_addresses + "\r\nX-bcc: " +
//                bcc_addresses + "\r\nX-Folder: \\Phillip_Allen_June2001\\Notes Folders\\Notes inbox" +
//                "\r\nX-Origin: " +
//                from_address + "\r\nX-FileName: pallen.nsf" +
//                "\r\n" + email_content;
        //System.out.println(email_file);

        return email_file;
//        System.out.println(email_file);
    }

    @Override
    public PutRecordsRequest createRequest(String streamName) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(createEmail().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
