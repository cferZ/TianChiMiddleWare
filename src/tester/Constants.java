package tester;

public class Constants {

    public final static String STORE_PATH = System.getProperty("store.path", "data.txt");
    public final static int PRO_NUM = Integer.valueOf(System.getProperty("pro.num", "20"));
    public final static int CON_NUM = Integer.valueOf(System.getProperty("con.num", "20"));
    public final static String PRO_PRE = System.getProperty("pro.pre","PRODUCER_");
    public final static int PRO_MAX = Integer.valueOf(System.getProperty("pro.max","20000"));
    public final static String CON_PRE = System.getProperty("con.pre", "CONSUMER_");
    public final static String TOPIC_PRE = System.getProperty("topic.pre", "TOPIC_");
    public final static String QUEUE_PRE = System.getProperty("topic.pre", "QUEUE_");


}
