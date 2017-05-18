package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MessageStore {

	private final int LOOPINTERVAL=1000;
	private final int MAXMESSAGELENGTH=256*1024;
	private final String OFFSETFILE="";
    private static final MessageStore INSTANCE = new MessageStore();
     
    private static final HashMap<String,FileChannel> fileHandlers=new HashMap<>();
    
    private static final HashMap<String , Integer> bucketIndex=new HashMap<>();
    
    private Thread loopSaveHandler=null;
    private Thread indexSaveHandler=null;							
    public MessageStore() {
		// TODO Auto-generated constructor stub
    	//TODO 从文件中读queueOffset
    	
    	
    	loopSaveHandler=new Thread(new LoopSave());
    	loopSaveHandler.start();
    	indexSaveHandler=new Thread(new IndexSave());
    	indexSaveHandler.start();
	}

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
    }

    public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
    }
   
   
    private class IndexSave implements Runnable{
	    @Override
		public void run() {
			// TODO Auto-generated method stub
	    	ByteBuffer buf=ByteBuffer.allocate(MAXMESSAGELENGTH);
			try {
				Thread.sleep(LOOPINTERVAL);
				FileChannel f=fileHandlers.get(OFFSETFILE);
				if(f==null){
					//TODO
				}
				else{
					buf.clear();
					buf.put(util.serializationUtil.getByteBuffer((HashMap)queueOffsets));
					f.write(buf);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
   
    private class LoopSave implements Runnable{
    	@Override
    	public void run() {
    		// TODO Auto-generated method stub
			Set<String> keys= messageBuckets.keySet();
			for(String key:keys){
				ArrayList<Message> bucketList=messageBuckets.get(key);
				if (bucketList == null) {
			          continue;
			    }  
			    int offset = bucketIndex.getOrDefault(key, 0);
			    if (offset >= bucketList.size()) {
			        continue;
			    }
			    Message message = bucketList.get(offset);
			    bucketIndex.put(key, ++offset);
			    //写文件
			    FileChannel file= fileHandlers.get(key);
			    if(file==null){
			    	//TODO
			    }
			    try {
			    	synchronized(file){
			    		file.write(((DefaultBytesMessage)message).getByteBuffer());
			    	}
			    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			    }
			      
			}
	   }
    }
}
