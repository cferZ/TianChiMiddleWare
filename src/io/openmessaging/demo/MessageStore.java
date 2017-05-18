package io.openmessaging.demo;

import io.openmessaging.Message;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    
    private static final HashMap<String,Integer[]> fileIndex=new HashMap<>();
    
    private static final HashMap<String,FileChannel> fileHandlers=new HashMap<>();
    
    

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
	   }
   }
}
