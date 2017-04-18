package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    
    private final int loopInterval=1;
    public static KeyValue filePos=null;
    private String filePath="./test/filePos.map";
    private Thread autoSaveThread=null;
    
	private MessageStore() {
		// TODO Auto-generated constructor stub
		if(filePos==null){
			File file=new File(filePath);
			if(file.exists()){
				//装填
				try {
					RandomAccessFile randomFile=new RandomAccessFile(file, "r");
					FileChannel channel=randomFile.getChannel();
					ByteBuffer buf=ByteBuffer.allocate((int)channel.size());
					channel.read(buf);
					ByteArrayInputStream byteIn=new ByteArrayInputStream(buf.array());
					ObjectInputStream in=new ObjectInputStream(byteIn);
					filePos=(KeyValue)in.readObject();
					channel.close();
					randomFile.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if(autoSaveThread==null){
			autoSaveThread=new Thread(new loopSave());
		}
		autoSaveThread.start();
	}
	private class loopSave implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				try {
					Thread.sleep(loopInterval);
					byte[] posFileByte=((DefaultKeyValue)filePos).getBytes();
					RandomAccessFile randomFile=new RandomAccessFile(filePath, "w");
					FileChannel channel=randomFile.getChannel();
					ByteBuffer buf=ByteBuffer.allocate(posFileByte.length);
					buf.put(posFileByte);
					channel.write(buf);
					channel.close();
					randomFile.close();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
		}
		
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
}
