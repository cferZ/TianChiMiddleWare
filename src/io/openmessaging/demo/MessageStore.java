package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class MessageStore {

	private final String EXTNAME=".queue";//扩展名
	private final int LOOPINTERVAL=1000;//循环存offset map 的间隔
	private final int MAXMESSAGELENGTH=256*1024;//每条消息最大长度
	private final String OFFSETFILE="MsgOffsets";//queueoffset 文件名
	private static String STORE_PATH=null;//存储路径
    private final int MAXMAPBYTEBUFFER=50*1024*1024;//内存映射文件最大大小
    
    private static final ConcurrentHashMap<String,MappedByteBuffer> fileHandlers=new ConcurrentHashMap<>();//内存映射文件们的句柄
    private static FileChannel queueOffsetFile=null;//queueOffset 文件的句柄
    private static final HashMap<String , Integer> bucketIndex=new HashMap<>();
    private static final MessageStore INSTANCE = new MessageStore();
    private Thread loopSaveHandler=null;//循环存消息线程句柄
    private Thread indexSaveHandler=null;				//循环存offset的线程句柄			
    
    private Map<String, ArrayList<Message>> messageBuckets = new ConcurrentHashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();//记录当前需要消费的消息在文件中的偏移
    
    public MessageStore() {
		// TODO Auto-generated constructor stub
    	//TODO 从文件中读queueOffset
    	try{
			if (queueOffsetFile == null) {
				//TODO
				File file = new File(STORE_PATH + File.pathSeparator + OFFSETFILE + EXTNAME);
				if (file.exists()) {
//					System.out.println(file.getAbsolutePath());
					queueOffsetFile = new RandomAccessFile(file, "rw").getChannel();
					ByteBuffer buff=ByteBuffer.allocate(MAXMESSAGELENGTH);
					queueOffsetFile.read(buff);
					queueOffsets=(HashMap) new ObjectInputStream(new ByteArrayInputStream(buff.array())).readObject();
				}	
			}
			
    	}catch (Exception e){
    		e.printStackTrace();
    	}
  //  	loopSaveHandler=new Thread(new LoopSave());
    	//loopSaveHandler.start();
    	indexSaveHandler=new Thread(new IndexSave());
    	//indexSaveHandler.start();
	}

    public static MessageStore getInstance() {
        return INSTANCE;
    }
    
    public void setSaveFilePathIfNot(String path){
    	if(STORE_PATH==null){
    		STORE_PATH=path;
    	//	loopSaveHandler.start();
    		indexSaveHandler.start();
    	}
    }
    
    
    public  long putMessage(String bucket, Message message) {
    	MappedByteBuffer mmb = fileHandlers.get(bucket);
    	long start=0;
    	long end =0;
		try {
			
			if (mmb == null) {
				//TODO
				synchronized (this) {
					mmb = fileHandlers.get(bucket);
					if (mmb == null) {
						File file = new File(STORE_PATH + File.pathSeparator + bucket + EXTNAME);
						if (!file.exists()) {
							file.createNewFile();
						}
						FileChannel f = new RandomAccessFile(file, "rw").getChannel();
						mmb=f.map(MapMode.READ_WRITE, 0, MAXMAPBYTEBUFFER);
						fileHandlers.put(bucket, mmb);
					}
				}
			}
			
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			ObjectOutputStream oos=new ObjectOutputStream(out);
			oos.writeObject(message);
			byte[] objectBuf=out.toByteArray();
			
//				System.out.println(objectBuf.length);
			ByteBuffer buf=ByteBuffer.allocate(objectBuf.length+4);
			buf.putInt(objectBuf.length);
			buf.put(objectBuf);
			buf.flip();
			start = System.currentTimeMillis();
			synchronized (mmb) {
				mmb.put(buf);
			}
			end = System.currentTimeMillis();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return end-start;
    }

    public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        MappedByteBuffer mmb = fileHandlers.get(bucket);
        
        try{
	        if (mmb == null) {//file not exist return null
	        	File file = new File(STORE_PATH + File.pathSeparator + bucket + EXTNAME);
				if (!file.exists()) {
					return null;
				}
				FileChannel f = new RandomAccessFile(file, "rw").getChannel();
				mmb=f.map(MapMode.READ_WRITE,0, MAXMAPBYTEBUFFER);
				fileHandlers.put(bucket, mmb);
	        }
	        
	        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
	        if (offsetMap == null) {
	            offsetMap = new HashMap<>();
	            queueOffsets.put(queue, offsetMap);
	        }
	        int[] offset =new int[1];
	        offset[0]=offsetMap.getOrDefault(bucket, 0);
	        //from file
	        if (offset[0] >= mmb.capacity()) {
	            return null;
	        }
	        Message message = null;
	        synchronized(mmb){
	        	message=getMessageFromFile(mmb, offset);
	        }
	        if(message==null)
	        	return null;
	        
	        offsetMap.put(bucket, offset[0]);
	        return message;
        }
        catch(Exception e){
        	e.printStackTrace();
        }
        return null;
    }
    private DefaultBytesMessage getMessageFromFile(MappedByteBuffer mmb,int[] offset){
    	try {
    		mmb.position(offset[0]);
    		int length=mmb.getInt();
    		byte[] buf=new byte[length];
    		mmb.get(buf);
    		ByteArrayInputStream in=new ByteArrayInputStream(buf);
    		ObjectInputStream ois=new ObjectInputStream(in);
    		DefaultBytesMessage result=(DefaultBytesMessage) ois.readObject();
    		offset[0]+=length+4;
			return result;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    
    private class IndexSave implements Runnable{
	    @Override
		public void run() {
			// TODO Auto-generated method stub
	    	ByteBuffer buf=ByteBuffer.allocate(MAXMESSAGELENGTH);
			try {
				while (true) {
					Thread.sleep(LOOPINTERVAL);
					
					if (queueOffsetFile == null) {
						//TODO
						File file = new File(STORE_PATH + File.pathSeparator + OFFSETFILE + EXTNAME);
						if (!file.exists()) {
							file.createNewFile();
						}
						queueOffsetFile = new RandomAccessFile(file, "rw").getChannel();
					}
					buf.clear();
					ByteArrayOutputStream out=new ByteArrayOutputStream();
					ObjectOutputStream oos=new ObjectOutputStream(out);
					oos.writeObject(queueOffsets);
					byte[] objectBuf=out.toByteArray();
					buf.put(objectBuf);
					buf.flip();
					synchronized (queueOffsetFile) {
						queueOffsetFile.position(0);
						queueOffsetFile.write(buf);
						queueOffsetFile.force(true);
					} 
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
   /**
    private class LoopSave implements Runnable{
    	@Override
    	public void run() {
    		while (true) {
				// TODO Auto-generated method stub
				Set<String> keys = messageBuckets.keySet();
				for (String key : keys) {
					ArrayList<Message> bucketList = messageBuckets.get(key);
					if (bucketList == null) {
						continue;
					}
					int offset = bucketIndex.getOrDefault(key, 0);
					if (offset >= bucketList.size()) {
						continue;
					}
					Message message = bucketList.get(offset);
					if(message==null){
						continue;
					}
//					System.out.println(message);
					bucketIndex.put(key, ++offset);
					//写文件
					FileChannel f = fileHandlers.get(key);
					try {
						if (f == null) {
							//TODO
							File file = new File(STORE_PATH + File.pathSeparator + key + EXTNAME);
							if (!file.exists()) {
								file.createNewFile();
							}
							f = new RandomAccessFile(file, "rw").getChannel();
							fileHandlers.put(key, f);
						}
						ByteArrayOutputStream out=new ByteArrayOutputStream();
						ObjectOutputStream oos=new ObjectOutputStream(out);
						oos.writeObject(message);
						byte[] objectBuf=out.toByteArray();
		//				System.out.println(objectBuf.length);
						ByteBuffer buf=ByteBuffer.allocate(objectBuf.length+4);
						buf.putInt(objectBuf.length);
						buf.put(objectBuf);
						buf.flip();
						synchronized (f) {
							f.position(f.size());
							int len=f.write(buf);
							f.force(true);
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} 
			}
	   }
    }
    */
}
