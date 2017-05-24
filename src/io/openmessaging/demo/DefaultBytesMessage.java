package io.openmessaging.demo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage,Serializable {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }
    public ByteBuffer getByteBuffer(){
    	ByteBuffer buf=null;
    	try{
	    	byte[] header=((DefaultKeyValue)headers).getBytes();
	    	byte[] propertiesbuf=null;
	    	if(properties==null){
	    		propertiesbuf=new byte[0];
	    	}
	    	else{
	    		propertiesbuf=((DefaultKeyValue)properties).getBytes();
	    	}
	    	
	    	int bufLength=header.length+propertiesbuf.length+body.length+27;
	    	buf=ByteBuffer.allocate(bufLength);
	    	buf.put((byte) 0x01);
	    	buf.putLong(header.length);
	    	buf.put(header);
	    	buf.put((byte) 0x02);
	    	buf.putLong(propertiesbuf.length);
	    	buf.put(propertiesbuf);
	    	buf.put((byte) 0x03);
	    	buf.putLong(body.length);
	    	buf.put(body);
	    	buf.flip();
    	}
    	catch(IOException e){}
	    return buf;
    }
    public static DefaultBytesMessage buildMessageFromByte(byte[] buf){
    	try {
	    	ByteArrayInputStream bais=new ByteArrayInputStream(buf);
	    	ObjectInputStream ois=new ObjectInputStream(bais);
	    	ois.read();
	    	long headerLength=ois.readLong();
	    	DefaultBytesMessage result=new DefaultBytesMessage(null);
			result.headers=(KeyValue)ois.readObject();
			ois.read();
			long propLength=ois.readLong();
			result.properties=(KeyValue)ois.readObject();
			ois.read();
			long bodyLength=ois.readLong();
			result.body=new byte[(int)bodyLength];
			ois.read(result.body, 0, (int)bodyLength);
			return result;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    @Override
    public boolean equals(Object obj) {
    	// TODO Auto-generated method stub
    	try{
	    	ByteArrayOutputStream out=new ByteArrayOutputStream();
			ObjectOutputStream oos=new ObjectOutputStream(out);
			oos.writeObject(obj);
			byte[] obj1=out.toByteArray();
			oos.close();
			out.close();
			out=new ByteArrayOutputStream();
			oos=new ObjectOutputStream(out);
			oos.writeObject(this);
			byte[] obj2=out.toByteArray();
			if(obj2.length!=obj1.length){
				return false;
			}
			for(int i=0;i<obj1.length;i++){
				if(obj1[i]!=obj2[i]){
					return false;
				}
			}
    	}catch(Exception e ){}
    	return true;
    }
}
