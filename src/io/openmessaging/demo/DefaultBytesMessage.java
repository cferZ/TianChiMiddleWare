package io.openmessaging.demo;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

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
	    	byte[] propertiesbuf=((DefaultKeyValue)properties).getBytes();
	    	int bufLength=header.length+propertiesbuf.length+body.length+3;
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
    	}
    	catch(IOException e){}
	    return buf;
    }
}
