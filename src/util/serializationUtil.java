package util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class serializationUtil {
	private final int BUFCAPABILITY=2048;
	public static byte[] getByteBuffer(Serializable obj) throws IOException{
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		ObjectOutputStream objOut=new ObjectOutputStream(out);
		objOut.writeObject(obj);
		byte[] buf=out.toByteArray();
		return buf;
	}
}
