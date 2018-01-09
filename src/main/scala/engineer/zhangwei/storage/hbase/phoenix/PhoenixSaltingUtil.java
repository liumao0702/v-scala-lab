package engineer.zhangwei.storage.hbase.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;


/**
 * Created by ZhangWei on 2017/11/22.s
 */
public class PhoenixSaltingUtil {

    public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
        byte[][] splits = new byte[saltBucketNum-1][];
        for (int i = 1; i < saltBucketNum; i++) {
            splits[i-1] = new byte[] {(byte) i};
        }
        return splits;
    }

    // Compute the hash of the key value stored in key and set its first byte as the value. The
    // first byte of key should be left empty as a place holder for the salting byte.

    /**
     *
     * @param key
     * @param bucketNum
     * @return
     */
    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getLength()];
        byte saltByte = getSaltingByte(key.get(), key.getOffset() + 1, key.getLength() - 1, bucketNum);
        keyBytes[0] = saltByte;
        System.arraycopy(key.get(), key.getOffset() + 1, keyBytes, 1, key.getLength() - 1);
        return keyBytes;
    }

    // Generate the bucket byte given a byte array and the number of buckets.

    /**
     *
     * @param value
     * @param offset
     * @param length
     * @param bucketNum
     * @return
     */
    public static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        int hash = calculateHashCode(value, offset, length);
        return (byte) Math.abs(hash % bucketNum);
    }

    private static int calculateHashCode(byte a[], int offset, int length) {
        if (a == null) {
            return 0;
        }
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    public static void main(String[] args) throws Exception{

        String pk1 = "3000000122222222";
        String pk2 = "0623300072225731";
        String pk3 = "030600000321504";
        String pk4 = "121";
        String pk5 = "0000";

        ByteArrayOutputStream bf = new ByteArrayOutputStream(100);
        bf.write(0);
        bf.write(pk1.getBytes(),0,pk1.getBytes().length);
        bf.write(0);
        bf.write(pk2.getBytes(),0,pk2.getBytes().length);
        bf.write(0);
        bf.write(pk3.getBytes(),0,pk3.getBytes().length);
        bf.write(0);
        bf.write(pk4.getBytes(),0,pk4.getBytes().length);
        bf.write(0);
        bf.write(pk5.getBytes(),0,pk5.getBytes().length);

        byte[] buf = bf.toByteArray();
        int size = bf.size();
        buf[0] = getSaltingByte(buf, 1, size-1, 3);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(buf,0,size);
        byte[] bytes = getSaltedKey(ptr, 3);
        Configuration conf =  HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("LC_CBXX");
        Table table = connection.getTable(tableName);
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes("0"),Bytes.toBytes("JLDXH"),Bytes.toBytes(2));
        put.addColumn(Bytes.toBytes("0"),"SDLX".getBytes(),Bytes.toBytes("2"));
        table.put(put);
    }
}
