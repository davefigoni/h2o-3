package water;

import water.fvec.ChunkUtils;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.parser.BufferedString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Add chunks and data to non-finalized frame from non-h2o environment (ie. Spark executors)
 */
public class ExternalFrameHandler {

    public static final int CREATE_NEW_CHUNK = 1;
    public static final int ADD_TO_FRAME = 2;
    public static final int CLOSE_NEW_CHUNK = 3;

    public static final int TYPE_NUM = 1;
    public static final int TYPE_STR = 2;
    public static final int TYPE_NA = 3;


    NewChunk[] nchnk = null;

    public void process(AutoBuffer ab) {
        ab.getPort(); // skip 2 bytes for port set by ab.putUdp ( which is zero anyway because the request came from non-h2o node)
        int requestType;
        do {
            requestType = ab.getInt();
            switch (requestType) {
                case CREATE_NEW_CHUNK: // Create new chunks
                    String frame_key = ab.getStr();
                    byte[] vec_types = ab.getA1();
                    int chunk_id = ab.getInt();
                    nchnk = ChunkUtils.createNewChunks(frame_key, vec_types, chunk_id);
                    break;
                case ADD_TO_FRAME: // Add to existing frame
                    int dataType = ab.getInt();
                    int colNum = ab.getInt();
                    switch (dataType) {
                        case TYPE_NA:
                            storeNA(colNum);
                            break;
                        case TYPE_NUM:
                            double d = ab.get8d();
                            store(d, colNum);
                            break;
                        case TYPE_STR:
                            String str = ab.getStr();
                            store(str, colNum);
                            break;
                    }
                    break;
                case CLOSE_NEW_CHUNK: // Close new chunks
                    ChunkUtils.closeNewChunks(nchnk);
                    break;
            }
        } while (requestType != CLOSE_NEW_CHUNK);
    }

    public void store(String string, int columnNum) {
        // Helper to hold H2O string
        BufferedString valStr = new BufferedString();
        nchnk[columnNum].addStr(valStr.setTo(string));
    }

    public void store(Double d, int columnNum) {
        nchnk[columnNum].addNum(d);
    }

    public void storeNA(int columnNum) {
        nchnk[columnNum].addNA();
    }

    // ---
    // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
    // Node, not local.  Wrong thread, wrong JVM.
    static class Adder extends UDP {
        @Override
        AutoBuffer call(AutoBuffer ab) {
            throw H2O.fail();
        }

        // Pretty-print bytes 1-15; byte 0 is the udp_type enum
        @Override
        String print16(AutoBuffer ab) {
            int flag = ab.getFlag();
            //String clazz = (flag == CLIENT_UDP_SEND) ? TypeMap.className(ab.getInt()) : "";
            return "task# " + ab.getTask() + " ";//+ clazz+" "+COOKIES[flag-SERVER_UDP_SEND];
        }
    }
}
