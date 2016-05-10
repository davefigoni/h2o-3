package water.fvec;

/**
 * Simple helper class which publishes some package private methods as public
 */
public class ChunkUtils {
    public static NewChunk[] createNewChunks(String name, byte[] vecTypes, int chunkId){
        return Frame.createNewChunks(name, vecTypes, chunkId);
    }

    public static void closeNewChunks(NewChunk[] nchks){
        Frame.closeNewChunks(nchks);
    }
}
