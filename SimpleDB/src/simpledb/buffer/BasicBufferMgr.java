package simpledb.buffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private int numAvailable;
   private int bufferSize;
   private Map<Block,Buffer> bufferPoolMap;
   private static boolean isUnpinnedAvailable=false;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
	  bufferPoolMap = new HashMap<Block,Buffer>();
      numAvailable = numbuffs;
      bufferSize=numbuffs;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	  Set<Block> keySet = bufferPoolMap.keySet();
      for (Block blk : keySet){
    	 if(blk == null || bufferPoolMap.get(blk) == null)
    		 continue;
         if ((bufferPoolMap.get(blk)).isModifiedBy(txnum))
        	 bufferPoolMap.get(blk).flush();
      }
   }
   
   	public void getStatistics(){
	Iterator<Buffer> iterator= bufferPoolMap.values().iterator();
	int i=1;
	while(iterator.hasNext()){
		Buffer buff=iterator.next();
		System.out.println(i+ "th Buffer");
		System.out.println("Number of Reads " +buff.getReads() );
		System.out.println("Number of Writes "+ buff.getWrites());
		i++;
	}
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
	   Buffer buff = findExistingBuffer(blk);
	   if (buff == null) {	  
         buff = chooseUnpinnedBuffer();
         if (buff == null){
        	 return null;
         }
         buff.assignToBlock(blk);
         bufferPoolMap.put(blk, buff);
       }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      //this.getStatistics();
      return buff;
   }
   
   	/**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;      
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block(), buff);
      if (!buff.isPinned())
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();   
      if (!buff.isPinned())
      {
         numAvailable++;
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
      
   private Buffer findExistingBuffer(Block blk) {
	  return getMapping(blk);
   }
   
   
   public Block chooseBufferForReplacement(Map<Block, Buffer> bufferPool) {
	   Block LRU = null;
		int minLSN = -1;
		
	   for (Map.Entry<Block, Buffer> entry : bufferPool.entrySet()) {
			Buffer value = entry.getValue();
			if(!value.isPinned()){
			isUnpinnedAvailable=true;
			break;
			}
		}
	    if(!isUnpinnedAvailable)
	   return null;
	   
	    for (Map.Entry<Block, Buffer> entry : bufferPool.entrySet()) {
			Block key = entry.getKey();
			Buffer value = entry.getValue();
			//System.out.println( "LSN " + value.getLogSequenceNumber()+"  " + value.isPinned());
			if (minLSN < 0  && !value.isPinned() && value.getLogSequenceNumber() >= 0) {
				minLSN = value.getLogSequenceNumber();
				LRU = key;
			}

			if (value.getLogSequenceNumber() >= 0 && minLSN >= value.getLogSequenceNumber() && !value.isPinned()) {
				minLSN = value.getLogSequenceNumber();
				LRU = key;
			}

	    }
	    
	    if(minLSN>-1)
	    	return LRU;
	    
	    
	    minLSN = -1;
		for (Map.Entry<Block, Buffer> entry : bufferPool.entrySet()) {
			Block key = entry.getKey();
			Buffer value = entry.getValue();
			//System.out.println("Modified By loop 2: LSN  " + value.getLogSequenceNumber()+"  " + value.isPinned());
			
		if (value.getLogSequenceNumber() == -1 && !value.isPinned()) {
				minLSN = value.getLogSequenceNumber();
				LRU = key;
			}
		
		if (minLSN >= value.getLogSequenceNumber() && !value.isPinned()) {
			minLSN = value.getLogSequenceNumber();
			LRU = key;
		}
		
		}
		return LRU;

   }
   
   

	private Buffer chooseUnpinnedBuffer() {
		Buffer final_buffer = null;
		isUnpinnedAvailable=false;
		if (bufferPoolMap.size() < bufferSize)
			return new Buffer();

		Block LRU = chooseBufferForReplacement(bufferPoolMap);
		
		if (LRU != null) {
			Buffer buffer = bufferPoolMap.get(LRU);
			//System.out.println("Buffer Selected: Modified By  " + buffer.getModifiedBy() + "  LSN  " + buffer.getLogSequenceNumber());
			
			bufferPoolMap.remove(LRU);
			return buffer;
		} else {
			
			if(!isUnpinnedAvailable){
				//System.out.println(" No Unpinned Buffer Available");
				return null;
			}
		}

		return final_buffer;
	}
   
   /**
   * Determines whether the map has a mapping from
   * the block to some buffer.
   * @paramblk the block to use as a key
   * @return true if there is a mapping; false otherwise
   */
   
   
   public boolean containsMapping(Block blk){
	   return bufferPoolMap.containsKey(blk);
   }
   
   /**
   * Returns the buffer that the map maps the specified block to.
   * @paramblk the block to use as a key
   * @return the buffer mapped to if there is a mapping; null otherwise */
   public Buffer getMapping(Block blk){
	   return bufferPoolMap.get(blk);
   }
   
}
