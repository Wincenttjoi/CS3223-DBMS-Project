package simpledb.tx.recovery;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetFloatRecord implements LogRecord {
   private int txnum, offset;
   private float val;
   private BlockId blk;

   /**
    * Create a new setfloat log record.
    * @param bb the bytebuffer containing the log values
    */
   public SetFloatRecord(Page p) {
      int tpos = Integer.BYTES;
      txnum = p.getInt(tpos);
      int fpos = tpos + Integer.BYTES;
      String filename = p.getString(fpos);
      int bpos = fpos + Page.maxLength(filename.length());
      int blknum = p.getInt(bpos);
      blk = new BlockId(filename, blknum);
      int opos = bpos + Integer.BYTES;
      offset = p.getInt(opos);
      int vpos = opos + Integer.BYTES;      
      val = p.getFloat(vpos);
   }

   public int op() {
      return SETFLOAT;
   }

   public int txNumber() {
      return txnum;
   }

   public String toString() {
      return "<SETFLOAT " + txnum + " " + blk + " " + offset + " " + val + ">";
   }

   /**
    * Replace the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls setInt to restore the saved value,
    * and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(Transaction tx) {
      tx.pin(blk);
      tx.setFloat(blk, offset, val, false); // don't log the undo!
      tx.unpin(blk);
   }

   /**
    * A static method to write a setFloat record to the log.
    * This log record contains the SETFLOAT operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * float value at that offset.
    * @return the LSN of the last log value
    */
   public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, float val) {
      int tpos = Integer.BYTES;
      int fpos = tpos + Integer.BYTES;
      int bpos = fpos + Page.maxLength(blk.fileName().length());
      int opos = bpos + Integer.BYTES;
      int vpos = opos + Integer.BYTES;
      byte[] rec = new byte[vpos + Integer.BYTES];
      Page p = new Page(rec);
      p.setInt(0, SETFLOAT);
      p.setInt(tpos, txnum);
      p.setString(fpos, blk.fileName());
      p.setInt(bpos, blk.number());
      p.setInt(opos, offset);
      p.setFloat(vpos, val);
      return lm.append(rec);
   }
}
