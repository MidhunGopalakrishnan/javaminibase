package iterator;

import btree.IndexFile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.Tuple;
import index.IndexScan;

public class BTreeSortedSky {

    public BTreeSortedSky (AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, java.lang.String
            relationName, int[] pref_list, int pref_list_length, IndexFile index_file_list, int n_pages)
            throws Exception {

        FldSpec[] projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);

        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        AttrType[] attrType = new AttrType[5];
        for(int i=0;i<5;i++){
            attrType[i] = new AttrType(AttrType.attrReal);
        }
        short[] attrSize = {};

        Tuple t = new Tuple();
        try {
            t.setHdr((short)5, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeSortedIndex",
                in1, t1_str_sizes, 5, 5, projlist, expr, 5, false);

//        RID rid;
//        Heapfile f = null;
//        try {
//            f = new Heapfile("btreesortedsky.in");
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        while((t=iscan.get_next())!=null){
//           // t.print(in1);
//            try {
//                f.insertRecord(t.returnTupleByteArray());
//            }catch (Exception e){
//                System.err.println("*** error in Heapfile.insertRecord() ***");
//                //status = FAIL;
//                e.printStackTrace();
//            }
//        }
//
//        //create iterator
//        FileScan am = null;
//        try {
//            am = new FileScan("btreesortedsky.in", in1, t1_str_sizes, (short) 5, (short) 5, projlist, null);
//           // PCounter.printCounter();
//            System.out.println("File Scan completed");
//        } catch (Exception e) {
//            System.err.println("" + e);
//        }

        //call nestedloopsky

        Tuple t1 = new Tuple();
        try {
            t1.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        BlockNestedLoopsSky bnl = new BlockNestedLoopsSky( in1,
                5,
                t1_str_sizes,
                iscan,  // This will be the filescan iterator that returns records one by one.
                relationName,
                pref_list, //Preference List
                pref_list_length, //Preference List Length
                n_pages);
        try {
            while((t1=bnl.get_next())!=null) {
                   t1.print(in1);
            }}
        catch(Exception e) {
            e.printStackTrace();
        }
        bnl.close();
    }

}
