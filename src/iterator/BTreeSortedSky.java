package iterator;

import btree.IndexFile;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;

public class BTreeSortedSky extends Iterator implements GlobalConst  {

    public BTreeSortedSky (AttrType[] in1, int numOfColumns, short[] t1_str_sizes, Iterator am1, java.lang.String
            relationName, int[] pref_list, int pref_list_length, IndexFile index_file_list, int n_pages)
            throws Exception {

        FldSpec[] projlist = new FldSpec[numOfColumns];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<numOfColumns;i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        Tuple t = new Tuple();
        try {
            t.setHdr((short)numOfColumns, in1, t1_str_sizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeSortedIndex",
                in1, t1_str_sizes, numOfColumns, numOfColumns, projlist, expr, numOfColumns, false);

        //call nestedloopsky

        Tuple t1 = new Tuple();
        try {
            t1.setHdr((short) numOfColumns, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

//        while((t1=iscan.get_next())!=null){
//            t1.print(in1);
//        }

        BlockNestedLoopsSky bnl = new BlockNestedLoopsSky( in1,
                numOfColumns,
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

        iscan.close();
    }

    public Tuple get_next()
    {
        return null;
    }

    //@Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

        if (!closeFlag) {

            try {
                //TODO : if we need to close anything
            }catch (Exception e) {
                throw new JoinsException(e, "BTreeSortedSky.java: error in closing iterator.");
            }
            closeFlag = true;
        }

    }

}
