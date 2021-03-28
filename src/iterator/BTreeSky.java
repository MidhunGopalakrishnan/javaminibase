package iterator;

import btree.IndexFile;
import diskmgr.PCounter;
import global.*;
import heap.HFPage;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;

public class BTreeSky extends Iterator implements GlobalConst {

    FileScan am = null;
    Heapfile f = null;
    String tempHeapFileName = "btreesky.in";

    public BTreeSky (AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, java.lang.String
            relationName, int[] pref_list, int pref_list_length, IndexFile[] index_file_list, int n_pages, Heapfile dataFile, int numOfColumns)
            throws Exception {

        FldSpec[] projlist = new FldSpec[numOfColumns];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i =0; i < numOfColumns;i++) {
            projlist[i] = new FldSpec(rel, i+1);
        }
        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        IndexScan[] iscan = new IndexScan[pref_list_length];
        for(int i = 0; i < pref_list_length; i++)
        {
            try {
                iscan[i] = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeIndexFile" + Integer.toString(pref_list[i]),
                        in1, t1_str_sizes, numOfColumns, numOfColumns, projlist, expr, numOfColumns, false);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        }

        Heapfile tempHeapFile = null;
        try {
            tempHeapFile = new Heapfile(tempHeapFileName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        Tuple temp = null;
        Tuple scanTuple = new Tuple();
        try {
            scanTuple.setHdr((short) numOfColumns, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        int tupleSize = scanTuple.size();

        HashMap<Tuple,Integer> tupleTracker = new HashMap<Tuple, Integer>();

        if(pref_list_length ==1) {
            // everything is a skyline candidate, so just insert the sorted records to a temp heap file and pass it to Nested Loop Sky
            while((temp= iscan[0].get_next())!=null){
                scanTuple.tupleCopy(temp);
                tempHeapFile.insertRecord(scanTuple.returnTupleByteArray());
            }
        } else {
            // find candidates and push to the heap file
            // get tuple from each index file and add to hashmap. If already present, increment the count.
            // Once count reach number of preference attribute length, then break
            boolean continueWhile = true;
            while(continueWhile) {

                for (int i = 0; i < pref_list_length; i++) {
                    scanTuple = new Tuple();
                    try {
                        scanTuple.setHdr((short) numOfColumns, in1, t1_str_sizes);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    scanTuple.tupleCopy(iscan[i].get_next());
                    //scanTuple.print(in1);
                    if(!tupleTracker.containsKey(scanTuple)) {
                        tupleTracker.put(scanTuple,1);
                    } else{
                        int count = tupleTracker.get(scanTuple);
                        count++;
                        tupleTracker.put(scanTuple,count);
                        if(count == pref_list_length) {
                            continueWhile= false;
                            break;
                        }
                    }

                }
            }
            // insert to heap file from HashMap
            Tuple[] candidateTuples = tupleTracker.keySet().toArray(new Tuple[0]);
            for (int i =0; i < candidateTuples.length;i++){
                //candidateTuples[i].print(in1);
                tempHeapFile.insertRecord(candidateTuples[i].returnTupleByteArray());
            }
        }

        //close the index scan since we got all candidates in list
        for(int k=0; k < iscan.length;k++){
            iscan[k].close();
        }

        //create iterator

        try {
            am = new FileScan(tempHeapFileName, in1, t1_str_sizes, (short) numOfColumns, (short) numOfColumns, projlist, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        //call nestedloopsky
        NestedLoopsSky nls = new NestedLoopsSky( in1,
        numOfColumns,
        t1_str_sizes,
        am, tempHeapFileName,
        pref_list, //Preference List
        pref_list_length, //Preference List Length
        n_pages);
        try {
        while((scanTuple=nls.get_next())!=null) {
            scanTuple.print(in1);
        }}
        catch(Exception e) {
            e.printStackTrace();
        }
        nls.close();
        am.close();
    }

    public Tuple get_next()
    {
        return null;
    }

    //@Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

        if (!closeFlag) {

            try {
                am.close();
            }catch (Exception e) {
                throw new JoinsException(e, "BTreeSky.java: error in closing iterator.");
            }
            closeFlag = true;
        }

    }

}
