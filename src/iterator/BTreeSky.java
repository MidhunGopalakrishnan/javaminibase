package iterator;

import btree.IndexFile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.ArrayList;

public class BTreeSky {

    private   Iterator  outer;

    public BTreeSky (AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, java.lang.String
            relationName, int[] pref_list, int pref_list_length, IndexFile[] index_file_list, int n_pages)
            throws Exception {

        // start index scan -- copied over from indexTest.java
        /*
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex", in1, attrSize, 2, 2, projlist, null, 2, true);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        */


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
        //expr[0] = new CondExpr();
        //expr[1] = new CondExpr();
        // mkg code
        IndexScan[] iscan = new IndexScan[pref_list_length];
        for(int i = 0; i < pref_list_length; i++)
        {
            try {
                iscan[i] = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeIndex" + Integer.toString(pref_list[i]),
                        in1, t1_str_sizes, 5, 5, projlist, expr, 5, false);
            }
            catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
            }

        }
        // mkg code start
        Tuple[] t = new Tuple[pref_list_length];
        t[0] = iscan[0].get_next();
        ArrayList<Integer> scanIndexes = new ArrayList<>();
        ArrayList<Tuple> skylineCandidates = new ArrayList<>();
        ArrayList<Float> tupleSums = new ArrayList<>();
        Tuple skyt = new Tuple(t[0]);
        try {
            skyt.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        skylineCandidates.add(skyt);
        for(int i=1;i < pref_list_length;i++){

            while(true) {
                if(scanIndexes.contains(i)){
                    //already present, quit loop
                    break;
                }
                if((t[i] = iscan[i].get_next())!=null) {
                    //compare with t0, if equal mark index file search complete and quit
                    if (TupleUtils.CompareTupleWithTuplePref(t[0], in1, t[i], in1, (short) len_in1, t1_str_sizes, pref_list, pref_list_length) == 0) {
                        scanIndexes.add(i);
                        break;
                    } else {
                        //check whether Tuple already in skyline candidates, else add
                        float tupleSum = TupleUtils.computeTupleSumOfPrefAttrs(t[i],in1,(short) len_in1, t1_str_sizes, pref_list, pref_list_length);
                        if(!tupleSums.contains(tupleSum)){ // already added in another index run then skip
                        tupleSums.add(tupleSum);
                        skyt = new Tuple(t[i]);
                        skylineCandidates.add(skyt);
                        //skyt.print(in1);
                        }
                    }
                } else {
                    break; //unlikely to happen - happens when a tuple present in one index file and not in another one
                }
            }
        }

        //mkg code end

        //print candidates
        Tuple t2 = new Tuple();
        try {
            t2.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Print skyline candidates");
        for(int i=0;i<skylineCandidates.size();i++){
            t2= skylineCandidates.get(i);
            t2.print(in1);
        }

        //put candidates in heap file
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("btreesky.in");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Tuple t1 = new Tuple();
        try {
            t1.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        for(int i = 0; i < skylineCandidates.size(); i++) {
            try {
                t1 = skylineCandidates.get(i);
                rid = f.insertRecord(t1.returnTupleByteArray());
                // PCounter.printCounter();
                //System.out.println("Record inserted");

            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                //status = FAIL;
                e.printStackTrace();
            }
        }

        //create iterator
        FileScan am = null;
        try {
            am = new FileScan("btreesky.in", in1, t1_str_sizes, (short) 5, (short) 5, projlist, null);
           // PCounter.printCounter();
            System.out.println("File Scan completed");
        } catch (Exception e) {
            System.err.println("" + e);
        }

        //call nestedloopsky
        NestedLoopsSky nls = new NestedLoopsSky( in1,
        5,
        t1_str_sizes,
        am,  // This will be the filescan iterator that returns records one by one.
                "btreesky.in",
        pref_list, //Preference List
        pref_list_length, //Preference List Length
        n_pages);
        try {
        while((t1=nls.get_next())!=null) {
           // t1.print(in1);
        }}
        catch(Exception e) {
            e.printStackTrace();
        }
        nls.close();

        /*
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
        //expr[0] = new CondExpr();
        //expr[1] = new CondExpr();

        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeIndex3", in1, t1_str_sizes, 5, 5, projlist, expr, 5, false);
            //iscan = new IndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex", attrType, attrSize, 2, 2, projlist, expr, 2, false);
        }
        catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
        }


        Tuple t = null;

        try {
            t = iscan.get_next();
        }
        catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
        }

        Tuple[] test = new Tuple[2];
        int count = 0;

        while (t != null) {

            Float outval;
            try {
               outval = t.getFloFld(pref_list[0]);
               if(count < 2)
               {
                   test[count] = t;
               }
            }
            catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
            }


            try {
                t = iscan.get_next();
            }
            catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
            }
            count++;
        }

        TupleUtils tupleTest = new TupleUtils();
        //boolean boolVal = tupleTest.Equal(test[0], test[1], in1, 5);
        int val = tupleTest.CompareTupleWithTuplePref(test[0], in1, test[1], in1, (short) len_in1, t1_str_sizes, pref_list, pref_list_length);
        System.out.println(val);
        /*
        if(boolVal == true)
        {
            System.out.println("error");
        }
        else
        {
            System.out.println("success");
        }
        */
        //int x = 0;


    }

    public Tuple get_next()
    {
        return null;
    }

    //@Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        /*
        if (!closeFlag) {

            try {
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }

         */
    }
}
