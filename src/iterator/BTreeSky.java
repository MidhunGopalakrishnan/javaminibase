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

public class BTreeSky extends Iterator implements GlobalConst {

    FileScan am = null;
    Heapfile f = null;

    public BTreeSky (AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, java.lang.String
            relationName, int[] pref_list, int pref_list_length, IndexFile[] index_file_list, int n_pages, Heapfile dataFile)
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

        //am1.close();

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
//        Tuple[] t = new Tuple[pref_list_length];
//        t[0] = iscan[0].get_next();
        Tuple t10 = new Tuple();
        try {
            t10.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        t10 = iscan[0].get_next();
       // t10.print(in1);
        ArrayList<Integer> scanIndexes = new ArrayList<>();
        ArrayList<Tuple> skylineCandidates = new ArrayList<>();
        ArrayList<Float> tupleSums = new ArrayList<>();
        Tuple skyt = new Tuple();
        try {
            skyt.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        skyt.tupleCopy(t10);

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
        //skyt.print(in1);
        f.insertRecord(skyt.returnTupleByteArray());
       // skyt.print(in1);
        skylineCandidates.add(skyt);
        if(pref_list_length ==1) {
           // if preference list is based on one attribute then all are skyline candidates
           //add all to skyline
            while(true) {
                if((t10=iscan[0].get_next())!=null){
                    skyt = new Tuple(t10);
                    skylineCandidates.add(skyt);
                    //skyt.print(in1);
                    //f.insertRecord(skyt.returnTupleByteArray());
                } else {
                    break;
                }
            }
        } else {
        for(int i=1;i < pref_list_length;i++) {

            while (true) {
                if (scanIndexes.contains(i)) {
                    //already present, quit loop
                    break;
                }
                Tuple t11 = new Tuple();
                try {
                    t11.setHdr((short) 5, in1, t1_str_sizes);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                if ((t11 = iscan[i].get_next()) != null) {
                    //compare with t0, if equal mark index file search complete and quit
                    if (TupleUtils.CompareTupleWithTuplePref(t10, in1, t11, in1, (short) len_in1, t1_str_sizes, pref_list, pref_list_length) == 0) {
                        scanIndexes.add(i);
                        break;
                    } else {
                        //check whether Tuple already in skyline candidates, else add
//                        float tupleSum = TupleUtils.computeTupleSumOfPrefAttrs(t[i], in1, (short) len_in1, t1_str_sizes, pref_list, pref_list_length);
//                        if (!tupleSums.contains(tupleSum)) { // already added in another index run then skip
//                            tupleSums.add(tupleSum); // TODO : check each attribute and confirm
                            skyt = new Tuple(t11);
                            skylineCandidates.add(skyt);
//                            //skyt.print(in1);
//                        }
//                        skyt = new Tuple(t[i]);
//                        f.insertRecord(skyt.returnTupleByteArray());
                    }
                } else {
                    break; //unlikely to happen - happens when a tuple present in one index file and not in another one
                }
            }
        }
        }

        //mkg code end
        //close the index scan since we got all candidates in list
        for(int k=0; k < iscan.length;k++){
            iscan[k].close();
        }

        //print candidates
        Tuple t2 = new Tuple();
        try {
            t2.setHdr((short) 5, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

//        System.out.println("Print skyline candidates");
//        for(int i=0;i<skylineCandidates.size();i++){
//            t2= skylineCandidates.get(i);
//            t2.print(in1);
//        }
        // delete the heap file created from data file and create a new heap file with only skyline candidates
//        HFPage currentDirPage = new HFPage();
//        PageId currentDirPageId = dataFile.get_file_entry(relationName);
//        PageId nextDirPageId = null;
//        while(currentDirPageId.pid != INVALID_PAGE)
//        {   dataFile.pinPage(currentDirPageId, currentDirPage, false);
//            dataFile.unpinPage(currentDirPageId,false);
//            nextDirPageId = currentDirPage.getNextPage();
//            currentDirPageId.pid = nextDirPageId.pid;
//        }

        //put candidates in heap file
       // int count =0;
        //FileWriter myWriter = new FileWriter("src/data/log.txt");
       // myWriter.write("Skyline candidate size :"+ skylineCandidates.size());
        for(int i = 0; i < skylineCandidates.size(); i++) {
            try {
                //count++;
                t1 = skylineCandidates.get(i);
                rid = f.insertRecord(t1.returnTupleByteArray());
                //System.out.println("Inserted record count :"+ count);
                //myWriter.write("Inserted record count :"+ count);
                // PCounter.printCounter();
                //System.out.println("Record inserted");

            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                //status = FAIL;
                e.printStackTrace();
            } finally{
               // myWriter.close();
            }
        }

        //create iterator

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
            t1.print(in1);
        }}
        catch(Exception e) {
            e.printStackTrace();
        }
        nls.close();
        am.close();
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
