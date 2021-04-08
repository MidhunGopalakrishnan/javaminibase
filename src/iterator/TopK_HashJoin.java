package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import heap.*;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.*;

public class TopK_HashJoin extends Iterator implements GlobalConst {

    private static short REC_LEN1 = 4;

    public TopK_HashJoin(
            AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1,
            FldSpec mergeAttr1,
            AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2,
            FldSpec mergeAttr2,
            java.lang.String relationName1,
            java.lang.String relationName2,
            int k,
            int n_pages,
            int numOfColumns1,
            int numOfColumns2,int joinAttrTable1,int joinAttrTable2,int mergeAttrTable1,int mergeAttrTable2,Heapfile table1,Heapfile table2
    ) {
//        //set projection list for two index scan objects
//        FldSpec[] projlistTable1 = new FldSpec[2];
//        projlistTable1[0] = joinAttr1;
//        projlistTable1[1] = mergeAttr1;
//        FldSpec[] projlistTable2 = new FldSpec[2];
//        projlistTable2[0] = joinAttr2;
//        projlistTable2[1] = mergeAttr2;

        FldSpec[] projlist1 = new FldSpec[numOfColumns1];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < numOfColumns1;i++) {
            projlist1[i] = new FldSpec(rel1, i+1);
        }

        FldSpec[] projlist2 = new FldSpec[numOfColumns2];
        RelSpec rel2 = new RelSpec(RelSpec.outer);
        for(int i =0; i < numOfColumns1;i++) {
            projlist2[i] = new FldSpec(rel2, i+1);
        }

        // TODO : set to null as of now considering user input will not have a where clause attached
        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        IndexScan[] iscan = new IndexScan[2];

        try {
            iscan[0] = new IndexScan(new IndexType(IndexType.B_Index), relationName1, "BTreeIndex" + relationName1,
                    in1, t1_str_sizes, numOfColumns1, numOfColumns1, projlist1, expr, numOfColumns1, false);
            iscan[1] = new IndexScan(new IndexType(IndexType.B_Index), relationName2, "BTreeIndex" + relationName2,
                    in2, t2_str_sizes, numOfColumns2, numOfColumns2, projlist2, expr, numOfColumns2, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        boolean continueWhile = true;
        int countK = 0;
        HashMap<String,Integer> tupleTracker = new HashMap<String, Integer>();
        int runningKCount =0;
        try {
            while (continueWhile) {

                for(int i=0; i < 2; i++) {
                    Tuple scanTuple = new Tuple();
                    try {
                        scanTuple.setHdr((short) numOfColumns1, in1, t1_str_sizes);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    scanTuple.tupleCopy(iscan[i].get_next());
                    if (scanTuple == null && countK < k) {
                        System.out.println("No Top K elements can be found from these two tables");
                    } else if (scanTuple != null) {
//                        scanTuple.print(in1);
                        if (!tupleTracker.containsKey(scanTuple.getStrFld(1))) { //TODO : change hardcoded value of 1. First value is not always the join attribute, also it wont be string always
                            int position = mergeAttrTable1;
                            if(i==1){
                              position = mergeAttrTable2;
                            }
                            tupleTracker.put(scanTuple.getStrFld(1), scanTuple.getIntFld(position));
                        } else {
                            int position = mergeAttrTable1;
                            if(i==1){
                                position = mergeAttrTable2;
                            }
                            int value = tupleTracker.get(scanTuple.getStrFld(1));
                            tupleTracker.put(scanTuple.getStrFld(1), (value+scanTuple.getIntFld(position))/2);
                            runningKCount++;
                            if (runningKCount == k) {
                                continueWhile = false;
                                break;
                            }
                        }
                    }
                }
            }

            try{
                iscan[0].close();
                iscan[1].close();
            } catch(Exception e){
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

            if (tupleTracker.size()==k){
                //first k in the list are the top k elements
                printResult(tupleTracker);

            }
            else if (tupleTracker.size()>k){
                //vet all elements in hashmap ie find A+B/2 for all top k elements
                //create index files on the join attribute based on table 1 and table 2
                BTreeFile btf1 = createIndexFile(table1,"table1",joinAttrTable1,numOfColumns1);
                BTreeFile btf2 = createIndexFile(table2,"table2",joinAttrTable2,numOfColumns2);

                Tuple scanTuple1 = new Tuple();
                try {
                    scanTuple1.setHdr((short) numOfColumns1, in1, t1_str_sizes);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Tuple scanTuple2 = new Tuple();
                try {
                    scanTuple2.setHdr((short) numOfColumns2, in2, t2_str_sizes);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                HashMap<String,Float> topKMap = new HashMap<>();

                for(String key : tupleTracker.keySet()){
                    //for each key compute average and fix the top k elements
                    KeyClass keyC = new StringKey(key);
                    BTFileScan bt1 = btf1.new_scan(keyC,keyC);
                    BTFileScan bt2 = btf2.new_scan(keyC,keyC);
                    KeyDataEntry kd1 = bt1.get_next();
                    KeyDataEntry kd2 = bt2.get_next();
                    if(kd1!=null && kd2!=null) {
                        LeafData leafData1 = (LeafData) kd1.data;
                        RID rid1 = new RID(leafData1.getData().pageNo,leafData1.getData().slotNo);
                        scanTuple1.tupleCopy(table1.getRecord(rid1));
//                        scanTuple1.print(in1);
                        int value1 = scanTuple1.getIntFld(mergeAttrTable1);
                        LeafData leafData2 = (LeafData) kd2.data;
                        RID rid2 = new RID(leafData2.getData().pageNo,leafData2.getData().slotNo);
                        scanTuple2.tupleCopy(table2.getRecord(rid2));
//                        scanTuple2.print(in2);
                        int value2 = scanTuple2.getIntFld(mergeAttrTable2);
                        float avg = (float)(value1+value2)/2;
                        topKMap.put(scanTuple1.getStrFld(joinAttrTable1),avg);
                    }
                }

                LinkedList<Map.Entry<String,Float>> topKList = new LinkedList<Map.Entry<String,Float>>(topKMap.entrySet());
                Collections.sort(topKList, new Comparator<Map.Entry<String, Float>>() {
                    @Override
                    public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                        return (o2.getValue()).compareTo(o1.getValue());
                    }
                });

                // print first K elements from LinkedList
                for(int j=0; j< k; j++) {
                    Map.Entry<String,Float> value = topKList.get(j);
                    System.out.println(value.getKey() + " ---> " + value.getValue());
                }
//                for(Map.Entry<String,Float> value : topKList){
//                        System.out.println(value.getKey() + " ---> " + value.getValue());
//                }
            }

        }catch(Exception e){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


    }

    private BTreeFile createIndexFile(Heapfile table,String tableName,int prefAttr,int numOfColumns) {
        Scan scan = null;

        try {
            scan = new Scan(table);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        AttrType[] attrType = new AttrType[numOfColumns];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);
        short[] attrSize = {10};

        Tuple temp = null;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }
        RID rid = new RID();
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("BTreeJoin" + tableName, AttrType.attrString, REC_LEN1, 1/*delete*/);
            KeyClass key;
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                String intKey = t.getStrFld(prefAttr);
                key = new StringKey(intKey);
                btf.insert(key, rid);
            }
        } catch (Exception e) {
            System.err.println("*** BTree File error ***");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        } finally {
            scan.closescan();
        }

        return btf;
    }

    private void printResult(HashMap<String, Integer> tupleTracker) {
        for(String key : tupleTracker.keySet()){
            System.out.println(key + " -->" + tupleTracker.get(key));
        }
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }
}
