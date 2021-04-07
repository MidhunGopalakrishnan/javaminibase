package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class TopK_HashJoin extends Iterator implements GlobalConst {

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
            int numOfColumns2,int joinAttrTable1,int joinAttrTable2,int mergeAttrTable1,int mergeAttrTable2
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
                        scanTuple.print(in1);
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

            if (tupleTracker.size()==k){
                //first k in the list are the top k elements
                printResult(tupleTracker);

            }
            else if (tupleTracker.size()>k){
                //vet all elements in hashmap ie find A+B/2 for all top k elements
                System.out.println("Here now");

            }

        }catch(Exception e){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        try{
            iscan[0].close();
            iscan[1].close();
        } catch(Exception e){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


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
