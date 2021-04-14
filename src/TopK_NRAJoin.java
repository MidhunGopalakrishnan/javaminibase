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
import jdk.jfr.Description;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;

public class TopK_NRAJoin extends Iterator implements GlobalConst {

    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param joinAttr1
     *@param mergeAttr1
     *@param in2  Array containing field types of S
     *@param len_in2  # of columns in S
     *@param t2_str_sizes shows the length of the string fields.
     *@param joinAttr2
     *@param mergeAttr2
     *@param n_pages  IN PAGES
     *@param k number of tuples to return
     *@param relationName1  access hfapfile for right i/p to join
     *@param relationName2
     *@param numOfColumns1
     *@param numOfColumns2
     *@param joinAttrTable1
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public TopK_NRAJoin(
            AttrType[] in1, int len_in1, short t1_str_sizes[],
            FldSpec joinAttr1, FldSpec mergeAttr1,
            AttrType    in2[], int len_in2, short t2_str_sizes[],
            FldSpec joinAttr2, FldSpec mergeAttr2,
            java.lang.String relationName1,
            java.lang.String relationName2,
            int k,
            int  n_pages,
            int numOfColumns1,
            int numOfColumns2,int joinAttrTable1,int joinAttrTable2,int mergeAttrTable1,int mergeAttrTable2
    )
    {

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

        k = k + 10; // TODO: if changed, change line pLBmin = pLB[k-10]

        Tuple temp_tuple = new Tuple();
        float findk = 0;
        float temp1 = 0;
        float temp2 = 0;
        int previous = 0;
        String prevKey = "";
        float maxValue = 100; //TODO: calculate the actual max
        boolean calculateThresh = false;
        boolean continueWhile = true;
        int countK = 0;
        HashMap<String,Integer> tupleTracker = new HashMap<String, Integer>();
        int runningKCount =0;
        float Thresh=0;
        String[][] Bounds = new String[k][2+1];
        Float[] pLB = new Float[k];
        float pLBmin = maxValue;
        float[][] temp = new float[k][2+1];
        //int[][] temp1 = new int[k][2];
        String[][] pLBCalc = new String[k][2+1];  // 2 Based on the fact that we only have two relations
        String[][] pUBCalc = new String[k][2+1];
        String[][] found = new String[k][2];  // used to check if an object is fully seen

        // Set all values in Lower bound calculation array to 0.0
        for(int i = 0; i < k; i++){
            for(int j = 0; j < 3; j++){
                pLBCalc[i][j] = String.valueOf(temp[i][j]);
                pUBCalc[i][j] = String.valueOf(maxValue); // set all of upper bound to max value
                Bounds[i][j] = String.valueOf(temp[i][j]);
                if(j < 2){
                    found[i][j] = "0"; // initialize the value of the found array to 0
                }
            }
        }

        try {
            while (continueWhile) {

                for(int i = 0; i < k; i++) {
                    for (int j = 1; j < 3; j++) {
                        if(j==1){
                            Bounds[i][0] = pLBCalc[i][0]; // set the identifier
                        }
                        temp1 += Float.parseFloat(pLBCalc[i][j]);
                        temp2 += Float.parseFloat(pUBCalc[i][j]);
                    }
                    if(temp1 < pLBmin){
                        pLBmin = temp1;
                    }
                    pLB[i] = temp1;
                    Bounds[i][1] = String.valueOf(temp1);
                    Bounds[i][2] = String.valueOf(temp2);
                    temp1 = 0;
                    temp2 = 0;
                }
                Arrays.sort(pLB, Collections.reverseOrder());
                pLBmin = pLB[k-11];         // TODO: CHANGE IF K +10 is changed.

                if(pLBmin > Thresh){  // EXIT CONDITION
                    continueWhile = false;
                    System.out.println("HIT EXIT CONDITION");
                    System.out.println("TOP K tuples");
                    for(int i = 0;i < k-10; i++){
                        findk = pLB[i];
                        for(int j = 0; j < Bounds.length; j++){
                            if(String.valueOf(findk).compareTo(Bounds[j][1]) == 0 ){
                                System.out.println(Bounds[j][0] + "-->" + Bounds[j][1]);
                            }
                        }
                    }
                    break;
                }
                Thresh = 0;

                for(int i=0; i < 2; i++) {
                    Tuple scanTuple = new Tuple();
                    try {
                        scanTuple.setHdr((short) numOfColumns1, in1, t1_str_sizes);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if((temp_tuple = iscan[i].get_next()) == null){
                        continueWhile = false;
                        System.out.println("No Top K elements can be found from these two tables\n not sufficient tuples");
                        break;
                    }
                    scanTuple.tupleCopy(temp_tuple);
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
                            // update the value of pLB
                // TODO Change tupleTracker.size to appropriate value
                // TODO: if statement for if k objects are found
                            // insert "KEY" or string value in this case
                            pLBCalc[tupleTracker.size() - 1][0] = scanTuple.getStrFld(1);
                            pUBCalc[tupleTracker.size() - 1][0] = scanTuple.getStrFld(1);
                            // if object is partially found it has potential for fully seen so add its identifier.
                            found[tupleTracker.size() - 1][0] = scanTuple.getStrFld(1);
                            // insert the Value
                            pLBCalc[tupleTracker.size() - 1][i+1] = String.valueOf(scanTuple.getIntFld(position));

                            if(i == 0){
                                pUBCalc[tupleTracker.size() - 1][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                found[tupleTracker.size() - 1][1] = "10"; // found on relation 1
                                previous = scanTuple.getIntFld(position);
                                prevKey = scanTuple.getStrFld(1);
                            }else{
                                pUBCalc[tupleTracker.size() - 1][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                found[tupleTracker.size() - 1][1] = "01"; //found on relation 2
                                pUBCalc[tupleTracker.size() - 1][i] = String.valueOf(previous);
                                for(int l = 0; l < k; l++){
                                    if(prevKey.compareTo(pUBCalc[l][0]) == 0) {
                                        pUBCalc[l][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                    }
                                }
                            }
                            Thresh += scanTuple.getIntFld(position);
                        } else {
                            int position = mergeAttrTable1;
                            if(i==1){
                                position = mergeAttrTable2;
                            }
                            int value = tupleTracker.get(scanTuple.getStrFld(1));
                            tupleTracker.put(scanTuple.getStrFld(1), (value+scanTuple.getIntFld(position))/2);
                            //

                            for(int j = 0; j < k; j++) {    // iterate through all objects

                                if(scanTuple.getStrFld(1).compareTo(pLBCalc[j][0]) == 0) { // if object matches
                                    pLBCalc[j][i + 1] = String.valueOf(scanTuple.getIntFld(position));  // add it to LB
                                    pUBCalc[j][i + 1] = String.valueOf(scanTuple.getIntFld(position));  // add to UB
                                    found[j][1] = "11";  // Completely found object (dont change pUBCalc)

                                    //break;
                                }else{  // if object does not match
                                    if(found[j][1].compareTo("11")== 0){
                                        // dont update
                                    }
                                    if(found[j][1].compareTo("10")== 0){
                                        if(i == 0){
                                            //this relation is found
                                        }else{
                                            pUBCalc[j][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                        }
                                    }
                                    if(found[j][1].compareTo("01")== 0){
                                        if(i == 0){
                                            pUBCalc[j][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                        }
                                    }

                                }   // end else (object does not match)
                            }   // end for (iterate through all objects)
                            Thresh += scanTuple.getIntFld(position);
                            if(i == 0){
                                previous = scanTuple.getIntFld(position);
                                prevKey = scanTuple.getStrFld(1);
                            }
                            runningKCount++;
                            if (runningKCount == k+10) { //TODO: exit condition is different
                                //continueWhile = false; //TODO: exit condition pLB > uUB
                                calculateThresh = true;
                                break;
                            }
                        }
                    }
                }
            } // end while ContinueTrue
// NOT SURE IF THIS IS NEEDED
            if (tupleTracker.size()==k) {
                //first k in the list are the top k elements
                printResult(tupleTracker);
            }
// NOT SURE IF ELSE IF IS NEEDED
            else if (tupleTracker.size()>k){
                //vet all elements in hashmap ie find A+B/2 for all top k elements
                System.out.println("Here now");
            }

        }catch(Exception e){    // end try
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


    } // End Constructor

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

