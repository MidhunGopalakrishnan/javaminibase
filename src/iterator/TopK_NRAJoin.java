package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.TableMetadata;

import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import jdk.jfr.Description;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;

import btree.*;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.*;

public class TopK_NRAJoin extends Iterator implements GlobalConst {

    private static final int TOPK_COLUMN_LENGTH = 6;
    private static short REC_LEN1 = 15;
    private static short STR_LEN = 13;
    HashMap<String, TableMetadata> tableMetadataMap = new HashMap<>();

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
     *@param //numOfColumns1
     *@param //numOfColumns2
     *@param //joinAttrTable1
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
            int  n_pages, String outputTableName,HashMap<String, TableMetadata> tableMetadataMap
            )
    {
        boolean updateMetadataTable = false;
        int joinAttrTable1 = joinAttr1.offset;
        int mergeAttrTable1 = mergeAttr1.offset;
        int joinAttrTable2 = joinAttr2.offset;
        int mergeAttrTable2 = mergeAttr2.offset;
        this.tableMetadataMap = tableMetadataMap;
        System.out.println("\n\nJOIN" + joinAttrTable1);
        //boolean[] attributes = new boolean[];
        boolean isString = false;
        boolean isFloat = false;
        if(in1[joinAttrTable1-1].attrType==AttrType.attrString){
            // if Join attr 1 is a String
            isString = true;
            if(joinAttrTable1 == 1){
                // this is the assumed case
                // String is in the first collumn
            }else if(joinAttrTable1 == 2){
                // String is in the Second Collumn
                //TODO: ADD FUNCTIONALITY

            }else if(joinAttrTable1 == 3){
                // String is in the Third Collumn
                //TODO: ADD FUNCTIONALITY

            }
        }
        // Check if Join attr 2 is String and where
        if(in2[joinAttrTable2-1].attrType==AttrType.attrString){
            // if Join attr 2 is a String
            if(joinAttrTable2 == 1){
                // this is the assumed case
                // String is in the first collumn
            }else if(joinAttrTable2 == 2){
                // String is in the Second Collumn
                //TODO: ADD FUNCTIONALITY

            }else if(joinAttrTable2 == 3){
                // String is in the Third Collumn
                //TODO: ADD FUNCTIONALITY

            }
        }
        // check if join attr is Float
        if(in2[joinAttrTable1-1].attrType==AttrType.attrReal) {
            isFloat = true;
        }

        if(in1[joinAttrTable1-1].attrType==AttrType.attrInteger || in1[joinAttrTable1-1].attrType==AttrType.attrReal){
            // if join attr is an int or float
            // TODO: ADD FUNCTIONALITY

        }
        // Table 2
        if(in2[joinAttrTable2-1].attrType==AttrType.attrInteger || in2[joinAttrTable2-1].attrType==AttrType.attrReal){
            // if join attr is an int or float
            // TODO: ADD FUNCTIONALITY

        }

        System.out.println("\n\nMERGE:" + mergeAttrTable1);

        AttrType[] outAttrType = new AttrType[TOPK_COLUMN_LENGTH];
        short[] outAttrSize =null;

        FldSpec[] projlist1 = new FldSpec[len_in1];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for (int i = 0; i < len_in1; i++) {
            projlist1[i] = new FldSpec(rel1, i + 1);
        }

        FldSpec[] projlist2 = new FldSpec[len_in2];
        RelSpec rel2 = new RelSpec(RelSpec.outer);
        for (int i = 0; i < len_in2; i++) {
            projlist2[i] = new FldSpec(rel2, i + 1);
        }

        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        IndexScan[] iscan = new IndexScan[2];
// NEW
        if(!tableMetadataMap.get(relationName1).indexNameList.contains(relationName1 + "UNCLUSTBTREE" + mergeAttrTable1)){
            //create index and add entry in metadata table
            createUnclusteredBTreeIndex(relationName1,  mergeAttrTable1);
        }
        if(!tableMetadataMap.get(relationName2).indexNameList.contains(relationName2 + "UNCLUSTBTREE" + mergeAttrTable2)) {
            //create index
            createUnclusteredBTreeIndex(relationName2,  mergeAttrTable2);
        }
//END
        try {
            iscan[0] = new IndexScan(new IndexType(IndexType.B_Index), relationName1, relationName1 + "UNCLUSTBTREE" + mergeAttrTable1,
                    in1, t1_str_sizes, len_in1, len_in1, projlist1, expr, len_in1, false);
            iscan[1] = new IndexScan(new IndexType(IndexType.B_Index), relationName2, relationName2 + "UNCLUSTBTREE" + mergeAttrTable2,
                    in2, t2_str_sizes, len_in2, len_in2, projlist2, expr, len_in2, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //k = k + 10; // TODO: if changed, change line pLBmin = pLB[k-10]
        boolean oneGone = false;
        boolean twoGone = false;
        int arrSize = 4000;
        Tuple temp_tuple = new Tuple();
        boolean t1 = false;
        boolean t2 = false;
        String tempStr1 = "";
        String tempStr2 = "";
        float tempNum1 = 0;
        float tempNum2 = 0;
        float findk = 0;
        float temp1 = 0;
        float temp2 = 0;
        int previous = 0;
        String prevKey = "";
        float maxValue = 200; //TODO: calculate the actual max
        boolean calculateThresh = false;
        boolean continueWhile = true;
        Boolean rel11 = false;
        Boolean rel22 = false;
        int countK = 0;
        HashMap<String,Integer> tupleTracker = new HashMap<String, Integer>();
        int tupleCount =0;
        float Thresh=0;
        String[][] Bounds = new String[arrSize][5];
        Float[] pLB = new Float[arrSize];
        float pLBmin = maxValue;
        float[][] temp = new float[arrSize][4];
        String[] tempArr1 = new String[4];

        int last = 0;
        int last1 = 0;
        int count = 0;
        String[][] added = new String[arrSize][2];
        String[][] pLBCalc = new String[arrSize][4];  // [String,index,relation,value]
        String[][] pUBCalc = new String[arrSize][4];
        String[][] found = new String[arrSize][2+1];  // used to check if an object is fully seen
        String[] pLBrow = new String[2];
        //ArrayList attempt
//        ArrayList<String[]> pLBCalc = new ArrayList<String[]>;
//        ArrayList<String[]> Bounds = new ArrayList<String[]>;
//        String [] row = new String[3];

        // Set all values in Lower bound calculation array to 0.0
        for(int i = 0; i < arrSize; i++){
            for(int j = 0; j < 4; j++){

                pLBCalc[i][j] = "0.0";
                pUBCalc[i][j] = String.valueOf(maxValue); // set all of upper bound to max value
                Bounds[i][j] = "0.0";
                if(j==3){
                    Bounds[i][j+1] = "0.0";
                }
                if(j < 3){
                    found[i][j] = "0"; // initialize the value of the found array to 0
                }
            }
        }

        try {
            while (continueWhile) {

                // sort Bounds
                // sort based on pLB
                for(int i = 0; i < arrSize-1 && !(Bounds[i][0].compareTo("0.0") == 0); i++){
                    int mindex = i;
                    for(int j = i+1; j<arrSize && !(Bounds[j][0].compareTo("0.0") == 0);j++) {
                        float tempy1 = Float.parseFloat(Bounds[j][3]);
                        float tempy2 = Float.parseFloat(Bounds[mindex][3]);
                        if (tempy1 > tempy2){
                            mindex = j;
                        }
                    }
                    tempArr1 = Bounds[mindex];
                    Bounds[mindex] = Bounds[i];
                    Bounds[i] = tempArr1;
                }
                // tie break with pUB
                for(int i = 1; i < Bounds.length && !(Bounds[i][0].compareTo("0.0")== 0); i++){
                    float check1 = Float.parseFloat(Bounds[i][3]);
                    float check2 = Float.parseFloat(Bounds[i-1][3]);
                    if(check1 == check2 && check1 != 0 ){
                        for(int j = 0; j < Bounds.length && !(Bounds[j][0].compareTo("0.0")== 0) ; j++){
                            // the current higher ranked in the tie
                            if(String.valueOf(pLB[i]).compareTo(Bounds[j][1]) == 0 && !t1){
                                tempNum1 = Float.parseFloat(Bounds[j][2]);
                                t1 = true;
                                continue;
                            }
                            // the current lower ranked in the tie
                            if(String.valueOf(pLB[i-1]).compareTo(Bounds[j][1]) == 0){
                                tempNum2 = Float.parseFloat(Bounds[j][2]);
                                t2 = true;
                                continue;
                            }
                            if(t1 && t2){
                                break;
                            }
                        }
                        // if tempNum1 and 2 were found
                        if(t1 && t2) {
                            // if current lower is higher in pUB swap them in the list
                            if (tempNum2 > tempNum1) {
                                tempNum1 = pLB[i];
                                pLB[i] = pLB[i - 1];
                                pLB[i - 1] = tempNum1;
                            }
                        }
                    }
                } // end for loop (duplicate conditional sorting)
// TODO: CONDITIONAL OUTPUT BASED ON INPUT DATA TYPES ie. float, int, string
// This may be done
                //initialize for output Tuple
                Heapfile outHeap = null;
                Tuple outTuple = new Tuple();
                if (!outputTableName.equals("")){
                    if (!outputTableName.equals("")) {
                        outHeap = new Heapfile(outputTableName);
                        //populate the attrType and attrSize and build a Tuple Header
                        //outAttrType[0] = new AttrType(in1[joinAttrTable1-1].attrType);
                        if(isString) {
                            outAttrType[0] = new AttrType(AttrType.attrString);
                        }else if(isFloat){
                            outAttrType[0] = new AttrType(AttrType.attrReal);
                        }else{
                            outAttrType[0] = new AttrType(AttrType.attrInteger);
                        }
                        outAttrType[1] = new AttrType(AttrType.attrReal);
                        //outAttrType[2] = new AttrType(in1[mergeAttrTable1-1].attrType);
                        outAttrType[2] = new AttrType(AttrType.attrReal);
                        //outAttrType[3] = new AttrType(in2[mergeAttrTable2-1].attrType);
                        outAttrType[3] = new AttrType(AttrType.attrReal);
                        outAttrType[4] = new AttrType(AttrType.attrInteger);
                        outAttrType[5] = new AttrType(AttrType.attrInteger);

                        if(in1[joinAttrTable1-1].attrType==AttrType.attrString){
                            outAttrSize = new short[1];
                            outAttrSize[0] = STR_LEN;
                        }

                        try {
                            outTuple.setHdr((short) outAttrType.length, outAttrType, outAttrSize);
                        } catch (Exception e) {
                            System.err.println("*** error in Tuple.setHdr() ***");
                            e.printStackTrace();
                        }

                    }
                }

                // explicitly set pLBmin to kth pLB value because it is sorted now
                // pLBmin = pLB[k-1];         // TODO: CHANGE IF K +10 is changed. //Done
                pLBmin = Float.parseFloat(Bounds[k-1][3]);
                boolean setZero = false;
                if(pLBmin > Thresh /*&& k < tupleTracker.size()*/){  // EXIT CONDITION
                    continueWhile = false;
                    System.out.println("HIT EXIT CONDITION");
                    System.out.println("TOP K tuples");
                    System.out.printf("%-20s%-8s%-14s%-14s%-8s%-8s\n","Join Attribute","Sum","Merge Attr1","Merge Attr2","Index1","Index2");
                    for(int i = 0;i < k; i++){
                        if(setZero){
                            findk = Float.parseFloat(Bounds[0][3]);
                        }
                        else{
                            findk = Float.parseFloat(Bounds[i][3]);
                        }
                        // print the values
                        for(int j = 0; j < Bounds.length; j++){
                            if(String.valueOf(findk).compareTo(Bounds[j][3]) == 0 ){
                                System.out.printf("%-20s%-8s",Bounds[j][0], Bounds[j][3]);
                                if(Bounds[j][1].compareTo("$") == 0){
                                    Bounds[j][1] = "-1";
                                }
                                if(Bounds[j][2].compareTo("$") == 0){
                                    Bounds[j][2] = "-1";
                                }
                                String tupleVal1 = Bounds[j][1];
                                String tupleVal2 = Bounds[j][2];
                                String foundVal1 = "";
                                String foundVal2 = "";
                                for(int m = 0; m < pLBCalc.length && !(pLBCalc[m][0].compareTo("0.0") == 0);m++){
                                    if(tupleVal1.compareTo(pLBCalc[m][1]) == 0){
                                        foundVal1 = pLBCalc[m][3];
                                    }
                                    if(tupleVal2.compareTo(pLBCalc[m][1]) == 0){
                                        foundVal2 = pLBCalc[m][3];
                                    }
                                }
                                if(foundVal1.compareTo("") == 0){
                                    foundVal1 = "0";
                                }
                                if(foundVal2.compareTo("") == 0){
                                    foundVal2 = "0";
                                }

                                //insert to output Heap File
                                if(!outputTableName.equals("")){
                                    //outTuple.setStrFld(1,Bounds[j][0]);
                                    if(isString) {
                                        outTuple.setStrFld(1,Bounds[j][0]);
                                    }else if(isFloat){
                                        outTuple.setFloFld(1,Float.parseFloat(Bounds[j][0]));
                                    }else{
                                        outTuple.setIntFld(1,Integer.parseInt(Bounds[j][0]));
                                    }
                                    outTuple.setFloFld(2,Float.parseFloat(Bounds[j][3]));
                                    outTuple.setFloFld(3,Float.parseFloat(foundVal1));
                                    outTuple.setFloFld(4,Float.parseFloat(foundVal2));
                                    outTuple.setIntFld(5,Integer.parseInt(tupleVal1));
                                    outTuple.setIntFld(6,Integer.parseInt(tupleVal2));

                                    outHeap.insertRecord(outTuple.returnTupleByteArray());
                                    updateMetadataTable = true;
                                }
                                System.out.printf("%-14s%-14s", foundVal1,foundVal2);
                                System.out.printf("%-8s%-8s\n",tupleVal1, tupleVal2);
                                for(int l = j; l < Bounds.length-1; l++){
                                    Bounds[l] = Bounds[l+1];
                                    pLB[l] = pLB[l+1];
                                    setZero = true;
                                }
                                break;
                            }
                        }
                    }
                    break;
                } // end Exit Condition

                // set Thresh back to 0 so it can accumulate for the current row
                Thresh = 0;
                int gone1 = -1;
                int gone2 = -1;
                // for loop to access all the tuples and store them in various arrays.
                for(int i=0; i < 2; i++) {
                    if(twoGone && oneGone){
                        continueWhile = false;
                        break;
                    }
                    if(gone1 == i){
                        continue;
                    }
                    if(gone2 == i){
                        continue;
                    }

                    Tuple scanTuple = new Tuple();
                    if (i == 0) {
                        try {
                        scanTuple.setHdr((short) len_in1, in1, t1_str_sizes);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    }else{
                        try {
                            scanTuple.setHdr((short) len_in2, in2, t2_str_sizes);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if((temp_tuple = iscan[i].get_next()) == null){
// TODO: Length of files
                        //continueWhile = false;
                        if(i==1){
                            gone1 = i;
                            oneGone = true;
                        }else if(i ==0){
                            gone2 = i;
                            twoGone = true;
                        }

                        break;
                    }
                    scanTuple.tupleCopy(temp_tuple);
                    if (scanTuple == null && countK < k) {
                        System.out.println("No Top K elements can be found from these two tables");
                    } else if (scanTuple != null) {
//Comment line below to surpress output
                        if(i == 0) {
                            //scanTuple.print(in1);
                        }else {
                            //scanTuple.print(in2);
                        }
                        //                      if (!tupleTracker.containsKey(scanTuple.getStrFld(1))) { //TODO : change hardcoded value of 1. First value is not always the join attribute, also it wont be string always
                        int position = mergeAttrTable1;
                        if(i==1){
                            position = mergeAttrTable2;
                        }

                        if(i == 0) {
                            if (isString) {
                                // add String
                                tupleTracker.put(scanTuple.getStrFld(joinAttrTable1), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = scanTuple.getStrFld(joinAttrTable1);
                                pUBCalc[tupleCount][0] = scanTuple.getStrFld(joinAttrTable1);
                                found[tupleCount][0] = scanTuple.getStrFld(joinAttrTable1);
                            } else if (isFloat) {
                                // add float
                                tupleTracker.put(String.valueOf(scanTuple.getFloFld(joinAttrTable1)), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable1));
                                pUBCalc[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable1));
                                found[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable1));
                            } else {
                                // add int
                                tupleTracker.put(String.valueOf(scanTuple.getIntFld(joinAttrTable1)), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable1));
                                pUBCalc[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable1));
                                found[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable1));
                            }
                        }else{
                            if (isString) {
                                // add String
                                tupleTracker.put(scanTuple.getStrFld(joinAttrTable2), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = scanTuple.getStrFld(joinAttrTable2);
                                pUBCalc[tupleCount][0] = scanTuple.getStrFld(joinAttrTable2);
                                found[tupleCount][0] = scanTuple.getStrFld(joinAttrTable2);
                            } else if (isFloat) {
                                // add float
                                tupleTracker.put(String.valueOf(scanTuple.getFloFld(joinAttrTable2)), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable2));
                                pUBCalc[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable2));
                                found[tupleCount][0] = String.valueOf(scanTuple.getFloFld(joinAttrTable2));
                            } else {
                                // add int
                                tupleTracker.put(String.valueOf(scanTuple.getIntFld(joinAttrTable2)), scanTuple.getIntFld(position));
                                pLBCalc[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable2));
                                pUBCalc[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable2));
                                found[tupleCount][0] = String.valueOf(scanTuple.getIntFld(joinAttrTable2));
                            }
                        }
                        // update the value of pLBcalc

                        // insert "KEY" or join attr value in this case
                        // add "index" unique identifier
                        pLBCalc[tupleCount][1] = String.valueOf(tupleCount);
                        pUBCalc[tupleCount][1] = String.valueOf(tupleCount);
                        // add relation #
                        pLBCalc[tupleCount][2] = String.valueOf(i);
                        pUBCalc[tupleCount][2] = String.valueOf(i);

                        // if object is partially found it has potential to become fully seen so add its identifier.
// moved up to if, else if, else
//
                        found[tupleCount][1] = String.valueOf(tupleCount);
                        // insert the Value
                        // always insert 2
                        pLBCalc[tupleCount][3] = String.valueOf((float)scanTuple.getIntFld(position));

                        //if(i == 0){
                        pUBCalc[tupleCount][3] = String.valueOf((float)scanTuple.getIntFld(position));
                        // TODO: found does not have which relation
                        found[tupleCount][2] = "1"; // found on relation 1
                        previous = scanTuple.getIntFld(position);
                        // Add all values for the pUB unless found correct values already
                        for(int j = 0; j < arrSize; j++) {
                            if(found[j][2].compareTo("1")== 0){
                                // dont update. Already Fully found
                            }else{
                                pUBCalc[j][3] = String.valueOf(scanTuple.getIntFld(position));
                            }
                        } // end for loop
                        tupleCount++;
                        Thresh += scanTuple.getIntFld(position);
                       /* } else {
                            int position = mergeAttrTable1;
                            if(i==1){
                                position = mergeAttrTable2;
                            }
                            int value = tupleTracker.get(scanTuple.getStrFld(1));
                            //tupleTracker.put(scanTuple.getStrFld(1), (value+scanTuple.getIntFld(position))/2);
                            //

                            for(int j = 0; j < arrSize; j++) {    // iterate through all objects

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
                            if (runningKCount == k+10) { //TODO: exit condition is different This is not needed
                                //continueWhile = false; //TODO: exit condition pLB > uUB
                                calculateThresh = true;
                                break;
                            }
                        }*/
                    }
                }
 // ATTEMPT TO ADD BOUNDS CALC AT THE END
                float temp11 = 0;
                float temp21 = 0;
                boolean complete = false;
                boolean notIncluded = false;
                boolean notIncluded1 = false;
                boolean notIncluded2 = false;
                boolean notIncluded3 = false;
                for(int i = count; i < arrSize && !(pLBCalc[i][0].compareTo("0.0") == 0);i++){
                    String checkerLB = pLBCalc[i][0];
                    pLBrow[i%2] = pLBCalc[i][0];
                    last = 0;
                    last1 = 0;
                    for(int j = 0; j < Bounds.length ; j++){
                        if(count > 0 && Bounds[j][0].compareTo("0.0") == 0){
                            notIncluded = contains(Bounds,pLBrow[0]);
                            notIncluded1 = contains(Bounds,pLBrow[1]);
                            notIncluded2 = contains2(Bounds,pLBrow[0],pLBCalc[i][1]);
                            //notIncluded3 = contains2(Bounds,pLBrow[1],pLBCalc[i][1]);
                            // add element That have new Join attr
                            if(!notIncluded){
                                int check = 0;
                                while(!(pLBCalc[check][0].compareTo("0.0") == 0)){
                                    if((pLBCalc[check][0].compareTo(pLBrow[0]) == 0)){
                                        Bounds[count][0] = pLBCalc[check][0];
                                        if(pLBCalc[check][2].compareTo("0") == 0) {
                                            Bounds[count][1] = pLBCalc[check][1];
                                            Bounds[count][2] = "$";
                                        }else{
                                            Bounds[count][2] = pLBCalc[check][1];
                                            Bounds[count][1] = "$";
                                        }
                                        Bounds[count][3] = pLBCalc[check][3];
                                        Bounds[count][4] = pUBCalc[check][3];
                                        count++;
                                    }
                                    check++;
                                }

                            }
                            // add element that has new join attr
                            if(!notIncluded1){
                                int check = 0;
                                while(!(pLBCalc[check][0].compareTo("0.0") == 0)){
                                    if((pLBCalc[check][0].compareTo(pLBrow[1]) == 0)){
                                        Bounds[count][0] = pLBCalc[check][0];
                                        if(pLBCalc[check][2].compareTo("0") == 0) {
                                            Bounds[count][1] = pLBCalc[check][1];
                                            Bounds[count][2] = "$";
                                        }else{
                                            Bounds[count][2] = pLBCalc[check][1];
                                            Bounds[count][1] = "$";
                                        }
                                        Bounds[count][3] = pLBCalc[check][3];
                                        Bounds[count][4] = pUBCalc[check][3];
                                        count++;
                                    }
                                    check++;
                                }
                            }
                            break;
                        }
                        String checkerB = Bounds[j][0];
                        if(checkerB.compareTo("2") == 0){
                            System.out.print("");
                        }
                        // if same string
                        if(checkerLB.compareTo(checkerB) == 0){
                            rel11 = false;
                            rel22 = false;
                            // found in Bounds seen 2+ The String is in bounds at least once.
                            if(pLBCalc[i][2].compareTo("0") == 0 ){
                                rel11 = true;
                            }
                            if(pLBCalc[i][2].compareTo("1") == 0){
                                // if  i is in second relation in other relation
                                rel22 = true;
                            }

                            if(rel11){ // if new String that has been seen before is in relation 1
                                for(int l = last; l < arrSize && !(pLBCalc[l][0].compareTo("0.0") == 0); l++){
                                    if(pLBCalc[l][2].compareTo("1") == 0 && pLBCalc[l][0].compareTo(Bounds[j][0]) == 0){
                                        Bounds[count][0] = pLBCalc[l][0];

                                            Bounds[count][1] = pLBCalc[i][1];
                                            Bounds[count][2] = pLBCalc[l][1];
                                            Bounds[count][3] = String.valueOf(Float.parseFloat(pLBCalc[i][3]) + Float.parseFloat(pLBCalc[l][3]));
                                            Bounds[count][4] = String.valueOf(Float.parseFloat(pUBCalc[i][3]) + Float.parseFloat(pUBCalc[l][3]));
                                            count++;
                                        complete = true;
                                        rel11 = false;
                                        boolean more = contains3(Bounds,pLBCalc[l][0],l+1,count-2);
                                        last = l+1;
                                            break;
                                    }
                                }
                            }
                            if(rel22){
                                for(int l = last1; l < arrSize && !(pLBCalc[l][0].compareTo("0.0") == 0); l++){
                                    if(pLBCalc[l][2].compareTo("0") == 0 && pLBCalc[l][0].compareTo(Bounds[j][0]) == 0 && !containsMatch(Bounds,pLBCalc[l][1],pLBCalc[i][1])){
                                        Bounds[count][0] = pLBCalc[l][0];

                                        Bounds[count][2] = pLBCalc[i][1];
                                        Bounds[count][1] = pLBCalc[l][1];
                                        Bounds[count][3] = String.valueOf(Float.parseFloat(pLBCalc[i][3]) + Float.parseFloat(pLBCalc[l][3]));
                                        Bounds[count][4] = String.valueOf(Float.parseFloat(pUBCalc[i][3]) + Float.parseFloat(pUBCalc[l][3]));
                                        count++;

                                        complete = true;
                                        rel11 = false;
                                        boolean more = contains3(Bounds,pLBCalc[l][0],l+1,count-2);
                                        last1 = l+1;

                                        break;

                                    }
                                }
                            }
                        }else if(checkerLB.compareTo(pLBCalc[i+1][0]) == 0){
                            // next value is the same as current seen 2+
                            // need to add pLB to Bounds, then check all of bounds for same String
                            if(containsMatch(Bounds,pLBCalc[i][1],pLBCalc[i+1][1])){
                                break;
                            }
                            Bounds[count][0] = pLBCalc[i][0];
                            if(Integer.parseInt(pLBCalc[i][1])%2 == 0) {
                                Bounds[count][1] = pLBCalc[i][1];
                                Bounds[count][2] = pLBCalc[i + 1][1];
                            }else if(Integer.parseInt(pLBCalc[i][1])%2 == 1){
                                Bounds[count][2] = pLBCalc[i][1];
                                Bounds[count][1] = pLBCalc[i + 1][1];
                            }
                            Bounds[count][3] = String.valueOf(Float.parseFloat(pLBCalc[i][3]) + Float.parseFloat(pLBCalc[i+1][3]));
                            Bounds[count][4] = String.valueOf(Float.parseFloat(pUBCalc[i][3]) + Float.parseFloat(pUBCalc[i+1][3]));
                            count++;
                            added[count][0] = pLBCalc[i][1];
                            added[count][1] = pLBCalc[i+1][1];;
                            // add both individually
                            Bounds[count][0] = pLBCalc[i][0];
                            Bounds[count+1][0] = pLBCalc[i+1][0];
                            if(Integer.parseInt(pLBCalc[i][1])%2 == 0) {
                                Bounds[count][1] = pLBCalc[i][1];
                                Bounds[count+1][1] = "$";
                                Bounds[count][2] = "$";
                                Bounds[count+1][2] = pLBCalc[i+1][1];
                            }else if(Integer.parseInt(pLBCalc[i][1])%2 == 1){
                                Bounds[count][2] = pLBCalc[i][1];
                                Bounds[count+1][2] = "$";
                                Bounds[count][1] = "$";
                                Bounds[count+1][1] = pLBCalc[i+1][1];
                            }
                            Bounds[count][3] = pLBCalc[i][3];
                            Bounds[count + 1][3] = pLBCalc[i+1][3];
                            Bounds[count][4] = String.valueOf(Float.parseFloat(pUBCalc[i][3]) + Float.parseFloat(pUBCalc[i+1][3]));
                            Bounds[count+1][4] = String.valueOf(Float.parseFloat(pUBCalc[i][3]) + Float.parseFloat(pUBCalc[i+1][3]));
                            count+=2;

                            break;


                        }else if(Bounds[0][0].compareTo("0.0") == 0){ //empty first add
                            // not in Bounds Partially seen
                            Bounds[0][0] = pLBCalc[i][0];
                            if(pLBCalc[i][2].compareTo("0") == 0 ){
                                Bounds[0][1] = pLBCalc[i][1];
                                Bounds[0][2] = "$";
                            }else{
                                Bounds[0][1] = "$";
                                Bounds[0][2] = pLBCalc[i][1];
                            }
                            Bounds[0][3] = pLBCalc[i][3];
                            Bounds[0][4] = pUBCalc[i][3];
                            count++;
                            // add second item
                            Bounds[count][0] = pLBCalc[i+1][0];
                            if(pLBCalc[i+1][2].compareTo("0") == 0 ){
                                Bounds[count][1] = pLBCalc[i][1+1];
                                Bounds[count][2] = "$";
                            }else{
                                Bounds[count][1] = "$";
                                Bounds[count][2] = pLBCalc[i+1][1];
                            }
                            Bounds[count][3] = pLBCalc[i+1][3];
                            Bounds[count][4] = pUBCalc[i+1][3];
                            count++;
                            break;
                        }else if(true){

                        }
                        // NEW !!!!!!!!!!!!!!!!!
                        notIncluded2 = contains2(Bounds,pLBCalc[i][0],pLBCalc[i][1]);
                        // Join attr is not new but index of the join attr is new
                        if(!notIncluded2){
                            int check = 0;
                            Bounds[count][0] = pLBCalc[i][0];
                            if(pLBCalc[i][2].compareTo("0") == 0) {
                                Bounds[count][1] = pLBCalc[i][1];
                                Bounds[count][2] = "$";
                            }else{
                                Bounds[count][2] = pLBCalc[i][1];
                                Bounds[count][1] = "$";
                            }
                            Bounds[count][3] = pLBCalc[i][3];
                            Bounds[count][4] = pUBCalc[i][3];
                            count++;
                            //}
                            check++;
                            //}
                        }
                        //END NEW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    }
                }
 // END ATTEMPT
            } // end while ContinueTrue

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

        //update metadatatable
        if(updateMetadataTable){
            TableMetadata tm = new TableMetadata(outputTableName, outAttrType, outAttrSize);
            tableMetadataMap.put(outputTableName, tm);
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

    public static boolean contains(String[][] arr, String item) {

        for (int count = 0; !(arr[count][0].compareTo("0.0") == 0) ;count++) {
            if (item.compareTo(arr[count][0]) == 0) {
                return true;
            }

        }
        return false;
    }
    public static boolean containsMatch(String[][] arr, String item, String item2) {

        for (int count = 0; !(arr[count][0].compareTo("0.0") == 0) ;count++) {
            if (item.compareTo(arr[count][1]) == 0) {
                if(item2.compareTo(arr[count][2]) == 0) {
                    return true;
                }
            }

        }
        return false;
    }

    public static boolean contains2(String[][] arr, String item, String item1) {

        for (int count = 0; !(arr[count][0].compareTo("0.0") == 0) ;count++) {

            if (item.compareTo(arr[count][0]) == 0) {
                if(item1.compareTo(arr[count][1]) == 0){
                    return true;
                }
                if(item1.compareTo(arr[count][2]) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean contains3(String[][] arr,String item, int count, int last){
        for(int count1 = count; count1 < last; count1++){
            if(arr[count1][0].compareTo(item) == 0) {
                return true;
            }
        }
        return false;
    }

    private void createUnclusteredBTreeIndex(String tableName, int attrNo) {
        String indexFileName = tableName+"UNCLUST"+"BTREE"+attrNo;
        // if already index present, print index already present else create

        TableMetadata tm = tableMetadataMap.get(tableName);
        // Check for TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
        if(!tm.getIndexNameList().contains(indexFileName)) {
            Heapfile hf = null;
            try {
                hf = new Heapfile(tableName);
            } catch (Exception e) {
            }

            Scan scan = null;

            try {
                scan = new Scan(hf);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

            AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
            short[] attrSize = tableMetadataMap.get(tableName).attrSize;

            Tuple temp = null;
            Tuple t = new Tuple();
            try {
                t.setHdr((short) attrType.length, attrType, attrSize);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
            RID rid = new RID();
            BTreeFile btf = null;
            try {
                //Convention Name : TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
                btf = new BTreeFile(indexFileName, attrType[attrNo-1].attrType, REC_LEN1, 1/*delete*/);
                KeyClass key = null;
                while ((temp = scan.getNext(rid)) != null) {
                    t.tupleCopy(temp);
                    if (attrType[attrNo-1].attrType == AttrType.attrInteger) {
                        int intKey = t.getIntFld(attrNo);
                        key = new IntegerKey(intKey);
                    } else if (attrType[attrNo-1].attrType == AttrType.attrString) {
                        String strKey = t.getStrFld(attrNo);
                        key = new StringKey(strKey);
                    }
                    btf.insert(key, rid);
                }
            } catch (Exception e) {
                System.err.println("*** BTree File error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            } finally {
                scan.closescan();
            }
            try {
                btf.close();
            } catch (Exception e) {
                System.err.println("*** BTree File closing error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

            updateIndexEntryToTableMetadata(tableName, indexFileName);
        } else {
            System.out.println("BTree index already exist for table "+tableName+ " - "+attrNo);
        }

    }

    private void updateIndexEntryToTableMetadata(String tableName, String indexFileName) {
        TableMetadata tm = tableMetadataMap.get(tableName);
        if(!tm.getIndexNameList().contains(indexFileName)) {
            tm.getIndexNameList().add(indexFileName);
        }
    }

}

