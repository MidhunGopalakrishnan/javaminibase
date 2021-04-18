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
import java.util.ArrayList;
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

        //k = k + 10; // TODO: if changed, change line pLBmin = pLB[k-10]
        int arrSize = 4000;
        Tuple temp_tuple = new Tuple();
        boolean t1 = false;
        boolean t2 = false;
        String tempStr1 = "";
        String tempStr2 = "";
        float tempNum1 = 0;
        float tempNum2 = 0;
        int findk = 0;
        float temp1 = 0;
        float temp2 = 0;
        int previous = 0;
        String prevKey = "";
        float maxValue = 200; //TODO: calculate the actual max
        boolean calculateThresh = false;
        boolean continueWhile = true;
        Boolean rel11 = false;
        int countK = 0;
        HashMap<String,Integer> tupleTracker = new HashMap<String, Integer>();
        int tupleCount =0;
        float Thresh=0;
        String[][] Bounds = new String[arrSize][5];
        Float[] pLB = new Float[arrSize];
        float pLBmin = maxValue;
        float[][] temp = new float[arrSize][4];
        String[] tempArr1 = new String[4];

        int count = 0;
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

                pLBCalc[i][j] = "0";
                pUBCalc[i][j] = String.valueOf(maxValue); // set all of upper bound to max value
                Bounds[i][j] = "0";
                if(j==3){
                    Bounds[i][j+1] = "0";
                }
                if(j < 3){
                    found[i][j] = "0"; // initialize the value of the found array to 0
                }
            }
        }

        try {
            while (continueWhile) {
                // for loop to calculate Bounds array, pLBmin(minimun lower bound)

                float temp11 = 0;
                float temp21 = 0;
                boolean complete = false;
                boolean notIncluded = false;
                boolean notIncluded1 = false;
                for(int i = count; i < arrSize && !(pLBCalc[i][0].compareTo("0") == 0);i++){
                    String checkerLB = pLBCalc[i][0];
                    pLBrow[i%2] = pLBCalc[i][0];
                    for(int j = 0; j < Bounds.length ; j++){
                        if(count > 0 && Bounds[j][0].compareTo("0") == 0){
                            notIncluded = contains(Bounds,pLBrow[0]);
                            notIncluded1 = contains(Bounds,pLBrow[1]);
                            if(!notIncluded){
                                int check = 0;
                                while(!(pLBCalc[check][0].compareTo("0") == 0)){
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
                            if(!notIncluded1){
                                int check = 0;
                                while(!(pLBCalc[check][0].compareTo("0") == 0)){
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
                        if(checkerLB.compareTo(checkerB) == 0){
                            rel11 = false;
                            // found in Bounds seen 2+ The String is in bounds at least once.
                            if(pLBCalc[i][2].compareTo("0") == 0 ){
                                rel11 = true;
                            }
                            if(rel11){ // if new String that has been seen before is in relation 1
                                for(int l = 0; l < arrSize && !(pLBCalc[l][0].compareTo("0") == 0); l++){
                                    if(pLBCalc[l][2].compareTo("1") == 0 && pLBCalc[l][0].compareTo(Bounds[j][0]) == 0){
                                        Bounds[j][0] = pLBCalc[l][0];
                                        if(Bounds[j][1].compareTo("$") == 0){
                                            Bounds[j][1] = pLBCalc[i][1];
                                        }else if(Bounds[j][2].compareTo("$") == 0){
                                            Bounds[j][2] = pLBCalc[l][1];
                                        }
                                        //Bounds[j][1] = pLBCalc[i][1];
                                        //Bounds[j][2] = pLBCalc[l][1];
                                        if(Bounds[j][3].compareTo(String.valueOf(Integer.parseInt(pLBCalc[i][3]) + Integer.parseInt(pLBCalc[l][3]))) > 0 ) {

                                        }else{
                                            Bounds[j][3] = String.valueOf(Integer.parseInt(pLBCalc[i][3]) + Integer.parseInt(pLBCalc[l][3]));
                                        }

                                        complete = true;
                                        rel11 = false;
                                        break;
                                    }
                                }
                            }
                            //if(Bounds[j][1])
                        }else if(checkerLB.compareTo(pLBCalc[i+1][0]) == 0){
                            // next value is the same as current seen 2+
                            // need to add pLB to Bounds, then check all of bounds for same String
                            Bounds[count][0] = pLBCalc[i][0];
                            Bounds[count][1] = pLBCalc[i][1];
                            Bounds[count][2] = pLBCalc[i+1][1];
                            Bounds[count][3] = String.valueOf(Integer.parseInt(pLBCalc[i][3]) + Integer.parseInt(pLBCalc[i+1][3]));

                            count++;

                            break;
                            /*for(int l = 0; l < Bounds.length && !(Bounds[l][0].compareTo("0") == 0);l++){
                                if(l==j){

                                }else{ // if not the same index of Bounds
                                    if(Bounds[l][0].compareTo(Bounds[j][0]) == 0){ // if the letter matches
                                        if (Bounds[l][1].compareTo(Bounds[j][1]) == 0 ) {

                                        }else{// if Same String with different index1
                                            for(int m = 0; m < arrSize && !(pLBCalc[m][0].compareTo("0") == 0); m++){ // iterate through pLBCalc for the correct index
                                                if(pLBCalc[m][1].compareTo(Bounds[l][1]) == 0 ){
                                                    Bounds[count][0] = pLBCalc[m][0];
                                                    if(pLBCalc[m][2].compareTo("0") == 0){
                                                        Bounds[count][1] = pLBCalc[m][1];
                                                        Bounds[count][2] = pLBCalc[i+1][1];
                                                    }else{
                                                        Bounds[count][2] = pLBCalc[m][1];
                                                        Bounds[count][1] = pLBCalc[i+1][1];
                                                    }
                                                    Bounds[count][3] = String.valueOf(Integer.parseInt(pLBCalc[m][3]) + Integer.parseInt(pLBCalc[i+1][3]));
                                                    count++;
                                                    break;
                                                }
                                            }
                                        }
                                        if(Bounds[l][2].compareTo(Bounds[j][2]) == 0 ) {

                                        }else{// if Same String with different index2
                                            for(int m = 0; m < arrSize && !(pLBCalc[m][0].compareTo("0") == 0); m++){
                                                if(pLBCalc[m][2].compareTo(Bounds[l][2]) == 0 ){
                                                    Bounds[count][3] = String.valueOf(Integer.parseInt(pLBCalc[m][3]) + Integer.parseInt(pLBCalc[i+1][3]));
                                                    count++;
                                                    break;
                                                }
                                            }
                                        }
                                        if((Bounds[l][2].compareTo("$") == 0)){ // and if on the other relation

                                        }
                                    }
                                }
                            }*/

                        }else if(Bounds[0][0].compareTo("0") == 0){ //empty first add
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
                            count++;
                            break;
                        }else if(true){

                        }

                    }
                }
                /*
                if (pLBCalc[0][0].compareTo("0") == 0) {

                } else{
                    for (int i = 0; i < arrSize && !(pLBCalc[i][0].compareTo("0") == 0); i += 2) {
                        count = 0;
                        temp11 = Float.parseFloat(pLBCalc[i][3]);
                        temp21 = Float.parseFloat(pUBCalc[i][3]);
                        for (int j = 0; j < arrSize; j++) {
                            if(pLBCalc[j][0].compareTo(pLBCalc[i][0]) != 0){
                                continue;
                            }
                            //while(pLBCalc[count][0].compareTo(pLBCalc[i][0])==0 ) {
                            //if(pLBCalc[j][0].compareTo(pLBCalc[i][0]) == 0) {
                            if (pLBCalc[j][2].compareTo("1") == 0) {
                                if(pLBCalc[i][0].compareTo(Bounds[count][0] ) != 0 && !complete){
                                    count++;
                                    j--;
                                    continue;
                                }
                                temp1 = temp11 + Float.parseFloat(pLBCalc[j][3]);
                                temp2 = temp21 + Float.parseFloat(pUBCalc[j][3]);
                                // Bounds contains the value for pLB and pUB with the Key
                                Bounds[count][0] = pLBCalc[i][0]; // set the String
                                Bounds[count][1] = pLBCalc[i][1];
                                Bounds[count][2] = pLBCalc[j][1];
                                Bounds[count][3] = String.valueOf((int) temp1);
                                Bounds[count][4] = String.valueOf((int) temp2);
                                count++;
                                temp1 = 0;
                                temp2 = 0;
                                complete = true;
                            }
                            //}
//                            else if (!(pLBCalc[j][0].compareTo("0") == 0) && !(pLBCalc[j][1].compareTo(pLBCalc[i][1]) == 0)) {
//                                Bounds[count][0] = pLBCalc[i][0]; // set the String
//                                Bounds[count][1] = pLBCalc[i][2];
//                                Bounds[count][2] = pLBCalc[j][2];
//                                Bounds[count][3] = String.valueOf((int) temp11);
//                                Bounds[count][4] = String.valueOf((int) temp21);
//                                count++;
//                            }
                            //}
                        }
                        for(int l = 0; l < 2; l++) {
                            if (!complete) {
                                Bounds[count][0] = pLBCalc[l][0]; // set the String
                                //Bounds[count + 1][0] = pLBCalc[l][0];
                                if (pLBCalc[i][2].compareTo("0") == 0) {
                                    Bounds[count][1] = pLBCalc[l][2];
                                    Bounds[count][2] = "$";
//                                    Bounds[count + 1][1] = pLBCalc[i + 1][2];
//                                    Bounds[count + 1][2] = "$";
                                } else {
                                    Bounds[count][1] = "$";
                                    Bounds[count][2] = pLBCalc[l][2];
//                                    Bounds[count + 1][1] = "$";
//                                    Bounds[count + 1][2] = pLBCalc[i + 1][2];
                                }
//                                Bounds[count][3] = String.valueOf((int) temp11);
//                                Bounds[count][4] = String.valueOf((int) temp21);
                                Bounds[count ][3] = pLBCalc[l][3];
                                Bounds[count][4] = pUBCalc[l][3];
                                count++;
                            }
                        }
                        //pLB[i] = temp1;
                    } // end for loop
                }

                 */
                // sort pLB
                //Arrays.sort(pLB, Collections.reverseOrder());
                // sort based on pLB
                for(int i = 0; i < arrSize-1 && !(Bounds[i][0].compareTo("0") == 0); i++){
                    int mindex = i;
                    for(int j = i+1; j<arrSize;j++) {
                        int tempy1 = Integer.parseInt(Bounds[j][3]);
                        int tempy2 = Integer.parseInt(Bounds[mindex][3]);
                        if (tempy1 > tempy2){
                            mindex = j;
                        }
                    }
                    tempArr1 = Bounds[mindex];
                    Bounds[mindex] = Bounds[i];
                    Bounds[i] = tempArr1;
                }
                //Arrays.sort(Bounds, (a, b) -> Integer.parseInt(a[3]) - Integer.parseInt(b[3]) );
                // tie break with pUB
                for(int i = 1; i < Bounds.length && !(Bounds[i][0].compareTo("0")== 0); i++){
                    float check1 = Float.parseFloat(Bounds[i][3]);
                    float check2 = Float.parseFloat(Bounds[i-1][3]);
                    if(check1 == check2 && check1 != 0 ){
                        for(int j = 0; j < Bounds.length && !(Bounds[j][0].compareTo("0")== 0) ; j++){
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

                // explicitly set pLBmin to kth pLB value because it is sorted now
                // pLBmin = pLB[k-1];         // TODO: CHANGE IF K +10 is changed. //Done
                pLBmin = Float.parseFloat(Bounds[k-1][3]);
                boolean setZero = false;
                if(pLBmin > Thresh && k < tupleTracker.size()){  // EXIT CONDITION
                    continueWhile = false;
                    System.out.println("HIT EXIT CONDITION");
                    System.out.println("TOP K tuples");
                    for(int i = 0;i < k; i++){
                        if(setZero){
                            findk = Integer.parseInt(Bounds[0][3]);
                        }
                        else{
                            findk = Integer.parseInt(Bounds[i][3]);
                        }
                        // print the values
                        for(int j = 0; j < Bounds.length; j++){
                            if(String.valueOf(findk).compareTo(Bounds[j][3]) == 0 ){
                                System.out.println(Bounds[j][0] + "-->" + Bounds[j][3]);
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
                // for loop to access all the tuples and store them in various arrays.
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
                        //                      if (!tupleTracker.containsKey(scanTuple.getStrFld(1))) { //TODO : change hardcoded value of 1. First value is not always the join attribute, also it wont be string always
                        int position = mergeAttrTable1;
                        if(i==1){
                            position = mergeAttrTable2;
                        }
                        tupleTracker.put(scanTuple.getStrFld(1), scanTuple.getIntFld(position));
                        // update the value of pLBcalc

                        // insert "KEY" or string value in this case
                        pLBCalc[tupleCount][0] = scanTuple.getStrFld(1);
                        pUBCalc[tupleCount][0] = scanTuple.getStrFld(1);
                        // add "index" unique identifier
                        pLBCalc[tupleCount][1] = String.valueOf(tupleCount);
                        pUBCalc[tupleCount][1] = String.valueOf(tupleCount);
                        // add relation #
                        pLBCalc[tupleCount][2] = String.valueOf(i);
                        pUBCalc[tupleCount][2] = String.valueOf(i);

                        // if object is partially found it has potential to become fully seen so add its identifier.
                        found[tupleCount][0] = scanTuple.getStrFld(1);
                        found[tupleCount][1] = String.valueOf(tupleCount);
                        // insert the Value
                        // always insert 2
                        pLBCalc[tupleCount][3] = String.valueOf(scanTuple.getIntFld(position));

                        //if(i == 0){
                        pUBCalc[tupleCount][3] = String.valueOf(scanTuple.getIntFld(position));
                        // TODO: found does not have which relation
                        found[tupleCount][2] = "1"; // found on relation 1
                        previous = scanTuple.getIntFld(position);
                        prevKey = scanTuple.getStrFld(1);
                        /*}else{
                            pUBCalc[tupleTracker.size() - 1][i+1] = String.valueOf(scanTuple.getIntFld(position));
                            found[tupleTracker.size() - 1][1] = "01"; //found on relation 2
                            pUBCalc[tupleTracker.size() - 1][i] = String.valueOf(previous);
                            for(int l = 0; l < k; l++){
                                if(prevKey.compareTo(pUBCalc[l][0]) == 0) {
                                    pUBCalc[l][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                }
                            }
                        }*/
                        // Add all values for the pUB unless found correct values already
                        for(int j = 0; j < arrSize; j++) {
                            if(found[j][2].compareTo("1")== 0){
                                // dont update. Already Fully found
                            }else{
                                pUBCalc[j][3] = String.valueOf(scanTuple.getIntFld(position));
                            }
                            /*if(found[j][1].compareTo("10")== 0){
                                if(i == 0){
                                    // this relation is found. don't rewrite it
                                }else{ // if i is 1 set the second relation
                                    pUBCalc[j][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                }
                            }
                            if(found[j][1].compareTo("01")== 0){
                                if(i == 0){
                                    pUBCalc[j][i+1] = String.valueOf(scanTuple.getIntFld(position));
                                }
                            }
                             */
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
            } // end while ContinueTrue
            // this was from TopK HASH
// NOT SURE IF THIS IS NEEDED
//            if (tupleTracker.size()==k) {
//                //first k in the list are the top k elements
//                printResult(tupleTracker);
//            }
// NOT SURE IF ELSE IF IS NEEDED
//            else if (tupleTracker.size()>k){
//                //vet all elements in hashmap ie find A+B/2 for all top k elements
//                System.out.println("Here now");
//            }

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

    public static boolean contains(String[][] arr, String item) {

        for (int count = 0; !(arr[count][0].compareTo("0") == 0) ;count++) {
            if (item.compareTo(arr[count][0]) == 0) {
                return true;
            }

        }
        return false;
    }

}

