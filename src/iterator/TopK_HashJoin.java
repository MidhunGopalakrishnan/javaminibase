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

    private static short REC_LEN1 = 15;

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
        int runningKCount =0;
//        int countK = 0;
//        HashMap<String,Integer> tupleTracker = new HashMap<String, Integer>();

        try {
//            while (continueWhile) {
//
//                for(int i=0; i < 2; i++) {
//                    Tuple scanTuple = new Tuple();
//                    try {
//                        scanTuple.setHdr((short) numOfColumns1, in1, t1_str_sizes);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    scanTuple.tupleCopy(iscan[i].get_next());
//                    if (scanTuple == null && countK < k) {
//                        System.out.println("No Top K elements can be found from these two tables");
//                    } else if (scanTuple != null) {
//                        scanTuple.print(in1);
//                        if (!tupleTracker.containsKey(scanTuple.getStrFld(1))) { //TODO : change hardcoded value of 1. First value is not always the join attribute, also it wont be string always
//                            int position = mergeAttrTable1;
//                            if(i==1){
//                              position = mergeAttrTable2;
//                            }
//                            tupleTracker.put(scanTuple.getStrFld(1), scanTuple.getIntFld(position));
//                        } else {
//                            int position = mergeAttrTable1;
//                            if(i==1){
//                                position = mergeAttrTable2;
//                            }
//                            int value = tupleTracker.get(scanTuple.getStrFld(1));
//                            tupleTracker.put(scanTuple.getStrFld(1), (value+scanTuple.getIntFld(position))/2);
//                            runningKCount++;
//                            if (runningKCount == k) {
//                                continueWhile = false;
//                                break;
//                            }
//                        }
//                    }
//                }
//            }

            HashMap<String,ArrayList<Integer>[]> tupleTrackerV2 = new HashMap<>();
            while (continueWhile) {
                for(int i=0; i < 2; i++) {
                    Tuple scanTuple = new Tuple();
                    try {
                        scanTuple.setHdr((short) numOfColumns1, in1, t1_str_sizes);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    scanTuple.tupleCopy(iscan[i].get_next());
                    if (scanTuple == null && runningKCount < k) {
                        System.out.println("No Top K elements can be found from these two tables");
                    } else if (scanTuple != null) {
                        scanTuple.print(in1);
                        if (!tupleTrackerV2.containsKey(scanTuple.getStrFld(1))) { //TODO : change hardcoded value of 1. First value is not always the join attribute, also it wont be string always
                            int position;
                            ArrayList<Integer> RelList1 = new ArrayList<>();
                            ArrayList<Integer> RelList2 = new ArrayList<>();
                            ArrayList<Integer> MatchingCount = new ArrayList<>();
                            MatchingCount.add(0);
                            ArrayList[] combinedList = new ArrayList[3];
                            combinedList[0] = RelList1;
                            combinedList[1] = RelList2;
                            combinedList[2] = MatchingCount;
                            if(i==0){
                                position = mergeAttrTable1;
                                RelList1.add(scanTuple.getIntFld(position));
                            } else {
                                position = mergeAttrTable2;
                                RelList2.add(scanTuple.getIntFld(position));
                            }
                            tupleTrackerV2.put(scanTuple.getStrFld(1),combinedList );
                        } else {
                            int position;
                            ArrayList<Integer> RelList1 = new ArrayList<>();
                            ArrayList<Integer> RelList2 = new ArrayList<>();
                            ArrayList<Integer> MatchingCount = new ArrayList<>();
                            ArrayList[] combinedList = new ArrayList[3];
                            if(i==0){
                                position = mergeAttrTable1;
                                combinedList = tupleTrackerV2.get(scanTuple.getStrFld(1));
                                RelList1 = combinedList[0];
                                RelList1.add(scanTuple.getIntFld(position));
                                RelList2 = combinedList[1];
                                int matchingCount = RelList1.size()*RelList2.size();
                                MatchingCount = combinedList[2];
                                if(MatchingCount!=null && MatchingCount.size()==0) {
                                    MatchingCount.add(matchingCount);
                                } else{
                                    MatchingCount.remove(0);
                                    MatchingCount.add(matchingCount);
                                }
                            } else {
                                position = mergeAttrTable2;
                                combinedList = tupleTrackerV2.get(scanTuple.getStrFld(1));
                                RelList1 = combinedList[0];
                                RelList2 = combinedList[1];
                                RelList2.add(scanTuple.getIntFld(position));
                                int matchingCount = RelList1.size()*RelList2.size();
                                MatchingCount = combinedList[2];
                                if(MatchingCount!=null && MatchingCount.size()==0) {
                                    MatchingCount.add(matchingCount);
                                } else{
                                    MatchingCount.remove(0);
                                    MatchingCount.add(matchingCount);
                                }
                            }
                            //iterate through map and find sum of matches made so far
                            java.util.Iterator keySetIterator = tupleTrackerV2.keySet().iterator();
                            runningKCount =0;
                            while (keySetIterator.hasNext()){
                                String key = (String)keySetIterator.next();
                                ArrayList[] combinedList2 = new ArrayList[3];
                                combinedList2 = (ArrayList[]) tupleTrackerV2.get(key);
                                ArrayList matchList = combinedList2[2];
                                if(matchList!=null) {
                                    runningKCount += (Integer) matchList.get(0);
                                }
                            }

                            if (runningKCount >= k) {
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

//            if (tupleTracker.size()==k){
//                //first k in the list are the top k elements
//                printResult(tupleTracker);
//
//            }
//            else if (tupleTracker.size()>k){
             if(tupleTrackerV2.size()!=0){
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
                ArrayList<TopKData> dataList = new ArrayList<>();
                 for(String key : tupleTrackerV2.keySet()){
                     KeyClass keyC = new StringKey(key);
                     BTFileScan bt1 = btf1.new_scan(keyC,keyC);
                     BTFileScan bt2 = btf2.new_scan(keyC,keyC);
                     KeyDataEntry kd1;
                     KeyDataEntry kd2;
                     while ((kd1 = bt1.get_next())!=null){
                         while ((kd2 = bt2.get_next())!=null){
                             LeafData leafData1 = (LeafData) kd1.data;
                             RID rid1 = new RID(leafData1.getData().pageNo,leafData1.getData().slotNo);
                             scanTuple1.tupleCopy(table1.getRecord(rid1));
                             scanTuple1.print(in1);
                             LeafData leafData2 = (LeafData) kd2.data;
                             RID rid2 = new RID(leafData2.getData().pageNo,leafData2.getData().slotNo);
                             scanTuple2.tupleCopy(table2.getRecord(rid2));
                             scanTuple2.print(in2);
                             int[] prefList = new int[2];
                             prefList[0] = mergeAttrTable1;
                             prefList[1] = mergeAttrTable2;
                             TopKData tt = new TopKData(scanTuple1.getStrFld(1),scanTuple1.getIntFld(2),scanTuple1.getIntFld(3),scanTuple2.getIntFld(2),scanTuple2.getIntFld(3),prefList);
                             dataList.add(tt);
                         }
                     }
                 }

                 Collections.sort(dataList);
                 for (int i =0; i < k;i++){
                        dataList.get(i).print();
                 }
//                HashMap<String,Float> topKMap = new HashMap<>();

//                for(String key : tupleTrackerV2.keySet()){
//                    //for each key compute average and fix the top k elements
//                    KeyClass keyC = new StringKey(key);
//                    BTFileScan bt1 = btf1.new_scan(keyC,keyC);
//                    BTFileScan bt2 = btf2.new_scan(keyC,keyC);
//                    KeyDataEntry kd1 = bt1.get_next();
//                    KeyDataEntry kd2 = bt2.get_next();
//                    if(kd1!=null && kd2!=null) {
//                        LeafData leafData1 = (LeafData) kd1.data;
//                        RID rid1 = new RID(leafData1.getData().pageNo,leafData1.getData().slotNo);
//                        scanTuple1.tupleCopy(table1.getRecord(rid1));
////                        scanTuple1.print(in1);
//                        int value1 = scanTuple1.getIntFld(mergeAttrTable1);
//                        LeafData leafData2 = (LeafData) kd2.data;
//                        RID rid2 = new RID(leafData2.getData().pageNo,leafData2.getData().slotNo);
//                        scanTuple2.tupleCopy(table2.getRecord(rid2));
////                        scanTuple2.print(in2);
//                        int value2 = scanTuple2.getIntFld(mergeAttrTable2);
//                        float avg = (float)(value1+value2)/2;
//                        topKMap.put(scanTuple1.getStrFld(joinAttrTable1),avg);
//                    }
//                }

//                LinkedList<Map.Entry<String,Float>> topKList = new LinkedList<Map.Entry<String,Float>>(topKMap.entrySet());
//                Collections.sort(topKList, new Comparator<Map.Entry<String, Float>>() {
//                    @Override
//                    public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
//                        return (o2.getValue()).compareTo(o1.getValue());
//                    }
//                });

                // print first K elements from LinkedList
//                for(int j=0; j< k; j++) {
//                    Map.Entry<String,Float> value = topKList.get(j);
//                    System.out.println(value.getKey() + " ---> " + value.getValue());
//                }
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
class TopKData implements Comparable {
    String name;
    int age1;
    int weight1;
    int age2;
    int weight2;
    int[] preference_attr = new int[2];
    int value1;
    int value2;

    public int getValue1() {
        return value1;
    }

    public void setValue1(int value1) {
        this.value1 = value1;
    }

    public int getValue2() {
        return value2;
    }

    public void setValue2(int value2) {
        this.value2 = value2;
    }

    public TopKData(String name, int age1, int weight1, int age2, int weight2, int[] preference_attr){
        this.setName(name);
        this.setAge1(age1);
        this.setWeight1(weight1);
        this.setAge2(age2);
        this.setWeight2(weight2);
        this.setPreference_attr(preference_attr);
        if(getPreference_attr()[0]==2) {
            setValue1(getAge1());
        } else if(getPreference_attr()[0]==3) {
            setValue1(getWeight1());
        }
        if(getPreference_attr()[1]==2) {
            setValue2(getAge2());
        } else if(getPreference_attr()[1]==3) {
            setValue2(getWeight2());
        }

    }

    public int getAge1() {
        return age1;
    }

    public void setAge1(int age1) {
        this.age1 = age1;
    }

    public int getWeight1() {
        return weight1;
    }

    public void setWeight1(int weight1) {
        this.weight1 = weight1;
    }

    public int getAge2() {
        return age2;
    }

    public void setAge2(int age2) {
        this.age2 = age2;
    }

    public int getWeight2() {
        return weight2;
    }

    public void setWeight2(int weight2) {
        this.weight2 = weight2;
    }

    public int[] getPreference_attr() {
        return preference_attr;
    }

    public void setPreference_attr(int[] preference_attr) {
        this.preference_attr = preference_attr;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void print(){
        System.out.println(getName()+" "+ getAge1()+ " "+ getWeight1() + " "+ getAge2() + " "+ getWeight2()+ " "+ ((getValue1()+getValue2())/2));
    }

    @Override
    public int compareTo(Object o) {

        return (((((TopKData)o).getValue1())+((TopKData)o).getValue2())- (this.getValue1()+this.getValue2()));
    }
}
