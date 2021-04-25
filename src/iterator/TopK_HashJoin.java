package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.*;

public class TopK_HashJoin extends Iterator implements GlobalConst {

    private static short REC_LEN1 = 15;
    private static final int TOPK_COLUMN_LENGTH = 6;
    private static short STR_LEN = 13;
    HashMap<String, TableMetadata> tableMetadataMap = new HashMap<>();

    public TopK_HashJoin(
            AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1,
            FldSpec mergeAttr1,
            AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2,
            FldSpec mergeAttr2,
            java.lang.String relationName1,
            java.lang.String relationName2,
            int k,
            int n_pages,String outputTableName, HashMap<String, TableMetadata> tableMetadataMap
    ) {
        if(outputTableName.equals("") || tableMetadataMap.get(outputTableName)==null) {

            int joinAttrTable1 = joinAttr1.offset;
            int mergeAttrTable1 = mergeAttr1.offset;
            int joinAttrTable2 = joinAttr2.offset;
            int mergeAttrTable2 = mergeAttr2.offset;
            this.tableMetadataMap = tableMetadataMap;

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
            //if not present , create BTree indexes

            if(!tableMetadataMap.get(relationName1).indexNameList.contains(relationName1 + "UNCLUSTBTREE" + mergeAttrTable1)){
                //create index and add entry in metadata table
                createUnclusteredBTreeIndex(relationName1,  mergeAttrTable1);
            }
            if(!tableMetadataMap.get(relationName1).indexNameList.contains(relationName1 + "UNCLUSTBTREE" + joinAttrTable1)){
                //create index and add entry in metadata table
                createUnclusteredBTreeIndex(relationName1,  joinAttrTable1);
            }
            if(!tableMetadataMap.get(relationName2).indexNameList.contains(relationName2 + "UNCLUSTBTREE" + mergeAttrTable2)) {
                //create index
                createUnclusteredBTreeIndex(relationName2,  mergeAttrTable2);
            }
            if(!tableMetadataMap.get(relationName2).indexNameList.contains(relationName2 + "UNCLUSTBTREE" + joinAttrTable2)) {
                //create index
                createUnclusteredBTreeIndex(relationName2,  joinAttrTable2);
            }

            try {
                iscan[0] = new IndexScan(new IndexType(IndexType.B_Index), relationName1, relationName1 + "UNCLUSTBTREE" + mergeAttrTable1,
                        in1, t1_str_sizes, len_in1, len_in1, projlist1, expr, len_in1, false);
                iscan[1] = new IndexScan(new IndexType(IndexType.B_Index), relationName2, relationName2 + "UNCLUSTBTREE" + mergeAttrTable2,
                        in2, t2_str_sizes, len_in2, len_in2, projlist2, expr, len_in2, false);
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean continueWhile = true;
            int runningKCount = 0;

            HashMap<String, ArrayList<Integer>[]> tupleTrackerV2 = new HashMap<>();
            HashMap<Integer, ArrayList<Integer>[]> tupleTrackerV3 = new HashMap<>();

            try {

                while (continueWhile) {
                    for (int i = 0; i < 2; i++) {
                        Tuple scanTuple = new Tuple();
                        if (i == 0) {
                            try {
                                scanTuple.setHdr((short) len_in1, in1, t1_str_sizes);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            try {
                                scanTuple.setHdr((short) len_in2, in2, t2_str_sizes);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        Tuple temp = iscan[i].get_next();
                        if(temp== null) {
                            continue;
                        }
                        scanTuple.tupleCopy(temp);
                        if (scanTuple == null && runningKCount < k) {
                            System.out.println("No Top K elements can be found from these two tables");
                            continueWhile = false;
                            break;
                        } else if (scanTuple != null) {
                            int joinAttrPosition = 0;
                            if (i == 0) {
                                joinAttrPosition = joinAttrTable1;
                            } else {
                                joinAttrPosition = joinAttrTable2;
                            }
                            if(tableMetadataMap.get(relationName1).attrType[joinAttrTable1-1].attrType== AttrType.attrString) {
                                if (!tupleTrackerV2.containsKey(scanTuple.getStrFld(joinAttrPosition))) {
                                    int position;
                                    ArrayList<Integer> RelList1 = new ArrayList<>();
                                    ArrayList<Integer> RelList2 = new ArrayList<>();
                                    ArrayList<Integer> MatchingCount = new ArrayList<>();
                                    MatchingCount.add(0);
                                    ArrayList[] combinedList = new ArrayList[3];
                                    combinedList[0] = RelList1;
                                    combinedList[1] = RelList2;
                                    combinedList[2] = MatchingCount;
                                    if (i == 0) {
                                        position = mergeAttrTable1;
                                        RelList1.add(scanTuple.getIntFld(position));
                                    } else {
                                        position = mergeAttrTable2;
                                        RelList2.add(scanTuple.getIntFld(position));
                                    }
                                    tupleTrackerV2.put(scanTuple.getStrFld(joinAttrPosition), combinedList);
                                } else {
                                    int position;
                                    ArrayList<Integer> RelList1 = new ArrayList<>();
                                    ArrayList<Integer> RelList2 = new ArrayList<>();
                                    ArrayList<Integer> MatchingCount = new ArrayList<>();
                                    ArrayList[] combinedList = new ArrayList[3];
                                    if (i == 0) {
                                        position = mergeAttrTable1;
                                        combinedList = tupleTrackerV2.get(scanTuple.getStrFld(joinAttrPosition));
                                        RelList1 = combinedList[0];
                                        RelList1.add(scanTuple.getIntFld(position));
                                        RelList2 = combinedList[1];
                                        int matchingCount = RelList1.size() * RelList2.size();
                                        MatchingCount = combinedList[2];
                                        if (MatchingCount != null && MatchingCount.size() == 0) {
                                            MatchingCount.add(matchingCount);
                                        } else {
                                            MatchingCount.remove(0);
                                            MatchingCount.add(matchingCount);
                                        }
                                    } else {
                                        position = mergeAttrTable2;
                                        combinedList = tupleTrackerV2.get(scanTuple.getStrFld(joinAttrPosition));
                                        RelList1 = combinedList[0];
                                        RelList2 = combinedList[1];
                                        RelList2.add(scanTuple.getIntFld(position));
                                        int matchingCount = RelList1.size() * RelList2.size();
                                        MatchingCount = combinedList[2];
                                        if (MatchingCount != null && MatchingCount.size() == 0) {
                                            MatchingCount.add(matchingCount);
                                        } else {
                                            MatchingCount.remove(0);
                                            MatchingCount.add(matchingCount);
                                        }
                                    }
                                    //iterate through map and find sum of matches made so far
                                    java.util.Iterator keySetIterator = tupleTrackerV2.keySet().iterator();
                                    runningKCount = 0;
                                    while (keySetIterator.hasNext()) {
                                        String key = (String) keySetIterator.next();
                                        ArrayList[] combinedList2 = new ArrayList[3];
                                        combinedList2 = (ArrayList[]) tupleTrackerV2.get(key);
                                        ArrayList matchList = combinedList2[2];
                                        if (matchList != null) {
                                            runningKCount += (Integer) matchList.get(0);
                                        }
                                    }

                                    if (runningKCount >= k) {
                                        continueWhile = false;
                                        break;
                                    }
                                }
                            }
                            else if(tableMetadataMap.get(relationName1).attrType[joinAttrTable1-1].attrType== AttrType.attrInteger) {
                                if (!tupleTrackerV3.containsKey(scanTuple.getIntFld(joinAttrPosition))) {
                                    int position;
                                    ArrayList<Integer> RelList1 = new ArrayList<>();
                                    ArrayList<Integer> RelList2 = new ArrayList<>();
                                    ArrayList<Integer> MatchingCount = new ArrayList<>();
                                    MatchingCount.add(0);
                                    ArrayList[] combinedList = new ArrayList[3];
                                    combinedList[0] = RelList1;
                                    combinedList[1] = RelList2;
                                    combinedList[2] = MatchingCount;
                                    if (i == 0) {
                                        position = mergeAttrTable1;
                                        RelList1.add(scanTuple.getIntFld(position));
                                    } else {
                                        position = mergeAttrTable2;
                                        RelList2.add(scanTuple.getIntFld(position));
                                    }
                                    tupleTrackerV3.put(scanTuple.getIntFld(joinAttrPosition), combinedList);
                                } else {
                                    int position;
                                    ArrayList<Integer> RelList1 = new ArrayList<>();
                                    ArrayList<Integer> RelList2 = new ArrayList<>();
                                    ArrayList<Integer> MatchingCount = new ArrayList<>();
                                    ArrayList[] combinedList = new ArrayList[3];
                                    if (i == 0) {
                                        position = mergeAttrTable1;
                                        combinedList = tupleTrackerV3.get(scanTuple.getIntFld(joinAttrPosition));
                                        RelList1 = combinedList[0];
                                        RelList1.add(scanTuple.getIntFld(position));
                                        RelList2 = combinedList[1];
                                        int matchingCount = RelList1.size() * RelList2.size();
                                        MatchingCount = combinedList[2];
                                        if (MatchingCount != null && MatchingCount.size() == 0) {
                                            MatchingCount.add(matchingCount);
                                        } else {
                                            MatchingCount.remove(0);
                                            MatchingCount.add(matchingCount);
                                        }
                                    } else {
                                        position = mergeAttrTable2;
                                        combinedList = tupleTrackerV3.get(scanTuple.getIntFld(joinAttrPosition));
                                        RelList1 = combinedList[0];
                                        RelList2 = combinedList[1];
                                        RelList2.add(scanTuple.getIntFld(position));
                                        int matchingCount = RelList1.size() * RelList2.size();
                                        MatchingCount = combinedList[2];
                                        if (MatchingCount != null && MatchingCount.size() == 0) {
                                            MatchingCount.add(matchingCount);
                                        } else {
                                            MatchingCount.remove(0);
                                            MatchingCount.add(matchingCount);
                                        }
                                    }
                                    //iterate through map and find sum of matches made so far
                                    java.util.Iterator keySetIterator = tupleTrackerV3.keySet().iterator();
                                    runningKCount = 0;
                                    while (keySetIterator.hasNext()) {
                                        Integer key = (Integer) keySetIterator.next();
                                        ArrayList[] combinedList2 = new ArrayList[3];
                                        combinedList2 = (ArrayList[]) tupleTrackerV3.get(key);
                                        ArrayList matchList = combinedList2[2];
                                        if (matchList != null) {
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
                }

                try {
                    iscan[0].close();
                    iscan[1].close();
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }

//                System.out.println("Content created");
                if (tupleTrackerV2.size() != 0) {
                    //vet all elements in hashmap ie find A+B/2 for all top k elements
                    //create index files on the join attribute based on table 1 and table 2
                    BTreeFile btf1 = new BTreeFile(relationName1 + "UNCLUSTBTREE" + joinAttrTable1);
                    BTreeFile btf2 = new BTreeFile(relationName2 + "UNCLUSTBTREE" + joinAttrTable2);
                    Heapfile hf1 = new Heapfile(relationName1);
                    Heapfile hf2 = new Heapfile(relationName2);

                    Scan scan = null;

                    if (btf1 != null && btf2 != null) {

                        Tuple scanTuple1 = new Tuple();
                        Tuple scanTuple2 = new Tuple();
                        Tuple temp = null;

                        ArrayList<TopKData> dataList = new ArrayList<>();

                        try {
                            scan = new Scan(hf1);
                        } catch (Exception e) {
                            //status = FAIL;
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                        RID rid = new RID();
                        ArrayList[] combinedList = new ArrayList[3];
                        try {
                            scanTuple1.setHdr((short) in1.length, in1, t1_str_sizes);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        while((temp = scan.getNext(rid))!=null){
                            scanTuple1.tupleCopy(temp);
                            String key = scanTuple1.getStrFld(joinAttrTable1);
                            if(tupleTrackerV2.keySet().contains(key)){
                                combinedList = tupleTrackerV2.get(key);
                                if(!combinedList[0].contains(scanTuple1.getIntFld(mergeAttrTable1))){
                                    combinedList[0].add(scanTuple1.getIntFld(mergeAttrTable1));
                                }
                            }
                        }
                        try {
                            scan = new Scan(hf2);
                        } catch (Exception e) {
                            //status = FAIL;
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                        rid = new RID();

                        try {
                            scanTuple2.setHdr((short) in2.length, in2, t2_str_sizes);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        temp = null;
                        while((temp = scan.getNext(rid))!=null){
                            scanTuple2.tupleCopy(temp);
                            String key = scanTuple2.getStrFld(joinAttrTable1);
                            if(tupleTrackerV2.keySet().contains(key)){
                                combinedList = tupleTrackerV2.get(key);
                                if(!combinedList[1].contains(scanTuple2.getIntFld(mergeAttrTable2))){
                                    combinedList[1].add(scanTuple2.getIntFld(mergeAttrTable2));
                                }
                            }
                        }

                        for (String key : tupleTrackerV2.keySet()) {
                                ArrayList[] combinedListTemp = tupleTrackerV2.get(key);
                                ArrayList<Integer> rel1ListTemp = combinedListTemp[0];
                                ArrayList<Integer> rel2ListTemp = combinedListTemp[1];
                                for(int i=0 ;i < rel1ListTemp.size();i++){
                                    for(int j=0; j < rel2ListTemp.size();j++){
                                        TopKData tt = new TopKData(key, rel1ListTemp.get(i), mergeAttrTable1, rel2ListTemp.get(j), mergeAttrTable2);
                                        dataList.add(tt);
                                    }
                                }

                        }

                        Collections.sort(dataList);
                        System.out.println("TopK Results : ");
                        System.out.println("Join_Attribute | Preference_Attribute1 | Preference_Attribute2 | Average_of_Preference_Attribute");
                        Heapfile outHeap = null;
                        AttrType[] outAttrType = new AttrType[TOPK_COLUMN_LENGTH];
                        short[] outAttrSize = new short[1];
                        Tuple outTuple = new Tuple();
                            if (!outputTableName.equals("")) {
                                outHeap = new Heapfile(outputTableName);
                                //populate the attrType and attrSize and build a Tuple Header
                                outAttrType[0] = new AttrType(in1[joinAttrTable1-1].attrType);
                                outAttrType[1] = new AttrType(in1[mergeAttrTable1-1].attrType);
                                outAttrType[2] = new AttrType(AttrType.attrInteger);
                                outAttrType[3] = new AttrType(in2[mergeAttrTable2-1].attrType);
                                outAttrType[4] = new AttrType(AttrType.attrInteger);
                                outAttrType[5] = new AttrType(AttrType.attrReal);

                                if(in1[joinAttrTable1-1].attrType==AttrType.attrString){
                                    outAttrSize[0] = STR_LEN;
                                }

                                try {
                                    outTuple.setHdr((short) outAttrType.length, outAttrType, outAttrSize);
                                } catch (Exception e) {
                                    System.err.println("*** error in Tuple.setHdr() ***");
                                    e.printStackTrace();
                                }
                            }
                        for (int i = 0; i < k; i++) {
                            dataList.get(i).print();
                            if (!outputTableName.equals("")){

                                TopKData tk = dataList.get(i);
                                if(in1[joinAttrTable1-1].attrType==AttrType.attrString) {
                                    outTuple.setStrFld(1,tk.getJoinAttr());
                                } else {
                                    outTuple.setIntFld(1,Integer.parseInt(tk.getJoinAttr()));
                                }

                                outTuple.setIntFld(2,tk.getMergeAttr1());
                                outTuple.setIntFld(3,tk.getMergeAttrPosition1());
                                outTuple.setIntFld(4,tk.getMergeAttr2());
                                outTuple.setIntFld(5,tk.getMergeAttrPosition2());
                                outTuple.setFloFld(6,tk.getAverage());

                                outHeap.insertRecord(outTuple.returnTupleByteArray());
                            }
                        }
                        if (!outputTableName.equals("")) {
                            //update the table metadata map
                            TableMetadata tm = new TableMetadata(outputTableName, outAttrType, outAttrSize);
                            tableMetadataMap.put(outputTableName, tm);
                        }
                    }
                }
                else if (tupleTrackerV3.size() != 0) {
                    //vet all elements in hashmap ie find A+B/2 for all top k elements
                    //create index files on the join attribute based on table 1 and table 2
                    BTreeFile btf1 = new BTreeFile(relationName1 + "UNCLUSTBTREE" + joinAttrTable1);
                    BTreeFile btf2 = new BTreeFile(relationName2 + "UNCLUSTBTREE" + joinAttrTable2);
                    Heapfile hf1 = new Heapfile(relationName1);
                    Heapfile hf2 = new Heapfile(relationName2);

                    Scan scan = null;

                    if (btf1 != null && btf2 != null) {

                        Tuple scanTuple1 = new Tuple();
                        Tuple scanTuple2 = new Tuple();
                        Tuple temp = null;

                        ArrayList<TopKData> dataList = new ArrayList<>();

                        try {
                            scan = new Scan(hf1);
                        } catch (Exception e) {
                            //status = FAIL;
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                        RID rid = new RID();
                        ArrayList[] combinedList = new ArrayList[3];
                        try {
                            scanTuple1.setHdr((short) in1.length, in1, t1_str_sizes);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        while((temp = scan.getNext(rid))!=null){
                            scanTuple1.tupleCopy(temp);
                            Integer key = scanTuple1.getIntFld(joinAttrTable1);
                            if(tupleTrackerV3.keySet().contains(key)){
                                combinedList = tupleTrackerV3.get(key);
                                if(!combinedList[0].contains(scanTuple1.getIntFld(mergeAttrTable1))){
                                    combinedList[0].add(scanTuple1.getIntFld(mergeAttrTable1));
                                }
                            }
                        }
                        try {
                            scan = new Scan(hf2);
                        } catch (Exception e) {
                            //status = FAIL;
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                        rid = new RID();

                        try {
                            scanTuple2.setHdr((short) in2.length, in2, t2_str_sizes);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        temp = null;
                        while((temp = scan.getNext(rid))!=null){
                            scanTuple2.tupleCopy(temp);
                            Integer key = scanTuple2.getIntFld(joinAttrTable1);
                            if(tupleTrackerV3.keySet().contains(key)){
                                combinedList = tupleTrackerV3.get(key);
                                if(!combinedList[1].contains(scanTuple2.getIntFld(mergeAttrTable2))){
                                    combinedList[1].add(scanTuple2.getIntFld(mergeAttrTable2));
                                }
                            }
                        }

                        for (Integer key : tupleTrackerV3.keySet()) {
                            ArrayList[] combinedListTemp = tupleTrackerV3.get(key);
                            ArrayList<Integer> rel1ListTemp = combinedListTemp[0];
                            ArrayList<Integer> rel2ListTemp = combinedListTemp[1];
                            for(int i=0 ;i < rel1ListTemp.size();i++){
                                for(int j=0; j < rel2ListTemp.size();j++){
                                    TopKData tt = new TopKData(key, rel1ListTemp.get(i), mergeAttrTable1, rel2ListTemp.get(j), mergeAttrTable2);
                                    dataList.add(tt);
                                }
                            }

                        }

                        Collections.sort(dataList);
                        System.out.println("TopK Results : ");
                        System.out.println("Join_Attribute | Preference_Attribute1 | Preference_Attribute2 | Average_of_Preference_Attribute");
                        Heapfile outHeap = null;
                        AttrType[] outAttrType = new AttrType[TOPK_COLUMN_LENGTH];
                        short[] outAttrSize =null;
                        Tuple outTuple = new Tuple();
                        if (!outputTableName.equals("")){
                            if (!outputTableName.equals("")) {
                                outHeap = new Heapfile(outputTableName);
                                //populate the attrType and attrSize and build a Tuple Header
                                outAttrType[0] = new AttrType(in1[joinAttrTable1-1].attrType);
                                outAttrType[1] = new AttrType(in1[mergeAttrTable1-1].attrType);
                                outAttrType[2] = new AttrType(AttrType.attrInteger);
                                outAttrType[3] = new AttrType(in2[mergeAttrTable2-1].attrType);
                                outAttrType[4] = new AttrType(AttrType.attrInteger);
                                outAttrType[5] = new AttrType(AttrType.attrReal);

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
                        for (int i = 0; i < k; i++) {
                            dataList.get(i).print();
                            if (!outputTableName.equals("")){

                                TopKData tk = dataList.get(i);
                                if(in1[joinAttrTable1-1].attrType==AttrType.attrString) {
                                    outTuple.setStrFld(1,tk.getJoinAttr());
                                } else {
                                    outTuple.setIntFld(1,tk.getJoinAttr1());
                                }

                                outTuple.setIntFld(2,tk.getMergeAttr1());
                                outTuple.setIntFld(3,tk.getMergeAttrPosition1());
                                outTuple.setIntFld(4,tk.getMergeAttr2());
                                outTuple.setIntFld(5,tk.getMergeAttrPosition2());
                                outTuple.setFloFld(6,tk.getAverage());

                                outHeap.insertRecord(outTuple.returnTupleByteArray());
                            }
                        }
                        if (!outputTableName.equals("")) {
                            //update the table metadata map
                            TableMetadata tm = new TableMetadata(outputTableName, outAttrType, outAttrSize);
                            tableMetadataMap.put(outputTableName, tm);
                        }
                    }
                }
                else {
                    System.out.println("No index present for attribute " + joinAttr1 + " for table " + relationName1 + "/" + relationName2 + ". Please make sure an index is present for the join attribute.");
                }

            } catch (Exception e) {
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        } else {
            System.out.println("Output table name mentioned already exist. Please enter a new table name ");
        }

    }

//    public TopK_HashJoin(
//            AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1,
//            FldSpec mergeAttr1,
//            AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2,
//            FldSpec mergeAttr2,
//            java.lang.String relationName1,
//            java.lang.String relationName2,
//            int k,
//            int n_pages,String outputTableName, HashMap<String, TableMetadata> tableMetadataMap
//    ) {
//        if(outputTableName.equals("") || tableMetadataMap.get(outputTableName)==null) {
//            int joinAttrTable1 = joinAttr1.offset;
//            int mergeAttrTable1 = mergeAttr1.offset;
//            int joinAttrTable2 = joinAttr2.offset;
//            int mergeAttrTable2 = mergeAttr2.offset;
//            boolean materialize = false;
//            if (!outputTableName.equals("")) {
//                materialize = true;
//            }
//
//            //projection list for outer table
//            FldSpec[] projlist1 = new FldSpec[len_in1];
//            RelSpec rel1 = new RelSpec(RelSpec.outer);
//            for (int i = 0; i < len_in1; i++) {
//                projlist1[i] = new FldSpec(rel1, i + 1);
//            }
//
//            //projection list for inner table
//            FldSpec[] projlist3 = new FldSpec[len_in2];
//            RelSpec rel3 = new RelSpec(RelSpec.outer);
//            for (int i = 0; i < len_in2; i++) {
//                projlist3[i] = new FldSpec(rel3, i + 1);
//            }
//
//            //projection list for joined table
//            FldSpec[] projlist2 = new FldSpec[in1.length + in2.length];
//            RelSpec rel2 = new RelSpec(RelSpec.innerRel);
//            for (int i = 0; i < in1.length; i++) {
//                projlist2[i] = new FldSpec(rel1, i + 1);
//            }
//            for (int i = 0; i < in2.length; i++) {
//                projlist2[i + in1.length] = new FldSpec(rel2, i + 1);
//            }
//
//            CondExpr[] expr = new CondExpr[2];
//            expr[0] = new CondExpr();
//            expr[1] = new CondExpr();
//            expr[0].next  = null;
//            expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//            expr[0].type1 = new AttrType(AttrType.attrSymbol);
//            expr[0].type2 = new AttrType(AttrType.attrSymbol);
//            expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),joinAttrTable1);
//            expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joinAttrTable2);
//            expr[1] = null;
//
//            iterator.Iterator am1 = null;
//            String rel1IndexFileName = relationName1 + "UNCLUSTBTREE" + joinAttrTable1;
//            CondExpr[] scanExpr = new CondExpr[2];
//            scanExpr[0] = null;
//            scanExpr[1] = null;
//
//            try {
//                am1 = new IndexScan(new IndexType(IndexType.B_Index), relationName1, rel1IndexFileName
//                        , in1, t1_str_sizes, in1.length, in1.length,
//                        projlist1, scanExpr, in1.length, false);
//            } catch (Exception e) {
//                System.err.println("*** Error creating scan for Index scan");
//                System.err.println("" + e);
//                Runtime.getRuntime().exit(1);
//            }
//
//            try {
//                //modify to accept metadata map and if it should be materialized
//                HashJoins hJoin = new HashJoins(
//                        in1, len_in1, t1_str_sizes, in2, len_in2, t2_str_sizes, n_pages,
//                        am1, relationName1, expr, scanExpr, projlist2, len_in1 + len_in2, materialize, joinAttrTable2,
//                        tableMetadataMap);
//            } catch (Exception e) {
//                e.printStackTrace();
//                Runtime.getRuntime().exit(1);
//            }
//        }
//    }

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
class TopKData implements Comparable {
    String joinAttr;
    int joinAttr1;
    int mergeAttr1;
    int mergeAttrPosition1;
    int mergeAttr2;
    int mergeAttrPosition2;
    float average;
    boolean joinIsString;


    public TopKData(String joinAttr, int mergeAttr1, int mergeAttrPosition1, int mergeAttr2, int mergeAttrPosition2){
        this.joinAttr = joinAttr;
        this.mergeAttr1 = mergeAttr1;
        this.mergeAttrPosition1 = mergeAttrPosition1;
        this.mergeAttr2 = mergeAttr2;
        this.mergeAttrPosition2 = mergeAttrPosition2;
        this.average = ((float)mergeAttr1+(float)mergeAttr2)/2;
        this.joinIsString = true;
    }

    public TopKData(Integer joinAttr1, int mergeAttr1, int mergeAttrPosition1, int mergeAttr2, int mergeAttrPosition2){
        this.joinAttr1 = joinAttr1;
        this.mergeAttr1 = mergeAttr1;
        this.mergeAttrPosition1 = mergeAttrPosition1;
        this.mergeAttr2 = mergeAttr2;
        this.mergeAttrPosition2 = mergeAttrPosition2;
        this.average = ((float)mergeAttr1+(float)mergeAttr2)/2;
        this.joinIsString = false;
    }

    public String getJoinAttr() {
        return joinAttr;
    }

    public int getJoinAttr1() {
        return joinAttr1;
    }

    public int getMergeAttr1() {
        return mergeAttr1;
    }

    public int getMergeAttrPosition1() {
        return mergeAttrPosition1;
    }

    public int getMergeAttr2() {
        return mergeAttr2;
    }

    public int getMergeAttrPosition2() {
        return mergeAttrPosition2;
    }

    public float getAverage() {
        return average;
    }

    public void print(){
        if(this.joinIsString) {
            System.out.println(getJoinAttr() + " " + getMergeAttr1() + "(Index Position : " + getMergeAttrPosition1() + ")" + " " + getMergeAttr2() + "(Index Position : " + getMergeAttrPosition2() + ")" + " " + getAverage());
        } else {
            System.out.println(getJoinAttr1() + " " + getMergeAttr1() + "(Index Position : " + getMergeAttrPosition1() + ")" + " " + getMergeAttr2() + "(Index Position : " + getMergeAttrPosition2() + ")" + " " + getAverage());
        }
    }

    @Override
    public int compareTo(Object o) {

        float diff = ((TopKData)o).getAverage() - this.getAverage();
        if(diff==0.0f){
            return 0;
        } else if(diff >0.0f){
             return 1;
        } else {
            return -1;
        }
    }
}
