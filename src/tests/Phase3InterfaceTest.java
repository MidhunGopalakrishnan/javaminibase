package tests;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import diskmgr.PCounter;
import global.*;
import hash.HashFile;
import heap.*;
import index.IndexScan;
import iterator.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

class Phase3InterfaceTestDriver extends TestDriver
        implements GlobalConst {

    HashMap<String, TableMetadata> tableMetadataMap = new HashMap<>();
    public static final int STR_LEN = 13;
    private static short REC_LEN1 = 15;
    public static final String metadatafileName = "/tmp/tablemetadata.ser";
    public static final String commandFile = "/tmp/reportcommands.txt";
    public static final boolean consoleMode = false;
    HashMap<String,Integer> operatorList = new HashMap<>();

    public Phase3InterfaceTestDriver() {
        super("phase3interfacetest");
    }

    protected boolean runAllTests() throws Exception {
        boolean _passAll = OK;
        if (!test1()) {
            _passAll = FAIL;
        }
        if (!test2()) {
            _passAll = FAIL;
        }
        if (!test3()) {
            _passAll = FAIL;
        }
        if (!test4()) {
            _passAll = FAIL;
        }
        if (!test5()) {
            _passAll = FAIL;
        }
        if (!test6()) {
            _passAll = FAIL;
        }

        return _passAll;
    }

    public boolean runTests() {

        boolean _pass = false;
        try {
            TestDriver.setTestNumber(testNumber);
            _pass = runAllTests();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return true;
    }

    protected boolean test1() {

        boolean continueWhile = true;
        File obj =null;
        String command;
        Scanner sc = null;

        if(consoleMode) {

            System.out.println("Welcome to MiniBase DB \n" +
                    " \n Available Options :" +
                    "\n 1. open_database DBNAME" +
                    "\n 2. close_database" +
                    "\n 3. create_table [CLUSTERED HASH ATT_NO] FILENAME" +
                    "\n 4. create_index BTREE/HASH ATT_NO TABLENAME" +
                    "\n 5. insert_data TABLENAME FILENAME" +
                    "\n 6. delete_data TABLENAME FILENAME" +
                    "\n 7. output_table TABLENAME" +
                    "\n 8. output_index TABLENAME ATT_NO" +
                    "\n 9. skyline NLS/BNLS/SFS/BTS/BTSS {ATTNO1,ATTNO2,...,ATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME]" +
                    "\n 10. GROUPBY SORT/HASH MAX/MIN/AGG/SKYGATTNO {AATTNO1,AATTNO2,...,AATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME] " +
                    "\n 11. JOIN NLJ/SMJ/INLJ/HJ OTABLENAME OATTNO ITABLENAME IATTNO OP NPAGES [MATER OUTTABLENAME] " +
                    "\n 12. TOPKJOIN HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_JATT_NO I_MATT_NO NPAGES [MATER OUTTABLENAME] " +
                    "\n 13. exit_db \n \n");
        }

        //open_database test1
        //create_table src/data/phase3demo/r_sii2000_1_75_200.csv
        //create_index BTREE 1 r_sii2000_1_75_200
        //create_index BTREE 2 r_sii2000_1_75_200
        //create_table src/data/phase3demo/r_sii2000_10_10_10.csv
        //create_index BTREE 1 r_sii2000_10_10_10
        //create_index BTREE 2 r_sii2000_10_10_10
        //create_index BTREE 2 r_sii2000_1_75_200 -- fail
        //create_index HASH 1 r_sii2000_1_75_200
        //create_index HASH 2 r_sii2000_1_75_200
        //create_index HASH 2 r_sii2000_1_75_200 --fail
        //insert_data r_sii2000_1_75_200 src/data/phase3demo/r_sii2000_10_10_10.csv
        //delete_data r_sii2000_1_75_200 src/data/phase3demo/r_sii2000_10_10_10.csv
        //output_table r_sii2000_1_75_200
        //output_index r_sii2000_1_75_200 1
        //skyline SFS 2,3 r_sii2000_1_75_200 5 MATER r_sii2000_1_75_200_sfs
        //output_table r_sii2000_1_75_200_sfs
        //skyline BTS 2,3 r_sii2000_1_75_200 5 MATER r_sii2000_1_75_200_bts
        //output_table r_sii2000_1_75_200_bts
        //skyline BTSS 2,3 r_sii2000_1_75_200 5 MATER r_sii2000_1_75_200_btss
        //TOPKJOIN HASH 3 r_sii2000_1_75_200 1 2 r_sii2000_10_10_10 1 2 5 MATER topk_hash1
        //TOPKJOIN NRA 3 r_sii2000_1_75_200 1 2 r_sii2000_10_10_10 1 2 5 MATER topk_hash1
        //output_table topk_hash1
        //JOIN NLJ r_sii2000_1_75_200 2 r_sii2000_10_10_10 2 = 5
        //JOIN SMJ r_sii2000_1_75_200 2 r_sii2000_10_10_10 2 = 5
        //GROUPBY HASH MAX 1 2,3 r_sii2000_1_75_200 5


        while(continueWhile){

            if(consoleMode) {
                sc = new Scanner(System.in);
                System.out.println(" Enter the query for execution : ");
                command = sc.nextLine();

                String[] commandList = command.split(" ");
                switch (commandList[0]) {
                    case "open_database":
                        PCounter.initialize();
                        open_database(commandList);
                        PCounter.printCounter();
                        System.out.println("DB opened successfully\n");
                        break;
                    case "close_database":
                        PCounter.initialize();
                        close_database();
                        PCounter.printCounter();
                        System.out.println("DB closed successfully\n");
                        break;
                    case "create_table":
                        PCounter.initialize();
                        create_table(commandList);
                        PCounter.printCounter();
                        System.out.println("Table created successfully\n");
                        break;
                    case "create_index":
                        PCounter.initialize();
                        create_index(commandList);
                        PCounter.printCounter();
                        System.out.println("Index created successfully\n");
                        break;
                    case "insert_data":
                        PCounter.initialize();
                        insert_data(commandList);
                        PCounter.printCounter();
                        System.out.println("Data inserted successfully\n");
                        break;
                    case "delete_data":
                        PCounter.initialize();
                        delete_data(commandList);
                        PCounter.printCounter();
                        System.out.println("Data deleted successfully\n");
                        break;
                    case "output_table":
                        PCounter.initialize();
                        output_table(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "output_index":
                        PCounter.initialize();
                        output_index(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "skyline":
                        PCounter.initialize();
                        output_skyline(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "GROUPBY":
                        PCounter.initialize();
                        output_groupby(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "JOIN":
                        PCounter.initialize();
                        output_join(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "TOPKJOIN":
                        PCounter.initialize();
                        output_topk(commandList);
                        PCounter.printCounter();
                        System.out.println();
                        break;
                    case "exit_db":
                        continueWhile = false;
                        System.out.println("Exiting DB\n");
                        break;
                    default:
                        System.out.println("Invalid syntax. Try again \n");
                        break;
                }

            } else {
                obj = new File(commandFile);

                try {
                    sc = new Scanner(obj);
                }catch(Exception e){
                    e.printStackTrace();
                }

                while (sc.hasNextLine()) {
                    command = sc.nextLine();
                    if(command.startsWith("Test")){
                        System.out.println("\n"+command+"\n");
                        continue;
                    }
                    System.out.println("Command : "+command);
                    String[] commandList = command.split(" ");
                    switch (commandList[0]) {
                        case "open_database":
                            PCounter.initialize();
                            open_database(commandList);
                            PCounter.printCounter();
                            System.out.println("DB opened successfully\n");
                            break;
                        case "close_database":
                            PCounter.initialize();
                            close_database();
                            PCounter.printCounter();
                            System.out.println("DB closed successfully\n");
                            break;
                        case "create_table":
                            PCounter.initialize();
                            create_table(commandList);
                            PCounter.printCounter();
                            System.out.println("Table created successfully\n");
                            break;
                        case "create_index":
                            PCounter.initialize();
                            create_index(commandList);
                            PCounter.printCounter();
                            System.out.println("Index created successfully\n");
                            break;
                        case "insert_data":
                            PCounter.initialize();
                            insert_data(commandList);
                            PCounter.printCounter();
                            System.out.println("Data inserted successfully\n");
                            break;
                        case "delete_data":
                            PCounter.initialize();
                            delete_data(commandList);
                            PCounter.printCounter();
                            System.out.println("Data deleted successfully\n");
                            break;
                        case "output_table":
                            PCounter.initialize();
                            output_table(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "output_index":
                            PCounter.initialize();
                            output_index(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "skyline":
                            PCounter.initialize();
                            output_skyline(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "GROUPBY":
                            PCounter.initialize();
                            output_groupby(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "JOIN":
                            PCounter.initialize();
                            output_join(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "TOPKJOIN":
                            PCounter.initialize();
                            output_topk(commandList);
                            PCounter.printCounter();
                            System.out.println();
                            break;
                        case "exit_db":
                            continueWhile = false;
                            System.out.println("Exiting DB\n");
                            break;
                        default:
                            System.out.println("Invalid syntax. Try again \n");
                            break;
                    }
                }
            }
        }

        return true;
    }

    private void output_join(String[] commandList) {
        operatorList.put("=",AttrOperator.aopEQ);
        operatorList.put("<=",AttrOperator.aopLE);
        operatorList.put("<",AttrOperator.aopLT);
        operatorList.put(">",AttrOperator.aopGT);
        operatorList.put(">=",AttrOperator.aopGE);
        String joinType = commandList[1];
        String outerTableName = commandList[2];
        int outerAttrNo = Integer.parseInt(commandList[3]);
        String innerTableName = commandList[4];
        int innerAttrNo = Integer.parseInt(commandList[5]);
        String operator = commandList[6];
        int n_pages = Integer.parseInt(commandList[7]);
        boolean materialize = false;
        String outputTableName = "";
        if(commandList.length == 10){
            materialize = true;
            outputTableName = commandList[9];
        }
        if(tableMetadataMap.get(outerTableName)!=null && tableMetadataMap.get(innerTableName)!=null) {
            switch (joinType) {
                case "NLJ":
                    output_nlj(outerTableName, outerAttrNo, innerTableName, innerAttrNo, operator, n_pages, materialize, outputTableName);
                    break;
                case "SMJ":
                    output_smj(outerTableName, outerAttrNo, innerTableName, innerAttrNo, operator, n_pages, materialize, outputTableName);
                    break;
                case "INLJ":
                    output_inlj(outerTableName, outerAttrNo, innerTableName, innerAttrNo, operator, n_pages, materialize, outputTableName);
                    break;
                case "HJ":
                    output_hj(outerTableName, outerAttrNo, innerTableName, innerAttrNo, operator, n_pages, materialize, outputTableName);
                    break;
                default:
                    System.out.println("Not a valid Join Type!");
                    break;
            }
        } else {
            System.out.println("One or more of the table Names specified does not exist");
        }
    }

    private void output_smj(String outerTableName, int outerAttrNo, String innerTableName, int innerAttrNo, String operator, int n_pages, boolean materialize, String outputTableName) {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(operatorList.get(operator));
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outerAttrNo);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerAttrNo);
        expr[1] = null;


        CondExpr[] scanExpr = new CondExpr[2];
        scanExpr[0] = null;
        scanExpr[1] = null;

        AttrType[] attrType1 = tableMetadataMap.get(outerTableName).attrType;
        short[] attrSize1 = tableMetadataMap.get(outerTableName).attrSize;

        AttrType[] attrType2 = tableMetadataMap.get(innerTableName).attrType;
        short[] attrSize2 = tableMetadataMap.get(innerTableName).attrSize;

        FldSpec[] projlist1 = new FldSpec[attrType1.length];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType1.length;i++) {
            projlist1[i] = new FldSpec(rel1, i+1);
        }

        FldSpec[] projlist2 = new FldSpec[attrType2.length ];
        RelSpec rel2 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType2.length ;i++) {
            projlist2[i] = new FldSpec(rel2, i+1);
        }

        FldSpec[] projlist3 = new FldSpec[attrType1.length + attrType2.length];
        rel2 = new RelSpec(RelSpec.innerRel);
        for(int i =0; i < attrType1.length ;i++) {
            projlist3[i] = new FldSpec(rel1, i+1);
        }
        for(int i =0; i < attrType2.length;i++) {
            projlist3[i+attrType1.length ] = new FldSpec(rel2, i+1);
        }

        FileScan am1 = null;
        try {
            am1  = new FileScan(outerTableName, attrType1, attrSize1,
                    (short)attrType1.length, (short)attrType1.length,
                    projlist1, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        FileScan am2 = null;
        try {
            am2  = new FileScan(innerTableName, attrType2, attrSize2,
                    (short)attrType2.length, (short)attrType2.length,
                    projlist2, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        try {
                TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
                SortMerge sm = new SortMerge(attrType1,
                        attrType1.length,
                        attrSize1,
                        attrType2,
                        attrType2.length,
                        attrSize2,

                        outerAttrNo,
                        4,
                        innerAttrNo,
                        4,

                        n_pages,
                        am1,
                        am2,

                        false,
                        false,
                        ascending,

                        expr,
                        projlist3,
                        attrType1.length + attrType2.length
                );

                Tuple t = new Tuple();
                int jTupleLength = attrType1.length + attrType2.length;
                AttrType[] jList = new AttrType[jTupleLength];
                for (int i = 0; i < attrType1.length; i++) {
                    jList[i] = attrType1[i];
                }
                for (int i = attrType1.length; i < jTupleLength; i++) {
                    jList[i] = attrType2[i - attrType1.length];
                }
                short[] jSize = new short[attrSize1.length+attrSize2.length];
                for (int i = 0; i < attrSize1.length; i++) {
                jSize[i] = attrSize1[i];
                }
                for (int i = attrSize1.length; i < jSize.length; i++) {
                jSize[i] = attrSize2[i - attrSize1.length];
                }

            try {
                t.setHdr((short) jList.length, jList, jSize);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

                while ((t = sm.get_next()) != null) {
                    //print the joined tuples
                    //if table should be materialized
                    //add join table to tablemetadata
                    //and create new table
                    t.print(jList);
                }
            } catch (Exception e){
                e.printStackTrace();
            }

    }

    private void output_nlj(String outerTableName, int outerAttrNo, String innerTableName, int innerAttrNo, String operator, int n_pages, boolean materialize, String outputTableName) {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(operatorList.get(operator));
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outerAttrNo);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerAttrNo);
        expr[1] = null;


        CondExpr[] scanExpr = new CondExpr[2];
        scanExpr[0] = null;
        scanExpr[1] = null;

        AttrType[] attrType1 = tableMetadataMap.get(outerTableName).attrType;
        short[] attrSize1 = tableMetadataMap.get(outerTableName).attrSize;

        AttrType[] attrType2 = tableMetadataMap.get(innerTableName).attrType;
        short[] attrSize2 = tableMetadataMap.get(innerTableName).attrSize;

        FldSpec[] projlist1 = new FldSpec[attrType1.length];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType1.length;i++) {
            projlist1[i] = new FldSpec(rel1, i+1);
        }

        FldSpec[] projlist2 = new FldSpec[attrType1.length + attrType2.length];
        RelSpec rel2 = new RelSpec(RelSpec.innerRel);
        for(int i =0; i < attrType1.length ;i++) {
            projlist2[i] = new FldSpec(rel1, i+1);
        }
        for(int i =0; i < attrType2.length;i++) {
            projlist2[i+attrType1.length ] = new FldSpec(rel2, i+1);
        }

        iterator.Iterator am1 = null;
        String indexFileName = outerTableName+"UNCLUSTBTREE"+outerAttrNo;
        if(tableMetadataMap.get(outerTableName).indexNameList.contains(indexFileName)) {
            try {
                am1 = new IndexScan(new IndexType(IndexType.B_Index), outerTableName, indexFileName
                        , attrType1, attrSize1, attrType1.length, attrType1.length,
                        projlist1, scanExpr, attrType1.length, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }

            try {
                NestedLoopsJoins nlj = new NestedLoopsJoins(attrType1,
                        attrType1.length,
                        attrSize1,
                        attrType2,
                        attrType2.length,
                        attrSize2,
                        n_pages,
                        am1,
                        outerTableName,
                        expr,
                        scanExpr,
                        projlist2,
                        attrType1.length + attrType2.length
                );
                Tuple t = new Tuple();
                int jTupleLength = attrType1.length + attrType2.length;
                AttrType[] jList = new AttrType[jTupleLength];
                for (int i = 0; i < attrType1.length; i++) {
                    jList[i] = attrType1[i];
                }
                for (int i = attrType1.length; i < jTupleLength; i++) {
                    jList[i] = attrType2[i - attrType1.length];
                }
                while ((t = nlj.get_next()) != null) {
                    //print the joined tuples
                    //if table should be materialized
                    //add join table to tablemetadata
                    //and create new table
                    t.print(jList);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        else{
            System.out.println("No unclustered BTree index present on attribute " + outerAttrNo + " for table " + outerTableName);
        }
    }

    private void output_hj(String outerTableName, int outerAttrNo, String innerTableName, int innerAttrNo, String operator, int n_pages, boolean materialize, String outputTableName) {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(operatorList.get(operator));
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outerAttrNo);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerAttrNo);
        expr[1] = null;


        CondExpr[] scanExpr = new CondExpr[2];
        scanExpr[0] = null;
        scanExpr[1] = null;

        AttrType[] attrType1 = tableMetadataMap.get(outerTableName).attrType;
        short[] attrSize1 = tableMetadataMap.get(outerTableName).attrSize;

        AttrType[] attrType2 = tableMetadataMap.get(innerTableName).attrType;
        short[] attrSize2 = tableMetadataMap.get(innerTableName).attrSize;

        FldSpec[] projlist1 = new FldSpec[attrType1.length];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType1.length;i++) {
            projlist1[i] = new FldSpec(rel1, i+1);
        }

        FldSpec[] projlist2 = new FldSpec[attrType1.length + attrType2.length];
        RelSpec rel2 = new RelSpec(RelSpec.innerRel);
        for(int i =0; i < attrType1.length ;i++) {
            projlist2[i] = new FldSpec(rel1, i+1);
        }
        for(int i =0; i < attrType2.length;i++) {
            projlist2[i+attrType1.length ] = new FldSpec(rel2, i+1);
        }

        iterator.Iterator am1 = null;
        String indexFileName = outerTableName+"UNCLUSTBTREE"+outerAttrNo;
        if(tableMetadataMap.get(outerTableName).indexNameList.contains(indexFileName)) {
            try {
                am1 = new IndexScan(new IndexType(IndexType.B_Index), outerTableName, indexFileName
                        , attrType1, attrSize1, attrType1.length, attrType1.length,
                        projlist1, scanExpr, attrType1.length, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }

            try {
                HashJoins inlj = new HashJoins(attrType1,
                        attrType1.length,
                        attrSize1,
                        attrType2,
                        attrType2.length,
                        attrSize2,
                        n_pages,
                        am1,
                        outerTableName,
                        expr,
                        scanExpr,
                        projlist2,
                        attrType1.length + attrType2.length,
                        materialize,
                        innerAttrNo,
                        tableMetadataMap
                );
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        else{
            System.out.println("No unclustered BTree index present on attribute " + outerAttrNo + " for table " + outerTableName);
        }
    }

    private void output_groupby(String[] commandList) {
       //GROUPBY SORT/HASH MAX/MIN/AVG/SKY G_ATTNO {AATTNO1...AATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME]
        String groupType = commandList[1];
        String aggrType = commandList[2];
        int grpAttrNo = Integer.parseInt(commandList[3]);
        String attrs = commandList[4];
        String[] attrArray = attrs.split(",");
        int[] aggAttrArray = new int[attrArray.length];
        for(int i=0; i < aggAttrArray.length;i++){
            aggAttrArray[i] = Integer.parseInt(attrArray[i]);
        }
        String tableName = commandList[5];
        int n_pages = Integer.parseInt(commandList[6]);
        boolean materialize = false;
        String outputTableName = "";
        if(commandList.length == 9){
            materialize = true;
            outputTableName = commandList[8];
        }
        HashMap<String,Integer> aggTypeMap = new HashMap<>();
        aggTypeMap.put("MAX",AggType.aggMax);
        aggTypeMap.put("MIN",AggType.aggMin);
        aggTypeMap.put("AVG",AggType.aggAvg);
        aggTypeMap.put("SKY",AggType.aggSky);

        switch (groupType) {
            case "SORT":
                output_gb_sort(aggTypeMap.get(aggrType),grpAttrNo,aggAttrArray,tableName,n_pages,materialize,outputTableName);
                break;
            case "HASH":
                output_gb_hash(aggTypeMap.get(aggrType),grpAttrNo,aggAttrArray,tableName,n_pages,materialize,outputTableName);
                break;
            default :
                System.out.println("GroupBy Type enetered is invalid");
        }

    }

    private void output_gb_hash(Integer aggrType, int grpAttrNo, int[] aggAttrArray, String tableName, int n_pages, boolean materialize, String outputTableName) {
        if(tableMetadataMap.get(tableName)!=null) {
            AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
            short[] attrSize = tableMetadataMap.get(tableName).attrSize;

            FldSpec group_by_attr = new FldSpec (new RelSpec(RelSpec.outer),grpAttrNo);
            FldSpec[] agg_list = new FldSpec[aggAttrArray.length];
            for(int i=0; i< aggAttrArray.length;i++){
                agg_list[i] = new FldSpec (new RelSpec(RelSpec.outer),aggAttrArray[i]);
            }

            FldSpec[] projlist1 = new FldSpec[attrType.length];
            RelSpec rel1 = new RelSpec(RelSpec.outer);
            for(int i =0; i < attrType.length;i++) {
                projlist1[i] = new FldSpec(rel1, i+1);
            }

            GroupBy gb = new GroupBy();
            try{
                gb.GroupBywithHash(attrType,attrType.length,attrSize,new Heapfile(tableName),group_by_attr,agg_list,new AggType(aggrType),projlist1,attrType.length,n_pages);
            } catch(Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void output_gb_sort(Integer aggrType, int grpAttrNo, int[] aggAttrArray, String tableName, int n_pages, boolean materialize, String outputTableName) {

        if(tableMetadataMap.get(tableName)!=null) {
        AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
        short[] attrSize = tableMetadataMap.get(tableName).attrSize;

        FldSpec group_by_attr = new FldSpec (new RelSpec(RelSpec.outer),grpAttrNo);
        FldSpec[] agg_list = new FldSpec[aggAttrArray.length];
        for(int i=0; i< aggAttrArray.length;i++){
            agg_list[i] = new FldSpec (new RelSpec(RelSpec.outer),aggAttrArray[i]);
        }

        FldSpec[] projlist1 = new FldSpec[attrType.length];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType.length;i++) {
                projlist1[i] = new FldSpec(rel1, i+1);
        }

        GroupBy gb = new GroupBy();
        try{
            gb.GroupBywithSort(attrType,attrType.length,attrSize,new Heapfile(tableName),group_by_attr,agg_list,new AggType(aggrType),projlist1,attrType.length,n_pages);
        } catch(Exception e) {
            e.printStackTrace();
        }

        }

    }

    private void output_inlj(String outerTableName, int outerAttrNo, String innerTableName, int innerAttrNo, String operator, int n_pages, boolean materialize, String outputTableName) {

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(operatorList.get(operator));
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outerAttrNo);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerAttrNo);
        expr[1] = null;


        CondExpr[] scanExpr = new CondExpr[2];
        scanExpr[0] = null;
        scanExpr[1] = null;

        AttrType[] attrType1 = tableMetadataMap.get(outerTableName).attrType;
        short[] attrSize1 = tableMetadataMap.get(outerTableName).attrSize;

        AttrType[] attrType2 = tableMetadataMap.get(innerTableName).attrType;
        short[] attrSize2 = tableMetadataMap.get(innerTableName).attrSize;

        FldSpec[] projlist1 = new FldSpec[attrType1.length];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        for(int i =0; i < attrType1.length;i++) {
            projlist1[i] = new FldSpec(rel1, i+1);
        }

        FldSpec[] projlist2 = new FldSpec[attrType1.length + attrType2.length];
        RelSpec rel2 = new RelSpec(RelSpec.innerRel);
        for(int i =0; i < attrType1.length ;i++) {
            projlist2[i] = new FldSpec(rel1, i+1);
        }
        for(int i =0; i < attrType2.length;i++) {
            projlist2[i+attrType1.length ] = new FldSpec(rel2, i+1);
        }

        iterator.Iterator am1 = null;
        String indexFileName = outerTableName+"UNCLUSTBTREE"+outerAttrNo;
        if(tableMetadataMap.get(outerTableName).indexNameList.contains(indexFileName)) {
            try {
                am1 = new IndexScan(new IndexType(IndexType.B_Index), outerTableName, indexFileName
                        , attrType1, attrSize1, attrType1.length, attrType1.length,
                        projlist1, scanExpr, attrType1.length, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }

            try {
                IndexNestedLoopJoins inlj = new IndexNestedLoopJoins(attrType1,
                        attrType1.length,
                        attrSize1,
                        attrType2,
                        attrType2.length,
                        attrSize2,
                        n_pages,
                        am1,
                        outerTableName,
                        expr,
                        scanExpr,
                        projlist2,
                        attrType1.length + attrType2.length,
                        materialize,
                        innerAttrNo,
                        tableMetadataMap
                );
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        else{
                System.out.println("No unclustered BTree index present on attribute " + outerAttrNo + " for table " + outerTableName);
            }

    }

    private void output_topk(String[] commandList) {
        String joinType = commandList[1];
        int k = Integer.parseInt(commandList[2]);
        String tableName1 = commandList[3];
        int joinAttr1 = Integer.parseInt(commandList[4]);
        int mergeAttr1 = Integer.parseInt(commandList[5]);
        String tableName2 = commandList[6];
        int joinAttr2 = Integer.parseInt(commandList[7]);
        int mergeAttr2 = Integer.parseInt(commandList[8]);
        int num_pages = Integer.parseInt(commandList[9]);
        String outTableName = "";
        boolean materialize = false;
        if(commandList.length == 12){
            materialize = true;
            outTableName = commandList[11];
        }
        AttrType[] attrType1 = tableMetadataMap.get(tableName1).attrType;
        short[] attrSize1 = tableMetadataMap.get(tableName1).attrSize;

        AttrType[] attrType2 = tableMetadataMap.get(tableName2).attrType;
        short[] attrSize2 = tableMetadataMap.get(tableName2).attrSize;

        if(tableMetadataMap.get(outTableName)==null) {
            //check if index files present for the mergeAttrs. Else throw error
            if (checkIfIndexExists(tableName1,mergeAttr1,"UNCLUST","BTREE")) {
                if (checkIfIndexExists(tableName1,mergeAttr1,"UNCLUST","BTREE")) {

                    if (joinType.equals("HASH")) {
                        try {

                            TopK_HashJoin topk = new TopK_HashJoin(
                                    attrType1, attrType1.length, attrSize1, new FldSpec(new RelSpec(RelSpec.outer), joinAttr1),
                                    new FldSpec(new RelSpec(RelSpec.outer), mergeAttr1),
                                    attrType2, attrType2.length, attrSize2, new FldSpec(new RelSpec(RelSpec.outer), joinAttr2),
                                    new FldSpec(new RelSpec(RelSpec.outer), mergeAttr2),
                                    tableName1,
                                    tableName2,
                                    k,
                                    num_pages,outTableName,tableMetadataMap
                            );
                        }catch (Exception e) {
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }

                    } else if (joinType.equals("NRA")) {
                        TopK_NRAJoin topk = new  TopK_NRAJoin(
                                attrType1, attrType1.length, attrSize1,
                                new FldSpec(new RelSpec(RelSpec.outer), joinAttr1), new FldSpec(new RelSpec(RelSpec.outer), mergeAttr1),
                                attrType2, attrType2.length, attrSize2,
                                new FldSpec(new RelSpec(RelSpec.outer), joinAttr2),  new FldSpec(new RelSpec(RelSpec.outer), mergeAttr2),
                                tableName1,
                                tableName2,
                                k,
                                num_pages,outTableName,tableMetadataMap);
                    }
                } else {
                    System.out.println("No index present for attribute "+mergeAttr2+ " for table "+tableName2+". Please create an index on this attribute to use TopK on this attribute");
                }
            } else {
                System.out.println("No index present for attribute "+mergeAttr1+ " for table "+tableName1+". Please create an index on this attribute to use TopK on this attribute");
            }
        } else {
            System.out.println("OUTTABLENAME specified already exists. Please specify a new output table name");
        }

    }

    private boolean checkIfIndexExists(String tableName, int attrNo, String clustType /*UNCLUST or CLUST*/, String indexType /* BTREE or HASH*/) {
        ArrayList<String> indexList = tableMetadataMap.get(tableName).indexNameList;
        String indexName = tableName+clustType+indexType+attrNo;
        return indexList.contains(indexName);
    }

    private void output_skyline(String[] commandList) {
        //skyline NLS/BNLS/SFS/BTS/BTSS {ATTNO1...ATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME]
        String skylineOperation = commandList[1];
        String attrList = commandList[2];
        String tableName = commandList[3];
        String outTableName = "";
        int n_pages = Integer.parseInt(commandList[4]);
        boolean materialize = false;
        if(commandList.length==7){
            materialize = true;
            outTableName = commandList[6];
        }
        switch (skylineOperation){
            case "NLS":
                output_NLS(tableName,attrList,outTableName,n_pages,materialize);
                break;
            case "BNLS":
                output_BNLS(tableName,attrList,outTableName,n_pages,materialize);
                break;
            case "SFS":
                output_SFS(tableName,attrList,outTableName,n_pages,materialize);
                break;
            case "BTS":
                output_BTS(tableName,attrList,outTableName,n_pages,materialize);
                break;
            case "BTSS":
                output_BTSS(tableName,attrList,outTableName,n_pages,materialize);
                break;
            default:
                System.out.println("Invalid skyline operator specified. Please recheck.");
        }

    }

    private void output_BTSS(String tableName, String attrList, String outTableName, int n_pages, boolean materialize) {
        //check whether all pref list attributes have a corresponding index file
        // Check for TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
            if (tableMetadataMap.get(outTableName) == null) {
                FileScan am = null;
                String[] preflistArr = attrList.split(",");
                int[] preflist = new int[preflistArr.length];

                Heapfile hf = null;
                try {
                    hf = new Heapfile(tableName);
                } catch (Exception e) {
                    System.err.println("" + e);
                }
                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;

                for (int i = 0; i < preflistArr.length; i++) {
                    preflist[i] = Integer.parseInt(preflistArr[i]) - 1;
                }

                FldSpec[] Pprojection = new FldSpec[attrType.length];
                for (int i = 0; i < attrType.length; i++) {
                    Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                }
                try {
                    BTreeFile btf = new BTreeFile("BSortedIndex", AttrType.attrInteger, REC_LEN1, 1/*delete*/);

                    Scan scan = null;

                    try {
                        scan = new Scan(hf);
                    } catch (Exception e) {
                        //status = FAIL;
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    RID rid = new RID();
                    KeyClass key;
                    Tuple temp = null;

                    Tuple t = new Tuple();
                    try {
                        t.setHdr((short) attrType.length, attrType, attrSize);
                    } catch (Exception e) {
                        System.err.println("*** error in Tuple.setHdr() ***");
                        e.printStackTrace();
                    }
                    while ((temp = scan.getNext(rid)) != null) {
                        t.tupleCopy(temp);
                        try {
                            float floatKey = (TupleUtils.computeTupleSumOfPrefAttrs(t, attrType, (short) attrType.length, attrSize, preflist, preflist.length));
                            int intKey = (int) floatKey;
                            key = new IntegerKey(intKey);
                            btf.insert(key, rid);
                        } catch (Exception e) {
                            //status = FAIL;
                            e.printStackTrace();
                        }
                    }

                    am = null;
                    try {
                        am = new FileScan(tableName, attrType, attrSize, (short) attrType.length, (short) attrType.length, Pprojection, null);

                    } catch (Exception e) {
                        System.err.println("" + e);
                    }

                    try {
                        BTreeSortedSky bts = new BTreeSortedSky(attrType, attrType.length, attrSize, am, tableName, preflist, preflist.length, btf, n_pages, outTableName, tableMetadataMap);
                        bts.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    System.err.println("" + e);
                }
            }
    }

    private void output_BTS(String tableName, String attrList, String outTableName, int n_pages, boolean materialize) {
        //check whether all pref list attributes have a corresponding index file
        // Check for TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
            if (tableMetadataMap.get(outTableName) == null) {
                FileScan am = null;
                String[] preflistArr = attrList.split(",");
                ArrayList<String> indexList = tableMetadataMap.get(tableName).indexNameList;
                boolean indexExists = true;
                int[] preflist = new int[preflistArr.length];
                ArrayList<String> indexFileList = new ArrayList<>();
                for (int i = 0; i < preflistArr.length; i++) {
                    String indexFileName = tableName + "UNCLUSTBTREE" + preflistArr[i];
                    if (!indexList.contains(indexFileName)) {
                        System.out.println("There is no unclustered BTree index on attribute " + preflistArr[i] + ". Please create an index on the attribute to use it.");
                        indexExists = false;
                    } else {
                        indexFileList.add(indexFileName);
                    }
                }
                if (indexExists) {
                    Heapfile hf = null;
                    try {
                        hf = new Heapfile(tableName);
                    } catch (Exception e) {
                        System.err.println("" + e);
                    }
                    AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                    short[] attrSize = tableMetadataMap.get(tableName).attrSize;

                    for (int i = 0; i < preflistArr.length; i++) {
                        preflist[i] = Integer.parseInt(preflistArr[i]) - 1;
                    }

                    FldSpec[] Pprojection = new FldSpec[attrType.length];
                    for (int i = 0; i < attrType.length; i++) {
                        Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                    }

                    am = null;
                    try {
                        am = new FileScan(tableName, attrType, attrSize, (short) attrType.length, (short) attrType.length, Pprojection, null);

                    } catch (Exception e) {
                        System.err.println("" + e);
                    }

                    BTreeFile[] BTreeFileList = new BTreeFile[indexFileList.size()];
                    for (int i = 0; i < indexFileList.size(); i++) {
                        try {
                            BTreeFileList[i] = new BTreeFile(indexFileList.get(i));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    try {
                        BTreeSky bts = new BTreeSky(attrType, attrType.length, attrSize, am, tableName,
                                preflist, preflist.length, BTreeFileList, n_pages, hf, attrType.length, outTableName, tableMetadataMap);
                        bts.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("OUTTABLENAME specified already exists. Please specify a new output table name");
            }

    }

    private void output_SFS(String tableName, String attrList, String outTableName, int n_pages, boolean materialize) {
            if (tableMetadataMap.get(outTableName) == null) {
                FileScan am = null;
                Heapfile outHeap = null;
                String[] preflistArr = attrList.split(",");
                int[] preflist = new int[preflistArr.length];
                for (int i = 0; i < preflistArr.length; i++) {
                    preflist[i] = Integer.parseInt(preflistArr[i]) - 1;
                }

                Tuple t = new Tuple();
                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;
                try {
                    t.setHdr((short) attrType.length, attrType, attrSize);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }

                FldSpec[] Pprojection = new FldSpec[attrType.length];
                for (int i = 0; i < attrType.length; i++) {
                    Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                }

                am = null;
                try {
                    am = new FileScan(tableName, attrType, attrSize, (short) attrType.length, (short) attrType.length, Pprojection, null);

                } catch (Exception e) {
                    System.err.println("" + e);
                }

                try {

                    SortFirstSky s = new SortFirstSky(attrType, attrType.length, attrSize, am,
                            tableName, preflist, preflist.length, n_pages);
                    if (materialize) {
                        outHeap = new Heapfile(outTableName);
                    }

                    while ((t = s.get_next()) != null) {
                        t.print(attrType);
                        if (materialize) {
                            outHeap.insertRecord(t.returnTupleByteArray());
                        }

                    }
                    s.close();
                    if (materialize) {
                        //update the table metadata map
                        TableMetadata tm = new TableMetadata(outTableName, attrType, attrSize);
                        tableMetadataMap.put(outTableName, tm);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
            } else {
                System.out.println("OUTTABLENAME specified already exists. Please specify a new output table name");
            }
    }

    private void output_BNLS(String tableName, String attrList, String outTableName, int n_pages, boolean materialize) {
            if (tableMetadataMap.get(outTableName) == null) {

                FileScan am = null;
                Heapfile outHeap = null;
                String[] preflistArr = attrList.split(",");
                int[] preflist = new int[preflistArr.length];
                for (int i = 0; i < preflistArr.length; i++) {
                    preflist[i] = Integer.parseInt(preflistArr[i]) - 1;
                }

                Tuple t = new Tuple();
                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;
                try {
                    t.setHdr((short) attrType.length, attrType, attrSize);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }

                FldSpec[] Pprojection = new FldSpec[attrType.length];
                for (int i = 0; i < attrType.length; i++) {
                    Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                }

                am = null;
                try {
                    am = new FileScan(tableName, attrType, attrSize, (short) attrType.length, (short) attrType.length, Pprojection, null);

                } catch (Exception e) {
                    System.err.println("" + e);
                }

                try {

                    BlockNestedLoopsSky s = new BlockNestedLoopsSky(attrType, attrType.length, attrSize, am,
                            tableName, preflist, preflist.length, n_pages);
                    if (materialize) {
                        outHeap = new Heapfile(outTableName);
                    }

                    while ((t = s.get_next()) != null) {
                        t.print(attrType);
                        if (materialize) {
                            outHeap.insertRecord(t.returnTupleByteArray());
                        }

                    }
                    s.close();
                    if (materialize) {
                        //update the table metadata map
                        TableMetadata tm = new TableMetadata(outTableName, attrType, attrSize);
                        tableMetadataMap.put(outTableName, tm);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
            } else {
                System.out.println("OUTTABLENAME specified already exists. Please specify a new output table name");
            }
    }

    private void output_NLS(String tableName, String attrList, String outTableName, int n_pages, boolean materialize) {
            if (tableMetadataMap.get(outTableName) == null) {
                FileScan am = null;
                Heapfile outHeap = null;
                String[] preflistArr = attrList.split(",");
                int[] preflist = new int[preflistArr.length];
                for (int i = 0; i < preflistArr.length; i++) {
                    preflist[i] = Integer.parseInt(preflistArr[i]) - 1;
                }

                Tuple t = new Tuple();
                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;
                try {
                    t.setHdr((short) attrType.length, attrType, attrSize);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }

                FldSpec[] Pprojection = new FldSpec[attrType.length];
                for (int i = 0; i < attrType.length; i++) {
                    Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                }

                am = null;
                try {
                    am = new FileScan(tableName, attrType, attrSize, (short) attrType.length, (short) attrType.length, Pprojection, null);

                } catch (Exception e) {
                    System.err.println("" + e);
                }

                try {

                    NestedLoopsSky s = new NestedLoopsSky(attrType, attrType.length, attrSize, am,
                            tableName, preflist, preflist.length, n_pages);
                    if (materialize) {
                        outHeap = new Heapfile(outTableName);
                    }

                    while ((t = s.get_next()) != null) {
                        t.print(attrType);
                        if (materialize) {
                            outHeap.insertRecord(t.returnTupleByteArray());
                        }

                    }
                    s.close();
                    if (materialize) {
                        //update the table metadata map
                        TableMetadata tm = new TableMetadata(outTableName, attrType, attrSize);
                        tableMetadataMap.put(outTableName, tm);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
            } else {
                System.out.println("OUTTABLENAME specified already exists. Please specify a new output table name");
            }

    }

    private void output_index(String[] commandList) {
        String tableName = commandList[1];
        int attrNo = Integer.parseInt(commandList[2]);
        ArrayList<String> attrIndexList = new ArrayList<>();

        ArrayList<String> indexList = tableMetadataMap.get(tableName).indexNameList;
        for(int i=0;i<indexList.size();i++){
            // Check for TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
            String indexName = indexList.get(i);
            int position = indexName.lastIndexOf(String.valueOf(attrNo));
            if(position!=-1){
                attrIndexList.add(indexName);
            }
        }
        if(attrIndexList.size()!=0){
            System.out.println(attrIndexList.size()+ " indexes present for the attribute number "+attrNo);
            System.out.println("Index List : ");
            for(String indexName : attrIndexList){
                System.out.println(indexName);
            }
            String indexFileName = attrIndexList.get(0);
            boolean unclustered = indexFileName.contains("UNCLUST");
            boolean btree = indexFileName.contains("BTREE");
            if(unclustered && btree){
                // unclustered Btree
                printKeysForUnclustBTree(tableName,indexFileName,attrNo,IndexType.B_Index);

            } else if(unclustered && !btree) {
                // unclustered Hash
                printKeysForUnclustBTree(tableName,indexFileName,attrNo,IndexType.Hash);
            } else {
                // clustered Hash
                printKeysForUnclustBTree(tableName,indexFileName,attrNo,IndexType.Hash);
            }

        } else {
            System.out.println("N/A");
        }


    }

    private void printKeysForUnclustBTree(String tableName, String indexFileName, int attrNo, int indexType) {
        AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
        short[] attrSize = tableMetadataMap.get(tableName).attrSize;
        int numOfColumns = attrType.length;

        FldSpec[] projlist = new FldSpec[numOfColumns];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i =0; i < numOfColumns;i++) {
            projlist[i] = new FldSpec(rel, i+1);
        }
        CondExpr[] expr = new CondExpr[2];
        expr[0] = null;
        expr[1] = null;

        try {
            IndexScan iscan = new IndexScan(new IndexType(indexType), tableName, indexFileName,
                    attrType, attrSize, numOfColumns, numOfColumns, projlist, expr, numOfColumns, false);
            Tuple temp = null;
            Tuple scanTuple = new Tuple();
            try {
                scanTuple.setHdr((short) numOfColumns, attrType, attrSize);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Keys from index file : ");
            while((temp= iscan.get_next())!=null){
                scanTuple.tupleCopy(temp);
                if(attrType[attrNo-1].attrType==AttrType.attrString){
                    System.out.println(scanTuple.getStrFld(attrNo));
                } else {
                    System.out.println(scanTuple.getIntFld(attrNo));
                }
            }

        } catch (Exception e){
            e.printStackTrace();
        }

    }

    private void delete_data(String[] commandList) {
        String tableName = commandList[1];
        String fileName = commandList[2];

        if(tableMetadataMap.get(tableName)!=null) {
            File obj = new File(fileName);
            Heapfile hf = null;

            try {
                Scanner s = new Scanner(obj);
                String firstLine = s.nextLine();
                String[] firstLineArray = firstLine.split(",");
                int numOfColumns2 = Integer.parseInt(firstLineArray[0]);
                AttrType[] attrType2 = new AttrType[numOfColumns2];
                for (int i = 0; i < numOfColumns2; i++) {
                    String columnInfo = s.nextLine();
                    String[] columnInfoArr = columnInfo.split(",");
                    String attrType3 = columnInfoArr[1];
                    if (attrType3.equals("STR")) {
                        attrType2[i] = new AttrType(AttrType.attrString);
                    } else {
                        attrType2[i] = new AttrType(AttrType.attrInteger);
                    }
                }

                hf = new Heapfile(tableName);
                Scan scan = null;

                try {
                    scan = new Scan(hf);
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }

                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;

                //compare table attrtype with file attrtype
                boolean typeMatch = true;
                if (attrType.length == attrType2.length) {
                    for (int i = 0; i < attrType.length; i++) {
                        if (attrType[i].attrType != attrType2[i].attrType) {
                            typeMatch = false;
                        }
                    }
                    if (typeMatch) {

                        Tuple t = new Tuple();
                        Tuple t2 = new Tuple();
                        Tuple temp = null;
                        try {
                            t.setHdr((short) attrType.length, attrType, attrSize);
                            t2.setHdr((short) attrType.length, attrType, attrSize);
                        } catch (Exception e) {
                            System.err.println("*** error in Tuple.setHdr() ***");
                            e.printStackTrace();
                        }
                        RID rid = new RID();

                        //capture all RIDs
                        ArrayList<RID> ridList = new ArrayList<>();
                        while ((temp = scan.getNext(rid)) != null) {
//                            System.out.print(" RID PageNo : "+rid.pageNo+ " SlotNo: "+ rid.slotNo);
                            ridList.add(rid);
                        }

                        try {
                            scan = new Scan(hf);
                        } catch (Exception e) {
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }

                        for(int j =0; j < ridList.size();j++) {
                                temp = scan.getNext(ridList.get(j));
                                if(temp!=null) {
                                    t2.tupleCopy(temp);
//                                    t2.print(attrType);
                                    while (s.hasNextLine()) {
                                        String dataLine = s.nextLine();
                                        String[] dataArray = dataLine.split(",");
                                        for (int i = 0; i < dataArray.length; i++) {
                                            try {
                                                if (attrType[i].attrType == AttrType.attrInteger) {
                                                    t.setIntFld(i + 1, Integer.parseInt(dataArray[i]));
                                                } else if (attrType[i].attrType == AttrType.attrString) {
                                                    t.setStrFld(i + 1, dataArray[i]);
                                                }
                                            } catch (Exception e) {
                                                System.err.println("*** Heapfile creation error ***");
                                                e.printStackTrace();
                                                Runtime.getRuntime().exit(1);
                                            }
                                        }
                                        // tuple is ready for comparison and delete
                                        try {
                                            t2.print(attrType);
                                            t.print(attrType);
                                            if(compareTuplesEquality(t2,t,attrType)) {
                                                System.out.println("Record deleted ");
                                                hf.deleteRecord(rid);
                                            }

                                        } catch (Exception e) {
                                            System.err.println("*** error in Heapfile.insertRecord() ***");
                                            e.printStackTrace();
                                            Runtime.getRuntime().exit(1);
                                        }
                                        t2.setHdr((short) attrType.length, attrType, attrSize);
                                    }
                                    // set scanner object again, but skip numOfColumns+1 lines
                                    s = new Scanner(obj);
                                    for(int jj=0;jj<= attrType.length;jj++) {
                                        s.nextLine();
                                    }
                                }
                        }
                    } else{
                        System.out.println("Table data type does not match with file data type");
                    }

                    s.close();
                }else{
                    System.out.println("Table data type does not match with file data type");
                }

            } catch (Exception e) {
            }
        }else {
            System.out.println("Table does not exist! Create table first.");
        }

    }

    private boolean compareTuplesEquality(Tuple t2, Tuple t,AttrType[] attrType) {
        boolean isequal = true;
        for (int i = 0; i < attrType.length; i++) {
            try {
                if (attrType[i].attrType == AttrType.attrInteger) {
                    if(t.getIntFld(i + 1)!=t2.getIntFld(i + 1)) {
                        isequal = false;
                        break;
                    }
                } else if (attrType[i].attrType == AttrType.attrString) {
                    if(!(t.getStrFld(i + 1).equals(t2.getStrFld(i + 1)))) {
                        isequal = false;
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("*** Heapfile creation error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
        return isequal;
    }

    private void output_table(String[] commandList) {
        String tableName = commandList[1];

        Heapfile heapFileName = null;

        try {
            heapFileName = new Heapfile(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple t = new Tuple();
        AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
        short[] attrSize = tableMetadataMap.get(tableName).attrSize;
        try {
            t.setHdr((short) attrType.length, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        Scan scan = null;

        try {
            scan = new Scan(heapFileName);
            RID rid = new RID();
            Tuple temp = null;
            System.out.println("Printing table data : "+ tableName);
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                t.print(attrType);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    private void insert_data(String[] commandList) {
        String tableName = commandList[1];
        String fileName = commandList[2];

        if(tableMetadataMap.get(tableName)!=null){
            File obj = new File(fileName);
            Heapfile hf = null;

            try {
                Scanner s = new Scanner(obj);
                String firstLine = s.nextLine();
                String[] firstLineArray = firstLine.split(",");
                int numOfColumns2 = Integer.parseInt(firstLineArray[0]);
                AttrType[] attrType2 = new AttrType[numOfColumns2];
                for (int i = 0; i < numOfColumns2; i++) {
                    String columnInfo = s.nextLine();
                    String[] columnInfoArr = columnInfo.split(",");
                    String attrType3 = columnInfoArr[1];
                    if (attrType3.equals("STR")) {
                        attrType2[i] = new AttrType(AttrType.attrString);
                    } else {
                        attrType2[i] = new AttrType(AttrType.attrInteger);
                    }
                }

                hf = new Heapfile(tableName);

                AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
                short[] attrSize = tableMetadataMap.get(tableName).attrSize;

                //compare table attrtype with file attrtype
                boolean typeMatch = true;
                if(attrType.length== attrType2.length) {
                    for (int i = 0; i < attrType.length; i++) {
                        if (attrType[i].attrType != attrType2[i].attrType) {
                            typeMatch = false;
                        }
                    }
                    if (typeMatch) {

                        Tuple t = new Tuple();
                        RID rid;
                        try {
                            t.setHdr((short) attrType.length, attrType, attrSize);
                        } catch (Exception e) {
                            System.err.println("*** error in Tuple.setHdr() ***");
                            e.printStackTrace();
                        }

                        while (s.hasNextLine()) {
                            String dataLine = s.nextLine();
                            String[] dataArray = dataLine.split(",");
                            for (int i = 0; i < dataArray.length; i++) {
                                try {
                                    if (attrType[i].attrType == AttrType.attrInteger) {
                                        t.setIntFld(i + 1, Integer.parseInt(dataArray[i]));
                                    } else if (attrType[i].attrType == AttrType.attrString) {
                                        t.setStrFld(i + 1, dataArray[i]);
                                    }
                                } catch (Exception e) {
                                    System.err.println("*** Heapfile creation error ***");
                                    e.printStackTrace();
                                    Runtime.getRuntime().exit(1);
                                }
                            }
                            // tuple is ready for insertion
                            try {
                                t.print(attrType);
                                rid = hf.insertRecord(t.returnTupleByteArray());
                            } catch (Exception e) {
                                System.err.println("*** error in Heapfile.insertRecord() ***");
                                e.printStackTrace();
                                Runtime.getRuntime().exit(1);
                            }

                        }
                    } else {
                        System.out.println("Table data type does not match with file data type");
                    }
                } else {
                    System.out.println("Table data type does not match with file data type");
                }
                s.close();
            }catch (Exception e) {
                    e.printStackTrace();
                }

        } else {
            System.out.println("Table does not exist! Create table first.");
        }
    }

    private void create_index(String[] commandList) {
        String tableName;
        int attrNo;

        attrNo = Integer.parseInt(commandList[2]);
        tableName = commandList[3];
        if(tableMetadataMap.containsKey(tableName)) {
            if (commandList[1].equals("BTREE")) {
                createUnclusteredBTreeIndex(tableName, attrNo);
            } else if (commandList[1].equals("HASH")) {
                createUnclusteredHashIndex(tableName, attrNo);
            } else {
                System.out.println("Index type is wrong. Please try again");
            }
        } else {
            System.out.println("Table doesn't exist. Create table before creating an index");
        }
    }

    private void createUnclusteredHashIndex(String tableName, int attrNo) {
        float utilization = 0.75f;
        String indexFileName = tableName + "UNCLUST" + "HASH" + attrNo;

        TableMetadata tm = tableMetadataMap.get(tableName);
        // Check for TABLE_NAME+CLUST/UNCLUST+BTREE/HASH+FIELDNO
        if (!tm.getIndexNameList().contains(indexFileName)) {
            Heapfile hf = null;
            try {
                hf = new Heapfile(tableName);
            } catch (Exception e) {
            }

            AttrType[] attrType = tableMetadataMap.get(tableName).attrType;
            short[] attrSize = tableMetadataMap.get(tableName).attrSize;

            Tuple t = new Tuple();
            try {
                t.setHdr((short) attrType.length, attrType, attrSize);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

            Scan scan = null;

            try {
                scan = new Scan(hf);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

            try {

                HashFile hash_file = new HashFile(indexFileName, attrType[attrNo-1].attrType, REC_LEN1, 1, utilization);
                RID rid = new RID();
                Tuple temp = null;
                hash.KeyClass key = null;

                while ((temp = scan.getNext(rid)) != null) {
                    t.tupleCopy(temp);
                    if (attrType[attrNo-1].attrType == AttrType.attrInteger) {
                        int intKey = t.getIntFld(attrNo);
                        key = new hash.IntegerKey(intKey);
                    } else if (attrType[attrNo-1].attrType == AttrType.attrString) {
                        String strKey = t.getStrFld(attrNo);
                        key = new hash.StringKey(strKey);
                    }
                    hash_file.insert(key, rid);
                }

                updateIndexEntryToTableMetadata(tableName, indexFileName);

            }catch(Exception e){
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
         else{
                System.out.println("Unclustered Hash index already exist for table " + tableName + " - " + attrNo);
            }
        }
    private void createClusteredHashIndex(String tableName, int attrNo) {
        String indexFileName = tableName+"CLUST"+"HASH"+attrNo;

        updateIndexEntryToTableMetadata(tableName,indexFileName);
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

    private void create_table(String[] commandList) {
        String fileName =null;
        String tableName;
        int attrNo =0;
        if(commandList.length ==2) {
            // only table creation
            fileName = commandList[1];
            createHeapFile(fileName);

        } else if(commandList.length ==5){
            attrNo = Integer.parseInt(commandList[3]);
            fileName = commandList[4];
            createHeapFile(fileName);
            createClusteredHashIndex(fileName,attrNo);
        } else {
            System.out.println("Invalid syntax. Please recheck");
        }

    }

    private void createHeapFile(String fileName) {

        int fileNameStartIndex = fileName.lastIndexOf("/");
        int dotIndex = fileName.lastIndexOf(".");
        String tableName = fileName.substring(fileNameStartIndex+1,dotIndex);

        if(!tableMetadataMap.containsKey(tableName)) {
            Scanner s;
            File obj = new File(fileName);
            int numOfColumns;

            Heapfile heapFileName = null;

            try {
                heapFileName = new Heapfile(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }

            RID rid;
            Tuple t = new Tuple();
            int stringCount = 0;
            try {
                s = new Scanner(obj);
                String firstLine = s.nextLine();
                String[] firstLineArray = firstLine.split(",");
                numOfColumns = Integer.parseInt(firstLineArray[0]);
                AttrType[] attrType = new AttrType[numOfColumns];
                for (int i = 0; i < numOfColumns; i++) {
                    String columnInfo = s.nextLine();
                    String[] columnInfoArr = columnInfo.split(",");
                    String attrType2 = columnInfoArr[1];
                    if (attrType2.equals("STR")) {
                        attrType[i] = new AttrType(AttrType.attrString);
                        stringCount++;
                    } else {
                        attrType[i] = new AttrType(AttrType.attrInteger);
                    }
                }
                //set attrSize
                short[] attrSize = new short[stringCount];
                for(int i=0;i< attrSize.length;i++){
                    attrSize[i] = STR_LEN;
                }

                try {
                    t.setHdr((short) numOfColumns, attrType, attrSize);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }

                while (s.hasNextLine()) {
                    String dataLine = s.nextLine();
                    String[] dataArray = dataLine.split(",");
                    for (int i = 0; i < dataArray.length; i++) {
                        try {
                            if (attrType[i].attrType == AttrType.attrInteger) {
                                t.setIntFld(i + 1, Integer.parseInt(dataArray[i]));
                            } else if (attrType[i].attrType == AttrType.attrString) {
                                t.setStrFld(i + 1, dataArray[i]);
                            }
                        } catch (Exception e) {
                            System.err.println("*** Heapfile creation error ***");
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                    }

                    // tuple is ready for insertion
                    try {
                        t.print(attrType);
                        rid = heapFileName.insertRecord(t.returnTupleByteArray());
                    } catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                }
                s.close();

                //update the table metadata map
                TableMetadata tm = new TableMetadata(tableName, attrType,attrSize);
                tableMetadataMap.put(tableName, tm);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Table already exist!");
        }
    }

    private void close_database() {
        try {
            //delete metadata file and push new map
            File myObj = new File(metadatafileName);
            myObj.delete();
            FileOutputStream fileOut =
                    new FileOutputStream(metadatafileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tableMetadataMap);
            out.close();
            fileOut.close();

            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e){
            System.out.println("Closing Database");
        }
    }

    private void open_database(String[] commandList) {
        String dbName = commandList[1];
        String dbpathUser = "/tmp/"+dbName+System.getProperty("user.name")+".minibasev8-db";
        SystemDefs sysdef = new SystemDefs(dbpathUser, 10000, NUMBUF, "Clock");
        //populate the table metadataList
        try{
            File tempFile = new File(metadatafileName);
            boolean exists = tempFile.exists();
            if (exists) {
                FileInputStream fileIn = new FileInputStream(metadatafileName);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                tableMetadataMap = (HashMap<String, TableMetadata>) in.readObject();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }
    }

    protected boolean test2(){
        return true;
    }
    protected boolean test3(){
        return true;
    }
    protected boolean test4(){
        return true;
    }
    protected boolean test5(){
        return true;
    }
    protected boolean test6(){
        return true;
    }

}

public class Phase3InterfaceTest {
    public static void main(String[] args){
        Phase3InterfaceTestDriver sDriver = null;
        try {
            sDriver = new Phase3InterfaceTestDriver();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sDriver.runTests();
    }

}

