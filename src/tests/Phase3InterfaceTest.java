package tests;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import global.*;
import hash.HashFile;
import heap.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

class Phase3InterfaceTestDriver extends TestDriver
        implements GlobalConst {

    HashMap<String, TableMetadata> tableMetadataMap = new HashMap<>();
    public static final int STR_LEN = 10;
    private static short REC_LEN1 = 15;
    public static final String metadatafileName = "/tmp/tablemetadata.ser";

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
                "\n 9. skyline NLS/BNLS/SFS/BTS/BTS {ATTNO1...ATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME]" +
                "\n 10. GROUPBY SORT/HASH MAX/MIN/AGG/SKYGATTNO {AATTNO1...AATTNOh} TABLENAME NPAGES [MATER OUTTABLENAME] " +
                "\n 11. JOIN NLJ/SMJ/INLJ/HJ OTABLENAME OATTNO ITABLENAME IATTNO OP NPAGES [MATER OUTTABLENAME] " +
                "\n 12. TOPKJOIN HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_JATT_NO I_MATT_NO NPAGES [MATER OUTTABLENAME] " +
                "\n 13. exit_db \n \n");

        //open_database test1
        //create_table src/data/phase3demo/r_sii2000_1_75_200.csv
        //create_index BTREE 1 r_sii2000_1_75_200
        //create_index BTREE 2 r_sii2000_1_75_200
        //create_index BTREE 2 r_sii2000_1_75_200 -- fail
        //create_index HASH 1 r_sii2000_1_75_200
        //create_index HASH 2 r_sii2000_1_75_200
        //create_index HASH 2 r_sii2000_1_75_200 --fail
        //insert_data r_sii2000_1_75_200 src/data/phase3demo/r_sii2000_10_10_10.csv
        //delete_data r_sii2000_1_75_200 src/data/phase3demo/r_sii2000_10_10_10.csv
        //output_table r_sii2000_1_75_200

        while(continueWhile){
            Scanner sc = new Scanner(System.in);
            System.out.println(" Enter the query for execution : ");

            String command = sc.nextLine();
            String[] commandList = command.split(" ");
            switch (commandList[0]) {
                case "open_database" :
                    open_database(commandList);
                    break;
                case  "close_database" :
                    close_database();
                    break;
                case "create_table":
                    create_table(commandList);
                    break;
                case "create_index" :
                    create_index(commandList);
                    break;
                case "insert_data" :
                    insert_data(commandList);
                    break;
                case "delete_data":
                    delete_data(commandList);
                    break;
                case "output_table":
                    output_table(commandList);
                    break;
                case "output_index":
                    break;
                case "skyline" :
                    break;
                case "GROUPBY" :
                    break;
                case "JOIN" :
                    break;
                case "TOPKJOIN":
                    break;
                case "exit_db":
                    continueWhile = false;
                    break;
                default :
                    System.out.println("Invalid syntax. Try again \n");
                    break;
            }

        }

        return true;
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

class TableMetadata implements Serializable {
    public String tableName;
    public AttrType[] attrType;
    public short[] attrSize;
    ArrayList<String> indexNameList = new ArrayList<>();

    public short[] getAttrSize() {
        return attrSize;
    }

    public void setAttrSize(short[] attrSize) {
        this.attrSize = attrSize;
    }

    public TableMetadata(String tableName, AttrType[] attrType, short[] attrSize) {
        this.tableName = tableName;
        this.attrType = attrType;
        this.attrSize = attrSize;
    }

    public ArrayList<String> getIndexNameList() {
        return indexNameList;
    }

    public void setIndexNameList(ArrayList<String> indexNameList) {
        this.indexNameList = indexNameList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public AttrType[] getAttrType() {
        return attrType;
    }

    public void setAttrType(AttrType[] attrType) {
        this.attrType = attrType;
    }
}
