package tests;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.TopK_HashJoin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

class Phase3TestDriver extends TestDriver
        implements GlobalConst {
        Heapfile table1 = null;
        Heapfile table2 = null;
        String table1Name = "table1";
        String table2Name = "table2";
        String table1WithFullPath = "src/data/phase3demo/r_sii2000_1_75_200.csv";
        String table2WithFullPath = "src/data/phase3demo/r_sii2000_10_10_10.csv";
        int numOfColumns =3;
        private static short REC_LEN1 = 15;
        public Phase3TestDriver() {
            super("phase3test");
        }

    @Override
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
        SystemDefs sysdef = new SystemDefs(dbpath, 10000, NUMBUF, "Clock");
        // Kill anything that might be hanging around
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

        // create table1 heap file
        try {
            Heapfile heapFile = new Heapfile(table1Name);
            heapFile.deleteFile();
            heapFile = new Heapfile(table2Name);
            heapFile.deleteFile();
        }catch(Exception e){
            System.out.println("Error in deleting heap file");
        }
        table1 = createHeapFileFromData(table1Name,table1WithFullPath);
        table2 = createHeapFileFromData(table2Name,table2WithFullPath);

//        try {
//            SystemDefs.JavabaseBM.flushAllPages();
//        }catch(Exception e){
//            System.out.println("Failed while flushing pages");
//        }

        //Run the tests. Return type different from C++
        boolean _pass = false;
        try {
            TestDriver.setTestNumber(testNumber);
            _pass = runAllTests();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");


        return true;
    }

    private Heapfile createHeapFileFromData(String tableName,String fileNameFullPath) {
        // add any code here
        AttrType[] attrType = new AttrType[numOfColumns];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = {10};
        Scanner s =null;
        Heapfile heapFileName =null;

        try {
            heapFileName = new Heapfile(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RID rid;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            File obj = new File(fileNameFullPath);
            try {
                s = new Scanner(obj);
                while (s.hasNextLine()) {
                    String dataLine = s.nextLine();
                    String[] dataArray = dataLine.split(",");
                    if (dataArray.length == 1) {
                        numOfColumns = Integer.parseInt(dataArray[0]);
//                        System.out.println("Number of columns : " + dataArray[0]);
                    } else if (dataArray.length > 2) {
                        for (int i = 0; i < dataArray.length; i++) {
//                            System.out.println(dataArray[i]);
                            try {
                                if (attrType[i].attrType == AttrType.attrInteger) {
                                    t.setIntFld(i + 1, Integer.parseInt(dataArray[i]));
                                } else if (attrType[i].attrType == AttrType.attrString) {
                                    t.setStrFld(i + 1, dataArray[i]);
                                }
                            } catch (Exception e) {
                                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                                e.printStackTrace();
                                Runtime.getRuntime().exit(1);
                            }
                        }
//                        System.out.println("Line completed");
                        try {
                            rid = heapFileName.insertRecord(t.returnTupleByteArray());
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        finally{
            s.close(); }
        return heapFileName;
    }

    protected boolean test1() {
        int joinAttrTable1 = 1;
        int joinAttrTable2 = 1;
        int mergeAttrTable1 = 2;
        int mergeAttrTable2 = 2;
        int n_pages = 5;
        int k = 3;
        AttrType[] attrType1 = new AttrType[numOfColumns];
        attrType1[0] = new AttrType(AttrType.attrString);
        attrType1[1] = new AttrType(AttrType.attrInteger);
        attrType1[2] = new AttrType(AttrType.attrInteger);

        AttrType[] attrType2 = new AttrType[numOfColumns];
        attrType2[0] = new AttrType(AttrType.attrString);
        attrType2[1] = new AttrType(AttrType.attrInteger);
        attrType2[2] = new AttrType(AttrType.attrInteger);


        short[] attrSize1 = {10};
        short[] attrSize2 = {10};

        int numOfColumns1 = numOfColumns;
        int numOfColumns2 = numOfColumns;

        //scan heap file 1 and built index file on mergeAttrTable1 column

        createIndexFile(table1, table1Name, mergeAttrTable1);
        createIndexFile(table2, table2Name, mergeAttrTable2);
//        System.out.println("Index file creation success");

        //get top k results
        try {

            TopK_HashJoin topk = new TopK_HashJoin(
                    attrType1, numOfColumns, attrSize1, new FldSpec(new RelSpec(RelSpec.outer), joinAttrTable1),
                    new FldSpec(new RelSpec(RelSpec.outer), mergeAttrTable1),
                    attrType2, numOfColumns, attrSize2, new FldSpec(new RelSpec(RelSpec.outer), joinAttrTable2),
                    new FldSpec(new RelSpec(RelSpec.outer), mergeAttrTable2),
                    table1Name,
                    table2Name,
            k,
            n_pages, "",null
    );

        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return true;
    }

    private void createIndexFile(Heapfile table,String tableName,int prefAttr) {
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
            btf = new BTreeFile("BTreeIndex" + tableName, AttrType.attrInteger, REC_LEN1, 1/*delete*/);
            KeyClass key;
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                int intKey = t.getIntFld(prefAttr);
                key = new IntegerKey(intKey);
                btf.insert(key,rid);
            }
        }catch(Exception e){
            System.err.println("*** BTree File error ***");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        } finally {
            scan.closescan();
        }
        try {
            btf.close();
        } catch(Exception e){
            System.err.println("*** BTree File closing error ***");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
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
public class Phase3Test {
    public static void main(String[] args){
        Phase3TestDriver sDriver = null;
        try {
            sDriver = new Phase3TestDriver();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sDriver.runTests();
    }
}
