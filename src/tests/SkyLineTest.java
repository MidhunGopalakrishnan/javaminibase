package tests;

import btree.*;
import diskmgr.PCounter;
import global.*;
import heap.*;
import iterator.*;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

class SkyLineTestDriver extends TestDriver
        implements GlobalConst {

    private static short REC_LEN1 = 50;
    String heapFileName = "projecttestdata.in";
    Heapfile f = null;
    int numOfColumns = 0;
    int numOfPages;
    String fileName = null;
    ArrayList<Integer> prefListTemp = new ArrayList<>();

    public SkyLineTestDriver() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        super("skylinetest");
    }

    public boolean runTests() throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {


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

        // Enter data using Scanner
        Scanner sc = new Scanner(System.in);

        // Reading data using readLine
        System.out.println("Enter filename :  ");
        fileName = sc.nextLine();
        System.out.println("Enter the pref list (comma separated) : ");
        String preflist1 = sc.nextLine();
        System.out.println("Enter number of pages : ");
        numOfPages = sc.nextInt();

        String[] preflistArray = preflist1.split(",");

        for (int i = 0; i < preflistArray.length; i++) {
            prefListTemp.add(Integer.parseInt(preflistArray[i]));
        }

        File myObj = new File(fileName);
        Scanner scanner = null;
        try {
            scanner = new Scanner(myObj);
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        numOfColumns = scanner.nextInt();

        AttrType[] attrType = new AttrType[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            attrType[i] = new AttrType(AttrType.attrReal);
        }
        short[] attrSize = {};

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = t.size();
        System.out.println("Tuple size : " + size);

        // Create unsorted data file "test4.in"
        RID rid;
        try {
            f = new Heapfile(heapFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) numOfColumns, attrType, attrSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int count = 0;
        while (scanner.hasNextLine() && scanner.hasNextFloat()) {

            float temp_file_read;
            for (int i = 0; i < numOfColumns; i++) // For each line, scan each column/attribute
            {
                temp_file_read = scanner.nextFloat();
                try {
                    t.setFloFld(i + 1, (float) temp_file_read);
                } catch (Exception e) {
                    System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                    //status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
                count++;
                //System.out.println("Record inserrted in heap : "+count );
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

        }

        scanner.close();
        sc.close();

        //Run the tests. Return type different from C++
        boolean _pass = false;
        try {
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

    protected boolean test1() throws Exception {
        PCounter.initialize();

        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Ptypes[i] = new AttrType(AttrType.attrReal);
        }

        short[] Psizes = {};

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = t.size();
        System.out.println("Tuple size : " + size);

        FldSpec[] Pprojection = new FldSpec[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        am = null;
        try {
            am = new FileScan(heapFileName, Ptypes, Psizes, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
            PCounter.printCounter();
            System.out.println("File Scan completed");
        } catch (Exception e) {
            System.err.println("" + e);
        }
        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        NestedLoopsSky s = new NestedLoopsSky(Ptypes, numOfColumns, x, am,
                heapFileName, preflist, preflist.length, numOfPages);
        System.out.println("Printing NestedLoop Sky elements for " + fileName);

        while ((t = s.get_next()) != null) {
            t.print(Ptypes); //print skyline elements
        }
        s.close();
        PCounter.printCounter();
        return true;
    }

    protected boolean test2() throws Exception {
        PCounter.initialize();
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Ptypes[i] = new AttrType(AttrType.attrReal);
        }

        short[] Psizes = {};

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = t.size();
        System.out.println("Tuple size : " + size);

        FldSpec[] Pprojection = new FldSpec[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        am = null;
        try {
            am = new FileScan(heapFileName, Ptypes, Psizes, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
            PCounter.printCounter();
            System.out.println("File Scan completed");
        } catch (Exception e) {
            System.err.println("" + e);
        }

        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        BlockNestedLoopsSky s = new BlockNestedLoopsSky(Ptypes, numOfColumns, x, am,
                heapFileName, preflist, preflist.length, numOfPages);
        System.out.println("Printing NestedLoop Sky elements for " + fileName);

        while ((t = s.get_next()) != null) {
            t.print(Ptypes); //print skyline elements
        }

        s.close();
        PCounter.printCounter();
        return true;

    }

    protected boolean test3() throws Exception {
        PCounter.initialize();

        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings


        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Ptypes[i] = new AttrType(AttrType.attrReal);
        }

        short[] Psizes = {};

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = t.size();
        System.out.println("Tuple size : " + size);

        FldSpec[] Pprojection = new FldSpec[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
            Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        am = null;
        try {
            am = new FileScan(heapFileName, Ptypes, Psizes, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
            PCounter.printCounter();
            System.out.println("File Scan completed");
        } catch (Exception e) {
            System.err.println("" + e);
        }

        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        SortFirstSky s = new SortFirstSky(Ptypes, numOfColumns, x, am,
                heapFileName, preflist, preflist.length, numOfPages);
        System.out.println("Printing NestedLoop Sky elements for " + fileName);

        while ((t = s.get_next()) != null) {
            t.print(Ptypes); //print skyline elements
        }

        s.close();
        PCounter.printCounter();
        return true;

    }

    protected boolean test4() throws Exception {
        PCounter.initialize();

        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings


        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        //ArrayList<BTreeFile> BTreeFileList = new ArrayList<>();
        BTreeFile[] BTreeFileList = new BTreeFile[preflist.length];
        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int k = 0; k < numOfColumns; k++) {
            Ptypes[k] = new AttrType(AttrType.attrReal);
        }

        short[] Psizes = {};
        //new code : only scan data file once
        try {
            //create index files for all pref attributes

            Tuple temp = null;
            Tuple t = new Tuple();
            //PageId pageno=new PageId();
            try {
                t.setHdr((short) numOfColumns, Ptypes, Psizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
            RID rid = new RID();
            PageId pageno = new PageId();
            pageno.pid = 0;
            SystemDefs.JavabaseDB.allocate_page(pageno, preflist.length);
            for (int i = 0; i < preflist.length; i++) {
                if (SystemDefs.JavabaseDB.get_file_entry("BTreeIndex" + preflist[i]) != null) {
                    SystemDefs.JavabaseDB.delete_file_entry("BTreeIndex" + preflist[i]);
                }
                BTreeFile btf = new BTreeFile("BTreeIndex" + preflist[i], AttrType.attrInteger, REC_LEN1, 1/*delete*/);
                //BTreeFileList.add(btf);
                if (SystemDefs.JavabaseDB.get_file_entry("BTreeIndex" + preflist[i]) == null) {
                    SystemDefs.JavabaseDB.add_file_entry("BTreeIndex" + preflist[i], pageno);
                }
                BTreeFileList[i] = btf;
            }
            KeyClass key;
            int count = 0;
            rid = new RID();
            RID keyRID = null;
            while ((temp = scan.getNext(rid)) != null) {
                count++;
                System.out.println();
                System.out.print("Processing record number : " + count + " Values : ");
                t.tupleCopy(temp);
                // loop and add to all index files
                try {
                    for (int k = 0; k < preflist.length; k++) {
                        t.print(Ptypes);
                        float floatkey = (float) ((t.getFloFld(preflist[k] + 1)) * Math.pow(10, 7));
                        int intKey = (int) floatkey;
                        System.out.print(intKey + " ");
                        key = new IntegerKey(intKey);
                        pageno.pid = intKey % 500;
                        keyRID = new RID(pageno, intKey % 500);
                        BTreeFileList[k].insert(key, keyRID);
                    }
                    // rid = new RID();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            scan.closescan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        } finally {
            //close all index files created
            for (int k = 0; k < BTreeFileList.length; k++) {
                BTreeFileList[k].close();
            }
        }
        PCounter.printCounter();

        try {
            FldSpec[] Pprojection = new FldSpec[numOfColumns];
            for (int i = 0; i < numOfColumns; i++) {
                Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

//            try {
//                am = new FileScan(heapFileName, Ptypes, Psizes, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
//                PCounter.printCounter();
//                System.out.println("File Scan completed");
//            } catch (Exception e) {
//                System.err.println("" + e);
//            }

            //BTreeSky testing
            BTreeSky bts = new BTreeSky(Ptypes, numOfColumns, x, am, heapFileName,
                    preflist, preflist.length, BTreeFileList, numOfPages, f);
            bts.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // close the file scan
        scan.closescan();
        //am.close();

        PCounter.printCounter();
        //return status;
        // close all index files
        for (int i = 0; i < BTreeFileList.length; i++) {
            BTreeFileList[i].close();
            BTreeFileList[i].destroyFile();
        }

        return true;

    }

    protected boolean test5() throws Exception {
        PCounter.initialize();
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int k = 0; k < numOfColumns; k++) {
            Ptypes[k] = new AttrType(AttrType.attrReal);
        }

        short[] Psizes = {};

        BTreeFile btf = new BTreeFile("BTreeSortedIndex", AttrType.attrString, REC_LEN1, 1/*delete*/);
        try {

            RID rid = new RID();
            String key = null;
            Tuple temp = null;

            Tuple t = new Tuple();
            try {
                t.setHdr((short) numOfColumns, Ptypes, Psizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }


            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
            }
            while (temp != null) {
                t.tupleCopy(temp);
                //t.print(attrType);

                try {
                    key = String.valueOf(TupleUtils.computeTupleSumOfPrefAttrs(t, Ptypes, (short) numOfColumns, Psizes, preflist, preflist.length));
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }

                try {
                    btf.insert(new StringKey(key), rid);
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }

                try {
                    temp = scan.getNext(rid);
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }
            }
            //reset scan so that we can create index on next preference attribute
            try {
                scan = new Scan(f);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        try {
            FldSpec[] Pprojection = new FldSpec[numOfColumns];
            for (int i = 0; i < numOfColumns; i++) {
                Pprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            try {
                am = new FileScan(heapFileName, Ptypes, Psizes, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
                PCounter.printCounter();
                System.out.println("File Scan completed");
            } catch (Exception e) {
                System.err.println("" + e);
            }

            //BTreeSky testing
            BTreeSortedSky bts = new BTreeSortedSky(Ptypes, numOfColumns, x, am, heapFileName, preflist, preflist.length, btf, numOfPages);
            bts.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // close the file scan
        scan.closescan();

        PCounter.printCounter();
        //return status;
        btf.close();
        btf.destroyFile();
        return true;

    }

    //    protected boolean test6() throws Exception {
//
//        Scan scan = null;
//        try {
//            scan = new Scan(f);
//        } catch (Exception e) {
//            //status = FAIL;
//            e.printStackTrace();
//            Runtime.getRuntime().exit(1);
//        }
//
//        AttrType[] Ptypes = new AttrType[numOfColumns];
//        for (int k = 0; k < numOfColumns; k++) {
//            Ptypes[k] = new AttrType(AttrType.attrReal);
//        }
//
//        int[] preflist = new int[prefListTemp.size()];
//
//        for (int i = 0; i < prefListTemp.size(); i++) {
//            preflist[i] = prefListTemp.get(i);
//        }
//
//        BTreeFile[] BTreeFileList = new BTreeFile[preflist.length];
//
//        for (int i = 0; i < preflist.length; i++) {
//            BTreeFile btf = new BTreeFile("BTreeIndex" + preflist[i], AttrType.attrInteger, REC_LEN1, 1/*delete*/);
//            BTreeFileList[i] = btf;
//        }
//
//        short[] Psizes = {};
//        KeyClass key;
//        try {
//
//            Tuple temp = null;
//            Tuple t = new Tuple();
//            RID rid = new RID();
//            try {
//                t.setHdr((short) numOfColumns, Ptypes, Psizes);
//            } catch (Exception e) {
//                System.err.println("*** error in Tuple.setHdr() ***");
//                e.printStackTrace();
//            }
//            while ((temp= scan.getNext(rid))!= null) {
//                t.tupleCopy(temp);
//                System.out.print("RID : "+ rid + " PageNo: "+ rid.pageNo + " SlotNo: "+ rid.slotNo+ " ");
//                t.print(Ptypes);
//                try {
//                    for (int k = 0; k < preflist.length; k++) {
//                         float value = t.getFloFld(preflist[k] + 1);
//                         double roundedValue = value * Math.pow(10,7);
//                         int intKey = (int) roundedValue;
//                         key = new IntegerKey(intKey);
//                         BTreeFileList[k].insert(key, rid);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            scan.closescan();
//        }
//        return true;
//    }
//}
    protected boolean test6() throws Exception {
        Scan scan = null;

        AttrType[] Ptypes = new AttrType[numOfColumns];
        for (int k = 0; k < numOfColumns; k++) {
            Ptypes[k] = new AttrType(AttrType.attrReal);
        }

        int[] preflist = new int[prefListTemp.size()];

        for (int i = 0; i < prefListTemp.size(); i++) {
            preflist[i] = prefListTemp.get(i);
        }

        short[] Psizes = {};
        KeyClass key;
        BTreeFile[] BTreeFileList = new BTreeFile[preflist.length];

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        BTreeFile btf = null;
        for (int i = 0; i < preflist.length; i++) {
            RID rid = new RID();
            try {
                scan = new Scan(f);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

            btf = new BTreeFile("SampleFiles" + preflist[i], AttrType.attrInteger, REC_LEN1, 1/*delete*/);
            Tuple temp = null;
            Tuple t = new Tuple();

            try {
                t.setHdr((short) numOfColumns, Ptypes, Psizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                System.out.print("RID : " + rid + " PageNo: " + rid.pageNo + " SlotNo: " + rid.slotNo + " ");
                t.print(Ptypes);
                try {
                    float value = t.getFloFld(preflist[i] + 1);
                    double roundedValue = value * Math.pow(10, 7);
                    int intKey = (int) roundedValue;
                    key = new IntegerKey(intKey);
                    btf.insert(key, rid);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            BTreeFileList[i] = btf;
            btf.close();
            scan.closescan();
        }
        //btf.close();
        //scan.closescan();
        return true;
    }
}
public class SkyLineTest {
    public static void main(String[] args) throws IOException, GetFileEntryException, ConstructPageException, AddFileEntryException {
        SkyLineTestDriver sDriver = null;
        try {
            sDriver = new SkyLineTestDriver();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sDriver.runTests();

    }
}
