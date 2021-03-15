package tests;

import btree.*;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

class SkyLineTestDriver extends TestDriver
        implements GlobalConst {

    private static short REC_LEN1 = 32;
    String heapFileName = "skylinenestedlooptest.in";
    Heapfile f = null;

    public SkyLineTestDriver() {
        super("skylinetest");
    }

    public boolean runTests() throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {
        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
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
        int numOfColumns = 0;
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        List preflistTemp = null;
        int numOfPages;
        String fileName = null;

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
        int[] preflist = new int[preflistArray.length];
        for (int i = 0; i < preflistArray.length; i++) {
            preflist[i] = Integer.parseInt(preflistArray[i]);
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
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                //status = FAIL;
                e.printStackTrace();
            }

        }

        scanner.close();

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

        t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        size = t.size();
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
        //String heapFileName = "skylineblocknestedtest.in";
        int numOfColumns = 0;
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        List preflistTemp = null;
        int numOfPages;
        String fileName = null;

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
        int[] preflist = new int[preflistArray.length];
        for (int i = 0; i < preflistArray.length; i++) {
            preflist[i] = Integer.parseInt(preflistArray[i]);
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
//        RID rid;
//        Heapfile f = null;
//        try {
//            f = new Heapfile(heapFileName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) numOfColumns, attrType, attrSize);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        while (scanner.hasNextLine() && scanner.hasNextFloat()) {
//
//            float temp_file_read;
//            for (int i = 0; i < numOfColumns; i++) // For each line, scan each column/attribute
//            {
//                temp_file_read = scanner.nextFloat();
//                try {
//                    t.setFloFld(i + 1, (float) temp_file_read);
//                } catch (Exception e) {
//                    System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
//                    //status = FAIL;
//                    e.printStackTrace();
//                }
//            }
//
//            try {
//                rid = f.insertRecord(t.returnTupleByteArray());
//            } catch (Exception e) {
//                System.err.println("*** error in Heapfile.insertRecord() ***");
//                //status = FAIL;
//                e.printStackTrace();
//            }
//
//        }
//
//        scanner.close();

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

        t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        size = t.size();
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
       // String heapFileName = "skylinesortedskytest.in";
        int numOfColumns = 0;
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        List preflistTemp = null;
        int numOfPages;
        String fileName = null;

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
        int[] preflist = new int[preflistArray.length];
        for (int i = 0; i < preflistArray.length; i++) {
            preflist[i] = Integer.parseInt(preflistArray[i]);
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
//        RID rid;
//        Heapfile f = null;
//        try {
//            f = new Heapfile(heapFileName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) numOfColumns, attrType, attrSize);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        while (scanner.hasNextLine() && scanner.hasNextFloat()) {
//
//            float temp_file_read;
//            for (int i = 0; i < numOfColumns; i++) // For each line, scan each column/attribute
//            {
//                temp_file_read = scanner.nextFloat();
//                try {
//                    t.setFloFld(i + 1, (float) temp_file_read);
//                } catch (Exception e) {
//                    System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
//                    //status = FAIL;
//                    e.printStackTrace();
//                }
//            }
//
//            try {
//                rid = f.insertRecord(t.returnTupleByteArray());
//            } catch (Exception e) {
//                System.err.println("*** error in Heapfile.insertRecord() ***");
//                //status = FAIL;
//                e.printStackTrace();
//            }
//
//        }
//
//        scanner.close();

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

        t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, Ptypes, Psizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        size = t.size();
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
       // String heapFileName = "skylinebtreeskytest.in";
        int numOfColumns = 0;
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        int numOfPages;
        String fileName = null;

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
        int[] preflist = new int[preflistArray.length];
        for (int i = 0; i < preflistArray.length; i++) {
            preflist[i] = Integer.parseInt(preflistArray[i]);
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
//        Heapfile f = null;
//        try {
//            f = new Heapfile(heapFileName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) numOfColumns, attrType, attrSize);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        while (scanner.hasNextLine() && scanner.hasNextFloat()) {
//
//            float temp_file_read;
//            for (int i = 0; i < numOfColumns; i++) // For each line, scan each column/attribute
//            {
//                temp_file_read = scanner.nextFloat();
//                try {
//                    t.setFloFld(i + 1, (float) temp_file_read);
//                } catch (Exception e) {
//                    System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
//                    //status = FAIL;
//                    e.printStackTrace();
//                }
//            }
//
//            try {
//                rid = f.insertRecord(t.returnTupleByteArray());
//            } catch (Exception e) {
//                System.err.println("*** error in Heapfile.insertRecord() ***");
//                //status = FAIL;
//                e.printStackTrace();
//            }
//
//        }
//
//        scanner.close();

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        }
        catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        //ArrayList<BTreeFile> BTreeFileList = new ArrayList<>();
        IndexFile[] BTreeFileList = new IndexFile[preflist.length];
        try {
            //create index files for all pref attributes
            for (int i = 0; i < preflist.length; i++) {
                BTreeFile btf = new BTreeFile("BTreeIndex" + preflist[i], AttrType.attrString, REC_LEN1, 1/*delete*/);
                //BTreeFileList.add(btf);
                BTreeFileList[i] = btf;

                rid = new RID();
                String key = null;
                Tuple temp = null;

                try {
                    temp = scan.getNext(rid);
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }
                while (temp != null) {
                    t.tupleCopy(temp);

                    try {
                        key = String.valueOf(t.getFloFld(preflist[i] + 1));
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
                }
                catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
            }
        }
        catch(Exception e){
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
                am = new FileScan(heapFileName, attrType, attrSize, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
                PCounter.printCounter();
                System.out.println("File Scan completed");
            } catch (Exception e) {
                System.err.println("" + e);
            }

            //BTreeSky testing
            BTreeSky bts = new BTreeSky(attrType, numOfColumns, x, am, heapFileName,
                    preflist, preflist.length, BTreeFileList, numOfPages);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        // close the file scan
        scan.closescan();

        PCounter.printCounter();
        //return status;
        return true;

    }

    protected boolean test5() throws Exception {
        PCounter.initialize();
       // String heapFileName = "skylinebtreesortedskytest.in";
        int numOfColumns = 0;
        FileScan am = null;
        short[] x = {}; // blank since we assume there are no strings

        int numOfPages;
        String fileName = null;

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
        int[] preflist = new int[preflistArray.length];
        for (int i = 0; i < preflistArray.length; i++) {
            preflist[i] = Integer.parseInt(preflistArray[i]);
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
//        Heapfile f = null;
//        try {
//            f = new Heapfile(heapFileName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) numOfColumns, attrType, attrSize);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        while (scanner.hasNextLine() && scanner.hasNextFloat()) {
//
//            float temp_file_read;
//            for (int i = 0; i < numOfColumns; i++) // For each line, scan each column/attribute
//            {
//                temp_file_read = scanner.nextFloat();
//                try {
//                    t.setFloFld(i + 1, (float) temp_file_read);
//                } catch (Exception e) {
//                    System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
//                    //status = FAIL;
//                    e.printStackTrace();
//                }
//            }
//
//            try {
//                rid = f.insertRecord(t.returnTupleByteArray());
//            } catch (Exception e) {
//                System.err.println("*** error in Heapfile.insertRecord() ***");
//                //status = FAIL;
//                e.printStackTrace();
//            }
//
//        }
//
//        scanner.close();

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        }
        catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        BTreeFile btf = new BTreeFile("BTreeSortedIndex", AttrType.attrString, REC_LEN1, 1/*delete*/);
        try {

            rid = new RID();
            String key = null;
            Tuple temp = null;

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
                    key = String.valueOf(TupleUtils.computeTupleSumOfPrefAttrs(t,attrType,(short)numOfColumns,attrSize,preflist,preflist.length));
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
            }
            catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
        catch(Exception e){
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
                am = new FileScan(heapFileName, attrType, attrSize, (short) numOfColumns, (short) numOfColumns, Pprojection, null);
                PCounter.printCounter();
                System.out.println("File Scan completed");
            } catch (Exception e) {
                System.err.println("" + e);
            }

            //BTreeSky testing
            BTreeSortedSky bts = new BTreeSortedSky(attrType, numOfColumns, x, am, heapFileName,preflist, preflist.length, btf, numOfPages);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        // close the file scan
        scan.closescan();

        PCounter.printCounter();
        //return status;
        return true;

    }
}

public class SkyLineTest {
    public static void main(String[] args) throws IOException, GetFileEntryException, ConstructPageException, AddFileEntryException {
        SkyLineTestDriver sDriver = new SkyLineTestDriver();
        sDriver.runTests();

    }
}
