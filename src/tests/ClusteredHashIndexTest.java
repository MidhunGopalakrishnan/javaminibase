package tests;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
//import btree.*;
import hash.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;


class IndexDriver2 extends TestDriver
        implements GlobalConst {

    private static short REC_LEN1 = 50;


    public IndexDriver2() {
        super("indextest");
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

    public boolean runTests () throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs( dbpath, 1500, NUMBUF, "Clock" );

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
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
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
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }

    protected boolean test1()
    {
        System.out.println("------------------------ TEST 1 --------------------------");
        File myObj = new File("src/data/phase3demo/r_sii2000_1_75_200.csv");

        Heapfile        f = null;
        RID rid = new RID();
        try
        {
            f = new Heapfile("test1.in");
        }catch (Exception e) {e.printStackTrace();}

        int numOfColumns =3;
        int [] pref_list = {2,3,4};
        int numOfPages = 100000, insert_count=0;
        float utilization = 0.75f;
        ClusteredHashFile c_hash_file=null;
        try{
            c_hash_file = new ClusteredHashFile("HashIndex"+ pref_list[2], AttrType.attrString, REC_LEN1, 1, utilization, f);
        }
        catch(Exception e){e.printStackTrace();}

        AttrType[] attrType = new AttrType[numOfColumns];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = {10};
        Scanner s =null;
        Heapfile heapFileName =null;

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numOfColumns, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            File obj = new File("src/data/phase3demo/r_sii2000_1_75_200.csv");
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
                            //System.out.println("\n The insert count is"+insert_count++);
                            String key = String.valueOf(t.getStrFld(1));
                            // System.out.println("Printing tuple "+insert_count++);
                            // t.print(attrType);
                            rid = c_hash_file.insertIntoDataFile(new StringKey(key), t.getTupleByteArray());
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

        c_hash_file.print_all();

        return true;
    }

    protected String testName()
    {
        return "Index";
    }
}

public class ClusteredHashIndexTest
{
    public static void main(String argv[]) throws ConstructPageException, GetFileEntryException, AddFileEntryException, IOException
    {
        boolean indexstatus;

        IndexDriver2 indext = new IndexDriver2();

        indexstatus = indext.runTests();
        if (indexstatus != true) {
            System.out.println("Error ocurred during index tests");
        }
        else {
            System.out.println("Index tests completed successfully");
        }
    }
}

