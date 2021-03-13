package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
/**
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */


public class BlockNestedLoopsSky  extends Iterator implements GlobalConst
{
    private AttrType      _in1[];
    private   int        in1_len;
    private   short t1_str_sizescopy[];
    private int [] pref_list;
    private int pref_list_length;
    private int n_pages;

    private   Iterator  file_iterator;
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean    done;        // Is the join complete
    private   Tuple     outer_tuple, inner_tuple;
    private   Heapfile  hf, temp, temp1;
    private   Scan      tempScan;

    private ArrayList<Tuple> skyline_mainm;
    private ArrayList<Integer> mainm_insert_time, tempfile_insert_time;
    private int time; // keeps track of insert times
    private int max_tuples_mainm;
    private int temp_file_number;
    boolean diskdone; //If we have read all records from disk and heapfile but there are records in mainm
    boolean get_next_returned;
    RID rid_prev;

    /**constructor
     *@param in1  Array containing field types of the tuples to be checked for dominates
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields
     *@param amt_of_mem  IN PAGES
     *@param am1  access method for left i/p to join
     *@param relationName  access hfapfile for right i/p to join


     * @throws Exception
     * @throws UnknownKeyTypeException
     * @throws UnknowAttrType
     * @throws LowMemException
     * @throws SortException
     * @throws PredEvalException
     * @throws TupleUtilsException
     * @throws PageNotReadException
     * @throws InvalidTypeException
     * @throws InvalidTupleSizeException
     * @throws IndexException
     * @throws JoinsException
     */
    public BlockNestedLoopsSky( AttrType    in1[],
                   int     len_in1,
                   short   t1_str_sizes[],
                   Iterator     am1,
                   String relationName,
                   int   p_list[],
                   int p_list_len,
                   int amt_of_mem
    ) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {

        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;


        file_iterator = am1;
        t1_str_sizescopy =  t1_str_sizes;
        inner_tuple = new Tuple();

        n_buf_pgs    = amt_of_mem;
        tempScan = null;
        done  = false;
        diskdone = false;

        pref_list = new int[p_list.length];
        System.arraycopy(p_list,0,pref_list,0,p_list.length);
        pref_list_length = p_list_len;

        // ADDED
        skyline_mainm = new ArrayList<Tuple>();
        mainm_insert_time = new ArrayList<Integer>();
        tempfile_insert_time = new ArrayList<Integer>();


        try {
            //hf = new Heapfile(relationName);
            temp = new Heapfile(null);
            temp1 = new Heapfile(null);
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }

        Tuple t = new Tuple(); // need Tuple.java
        try {
            t.setHdr((short)in1_len, in1, t1_str_sizescopy);
        }
        catch (Exception e) {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
        }

        int tuple_size = t.size();
        int tup_per_page = MINIBASE_PAGESIZE / tuple_size;
        max_tuples_mainm = tup_per_page * (n_buf_pgs-1);
        time=0;


        rid_prev = null;
        get_next_returned = false;

        // Now we read all the records from the records file and fill them in main memory and tempfile
        boolean outer_tuple_insert,dominated;

        outer_tuple = new Tuple();
        outer_tuple.setHdr((short) in1_len,_in1,t1_str_sizes);
        while((outer_tuple=file_iterator.get_next()) != null)
        {

            float mine = outer_tuple.getFloFld(1);
            float mine1 = (float) .9998579;
            if( mine == mine1 ){
                boolean foundit = true;
                //for(int k = 0; k < skyline_mainm.size(); k++){
                //  System.out.print("Skyline: " + skyline_mainm.get(k));
                //}
            }

            outer_tuple_insert = false;
            dominated = false;

/*            System.out.println("The read tuple is");
            outer_tuple.print(_in1);*/

            for(int i=0; i<skyline_mainm.size(); i++)
            {
/*                System.out.println("The skyline_main_mem tuple is");
                skyline_mainm.get(i).print(_in1);*/

                if(TupleUtils.dominates(outer_tuple, _in1, skyline_mainm.get(i),
                        _in1, (short)in1_len, t1_str_sizescopy, pref_list, pref_list_length))
                {
                    if( mine == mine1 ){
                        boolean foundit = true;
                        for(int k = 0; k < skyline_mainm.size(); k++){
                            skyline_mainm.get(k).print(_in1);
                        }
                    }
                    //Tuple read from temp_heap_file is inserted to main mem,
                    //those that are dominated are removed from main mem
                    skyline_mainm.remove(i);
                    mainm_insert_time.remove(i);
                    i--;
/*                    if(!outer_tuple_insert)
                    {
                        time++;
                        t = new Tuple(outer_tuple);
                        skyline_mainm.add(t);
                        mainm_insert_time.add(time);
                        outer_tuple_insert = true;
                    }*/

                }

                else if(TupleUtils.dominates(skyline_mainm.get(i), _in1, outer_tuple,
                        _in1, (short)in1_len, t1_str_sizescopy, pref_list, pref_list_length))
                {
                    dominated = true;
                    //break;
                }
            }

            if(dominated==false /*&& outer_tuple_insert==false*/) {
                //check whether there is space in main_memory, else insert into temp

                if(skyline_mainm.size() < max_tuples_mainm)
                {

                    t = new Tuple(outer_tuple);
                    skyline_mainm.add(t);
                    time++;
                    mainm_insert_time.add(time);
                    //mainm_comp_disk.add(0);
                }

                else {
                    try {
                        outer_tuple.print(_in1);
                        /* RID rid = */
                        System.out.println(outer_tuple.getLength());
                        temp.insertRecord(outer_tuple.returnTupleByteArray());
                        time++;
                        tempfile_insert_time.add(time);
                    }

                    catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        e.printStackTrace();
                    }
                }
            }
        }

        tempScan = new Scan(temp);

    }

    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions

     */

    public Tuple get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {


        if (done)
            return null;

        if(diskdone)
        {
            if(skyline_mainm.size()>0)
            {
                Tuple t = skyline_mainm.get(0);
                skyline_mainm.remove(0);
                mainm_insert_time.remove(0);
                return t;
            }

            done = true;
        }

        RID rid;

        do
        {

            boolean outer_tuple_insert = false;
            boolean dominated = false;

            if(get_next_returned)
            {
                rid = rid_prev;
                get_next_returned = false;
            }
            else {
                rid = new RID();
                rid_prev = rid;
            }
            while((outer_tuple=tempScan.getNext(rid)) != null)
            {
                outer_tuple.setHdr((short)in1_len, _in1, t1_str_sizescopy);

                for(int i=0; i<skyline_mainm.size(); i++)
                {
 /*                   if(tempfile_insert_time.size() == 0){
                        try {
                            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                            outer_tuple.print(_in1);
                        }
                        catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                        outer_tuple = tempScan.getNext(rid);
                        try {
                            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                            outer_tuple.print(_in1);
                        }
                        catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }

                    }*/

                    // Check whether the main memory cand timestamp is lesser than that from disk
                    if(mainm_insert_time.get(i)<tempfile_insert_time.get(0))
                    {
                        Tuple t = skyline_mainm.get(i);
                        skyline_mainm.remove(i);
                        mainm_insert_time.remove(i);
                        tempScan.position(rid_prev);
                        get_next_returned = true;
                        //tempScan.closescan();
                        //tempScan = new Scan(temp);
                        return t;
                    }

                    if(TupleUtils.dominates(outer_tuple, _in1, skyline_mainm.get(i),
                            _in1, (short)in1_len, t1_str_sizescopy, pref_list, pref_list_length))
                    {
                        //Tuple read from temp_heap_file is inserted to main mem,
                        //those that are dominated are removed from main mem
                        skyline_mainm.remove(i);
                        mainm_insert_time.remove(i);
/*
                        if(!outer_tuple_insert)
                        {
                            time++;
                            Tuple t;
                            t = new Tuple(outer_tuple);
                            skyline_mainm.add(t);
                            //skyline_mainm.add(outer_tuple);
                            mainm_insert_time.add(time);
                            tempfile_insert_time.remove(0);
                            outer_tuple_insert = true;
                        }

 */

                    }

                    else if(TupleUtils.dominates(skyline_mainm.get(i), _in1, outer_tuple,
                            _in1, (short)in1_len, t1_str_sizescopy, pref_list, pref_list_length))
                    {
                        dominated = true;
                        //break;
                    }
                }

                if(dominated==false /*&& outer_tuple_insert==false*/) {
                    //check whether there is space in main_memory, else insert into temp

                    if(skyline_mainm.size() < max_tuples_mainm)
                    {
                        //skyline_mainm.add(outer_tuple);
                        Tuple t;
                        t = new Tuple(outer_tuple);
                        skyline_mainm.add(t);
                        time++;
                        mainm_insert_time.add(time);

                        //Just Trying out!
                        //tempfile_insert_time.remove(0);

                        //mainm_comp_disk.add(0);
                    }

                    else {
                        try {
                            temp1.insertRecord(outer_tuple.returnTupleByteArray());
                            time++;
                            tempfile_insert_time.add(time);
                        }

                        catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }

                try {
                //  temp.deleteRecord(rid);
                    tempfile_insert_time.remove(0);
                }
                catch (Exception e) {
                    System.err.println ("*** Error deleting record \n");
                    e.printStackTrace();
                }

                rid_prev = rid;
                rid = new RID();
            }

            //The temp file has been scanned completely
            //Now let's check whether the spill file contains some records
            // If yes, close the current temp file, point the tempfile to spill file,
            // point spill file to new file and rerun the loop
            // If not we are done!

            tempScan.closescan();
            tempScan = new Scan(temp1);
            rid = new RID();
            Tuple t;
            if((t = tempScan.getNext(rid))==null)
            {
                diskdone = true; //Skyline only in mainmemory
                tempScan.closescan();
                if(skyline_mainm.size()>0)
                {
                    t = skyline_mainm.get(0);
                    skyline_mainm.remove(0);
                    mainm_insert_time.remove(0);
                    return t;
                }

                else
                {
                    done = true;
                    return null;
                }
            }

            else
            {
                temp = temp1;
                temp1 = new Heapfile(null);
            }


        }while(true);

    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag) {

            try {
                file_iterator.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}


