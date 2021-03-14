package iterator;


import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;

public class SortFirstSky  extends Iterator implements GlobalConst {
    private Iterator    outer;
    private   short     t1_str_sizescopy[]; //UNCOMMENTED BLAKE
    private int         n_buf_pgs;          // # of buffer pages available.
    private boolean     done,               // Is the join complete
                        get_from_outer;     // if TRUE, a tuple is got from outer
    private Tuple       outer_tuple, inner_tuple;
    private Heapfile    hf;
    private Scan        inner;

    private AttrType    _in1[];
    private int         in1_len;
    private short       t2_str_sizescopy[];
    private int[]       pref_list;
    private int         pref_list_length;
    private int         n_pages;
    private int         first_time;
    private SortPref    sortPref;
    private   Heapfile  temp, temp1;
    private   Scan      tempScan;

    private ArrayList<Tuple>    skyline_mainm;
    private ArrayList<Integer>  mainm_insert_time, tempfile_insert_time;
    private int                 max_tuples_mainm;
    private int                 temp_file_number;
    boolean                     diskdone;
    private Iterator            file_iterator;
    private RID                 rid;
    boolean                     file_read;

    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of the tuples to be checked for dominates
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param amt_of_mem  IN PAGES
     *@param am1  access method for left i/p to join
     *@param relationName  access hfapfile for right i/p to join
     *@param p_list array that holds indexes of elements to be checked for skyline candidacy
     *@param p_list_len length of the array p_list
     *@param amt_of_mem the number of pages that are available
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public SortFirstSky(AttrType in1[],
                        int len_in1,
                        short   t1_str_sizes[],
                        Iterator am1,  // This will be the filescan iterator that returns records one by one.
                        String relationName,
                        int p_list[], //Preference List
                        int p_list_len, //Preference List Length
                        int amt_of_mem
    ) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        // Initialize Global variables from the parameters passed
        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;

        t1_str_sizescopy = t1_str_sizes;
        n_pages = amt_of_mem;
        outer = am1;
        inner_tuple = new Tuple();

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        pref_list = new int[p_list.length];
        System.arraycopy(p_list,0,pref_list,0,p_list.length);
        pref_list_length = p_list_len;

        first_time = 0;


        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
        sortPref = null;
        try {
            sortPref = new SortPref(_in1, (short) in1_len, t1_str_sizescopy, outer,
                    order[1], pref_list, pref_list.length, n_pages);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //ADDED FROM BNLSky

        skyline_mainm = new ArrayList<Tuple>();

        try {
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
        tempScan = new Scan(temp);
// END ADD

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
        // This is a DUMBEST form of a join, not making use of any key information...

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
            return null;
        }

        //ADDED BLAKE

        // Now we read all the records from the records file and fill them in main memory and tempfile
        boolean outer_tuple_insert,dominated;

            while (true) {

                if (file_read) {
                    rid = new RID();
                    outer_tuple = tempScan.getNext(rid);
                }

                else
                    outer_tuple = sortPref.get_next();

                if (outer_tuple == null) {
                    if (file_read == false)
                        file_read = true;

                    break;
                }

                else if(outer_tuple != null && file_read == true)
                    outer_tuple.setHdr((short)in1_len, _in1, t1_str_sizescopy);
                dominated = false;

                for (int i = 0; i < skyline_mainm.size(); i++) {
                    
                    if (TupleUtils.dominates(skyline_mainm.get(i), _in1, outer_tuple,
                            _in1, (short) in1_len, t1_str_sizescopy, pref_list, pref_list_length)) {
                        dominated = true;
                        break;
                    }
                }

                if (dominated == false) {

                    if (skyline_mainm.size() < max_tuples_mainm) {

                        Tuple t = new Tuple(outer_tuple);
                        skyline_mainm.add(t);
                        return outer_tuple;
                    } else {
                        try {
                            temp.insertRecord(outer_tuple.returnTupleByteArray());
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }
            }
            tempScan.closescan();
            temp = new Heapfile(null);
            skyline_mainm.clear();   // remove all
            RID rid = new RID();

            if ((outer_tuple = tempScan.getNext(rid)) == null) {

                    diskdone = true;
                    tempScan.closescan();
                    if (skyline_mainm.size() == 0) {
                        done = true;
                        return null;
                    }

                    Tuple t = skyline_mainm.get(0);
                    skyline_mainm.remove(0);
                    return t;

                } else {
                    outer_tuple.setHdr((short)in1_len, _in1, t1_str_sizescopy);
                    skyline_mainm.add(outer_tuple);
                    tempScan = new Scan(temp);
                    return outer_tuple;
                }
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
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
