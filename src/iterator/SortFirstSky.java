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
    //private AttrType      _in1[],  _in2[];
    //private   int        in1_len, in2_len;
    private Iterator outer;
    private   short t1_str_sizescopy[];  //UNCOMMENTED BLAKE
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Heapfile hf;
    private Scan inner;

    private AttrType _in1[];
    private int in1_len;
    private short t2_str_sizescopy[];
    private int[] pref_list;
    private int pref_list_length;
    private int n_pages;
    private int first_time;
    private SortPref sortPref;
    private SortPref sortPref1;


    private   Heapfile  temp, temp1;
    private   Scan      tempScan;

    private ArrayList<Tuple> skyline_mainm;
    private ArrayList<Integer> mainm_insert_time, tempfile_insert_time;
    private int time; // keeps track of insert times
    private int max_tuples_mainm;
    private int temp_file_number;
    boolean diskdone;
    private   Iterator  file_iterator;
    //private Tuple t;
    private RID rid;
    //private RID rid_prev;
    boolean file_read;

    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of the tuples to be checked for dominates
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param //in2  Array containing field types of S
     *@param //len_in2  # of columns in S
     *@param  //t2_str_sizes shows the length of the string fields.
     *@param amt_of_mem  IN PAGES
     *@param am1  access method for left i/p to join
     *@param relationName  access hfapfile for right i/p to join
     *@param //outFilter   select expressions
     *@param //rightFilter reference to filter applied on right i/p
     *@param //proj_list shows what input fields go where in the output tuple
     *@param //n_out_flds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public SortFirstSky(AttrType in1[],
                        int len_in1,
                        short   t1_str_sizes[],
                        //AttrType    in2[],
                        //int     len_in2,
                        //short t2_str_sizes[],
                        Iterator am1,  // This will be the filescan iterator that returns records one by one.
                        String relationName,
                        int p_list[], //Preference List
                        int p_list_len, //Preference List Length
                        int amt_of_mem
    ) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        _in1 = new AttrType[in1.length];
        //_in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        //System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        //in2_len = len_in2;

        // ADDED
        t1_str_sizescopy = t1_str_sizes;
        n_pages = amt_of_mem;
        //END
        outer = am1;
        //t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Tuple();
        // Jtuple = new Tuple();
        //OutputFilter = outFilter;
        //RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        pref_list = new int[p_list.length];
        System.arraycopy(p_list,0,pref_list,0,p_list.length);
        pref_list_length = p_list_len;


        first_time = 0;

//Commented by Pranav Iyer!!!!

        // AttrType[] Jtypes = new AttrType[pref_list_length];

        // short[]    t_size;

        // perm_mat = proj_list;
        //   nOutFlds = n_out_flds;


        //     try {
        // t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
        // 				   in1, len_in1,
        // 				   t2_str_sizes,
        // 				   pref_list_types, pref_list_length);
        //     }catch (TupleUtilsException e){
        // throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        //     }


        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
        sortPref = null;
        try {
            sortPref = new SortPref(_in1, (short) in1_len, t1_str_sizescopy, outer,
                    order[1], pref_list, pref_list.length, n_pages);
            // sortPref1 = new SortPref(_in1, (short) in1_len, t1_str_sizescopy, outer,
            //       order[1], pref_list, pref_list.length, n_pages);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
        }

        //ADDED FROM BNLSky

        skyline_mainm = new ArrayList<Tuple>();
        //mainm_insert_time = new ArrayList<Integer>();
        //tempfile_insert_time = new ArrayList<Integer>();

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
        //time=0;
        tempScan = new Scan(temp);
// END ADD
/*
        try {
            hf = new Heapfile(relationName);
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }

 */
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

        //do {
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

                //System.out.println("The read tuple is");
                //outer_tuple.print(_in1);

                for (int i = 0; i < skyline_mainm.size(); i++) {
                    //System.out.println("The skyline_main_mem tuple is");
                    //skyline_mainm.get(i).print(_in1);


                    if (TupleUtils.dominates(skyline_mainm.get(i), _in1, outer_tuple,
                            _in1, (short) in1_len, t1_str_sizescopy, pref_list, pref_list_length)) {
                        dominated = true;
                        break;
                    }
                }

                if (dominated == false) {           //&& outer_tuple_insert==false

                    //check whether there is space in main_memory, else insert into temp

                    if (skyline_mainm.size() < max_tuples_mainm) {

                        Tuple t = new Tuple(outer_tuple);
                        skyline_mainm.add(t);
                        return outer_tuple;
                        //time++;
                        //mainm_insert_time.add(time);
                        //mainm_comp_disk.add(0);
                    } else {
                        try {
                            //outer_tuple.print(_in1);
                            // RID rid =
                            temp.insertRecord(outer_tuple.returnTupleByteArray());
                            //return outer_tuple;

                            //time++;
                            //tempfile_insert_time.add(time);
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            e.printStackTrace();
                        }
                    }
                }
            }
            tempScan.closescan();
            //tempScan = new Scan(temp);
            temp = new Heapfile(null);
            skyline_mainm.clear();   // remove all
            RID rid = new RID();

            //for (int i = 0; i < max_tuples_mainm; i++) {
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
                    //outer_tuple = tempScan.getNext(rid);
                    skyline_mainm.add(outer_tuple);
                    tempScan = new Scan(temp);
                    return outer_tuple;
                }
            //}
      //  }
    //while (true);


        //tempScan.closescan();
        //tempScan = new Scan(temp1);
        //rid = new RID();
        //Tuple t;
/*
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
*/




        //END


/*
        do
        {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            //join is done too.


            if (get_from_outer == true)
            {

                get_from_outer = false;

                if (inner != null)     // If this not the first time,
                {
                    // close scan
                    inner.closescan();
                    inner = null;
                }

                try {
                    inner = hf.openScan();
//                    PCounter.printCounter();
//                    System.out.println("Inner Open Scan complete");
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }

                //if ((outer_tuple=outer.get_next()) == null)  // correct code

                // BLAKE Additions
                try {

                    outer_tuple = sortPref.get_next();
                    //System.out.println("OUTER_TUPLE:");
                    //outer_tuple.print(_in1);
                }
                catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }
                //outer_tuple = SortPref.get_next();
                if (outer_tuple == null)
                // END
                {
                    done = true;
                    if (inner != null)
                    {
                        inner.closescan();
                        inner = null;
                    }

                    return null;
                } else {
//                    PCounter.printCounter();
//                    System.out.println("Outer loop incremented");
                }
            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            RID rid = new RID();
            while ((inner_tuple = inner.getNext(rid)) != null) // ERROR HERE INNER TUPLE NOT BEING ASSIGNED
            // while ((inner_tuple = inner.getNext()) != null)
            {
                //            PCounter.printCounter();
//                System.out.println("Inner loop incremented");
                inner_tuple.setHdr((short)in1_len, _in1, t2_str_sizescopy);
                //System.out.println("INNER_TUPLE:");
                //inner_tuple.print(_in1);
                // HERE CHECK WHETHER THE INNER TUPLE DOMINATES THE OUTER TUPLE
                // IF YES THEN BREAK OUT OF INNER LOOP TO ALLOW OUTER TO CONTINUE TO NEXT ELEMENT, RESTART INNER
                // ELSE ALLOW OUTER TO CHECK NEXT INNER IN INNER LOOP
                // IF INNER IS DONE, THAT MEANS THE CORRESPONDING OUTER IS A SKYLINE ELEMENT

                // int pref_list [] = {2,4}; // Out of 5 elements in sample input file these are in skyline
                // int pref_list_length = pref_list.length;


                if(iterator.TupleUtils.dominates(inner_tuple, _in1, outer_tuple, _in1,
                        (short)in1_len, t2_str_sizescopy, pref_list, pref_list_length)) //IF INNER ELEMENT DOMINATES
                {
                    break;
                }
            }

            get_from_outer = true; // Increment the outer loop

            // IF our code reaches here and inner_tuple is null, that means that the outer_tuple is in skyline

            if (inner_tuple == null)
            {
                System.out.println("New Skyline element found");
                float[] output = new float[in1_len];
                float values;
                outer_tuple.print(_in1);
                return outer_tuple;
            }

        } while (true);

 */

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
