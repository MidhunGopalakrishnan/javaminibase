package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
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


public class NestedLoopsSky  extends Iterator
{
    //private AttrType      _in1[],  _in2[];
    //private   int        in1_len, in2_len;
    private   Iterator  outer;
    //private   short t2_str_sizescopy[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean    done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private   Tuple     outer_tuple, inner_tuple;
    private   Heapfile  hf;
    private   Scan      inner;

    private AttrType      _in1[];
    private   int        in1_len;
    private   short t2_str_sizescopy[];
    private int [] pref_list;
    private int pref_list_length;
    private int n_pages;

    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of the tuples to be checked for dominates
     *@param len_in1  # of columns in R.
     *@param //t1_str_sizes shows the length of the string fields.
     *@param //in2  Array containing field types of S
     *@param //len_in2  # of columns in S
     *@param  t2_str_sizes shows the length of the string fields.
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
    public NestedLoopsSky( AttrType    in1[],
                                    int     len_in1,
                                    //short   t1_str_sizes[],
                                    //AttrType    in2[],
                                    //int     len_in2,
                                    short   t2_str_sizes[],
                                    Iterator     am1,  // This will be the filescan iterator that returns records one by one.
                                    String relationName,
                                    int   p_list[], //Preference List
                                    int p_list_len, //Preference List Length
                                    int amt_of_mem
    ) throws IOException,NestedLoopException
    {

        _in1 = new AttrType[in1.length];
        //_in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        //System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        //in2_len = len_in2;


        outer = am1;
        t2_str_sizescopy =  t2_str_sizes;
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


        try {
            hf = new Heapfile(relationName);
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }
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

                if ((outer_tuple=outer.get_next()) == null)
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

                // HERE CHECK WHETHER THE INNER TUPLE DOMINATES THE OUTER TUPLE
                // IF YES THEN BREAK OUT OF INNER LOOP TO ALLOW OUTER TO CONTINUE TO NEXT ELEMENT, RESTART INNER
                // ELSE ALLOW OUTER TO CHECK NEXT INNER IN INNER LOOP
                // IF INNER IS DONE, THAT MEANS THE CORRESPONDING OUTER IS A SKYLINE ELEMENT

                // int pref_list [] = {2,4}; // Out of 5 elements in sample input file these are in skyline
                // int pref_list_length = pref_list.length;


                if(heap.Tuple.dominates(inner_tuple, _in1, outer_tuple, _in1,
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

