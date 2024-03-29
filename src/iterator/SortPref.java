package iterator;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import index.*;
import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class SortPref extends Iterator implements GlobalConst{
    private static final int ARBIT_RUNS = 10;

    public static AttrType[] _in;
    public short n_cols;
    public short[] str_lens;
    public Iterator _am;
    private int _sort_fld;

    public static int[] _pref_list;
    public static int _pref_list_len;

    private TupleOrder order;
    private int _n_pages;
    private byte[][] bufs;
    private boolean first_time;

    private int Nruns;

    private int _count;

    private int max_elems_in_heap;
    private int sortFldLen;
    private int tuple_size;

    private pnodeSplayPQ Q;

    private Heapfile[] temp_files;
    private int n_tempfiles;
    private Tuple output_tuple;
    private int[] n_tuples;
    private int n_runs;
    private Tuple op_buf;
    private OBuf o_buf;
    private SpoofIbuf[] i_buf;
    private PageId[] bufs_pids;
    private boolean useBM = true; // flag for whether to use buffer manager

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     *
     * @param tuple_size size (in bytes) of each tuple
     * @param n_R_runs   number of runs
     * @throws IOException     from lower layers
     * @throws LowMemException there is not enough memory to
     *                         sort in two passes (a subclass of SortException).
     * @throws SortException   something went wrong in the lower layer.
     * @throws Exception       other exceptions
     */
    private void setup_for_merge(int tuple_size, int n_R_runs)
            throws IOException,
            LowMemException,
            SortException,
            Exception {
//        System.out.println("SETUP FOR MERGE!!!");
        // don't know what will happen if n_R_runs > _n_pages
//        System.out.println("Number of runs: " + n_R_runs);
//        System.out.println("NPAGES: " + _n_pages);
        if (n_R_runs > _n_pages)
            throw new LowMemException("SortPref.java: Not enough memory to sort in two passes.");

        int i;
        pnode cur_node;  // need pq_defs.java

        i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
        for (int j = 0; j < n_R_runs; j++) i_buf[j] = new SpoofIbuf();

        // construct the lists, ignore TEST for now
        // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

        for (i = 0; i < n_R_runs; i++) {
            byte[][] apage = new byte[1][];
            apage[0] = bufs[i];

            // need iobufs.java
            i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

            cur_node = new pnode();
            cur_node.run_num = i;

            // may need change depending on whether Get() returns the original
            // or make a copy of the tuple, need io_bufs.java ???
            Tuple temp_tuple = new Tuple(tuple_size);

            try {
                temp_tuple.setHdr(n_cols, _in, str_lens);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
            }

            temp_tuple = i_buf[i].Get(temp_tuple);  // need io_bufs.java

            if (temp_tuple != null) {
                cur_node.tuple = temp_tuple; // no copy needed
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }

            }
        }
        return;
    }

    /**
     * Remove the minimum value among all the runs.
     *
     * @return the minimum tuple removed
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    private Tuple delete_min()
            throws IOException,
            SortException,
            Exception {
        pnode cur_node;                // needs pq_defs.java
        Tuple new_tuple, old_tuple;

        cur_node = Q.deq();
        old_tuple = cur_node.tuple;

        // we just removed one tuple from one run, now we need to put another
        // tuple of the same run into the queue
        if (i_buf[cur_node.run_num].empty() != true) {
            // run not exhausted
            new_tuple = new Tuple(tuple_size); // need tuple.java??

            try {
                new_tuple.setHdr(n_cols, _in, str_lens);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: setHdr() failed");
            }

            new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
            if (new_tuple != null) {
                cur_node.tuple = new_tuple;  // no copy needed -- I think Bingjie 4/22/98
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }
            } else {
                throw new SortException("********** Wait a minute, I thought input is not empty ***************");
            }
        }
        // changed to return Tuple instead of return char array ????
        return old_tuple;
    }

    /**
     * Dummy Class constructor used to give pnodePQ access to global static variables
     */
    public SortPref(){

    }

    /**
     * Class constructor, take information about the tuples, and set up
     * the sorting
     *
     * @param in               array containing attribute types of the relation
     * @param len_in           number of columns in the relation
     * @param str_sizes        array of sizes of string attributes
     * @param am               an iterator for accessing the tuples
     * @param pref_list         the list of attributes that are going to be used for sorting
     * @param sort_order       the sorting order (ASCENDING, DESCENDING)
     * @param pref_list_len    number of preference attributes.
     * @param n_pages          amount of memory (in pages) available for sorting
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public SortPref(AttrType[] in,
                    short      len_in,
                    short[]    str_sizes,
                    Iterator   am,
                    TupleOrder sort_order,
                    int []       pref_list,
                    int        pref_list_len,
                    int        n_pages
    ) throws IOException, SortException
    {
        // store all parameters in global variables
        _in = new AttrType[len_in];
        n_cols = len_in;
        int n_strs = 0;

        for (int i=0; i< len_in; i++)
        {
            _in[i] = new AttrType(in[i].attrType);
            if (in[i].attrType == AttrType.attrString)
            {
                n_strs++;
            }
        }

        // n_strs = number of strings in data
        // str_lens = array of shorts of length
        str_lens = new short[n_strs];

        // store the string lengths in global variable
        n_strs = 0;
        for (int i=0; i<len_in; i++)
        {
            if (_in[i].attrType == AttrType.attrString)
            {
                str_lens[n_strs] = str_sizes[n_strs];
                n_strs ++;
            }
        }

        Tuple t = new Tuple(); // need Tuple.java
        try
        {
            t.setHdr(len_in, _in, str_sizes);
        }
        catch (Exception e)
        {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
        }
        tuple_size = t.size();


        _am = am;
        _pref_list_len = pref_list_len;
        _pref_list = new int[_pref_list_len];
        for(int i=0; i < _pref_list_len; i++){
            _pref_list[i] = pref_list[i];
        }
        order = sort_order;
        _n_pages = n_pages;

        // this may need change, bufs ???  need io_bufs.java
        //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
        bufs_pids = new PageId[_n_pages];
        bufs = new byte[_n_pages][];

        if (useBM)
        {
            try
            {
                get_buffer_pages(_n_pages, bufs_pids, bufs);
            }
            catch (Exception e)
            {
                throw new SortException(e, "Sort.java: BUFmgr error");
            }
        }
        else
        {
            for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
        }

        first_time = true;

        // as a heuristic, we set the number of runs to an arbitrary value
        // of ARBIT_RUNS
        temp_files = new Heapfile[ARBIT_RUNS];
        n_tempfiles = ARBIT_RUNS;
        n_tuples = new int[ARBIT_RUNS];
        n_runs = ARBIT_RUNS;

        try
        {
            temp_files[0] = new Heapfile(null);
        }
        catch (Exception e)
        {
            throw new SortException(e, "Sort.java: Heapfile error");
        }

        o_buf = new OBuf();



        o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
        max_elems_in_heap = 2500;     // should be around 2500 for large data sets!!!!

        // initialize with the modified pnodeSplayPQ constructor
        Q = new pnodeSplayPQ(pref_list, in, order);

        op_buf = new Tuple(tuple_size);   // need Tuple.java
        try
        {
            op_buf.setHdr(n_cols, _in, str_lens);
        }
        catch (Exception e)
        {
            throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
        }

    }

    /**
     * Returns the next tuple in sorted order.
     * Note: You need to copy out the content of the tuple, otherwise it
     * will be overwritten by the next <code>get_next()</code> call.
     *
     * @return the next tuple, null if all tuples exhausted
     * @throws IOException     from lower layers
     * @throws SortException   something went wrong in the lower layer.
     * @throws JoinsException  from <code>generate_runs()</code>.
     * @throws UnknowAttrType  attribute type unknown
     * @throws LowMemException memory low exception
     * @throws Exception       other exceptions
     */
    public Tuple get_next()
            throws IOException,
            SortException,
            UnknowAttrType,
            LowMemException,
            JoinsException,
            Exception {
        if (first_time) {
            // first get_next call to the sort routine
            first_time = false;

            // generate runs
            // call the modified generate runs function that takes an array of types
            Nruns = generate_runs(max_elems_in_heap, _in, sortFldLen);

            // setup state to perform merge of runs.
            // Open input buffers for all the input file

            setup_for_merge(tuple_size, Nruns);

        }

        if (Q.empty()) {
//            System.out.print("no more tuples availble!!!!!!!!!!!\n");
            return null;
        }

        output_tuple = delete_min();
        if (output_tuple != null) {
            op_buf.tupleCopy(output_tuple);
            return op_buf;
        } else
            return null;
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public void close() throws SortException, IOException {
        // clean up
        if (!closeFlag) {

            try {
                _am.close();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: error in closing iterator.");
            }

            if (useBM) {
                try {
                    free_buffer_pages(_n_pages, bufs_pids);
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: BUFmgr error");
                }
                for (int i = 0; i < _n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
            }

            for (int i = 0; i < temp_files.length; i++) {
                if (temp_files[i] != null) {
                    try {
                        temp_files[i].deleteFile();
                    } catch (Exception e) {
                        throw new SortException(e, "SortPref.java: Heapfile error");
                    }
                    temp_files[i] = null;
                }
            }
            closeFlag = true;
        }
    }

    /**
     * Generate sorted runs.
     * Using heap sort.
     *
     * @param max_elems   maximum number of elements in heap
     * @param sortFldType attribute type of the sort field
     * @param count  length of the sort field
     * @return number of runs generated
     * @throws IOException    from lower layers
     * @throws SortException  something went wrong in the lower layer.
     * @throws JoinsException from <code>Iterator.get_next()</code>
     */

    private int generate_runs(int max_elems, AttrType[] sortFldType, int count)
            throws IOException,
            SortException,
            UnknowAttrType,
            TupleUtilsException,
            JoinsException,
            Exception {
        _count = count;
        Tuple tuple;
        pnode cur_node;
        pnodeSplayPQ Q1 = new pnodeSplayPQ(_pref_list, sortFldType, order);
        pnodeSplayPQ Q2 = new pnodeSplayPQ(_pref_list, sortFldType, order);
        pnodeSplayPQ pcurr_Q = Q1;
        pnodeSplayPQ pother_Q = Q2;
        Tuple lastElem = new Tuple(tuple_size);  // need tuple.java
        try {
            lastElem.setHdr(n_cols, _in, str_lens);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: setHdr() failed");
        }

        int run_num = 0;  // keeps track of the number of runs
        // counter for number of elements in the current priority queue
        int p_elems_curr_Q = 0;
        // counter for number of elements in the other priority queue
        int p_elems_other_Q = 0;

        int comp_res;

        // set the lastElem to be the minimum value for the sort field
        if (order.tupleOrder == TupleOrder.Ascending) {
            try {
                MIN_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        } else {
            try {
                MAX_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MAX_VAL failed");
            }
        }

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
            try {
                tuple = _am.get_next();  // according to Iterator.java
            } catch (Exception e) {
                e.printStackTrace();
                throw new SortException(e, "Sort.java: get_next() failed");
            }

            if (tuple == null) {
                break;
            }
            cur_node = new pnode();
            cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98
            pcurr_Q.enq(cur_node);
            p_elems_curr_Q++;
        }

        // now the queue is full, starting writing to file while keep trying
        // to add new tuples to the queue. The ones that does not fit are put
        // on the other queue temperarily
        while (true) {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q--;

            // call to the CompareTupleWithTuplePref method that we have created
            comp_res = TupleUtils.CompareTupleWithTuplePref(cur_node.tuple, _in,lastElem, _in
                    ,n_cols,str_lens,_pref_list,_pref_list_len);

            if ((comp_res < 0 && order.tupleOrder == TupleOrder.Ascending) || (comp_res > 0 && order.tupleOrder == TupleOrder.Descending)) {
                // doesn't fit in current run, put into the other queue
                try {
                    pother_Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q++;
            } else {
                // set lastElem to have the value of the current tuple,
                // need tuple_utils.java
                // call new setValue function that take the array _pref_list
                TupleUtils.SetValue(lastElem, cur_node.tuple, _pref_list, sortFldType);
                // write tuple to output file, need io_bufs.java, type cast???

                o_buf.Put(cur_node.tuple);
            }

            // check whether the other queue is full
            if (p_elems_other_Q == max_elems) {
                // close current run and start next run
                n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
                run_num++;

                // check to see whether need to expand the array
                if (run_num == n_tempfiles) {
                    Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                    for (int i = 0; i < n_tempfiles; i++) {
                        temp1[i] = temp_files[i];
                    }
                    temp_files = temp1;
                    n_tempfiles *= 2;

                    int[] temp2 = new int[2 * n_runs];
                    for (int j = 0; j < n_runs; j++) {
                        temp2[j] = n_tuples[j];
                    }
                    n_tuples = temp2;
                    n_runs *= 2;
                }

                try {
                    temp_files[run_num] = new Heapfile(null);
                } catch (Exception e) {
                    throw new SortException(e, "SortPref.java1022: create Heapfile failed");
                }

                // need io_bufs.java
                o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

                // set the last Elem to be the minimum value for the sort field
                if (order.tupleOrder == TupleOrder.Ascending) {
                    try {
                        MIN_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                } else {
                    try {
                        MAX_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                }

                // switch the current heap and the other heap
                pnodeSplayPQ tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            }

            // now check whether the current queue is empty
            else if (p_elems_curr_Q == 0) {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
                    try {
                        tuple = _am.get_next();  // according to Iterator.java
                    } catch (Exception e) {
                        throw new SortException(e, "get_next() failed");
                    }

                    if (tuple == null) {
                        break;
                    }
                    cur_node = new pnode();
                    cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98

                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q++;
                }
            }

            // Check if we are done
            if (p_elems_curr_Q == 0) {
//                System.out.println("Check if we are done");
                // current queue empty despite our attemps to fill in
                // indicating no more tuples from input
                if (p_elems_other_Q == 0) {
                    // other queue is also empty, no more tuples to write out, done
                    break; // of the while(true) loop
                } else {
                    // generate one more run for all tuples in the other queue
                    // close current run and start next run
                    n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
                    run_num++;

                    // check to see whether need to expand the array
                    if (run_num == n_tempfiles) {
                        Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                        for (int i = 0; i < n_tempfiles; i++) {
                            temp1[i] = temp_files[i];
                        }
                        temp_files = temp1;
                        n_tempfiles *= 2;

                        int[] temp2 = new int[2 * n_runs];
                        for (int j = 0; j < n_runs; j++) {
                            temp2[j] = n_tuples[j];
                        }
                        n_tuples = temp2;
                        n_runs *= 2;
                    }

                    try {
                        temp_files[run_num] = new Heapfile(null);
                    } catch (Exception e) {
                        throw new SortException(e, "SortPref.java: create Heapfile failed");
                    }

                    // need io_bufs.java
                    o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

                    // set the last Elem to be the minimum value for the sort field
                    if (order.tupleOrder == TupleOrder.Ascending) {
                        try {
                            MIN_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    } else {
                        try {
                            MAX_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    }

                    // switch the current heap and the other heap
                    pnodeSplayPQ tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            } // end of if (p_elems_curr_Q == 0)
        } // end of while (true)

        // close the last run
        n_tuples[run_num] = (int) o_buf.flush();
        count++;
        run_num++;

        return run_num;
    }




    /**
     * Set lastElem to be the minimum value of the appropriate types
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field types
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */

    private void MIN_VAL(Tuple lastElem, AttrType[] sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        char[] c = new char[1];
        c[0] = Character.MIN_VALUE;
        String s = new String(c);
for(int i = 0; i < _pref_list_len;i++) {
    switch (sortFldType[i].attrType) {
        case AttrType.attrInteger:
            lastElem.setIntFld(_pref_list[i] + 1, Integer.MIN_VALUE);
            break;
        case AttrType.attrReal:
            lastElem.setFloFld(_pref_list[i] + 1, Float.MIN_VALUE);
            break;
        case AttrType.attrString:
            lastElem.setStrFld(_pref_list[0], s);
            break;
        default:
            // don't know how to handle attrSymbol, attrNull
            //System.err.println("error in sort.java");
            throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
}

        return;
    }



    /**
     * Set lastElem to be the maximum value of the appropriate types
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field types
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */

    private void MAX_VAL(Tuple lastElem, AttrType[] sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        char[] c = new char[1];
        c[0] = Character.MAX_VALUE;
        String s = new String(c);

        for(int i = 0; i < _pref_list_len;i++) {
            switch (sortFldType[i].attrType) {
                case AttrType.attrInteger:
                    lastElem.setIntFld(_pref_list[i], Integer.MAX_VALUE);
                    break;
                case AttrType.attrReal:
                    lastElem.setFloFld(_pref_list[i] + 1, Float.MAX_VALUE);
                    break;
                case AttrType.attrString:
                    lastElem.setStrFld(_pref_list[i], s);
                    break;
                default:
                    // don't know how to handle attrSymbol, attrNull
                    //System.err.println("error in sort.java");
                    throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
            }
        }
        return;
    }





}
