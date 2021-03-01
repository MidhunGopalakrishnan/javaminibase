package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;

public class NestedLoopsSky extends Iterator{

    private   Iterator  outer;
    private   CondExpr OutputFilter[];
    private   CondExpr RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private   Tuple     outer_tuple, inner_tuple;
    private   Tuple     Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private Heapfile hf;
    private Scan inner;

    private AttrType in1[], _in2[];
    private   int len_in1;
    private   short t2_str_sizescopy[];
    private int [] pref_list;
    private int pref_list_length;
    private int n_pages;

    public NestedLoopsSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, String
            relationName, int[] pref_list, int pref_list_length,
                          int n_pages) throws Exception {
        this.in1 = new AttrType[in1.length];
        //_in2 = new AttrType[in2.length];
        System.arraycopy(in1,0, this.in1,0,in1.length);
        //System.arraycopy(in2,0,_in2,0,in2.length);
        this.len_in1 = len_in1;
        //in2_len = len_in2;


        outer = am1;
        t2_str_sizescopy =  t1_str_sizes;
        inner_tuple = new Tuple();
        outer_tuple = new Tuple();
        Jtuple = new Tuple();
        //OutputFilter = outFilter;
        //RightFilter  = rightFilter;

        n_buf_pgs    = n_pages;
        inner = null;
        done  = false;
        get_from_outer = true;

        //AttrType[] Jtypes = new AttrType[n_out_flds];

        short[]    t_size;

        // Commented by Pranav Iyer!!!!
        //   perm_mat = proj_list;
        //   nOutFlds = n_out_flds;
        //   try {
        // t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
        // 				   in1, len_in1, in2, len_in2,
        // 				   t1_str_sizes, t2_str_sizes,
        // 				   proj_list, nOutFlds);
        //   }catch (TupleUtilsException e){
        // throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        //   }



        try {
            hf = new Heapfile(relationName);

        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }


    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
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
                if(inner == null) // The inner loop has run fully and it is not dominated by any other element
                {
                    System.out.println("New Skyline element found");
                    float [] output = new float [len_in1];
                    float values;

                    System.out.println("The skyline element is ");
                    for(int i = 0; i< len_in1; i++)
                    {
                        if (in1[i].toString().equals("attrInteger")) {
                            values = (float) outer_tuple.getIntFld(i);
                            output[i] = values;
                        } else if (in1[i].toString().equals("attrReal")) {
                            values = (float) outer_tuple.getFloFld(i);
                            output[i] = values;
                        }
                    }
                    System.out.println(Arrays.toString(output));

                }

                get_from_outer = false;
                if (inner != null)     // If this not the first time,
                {
                    // close scan
                    inner = null;
                }

                try {
                    inner = hf.openScan();
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }

                if ((outer_tuple=outer.get_next()) == null)
                {
                    done = true;
                    if (inner != null)
                    {

                        inner = null;
                    }

                    return null;
                }
            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            RID rid = new RID();
            while ((inner_tuple = inner.getNext(rid)) != null)
            {
                // HERE CHECK WHETHER THE INNER TUPLE DOMINATES THE OUTER TUPLE
                // IF YES THEN BREAK OUT OF INNER LOOP TO ALLOW OUTER TO CONTINUE TO NEXT ELEMENT, RESTART INNER
                // ELSE ALLOW OUTER TO CHECK NEXT INNER IN INNER LOOP
                // IF INNER IS DONE, THAT MEANS THE CORRESPONDING OUTER IS A SKYLINE ELEMENT

                int pref_list [] = {2,4}; // Out of 5 elements in sample input file these are in skyline
                int pref_list_length = pref_list.length;

                if(heap.Tuple.dominates(inner_tuple, in1, outer_tuple, _in2,
                        (short) len_in1, t2_str_sizescopy, pref_list, pref_list_length)) //IF INNER ELEMENT DOMINATES
                {
                    get_from_outer = true; // Increment the outer loop
                }

                //   inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
                //   if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
                //     {
                //       if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
                // 	{
                // 	  // Apply a projection on the outer and inner tuples.
                // 	  Projection.Join(outer_tuple, _in1,
                // 			  inner_tuple, _in2,
                // 			  Jtuple, perm_mat, nOutFlds);
                // 	  return Jtuple;
                // 	}
                //     }
            }

            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {

            try {
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }

    public void printData() throws Exception {
        Tuple next = this.get_next();
        System.out.println(next.toString());
    }
}
