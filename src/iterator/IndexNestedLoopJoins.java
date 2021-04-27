package iterator;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.io.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class IndexNestedLoopJoins extends Iterator {

    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t1_str_sizescopy[];
    private short t2_str_sizescopy[];
    private CondExpr OutputFilter[];
    private CondExpr RightFilter[];
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private Scan inner;


    /**
     * constructor
     * Initialize the two relations which are joined, including relation type,
     *
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S
     * @param t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left i/p to join
     * @param relationName access hfapfile for right i/p to join
     * @param outFilter    select expressions
     * @param rightFilter  reference to filter applied on right i/p
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws NestedLoopException exception from this class
     */
    public IndexNestedLoopJoins(AttrType in1[],
                                int len_in1,
                                short t1_str_sizes[],
                                AttrType in2[],
                                int len_in2,
                                short t2_str_sizes[],
                                int amt_of_mem,
                                Iterator am1,
                                String relationName,
                                CondExpr outFilter[],
                                CondExpr rightFilter[],
                                FldSpec proj_list[],
                                int n_out_flds,
                                boolean materialize,
                                int innerAttNo,
                                HashMap<String, TableMetadata> tableMetadataMap,
                                String outputTableName
    ) throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t1_str_sizescopy = t1_str_sizes;
        t2_str_sizescopy = t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter = rightFilter;

        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        } catch (TupleUtilsException e) {
        }

        try {
            hf = new Heapfile(relationName);
        } catch (Exception e) {
        }

        int jTupleLength = len_in1 + len_in2;
        AttrType[] jList = new AttrType[jTupleLength];
        for (int i = 0; i < len_in1; i++) {
            jList[i] = _in1[i];
        }
        for (int i = len_in1; i < jTupleLength; i++) {
            jList[i] = _in2[i - len_in1];
        }

        short[] jSize = new short[t1_str_sizes.length+t2_str_sizes.length];
        for (int i=0; i< t1_str_sizes.length;i++){
            jSize[i] = t1_str_sizes[i];
        }
        for(int i= t1_str_sizes.length; i < (t1_str_sizes.length+t2_str_sizes.length) ;i++){
            jSize[i] = t2_str_sizes[i-t1_str_sizes.length];
        }


        //*********************************************************************
        //TODO: add logic to determine if index structure already exists for inner relation
        //TODO: and call appropriate method based on result
        //*********************************************************************
        ArrayList<String> indexNames = tableMetadataMap.get(relationName).indexNameList;
        Heapfile outHeap =null;
        if(!outputTableName.equals("")) {
            outHeap = new Heapfile(outputTableName);
        }

        if (indexNames.contains(relationName + "UNCLUST" + "BTREE" + innerAttNo)) {
            iterator.Iterator am2 = null;


            do {
                // If get_from_outer is true, Get a tuple from the outer, delete
                // an existing scan on the file, and reopen a new scan on the file.
                // If a get_next on the outer returns DONE?, then the nested loops
                //join is done too.

                if (get_from_outer == true) {
                    get_from_outer = false;
                    if (inner != null)     // If this not the first time,
                    {
                        // close scan
                        inner = null;
                    }

                /*
                try {
                    inner = hf.openScan();
                }
                catch(Exception e){}
                 */

                    FldSpec[] projList = new FldSpec[in2_len];
                    RelSpec rel2 = new RelSpec(RelSpec.outer);
                    for (int i = 0; i < in2_len; i++) {
                        projList[i] = perm_mat[i];
                    }
                    try {
                        am2 = new IndexScan(new IndexType(IndexType.B_Index), relationName, relationName + "UNCLUST" + "BTREE" + innerAttNo,
                                _in2, t2_str_sizescopy, in2_len, in2_len, projList, rightFilter, in2_len, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if ((outer_tuple = outer.get_next()) == null) {
                        done = true;
                        if (inner != null) {

                            inner = null;
                        }

                        if(!outputTableName.equals("") && materialize){
                            //update the table metadata map
                            TableMetadata tm = new TableMetadata(outputTableName, jList, jSize);
                            tableMetadataMap.put(outputTableName, tm);
                        }

                        return;
                    }
                }  // ENDS: if (get_from_outer == TRUE)


                // The next step is to get a tuple from the inner,
                // while the inner is not completely scanned && there
                // is no match (with pred),get a tuple from the inner.


                //RID rid = new RID();
                while ((inner_tuple = am2.get_next()) != null) {
                    //inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
                    //if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
                    //{
                    //System.out.println("printing outer tuple");
                    //outer_tuple.print(_in1);
                    //System.out.println("printing inner tuple");
                    //inner_tuple.print(_in2);
                    if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true) {
                        // Apply a projection on the outer and inner tuples.
                        Projection.Join(outer_tuple, _in1,
                                inner_tuple, _in2,
                                Jtuple, perm_mat, nOutFlds);

                        //Jtuple.print(_in1);
                        //Jtuple.print(_in2);
//                        System.out.println("printing tuple after join occurs");
                        Jtuple.print(jList);
                        if(!outputTableName.equals("") && materialize) {
                            outHeap.insertRecord(Jtuple.returnTupleByteArray());
                        }

                        //return;
                    }
                    //}
                }

                // There has been no match. (otherwise, we would have
                //returned from t//he while loop. Hence, inner is
                //exhausted, => set get_from_outer = TRUE, go to top of loop

                get_from_outer = true; // Loop back to top and get next outer tuple.
            } while (true);

        } else if ((indexNames.contains(relationName + "UNCLUST" + "HASH" + innerAttNo)) || (indexNames.contains(relationName + "CLUST" + "HASH" + innerAttNo))) {
            try {
                //modify to accept metadata map and if it should be materialized
                HashJoins hJoin = new HashJoins(
                        _in1, in1_len, t1_str_sizescopy, _in2, in2_len, t2_str_sizescopy, n_buf_pgs,
                        am1, relationName, OutputFilter, RightFilter, perm_mat, n_out_flds, materialize, innerAttNo,
                        tableMetadataMap,outputTableName);
            } catch (Exception e) {
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        } else {
            try {
                NestedLoopsJoins nLJoin = new NestedLoopsJoins(
                        _in1, in1_len, t1_str_sizescopy, _in2, in2_len, t2_str_sizescopy, n_buf_pgs,
                        am1, relationName, OutputFilter, RightFilter, perm_mat, n_out_flds);

                Tuple t = new Tuple();
                while ((t = nLJoin.get_next()) != null) {
                    //print the joined tuples
                    //if table should be materialized
                    //add join table to tablemetadata
                    //and create new table
                    t.print(jList);
                    if(!outputTableName.equals("") && materialize) {
                        outHeap.insertRecord(t.returnTupleByteArray());
                    }
                }
                if(!outputTableName.equals("") && materialize){
                    //update the table metadata map
                    TableMetadata tm = new TableMetadata(outputTableName, jList, jSize);
                    tableMetadataMap.put(outputTableName, tm);
                }

            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }

        }

    }

    @Override
    public Tuple get_next() {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }
}

 /*
        Scan scan = null;
        short REC_LEN1 = 4;

        try {
            scan = new Scan(hf);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple temp = null;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) in2_len, _in2, t2_str_sizescopy);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }
        RID rid = new RID();
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("BTreeIndex" + relationName, AttrType.attrInteger, REC_LEN1, 1delete);
            KeyClass key;
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                int intKey = t.getIntFld(1);
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
        */

        /*
        need to create index scan object to scan through indices of inner relation
        this opens an index scan on the inner relation
         */

        /*
        try {
            am2 = new IndexScan(new IndexType(IndexType.B_Index), relationName, "BTreeIndex" + relationName,
                    _in2, t2_str_sizescopy, in2_len, in2_len, perm_mat, RightFilter, in2_len, false);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
         */

        /*
        FldSpec[] projlist3 = new FldSpec[in1_len + in2_len];
        RelSpec rel3 = new RelSpec(RelSpec.outer);
        for(int i =0; i <in1_len + in2_len;i++) {
            projlist3[i] = new FldSpec(rel3, i+1);
        }
        */