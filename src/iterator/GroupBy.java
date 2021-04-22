package iterator;

import btree.BTreeFile;
import btree.KeyClass;
import bufmgr.PageNotReadException;
import global.*;
import hash.HashFile;
import hash.StringKey;
import heap.*;
import index.IndexException;
import index.IndexScan;

import java.beans.Expression;
import java.io.IOException;
import java.util.ArrayList;

public class GroupBy extends Iterator implements GlobalConst {
    private Iterator    outer;
    private int         n_buf_pgs;          // # of buffer pages available.
    private boolean     done,               // Is the join complete
            get_from_outer;     // if TRUE, a tuple is got from outer
    private Tuple       outer_tuple, inner_tuple;
    private Scan        inner;


    private int         tuple_size;
    private FldSpec     _perm_mat[];
    private int         _nOutFlds, count;
    private Tuple       _Jtuple, cur, ret, t;
    private String      prev;

    private AttrType    _in1[];
    public  int         n_cols;
    private short       _t1_str_sizescopy[];
    private Heapfile    _am1;
    private Scan        _scan;
    private IndexScan   _iscan;
    private Heapfile    _hf, _skyline;
    private FldSpec     _group_by_attr;
    private FldSpec[]   _agg_list;
    private AggType     _agg_type;
    private int         _n_out_fields;
    private Sort        sort;
    private Heapfile    temp, temp1;
    boolean             heapFileDone;
    private Scan        tempScan;
    //private int[]       output;
    private boolean     _isHash;
    private HashFile    _hash_file;
    private FileScan    _am;

    private BlockNestedLoopsSky _s;
    private ArrayList<String>   outputs;

    private ArrayList<Tuple>    skyline_mainm, rets;
    private ArrayList<Integer>  mainm_insert_time, tempfile_insert_time;
    private int                 max_tuples_mainm;
    private int                 temp_file_number;
    boolean                     diskdone;
    private Iterator            file_iterator;
    private RID                 rid;
    boolean                     file_read;

    private TupleOrder  order;

    private int         _n_pages;
    private byte[][]    bufs;
    private boolean     first;

    public void GroupBywithSort(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            Heapfile am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages,
            String tableName
    ) throws Exception {
        _isHash = false;
        _in1 = new AttrType[len_in1];
        n_cols = len_in1;
        _t1_str_sizescopy = t1_str_sizes;

        for (int i = 0; i < len_in1; i++) {
            _in1[i] = new AttrType(in1[i].attrType);
        }

        Tuple t = new Tuple(); // need Tuple.java
        try {
            t.setHdr((short) len_in1, _in1, t1_str_sizes);
        } catch (Exception e) {
            throw new GroupByException(e, "GroupBy.java: t.setHdr() failed");
        }
        tuple_size = t.size();

        _am1 = am1;
        _group_by_attr = group_by_attr;
        _agg_list = new FldSpec[agg_list.length];
        for(int i = 0; i < agg_list.length; i++) {
            _agg_list[i] = agg_list[i];
        }
        _agg_type = agg_type;
        _perm_mat = proj_list;
        _nOutFlds = n_out_flds;
        _n_pages = n_pages;

        Scan scan = null;
        try {
            scan = new Scan(_am1);
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        BTreeFile btf = null;
        BTreeFile[] BTreeFileList = new BTreeFile[1];

        try {
            //create index files for all pref attributes in one shot

            Tuple temp = null;
            t = new Tuple();
            try {
                t.setHdr((short) n_cols, _in1, _t1_str_sizescopy);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
            RID rid = new RID();
            for (int i = 0; i < 1; i++) {
                btf = new BTreeFile("BTreeIndexFile" + 1, AttrType.attrString, 32, 1/*delete*/);
                BTreeFileList[i] = btf;
            }
            KeyClass key;
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                String strKey = t.getStrFld(1);
                key = new btree.StringKey(strKey);
                btf.insert(key,rid);
            }
            CondExpr[] expr = new CondExpr[2];
            expr[0] = null;
            expr[1] = null;
            IndexScan iscan = new IndexScan(new IndexType(1), tableName, "BTreeIndexFile" + 1,
                    _in1, _t1_str_sizescopy, n_cols, n_cols, proj_list, expr, n_cols, false);
            _iscan = iscan;
            //btree.BT.printAllLeafPages(BTreeFileList[0].getHeaderPage());
            scan.closescan();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        } finally {
            //close all index files created
            for (int k = 0; k < BTreeFileList.length; k++) {
                try {
                    BTreeFileList[k].close();
                } catch (Exception e) {
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
            }
            try {
                btf.close();
            } catch(Exception e){
                System.err.println("*** BTree File closing error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }

        //System.out.println(t.getStrFld(1));
        //Runtime.getRuntime().exit(1);


        first = true;

    }

    public void GroupBywithHash(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            Heapfile am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
    ) throws Exception {
        _isHash = true;
        _in1 = new AttrType[len_in1];
        n_cols = len_in1;
        _t1_str_sizescopy = t1_str_sizes;

        for (int i=0; i< len_in1; i++)
        {
            _in1[i] = new AttrType(in1[i].attrType);
        }

        _hf = am1;
        _group_by_attr = group_by_attr;
        _agg_list = new FldSpec[agg_list.length];
        for(int i = 0; i < agg_list.length; i++) {
            _agg_list[i] = agg_list[i];
        }
        _agg_type = agg_type;
        _perm_mat = proj_list;
        _nOutFlds = n_out_flds;
        _n_pages = n_pages;

        Scan scan = null;

        try {
            scan = new Scan(am1);
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = {_agg_list[0].offset};
        float utilization = 0.75f;

        try {
            _hash_file = new HashFile("HashIndex" + pref_list[0], AttrType.attrString, 50, 1, utilization);
            rid = new RID();
            PageId deletepid;
            String key = null;
            Tuple temp = null;

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
            }

            t = new Tuple(temp.getLength());
            t.setHdr((short) n_cols, _in1, _t1_str_sizescopy);

            outputs = new ArrayList<String>();
            while (temp != null) {
                t.tupleCopy(temp);

                try {
                    key = t.getStrFld(_group_by_attr.offset);
                    if(!outputs.contains(key))
                        outputs.add(key);
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }

                try {
                    _hash_file.insert(new StringKey(key), rid);
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

            //((HashFile)hash_file).print_all();

        }
        catch(Exception e){
            //status = FAIL;
            e.printStackTrace();
            scan.closescan();
            Runtime.getRuntime().exit(1);
        }

        scan.closescan();

        first = true;
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
            GroupByException,
            UnknowAttrType,
            LowMemException,
            GroupByException,
            Exception {
        float[] output = new float[_nOutFlds];
        float[] sum = new float[_nOutFlds];
        cur = new Tuple();
        prev = new String();
        String outputStr = null;

        int out_flds = _nOutFlds+1;
        AttrType[] attrOutput = new AttrType[out_flds];
        attrOutput[0] = new AttrType(AttrType.attrString);
        for(int i = 1; i < out_flds; i++) {
            attrOutput[i] = new AttrType(AttrType.attrReal);
        }

        if (_isHash) {
            first = true;
            if(outputs.isEmpty()) return null;
            outputStr = outputs.get(0);
            ArrayList <RID> searchResults = _hash_file.search(new StringKey(outputs.get(0)));

            for(int i=0; i<searchResults.size(); i++) {
                rid = searchResults.get(i);
                cur = _hf.getRecord(rid);
                cur.setHdr((short) n_cols, _in1, _t1_str_sizescopy);

                for (int j = 0; j < _nOutFlds; j++) {
                    if (first) {
                        for (int k = 0; k < _nOutFlds; k++) {
                            output[k] = cur.getIntFld(_agg_list[k].offset);
                        }
                        first = false;
                    }
                    switch (_agg_type.aggType) {
                        case (AggType.aggMax):
                            if (output[j] < (float) cur.getIntFld(_agg_list[j].offset)) {
                                output[j] = (float) cur.getIntFld(_agg_list[j].offset);
                            }
                            break;
                        case (AggType.aggMin):
                            if (output[j] > (float) cur.getIntFld(_agg_list[j].offset)) {
                                output[j] = (float) cur.getIntFld(_agg_list[j].offset);
                            }
                            break;
                        case (AggType.aggAvg):
                            sum[j] = sum[j] + (float) cur.getIntFld(_agg_list[j].offset);
                            output[j] = sum[j] / (i + 1);
                            break;
                    }

                }
            }

            outputs.remove(0);
        } else {

            if (first) {
                first = false;

                rets = new ArrayList<Tuple>();

                cur = _iscan.get_next();
                prev = cur.getStrFld(1);

                for (int i = 0; i < _agg_list.length; i++) {
                    output[i] = cur.getIntFld(_agg_list[i].offset);
                    if (_agg_type.aggType == AggType.aggAvg) {
                        sum[i] = output[i];
                    }
                }

                cur = _iscan.get_next();

                while(cur != null) {
                    if(prev.compareTo(cur.getStrFld(_group_by_attr.offset)) != 0) {
                        t = new Tuple();
                        t.setHdr((short) (output.length+1), attrOutput, _t1_str_sizescopy);
                        for(int i=0; i<=output.length; i++) {
                            if(i == 0) {
                                t.setStrFld(i+1, prev);
                            } else {
                                t.setFloFld(i+1, output[i-1]);
                            }
                        }
                        rets.add(t);
                        for (int i = 0; i < _agg_list.length; i++) {
                            output[i] = cur.getIntFld(_agg_list[i].offset);
                            if (_agg_type.aggType == AggType.aggAvg) {
                                sum[i] = output[i];
                            }
                        }
                    }
                    for (int i = 0; i < _nOutFlds; i++) {
                        switch (_agg_type.aggType) {
                            case (AggType.aggMax):
                                if (output[i] < (float) cur.getIntFld(_agg_list[i].offset)) {
                                    output[i] = (float) cur.getIntFld(_agg_list[i].offset);
                                }
                                break;
                            case (AggType.aggMin):
                                if (output[i] > (float) cur.getIntFld(_agg_list[i].offset)) {
                                    output[i] = (float) cur.getIntFld(_agg_list[i].offset);
                                }
                                break;
                            case (AggType.aggAvg):
                                sum[i] = sum[i] + (float) cur.getIntFld(_agg_list[i].offset);
                                output[i] = sum[i] / (i + 2);
                                break;
                        }

                    }

                    prev = cur.getStrFld(1);
                    cur = _iscan.get_next();

                    if(cur == null) {
                        t = new Tuple();
                        t.setHdr((short) (output.length+1), attrOutput, _t1_str_sizescopy);
                        for(int i=0; i<=output.length; i++) {
                            if(i == 0) {
                                t.setStrFld(i+1, prev);
                            } else {
                                t.setFloFld(i+1, output[i-1]);
                            }
                        }
                        rets.add(t);
                    }
                }
            }

            if(rets.isEmpty()) { return null; }
            t = rets.get(0);

            outputStr = t.getStrFld(1);
            output = new float[out_flds-1];
            for(int i=0; i<out_flds-1; i++) {
                output[i] = t.getFloFld(i+2);
            }

            rets.remove(0);
        }

        ret = new Tuple();
        ret.setHdr((short) out_flds, attrOutput, _t1_str_sizescopy);
        ret.setStrFld(1, outputStr);
        for(int i=0; i<output.length; i++) {
            ret.setFloFld(i+2, output[i]);
        }

        return ret;
    }

    /**
     * Returns the next set of tuples in sorted order.
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
    public ArrayList<Tuple> get_skys()
            throws IOException,
            GroupByException,
            UnknowAttrType,
            LowMemException,
            GroupByException,
            Exception {
        int[] pref_list = new int[_agg_list.length];
        for(int i = 0; i < _agg_list.length; i++) {
            pref_list[i] = _agg_list[i].offset-1;
        }

        if(skyline_mainm == null && rets == null) {
            skyline_mainm = new ArrayList<Tuple>();
            rets = new ArrayList<Tuple>();
        }

        if(_isHash) {
            if (first = true) {
                first = false;

                while (!outputs.isEmpty()) {
                    _skyline = new Heapfile("sky");

                    ArrayList<RID> searchResults = _hash_file.search(new StringKey(outputs.get(0)));

                    for (int i = 0; i < searchResults.size(); i++) {
                        rid = searchResults.get(i);
                        cur = _hf.getRecord(rid);
                        cur.setHdr((short) n_cols, _in1, _t1_str_sizescopy);

                        rid = _skyline.insertRecord(cur.returnTupleByteArray());
                    }

                    _am = null;
                    _am = new FileScan("sky", _in1, _t1_str_sizescopy, (short) n_cols, (short) n_cols, _perm_mat, null);

                    _s = null;
                    _s = new BlockNestedLoopsSky(_in1, n_cols, _t1_str_sizescopy, _am,
                            "sky", pref_list, pref_list.length, _n_pages);

                    Tuple t1 = new Tuple();
                    //t1 = _s.get_next();

                    while ((t1 = _s.get_next()) != null) {
                        skyline_mainm.add(t1);
                        //t1 = _s.get_next();
                    }

                    outputs.remove(0);
                }

                _am.close();
                _s.close();
            }

        } else {
            if(first) {
                _skyline = new Heapfile("sky");

                cur = _iscan.get_next();

                while(cur != null) {
                    prev = cur.getStrFld(1);
                    while (cur != null && prev.compareTo(cur.getStrFld(1)) == 0) {
                        rid = _skyline.insertRecord(cur.returnTupleByteArray());
                        cur = _iscan.get_next();
                    }

                    _am = null;
                    _am = new FileScan("sky", _in1, _t1_str_sizescopy, (short) n_cols, (short) n_cols, _perm_mat, null);

                    _s = null;
                    _s = new BlockNestedLoopsSky(_in1, n_cols, _t1_str_sizescopy, _am,
                            "sky", pref_list, pref_list.length, _n_pages);

                    Tuple t1 = new Tuple();
                    //t1 = _s.get_next();

                    while ((t1 = _s.get_next()) != null) {
                        skyline_mainm.add(t1);
                        //t1 = _s.get_next();
                    }

                    cur = _iscan.get_next();
                }
            }
        }

        String outputStr = null;
        if (!skyline_mainm.isEmpty()) {
            outputStr = skyline_mainm.get(0).getStrFld(1);
        }
        String str = null;

        while (!skyline_mainm.isEmpty()) {
            str = skyline_mainm.get(0).getStrFld(1);
            if (outputStr.compareTo(str) == 0) {
                Tuple returnTuple = new Tuple();
                int out_flds = _nOutFlds + 1;
                AttrType[] attrOutput = new AttrType[out_flds];
                attrOutput[0] = new AttrType(AttrType.attrString);
                for (int i = 1; i < out_flds; i++) {
                    attrOutput[i] = new AttrType(AttrType.attrReal);
                }
                returnTuple.setHdr((short) out_flds, attrOutput, _t1_str_sizescopy);
                returnTuple.setStrFld(1, outputStr);
                for (int i = 0; i < _nOutFlds; i++) {
                    returnTuple.setFloFld(i + 2, skyline_mainm.get(0).getIntFld(i + 2));
                }

                skyline_mainm.remove(0);
                rets.add(returnTuple);
            } else {
                break;
            }
        }

        return rets;
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public void close() throws JoinsException, IOException {
        // clean up
        if (!closeFlag) {

            try {
                //_am1.close();
            }
            catch (Exception e) {
                //throw new GroupByException(e, "GroupBy.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
