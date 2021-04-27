package iterator;

import btree.BTreeFile;
import btree.KeyClass;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import hash.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class HashJoins extends Iterator{
    private AttrType _in1[],  _in2[];
    private   int        in1_len, in2_len;
    private   Iterator  outer;
    private   short t1_str_sizescopy[];
    private   short t2_str_sizescopy[];
    private   CondExpr OutputFilter[];
    private   CondExpr RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private int         numPartitions;
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private   Tuple     Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private Heapfile hf;
    private Scan inner;


    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param in2  Array containing field types of S
     *@param len_in2  # of columns in S
     *@param  t2_str_sizes shows the length of the string fields.
     *@param amt_of_mem  IN PAGES
     *@param am1  access method for left i/p to join
     *@param relationName  access hfapfile for right i/p to join
     *@param outFilter   select expressions
     *@param rightFilter reference to filter applied on right i/p
     *@param proj_list shows what input fields go where in the output tuple
     *@param n_out_flds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public HashJoins( AttrType    in1[],
                      int     len_in1,
                      short   t1_str_sizes[],
                      AttrType    in2[],
                      int     len_in2,
                      short   t2_str_sizes[],
                      int     amt_of_mem,
                      Iterator     am1,
                      String relationName,
                      CondExpr outFilter[],
                      CondExpr rightFilter[],
                      FldSpec   proj_list[],
                      int        n_out_flds,
                      boolean materialize,
                      int innerAttNo,
                      HashMap<String, TableMetadata> tableMetadataMap,
                      String outputTableName
    ) throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t1_str_sizescopy = t1_str_sizes;
        t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        numPartitions = n_buf_pgs - 1;
        inner = null;
        done  = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        }catch (TupleUtilsException e){}

        int jTupleLength = len_in1 + len_in2;
        AttrType[] jList = new AttrType[jTupleLength];
        for(int i = 0; i < len_in1; i++)
        {
            jList[i] = _in1[i];
        }
        for(int i = len_in1; i < jTupleLength; i++)
        {
            jList[i] = _in2[i - len_in1];
        }

        short[] jSize = new short[t1_str_sizes.length+t2_str_sizes.length];
        for (int i=0; i< t1_str_sizes.length;i++){
            jSize[i] = t1_str_sizes[i];
        }
        for(int i= t1_str_sizes.length; i < (t1_str_sizes.length+t2_str_sizes.length) ;i++){
            jSize[i] = t2_str_sizes[i-t1_str_sizes.length];
        }

        Heapfile outHeap =null;
        if(!outputTableName.equals("")) {
            outHeap = new Heapfile(outputTableName);
        }


        //create hash index**************************************
        int REC_LEN1 = 50;

        //ArrayList<BTreeFile> BTreeFileList = new ArrayList<>();
        float utilization = 0.75f;

        try {
            hf = new Heapfile(relationName);
        }
        catch(Exception e) {}

        Tuple t = new Tuple();
        try {
            t.setHdr((short) in2_len, _in2, t2_str_sizescopy);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Scan scan = null;

        try {
            scan = new Scan(hf);
        }
        catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        try {

            //TODO: check naming convention
            String indexFileName = relationName + "UNCLUST" + "HASH"+ innerAttNo;
            HashFile hash_file = null;
            if(_in2[innerAttNo].attrType == AttrType.attrString) {
                hash_file = new HashFile(relationName + "UNCLUST" + "HASH" + innerAttNo, AttrType.attrString, REC_LEN1, 1, utilization);
            }
            else{
                hash_file = new HashFile(relationName + "UNCLUST" + "HASH" + innerAttNo, AttrType.attrInteger, REC_LEN1, 1, utilization);
            }
            if(!tableMetadataMap.get(relationName).indexNameList.contains(indexFileName)){
                tableMetadataMap.get(relationName).indexNameList.add(indexFileName);
            }
            RID rid = new RID();
            RID ridToDelete = null;
            PageId deletepid;
            String strKey = null;
            int intKey = -1;
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
                    //key = String.valueOf(t.getFloFld(pref_list[0] + 1));
                    //TODO: remove hardcoding of field number that is join attr
                    if(_in2[innerAttNo].attrType == AttrType.attrString)
                    {
                        strKey = t.getStrFld(innerAttNo);
                        intKey = -1;
                        hash_file.insert(new StringKey(strKey), rid);
                    }
                    else if(_in2[innerAttNo].attrType == AttrType.attrInteger)
                    {
                        intKey = t.getIntFld(innerAttNo);
                        strKey = "";
                        hash_file.insert(new IntegerKey(intKey), rid);
                    }
                    else
                    {
                        System.out.println("error!");
                    }

                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }

                /*
                try {
                    //System.out.println("Insert Count is " + (++insert_count) + "\n");
                    if(_in2[innerAttNo].attrType == AttrType.attrString)
                    {
                        hash_file.insert(new StringKey(strKey), rid);
                    }
                    else
                    {
                        hash_file.insert(new IntegerKey(intKey), rid);
                    }
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }

                 */

                try {
                    temp = scan.getNext(rid);
                } catch (Exception e) {
                    //status = FAIL;
                    e.printStackTrace();
                }
            }

            //this does the checking *****************************************
            Tuple tempOuter = new Tuple();
            //TODO: figure out how to access join attribute
            //int joinAttr = 1;

            //ArrayList<RID> ridList = new ArrayList<>();
            //int count = 0;
            while ((tempOuter = outer.get_next()) != null) {
                //System.out.println("printing outer tuple");
                //tempOuter.print(_in1);
                //System.out.println(tempOuter.getStrFld(innerAttNo));

                //searchKey = Integer.toString(tempOuter.getIntFld(joinAttr));
                //TODO: the key type that is called depends on the attrtype of the join attr
                ArrayList<RID> searchResults = new ArrayList<>();

                if(_in2[innerAttNo].attrType == AttrType.attrString)
                {
                    String strSearchKey = "";
                    strSearchKey = tempOuter.getStrFld(innerAttNo);
                    searchResults = hash_file.search(new StringKey(strSearchKey));
                    if(searchResults != null) {
                        System.out.println(searchResults.size());
                        for (int i = 0; i < searchResults.size(); i++) {
                            rid = searchResults.get(i);
                            rid = searchResults.get(0);
                            t = hf.getRecord(rid);
                            t.setHdr((short) in2_len, _in2, t2_str_sizescopy);
                            if (PredEval.Eval(OutputFilter, tempOuter, t, _in1, _in2) == true)
                            {
                                // Apply a projection on the outer and inner tuples.
                                Projection.Join(tempOuter, _in1,
                                        t, _in2,
                                        Jtuple, perm_mat, nOutFlds);

//                            System.out.println("printing tuple after join occurs");
                                //count++;
                                //System.out.println(count);
                                Jtuple.print(jList);
                                if(!outputTableName.equals("") && materialize) {
                                    outHeap.insertRecord(Jtuple.returnTupleByteArray());
                                }

                                //return;
                            }
                        }
                    }
                }
                else
                {
                    int intSearchKey = -1;
                    intSearchKey = tempOuter.getIntFld(innerAttNo);
                    searchResults = hash_file.search(new IntegerKey(intSearchKey));
                    if(searchResults != null) {
//                        System.out.println(searchResults.size());
                        for (int i = 0; i < searchResults.size(); i++) {
                            rid = searchResults.get(i);
                            //rid = searchResults.get(0);
                            t = hf.getRecord(rid);
                            t.setHdr((short) in2_len, _in2, t2_str_sizescopy);
                            if (PredEval.Eval(OutputFilter, tempOuter, t, _in1, _in2) == true)
                            {
                                // Apply a projection on the outer and inner tuples.
                                Projection.Join(tempOuter, _in1,
                                        t, _in2,
                                        Jtuple, perm_mat, nOutFlds);

//                            System.out.println("printing tuple after join occurs");
                                //count++;
                                //System.out.println(count);
                                Jtuple.print(jList);
                                if(!outputTableName.equals("") && materialize) {
                                    outHeap.insertRecord(Jtuple.returnTupleByteArray());
                                }

                                //return;
                            }
                        }
                    }
                }

            }

            if(!outputTableName.equals("") && materialize){
                //update the table metadata map
                TableMetadata tm = new TableMetadata(outputTableName, jList, jSize);
                tableMetadataMap.put(outputTableName, tm);
            }
        }
        catch(Exception e){
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }
        /*
        we create n_pages - 1 heapfile partitions for both relations

        we read in data for outer relation
        we scan through each tuple, get the value of the join attribute,
        and hash it into a partition based on that value

        we do the same thing for the inner relation, since there are the
        same number of partitions and the same hash function is used, we know
        that all equivalent values will be in the same partition

        when we need to scan through a partition of an outer relation, we can
        create a hash table on top of  the inner relation to reduce the number
        of scans

        as we scan through the partition of the outer relation, for each tuple
        we query the hash table based on the join attribute key for any values
        in the table that match

        if they do, then we join them and print out the result
         */

        /*
        //creates heap files to partition data for both relations
        Heapfile[] outerHeapFileArray = new Heapfile[n_buf_pgs - 1];
        String heapFileName = "outerHeapFile";
        for(int i = 0; i < numPartitions; i++)
        {
            Heapfile heapFile = null;
            try {
                heapFile = new Heapfile(heapFileName + i);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            Tuple temp = null;
            Tuple scanTuple = new Tuple();
            try {
                scanTuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            outerHeapFileArray[i] = heapFile;
        }

        Heapfile[] innerHeapFileArray = new Heapfile[n_buf_pgs - 1];
        String innerHeapFileName = "innerHeapFile";
        for(int i = 0; i < numPartitions; i++)
        {
            Heapfile heapFile = null;
            try {
                heapFile = new Heapfile(heapFileName + i);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            Tuple temp = null;
            Tuple scanTuple = new Tuple();
            try {
                scanTuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            innerHeapFileArray[i] = heapFile;
        }

        //read in data from outer relation and populate it in the heap files
        int joinValue = 0;
        int hashIndex = 0;
        Tuple scanTuple = new Tuple();
        try {
            scanTuple.setHdr((short) in1_len, in1, t1_str_sizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Tuple tempOuter = new Tuple();
        while((tempOuter = outer.get_next()) != null)
        {
            scanTuple.tupleCopy(tempOuter);
            //need to figure out how to reference the join attribute
            joinValue = scanTuple.getIntFld(2);
            hashIndex = joinValue % numPartitions;
            outerHeapFileArray[hashIndex].insertRecord(scanTuple.getTupleByteArray());
        }

        //read in data from inner relation and populate it in the heap files
        Heapfile tempHeapFile = null;
        try {
            tempHeapFile = new Heapfile(relationName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Scan scan = null;
        try {
            scan = new Scan(tempHeapFile);
        } catch (Exception e) {
            //status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        Tuple temp = null;
        scanTuple = new Tuple();
        try {
            scanTuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        RID rid = new RID();
        try {
            while (( temp = scan.getNext(rid)) != null) {
                scanTuple.tupleCopy(temp);
                joinValue = scanTuple.getIntFld(2);
                hashIndex = joinValue % numPartitions;
                innerHeapFileArray[hashIndex].insertRecord(scanTuple.getTupleByteArray());
            }
        }catch(Exception e){
            System.err.println("*** BTree File error ***");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        } finally {
            scan.closescan();
        }

        //now we populate the data from the partition heap file of an inner relation into
        //a hashmap and iterate through the partition of the outer relation to look for matches
        for(int i = 0; i < numPartitions; i++)
        {
            HashMap<Integer,Tuple> partitionMap = new HashMap<>();
            Heapfile partitionHF = innerHeapFileArray[i];
            int hashKey = 0;

            Scan partitionScan = null;
            try {
                partitionScan = new Scan(partitionHF);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
            Tuple partitionTemp = null;
            Tuple partitionTuple = new Tuple();
            try {
                partitionTuple.setHdr((short) in2_len, in2, t2_str_sizescopy);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            RID rid2 = new RID();
            try {
                while (( partitionTemp = partitionScan.getNext(rid2)) != null) {
                    partitionTuple.tupleCopy(partitionTemp);
                    hashKey = partitionTuple.getIntFld(2);
                    System.out.println(hashKey);
                    partitionMap.put(hashKey,partitionTuple);
                }
            }catch(Exception e){
                System.err.println("*** BTree File error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            } finally {
                scan.closescan();
            }

            //now we do the comparison between relations
            Heapfile outerHF = outerHeapFileArray[i];
            Scan outerScan = null;
            try {
                outerScan = new Scan(outerHF);
            } catch (Exception e) {
                //status = FAIL;
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
            Tuple outerTuple = new Tuple();
            try {
                outerTuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            Tuple outerTemp = new Tuple();

            RID rid3 = new RID();
            Tuple matchedTuple = new Tuple();
            try {
                while (( outerTemp = outerScan.getNext(rid3)) != null) {
                    outerTuple.tupleCopy(outerTemp);
                    //once again, need to figure out the issue with getting the merge attribute
                    if(partitionMap.containsKey(outerTuple.getIntFld(2)))
                    {
                        matchedTuple = partitionMap.get(outerTuple.getIntFld(2));

                        System.out.println("printing outer tuple");
                        outerTuple.print(_in1);
                        System.out.println("printing inner tuple");
                        matchedTuple.print(_in2);

                        if (PredEval.Eval(OutputFilter, outerTuple, matchedTuple, _in1, _in2) == true) {
                            // Apply a projection on the outer and inner tuples.
                            Projection.Join(outerTuple, _in1,
                                    matchedTuple, _in2,
                                    Jtuple, perm_mat, nOutFlds);

                            AttrType[] jList = new AttrType[6];
                            jList[0] = _in1[0];
                            jList[1] = _in1[1];
                            jList[2] = _in1[2];
                            jList[3] = _in2[0];
                            jList[4] = _in2[1];
                            jList[5] = _in2[2];
                            //Jtuple.print(_in1);
                            //Jtuple.print(_in2);
                            System.out.println("printing tuple after join occurs");
                            Jtuple.print(jList);
                        }
                    }
                }
            }catch(Exception e){
                System.err.println("*** BTree File error ***");
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            } finally {
                scan.closescan();
            }
        }
         */

    //}

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }
}
