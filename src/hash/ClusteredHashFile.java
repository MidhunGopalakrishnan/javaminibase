/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package hash;

import java.io.*;
import java.util.ArrayList;

import hash.KeyDataEntry;
import hash.StringKey;

import java.lang.String.*;
import java.lang.reflect.Array;
import java.lang.Math.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.TupleUtils;

/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class ClusteredHashFile extends IndexFile implements GlobalConst,Serializable {

    private final static int MAGIC0 = 1989;

    private final static String lineSep = System.getProperty("line.separator");

    private static FileOutputStream fos;
    private static DataOutputStream trace;

    // Tuple t;

    private int h_0 = 3, level, split_ptr_loc, max_per_page;

    private float utilization;
    int max_key_values_per_page; // maximum key,value pairs we can store per bucket page
    int max_bucket_key_value, cur_bucket_key_value; // total key,value pairs in overall primary buckets excluding
    // overflow
    private ArrayList<PageId> bucket_location = new ArrayList<PageId>();
    private ArrayList<PageId> latest_inserted_datapage = new ArrayList<PageId>();
    Heapfile data_hf;

    /**
     * It causes a structured trace to be written to a file. This output is used to
     * drive a visualization tool that shows the inner workings of the b-tree during
     * its operations.
     *
     * @param filename input parameter. The trace file name
     * @exception IOException error from the lower layer
     */
    public static void traceFilename(String filename) throws IOException {

        fos = new FileOutputStream(filename);
        trace = new DataOutputStream(fos);
    }

    /**
     * Stop tracing. And close trace file.
     *
     * @exception IOException error from the lower layer
     */
    public static void destroyTrace() throws IOException {
        if (trace != null)
            trace.close();
        if (fos != null)
            fos.close();
        fos = null;
        trace = null;
    }

    private HashHeaderPage headerPage;
    private PageId headerPageId;
    private String dbname;

    /**
     * Access method to data member.
     *
     * @return Return a BTreeHeaderPage object that is the header page of this btree
     *         file.
     */
    public HashHeaderPage getHeaderPage() {
        return headerPage;
    }

    private PageId get_file_entry(String filename) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageno) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno) throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void delete_file_entry(String filename) throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    /**
     * BTreeFile class an index file with given filename should already exist; this
     * opens it.
     *
     * @param filename the B+ tree file name. Input parameter.
     * @exception GetFileEntryException  can not ger the file from DB
     * @exception PinPageException       failed when pin a page
     * @exception ConstructPageException BT page constructor failed
     */
    public ClusteredHashFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {
        headerPageId = get_file_entry(filename);
        headerPage = new HashHeaderPage(headerPageId);
        dbname = new String(filename);
        /*
         *
         * - headerPageId is the PageId of this BTreeFile's header page; - headerPage,
         * headerPageId valid and pinned - dbname contains a copy of the name of the
         * database
         */
    }

    /**
     * if index file exists, open it; else create it.
     *
     * @param filename       file name. Input parameter.
     * @param keytype        the type of key. Input parameter.
     * @param keysize        the maximum size of a key. Input parameter.
     * @param delete_fashion full delete or naive delete. Input parameter. It is
     *                       either DeleteFashion.NAIVE_DELETE or
     *                       DeleteFashion.FULL_DELETE.
     * @exception GetFileEntryException  can not get file
     * @exception ConstructPageException page constructor failed
     * @exception IOException            error from lower layer
     * @exception AddFileEntryException  can not add file into DB
     */
    public ClusteredHashFile(String filename, int keytype, int keysize, int delete_fashion, float utilization_input, Heapfile hf)
            throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {
//        headerPageId = get_file_entry(filename);
        if (headerPageId == null) // file not exist
        {
            headerPage = new HashHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_magic0(MAGIC0);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
            headerPage.set_keyType((short) keytype);
            headerPage.set_maxKeySize(keysize);
            headerPage.set_deleteFashion(delete_fashion);
            headerPage.setType(NodeType.HASHHEAD);

            utilization = utilization_input;
            AttrType[] attrType = new AttrType[1];
            attrType[0] = new AttrType(AttrType.attrString);

            data_hf = hf;

            // t = new Tuple(); //CREATE TUPLE for h_0, level, split_ptr_loc AND INSERT INTO
            // HEADER PAGE
            // Tuple copy_t;
            // try {
            // t.setHdr((short) numOfColumns, attrType, attrSize);
            // }
            // catch (Exception e) {
            // e.printStackTrace();
            // }

            // Since data are stored as key value pairs and rid consists of 2 integers
            int key_values_size = keysize + 4 * 2;
            max_per_page = (int) Math.floor((MINIBASE_PAGESIZE / key_values_size) * utilization);
            System.out.println("Maximum elements in a page are "+max_per_page);
            level = 0;
            max_bucket_key_value = (int) (max_per_page * Math.pow(2, h_0 + level));
            cur_bucket_key_value = 0;

            split_ptr_loc = -1;

            try {
                // t.setStrFld(1, Integer.toString(h_0)); //Will Specify Initial Number of
                // buckets
                // copy_t = new Tuple(t);
                // headerPage.insertRecord(copy_t.getTupleByteArray());

                // t.setStrFld(1, Integer.toString(level));
                // copy_t = new Tuple(t);
                // headerPage.insertRecord(copy_t.getTupleByteArray());

                // t.setStrFld(1, Integer.toString(split_ptr_loc));
                // copy_t = new Tuple(t);
                // headerPage.insertRecord(copy_t.getTupleByteArray());
            }

            catch (Exception e) {
                System.out.println("Error in HashFile.java line 251");
                e.printStackTrace();
            }

            // Create new bucket page from 0 till h_0 - 1
            PageId newRootPageId;
            HashLeafPage newRootPage;

            for (int i = 0; i < ((int) Math.pow(2, h_0)); i++) {
                newRootPage = new HashLeafPage(headerPage.get_keyType());
                newRootPage.setNextPage(new PageId(INVALID_PAGE));
                newRootPage.setPrevPage(new PageId(INVALID_PAGE));
                newRootPageId = newRootPage.getCurPage();

                try {
                    // t.setStrFld(1, Integer.toString(newRootPageId.pid));
                    bucket_location.add(newRootPageId);
                    latest_inserted_datapage.add(null);
                    // copy_t = new Tuple(t);
                    // headerPage.insertRecord(copy_t.getTupleByteArray());
                    unpinPage(newRootPageId, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        } else {
            headerPage = new HashHeaderPage(headerPageId);
        }

        dbname = new String(filename);

    }

    /**
     * Close the B+ tree file. Unpin header page.
     *
     * @exception PageUnpinnedException       error from the lower layer
     * @exception InvalidFrameNumberException error from the lower layer
     * @exception HashEntryNotFoundException  error from the lower layer
     * @exception ReplacerException           error from the lower layer
     */
    public void close()
            throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    /**
     * Destroy entire B+ tree file.
     *
     * @exception IOException              error from the lower layer
     * @exception IteratorException        iterator error
     * @exception UnpinPageException       error when unpin a page
     * @exception FreePageException        error when free a page
     * @exception DeleteFileEntryException failed when delete a file from DM
     * @exception ConstructPageException   error in BT page constructor
     * @exception PinPageException         failed when pin a page
     */
    public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
            DeleteFileEntryException, ConstructPageException, PinPageException {
        if (headerPage != null) {
            PageId pgId = headerPage.get_rootId();
            if (pgId.pid != INVALID_PAGE)
                _destroyFile(pgId);
            unpinPage(headerPageId);
            freePage(headerPageId);
            delete_file_entry(dbname);
            headerPage = null;
        }
    }

    private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
            ConstructPageException, UnpinPageException, FreePageException {

        BTSortedPage sortedPage;
        Page page = pinPage(pageno);
        sortedPage = new BTSortedPage(page, headerPage.get_keyType());

        if (sortedPage.getType() == NodeType.INDEX) {
            BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
            RID rid = new RID();
            PageId childId;
            KeyDataEntry entry;
            for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
                childId = ((IndexData) (entry.data)).getData();
                _destroyFile(childId);
            }
        } else { // BTLeafPage

            unpinPage(pageno);
            freePage(pageno);
        }

    }

    // private void updateHeader(PageId newRoot)
    // throws IOException,
    // PinPageException,
    // UnpinPageException
    // {

    // HashHeaderPage header;
    // PageId old_data;

    // header= new HashHeaderPage( pinPage(headerPageId));

    // old_data = headerPage.get_rootId();
    // header.set_rootId( newRoot);

    // // clock in dirty bit to bm so our dtor needn't have to worry about it
    // unpinPage(headerPageId, true /* = DIRTY */ );

    // // ASSERTIONS:
    // // - headerPage, headerPageId valid, pinned and marked as dirty

    // }

    void removeEmptyPage(HashLeafPage pageToBeDeleted) throws Exception
    {
        PageId deleteid = pageToBeDeleted.getCurPage();
        PageId previd = pageToBeDeleted.getPrevPage();
        if(previd.pid==INVALID_PAGE) {throw new Exception ();} // Our page has no previous link how's this possible?
        PageId nextid = pageToBeDeleted.getNextPage();
        if(nextid.pid!=INVALID_PAGE)
        {
            try{
                HashLeafPage prevPage = new HashLeafPage(previd, headerPage.get_keyType());
                HashLeafPage nextPage = new HashLeafPage(nextid, headerPage.get_keyType());
                prevPage.setNextPage(nextid);
                nextPage.setPrevPage(previd);
                unpinPage(deleteid);
                freePage(deleteid);
            }catch(Exception e){e.printStackTrace();}
        }
        unpinPage(deleteid);
        freePage(deleteid);
    }

    // todo Better Way of checking for empty space than insert and then null

    void rehash(PageId bucket_id_1, PageId bucket_id_2, Boolean firstTime) {
        PageId inserted_pageid;
        PCounter.readIncrement();
        PCounter.writeIncrement();
        // Read records from bucket_page_1, hash it with level+1 and insert/leave it in
        // the corresponding page
        HashLeafPage currentPage;
        try {
            currentPage = new HashLeafPage(bucket_id_1, headerPage.get_keyType());
            KeyDataEntry temp_entry = null;
            RID temp_rid = new RID();
            ArrayList<KeyDataEntry> toBeRehashed = new ArrayList<KeyDataEntry>();
            ArrayList<RID> ridRehashed = new ArrayList<RID>();
            for (temp_entry = currentPage.getFirst(temp_rid); temp_entry != null; temp_entry = currentPage
                    .getNext(temp_rid)) {
                int temp_hash_value = hash_record(temp_entry.key);
                int mod_value = temp_hash_value % (int) Math.pow(2, h_0 + level + 1); // Some error in logic here
                // If it gets hashed to a new primary bucket, put it there or its overflow
                // and delete it from the current table
                if (mod_value == bucket_location.size() - 1) {
                    KeyDataEntry copy_temp_entry = new KeyDataEntry(temp_entry.key, temp_entry.data);
                    toBeRehashed.add(copy_temp_entry);
                    RID r = new RID();
                    r.copyRid(((LeafData) temp_entry.data).getData());
                    ridRehashed.add(r);
                }
            }

            PageId rehash_inserted_bucket = null;
            for (int i = 0; i < toBeRehashed.size(); i++) {
                temp_entry = toBeRehashed.get(i);
                temp_rid = ridRehashed.get(i);
                rehash_inserted_bucket = _insert(temp_entry.key, temp_rid, bucket_id_2); // replace temp_rid with rid of
                // keydata class
                if ((firstTime == false)
                        && (rehash_inserted_bucket.pid == bucket_location.get(bucket_location.size() - 1).pid))
                    cur_bucket_key_value++;

                currentPage.delEntry(temp_entry);
            }

            PageId overflow_page = currentPage.getNextPage();
            //todo Implement removing empty bucketpage
            if(!firstTime && currentPage.getFirst(temp_rid)==null) //Check whether an empty page needs to be removed
            {
                removeEmptyPage(currentPage);
            }
            else
                unpinPage(bucket_id_1, true);

            if (overflow_page.pid == INVALID_PAGE) {
                // No overflow pages, we are done!
                // unpinPage(bucket_id_2, true);
                // System.out.println("REHASHING DONE!!!!!");
                return;
            }

            rehash(overflow_page, rehash_inserted_bucket, false);

        }

        catch (Exception e) {
            e.printStackTrace();
        }

    }

    int hash_record(KeyClass key) {
        if (key instanceof hash.StringKey) {
            String key_value = new String(((hash.StringKey) key).getKey());
            return Math.abs(key_value.hashCode());
        }

        else if (key instanceof hash.IntegerKey) {
            Integer key_value = ((hash.IntegerKey) key).getKey();
            return Math.abs(key_value.hashCode());
        }

        return -1;
    }

    public RID insertIntoDataFile(KeyClass key, byte[] recPtr) throws Exception
    {
        ArrayList<RID> ridInDataFile = search(key); //Can search handle bucketpages having no values?
        int hash_value = hash_record(key);
        RID df_rid, insertDatafileRID=null;
        if(ridInDataFile != null)
        {	for(int i=0; i<ridInDataFile.size(); i++)
        {
            df_rid = ridInDataFile.get(i);
            df_rid = data_hf.insertRecordIntoPage(recPtr, df_rid.pageNo, true);
            if(df_rid!=null) return df_rid;
        }
        }

        //If code reaches here it means that either the key doesn't exist or
        // the existing pages corresponding to key are full
        //We just have to create a new heappage, initialize it, insert into it we also have
        //to insert the (key,rid) into the index

        try{
            Page apage = new Page();
            PageId pageId = new PageId();
            try {
                pageId = SystemDefs.JavabaseBM.newPage(apage,1);
            }
            catch (Exception e) {
                throw new HFBufMgrException(e,"ClusteredHashFile.java: newPage() failed");
            }
            if(pageId == null)
                throw new HFException(null, "can't new pae");
            // initialize internal values of the new page:
            HFPage hfpage = new HFPage();
            hfpage.init(pageId, apage);
            unpinPage(pageId, true /*Dirty*/);

            insertDatafileRID =  data_hf.insertRecordIntoPage(recPtr, pageId, false); //todo check if unpin is occurring

            insert(key, insertDatafileRID);
        } catch (Exception e)
        {
            e.printStackTrace();}

        return insertDatafileRID;

        //****************************************************************************** */
        // int bucket_to_insert = hash_value % (int) Math.pow(2, h_0 + level);
        // if (bucket_to_insert <= split_ptr_loc)
        // 	bucket_to_insert = hash_value % (int) Math.pow(2, h_0 + level + 1);
        // PageId datafile_pid = latest_inserted_datapage.get(bucket_to_insert);

        // RID insertDatafileRID = new RID();
        // if(datafile_pid==null)
        // {
        //      try{
        //         Page apage = new Page();
        //         PageId pageId = new PageId();
        //         try {
        //         pageId = SystemDefs.JavabaseBM.newPage(apage,1);
        //         }
        //         catch (Exception e) {
        //         throw new HFBufMgrException(e,"ClusteredHashFile.java: newPage() failed");
        //         }
        //         if(pageId == null)
        //             throw new HFException(null, "can't new pae");
        //         // initialize internal values of the new page:
        //         HFPage hfpage = new HFPage();
        //         hfpage.init(pageId, apage);
        //         unpinPage(pageId, true /*Dirty*/);

        //         insertDatafileRID =  data_hf.insertRecordIntoPage(recPtr, pageId, false); //todo check if unpin is occurring

        //         PageId x = new PageId(insertDatafileRID.pageNo.pid);
        //         latest_inserted_datapage.set(bucket_to_insert, x);
        //         } catch (Exception e)
        //         {
        //             e.printStackTrace();}
        // }

        // else
        // {
        //     try{
        //         insertDatafileRID =  data_hf.insertRecordIntoPage(recPtr, datafile_pid, true);
        //         if(insertDatafileRID == null)
        //         {
        //             // We don't have space for another tuple so we create a new heappage,
        //             // initialize its values and send its pid to be inserted
        //             Page apage = new Page();
        //             PageId pageId = new PageId();
        //             try {
        //             pageId = SystemDefs.JavabaseBM.newPage(apage,1);
        //             }
        //             catch (Exception e) {
        //             throw new HFBufMgrException(e,"ClusteredHashFile.java: newPage() failed");
        //             }
        //             if(pageId == null)
        //                 throw new HFException(null, "can't new pae");
        //             // initialize internal values of the new page:
        //             HFPage hfpage = new HFPage();
        //             hfpage.init(pageId, apage);
        //             unpinPage(pageId);

        //             insertDatafileRID =  data_hf.insertRecordIntoPage(recPtr, pageId, false);
        //             PageId x = new PageId(insertDatafileRID.pageNo.pid);
        //             latest_inserted_datapage.set(bucket_to_insert, x);
        //         }
        //     }catch (Exception e)
        //     {
        //         e.printStackTrace();
        //     }
        // }

        // return insertDatafileRID;
    }

    /**
     * insert record with the given key and rid
     *
     * @param key the key of the record. Input parameter.
     * @param rid the rid of the record. Input parameter.
     * @exception KeyTooLongException     key size exceeds the max keysize.
     * @exception KeyNotMatchException    key is not integer key nor string key
     * @exception IOException             error from the lower layer
     * @exception LeafInsertRecException  insert error in leaf page
     * @exception IndexInsertRecException insert error in index page
     * @exception ConstructPageException  error in BT page constructor
     * @exception UnpinPageException      error when unpin a page
     * @exception PinPageException        error when pin a page
     * @exception NodeNotMatchException   node not match index page nor leaf page
     * @exception ConvertException        error when convert between revord and byte
     *                                    array
     * @exception DeleteRecException      error when delete in index page
     * @exception IndexSearchException    error when search
     * @exception IteratorException       iterator error
     * @exception LeafDeleteException     error when delete in leaf page
     * @exception InsertException         error when insert in index page
     */
    public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException

    {
        KeyDataEntry newRootEntry;

        if (BT.getKeyLength(key) > headerPage.get_maxKeySize())
            throw new KeyTooLongException(null, "");

        if (key instanceof StringKey) {
            if (headerPage.get_keyType() != AttrType.attrString) {
                throw new KeyNotMatchException(null, "");
            }
        } else if (key instanceof IntegerKey) {
            if (headerPage.get_keyType() != AttrType.attrInteger) {
                throw new KeyNotMatchException(null, "");
            }
        } else
            throw new KeyNotMatchException(null, "");

        // Pranav Iyer
        // THREE CASES:
        // 1. The split pointer is before the bucket page the
        // record is hashed to and it has space so we insert the record in there

        // 2. The split pointer is before the bucket page the
        // record is hashed to and it doesn't have space so we create an
        // overflow page and insert the record in there

        // 3. The split pointer is after the bucket page, so we apply hash_level+1
        // and check which page the record should lie in

        if (trace != null) {
            trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
            trace.writeBytes("DO" + lineSep);
            trace.flush();
        }

        if (max_bucket_key_value < cur_bucket_key_value + 1) {
            // Max utilization is exceeded
            // We need to rehash!
            HashLeafPage new_bucket_page = null;
            try{ new_bucket_page = new HashLeafPage(headerPage.get_keyType());}
            catch(Exception e){
                e.printStackTrace();}

            PageId new_bucket_page_id = new_bucket_page.getCurPage();
            new_bucket_page.setNextPage(new PageId(INVALID_PAGE));
            new_bucket_page.setPrevPage(new PageId(INVALID_PAGE));

            if (new_bucket_page_id.pid < 0) {
                throw new InsertException();
            }

            try {
                // t.setStrFld(1, Integer.toString(new_bucket_page_id.pid)); // Insert the
                // bucketpageid to disk
                bucket_location.add(new_bucket_page_id);
                latest_inserted_datapage.add(latest_inserted_datapage.get(split_ptr_loc + 1));
                // Tuple copy_t = new Tuple(t);
                // headerPage.insertRecord(copy_t.getTupleByteArray());
                unpinPage(new_bucket_page_id, true);
                max_bucket_key_value += max_per_page;
                rehash(bucket_location.get(split_ptr_loc + 1), new_bucket_page_id, true /* firsttime */);

            } catch (Exception e) {
                e.printStackTrace();
            }

            split_ptr_loc++;
            if (split_ptr_loc == (int) Math.pow(2, h_0 + level) - 1) {
                level++;
                // System.out.println("*********************CURRENT LEVEL IS "+level+"
                // *************************");
                // max_bucket_key_value = (int) (Math.floor(max_per_page*utilization*
                // Math.pow(2,h_0+level)));
                split_ptr_loc = -1;
            }
        }

        // Get the hash value of a record based on an attribute
        int hash_value = hash_record(key);
        // System.out.println("The hash of key currently being inserted is "+
        // hash_value);
        int bucket_to_insert = hash_value % (int) Math.pow(2, h_0 + level);
        if (bucket_to_insert <= split_ptr_loc)
            bucket_to_insert = hash_value % (int) Math.pow(2, h_0 + level + 1);
        PageId bucket_pid = bucket_location.get(bucket_to_insert);

        _insert(key, rid, bucket_pid);

    }

    private PageId _insert(KeyClass key, RID rid, PageId currentPageId)
            throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
            DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
            IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException

    {

        // Pin the page and insert into the page
        HashLeafPage bucketPage = null;
        try {
            bucketPage = new HashLeafPage(currentPageId, headerPage.get_keyType());
        } catch (Exception e) {
            e.printStackTrace();
        }

        RID inserted_record_rid = bucketPage.insertRecord(key, rid);

        if (inserted_record_rid == null) {
            // This means the page doesn't have enough space, now we'll create the overflow
            // page

            /// CHECK FOR UNPIN ERRORS!!!!!

            HashLeafPage presentPage = bucketPage;
            HashLeafPage overflow_page = null;

            while (true) {
                PageId overflow_pageid = presentPage.getNextPage();

                if (overflow_pageid.pid == INVALID_PAGE) {
                    overflow_page = new HashLeafPage(headerPage.get_keyType());
                    overflow_page.setNextPage(new PageId(INVALID_PAGE));
                    overflow_pageid = overflow_page.getCurPage();
                    overflow_page.setPrevPage(presentPage.getCurPage());
                    presentPage.setNextPage(overflow_page.getCurPage());
                    RID insert_rid = overflow_page.insertRecord(key, rid);
                    if (insert_rid == null) {
                        // We are unable to insert despite creating a new page w/ empty space
                        unpinPage(presentPage.getCurPage(), true);
                        throw new LeafInsertRecException();
                    }

                    unpinPage(overflow_page.getCurPage(), true);
                    unpinPage(presentPage.getCurPage(), true); // This page is not dirty
                    return overflow_pageid;
                }

                else {
                    try {
                        overflow_page = new HashLeafPage(overflow_pageid, headerPage.get_keyType());
                        RID insert_rid = overflow_page.insertRecord(key, rid);
                        // unpinPage(overflow_page.getCurPage(), true);
                        if (insert_rid != null) {
                            unpinPage(presentPage.getCurPage(), false);
                            unpinPage(overflow_page.getCurPage(), true);
                            return overflow_pageid;
                        }
                        // System.out.println("We have an issue here!!!!!!!!");
                        unpinPage(presentPage.getCurPage());
                        unpinPage(overflow_page.getCurPage(), true);
                        presentPage = new HashLeafPage(overflow_pageid, headerPage.get_keyType());
                    }

                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        unpinPage(currentPageId);
        cur_bucket_key_value++;
        return currentPageId;

    }

    public boolean Delete(KeyClass key, RID rid)
            throws  DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
            RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
            ConstructPageException, DeleteRecException, IndexSearchException, IOException
    {

        ArrayList <KeyDataEntry> toBeDeleted = new ArrayList<KeyDataEntry>();
        int hash_value = hash_record(key);
        int bucket_to_search = hash_value % (int) Math.pow(2, h_0 + level);
        if (bucket_to_search <= split_ptr_loc)
            bucket_to_search = hash_value % (int) Math.pow(2, h_0 + level + 1);
        PageId bucket_pid = bucket_location.get(bucket_to_search);
        HashLeafPage current_page = null;
        KeyDataEntry temp_entry;
        RID temp_rid = new RID();
        int delete_count=0;
        try
        {  current_page = new HashLeafPage(bucket_pid, headerPage.get_keyType());}
        catch(Exception e){ e.printStackTrace();}

        while (true) {
            for (temp_entry = current_page.getFirst(temp_rid); temp_entry != null; temp_entry = current_page
                    .getNext(temp_rid)) {
                if (BT.keyCompare(key, temp_entry.key) == 0) // Check whether the keys are equal
                {
                    // get the rid from temp_entry and check whether its rid matches one to be deleted
                    RID tupleRidInDataFile = ((LeafData) (temp_entry.data)).getData();
                    if(tupleRidInDataFile.equals(rid))
                        toBeDeleted.add(temp_entry);
                }
            }
            for (int i = 0; i < toBeDeleted.size(); i++) {
                temp_entry = toBeDeleted.get(i);
                current_page.delEntry(temp_entry);
            }

            PageId overflow_pageid = current_page.getNextPage();
            if(delete_count>0) unpinPage(current_page.getCurPage(), true);
            else unpinPage(current_page.getCurPage());

            if (overflow_pageid.pid == INVALID_PAGE)
            {
                if(delete_count==0)
                    return false;

                return true;
            }

            try{current_page = new HashLeafPage(overflow_pageid, headerPage.get_keyType());}
            catch(Exception e){e.printStackTrace();}
        }

    }




    /**
     * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry; it
     * is not the id of the data entry)
     *
     * @param key the key in pair <key, rid>. Input Parameter.
     * @param //rid the rid in pair <key, rid>. Input Parameter.
     * @return true if deleted. false if no such record.
     * @exception DeleteFashionException    neither full delete nor naive delete
     * @exception LeafRedistributeException redistribution error in leaf pages
     * @exception RedistributeException     redistribution error in index pages
     * @exception InsertRecException        error when insert in index page
     * @exception KeyNotMatchException      key is neither integer key nor string
     *                                      key
     * @exception UnpinPageException        error when unpin a page
     * @exception IndexInsertRecException   error when insert in index page
     * @exception FreePageException         error in BT page constructor
     * @exception RecordNotFoundException   error delete a record in a BT page
     * @exception PinPageException          error when pin a page
     * @exception IndexFullDeleteException  fill delete error
     * @exception LeafDeleteException       delete error in leaf page
     * @exception IteratorException         iterator error
     * @exception ConstructPageException    error in BT page constructor
     * @exception DeleteRecException        error when delete in index page
     * @exception IndexSearchException      error in search in index pages
     * @exception IOException               error from the lower layer
     * @throws InvalidTupleSizeException
     * @throws BufMgrException
     * @throws PagePinnedException
     * @throws BufferPoolExceededException
     * @throws PageNotReadException
     * @throws InvalidFrameNumberException
     * @throws PageUnpinnedException
     * @throws HashOperationException
     * @throws ReplacerException
     *
     */
    public ArrayList search(KeyClass key)
            throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
            RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
            ConstructPageException, DeleteRecException, IndexSearchException, IOException, InvalidTupleSizeException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException
    {

        ArrayList<RID> pidInDataFile = new ArrayList<RID>();
        int hash_value = hash_record(key);
        int bucket_to_search = hash_value % (int) Math.pow(2, h_0 + level);
        if (bucket_to_search <= split_ptr_loc)
            bucket_to_search = hash_value % (int) Math.pow(2, h_0 + level + 1);
        PageId bucket_pid = bucket_location.get(bucket_to_search);
        HashLeafPage bucket_page_1 = new HashLeafPage(bucket_pid, headerPage.get_keyType());
        KeyDataEntry temp_entry;
        RID temp_rid = new RID();

        while (true) {
            for (temp_entry = bucket_page_1.getFirst(temp_rid); temp_entry != null; temp_entry = bucket_page_1
                    .getNext(temp_rid)) {
                if (BT.keyCompare(key, temp_entry.key) == 0) // Check whether the keys are equal
                {
                    // get the rid from temp_entry and access the record to check whether the tuple
                    // equals the one to be deleted
                    RID tupleRidInDataFile = ((LeafData) (temp_entry.data)).getData();
                    pidInDataFile.add(new RID(new PageId(tupleRidInDataFile.pageNo.pid), tupleRidInDataFile.slotNo));
                }
            }
            PageId overflow_pageid = bucket_page_1.getNextPage();
            try{unpinPage(bucket_page_1.getCurPage(), true);}
            catch(Exception e)
            {e.printStackTrace();}

            if (overflow_pageid.pid == INVALID_PAGE)
            {
                if(pidInDataFile.size()==0)
                    return null;

                return pidInDataFile;
            }

            bucket_page_1 = new HashLeafPage(overflow_pageid, headerPage.get_keyType());
        }
    }

    void print_bucket(PageId bucket_id_1) {
        HashLeafPage bucket_page_1;
        try {
            bucket_page_1 = new HashLeafPage(bucket_id_1, headerPage.get_keyType());

            KeyDataEntry temp_entry = null;
            KeyClass temp_key;
            RID temp_rid = new RID();

            int i = 0;

            for (temp_entry = bucket_page_1.getFirst(temp_rid); temp_entry != null; temp_entry = bucket_page_1
                    .getNext(temp_rid)) {
                temp_key = temp_entry.key;

                if (headerPage.get_keyType() == AttrType.attrInteger)
                    System.out.println(i + " (key, pageId, hashvalue):   (" + (IntegerKey) temp_entry.key + ",  "
                            + (LeafData) temp_entry.data + " )");
                if (headerPage.get_keyType() == AttrType.attrString)
                {
                    StringKey s = (StringKey) temp_key;
                    System.out.println(i + " (key, pageId, hashvalue):   (" + s.toString() + ",  "
                            + (LeafData) temp_entry.data + ",  " + hash_record(temp_key) + " )");
                }

                i++;
            }

            PageId overflow_page = bucket_page_1.getNextPage();
            unpinPage(bucket_id_1);

            if (overflow_page.pid == INVALID_PAGE) {
                // No overflow pages, we are done!

                return;
            }

            print_bucket(overflow_page);

        }

        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void print_all() {
        System.out.println("\n Current Level is \n" + level);
        System.out.println("Split pointer location is \n" + split_ptr_loc);

        for (int i = 0; i < bucket_location.size(); i++) {
            PageId bucket_id = bucket_location.get(i);
            System.out.println("Printing Bucket: " + i + "\n");
            print_bucket(bucket_id);
        }
    }

}
