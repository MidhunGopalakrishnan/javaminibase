package global;

import java.io.Serializable;
import java.util.ArrayList;

public class TableMetadata implements Serializable {
    public String tableName;
    public AttrType[] attrType;
    public short[] attrSize;
    public ArrayList<String> indexNameList = new ArrayList<>();

    public short[] getAttrSize() {
        return attrSize;
    }

    public void setAttrSize(short[] attrSize) {
        this.attrSize = attrSize;
    }

    public TableMetadata(String tableName, AttrType[] attrType, short[] attrSize) {
        this.tableName = tableName;
        this.attrType = attrType;
        this.attrSize = attrSize;
    }

    public ArrayList<String> getIndexNameList() {
        return indexNameList;
    }

    public void setIndexNameList(ArrayList<String> indexNameList) {
        this.indexNameList = indexNameList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public AttrType[] getAttrType() {
        return attrType;
    }

    public void setAttrType(AttrType[] attrType) {
        this.attrType = attrType;
    }
}