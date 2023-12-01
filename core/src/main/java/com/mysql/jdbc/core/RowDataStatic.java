package com.mysql.jdbc.core;

import java.util.ArrayList;

/**
 * @author hjx
 */
public class RowDataStatic implements RowData {
    ArrayList<ResultSetRow> rows;
    Field[] metadata;
    private int index;

    public RowDataStatic(ArrayList<ResultSetRow> rows) {
        this.rows = rows;
        this.index = -1;
    }

    @Override
    public void setMetadata(Field[] metadata) {
        this.metadata = metadata;
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public ResultSetRow next() {
        this.index++;
        if (this.index < this.rows.size()) {
            ResultSetRow row = (ResultSetRow) this.rows.get(this.index);
            return row.setMetadata(this.metadata);
        }
        return null;
    }

    @Override
    public boolean isBeforeFirst() {
        return (this.index == -1) && (this.size() != 0);
    }

    @Override
    public boolean isAfterLast() {
        return this.index >= this.rows.size();
    }

    @Override
    public boolean isFirst() {
        return this.index == 0;
    }

    @Override
    public boolean isLast() {
        if (this.rows.size() == 0) {
            return false;
        }
        return this.index == (this.size() - 1);
    }

    @Override
    public void beforeFirst() {
        this.index = -1;
    }

    @Override
    public void afterLast() {
        this.index = this.size();
    }

    @Override
    public boolean isEmpty() {
        return this.rows.size() == 0;
    }

    @Override
    public void beforeLast() {
        this.index = this.size() - 2;
    }

    @Override
    public int getCurrentRowNumber() {
        return this.index;
    }
}
