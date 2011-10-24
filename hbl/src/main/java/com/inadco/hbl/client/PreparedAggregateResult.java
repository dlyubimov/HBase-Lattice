package com.inadco.hbl.client;

public interface PreparedAggregateResult extends AggregateResult {

    /**
     * get result by alias
     * 
     * @param alias
     *            select expression alias
     * @return query select expression result
     * @throws HblException
     */
    Object getObject(String alias) throws HblException;

    /**
     * get result by index
     * 
     * @param index
     *            0-based index of expression in the select expression list
     * @return
     * @throws HblException
     */
    Object getObject(int index) throws HblException;

}
