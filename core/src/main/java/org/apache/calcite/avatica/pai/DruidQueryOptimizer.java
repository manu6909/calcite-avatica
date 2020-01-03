/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.calcite.avatica.pai;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public  class DruidQueryOptimizer {
    public static String changeQuery(String mainQuery){
        Select statement = null;

        Map<String,String> outerAliasMap = null;

        try {
            statement = (Select) CCJSqlParserUtil.parse(mainQuery);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        PlainSelect plainSelect = (PlainSelect) statement.getSelectBody();
        outerAliasMap = getOuterAliasMap(plainSelect);


        PlainSelect plainInnerQuery = getInnerQuery(plainSelect);
        if ( plainInnerQuery == null){
            return mainQuery;
        }else{
            // Handling second level in case we have queries inside FROM clause.
            FromItem from = plainInnerQuery.getFromItem();
            String inQuery = from.toString().split(from.getAlias().toString())[0];
            plainInnerQuery = handleSecondLevel(inQuery) != null? handleSecondLevel(inQuery):plainInnerQuery;
            return changeAlias(plainInnerQuery,outerAliasMap).toString();
        }
    }

    /**
     * Check wheather the from clause having inner query by parsing it and checking for exceptions.
     * @param sql
     * @return
     */
    public static PlainSelect handleSecondLevel(String sql){
        Select newmt =null;
        try {
            newmt = (Select) CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            return null;
        }
        return (PlainSelect)newmt.getSelectBody();

    }
    /**
     * Utility function to create Map of select column as keys and values as alias names.
     * @param query
     * @return
     */
    public static Map<String,String> getOuterAliasMap(PlainSelect query){
        Map<String,String>selectAliasMap = new HashMap<String, String>();
        for (SelectItem item:query.getSelectItems()
        ) {
            SelectExpressionItem sei = (SelectExpressionItem) item;
            selectAliasMap.put(sei.getExpression().toString(),sei.getAlias().getName());

        }
        return selectAliasMap;
    }

    /**
     * Function to get inner query from main query if join is found.Only handles one Join.
     * @param query
     * @return
     */
    public static PlainSelect getInnerQuery(PlainSelect query){
        List<Join> joins = query.getJoins();
        Select innerQuery = null;
        if ( joins != null) {
            try {
                for (Join jo : joins
                ) {
                    String[] arrays = jo.getRightItem().toString().split(jo.getRightItem().getAlias().toString());
                    innerQuery = (Select) CCJSqlParserUtil.parse(arrays[0]);
                }
            } catch (JSQLParserException e) {
                e.printStackTrace();
                return null;
            }
        }
        return  (innerQuery == null) ? null : (PlainSelect) innerQuery.getSelectBody();
    }

    public static PlainSelect changeAlias(PlainSelect query,Map<String,String> outerAliasMap){
        for (SelectItem item:query.getSelectItems()
        ) {
            SelectExpressionItem sei = (SelectExpressionItem) item;
            if(outerAliasMap.containsKey(sei.getExpression().toString()))
                sei.setAlias(new Alias(outerAliasMap.get(sei.getExpression().toString())));
        }
        return query;

    }
}
