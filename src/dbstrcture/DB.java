package dbstrcture;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.HashMap;

/**
 *
 * @author ZhangJiangwei
 */
public class DB {
    public Table[] tables;
    public HashMap<String, Integer> tableMapping = new HashMap<>();
    
    public int getTableNum(String table){
        return tableMapping.get(table);
    }

    
    public void dropFKs() {
        for (Table t : tables) t.fks = null;
    }
}
