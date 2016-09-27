/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbstrcture;

import dsapara.*;
import java.util.ArrayList;
import java.util.HashMap;
//import gnu.trove.map.hash;
/**
 *
 * @author ZhangJiangwei
 */
public class Table {
   public int[][] fks; //exclude pk
   public String[] nonKeys;
 
   public String tableName;
   public int tuplesize; //fk size
}
