This tool implements the algorithm of GScaler Algorithm published in VLDB2016.
http://www.comp.nus.edu.sg/~upsizer/dscaler.pdf
 
It is implemented using Netbeans IDE.

The programme only works for relational database. All files must be stored in one dir.
Assuming we have four table, A.txt, B.txt, C.txt, D.txt stored in the dir testDB/
We first need to convert the keys to integers for efficiency purpose. 

====================================Preparation===============================================
We need to prepare a configuration file, called inputDB/config.txt

Assuming 
1. C references A and B, and D references C. 
2. C is UNIQUE(the foreign key pairs does not repeat)

The configuration will look like the following:

A #numberOfFKs #numberOfTuples Uniqness
a_pk a_fk1 a_fk2 nonkeys
B #numberOfFKs #numberOfTuples ...
......

In our example, A's pk is call "id", A has 0 FKs, A has 4 tuples, A is not UNIQUE.
Hence, it will look like the following:
A 0 4 false
id nonkeys

For C, C's pk is "id", C has 2 FKs, C has 4 tuples, C is UNIQUE. Then it will look like

C 2 4 true
id A_id-A:id B_id:B-id nonkeys

Before "-" is the attribute name in C,"A_id"
Between "-" and ":" is the referenced table for "A_id", "A"
After ":" is the referenced attribute name in the reference table. 

For example, config.txt might look like the following:
A	0	4	0
id	nonKeys
B	0	4	0
id	nonKeys
C	2	4	true
id	A_id-A:id	B_id-B:id	nonKeys
D	1	4	0
id	C_id-C:id	nonKeys



=============================================Scaling=============================================
To scale DB, you can do a uniform scaling (all tables scale by the same ratio):

java -jar -Xmx50g DscalerSource.jar -i inputDB/ -o output/ -static 0.1

Or, you can scale the table non-uniformly (specify the scaled table size). 
To specify the tablesize, you need to have inputDB/scaleTable.txt which indicates the scaled table size.
The format is the following:

#scaledSize A
#scaledSize B
....

By having that, we can scale the database by using the following command:

java -jar Dscaler.jar -i inputDB/ -o output/ -dynamic inputDB/scaleTable.txt 


*** -dynamic inputDB/scaleTable.txt  tells the location of the scaling file. 
*** config.txt must always be stored in the input folder



Please feel free to contact a0054808@u.nus.edu.
