#**Pocket_ETL** 
[**ETL**][WP] in PL/SQL, the easy way.  

**Pocket_BI** is a PL/SQL Business Intelligence (**BI**) framework with the components: 

* [Pocket_ETL](https://github.com/HelderVelez/Pocket_ETL)  *Extraction,here documented*
* [Pocket_Storage](https://github.com/HelderVelez/Pocket_Storage) *Transform and Load* 
* Pocket_Reports, to be documented later

The Pocket_BI framework has the ability to update and be updated from a **Couch_DB** database to provide an exterior backup and a **Web frontend**.   

Pocket_ETL in short:
:   Write one line to define the extraction from one table
:   Write one line to *import* from a group of tables
:   Write one line to *launch* a job 

   
In TI, and in BI (Business Intelligence), there is the constant need to extract  (<b>E</b>TL) information from the *source tables* in the the working environment, where the contents of the tables are in evolution due to the interactions of the users and batch jobs, to the *target tables* frequently in others schemas and sites.  
This application is generic and it is coded in PL/SQL. Both the sources and the targets are oracle tables.  

In the **Pocket_Storage** document, following this one, a strongly opinionated view on the <b>T</b>ransformation/<b>L</b>oading (**TL**) phase will be detailed.  

**Pocket_ETL** does the <b>E</b>xtraction phase - in the BI parlance is equivalent to populate the *stagging area* with  snapshots of information.  

The table WORK *is the interface.*

- [Pocket_ETL](#pocketetl)
    - [TABLES](#tables)
        - [**WORK** - drives the Data Imports and the Job Launcher, via trigger](#work---drives-the-data-imports-and-the-job-launcher-via-trigger)
        - [TLOG - the log record](#tlog---the-log-record)
    - [PACKAGES](#packages)
        - [KJOBS - launches the jobs of data import (ETL)](#kjobs---launches-the-jobs-of-data-import-etl)
        - [KETL main package of ETL](#ketl-main-package-of-etl)
        - [KETL2 user code](#ketl2-user-code)
        - [KPBI  user code](#kpbi--user-code)
    - [List of objects](#list-of-objects)
    - [Notes](#notes)
    - [**Example**](#example)

##**TABLES**
###**WORK** - drives the Data Imports and the Job Launcher, via trigger  
>|Fields|Type|Constraint|Use|
|-----|-----|--------|-------|
|work_id|number()|not null|primary key, autofilled by the sequence SWORK
|work_nota|varchar2()|not null|source table name; or `IMPORT`: start the import job; or  `GO_JOB` start job named in WORK_PROC
|work_proc|varchar2()|not null|process name; ex. `IMP1`  as import name or `p_doc_daily` as a job name - define it in KPBI
|work_wher|varchar2()||null or *where* clause; ex: `` where ipgVIDE_chav in (76,77,80) and IPGVICT_stat='F'``
|work_etl2|varchar2()||do this ``<procedure or sql block>`` after import conclusion (define it in KETL2). Ex: ``Ketl2.etl2_tdocdocu``  or  ``update VMAP set VMAP_sql = replace(VMAP_sql, 'IPG','') where VIDE_ID in (76,77,80)``
|work_meto|varchar2()||method of import; DC - drop/create table;TI - truncate/insert into table; ID - merge - preverves existing data outside the range WORK_WHER
|work_inx|varchar2()||control creation of indexes: N - dont create indexes, idem when _METO=ID
|work_flds|varchar2(4)|not null|`*` or List of field names as in the ext_DB table with optional  alias. Use `,` as sep
|work_newn|varchar2()||New table Name, if null defaults to TABS_NOTA
|**Minor Fields**||||
|work_orde|number(1)||order of import whitin processName in WORK_PROC
|work_done|varchar2()||shows: N before import, D after data import, DI after creation of indexes
|work_daac|date(7)||timestamp of last change of WORK_DONE
|work_wait|number(1)||number of seconds to wait before the start of the import
|work_noat|varchar2()||schema of source table, not implemented ( in use dbLink `EXT_DB``)
|work_neat|varchar2()||schema of target  table, not implemented
|**special fields**|||
|type|varchar2(6)||type of record = `work`
|id|varchar2()||couchdb _ID
|rev|varchar2()||couchdb _REV
|status|varchar2(5)||`OK`, `ERROR`, `TO_VALIDATE`, ...
|tags|varchar2(8)||[tags, comma sep]
|time_stamp|varchar2()||when updated filled via trigger
|msg|varchar2()||a message
|user_id|number(2)|not null|default 1 - User POCKET_BI
 |***keys***|
|work_pk|primary key| |  work_id 
|work_uk|unique key| | work_nota, work_proc
|work_users_fk| foreign key||user_id --&lt;users.user_id 
|***triggers***||| 
|gwork_exe|trigger ||`IMPORT`and `GO_JOB`invokes KJOBS
|gwork|trigger ||for frontend control. Interface with couchdb. In the absence of this it should be deactivated (comment code from `NOTSYNC` to `NONOTSYNC`). It uses KCOUCH package, TSYNC table, etc... ||||


###TLOG - the log record
**todo** - must include an importance level to the message (Err,Warn,DataCheck, ...) and a number field for DataCheck 
>|Fields|Type|Constraint|Use|
|-----|-----|--------|-------|
|**tlog_id**|number|key|auto||
|tlog_proc|varchar2(30)|not null|process_key
|tlog_msg|varchar2(4000)|not null|the message|
|cdb_db|varchar2()||used in the sync _changes
|id|varchar2()||couchdb _id
|rev|varchar2()||couchdb _rev
|**special fields**|||
|tags|varchar2(8)||[tags, comma sep]
|time_stamp|varchar2()||when updated, via trigger
|tprocess|varchar2()||used in the sync after process in ora
|type|varchar2(6)||type of record
|user_id|number(2)|not null|default 1 - user pocket_bi
 |***keys***|
|tlog_pk|primary key| |  tlog_id 
|tlog_user_fk| foreign key||user_id --&lt;users.user_id 
|**triggers**|||
|gtlog|||

##**PACKAGES**
###**KJOBS** - launches the jobs of data import (ETL) 
It is invoked by the `on insert` triggers of WORK in response to `IMPORT` or `GO_JOB` in WORK_NOTA  and waits for conclusion
>* **import_data_now**(importName), where importName is the content of WORK_PROC 
>* **go_job**(jobName), jobName is the content of WORK_PROC 
###**KETL** main package of ETL 
>* **exec_job**(jobName) -- launches the *fully qualified* jobName, eventually defined inside the package KPBI, ex:  'KPBI.p_appx(sysdate)'
>* **copy_source_data**(processName),  
>* **wait_ended**('process1, process2,...', sleep_seconds), wait until all listed processes write END/ENDED or ABORTED in the log 
>* **wait_job_end**('process1, process2,...', sleep_seconds), wait until conclusion of all listed jobs 
>* create_indexes(tableName), internal 
>* execute_etl2(processName), execute the WORK_ETL2(processName), internal, 
>*    p_tlog(proc,msg ,user_id )

>* make_table_equal_to_ext_db(tableName) , internal, not in use
###**KETL2** user code
suggested package name for the procedures invoked in WORK_ETL2
###**KPBI**  user code 
suggested package name for user code for transformations/loading, named in `GO_JOB` jobName


----------


##**List of objects**

|Type|Name|Obs||
|:--|:--|:--|:--|
|DbLink|EXT_DB|DATABASE LINK <br> to external DB|
|Type|t_str
|Type|t_str_table
|Type|t_2str
|Type|t_2str_table
|Procedure| pcompile
|Function|create_link_to_extdb(<br>'POCKET_BI','POCKET_BI','PBI_SA','XE')
|Function|split
|Function|split2
|Sequence|swork
|Sequence|stlog
|Table|work
|Table|tlog
|Table|users
|Package|kjobs
|Package|ketl
|Package|ketl2|user code
|Package|kpbi|user code
|Trigger|gwork|commented out NOTSYNC
|Trigger|gwork_exe
|Trigger|gtlog
|File|Pocket_ETL_0.sql|installation, adapt dblink
|File|Pocket_ETL_1.sql|installation
|File|user_Pocket_BI.md| create the user (Priviledges)
|File|README.md|*this* document



##Notes  
>*  the source tables must reside in the schema pointed to the dbLink **EXT_DB**  
>*  the target tables will reside in the schema were Pocket_ETL is installed,  
(for consistency with the overall application lets say it resides in Pocket_BI)
>*  **commit before (and after) the inserts of `IMPORT` and `GO_JOB`**
>* If  something went wrong and the the jobs were abnormally stopped you must delete the records with the corresponding IMPORT/GO_JOB before relaunch
>* the Ketl.make_table_equal_to_ext_db(tableName) was deactivated after the introduction of the _FLDS feature. It was used to create/drops fields and made all the table definition and indexes the same as in the source table. 
>* must have access to:  dba_scheduler_running_jobs, dbms_lock.sleep() 
 

----------

```
 -- install  
mkdir Pocket_ETL  
cd Pocket_ETL  
-- extract the sql files here  
sqlplus <user>/<password>@connect_identifier 
@Pocket_ETL_0  
@Pocket_ETL_1    
```
```
--uninstall
...
@Pocket_ETL_uninstal
```

##**Example**   
look inside of KPBI.p_doc_daily, invoked by a cron schedule, where IMP0, IMP3 and IMP4 are executed in parallel, waited for the conclusion --  'END' in TLOG-- and start the transformation jobs defined in the package Kdoc;  

```
/* define the imports */
insert into work (WORK_NOTA, WORK_PROC,WORK_METO) values ('ASSU','IMP3','TI');
insert into work (WORK_NOTA, WORK_PROC,WORK_METO) values ('USERS','IMP3','TI');
insert into work (WORK_NOTA, WORK_PROC,WORK_METO) values ('DOCU','IMP4','TI');
commit; /* important: commit before the next inserts */
```

```
/* launch the imports, (IMP3 runs in parallel with IMP4) */
insert into work (WORK_NOTA, WORK_PROC) values ('IMPORT','IMP3');
insert into work (WORK_NOTA, WORK_PROC) values ('IMPORT','IMP4');
commit; 
```
upon the successful completion of the IMPORT this record is deleted
```
/* wait the conclusion */
/* see the log: select * from TLOG order by 1 desc;*/
/* see evolution: select * from work where WORK_PROC in ('IMP3','IMP4');*/
/*and now we WAIT, ok?*/
kjobs.wait_ended('IMP3, IMP4', 30);
```
```
/* launch transformation/load jobs */
insert into work (WORK_NOTA, WORK_PROC) values ('GO_JOB','p_doc_daily');
commit;
```
upon completion of the job, the GO_JOB line is deleted



   
```
/* sql to extract columns metadata in md-extra format: 
   replace &tableName */  
select -1 as col_id ,  '|&tableName|&tableName||||' from dual 
union select  00, ' |:-----|:-----|:-----|:-----|:-------|:--|'  
     as "||Fields|Type|Constraint|Use|" from dual  
union 
select * from (
  select COLUMN_ID, lower('||'||tc.column_name||'|'||data_type||'('||default_length||')'||'|'||  
          decode(nullable,'Y','','not null')||'|') || trim(cc.comments)
      from user_tab_columns tc , user_col_comments cc  
      where tc.column_name = cc.column_name  
      and cc.table_name=tc.TABLE_NAME  
      and tc.table_name= '&tableName'
    order by COLUMN_ID   
)    
      union select 96,'||**special fields**||CouchDB related||' from dual 
union select 97,'||**keys**|'   from dual 
union select 98,'||**triggers**|||' from dual  
union select 99,'|||||' from dual
;

```


[**MIT** License](http://opensource.org/licenses/mit-license.php)  
&copy;  2013, Helder Velez.  

[WP]:(http://en.wikipedia.org/wiki/Extract,_transform,_load "WP")

> Written with [StackEdit](http://benweet.github.io/stackedit/).
