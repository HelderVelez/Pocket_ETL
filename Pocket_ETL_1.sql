----------------------------------------------
-- Export file for user POCKET_BI           --
-- Created by Velez on 2013-10-20, 02:15:44 --
----------------------------------------------

spool Pocket_ETL_1.log


---- conditional compilation on CouchdbON
alter session set PLSQL_CCFlags = 'CoucdbON:0'


prompt
prompt Creating table USERS
prompt ====================
prompt
create table USERS
(
  USER_ID    NUMBER not null,
  IDEPT_ID   NUMBER,
  USER_SIGLA VARCHAR2(32) default 'pocket_bi' not null,
  USER_NAME  VARCHAR2(32) not null,
  USER_EMAIL VARCHAR2(100),
  TYPE       VARCHAR2(32) default 'users',
  TIME_STAMP VARCHAR2(24),
  REV        VARCHAR2(40),
  ID         VARCHAR2(40),
  TAGS       VARCHAR2(2000) default '[ admin, users ]',
  STATUS     VARCHAR2(10) default 'OK',
  MSG        VARCHAR2(1000),
  USER_PWD   VARCHAR2(32)
)
;
comment on table USERS
  is 'user identification';
comment on column USERS.USER_ID
  is 'user KEY ';
comment on column USERS.USER_SIGLA
  is 'user SIGLA';
comment on column USERS.TIME_STAMP
  is 'when updated';
comment on column USERS.STATUS
  is 'OK, ERROR, TO_VALIDATE,';
comment on column USERS.USER_PWD
  is 'password';
alter table USERS
  add constraint USERS_PK primary key (USER_ID);
alter table USERS
  add constraint USERS_UK unique (USER_SIGLA, IDEPT_ID);
alter table USERS
  add constraint USERS_IDEPT_FK foreign key (IDEPT_ID)
  references IDEPT (IDEPT_ID);

prompt
prompt Creating table TLOG
prompt ===================
prompt
create table TLOG
(
  TLOG_ID    NUMBER not null,
  TLOG_PROC  VARCHAR2(30) not null,
  TLOG_MSG   VARCHAR2(4000) not null,
  CDB_DB     VARCHAR2(60),
  TPROCESS   VARCHAR2(60),
  TYPE       VARCHAR2(32) default 'tlog',
  TIME_STAMP VARCHAR2(24),
  USER_ID    NUMBER default 1 not null,
  REV        VARCHAR2(40),
  ID         VARCHAR2(40),
  TAGS       VARCHAR2(2000) default '[   ]'
)
;
comment on table TLOG
  is 'the LOG record';
comment on column TLOG.TLOG_ID
  is 'autofilled with sequence STLOG';
comment on column TLOG.TLOG_PROC
  is 'job reference';
comment on column TLOG.TLOG_MSG
  is 'output message';
comment on column TLOG.CDB_DB
  is 'used in the sync _changes';
comment on column TLOG.TPROCESS
  is 'used in the sync after process in ORA';
comment on column TLOG.TYPE
  is 'type of record';
comment on column TLOG.TIME_STAMP
  is 'when updated, via trigger';
comment on column TLOG.USER_ID
  is 'default 1 - User POCKET_BI';
comment on column TLOG.REV
  is 'couchdb _rev';
comment on column TLOG.ID
  is 'couchdb _id';
comment on column TLOG.TAGS
  is '[tags, comma sep]';
alter table TLOG
  add constraint TLOG_PK primary key (TLOG_ID);
alter table TLOG
  add constraint TLOG_USER_FK foreign key (USER_ID)
  references USERS (USER_ID);

prompt
prompt Creating table WORK
prompt ===================
prompt
create table WORK
(
  WORK_ID    NUMBER not null,
  WORK_NOTA  VARCHAR2(32) not null,
  WORK_PROC  VARCHAR2(100) not null,
  WORK_WHER  VARCHAR2(4000),
  WORK_ORDE  NUMBER default 0,
  WORK_DONE  VARCHAR2(2),
  WORK_DAAC  DATE default sysdate,
  WORK_ETL2  VARCHAR2(4000),
  WORK_METO  VARCHAR2(2),
  WORK_INX   VARCHAR2(2),
  WORK_WAIT  NUMBER default 0,
  WORK_FLDS  VARCHAR2(4000) default '*' not null,
  WORK_NEWN  VARCHAR2(32),
  TYPE       VARCHAR2(32) default 'work',
  TIME_STAMP VARCHAR2(24),
  USER_ID    NUMBER default 1 not null,
  REV        VARCHAR2(40),
  ID         VARCHAR2(40),
  TAGS       VARCHAR2(2000) default '[   ]',
  WORK_NOAT  VARCHAR2(32),
  WORK_NEAT  VARCHAR2(32),
  STATUS     VARCHAR2(10) default 'OK',
  MSG        VARCHAR2(1000)
)
;
comment on table WORK
  is 'ETL table, drives Data Imports and Job Launcher, via trigger';
comment on column WORK.WORK_ID
  is 'primary key, autofilled by the sequence SWORK';
comment on column WORK.WORK_NOTA
  is 'source table name; or `IMPORT`: start the import job; or  `GO_JOB` start job named in WORK_PROC';
comment on column WORK.WORK_PROC
  is 'process name; ex. `IMP1`  as import name or `p_doc_daily` as a job name - define it in KPBI';
comment on column WORK.WORK_WHER
  is 'null or *where* clause; ex: `` where ipgVIDE_chav in (76,77,80) and IPGVICT_stat=''F''``';
comment on column WORK.WORK_ORDE
  is 'order of import whitin processName in WORK_PROC';
comment on column WORK.WORK_DONE
  is 'shows: N before import, D after data import, DI after creation of indexes ';
comment on column WORK.WORK_DAAC
  is 'timestamp of last change of WORK_DONE';
comment on column WORK.WORK_ETL2
  is 'do this ``<procedure or sql block>`` after import conclusion (define it in KETL2). Ex: ``Ketl2.etl2_tdocdocu``  or  ``update VMAP set VMAP_sql = replace(VMAP_sql, ''IPG'','''') where VIDE_ID in (76,77,80)``';
comment on column WORK.WORK_METO
  is 'method of import; DC - drop/create table;TI - truncate/insert into table; ID - merge - preverves existing data outside the range WORK_WHER';
comment on column WORK.WORK_INX
  is 'control creation of indexes: N - dont create indexes, idem when _METO=ID';
comment on column WORK.WORK_WAIT
  is 'number of seconds to wait before the start of the import';
comment on column WORK.WORK_FLDS
  is '`*` or List of field names as in the ext_DB table with optional  alias. Use `,` as sep';
comment on column WORK.WORK_NEWN
  is 'New table Name, if null defaults to TABS_NOTA';
comment on column WORK.TYPE
  is 'type of record = `work`';
comment on column WORK.TIME_STAMP
  is 'when updated filled via trigger';
comment on column WORK.USER_ID
  is 'default 1 - User POCKET_BI';
comment on column WORK.REV
  is 'couchdb _REV';
comment on column WORK.ID
  is 'couchdb _ID';
comment on column WORK.TAGS
  is '[tags, comma sep]';
comment on column WORK.WORK_NOAT
  is 'schema of source table, not implemented ( in use dbLink `EXT_DB``)';
comment on column WORK.WORK_NEAT
  is 'schema of target  table, not implemented';
comment on column WORK.STATUS
  is '`OK`, `ERROR`, `TO_VALIDATE`, ...';
comment on column WORK.MSG
  is 'a message';
alter table WORK
  add constraint WORK primary key (WORK_ID);
alter table WORK
  add constraint WORK_UK unique (WORK_NOTA, WORK_PROC);
alter table WORK
  add constraint WORK_USERS_FK foreign key (USER_ID)
  references USERS (USER_ID);

prompt
prompt Creating package KETL
prompt =====================
prompt
create or replace package KETL is
  /*
  -- Author  : HelderVelez
  -- Created : 2007-08-07 15:40:12
  -- Purpose : Extract data from external oracle tables, usually the operational system.
  --           The table TWORK describe all the operations to be done
  --           this package is invoked via a trigger on TWORK as described bellow
  -- note :
  -- database  LINK "ext_db" must exist owned by POCKET_BI
  -- depends on the visibility of sys.all_tab_columns@ext_db
  
  -- TABS_meto = 'ID' -- merge a selected range as specified in _where
  -- TABS_meto = 'TI' -- truncate_insert , the preferred way
  -- TABS_meto = 'DC' -- drop_create. A compile phase follows if needed, slow.
  
  -- indices are not created if TABS_inx = 'N' or TABS_meto = 'ID'
  
  */

  --  main
  -- parameter p_process must be the full name of a procedure, eventually inside the package KPBI.
  -- Import data
  procedure copy_source_data(p_process IN VARCHAR2); -- parameter IMP1,IMP2,...
  -- launch Job
  procedure exec_job(p_process IN VARCHAR2); -- launch the JOB p_process

  --- housekeeping

  -- internal proc used when 'DC' method and all fields (*) selected
  procedure make_table_equal_to_ext_db(p_tabela IN VARCHAR2); -- DDL
  -- invoked in pkg KJOBS activated by the trigger on insert on TWORK
  --   start the process of named tables import
  -- internal use
  procedure create_indexes(p_process varchar2); -- is used in copy_direct
  -- internal use to perform post-import TWORK
  procedure execute_etl2(p_process varchar2);

  -- wait until 'ENDED' or 'END' or 'ABORTED' in tlog
  -- processes is a comma separated list of processes , ex: 'IMP1, IMP2'
  procedure wait_ended(processes varchar2, sleep_time integer);

  procedure wait_job_end(processes varchar2, sleep_time integer);

  -- replicated from KHOUS for containment
  procedure p_tlog(proc     IN varchar2,
                   msg      in TLOG.TLOG_msg%TYPE,
                   PUSER_ID in number); --prus.User_Id%TYPE);

/*  usage example as invoked in the external db
   -- start the loading from the outside of POCKET_BI  (via a trigger on TWORK)
   --   , a dblink must be defined there !
    insert into TWORK@pbi (TABS_nota,TABS_proc) values ('IMPORT','IMP1');
    insert into TWORK@pbi (TABS_nota,TABS_proc) values ('IMPORT','IMP2');
    commit;

  -- wait for the end of the data transfer
  loop
    dbms_lock.sleep(60);
    select count(*)
      into i
      from TWORK@pbi
     where TABS_nota = 'IMPORT'
       and TABS_proc in ('IMP1', 'IMP2');
    exit when i = 0;
  end loop;

  -- invoke the execution of the desired chains at POCKET_BI
  -- and  KPBI.p_job_* are the procedures to call in the package KPBI (here suggested)
    insert into TWORK@pbi  (TABS_nota,TABS_proc) values ('GO_JOB','KPBI.p_job_a1');
    insert into TWORK@pbi  (TABS_nota,TABS_proc) values ('GO_JOB','KPBI.p_job_a2');
    insert into TWORK@pbi  (TABS_nota,TABS_proc) values ('GO_JOB','KPBI.p_job_a3');
    commit;
 -- to follow the progress of the work_done: null->'D' data, -> DI (indices created)
 -- select * from twork@pbi where work_nota in ( 'IMPORT', 'GO_JOB')
 --        or work_proc in ('IMP1','IMP2') order by 1, 2, 4, 3
 -- view the log
 -- select * from tlog@pbi where tlog_proc in ('IMP1','IMP2')
               or tlog_proc like 'p_job_a%) order by 1 desc;
  */

end;
/

prompt
prompt Creating package KETL2
prompt ======================
prompt
create or replace package Ketl2 is

  -- Author  : Helder Velez
  -- Created : 2007-11-13 19:26:30
  -- Purpose :
  --           post import data massaging
  --           invoke the procedures within WORK ,
  --              field TABS_ETL2 , !! DO NOT USE final ";"     !!
  --                example : Ketl2.trata_tabodaab
  --                example :  update tabley Y set Y.FLDx=Y.FLDx+1
  --
  -- use here the functions that need recompilation at ETL execution time and not inside Ketl

  -- procedure etl2_tdocdocu; -- fill assu_id from tdarasdo
  procedure p_getl2_dummy;

end Ketl2;
/

prompt
prompt Creating package KJOBS
prompt ======================
prompt
create or replace package KJOBS is

  -- Author : Helder Velez
  -- purpose :
  --  launch the jobs of data import (ETL)      

  --  this package is called by the trigger of TWORK
  --   1 - to launch the jobs of data import
  --   2 - to execute a job        

  --  Important note:
  --  AFTER COMPILING THIS PACKAGE :
  --    - RECOMPILE THE TRIGGER GWORK_EXE

  procedure go_job(p_procedure varchar2); -- p_procedure = nome de uma procedure dentro de KPBI
  procedure import_data_now(data_set varchar2); -- data_set = IMP1,2,...  

/*
 see
http://www.oradev.com/dbms_scheduler.jsp

Job scheduling from Oracle 10g with dbms_scheduler
*/

end;
/

prompt
prompt Creating package KPBI
prompt =====================
prompt
create or replace package KPBI is

  /* =====================================================
  Purpose : --  USER JOBS Launcher
            --  stats of PBI
            --  DROP of old data
                        (p_imgDropParM  (DataAte date) );
  --------------------------------------------------------
        : Private
  --------------------------------------------------------
  AUTOR     : HLV - Helder Velez       DATA CRIA«√O: 2001-09-07
  --------------------------------------------------------
  --------------------------------------------------------
  OBS       :
  --------------------------------------------------------
  HIST”RIA  : - AUTOR: HLV               DATA:2001-09-07
                CriaÁ„o.
  
  ======================================================== */

  VIEWDataKey CONSTANT number := 0; -- not used ; key to keep last refer date date (LAST)
  -- PBI stats

  /*
  procedure p_estat_PBI;
  -- procedure p_estat_PBI_all;
 */
/*
  procedure p_DOC_daily; -- called by a daily oracle, or crontab, job
*/
end KPBI;
/

prompt
prompt Creating type T_STR
prompt ===================
prompt
create or replace type t_str is OBJECT( str varchar2(32767))
/

prompt
prompt Creating type T_STR_TABLE
prompt =========================
prompt
create or replace type t_str_table as table of t_str
/

prompt
prompt ===================
prompt
create or replace type t_2str is OBJECT( str1 varchar2(32767), str2 varchar2(32767))
/

prompt
prompt Creating type T_2STR_TABLE
prompt =========================
prompt
create or replace type t_2str_table as table of t_2str
/

prompt
prompt Creating function CREATE_LINK_TO_EXT_DB
prompt =======================================
prompt
CREATE OR REPLACE FUNCTION create_Link_to_ext_db(username_in IN VARCHAR2,
                                                 password_in IN VARCHAR2,
                                                 plinkname   IN VARCHAR2,
                                                 dbstring    IN VARCHAR2 DEFAULT 'XE')
  RETURN BOOLEAN AUTHID CURRENT_USER

  /* verify_user: given a username, password, and connect string,
  || this function returns TRUE if a connection to the database
  || succeeds.
  ||
  || Creates and destroys a database link named 'tmp$' || username_in
  ||
  || Requires Net8; raises -20001 exception if Net8 is not up.
  */

 AS
  dummy    dual.dummy%TYPE;
  linkname VARCHAR2(34) := nvl(plinkname, 'SO');
  -- SUBSTR('tmp$' || username_in, 1, 34);
  retval BOOLEAN := TRUE;

  no_listener EXCEPTION;
  PRAGMA EXCEPTION_INIT(no_listener, -12541);

  PROCEDURE cleanup IS
  BEGIN
    EXECUTE IMMEDIATE 'DROP DATABASE LINK ' || linkname;
    dbms_output.put_line('dblink droped');
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;

BEGIN
  cleanup;
  BEGIN
    dbms_output.put_line('creating dblink ext_db');
    EXECUTE IMMEDIATE 'CREATE DATABASE LINK ' || linkname || ' CONNECT TO ' ||
                      username_in || ' IDENTIFIED BY ' || password_in ||
                      ' USING ''' || dbstring || '''';
    dbms_output.put_line('dblink ext_db created');
    EXECUTE IMMEDIATE 'SELECT dummy FROM dual@' || linkname;
    rollback;
    dbms_output.put_line('user ' || username_in || '@' || dbstring ||
                         ' exists');
  EXCEPTION
    WHEN no_listener THEN
      RAISE_APPLICATION_ERROR(-20001,
                              'Cannot verify user/pass combination because ' ||
                              'this function requires Net8, which is apparently not running.');
      EXECUTE IMMEDIATE 'DROP DATABASE LINK ' || linkname;
    WHEN OTHERS THEN
      EXECUTE IMMEDIATE 'DROP DATABASE LINK ' || linkname;
      dbms_output.put_line('user ' || username_in || '  do NOT exist @' ||
                           dbstring);
      retval := FALSE;
  END;
  -- cleanup;
  RETURN retval;
END;
/

prompt
prompt Creating function SPLIT
prompt =======================
prompt
create or replace function split(p_list varchar2, p_del varchar2 := ',')

  /*
  create or replace type t_str is OBJECT( str varchar2(32767));
  create or replace type t_str_table as table of t_str;

  create or replace type t_2str is OBJECT( str1 varchar2(32767), str2 varchar2(32767));
  create or replace type t_2str_table as table of t_2str;

  select * from table(split( '950106, abs',',' ));
  select * from table(split( '950106 as abs',' as ' ));
  select * from
  (
    select split2(STR,':').str1 head, split2(STR,':').str2 tail
    from (
      select STR from

      table(split( '{"seq":4,"id":"ea6e98ebfe66cca545c2f1be5f00005c","changes":[{"rev":"1-a9980115da4c84756b4629203fc3ac0d"}],"doc":{"_id":"ea6e98ebfe66cca545c2f1be5f00005c","_rev":"1-a9980115da4c84756b4629203fc3ac0d","inst_id":"0","inst_name":"POCKET_BI","inst_country":"PT_pt","inst_address":null,"inst_sigla":"POCKET_BI","type":"inst","time_stamp":"2013-03-09 11:35:49:+00","user_id":"pocket_bi","tags":["defs","inst"]}}',','))
      )
    --where  split2(STR,':').str1 = '"type"'
  )
  --where  head = '"type"'
  ;
  */

 return t_str_table
  pipelined is
  l_idx   pls_integer;
  l_list  varchar2(32767) := p_list;
  l_value varchar2(32767);
  v_str   t_str;
begin
  v_str := NEW t_str('');
  loop
    l_idx := instr(l_list, p_del);
    if l_idx > 0 then
      v_str.str := trim(substr(l_list, 1, l_idx - 1));
      pipe row(v_str);
      l_list    := substr(l_list, l_idx + length(p_del));
      v_str.str := trim(l_list);
    else
      pipe row(v_str);
      exit;
    end if;
  end loop;
  return;
end;
/

prompt
prompt Creating function SPLIT2
prompt ========================
prompt
create or replace function split2(p_list varchar2,
                                  p_del  varchar2 := ',')

  /*
  create or replace type t_str is OBJECT( str varchar2(32767));
  create or replace type t_str_table as table of t_str;

  create or replace type t_2str is OBJECT( str1 varchar2(32767), str2 varchar2(32767));
  create or replace type t_2str_table as table of t_2str;

   call:
   select split2(str, ' as ') Y
          from table(
                  split( '950106 as abs, 950 as xyz' , ',' )
                   )
               select
                      from table(
                              split( '950106 as abs, 950 as xyz' , ',' )
                               )
   results:
        Y.STR1  Y.STR2
        950106  abs
        950 xyz
  */

 return t_2str is
  l_idx  pls_integer;
  l_list varchar2(32767) := p_list;
  v_str  t_2str;
begin
  v_str := NEW t_2str('', '');
  loop
    l_idx      := instr(l_list, p_del);
    v_str.str1 := '';
    v_str.str2 := '';
    if l_idx > 0 then
      v_str.str1 := trim(substr(l_list, 1, l_idx - 1));
      l_list     := substr(l_list, l_idx + length(p_del));
      l_idx      := instr(l_list, p_del);
      if l_idx > 0 then
        v_str.str2 := trim(substr(l_list, 1, l_idx - 1));
        l_list     := substr(l_list, l_idx + length(p_del));
      else
        v_str.str2 := trim(l_list);
      end if;
      return(v_str);
      l_list := substr(l_list, l_idx + length(p_del));
    else
      return(v_str);
      exit;
    end if;
  end loop;

end;
/
SHOW ERRORS

create or replace procedure PCOMPILE is
  cursor invalidos is
    SELECT decode(object_type,
                  'PROCEDURE',
                  ' alter procedure ' || object_name || ' compile',
                  'FUNCTION',
                  'alter function ' || object_name || ' compile',
                  'TRIGGER',
                  ' alter trigger ' || object_name || ' compile',
                  'VIEW',
                  ' alter view ' || object_name || ' compile',
                  'PACKAGE',
                  ' alter package ' || object_name || ' compile',
                  'PACKAGE BODY',
                  ' alter package ' || object_name || ' compile body') txt
      FROM user_objects
     WHERE object_type IN ('PACKAGE',
                           'PACKAGE BODY',
                           'PROCEDURE',
                           'VIEW',
                           'TRIGGER',
                           'FUNCTION')
       AND status = 'INVALID'
     ORDER BY decode(object_type,
                     'PROCEDURE',
                     2,
                     'FUNCTION',
                     1,
                     'TRIGGER',
                     5,
                     'VIEW',
                     6,
                     'PACKAGE',
                     0,
                     'PACKAGE BODY',
                     4),
              decode(substr(object_name, 2, 3),
                     'ECO',
                     1,
                     'ABO',
                     2,
                     'ICE',
                     3,
                     'IPR',
                     4,
                     'DAR',
                     5,
                     'DIV',
                     6);
  i        integer;
  text_sql invalidos%ROWTYPE;
  retry    boolean;

begin
  i     := 10;
  retry := true;
  while retry and i > 0 LOOP
    OPEN invalidos;
    FETCH invalidos
      into text_sql;
    IF invalidos%NOTFOUND then
      retry := false;
      exit;
    end if;
    CLOSE invalidos;
    OPEN invalidos;
    LOOP
      FETCH invalidos
        into text_sql;
      EXIT WHEN invalidos%NOTFOUND;
      begin
        execute immediate text_sql.txt;
        Ketl.p_TLOG('PCOMPILE', 'compiled ' || text_sql.txt, 1);
      exception
        when others then
          Ketl.p_TLOG('PCOMPILE', sqlerrm || text_sql.txt, 1);
      end;
    END LOOP;
    i := i - 1;
    CLOSE invalidos;
  END LOOP;

end PCOMPILE;
/
SHOW ERRORS

prompt
prompt Creating package body KETL
prompt ==========================
prompt
create or replace package body KETL is
  --
  --  WORK_proc ; processo IMP1 = IMPORT ,IMP2 = IMPORT ,IMP3 = IMPORT , para paralelizar

  ext_db_owner varchar2(32);
  dummy        varchar2(3);

  procedure exec_job(p_process IN VARCHAR2) -- launch the JOB p_process
    -- parameter p_process must be the full name of a procedure, eventually  inside package KPBI, ex:  'KPBI.p_appx'
   is
    its_me varchar2(32);
  begin
  
    select user into its_me from dual;
  
    -- execute the solicited command
    execute immediate 'begin ' || p_process || '();end;';
  
    -- clear this command GO_JOB from table work
    begin
      delete from work t
       where t.WORK_nota = 'GO_JOB'
         and t.WORK_proc = p_process;
    exception
      when others then
        p_TLOG('exec_job-err', p_process, 1);
    end;
  
  end exec_job;
  ---------------------------------------------------
  --
  -- this proc is yet malfunctioning , it must return the list of fields in
  --  the correct order to inject in WORK_flds, or simply return a flag to do
  --  a create as select with a correct list
  -- it is a pormenor
  procedure make_table_equal_to_ext_db(p_tabela IN VARCHAR2) -- DDL
    -- invoked in pkg KJOBS activated by the trigger on insert on work
    --   start the process of named tables import
   is
    its_me varchar2(32);
    cursor c_data_dif is
      select *
        from (select table_name,
                     its_me EU,
                     column_name,
                     data_type,
                     data_length,
                     data_precision,
                     data_scale
                from all_tab_columns atc
               where atc.TABLE_NAME in
                     (SELECT WORK_nota FROM work WHERE WORK_nota = p_tabela)
                 and owner = its_me
              minus
              select table_name,
                     its_me,
                     column_name,
                     data_type,
                     data_length,
                     data_precision,
                     data_scale
                from sys.all_tab_columns@ext_db atc
               where atc.TABLE_NAME in
                     (SELECT WORK_nota FROM work WHERE WORK_nota = p_tabela)
                 and owner = ext_db_owner
              union
              select table_name,
                     'EXT_DB',
                     column_name,
                     data_type,
                     data_length,
                     data_precision,
                     data_scale
                from sys.all_tab_columns@ext_db atc
               where atc.TABLE_NAME in
                     (SELECT WORK_nota FROM work WHERE WORK_nota = p_tabela)
                 and owner = ext_db_owner
              minus
              select table_name,
                     'EXT_DB',
                     column_name,
                     data_type,
                     data_length,
                     data_precision,
                     data_scale
                from all_tab_columns atc
               where atc.TABLE_NAME in
                     (SELECT WORK_nota FROM work WHERE WORK_nota = p_tabela)
                 and owner = its_me)
       order by 1, 2 desc, 3;
  
  begin
    select user into its_me from dual;
  
    for v_data_dif in c_data_dif loop
      null;
      IF v_data_dif.eu = its_me then
        -- primeiro apaga colunas
        execute immediate 'alter table ' || v_data_dif.table_name ||
                          ' drop column ' || v_data_dif.column_name;
      else
        -- agora cria colunas
        if v_data_dif.data_type = 'DATE' then
          execute immediate 'alter table ' || v_data_dif.table_name ||
                            ' add ' || v_data_dif.column_name || ' date';
        else
          if v_data_dif.data_type = 'NUMBER' then
            execute immediate 'alter table ' || v_data_dif.table_name ||
                              ' add ' || v_data_dif.column_name ||
                              ' number(' || v_data_dif.data_precision || ',' ||
                              v_data_dif.data_scale || ')';
          else
            execute immediate 'alter table ' || v_data_dif.table_name ||
                              ' add ' || v_data_dif.column_name || ' ' ||
                              v_data_dif.data_type || '(' ||
                              v_data_dif.data_length || ')';
          end if;
        
        end if;
      end if;
    end loop;
  
  end;

  procedure copy_source_data(p_process IN VARCHAR2) -- parameter IMP1,IMP2,...
   is
    /*
    ---------------------------------------------------
    -- invoked in pkg KJOBS by the trigger at work
    -- invoked in pkg KJOBS by the trigger at work
    --   start the process of named tables import (or partial table import (merge))
    --        WORK_meto:
                  'DC' -- Drop, Create , and always recompiling
    --            'TI' -- Truncate, Insert , better
    --            'ID' -- Merge,  data is preserved except in the range of the provided WORK_WHER
     from ext_db
    --   ext_db process can monitor the end of this phase ('IMPORT' ceases to exist)
    --
    --   next step create √≠ndices
    --   finally call post-import procedures
    */
    its_me varchar2(32); -- current schema
    i      integer := 0; -- se <> 0 entao houve erro de dados e nao sinaliza o fim
    -- if <> 0 then a data error exist
  
    -- cursor com tabelas a carregar
    cursor c_work(done varchar2) is
      select *
        from work t
       where WORK_proc = p_process
         and WORK_nota <> 'IMPORT'
         and work_done = done --  'N' vai passar a 'D'ados depois a 'DI'e indices , ou 'Er'ro ou 'Ep'erro p√≥s
       order by work_orde;
    --         for update of WORK_done, WORK_DAAC;
    v_work c_work%rowtype;
    -- cursor indices to drop
    cursor c_drop_indices(tabela varchar2) is
      select distinct ' drop index ' || index_name as drop_sql
        from sys.all_ind_columns aic
       where 1 = 1
         and aic.table_owner = its_me
         and aic.table_name = tabela;
    v_drop_indices c_drop_indices%rowtype;
  
    str_sql varchar2(32000) := '';
    analise varchar2(2000) := '';
    wait    number;
  begin
    p_TLOG(p_process, 'STARTED', 1);
    commit;
    begin
      select USERNAME
        into ext_db_owner
        from all_db_links
       where upper(DB_LINK) = 'EXT_DB';
    exception
      when others then
        p_TLOG(p_process, 'DB_LINK = EXT_DB :' || sqlerrm, 1);
    end;
  
    -------------  preparacao done = 'N' vai passar a D  e finalmente a OK -----------------------
    update work t
       set work_done = 'N', t.work_daac = sysdate
     where WORK_proc = p_process;
    commit;
    select user into its_me from dual;
    ------------------------------------
    --  importar dados  ---  done  N passa a D
    <<REPETE_dados>>
  
    for v_work in c_work('N') loop
      -- wait before proceed explicit seconds or random secs (1..30)
      wait := nvl(v_work.work_wait, 0);
      if wait = 0 then
        --  select dbms_random.value(1, 30) into wait from dual;
        null;
      end if;
      dbms_lock.SLEEP(wait);
    
      analise := 'ANALYZE TABLE ' ||
                 nvl(v_work.work_newn, v_work.WORK_nota) ||
                 ' compute statistics';
      -- para copiar parte dos dados o 'slice' do _WHER primeiro tem de ser limpo
      if v_work.work_meto = 'ID' then
        str_sql := 'delete from ' ||
                   nvl(v_work.work_newn, v_work.WORK_nota) ||
                   v_work.work_wher;
        begin
          execute immediate str_sql;
        exception
          when others then
            p_TLOG(p_process,
                   'empty failed: ' || v_work.WORK_nota || ':' || sqlerrm,
                   1);
        end;
      end if;
      commit;
      --
      if v_work.work_meto = 'TI' then
        -- drop dos Indices
        for v_drop_indices in c_drop_indices(nvl(v_work.work_newn,
                                                 v_work.WORK_nota)) loop
          begin
            execute immediate v_drop_indices.drop_sql;
          exception
            when others then
              null;
          end;
        end loop;
      
      end if;
      --
      begin
        if v_work.work_meto = 'DC' then
          execute immediate 'drop table ' ||
                            nvl(v_work.work_newn, v_work.WORK_nota);
        else
          if v_work.work_meto = 'TI' then
            execute immediate 'alter table ' ||
                              nvl(v_work.work_newn, v_work.WORK_nota) ||
                              ' noLOGging';
            --execute immediate 'truncate table ' ||
            execute immediate 'delete from ' ||
                              nvl(v_work.work_newn, v_work.WORK_nota);
            --                   ||'  REUSE STORAGE';
            --fazer tabelas iguais
            --fazer tabelas iguais
            if v_work.work_flds = '*' then
              dummy := 'continuar';
              -- temporary offside, must be recrafted
              -- Ketl.make_table_equal_to_ext_db(v_work.WORK_nota);
            end if;
          
          end if;
        end if;
      exception
        when others then
          p_TLOG('cp_ext_db-err00',
                 nvl(v_work.work_newn, v_work.WORK_nota) || ':' || sqlerrm,
                 1);
          commit;
      end;
      -- create or insert table as select ... WHERE
      str_sql := '';
      if v_work.work_meto = 'DC' then
        str_sql := 'create table ' ||
                   nvl(v_work.work_newn, v_work.WORK_nota) ||
                   '  noLOGging tablespace users  as select ' ||
                   v_work.work_flds || ' from ' || v_work.WORK_nota ||
                   '@ext_db where 1=0 ';
        -- create empty table
        begin
          execute immediate str_sql;
        
        exception
          when others then
            if v_work.work_meto = 'TI' then
              -- ??
              v_work.work_meto := 'DC';
              p_TLOG('cp_ext_table_created',
                     nvl(v_work.work_newn, v_work.WORK_nota) || ':' ||
                     sqlerrm,
                     1);
            else
              p_TLOG('cp_ext_db-err01',
                     nvl(v_work.work_newn, v_work.WORK_nota) || ':' ||
                     str_sql || ':' || sqlerrm,
                     1);
            end if;
            commit;
        end;
      end if;
    
      -- recompilar
      PCOMPILE;
      --SELECT FPBI_COMPILE_INVALID into lixo FROM SYS.DUAL;
    
      str_sql := 'insert /*+ APPEND */ into ' ||
                 nvl(v_work.work_newn, v_work.WORK_nota) || '   select ' ||
                 v_work.work_flds || ' from ' || v_work.WORK_nota ||
                 '@ext_db' || v_work.work_wher;
    
      -- execucao da importacao
      begin
      
        -- 'D' para Dados transferidos, senao 'Er'
        if str_sql is not null then
          p_TLOG(p_process,
                 'table data ' || nvl(v_work.work_newn, v_work.WORK_nota) ||
                 ' start import',
                 1);
          commit;
          execute immediate str_sql;
        
        end if;
        --      retirar daqui o etl2, pq sem indices e' pachorrento
      
        p_TLOG(p_process,
               'table ' || nvl(v_work.work_newn, v_work.WORK_nota) ||
               ' imported',
               1);
        /*      if v_work.work_meto in ('TI','DC') then --<> 'ID' then
          execute immediate analise;
        end if;  */
      
      exception
        when others then
          p_TLOG('cp_ext_db-err02-Er',
                 nvl(v_work.work_newn, v_work.WORK_nota) || '-' || str_sql || ':' ||
                 sqlerrm,
                 1);
          /*
            update work
              set work_done = 'Er',
                work_DAAC = sysdate
              where current of c_work;
          */
          i := i + 1;
      end;
      --
      commit;
    end loop;
    commit;
  
    <<continua_indices>>
  ------ dados recebidos para este processo ------------------------------
  
    --
    p_TLOG(p_process, 'signal end data import', 1);
  
    begin
      -------------  preparacao done = 'N' vai passar a D  e finalmente a OK -----------------------
      update work
         set work_done = 'D', work_daac = sysdate
       where WORK_proc = p_process;
      -- sinalizar o fim do procedimento √; maquina chamadora
      delete from work t
       where t.WORK_nota = 'IMPORT'
         and WORK_proc = p_process;
    exception
      when others then
        p_TLOG('cp_ext_db-err03', 'DELETE .. IMPORT' || ':' || sqlerrm, 1);
    end;
    commit;
  
    -- agora vamos criar os √≠ndices    ---  done D passa a DI
    create_indexes(p_process);
  
    -- recompilar
    --    PCOMPILE;
    --SELECT FPBI_COMPILE_INVALID into lixo FROM SYS.DUAL;
  
    -- ainda falta executar etl2, ja com os indices
    execute_etl2(p_process); --, v_work.WORK_nota);
  
    p_TLOG(p_process, 'ENDED', 1);
    commit;
  end;
  ---------------------------------------------------

  procedure create_indexes(p_process varchar2) is
    -- is used in copy_direct
    -- Created on 2007-11-16 by Helder Velez
    last_index_name varchar2(32); -- indice anterior na tabela
    unique_bitmap   varchar2(20);
  
    -- cursor com tabelas a carregar
    cursor c_work is
      select *
        from work t
       where WORK_proc = p_process
         and WORK_nota <> 'IMPORT'
         and work_meto <> 'ID' -- neste modo nao cria indices, ja estao criados
         and (work_inx <> 'N' --  nao cria indices, a pedido
             or work_inx is null)
       order by work_orde;
    v_work c_work%rowtype;
    --  cursor indices a criar
    cursor c_ind_cols(p_process varchar2,
                      tabela    varchar2,
                      inx_type  varchar2) is
      select X.*, nvl(upper(z.Y.str2), X.column_name) new_column_name
        from (select aic.table_name,
                     index_type,
                     uniqueness,
                     ai.index_name,
                     column_position,
                     COLUMN_NAME
                from sys.all_ind_columns@ext_db aic,
                     sys.all_indexes@ext_db     ai
               where 1 = 1
                 and aic.table_owner = ai.table_owner
                 and aic.INDEX_NAME = ai.INDEX_NAME
                 and aic.table_owner = ext_db_owner
                 and aic.table_name = tabela
                 and ((inx_type is null) or
                     (inx_type = 'U' and uniqueness = 'UNIQUE'))
              
              ) X,
             (
              
              select split2(str, ' as ') y
                from table(split((select t.work_flds
                                    from work t
                                   where t.WORK_nota = tabela
                                     and WORK_proc = p_process
                                     and WORK_nota <> 'IMPORT'
                                     and work_meto <> 'ID' -- neste modo nao cria indices, ja estao criados
                                     and (work_inx <> 'N' --  nao cria indices, a pedido
                                         or work_inx is null))
                                  
                                 ,
                                  ','))) Z
       where upper(X.COLUMN_NAME) = upper(z.y.str1(+))
         and (z.y.str1 is not null and z.y.str2 is not null)
       order by INDEX_NAME, column_position;
  
    v_ind_cols c_ind_cols%rowtype;
    str_sql    varchar2(32000) := '';
    analise    varchar2(2000) := '';
    rnd        varchar2(5);
  begin
  
    ------------------------------------
    -- agora vamos criar os Indices    ---  done D passa a DI
  
    for v_work in c_work --('D')
     loop
      dbms_output.put_line('tabela ' || v_work.WORK_nota);
      str_sql := '';
      if v_work.work_meto = 'TI' or v_work.work_meto = 'DC' then
        last_index_name := ''; -- controlo de quebra
        rnd             := ''; --          := dbms_random.string('x', 3);
        for v_ind_cols in c_ind_cols(p_process,
                                     v_work.WORK_nota,
                                     v_work.work_inx) loop
        
          dbms_output.put_line('index name: ' || v_ind_cols.index_name || rnd);
          analise := 'ANALYZE INDEX ' || v_ind_cols.index_name || rnd ||
                     ' compute statistics';
        
          if last_index_name = v_ind_cols.index_name then
            str_sql := str_sql || ',' || v_ind_cols.new_column_name;
          else
            if last_index_name is not null then
              str_sql := str_sql || ')  tablespace users '; --
              begin
                null;
                execute immediate str_sql;
                p_TLOG(p_process, 'index ' || str_sql || ' :created', 1);
                --                execute immediate analise;
              exception
                when others then
                  if substr(sqlerrm, 1, 9) <> 'ORA-01408' then
                    -- column already indexed
                    p_TLOG('cria_indices-err01',
                           'exec ' || str_sql || ':' || sqlerrm,
                           1);
                  end if;
                  commit;
              end;
              dbms_output.put_line(str_sql);
              str_sql := '';
            end if;
            if v_ind_cols.index_type = 'BITMAP' then
              unique_bitmap := ' BITMAP ';
            else
              if v_ind_cols.uniqueness = 'UNIQUE' then
                unique_bitmap := ' UNIQUE ';
              else
                unique_bitmap := ' ';
              end if;
            end if;
            str_sql := 'create ' || unique_bitmap || ' index ' ||
                       v_ind_cols.index_name || rnd || ' on ' ||
                       nvl(v_work.work_newn, v_work.WORK_nota) || ' ( ' ||
                       v_ind_cols.new_column_name;
          end if;
          last_index_name := v_ind_cols.index_name;
        end loop;
      
      end if;
      if str_sql is not null then
        str_sql := str_sql || ') tablespace users noLOGging';
        begin
          null;
          execute immediate str_sql;
          p_TLOG(p_process, 'index ' || str_sql || ' :created', 1);
          commit;
          --          execute immediate analise;
        exception
          when others then
            p_TLOG('cria_indices-err02',
                   'exec ' || str_sql || ':' || sqlerrm,
                   1);
            commit;
        end;
        dbms_output.put_line(str_sql);
      end if;
    end loop;
    -- actualiza done de 'D' para 'DI'
    update work
       set work_done = 'DI', work_daac = sysdate
     where WORK_proc = p_process
       and (work_inx <> 'N' --  nao cria indices, a pedido
           or work_inx is null);
    commit;
    -----------
    /*
    -- para saber as diferencas entre indices de dois schemas
    select distinct index_name from (
    select index_name, column_position , COLUMN_NAME from (
          select index_name, column_position , COLUMN_NAME
            from sys.all_ind_columns@orclp aic
           where 1 = 1
             and aic.table_owner = ext_db_owner
             and aic.index_owner = ext_db_owner
             and aic.table_name in ('WORK_nota','TICEENTI','TIPRINAB','TPRETIEN','TPRETIVA','TPREA2CC','TPREGDI4','TECOUTIL',
             'TABOGRPA','TABOTIDE','TABOTIAB','TECOCOPO','TECOCONC','TECODIST','TIPRSISU','TABOMEAB','TABODAAB','TABORELA','TABOOPRE',
             'TIPRDSSU','TIPRFULE','TIPRFLIA','TABODESC','TABOENCR','TABOEGCA','TABOVAAB','TABOMOAL','TICEIDPE','TICEMORA','IMAB',
             'TABODIEN','TABOMOAB')
    minus
          select index_name, column_position , COLUMN_NAME
            from all_ind_columns
           where 1 = 1
             and index_owner = 'PBI_STAT'
             and table_owner = 'PBI_STAT'
             and table_name in ('WORK_nota','TICEENTI','TIPRINAB','TPRETIEN','TPRETIVA','TPREA2CC','TPREGDI4','TECOUTIL',
             'TABOGRPA','TABOTIDE','TABOTIAB','TECOCOPO','TECOCONC','TECODIST','TIPRSISU','TABOMEAB','TABODAAB','TABORELA','TABOOPRE',
             'TIPRDSSU','TIPRFULE','TIPRFLIA','TABODESC','TABOENCR','TABOEGCA','TABOVAAB','TABOMOAL','TICEIDPE','TICEMORA','IMAB',
             'TABODIEN','TABOMOAB')
     )       order by INDEX_NAME, column_position
     )
    */
  end create_indexes;
  ----------------------------------------------------------------------
  procedure execute_etl2(p_process varchar2) is
    -- ,tabela     varchar2) is
    cursor c_work is
    --(done varchar2, n_work_max integer) is
      select *
        from work t
       where WORK_proc = p_process
            --         and WORK_nota <> 'IMPORT'
            --         and WORK_nota = tabela
         and work_etl2 is not null
      --         and work_done = done --  'N' vai passar a 'D'ados depois a 'DI'e indices , ou 'Er'ro ou 'Ep'erro p√≥s
      --         and rownum < 1 + n_work_max --- s√≥ um registo de cada vez
       order by work_orde;
    --       for update of work_done, work_DAAC;
    v_work c_work%rowtype;
  
  begin
    for v_work in c_work --('D', 200)
     loop
      begin
        dbms_output.put_line('tabela ' || v_work.WORK_nota);
        p_TLOG(p_process, 'etl2 ' || v_work.WORK_nota || ' start', 1);
        commit;
        execute immediate 'begin ' || v_work.work_etl2 || ';end;';
        p_TLOG(p_process, 'etl2 ' || v_work.WORK_nota || ' end', 1);
        commit;
      exception
        when others then
          p_TLOG('executar_etl2-err01',
                 'exec begin ' || v_work.work_etl2 || ';end;' || ':' ||
                 sqlerrm,
                 1);
          commit;
      end;
    end loop;
  end;

  -------------------------------
  procedure wait_ended(processes varchar2, sleep_time integer) is
    -- wait until 'ENDED', 'END', 'ABORTED' in tlog
    v_processes   varchar2(100) := processes;
    num_processes integer := 0;
    inicial       VARCHAR2(24);
    i             integer := 0;
    v_sleep_time  integer := nvl(sleep_time, 60);
  begin
    inicial := to_char(SYSTIMESTAMP, 'yyyy-mm-dd HH24:MI:SS:TZH');
  
    select count(*) into num_processes from table(split(v_processes, ','));
  
    loop
      dbms_lock.sleep(v_sleep_time);
      select count(*)
        into i
        from TLOG l
       where 1 = 1
         and l.TIME_STAMP > inicial
            -- and WORK_nota in ('IMPORT', 'GO_JOB')
         and l.TLOG_proc in (select * from table(split(v_processes, ',')))
         and upper(l.TLOG_msg) in ('ENDED', 'END', 'ABORTED');
      exit when i = num_processes;
    end loop;
  end;
  ------------------------------------------
  -------------------------------
  procedure wait_job_end(processes varchar2, sleep_time integer) is
    -- wait until the job ends ( after _etl2 )
    v_processes   varchar2(100) := processes;
    num_processes integer := 0;
    i             integer := 0;
    v_sleep_time  integer := nvl(sleep_time, 60);
    its_me        varchar2(32);
  
  begin
  
    select count(*) into num_processes from table(split(v_processes, ','));
  
    select user into its_me from dual;
    i := 0;
    loop
      -- repeat
      for proc in (select str from table(split(v_processes, ','))) loop
        for jobn in (select job_name
                       from dba_scheduler_running_jobs
                      where owner = its_me) loop
          --          if instr(jobn.job_name, proc.str, 1) > 0 then
          if jobn.job_name = proc.str then
            i := i + 1;
          end if;
        
        end loop;
      end loop;
      exit when i = 0;
      dbms_lock.sleep(v_sleep_time);
    end loop;
  end;
  ---------------------------------------------------------
  ----
  -- write TLOG (proc max 20 char)
  -- replicated from KHOUS for containment

  procedure p_tlog(proc     IN varchar2,
                   msg      in TLOG.TLOG_msg%TYPE,
                   PUSER_ID in number) is -- prus.User_Id%TYPE) is
  begin
    insert into TLOG
      (TLOG_proc, TLOG_msg, user_id)
    values
      (substr(proc, 1, 20), msg, puser_id);
    commit;
  end;

end KETL;
/
SHOW ERRORS

prompt
prompt Creating package body KETL2
prompt ===========================
prompt
create or replace package body Ketl2 is

/*
  procedure etl2_tdocdocu is
  begin
    update tdocdocu@sa d
       set d.assu_id =
           (select a.assu_id from tdocasdo@sa a where d.docu_id = a.docu_id);
    commit;
  end;
 */
  -------
  procedure p_getl2_dummy is
    i integer;
  begin
    --------------------------------
    i := 1;
  end;
end Ketl2;
/
SHOW ERRORS

prompt
prompt Creating package body KJOBS
prompt ===========================
prompt
create or replace package body KJOBS is

  -- Author : Helder Velez
  -- purpose :
  --  launch the jobs of data import (ETL)      

  --  this package is called by the trigger of TWORK
  --   1 - to launch the jobs of data import
  --   2 - to execute a job        

  --  Important note:
  --  AFTER COMPILING THIS PACKAGE :
  --    - RECOMPILE THE TRIGGER GWORK_EXE

  procedure import_data_now(data_set varchar2) is
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
  
    begin
      dbms_scheduler.drop_job(job_name => 'JOB_IMPORT_DATA_NOW_' ||
                                          data_set,
                              force    => true);
    exception
      when others then
        null;
    end;
    dbms_scheduler.create_job(job_name        => 'JOB_IMPORT_DATA_NOW_' ||
                                                 data_set,
                              job_type        => 'PLSQL_BLOCK',
                              job_action      => 'begin Ketl.copy_source_data(''' ||
                                                 data_set || '''); end; ',
                              start_date      => null,
                              repeat_interval => null,
                              enabled         => true,
                              comments        => 'corre o job X de importa√ß√£o agora');
  
  end;

  --------------------------------

  procedure go_job(p_procedure varchar2) is
    --  procedure to execute a procedure defined in KETL
    --  parameter : procedure defined in KETL    
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
  
    begin
      dbms_scheduler.drop_job(job_name => 'JOB_' || p_procedure,
                              force    => true);
    exception
      when others then
        null;
    end;
    dbms_scheduler.create_job(job_name        => 'JOB_' || p_procedure,
                              job_type        => 'PLSQL_BLOCK',
                              job_action      => 'begin Ketl.exec_job(''' ||
                                                 p_procedure || '''); end; ',
                              start_date      => null,
                              repeat_interval => null,
                              enabled         => true,
                              comments        => 'corre o job X de execu√ß√£o agora');
  
  end;

/*begin
  -- Initialization
  null;*/
end KJOBS;
/

prompt
prompt Creating package body KPBI
prompt ==========================
prompt
create or replace package body KPBI is
/*
  procedure p_estat_pbi
  
   is
    VIEWKey CONSTANT number := 5; -- chave da VIEW vem de VIEW
    data_VIEW_4 varchar2(32); -- IMGS.IMGS_DAIM%Type;
  
    --##########  obtain stats of PBI
    cursor C_contador is
      select VIEWKey,
             to_char(i.VIEW_ID, '9,990') PVIEW,
             i.IMGS_daim daim,
             to_char(i.IMGS_dacr, 'YYYY-MM-DD') dacr,
             to_char(i.IMGS_dacr, 'DD') diacr,
             to_char(i.IMGS_dacr, 'HH') HoraLanÁ,
             '',
             '',
             '',
             trunc(sum(nvl(IMGS_dacf, IMGS_dacr) - IMGS_dacr) * 86400 /
                   count(*),
                   0) seg,
             trunc(sum(nvl(IMGS_dacf, IMGS_dacr) - IMGS_dacr) * 86400 / 3600 /
                   count(*),
                   1) Horas,
             'seg',
             count(*),
             null
      
        from IMGS I, DEIM De
       where 1 = 1
         and I.IMGS_ID = De.IMGS_ID
         and to_char(i.IMGS_dacr, 'YYYY-MM') like data_VIEW_4 || '%'
       group by i.VIEW_ID,
                i.IMGS_daim,
                to_char(i.IMGS_dacr, 'YYYY-MM-DD'),
                to_char(i.IMGS_dacr, 'HH'),
                to_char(i.IMGS_dacr, 'DD');
    --##########  vari·veis
  
    v_contador    Khous.t_img;
    v_data_cria   date;
    v_dtDropAte_1 date;
    to_be_closed  boolean := false;
  
  begin
    -- OBTER A DATA MAIS ACTUAL (DA VIEW 4)
    data_VIEW_4 := Khous.p_imgDataLast(VIEWDataKey);
    Khous.p_imgini(VIEWKey);
    select sysdate into v_data_cria from dual;
    --##########  OBTER total de registos e tempos usados em pbi
    OPEN c_contador;
    LOOP
      FETCH c_contador
        into v_contador;
      EXIT WHEN c_contador%NOTFOUND;
      Khous.p_imgnew_rec(v_contador);
      to_be_closed := true;
    END LOOP;
    CLOSE c_contador;
    -- finalizar a imagem
    if to_be_closed = true then
      Khous.p_imgclose(VIEWKey, data_VIEW_4, v_data_cria, 1);
    end if;
    -- OFF --
    -- invocar DropPar  para as imagens criadas atÈ ao mÍs passado
    v_dtDropAte_1 := ADD_MONTHS(to_date(data_VIEW_4, 'yyyy-mm'), -1);
    --     Khous.p_imgDropParM(v_dtDropAte_1);
  end;

  --######################################################################
  --            obter todas as imgs de uma vez
  --##########  obter estatÌstica de estatÌsticas
  --##########  E executar o DROP dos par‚metros definidos em VIEW
  --######################################################################
  /*
    procedure p_estat_pbi_all
  
     is
      VIEWKey CONSTANT number := 5; -- chave da VIEW vem de VIEW
  
      data_VIEW_4 IMGS.IMGS_DAIM%Type;
      --## meab
      cursor c_meab is
        select to_char(M.ABOMEAB_MEAB, 'yyyy-mm') Mes
          from tabomeab M
         where M.ABOMEAB_MEAB > to_date('2002-07', 'yyyy-mm');
      v_meab c_meab%rowtype;
      --##########  obter estatÌstica de estatÌsticas
      cursor C_contador is
        select VIEWKey,
               to_char(i.VIEW_ID, '9,990') VIEW,
               i.IMGS_daim daim,
               to_char(i.IMGS_dacr, 'YYYY-MM-DD') dacr,
               to_char(i.IMGS_dacr, 'DD') diacr,
               to_char(i.IMGS_dacr, 'HH') HoraLanÁ,
               '',
               '',
               '',
               trunc(sum(nvl(IMGS_dacf, IMGS_dacr) - IMGS_dacr) *
                     86400 / count(*), 0) seg,
               trunc(sum(nvl(IMGS_dacf, IMGS_dacr) - IMGS_dacr) *
                     86400 / 3600 / count(*), 1) Horas,
               'seg',
               count(*),
               null
  
          from IMGS I, DEIM De
         where 1 = 1
           and I.IMGS_ID = De.IMGS_ID
           and to_char(i.IMGS_dacr, 'YYYY-MM') like data_VIEW_4 || '%'
         group by i.VIEW_ID,
                  i.IMGS_daim,
                  to_char(i.IMGS_dacr, 'YYYY-MM-DD'),
                  to_char(i.IMGS_dacr, 'HH'),
                  to_char(i.IMGS_dacr, 'DD');
      --##########  vari·veis
  
      v_contador    Khous.t_img;
      v_data_cria   date;
      v_dtDropAte_1 date;
      to_be_closed  boolean := false;
  
    begin
      -- out OBTER A DATA MAIS ACTUAL (DA VIEW 4)
      -- out   data_VIEW_4 := Khous.p_imgDataLast( VIEWDataKey );
      open c_meab;
      loop
        fetch c_meab
          into v_meab;
        exit when c_meab%notfound;
        data_VIEW_4 := v_meab.mes;
        Khous.p_imgini(VIEWKey);
        select sysdate into v_data_cria from dual;
        --##########  OBTER total de registos e tempos usados em pbi
        OPEN c_contador;
        LOOP
          FETCH c_contador
            into v_contador;
          EXIT WHEN c_contador%NOTFOUND;
          Khous.p_imgnew_rec(v_contador);
          to_be_closed := true;
        END LOOP;
        CLOSE c_contador;
        -- finalizar a imagem
        if to_be_closed = true then
          Khous.p_imgclose(VIEWKey, data_VIEW_4, v_data_cria);
        end if;
      end loop;
      close c_meab;
    end;
  */
  ---------------
  --------------------------        USER JOBS Launcher
  ------------------
/*
  procedure p_DOC_daily is
    i        integer := 0;
    inicial  VARCHAR2(24);
    ended    VARCHAR2(24);
    tot_time number;
    stop_it  integer;
    wproc    proc.proc_id%type := 2; --  cons from PROC to run
    wuser_id proc.user_id%type;
  begin
    select user_id into wuser_id from proc where proc_id = wproc;
  
    inicial := to_char(SYSTIMESTAMP, 'yyyy-mm-dd HH24:MI:SS:TZH');
    KETL.p_TLOG('DOC', 'STARTED', wuser_id);
    commit;
  
    begin
      select count(*)
        into stop_it
        from work t
       where WORK_nota = 'IMPORT'
         and WORK_proc in ('IMP0', 'IMP3', 'IMP4');
      if stop_it > 0 then
        ketl.p_TLOG('DOC',
                    'record IMPORT in work is preventing execution ',
                    wuser_id);
        ketl.p_TLOG('DOC', 'ABORTED', wuser_id);
        commit;
        dbms_standard.raise_application_error(-20110,
                                              'record IMPORT in work is preventing execution');
      else
        insert into work
          (WORK_nota, WORK_proc, WORK_wait)
        values
          ('IMPORT', 'IMP3', 15);
        insert into work
          (WORK_nota, WORK_proc, WORK_wait)
        values
          ('IMPORT', 'IMP4', 30);
      
        commit;
      end if;
    end;
  
    --
    -- aguarda o end da importaÁ„o dos dados     
    ketl.wait_ENDED('IMP3, IMP4', 30);
    -- this one depends on previous TdocDocu !
    insert into work
      (WORK_nota, WORK_proc, WORK_wait)
    values
      ('IMPORT', 'IMP0', 1);
    commit;
    ketl.wait_ENDED('IMP0', 30);
    --
    ---- Documents
  
    Kdoc.p_Doc_MAX_DOCU;
    Kdoc.p_Doc_NOT_CONC;
    Kdoc.p_Doc_CONC_MONTH;
    Kdoc.p_Doc_INIC_MONTH;
  
    -- finalizar o TLOG
    ended := to_char(SYSTIMESTAMP, 'yyyy-mm-dd HH24:MI:SS:TZH');
    -- TODO    tot_time := trunc(0.51 + (ended - inicial) * 60 * 24, 0); -- minutos
    ketl.p_TLOG('DOC',
                'ENDED - ' || to_char(tot_time, '0999') || ' minutes',
                wuser_id);
    commit;
  end;
  */
begin
  null;
end KPBI;
/
SHOW ERRORS


prompt
prompt Creating trigger GTLOG
prompt ======================
prompt
create or replace trigger GTLOG
  before insert on TLOG
  for each row
declare
  -- local variables here
begin
  select STLOG.nextval, to_char(SYSTIMESTAMP, 'yyyy-mm-dd HH24:MI:SS:TZH')
    into :NEW.TLOG_ID, :NEW.time_stamp
    from dual;
end GTLOG;
/

prompt
prompt Creating trigger GWORK
prompt ======================
prompt
create or replace trigger GWORK
  before update or insert or delete on WORK
  for each row
declare

begin
  if inserting then
    select SWORK.nextval into :NEW.WORK_id from dual;
  end if;
  if inserting or updating then
    select to_char(SYSTIMESTAMP, 'yyyy-mm-dd HH24:MI:SS:TZH')
      into :NEW.time_stamp
      from dual;
  end if;

  -- CouchdbON
  -- if the table TSYNC do not exist then 
  --   comment code from here       
  $if $$CouchdbON = 1 $then
  if deleting or (nvl(:OLD.REV, 'xx') = nvl(:NEW.REV, 'xx')) then
    -- copy to cdb
    if deleting then
      str_val := 'begin kcouch.del_cdb(''WORK'', ''WORK_id'' , ''' ||
                 :old.id || ''' ,''' || :old.rev || ''' ,' || :old.user_id ||
                 ' ); end; ';
    else
      str_val := 'begin kcouch.ora2cdb(''WORK'', ''WORK_id'' , ' ||
                 :new.WORK_id || ' ,' || :new.user_id || ' ); end;';
    
    end if;
    str_val := replace(str_val, '''', '''''');
    insert into TSYNC t
      (sync_dir, sync_PROC, sync_todo, user_id)
    values
      ('ora2cdb', 'exec_immediate', str_val, :new.user_id);
  
  end if;
  $end
  -- END_CouchdbON
end GWORK;
/
SHOW ERRORS

prompt
prompt Creating trigger GWORK_EXE
prompt ==========================
prompt
create or replace trigger GWORK_EXE
  after insert on work
  for each row
declare
  -- local variables here
begin
  -- nome da tabela = 'IMPORT' e proceso 'IMP1',2... invoca o job da importa?o respectiva
  if :NEW.work_nota = 'IMPORT' then
    kjobs.import_data_now(:NEW.work_proc);
    -- nome da tabela = 'GO_JOB' e process 'xxx',... invokes the respective exec job
  else
    if :NEW.work_nota = 'GO_JOB' then
      kjobs.go_job(:NEW.work_proc);
    end if;
  end if;
end GWORK_EXE;
/


spool off
