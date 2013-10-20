/*
compilation flaga
grants
create dblink
sequences swork, stlog
drop table work,tlog
*/

----------------------------------------------------------
 --------------------------------- conditional compilation on CouchdbO
alter session set PLSQL_CCFlags = 'CoucdbON:0'

 --------------------------------- conditional compilation on CouchdbO
----------------------------------------------------------

----------------------------------------------
-- Create database link 
-- the user sys previously Created a database link and granted 
/*
create public database link EXT_DB
  connect to HR identified by HR
  using 'XE';

  grant execute on SYS.DBMS_LOCK to &USER;
  grant select on dba_scheduler_running_jobs to &USER; 

  */
 

----------------------------------------------
drop table users CASCADE CONSTRAINTS;
drop table tlog CASCADE CONSTRAINTS;
drop table work CASCADE CONSTRAINTS;
drop type t_str_table;
drop type t_str;  
drop type t_2str_table;
drop type t_2str;  
----------------------------------------------
  -- Create sequence 
drop sequence SWORK;  
create sequence SWORK
	minvalue 1
	maxvalue 9999999999999999999999999999
	start with 1
	increment by 1
	cache 20;  
----------------------------------------------
-- Create sequence 
drop sequence STLOG;
create sequence STLOG
	minvalue 1
	maxvalue 9999999999999999999999999999
	start with 1
	increment by 1
	cache 20;  
----------------------------------------------
