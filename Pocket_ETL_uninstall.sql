-- uninstall Pocket_ETL 

drop function pcompile;

drop package kjobs;
drop package ketl;
drop package ketl2;
drop package kpbi;

drop function split2;
drop function split;
drop type t_str;
drop type t_str_table;
drop type T_2STR;
drop type T_2STR_table;
drop procedure pcompile;

drop sequence swork;
drop sequence stlog;

drop table work CASCADE CONSTRAINTS;
drop table tlog CASCADE CONSTRAINTS;
drop table users CASCADE CONSTRAINTS;