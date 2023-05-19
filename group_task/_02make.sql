create database saikr;
use saikr;

create table contest
(
    id            int auto_increment,
    name          varchar(128),
    organizers    varchar(128),
    register_time varchar(128),
    contest_time  varchar(128),
    types         varchar(256),
    url           varchar(128),
    primary key (id)
);