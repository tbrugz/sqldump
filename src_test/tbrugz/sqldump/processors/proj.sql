--drop table proj if exists

create table proj (
	id integer not null,
	name varchar(100) not null,
	owner_dept_id integer,
	startdate date,
	enddate date,
	constraint proj_pk primary key (id),
	constraint proj_dept_fk foreign key (owner_dept_id) references dept (id)
);
