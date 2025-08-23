--drop table emp if exists

create table emp (
	id integer not null,
	name varchar(100) not null,
	supervisor_id integer,
	department_id integer,
	salary integer,
	constraint emp_pk primary key (id),
	constraint emp_emp_fk foreign key (supervisor_id) references emp (id)
);

--drop table dept if exists

create table dept (
	id integer not null,
	name varchar(100),
	parent_id integer,
	constraint dept_pk primary key (id)
);

alter table emp add constraint emp_dept_fk foreign key (department_id) references dept (id);

create table etc (
	id integer not null,
	dt_x date,
	description varchar(1000),
	constraint etc_pk primary key (id)
);
