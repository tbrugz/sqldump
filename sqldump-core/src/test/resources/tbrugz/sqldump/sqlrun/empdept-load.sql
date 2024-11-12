insert into DEPT (ID, NAME, PARENT_ID) values (0, 'CEO', 0);
insert into DEPT (ID, NAME, PARENT_ID) values (1, 'HR', 0);
insert into DEPT (ID, NAME, PARENT_ID) values (2, 'Engineering', 0);

insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (1, 'john', 1, 1, 2000);
insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (2, 'mary', 2, 2, 2000);
insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (3, 'jane', 2, 2, 1000);
insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (4, 'lucas', 2, 2, 1200);
insert into EMP (ID, NAME, SUPERVISOR_ID, DEPARTMENT_ID, SALARY) values (5, 'wilson', 1, 1, 1000);
