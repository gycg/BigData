emp = load 'ex_data/emp_dept/emp.csv' as (empno:int, ename:chararray, job:chararray, mgr:int, hiredate:datetime, sal:float,deptno: int);

dept = load 'ex_data/emp_dept/dept.csv' as (deptno:int, dname:chararray, loc: chararray);

salgrade = load 'ex_data/emp_dept/salgrade.csv' as (grade:int,losal:int, hisal:int);

dump emp;
dump dept;
dump salgrade;

--1.Smith’s employment date
tmp = FILTER emp BY ename == 'SMITH';
Smith_emp_date = FOREACH tmp GENERATE hiredate;

--2.Ford’s job title
tmp = FILTER emp BY ename == 'FORD';
Ford_job_titile = FOREACH tmp GENERATE job;

--3.The first employee (by the hiredate)
tmp = ORDER emp BY hiredate;
First_employee_emp = LIMIT tmp 1;
First_employee = FOREACH First_employee_emp GENERATE ename;

--4.The number of employees in each department
tmp = GROUP emp BY deptno;
Number_each_dept = FOREACH tmp GENERATE group, COUNT(emp.empno);

--5.The number of employees in each city
tmp = JOIN emp BY deptno, dept BY deptno;
tmp2 = GROUP tmp BY loc;
Number_each_city = FOREACH tmp2 GENERATE group, COUNT(tmp.empno);

--6.The average salary in each city
tmp = JOIN emp BY deptno, dept BY deptno;
tmp2 = GROUP tmp BY loc;
Avgsal_each_city = FOREACH tmp2 GENERATE group, AVG(tmp.sal);

--7.The highest paid employee in each department
tmp = GROUP emp BY deptno;
tmp1 = FOREACH tmp GENERATE group, MAX(emp.sal) as maxsal;
tmp2 = join emp by sal, tmp1 by maxsal;
Highest_paid_emp = foreach tmp2 generate emp::deptno, emp::ename;

--8.Managers whose subordinates have at least one subordinate
managers = FOREACH emp GENERATE mgr;
emp2 = FOREACH emp GENERATE empno, mgr;
emp3 = FOREACH emp GENERATE empno, ename;
c = JOIN emp2 BY empno LEFT OUTER, managers BY mgr;
d = filter c by emp2::mgr is not NULL and managers::mgr is not null;
e = foreach d generate emp2::mgr;
f = distinct e;
g = JOIN f by emp2::mgr, emp3 by empno;
h = FOREACH g GENERATE emp3::ename;

--(JONES)
--(KING)

--9.The number of employees for each hiring year
tmp = FOREACH emp GENERATE empno, ToString(hiredate,'YYYY') as year;
tmp2 = GROUP tmp BY year;
ans = FOREACH tmp2 GENERATE group, COUNT(tmp.empno);

--10.The pay grade of each employee
crossed = cross emp, salgrade;
emp_salgrade = filter crossed by losal < sal and sal <= hisal;
ans = FOREACH emp_salgrade GENERATE ename, grade;
