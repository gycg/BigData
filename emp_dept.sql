select HIREDATE from emp where ENAME = 'SMITH';
select JOB from emp where ENAME = 'FORD';
select HIREDATE from emp order by HIREDATE asc where rownum <= 1;
select DEPTNO, COUNT(*) from emp group by DEPTNO;
select LOC, COUNT(*) from emp, dept where emp.DEPTNO = dept.DEPTNO group by LOC;
select LOC, AVG(SAL) from emp, dept where emp.DEPTNO = dept.DEPTNO group by LOC;

select distinct e.ENAME, e.DEPTNO from emp e, (select max(SAL) maxsal, DEPTNO from emp group by DEPTNO) m where e.SAL = m.maxsal and e.DEPTNO = m.DEPTNO;

select distinct e3.ENAME from (select distinct e1.MGR from emp e1 LEFT OUTER JOIN (select MGR from emp) e2 ON e1.EMPNO = e2.MGR where e1.MGR is not null and e2.MGR is not null) e4, emp e3 where e4.MGR = e3.EMPNO;

select extract(year from HIREDATE) year, count(*) from emp group by extract(year from HIREDATE);

select ENAME, GRADE from (select * from emp cross join salgrade) crossed where SAL > LOSAL and SAL <= HISAL;

