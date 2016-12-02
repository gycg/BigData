A = load 'ex_data/roadnet/roadNet-CA.txt' as (nodeA:chararray, nodeB:chararray);
A1 = load 'ex_data/roadnet/roadNet-CA.txt' as (nodeA:chararray, nodeB:chararray);
B = group A by nodeA;
C = foreach B generate COUNT(A) as freq, group;
GroupC = group C by freq;
Freq = foreach GroupC generate group, COUNT(C) as freq_freq;
--dump Freq 

B2 = group A by nodeB;
C2 = foreach B2 generate COUNT(A) as freq, group;

/*(1,30)
(2,18)
(3,153)
(4,111)
(5,5)
(6,1)*/


D = join C by group right outer, C2 by group;
E = filter D by C::group is null;
F = group E all;
G = foreach F generate COUNT(E.$3); --dead-end

H = union C,C2;
H1 = foreach H generate $1;
I = distinct H1;
J = group I all;
K = foreach J generate COUNT(I);  --total number of node


crossGK = cross G, K;
Percentage = foreach crossGK generate (double)$0/$1;

L = group A all;
M = foreach L generate COUNT(A);
crossKM = cross K, M;
average = foreach crossKM generate (double)$1*2/$0;

N = join A by nodeB, A1 by nodeA;
O = filter N by $0 != $3;
P = foreach O generate $0 as n1, $2 as n2, $3 as n4;
Q = join P by $2, A by $0;
R = foreach Q generate $0 as n1, $1 as n2, $2 as n4, $4 as n6;
S = filter R by n1 == n6;
T = group S all;
U = foreach T generate COUNT(S)/3;    --size of triangle

/*
*Problem: Some triangle may count for more than one time. For example, (n1, n2, n4, n1) and (n1, n4, n2, n1), this is one triangle, but count for twice.
*/
