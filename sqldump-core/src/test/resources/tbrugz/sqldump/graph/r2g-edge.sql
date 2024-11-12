select '1' as SOURCE, '2' as TARGET, 'rel1' as EDGE_LABEL, '' as EDGE_TYPE from dual union all
select '1', '2', 'rel2', 'negative' from dual union all
select '2', ${test.id}, 'rel3', '${test.newlabel}' from dual union all
select '2', '1', 'rel4', '' from dual