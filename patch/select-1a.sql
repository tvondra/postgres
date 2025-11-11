--set join_collapse_limit = 1;
set enable_starjoin_join_search = OPT;
select * from t
    join dim1 on (dim1.id = id1)
    join dim2 on (dim2.id = id2)
    join dim3 on (dim3.id = id3)
    join dim4 on (dim4.id = id4)
    join dim5 on (dim5.id = id5)
    join dim6 on (dim6.id = id6)
    join dim7 on (dim7.id = id7);
