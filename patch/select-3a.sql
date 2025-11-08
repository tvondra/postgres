--set join_collapse_limit = 1;
set enable_starjoin_join_search = OPT;
select * from t
    join dim1 on (dim1.id = id1)
    join dim2 on (dim2.id = id2)
    join dim3 on (dim3.id = id3)
    join dim4 on (dim4.id = id4)
    join dim1_1 on (id1_1 = dim1_1.id)
    join dim1_2 on (id1_2 = dim1_2.id)
    join dim2_1 on (id2_1 = dim2_1.id)
    join dim2_2 on (id2_2 = dim2_2.id)
    join dim3_1 on (id3_1 = dim3_1.id)
    join dim3_2 on (id3_2 = dim3_2.id)
    join dim4_1 on (id4_1 = dim4_1.id)
    join dim4_2 on (id4_2 = dim4_2.id);
