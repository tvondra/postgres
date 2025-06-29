-- set join_collapse_limit = 1;
select * from t
    left join dim1 on (id = id1)
    left join dim2 on (id = id2)
    left join dim3 on (id = id3)
    left join dim4 on (id = id4)
    left join dim5 on (id = id5)
    left join dim6 on (id = id6)
    left join dim7 on (id = id7);
