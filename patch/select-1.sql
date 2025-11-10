--set join_collapse_limit = 1;
select * from t
    join dim1 on (dim1.id = id1)
    join dim2 on (dim2.id = id2)
    join dim3 on (dim3.id = id3)
    join dim4 on (dim4.id = id4)
    join dim5 on (dim5.id = id5)
    join dim6 on (dim6.id = id6)
    join dim7 on (dim7.id = id7);

select * from t
    left join dim1 on (dim1.id = id1)
    left join dim2 on (dim2.id = id2)
    left join dim3 on (dim3.id = id3)
    left join dim4 on (dim4.id = id4)
    left join dim5 on (dim5.id = id5)
    left join dim6 on (dim6.id = id6)
    left join dim7 on (dim7.id = id7);
