-- Shows that without full finalization / costing when selecting seeds, the
-- full cost may invert with the approximate seed cost. The consequence is
-- that after increasing the number of seeds, we get a more expensive plan
-- because we pick a different seed, but the final cost is higher.
--
-- In this case it happens because with final=false we don't build the gather
-- paths for the topmost relation in finalize_join_rel, so that we effectively
-- judge the seeds only for serial plans. But maybe the "more expensive" seed
-- may be parallelized, and the "cheaper one" can't, inverting the cost.
--
-- This test is rather expensive, as it needs to build a data set.

SELECT SETSEED(0.125034988232524);

DROP TABLE IF EXISTS t_0;
CREATE TABLE t_0 (c_0_0 int, c_0_1 int, c_0_2 int, c_0_3 int, c_0_4 int, c_0_5 int, c_0_6 int, c_0_7 int, c_0_8 int, c_0_9 int, c_0_10 int, c_0_11 int, c_0_12 int, c_0_13 int, c_0_14 int, c_0_15 int, c_0_16 int, c_0_17 int, c_0_18 int, c_0_19 int, c_0_20 int, c_0_21 int, c_0_22 int, c_0_23 int, c_0_24 int, c_0_25 int, c_0_26 int, c_0_27 int, c_0_28 int, c_0_29 int, c_0_30 int, c_0_31 int);
DROP TABLE IF EXISTS t_1;
CREATE TABLE t_1 (c_1_0 int, c_1_1 int, c_1_2 int, c_1_3 int, c_1_4 int, c_1_5 int, c_1_6 int, c_1_7 int, c_1_8 int, c_1_9 int, c_1_10 int, c_1_11 int, c_1_12 int, c_1_13 int, c_1_14 int, c_1_15 int, c_1_16 int, c_1_17 int, c_1_18 int, c_1_19 int, c_1_20 int, c_1_21 int, c_1_22 int, c_1_23 int, c_1_24 int, c_1_25 int, c_1_26 int, c_1_27 int, c_1_28 int, c_1_29 int, c_1_30 int, c_1_31 int);
DROP TABLE IF EXISTS t_2;
CREATE TABLE t_2 (c_2_0 int, c_2_1 int, c_2_2 int, c_2_3 int, c_2_4 int, c_2_5 int, c_2_6 int, c_2_7 int, c_2_8 int, c_2_9 int, c_2_10 int, c_2_11 int, c_2_12 int, c_2_13 int, c_2_14 int, c_2_15 int, c_2_16 int, c_2_17 int, c_2_18 int, c_2_19 int, c_2_20 int, c_2_21 int, c_2_22 int, c_2_23 int, c_2_24 int, c_2_25 int, c_2_26 int, c_2_27 int, c_2_28 int, c_2_29 int, c_2_30 int, c_2_31 int);
DROP TABLE IF EXISTS t_3;
CREATE TABLE t_3 (c_3_0 int, c_3_1 int, c_3_2 int, c_3_3 int, c_3_4 int, c_3_5 int, c_3_6 int, c_3_7 int, c_3_8 int, c_3_9 int, c_3_10 int, c_3_11 int, c_3_12 int, c_3_13 int, c_3_14 int, c_3_15 int, c_3_16 int, c_3_17 int, c_3_18 int, c_3_19 int, c_3_20 int, c_3_21 int, c_3_22 int, c_3_23 int, c_3_24 int, c_3_25 int, c_3_26 int, c_3_27 int, c_3_28 int, c_3_29 int, c_3_30 int, c_3_31 int);
DROP TABLE IF EXISTS t_4;
CREATE TABLE t_4 (c_4_0 int, c_4_1 int, c_4_2 int, c_4_3 int, c_4_4 int, c_4_5 int, c_4_6 int, c_4_7 int, c_4_8 int, c_4_9 int, c_4_10 int, c_4_11 int, c_4_12 int, c_4_13 int, c_4_14 int, c_4_15 int, c_4_16 int, c_4_17 int, c_4_18 int, c_4_19 int, c_4_20 int, c_4_21 int, c_4_22 int, c_4_23 int, c_4_24 int, c_4_25 int, c_4_26 int, c_4_27 int, c_4_28 int, c_4_29 int, c_4_30 int, c_4_31 int);
DROP TABLE IF EXISTS t_5;
CREATE TABLE t_5 (c_5_0 int, c_5_1 int, c_5_2 int, c_5_3 int, c_5_4 int, c_5_5 int, c_5_6 int, c_5_7 int, c_5_8 int, c_5_9 int, c_5_10 int, c_5_11 int, c_5_12 int, c_5_13 int, c_5_14 int, c_5_15 int, c_5_16 int, c_5_17 int, c_5_18 int, c_5_19 int, c_5_20 int, c_5_21 int, c_5_22 int, c_5_23 int, c_5_24 int, c_5_25 int, c_5_26 int, c_5_27 int, c_5_28 int, c_5_29 int, c_5_30 int, c_5_31 int);
DROP TABLE IF EXISTS t_6;
CREATE TABLE t_6 (c_6_0 int, c_6_1 int, c_6_2 int, c_6_3 int, c_6_4 int, c_6_5 int, c_6_6 int, c_6_7 int, c_6_8 int, c_6_9 int, c_6_10 int, c_6_11 int, c_6_12 int, c_6_13 int, c_6_14 int, c_6_15 int, c_6_16 int, c_6_17 int, c_6_18 int, c_6_19 int, c_6_20 int, c_6_21 int, c_6_22 int, c_6_23 int, c_6_24 int, c_6_25 int, c_6_26 int, c_6_27 int, c_6_28 int, c_6_29 int, c_6_30 int, c_6_31 int);
INSERT INTO t_0 SELECT mod(i,972835), mod(i,927536), mod(i,746697), mod(i,493040), mod(i,276943), mod(i,20609), mod(i,307601), mod(i,581954), mod(i,720622), mod(i,220222), mod(i,58073), mod(i,460513), mod(i,904474), mod(i,85090), mod(i,755325), mod(i,876386), mod(i,675099), mod(i,4633), mod(i,47076), mod(i,661671), mod(i,355592), mod(i,812743), mod(i,843337), mod(i,915938), mod(i,618248), mod(i,162350), mod(i,572036), mod(i,254130), mod(i,933418), mod(i,667307), mod(i,118453), mod(i,624940) FROM generate_series(1, 31182.44921285125) s(i);
INSERT INTO t_1 SELECT mod(i,811763), mod(i,917117), mod(i,486360), mod(i,706825), mod(i,930766), mod(i,471858), mod(i,129611), mod(i,160410), mod(i,575557), mod(i,990359), mod(i,116634), mod(i,932567), mod(i,982987), mod(i,472769), mod(i,85479), mod(i,395942), mod(i,692735), mod(i,7293), mod(i,901024), mod(i,643979), mod(i,578593), mod(i,945493), mod(i,611375), mod(i,807305), mod(i,423628), mod(i,508888), mod(i,984074), mod(i,448492), mod(i,44544), mod(i,31755), mod(i,300834), mod(i,373903) FROM generate_series(1, 229287.14755886144) s(i);
INSERT INTO t_2 SELECT mod(i,258457), mod(i,417298), mod(i,837867), mod(i,806284), mod(i,884188), mod(i,316221), mod(i,420187), mod(i,254245), mod(i,975896), mod(i,936882), mod(i,384384), mod(i,171735), mod(i,768280), mod(i,224506), mod(i,872949), mod(i,907588), mod(i,679532), mod(i,105910), mod(i,953174), mod(i,888501), mod(i,355294), mod(i,744664), mod(i,195462), mod(i,513291), mod(i,972820), mod(i,847999), mod(i,697304), mod(i,334570), mod(i,5985), mod(i,843386), mod(i,535661), mod(i,116190) FROM generate_series(1, 752737.5471009929) s(i);
INSERT INTO t_3 SELECT mod(i,524824), mod(i,895888), mod(i,28028), mod(i,954266), mod(i,376831), mod(i,402491), mod(i,120452), mod(i,474468), mod(i,847031), mod(i,488531), mod(i,146084), mod(i,598049), mod(i,808324), mod(i,856410), mod(i,899847), mod(i,805705), mod(i,985489), mod(i,991587), mod(i,257907), mod(i,970719), mod(i,624390), mod(i,481712), mod(i,256685), mod(i,45612), mod(i,889036), mod(i,852325), mod(i,226228), mod(i,637581), mod(i,806923), mod(i,232543), mod(i,246150), mod(i,306242) FROM generate_series(1, 100.00000012778344) s(i);
INSERT INTO t_4 SELECT mod(i,469021), mod(i,886862), mod(i,190193), mod(i,418845), mod(i,964643), mod(i,266847), mod(i,523004), mod(i,746020), mod(i,197859), mod(i,993836), mod(i,501864), mod(i,221447), mod(i,23798), mod(i,795932), mod(i,310897), mod(i,870622), mod(i,863166), mod(i,727849), mod(i,744094), mod(i,131988), mod(i,570669), mod(i,495258), mod(i,728526), mod(i,333277), mod(i,707374), mod(i,803308), mod(i,807893), mod(i,352035), mod(i,7752), mod(i,197941), mod(i,36604), mod(i,572532) FROM generate_series(1, 872475.1197335212) s(i);
INSERT INTO t_5 SELECT mod(i,990117), mod(i,48587), mod(i,795185), mod(i,39016), mod(i,691798), mod(i,856674), mod(i,723226), mod(i,936067), mod(i,699108), mod(i,308122), mod(i,575496), mod(i,269948), mod(i,891578), mod(i,892402), mod(i,631324), mod(i,509306), mod(i,263211), mod(i,121272), mod(i,738865), mod(i,767713), mod(i,956063), mod(i,941605), mod(i,245379), mod(i,711118), mod(i,692849), mod(i,539448), mod(i,231896), mod(i,327934), mod(i,526894), mod(i,159759), mod(i,40525), mod(i,224102) FROM generate_series(1, 100.64701257453642) s(i);
INSERT INTO t_6 SELECT mod(i,167609), mod(i,759031), mod(i,190056), mod(i,766905), mod(i,623414), mod(i,545997), mod(i,352402), mod(i,482489), mod(i,649283), mod(i,84668), mod(i,833400), mod(i,763016), mod(i,546541), mod(i,987728), mod(i,969433), mod(i,808457), mod(i,918798), mod(i,671656), mod(i,293591), mod(i,770744), mod(i,475768), mod(i,927194), mod(i,525673), mod(i,320924), mod(i,72857), mod(i,960248), mod(i,326741), mod(i,234980), mod(i,267712), mod(i,752312), mod(i,271528), mod(i,291172) FROM generate_series(1, 199.6470927848302) s(i);
CREATE INDEX ON t_0 (c_0_22);
CREATE INDEX ON t_0 (c_0_5);
CREATE INDEX ON t_0 (c_0_28);
CREATE INDEX ON t_0 (c_0_30);
CREATE INDEX ON t_0 (c_0_0);
CREATE INDEX ON t_0 (c_0_9);
CREATE INDEX ON t_0 (c_0_7);
CREATE INDEX ON t_0 (c_0_15);
CREATE INDEX ON t_0 (c_0_4);
CREATE INDEX ON t_0 (c_0_13);
CREATE INDEX ON t_0 (c_0_10);
CREATE INDEX ON t_0 (c_0_24);
CREATE INDEX ON t_0 (c_0_23);
CREATE INDEX ON t_0 (c_0_16);
CREATE INDEX ON t_0 (c_0_3);
CREATE INDEX ON t_0 (c_0_29);
CREATE INDEX ON t_1 (c_1_3);
CREATE INDEX ON t_1 (c_1_31);
CREATE INDEX ON t_1 (c_1_0);
CREATE INDEX ON t_1 (c_1_10);
CREATE INDEX ON t_1 (c_1_23);
CREATE INDEX ON t_1 (c_1_4);
CREATE INDEX ON t_1 (c_1_14);
CREATE INDEX ON t_1 (c_1_27);
CREATE INDEX ON t_1 (c_1_28);
CREATE INDEX ON t_1 (c_1_12);
CREATE INDEX ON t_1 (c_1_13);
CREATE INDEX ON t_1 (c_1_18);
CREATE INDEX ON t_1 (c_1_8);
CREATE INDEX ON t_1 (c_1_2);
CREATE INDEX ON t_1 (c_1_6);
CREATE INDEX ON t_1 (c_1_29);
CREATE INDEX ON t_2 (c_2_7);
CREATE INDEX ON t_2 (c_2_10);
CREATE INDEX ON t_2 (c_2_8);
CREATE INDEX ON t_2 (c_2_26);
CREATE INDEX ON t_2 (c_2_24);
CREATE INDEX ON t_2 (c_2_12);
CREATE INDEX ON t_2 (c_2_0);
CREATE INDEX ON t_2 (c_2_29);
CREATE INDEX ON t_2 (c_2_27);
CREATE INDEX ON t_2 (c_2_2);
CREATE INDEX ON t_2 (c_2_4);
CREATE INDEX ON t_2 (c_2_25);
CREATE INDEX ON t_2 (c_2_16);
CREATE INDEX ON t_2 (c_2_23);
CREATE INDEX ON t_2 (c_2_15);
CREATE INDEX ON t_2 (c_2_28);
CREATE INDEX ON t_3 (c_3_12);
CREATE INDEX ON t_3 (c_3_13);
CREATE INDEX ON t_3 (c_3_6);
CREATE INDEX ON t_3 (c_3_18);
CREATE INDEX ON t_3 (c_3_4);
CREATE INDEX ON t_3 (c_3_16);
CREATE INDEX ON t_3 (c_3_23);
CREATE INDEX ON t_3 (c_3_9);
CREATE INDEX ON t_3 (c_3_8);
CREATE INDEX ON t_3 (c_3_0);
CREATE INDEX ON t_3 (c_3_27);
CREATE INDEX ON t_3 (c_3_28);
CREATE INDEX ON t_3 (c_3_22);
CREATE INDEX ON t_3 (c_3_5);
CREATE INDEX ON t_3 (c_3_14);
CREATE INDEX ON t_3 (c_3_7);
CREATE INDEX ON t_4 (c_4_21);
CREATE INDEX ON t_4 (c_4_23);
CREATE INDEX ON t_4 (c_4_27);
CREATE INDEX ON t_4 (c_4_25);
CREATE INDEX ON t_4 (c_4_1);
CREATE INDEX ON t_4 (c_4_31);
CREATE INDEX ON t_4 (c_4_5);
CREATE INDEX ON t_4 (c_4_16);
CREATE INDEX ON t_4 (c_4_15);
CREATE INDEX ON t_4 (c_4_22);
CREATE INDEX ON t_4 (c_4_12);
CREATE INDEX ON t_4 (c_4_19);
CREATE INDEX ON t_4 (c_4_3);
CREATE INDEX ON t_4 (c_4_6);
CREATE INDEX ON t_4 (c_4_29);
CREATE INDEX ON t_4 (c_4_26);
CREATE INDEX ON t_5 (c_5_31);
CREATE INDEX ON t_5 (c_5_16);
CREATE INDEX ON t_5 (c_5_14);
CREATE INDEX ON t_5 (c_5_7);
CREATE INDEX ON t_5 (c_5_23);
CREATE INDEX ON t_5 (c_5_24);
CREATE INDEX ON t_5 (c_5_18);
CREATE INDEX ON t_5 (c_5_27);
CREATE INDEX ON t_5 (c_5_0);
CREATE INDEX ON t_5 (c_5_30);
CREATE INDEX ON t_5 (c_5_26);
CREATE INDEX ON t_5 (c_5_12);
CREATE INDEX ON t_5 (c_5_2);
CREATE INDEX ON t_5 (c_5_29);
CREATE INDEX ON t_5 (c_5_17);
CREATE INDEX ON t_5 (c_5_3);
CREATE INDEX ON t_6 (c_6_12);
CREATE INDEX ON t_6 (c_6_4);
CREATE INDEX ON t_6 (c_6_11);
CREATE INDEX ON t_6 (c_6_7);
CREATE INDEX ON t_6 (c_6_0);
CREATE INDEX ON t_6 (c_6_1);
CREATE INDEX ON t_6 (c_6_14);
CREATE INDEX ON t_6 (c_6_15);
CREATE INDEX ON t_6 (c_6_5);
CREATE INDEX ON t_6 (c_6_25);
CREATE INDEX ON t_6 (c_6_2);
CREATE INDEX ON t_6 (c_6_27);
CREATE INDEX ON t_6 (c_6_30);
CREATE INDEX ON t_6 (c_6_19);
CREATE INDEX ON t_6 (c_6_28);
CREATE INDEX ON t_6 (c_6_31);

-- make sure the costing is stable
SET default_statistics_target = 10000;

VACUUM ANALYZE;

-- single seed, this produces a parallel plan with cost 24224.18 (can change
-- a bit due to sampling etc)

SET join_collapse_limit = 100;
SET from_collapse_limit = 100;
SET geqo = off;

LOAD 'lindp';
SET lindp.enabled = on;
SET lindp.min_relations = 2;
SET lindp.max_relations = 100;
SET lindp.seeds = 1;

-- show just the plan shape
EXPLAIN
SELECT * FROM   t_0
  LEFT JOIN t_1 ON ((c_0_12 = c_1_1) AND (c_0_21 = c_1_7))
  JOIN (
    t_2
    FULL JOIN t_6 ON ((c_2_25 = c_6_10))
  ) ON ((c_1_8 = c_2_13) AND (c_0_24 = c_6_0))
  LEFT JOIN (
    t_3
    LEFT JOIN t_5 ON ((c_3_28 = c_5_23) AND (c_3_21 = c_5_26) AND (c_3_30 = c_5_25))
  ) ON ((c_1_31 = c_5_22) AND (c_0_13 = c_5_3))
  LEFT JOIN t_4 ON ((c_2_24 = c_4_11) AND (c_5_9 = c_4_10));

-- with 7 seeds, it should produce the same plan, but without the finalization
-- it picks a serial plan with cost ~32180.92 (because it's not parallel, and
-- the approximate cost just looks at serial costs)

SET lindp.seeds = 7;

EXPLAIN
SELECT * FROM   t_0
  LEFT JOIN t_1 ON ((c_0_12 = c_1_1) AND (c_0_21 = c_1_7))
  JOIN (
    t_2
    FULL JOIN t_6 ON ((c_2_25 = c_6_10))
  ) ON ((c_1_8 = c_2_13) AND (c_0_24 = c_6_0))
  LEFT JOIN (
    t_3
    LEFT JOIN t_5 ON ((c_3_28 = c_5_23) AND (c_3_21 = c_5_26) AND (c_3_30 = c_5_25))
  ) ON ((c_1_31 = c_5_22) AND (c_0_13 = c_5_3))
  LEFT JOIN t_4 ON ((c_2_24 = c_4_11) AND (c_5_9 = c_4_10));

DROP TABLE IF EXISTS t_0;
DROP TABLE IF EXISTS t_1;
DROP TABLE IF EXISTS t_2;
DROP TABLE IF EXISTS t_3;
DROP TABLE IF EXISTS t_4;
DROP TABLE IF EXISTS t_5;
DROP TABLE IF EXISTS t_6;
