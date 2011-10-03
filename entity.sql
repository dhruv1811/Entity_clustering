-- entities are passed here one at a time. Incoming entity is matched with the clusters seen so far and if it 
-- matches a cluster it joins that cluster and centroid of that cluster is updated else this incoming entity forms
-- a new cluster, and after this idf and normalization factor of the grams present in the clusters is updated since 
-- the number of total clusters may change which changes the number of documents. 
-- The time is indicated for each sql query, this time is for the case when a new incoming entity is compared
-- with 500 clusters seen so far.




CREATE OR REPLACE FUNCTION entity_match(integer) RETURNS void AS
$$
BEGIN

raise info 'compare';

truncate table results;                                  // 6.45 ms


--- matching string columns in incoming entities with clusters seen uptill now,
--- entity_id - incoming entity ,  cid - cluster id from clusters seen so far
--- in_strings contain the string columns of entities from the incoming source

insert into results(entity_id,cid, att_id, score)        // 2.28 ms
     select  d.entity_id, e.cid , d.att_id, ((SUM(d.tf*d.idf * e.tf*e.idf)::float) / (d.norm * e.norm)) as score
     FROM in_strings d, prefinal_strings e
     WHERE d.gram = e.gram
     AND d.att_id =e.att_id
     AND d.entity_id = $1
     GROUP BY d.entity_id,e.cid, d.att_id,d.norm,e.norm;


truncate table temp_a;                                  // 15.84 ms

--- matching numerical columns in incoming entities with clusters seen so far 
--- dists contain numerical columns of the entities from the incoming source 

insert into temp_a(entity_id, cid, name, score)         // 1.36 ms
    select d.entity_id, e.cid ,d.name, (abs(d.value::float - e.value::float)  / e.stdev)
    from dists d , seen_dists e 
    where d.name = e.name
    and e.stdev <> 0
    and d.entity_id = $1
    group by d.entity_id, e.cid,d.name,d.value,e.value,e.stdev;



truncate table results_dist;                            // 15.85 ms

-- scores for numerical columns assuming difference of >5 stddev is equivalent to zero similarity

insert into results_dist(entity_id, cid, name, score)   // 1.98 ms
select entity_id, cid,name, (5.0 - score)/5.0
from temp_a
where score < 5.0;

insert into results_dist(entity_id, cid, name, score)   //0.70 ms
select entity_id, cid,name, 0.0
from temp_a
where score >= 5.0;

truncate table final;                                  // 6 ms

--------- weighted sum for string column scores

insert into final(entity_id,cid, score)               // 1.17 ms
select r.entity_id, r.cid , sum(r.score * u.weight) as score
from results r, UC_strings u
where r.att_id =  u.att_id
group by r.entity_id, r.cid;

truncate table final_dists;                           // 5.57 ms

------ weighted sum for numerical column scores

insert into final_dists(entity_id,cid, score)         //  1.13 ms
select r.entity_id, r.cid, sum(r.score *u.weight) as score
from results_dist r, UC_dists u
where r.name = u.name
group by r.entity_id, r.cid;


update final                                          // 0.62 ms
set score = (final.score + t.score)
from final_dists t
where final.entity_id = t.entity_id
and final.cid = t.cid;




truncate table temp_matches_temp;                      // 5.639 ms

----- threshold(k) for weighted sum of the column scores

insert into temp_matches_temp(entity_id, cid,score)    // 0.967 ms
select  i.entity_id, i.cid, i.score
from final i
where i.score > k;

truncate table temp_scores;                            // 6.96 ms

---- selecting the cid which got the highest score in case entity matches multiple clusters

insert into temp_scores(score)                         // 1.53 ms
select max(score)
from temp_matches_temp;

truncate table temp_matches;                           // 5.62 ms

insert into temp_matches(entity_id, cid)               // 1.58 ms
select t.entity_id, t.cid
from temp_matches_temp t, temp_scores s
where t.score = s.score ;


truncate table temp_strings;                              // 17.06 ms
 
---- getting the att_id's and grams for the matched entities

insert into temp_strings(entity_id, cid, att_id, gram)    //  1.5 ms
select t.entity_id, t.cid, i.att_id, i.gram
from temp_matches t, new_string i, prefinal_strings f
where i.att_id = f.att_id
and i.gram = f.gram
and i.entity_id = t.entity_id
and f.cid = t.cid
group by t.entity_id, t.cid, i.att_id, i.gram;


raise info 'insert into table_temp';

truncate table table_temp;                                // 16.43 ms

--updating the centroid for string columns 

insert into table_temp(cid, att_id, gram, tf,idf,norm)    // 0.94 ms
select p.cid, p.att_id, p.gram,p.tf,p.idf,p.norm
from temp_strings t, prefinal_strings p
where t.cid = p.cid
and t.att_id = p.att_id
and t.gram = p.gram;


truncate table table_temp1;                                    // 15.67 ms


insert into table_temp1(cid, att_id, gram, tf, idf, norm)      // 1.29 ms 
select t.cid, i.att_id, i.gram, i.tf, i.idf, i.norm
from new_string i, temp_matches t
where i.entity_id = t.entity_id
and not exists(select * from prefinal_strings p where p.att_id = i.att_id and p.gram = i.gram and p.cid = t.cid);




raise info 'delete from prefinal_strings';        

delete from prefinal_strings                                   //  0.78 ms
where exists (select * from table_temp t
where prefinal_strings.cid = t.cid
and prefinal_strings.att_id = t.att_id
and prefinal_strings.gram = t.gram);


raise info 'update prefinal strings';

update prefinal_strings                                        // 0.55 ms
set tf = (prefinal_strings.tf)/2
from temp_matches t
where  t.cid = prefinal_strings.cid;



raise info 'insert into prefinal_strings';

insert into prefinal_strings(cid, att_id, gram, tf,idf,norm)    // 0.518 ms
select cid,att_id, gram, tf,idf,norm
from table_temp
group by cid, att_id, gram, tf,idf,norm;


RAISE INFO 'UPDATING matches';


update prefinal_strings                                          // 0.854 ms
set tf = (prefinal_strings.tf+e.tf)/2
from temp_strings t, new_string e
where t.entity_id = e.entity_id
and t.att_id = e.att_id
and t.gram = e.gram
and prefinal_strings.cid = t.cid
and prefinal_strings.att_id = t.att_id
and prefinal_strings.gram = t.gram;


insert into prefinal_strings(cid, att_id, gram, tf, idf, norm)       // 0.746 ms
select cid, att_id, gram, tf/2.0, idf, norm
from table_temp1;



--------- inserting in cluster table,  either an entity joins an older cluster or creates a new one 


insert into cluster_table(entity_id,cid)                            // 0.961 ms
select e.entity_id, d.cid
from temp_matches e, cluster_table d
where e.cid = d.cid
group by e.entity_id;

insert into cluster_table(entity_id, cid)                           // 1.1 ms
select d.entity_id, nextval('seq_no')
from new_string d
where not exists(select distinct e.entity_id from temp_matches e where e.entity_id = d.entity_id)
group by d.entity_id;

--- entities that dont match already have a cluster id from above so inserting them in clusters seen uptill now 

INSERT INTO prefinal_strings(cid, att_id, gram, tf,idf,norm)        // 1.293 ms
select  f.cid , d.att_id, d.gram, d.tf,d.idf,d.norm
from new_string d, cluster_table f
where not exists(select distinct e.entity_id from temp_matches e where e.entity_id = d.entity_id)
and d.entity_id = f.entity_id;

--- adding numerical columns of the unmatched incoming entity to the new cluster formed


insert into prefinal_dists(cid,name, value)                         // 0.85 ms
select f.cid, d.name, d.value
from new_dist d, cluster_table f
where not exists(select distinct entity_id from temp_matches e where e.entity_id = d.entity_id)
and d.entity_id = f.entity_id
group by f.cid,d.name,d.value;


---- if there is a match on numerical column then inserting that column from the incoming entity into the cluster matched  

insert into prefinal_dists(cid, name, value)                        // 0.83 ms
select t.cid, i.name, i.value
from new_dist i, temp_matches t
where i.entity_id = t.entity_id
group by t.cid,i.name,i.value;



------ updating  idf,norm and stdev of the clusters seen so far


truncate table public.seen_strings;                                // 16.89 ms

insert into public.seen_strings(cid, att_id,gram,tf, idf, norm)    // 1.09 ms
select cid, att_id, gram, tf, NULL, NULL
from prefinal_strings
group by cid, att_id, gram, tf;




truncate table distinct_entiti;                                     // 5.74 ms

insert into distinct_entiti(entities)                               // 1.12 ms
select count(distinct cid)
from public.seen_strings;



truncate table seen_idf;                                            // 16.13 ms

insert into seen_idf(att_id,gram,idf)                               // 1.63 ms
select y.att_id,y.gram,( d.entities ) /(count(*)::float) as idf
from public.seen_strings y, distinct_entiti d
group by y.att_id,y.gram, d.entities;


update public.seen_strings                                          // 0.6 ms
set idf = d.idf
from seen_idf d
where public.seen_strings.att_id = d.att_id
and public.seen_strings.gram = d.gram;


truncate table seen_norms;                                          // 5.52 ms

insert into seen_norms(cid, att_id, norm)                           // 1.2 ms
     SELECT a.cid,a.att_id,(sqrt(SUM((a.tf*b.idf)^2))) norm
       FROM public.seen_strings a, seen_idf b
      WHERE a.gram = b.gram
        and a.att_id = b.att_id 
   GROUP BY a.cid,a.att_id;


update public.seen_strings                                          // 0.57 ms
set norm = n.norm
from seen_norms n
where public.seen_strings.cid = n.cid
and public.seen_strings.att_id = n.att_id;


truncate table prefinal_strings;                                    // 16.25 ms

insert into prefinal_strings(cid, att_id,gram, tf, idf,norm)        // 1.06 ms
select cid, att_id, gram,tf, idf, norm
from public.seen_strings;


---updating standard deviation for numeric columns 


truncate table seen_dist_sums;                                      // 16.37 ms
 
truncate table seen_temp_dists;                                     // 15.61 ms

insert into seen_dist_sums( name , n, sm , smsqr )                  // 1.164 ms
     SELECT name, COUNT(*) n,
            SUM(value::float)::float sm, SUM(value::float*value::float)::float smsqr
       FROM prefinal_dists
      GROUP BY name;


insert into seen_temp_dists( name,count, mean, stdev )              // 0.67 ms
SELECT  name, n, sm/n mean, sqrt(abs((smsqr - sm*sm/n) / (n-1)) ) stdev
 FROM seen_dist_sums
 WHERE n > 1;

truncate table seen_dists;                                         // 10.2 ms 

insert into seen_dists(cid, name, value, stdev)                    // 0.4 ms
select cid, name, value, NULL
from prefinal_dists;

update seen_dists                                                  // 0.2 ms
set stdev =  a.stdev
from seen_temp_dists a 
where seen_dists.name = a.name;


end
$$ LANGUAGE plpgsql;



