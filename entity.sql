-- entities are passed here one at a time. Incoming entity is matched with the clusters seen so far and if it 
-- matches a cluster it joins that cluster and centroid of that cluster is updated else this incoming entity forms
-- a new cluster, and after this idf and normalization factor of the grams present in the clusters is updated since 
-- the number of total clusters may change which changes the number of documents. 
-- The time is indicated for each sql statement, this time is for the case when a new incoming entity is compared
-- with 330 clusters seen so far.



CREATE OR REPLACE FUNCTION entity_match(integer) RETURNS void AS
$$
BEGIN



truncate table new_string;


insert into new_string(att_id, gram, tf, idf, norm)             //3 ms
select att_id, gram, tf, idf, norm
from in_strings
where  entity_id = $1
group by att_id,gram,tf,idf,norm;


truncate table new_dist;

insert into new_dist(name,value,stdev)                          //0.92 ms
select  name, value, stdev
from in_dists
where entity_id = $1
group by name, value, stdev;


raise info ' on results';



-- matching string columns in incoming entity with clusters seen so far,
-- new_string contains the grams for incoming entity ,  cid - cluster id from clusters seen so far

truncate table results;

insert into results(cid, att_id, score)                         //58.8 ms
select  e.cid , d.att_id, ((SUM(d.tf*d.idf * e.tf*e.idf)::float) / (d.norm * e.norm))  as score
     FROM new_string d, seen_strings e
          WHERE d.gram = e.gram
          AND d.att_id =e.att_id
         and d.norm <> 0
         and e.norm <> 0
          GROUP BY e.cid, d.att_id,d.norm,e.norm;

--matching numerical columns in incoming entity with clusters seen so far 
-- new_dist contains numerical columns of the incoming entity  
 

truncate table temp_a;

insert into temp_a(cid, name, score)                            // 3.93 ms
select e.cid ,d.name, (abs(d.value::float - e.value::float)  / e.stdev)
from new_dist d , seen_dists e
where d.name = e.name
and e.stdev <> 0
group by e.cid,d.name,d.value,e.value,e.stdev;


-- score for numerical columns assuming difference of > 5 stddev is equivalent to zero similarity

raise info ' calculating result';

truncate table results_dist;

insert into results_dist(cid, name, score)                    // 3 ms
select cid,name, (5.0 - score)/5.0
from temp_a
where score < 5.0;

insert into results_dist( cid, name, score)                   //  0.98 ms
select  cid,name, 0.0
from temp_a
where score >= 5.0;

-- weighted sum for string column scores

truncate table final;

insert into final(cid, score)                                 //2.927 ms
select  r.cid , sum(r.score * u.weight) as score
from results r, UC_strings u
where r.att_id =  u.att_id
group by r.cid;

-- weighted sum for numerical column scores

truncate table final_dists;

insert into final_dists(cid, score)                           //   2.128 ms
select r.cid, sum(r.score *u.weight) as score
from results_dist r, UC_dists u
where r.name = u.name
group by r.cid;


update final                                                  // 1.769 ms
set score = (final.score + t.score)
from final_dists t
where final.cid = t.cid;

-- threshold(k) for weighted sum of the column scores

truncate table temp_matches_temp;                              

insert into temp_matches_temp(cid,score)                      //1.375 ms
select  i.cid, i.score
from final i
where i.score > k;

-- selecting the cid which got the highest score in case entity matches multiple clusters

truncate table temp_scores;

insert into temp_scores(score)                                //1.355 ms
select max(score)
from temp_matches_temp;

truncate table temp_matches;

insert into temp_matches(cid)                                 //1.201 ms
select  t.cid
from temp_matches_temp t, temp_scores s
where t.score = s.score ;


truncate table matched_string;

insert into matched_string(att_id, gram)                      //22.634 ms
select d.att_id, d.gram
from seen_strings d, temp_matches e
where d.cid = e.cid; 



raise info 'updating the centroid setting tf/2 for new_string in case of match ';

update new_string set tf =tf/2 where exists(select * from temp_matches);     //1.317 ms

raise info 'updating centroid i.e. tf/2 in seen_strings for the cluster ';

update seen_strings e set tf = e.tf/2 from temp_matches d where d.cid = e.cid;   //18.039 ms

raise info 'updating the tf for matching att_id, gram pair for the cluster';

update seen_strings e set tf = (e.tf+d.tf) from in_strings d, temp_matches f    // 693.321 ms <--------------
       where d.att_id = e.att_id 
       and d.gram = e.gram 
       and f.cid = e.cid;

raise info 'inserting new grams in the cluster if matched from the incoming entity';
       
insert into seen_strings(cid, att_id, gram, tf, idf, norm)        //3.579 ms
select d.cid, e.att_id, e.gram, e.tf, e.idf, e.norm
from temp_matches d, new_string e
where not exists ( select * from matched_string f where e.att_id = f.att_id and e.gram = f.gram)
group by d.cid, e.att_id, e.gram, e.tf, e.idf, e.norm;  



RAISE INFO 'inserting in  cluster table for the case of match';


insert into cluster_table(entity_id,cid)                           //1.163 ms
select $1, cid
from temp_matches ;

RAISE INFO ' inserting in case of a new cluster';

insert into cluster_table(entity_id, cid)                          //0.741 ms

select $1, nextval('this_is_it')
where not exists( select * from temp_matches)
;

-------  in case when incoming entity didnt match any cluster

raise info 'inserting in seen_strings';

INSERT INTO seen_strings(cid, att_id, gram, tf,idf,norm)            // 1.129 ms
select  f.cid,d.att_id, d.gram, d.tf,d.idf,d.norm
from new_string d, cluster_table f
where not exists(select * from temp_matches)
and f.entity_id = $1
group by f.cid, d.att_id, d.gram, d.tf, d.idf, d.norm;


raise info ' inserting in seen_dists';

--- seen_dists has the numerical columns of clusters seen so far

insert into seen_dists(cid,name, value,stdev)                       //0.991 ms
select f.cid, d.name, d.value,d.stdev
from new_dist d, cluster_table f
where f.entity_id = $1
group by f.cid, d.name, d.value, d.stdev;


raise info 'distinct_entiti';

truncate table distinct_entiti;

insert into distinct_entiti(entities)                                //9.577 ms
select count(distinct cid)
from seen_strings;

--- updating the idf, norm of the clusters seen so far 

truncate table seen_idf;

raise info 'seen_idf';

insert into seen_idf(att_id,gram,idf)                                 //59.210 ms
select y.att_id,y.gram,( d.entities ) /(count(*)::float) as idf
from seen_strings y, distinct_entiti d
group by y.att_id,y.gram, d.entities;

raise info 'update seen_strings';

update seen_strings                                                   //48.659 ms
set idf = d.idf
from seen_idf d
where seen_strings.att_id = d.att_id
and seen_strings.gram = d.gram;

raise info 'insert into seen_norms';

truncate table seen_norms;

insert into seen_norms(cid, att_id, norm)                              // 42.161 ms
SELECT a.cid,a.att_id,(sqrt(SUM((a.tf*b.idf)^2))) norm
       FROM seen_strings a, seen_idf b
      WHERE a.gram = b.gram
        and a.att_id = b.att_id
   GROUP BY a.cid,a.att_id;

raise info ' update seen_strings';

update seen_strings                                                   //37.321 ms
set norm = n.norm
from seen_norms n
where seen_strings.cid = n.cid
and seen_strings.att_id = n.att_id;



-- updating standard deviation of the numerical columns of the clusters seen so far 

raise info 'insert into seen_dist_sums';

truncate seen_dist_sums;
truncate seen_temp_dists;

insert into seen_dist_sums( name , n, sm , smsqr )                      //2.95
     SELECT name, COUNT(*) n,
            SUM(value::float)::float sm, SUM(value::float*value::float)::float smsqr
       FROM seen_dists
      GROUP BY name;

raise info 'insert into seen_temp_dists';


insert into seen_temp_dists( name,count, mean, stdev )                    //1.41 ms
SELECT  name, n, sm/n mean, sqrt(abs((smsqr - sm*sm/n) / (n-1)) ) stdev
 FROM seen_dist_sums
 WHERE n > 1;


update seen_dists                                        //3.22 ms
set stdev =  a.stdev
from seen_temp_dists a
where a.name = seen_dists.name;


raise info 'one entity done';

end
$$ LANGUAGE plpgsql;



