/* Copyright (c) 2011 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */



/*
* entities are passed here one at a time. Incoming entity is matched with the clusters seen so far and if it 
* matches a cluster it joins that cluster and centroid of that cluster is updated else this incoming entity forms
* a new cluster, and after this idf and normalization factor of the grams present in the clusters is updated since 
* the number of total clusters may change which changes the number of documents. 
*/



CREATE OR REPLACE FUNCTION UC_entity(integer, integer) RETURNS void AS
$$
DECLARE

hello int;

big float;

gh float;

cluster integer;

BEGIN

hello := nextval('counter');

raise info 'start';

truncate table new_string;

insert into new_string(att_id, gram, tf, idf, norm)
select att_id, gram, tf, idf, norm
from in_strings
where  entity_id = $1
group by att_id,gram,tf,idf,norm;


truncate table new_dist;

insert into new_dist(name,value,stdev)
select  name, value, stdev
from in_dists
where entity_id = $1
group by name, value, stdev;


raise info ' on results';

--made just one table ------seen_strings, and replcaed with $1 --in both temp_a, and results, remove entity_id from , group by 

truncate table results;

insert into results(cid, att_id, score)
select  e.cid , d.att_id, round(((SUM(d.tf*d.idf * e.tf*e.idf)) / (d.norm * e.norm))::numeric ,5) as score
     FROM new_string d, seen_strings e
          WHERE d.att_id = e.att_id
          AND d.gram =e.gram
         and d.norm <> 0
         and e.norm <> 0
          GROUP BY e.cid, d.att_id,d.norm,e.norm;


raise info ' on numerical results now ';

truncate table temp_a;

insert into temp_a(cid, name, score)
select e.cid ,d.name, (abs(d.value::float - e.value::float)  / e.stdev)
from new_dist d , seen_dists e
where d.name = e.name
and e.stdev <> 0
group by e.cid,d.name,d.value,e.value,e.stdev;

raise info ' calculating result';

truncate table results_dist;

insert into results_dist(cid, name, score)
select cid,name, (3.0 - score)/3.0
from temp_a
where score < 3.0;

insert into results_dist( cid, name, score)
select  cid,name, 0.0
from temp_a
where score >= 3.0;


truncate table final;

insert into final(cid, score)
select  r.cid , sum(r.score * u.weight) as score
from results r, UC_strings u
where r.att_id =  u.att_id
and r.score >0.5
group by r.cid;

truncate table final_dists;

insert into final_dists(cid, score)
select r.cid, sum(r.score *u.weight) as score
from results_dist r, UC_dists u
where r.name = u.name
and r.score >0.3
group by r.cid;


update final
set score = (final.score + t.score)
from final_dists t
where final.cid = t.cid;

gh := mean from UC_mean;

truncate table temp_matches_temp;



if (gh>5.2) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 34.0;

elsif( gh>4.6 AND gh<=5.2) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 31.5;

elsif( gh>3.8 AND gh<=4.6) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 28.0;

elsif( gh>=3.0 AND gh<=3.8) then


insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 24.0;

elsif( gh>=2.0 AND gh<3.0) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 18.0;

elsif(gh>=1.6 AND  gh<2.0 ) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score > 12.0;

elsif(gh>=0.8 AND  gh<1.6 ) then

insert into temp_matches_temp(cid,score)
select  i.cid, i.score
from final i
where i.score >6.2;

elsif(gh<0.8) then

insert into temp_matches_temp(cid,score)
select i.cid, i.score
from final i
where i.score >4.0;

end if;

truncate table temp_scores;

insert into temp_scores(score)
select max(score)
from temp_matches_temp;


truncate table temp_matches;

insert into temp_matches(cid)
select max(t.cid)
from temp_matches_temp t, temp_scores s
where t.score = s.score
and t.cid >1;


cluster := cid from temp_matches;

----------- updating the centroid


if exists(select * from temp_matches e where e.cid > 1 ) then


update new_string set tf =tf/2 ;


update seen_strings e
set tf = e.tf/2
where e.cid = cluster;


raise info 'combining the tf for matching att_id, gram pair';

update seen_strings e
set tf = (e.tf+d.tf)
from new_string d
where e.cid = cluster
and e.att_id = d.att_id
and e.gram = d.gram;


truncate table matched_string;

insert into matched_string( att_id, gram)
select d.att_id, d.gram
from seen_strings d, new_string e
where d.cid = cluster
and d.att_id = e.att_id
and d.gram = e.gram;

raise info 'inserting in seen_strings those att_id, gram which dont match from new_string';

insert into seen_strings(cid, att_id, gram, tf, idf, norm)
select cluster, e.att_id, e.gram, e.tf, e.idf, e.norm
from  new_string e
where not exists ( select * from matched_string f where e.att_id = f.att_id and e.gram = f.gram)
group by cluster, e.att_id, e.gram, e.tf, e.idf, e.norm;

raise info 'insert in just_testing norm';

truncate table just_testing_norm;

insert into just_testing_norm(att_id, norm)
select att_id, round((sqrt(SUM((tf*idf)^2)))::numeric,5) norm
from seen_strings
 where cid = cluster
 and tf > 0.00001
and idf > 0.00001
group by att_id;

raise info 'updating norm of the seen_strings';

update seen_strings
set norm = n.norm
from just_testing_norm n
where seen_strings.cid = cluster
and seen_strings.att_id = n.att_id;


end if;


-------------------------------------------------------------------------------  CLUSTER_TABLE
RAISE INFO 'inserting in  cluster table for the case of match';

---------changed the code here

if exists(select * from temp_matches e where e.cid > 1) then

insert into cluster_table(entity_id,cid)
values($1,cluster);

else

insert into cluster_table(entity_id, cid)
values ($1, nextval('sequence1'));


end if;

-------   entities that dont match

raise info 'inserting in seen_strings';

INSERT INTO seen_strings(cid, att_id, gram, tf,idf,norm)
select  f.cid,d.att_id, d.gram, d.tf,d.idf,d.norm
from new_string d, cluster_table f
where not exists(select * from temp_matches e where e.cid > 1)
and f.entity_id = $1
group by f.cid, d.att_id, d.gram, d.tf, d.idf, d.norm;

----strings done uptill here , now numeric updation

raise info ' inserting in seen_dists';
------ inserts in both cases, match or no match

insert into seen_dists(cid,name, value,stdev)
select f.cid, d.name, d.value,d.stdev
from new_dist d, cluster_table f
where f.entity_id = $1
and f.cid >1
group by f.cid, d.name, d.value, d.stdev;




---------------------- ok here is the point where you need to update your code with if else


if (hello%100 = 0) then

raise info 'distinct_entiti';

truncate table distinct_entiti;

insert into distinct_entiti(entities)
select count(distinct cid)
from seen_strings;

truncate table seen_idf;

raise info 'seen_idf';

insert into seen_idf(att_id,gram,idf)
select y.att_id,y.gram,( d.entities ) /(count(*)::float) as idf
from seen_strings y, distinct_entiti d
group by y.att_id,y.gram, d.entities;

raise info 'update public.seen_strings';

update seen_strings
set idf = d.idf
from seen_idf d
where seen_strings.att_id = d.att_id
and seen_strings.gram = d.gram;

raise info 'insert into seen_norms';

truncate table seen_norms;

insert into seen_norms(cid, att_id, norm)
     SELECT a.cid,a.att_id,round((sqrt(SUM((a.tf*b.idf)^2)))::numeric,5) norm
       FROM seen_strings a, seen_idf b
      WHERE a.gram = b.gram
        and a.att_id = b.att_id
         and a.tf > 0.00001
        and b.idf > 0.00001
   GROUP BY a.cid,a.att_id;

raise info ' update public.seen_strings';

update seen_strings
set norm = n.norm
from seen_norms n
where seen_strings.cid = n.cid
and seen_strings.att_id = n.att_id;


end if ;


raise info 'insert into seen_dist_sums';

truncate seen_dist_sums;
truncate seen_temp_dists;

insert into seen_dist_sums( name , n, sm , smsqr )
     SELECT name, COUNT(*) n,
            SUM(value::float)::float sm, SUM(value::float*value::float)::float smsqr
       FROM seen_dists
      GROUP BY name;

raise info 'insert into seen_temp_dists';


insert into seen_temp_dists( name,count, mean, stdev )
SELECT  name, n, sm/n mean, sqrt(abs((smsqr - sm*sm/n) / (n-1)) ) stdev
 FROM seen_dist_sums
 WHERE n > 1;


update seen_dists
set stdev =  a.stdev
from seen_temp_dists a
where a.name = seen_dists.name;

raise info 'one_entity_over';

END
$$ LANGUAGE plpgsql;


drop table if exists jj;
drop table if exists scoringsfp1 cascade;
drop table if exists scoringsfn1 cascade;
drop table if exists fp;
drop table if exists fp1;
drop table if exists table231;
drop table if exists table232;
drop table if exists fn1;
drop table if exists fn;

drop table if exists fp2;
drop table if exists fp3 cascade;
drop table if exists fn2;
drop table if exists fn3 cascade;


create table jj(global_id integer, cid integer, entity_id integer);

--create table scoringsfp(cid integer, score integer);
create table fp1(global_id integer, cid integer, entity_id integer);
create table table231(cid integer, count1 integer, count2 integer);
create table fp(cid integer, count integer);
create table fp2(global_id integer,cid integer, count integer);
create table fp3(entity_id integer);


--create table scoringsfn(global_id integer, score integer);
create table fn1(global_id integer, cid integer, entity_id integer);
create table fn2(global_id integer,cid integer, count integer);
create table fn3(entity_id integer);
create table  table232(global_id integer, count1 integer, count2 integer);
create table fn(global_id integer, count integer);
create table scoringsfp1(count integer);
create table scoringsfn1(count integer);



CREATE OR REPLACE FUNCTION fpfn(integer) RETURNS void AS
$$
BEGIN

raise info 'truncating tables';

truncate table fn;
truncate table  fn1;
truncate table fn2;
truncate table fn3;

truncate table fp;
truncate table fp1;
truncate table  fp2;
truncate table fp3;
truncate table  jj;
truncate table scoringsfp1;
truncate table scoringsfn1;

raise info ' insert into jj';

insert into jj(global_id, cid,  entity_id)
select d.global_id, e.cid, e.entity_id
from cluster_table e, test_matching d
where d.local_id = e.entity_id
group by d.global_id, e.cid, e.entity_id;


raise info 'fp';

insert into fp(cid, count)
select cid , count(*) from jj
group by cid;

insert into fn(global_id, count)
select global_id, count(*) from jj
group by global_id;


insert into fp1(global_id, cid, entity_id)
select e.global_id, e.cid, e.entity_id
from jj e, fp d
where d.count>1 and d.cid = e.cid;

insert into fp2(global_id,cid, count)
select d.global_id, d.cid, count(*)
from fp1 d
group by d.global_id, d.cid;

insert into fp3(entity_id)
select distinct d.entity_id
from fp1 d, fp2 e
where d.global_id = e.global_id
and d.cid = e.cid
and e.count = 1;



insert into fn1(global_id, cid, entity_id)
select e.global_id, e.cid, e.entity_id
from jj e, fn d
where d.count>1 and d.global_id = e.global_id;

insert into fn2(global_id,cid, count)
select d.global_id, d.cid, count(*)
from fn1 d
group by d.global_id, d.cid;

insert into fn3(entity_id)
select distinct d.entity_id
from fn1 d, fn2 e

delete from fn3
where exists(select d.entity_id from fp3 d where  d.entity_id = fn3.entity_id);


insert into  scoringsfp1(count)
 select count(distinct entity_id) as count
from fp3;

insert into  scoringsfn1(count)
 select count(distinct entity_id) as count
from fn3;


insert into output48(category_id,entity, fp, fn)
select $1, count(distinct d.entity_id), k.count, p.count
from test_category d, scoringsfp1 k, scoringsfn1 p
group by k.count, p.count;


end
$$ LANGUAGE plpgsql;




