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

-----takes source_id, category_id as an input 

CREATE OR REPLACE FUNCTION UC_scores1( integer, integer) RETURNS void AS
$$
DECLARE

small integer;

BEGIN

truncate table UC_strings;
Truncate table UC_dists;
truncate table in_dist_sums;
truncate table temp_dists;
truncate table in_dists;
truncate table dists;
truncate table in_val_ngrams_yo;
truncate table denr;
truncate table numr;
truncate table in_strings;
truncate table in_idf;
truncate table in_norms;
truncate table temp_dist;
truncate table temp_score;
truncate table UC_mean;
truncate table att_ids;



insert into att_ids(source_id,entity_id,att_id,value)
select i.source_id, i.entity_id, g.global_id as att_id, i.value
from in_data1 i, attribute_clusters g
where i.source_id = g.local_source_id
and i.name = g.local_name
and i.source_id = $1
and to_num(i.value::text) is NULL
group by i.source_id,i.entity_id, g.global_id, i.value;


insert into UC_strings(att_id, weight)
select att_id, ((count(distinct value)::float)/(count(value)::float))*(log(2.0,(count(value)::numeric)))  as weight
from att_ids
group by att_id;

insert into dists(entity_id, name, value)
select entity_id, name, value
from in_data1
where to_num(value::text) is not NULL
and source_id = $1
group by entity_id, name, value;

insert into UC_dists(name, weight)
select name, ((count(distinct value)::float)/(count(value)::float))*(log(2.0,(count(value)::numeric)))  as weight
 from dists
group by name;

truncate table UC_mean;

insert into UC_mean(mean, count)
select (sum(s.weight)+sum(d.weight))::float /(count(distinct s.att_id)+count(distinct d.name))::float, (count(distinct s.att_id)+count(distinct d.name))::float
from UC_strings s, UC_dists d;


truncate table in_dist_sums;

insert into in_dist_sums( name , n, sm , smsqr )
     SELECT name, COUNT(*) n,
            SUM(value::float)::float sm, SUM(value::float*value::float)::float smsqr
       FROM dists
       GROUP BY name;


truncate table temp_dists;

insert into temp_dists( name,count, mean, stdev )
SELECT name, n, sm/n mean, sqrt( (smsqr - sm*sm/n) / (n-1) ) stdev
 FROM in_dist_sums
 WHERE n > 1;


truncate table in_dists;

insert into in_dists(entity_id, name, value, stdev)
select i.entity_id , i.name, i.value, d.stdev
from dists i, temp_dists d
where i.name = d.name
group by i.entity_id, i.name, i.value, d.stdev;


truncate table in_val_ngrams_yo;
truncate table denr;
truncate table numr;

insert into  in_val_ngrams_yo(source_id , entity_id, att_id, gram)
SELECT source_id, entity_id, att_id, tokenize(value) gram
from att_ids
where to_num(value::text) is NULL
;


insert into denr(entity_id, att_id, b)
select entity_id, att_id, count(gram)
from in_val_ngrams_yo
group by entity_id, att_id;



insert into numr(entity_id, att_id, gram, a)
select entity_id, att_id, gram, count(gram)
from in_val_ngrams_yo
group by entity_id,att_id,gram;

truncate table in_strings;
truncate table in_idf;

insert into in_strings(entity_id, att_id, gram, tf, idf, norm)
select n.entity_id, n.att_id, n.gram, n.a/d.b as tf, NULL, NULL
from numr n, denr d
where d.entity_id = n.entity_id
and d.att_id = n.att_id
group by n.entity_id,n.att_id,n.gram, n.a, d.b;



insert into in_idf(att_id, gram,idf)
select y.att_id,y.gram,( d.c::float ) /(count(*)::float) as idf
from in_val_ngrams_yo y, distinct_entity d
where d.source_id =$1
group by y.att_id,y.gram, d.c;

update in_strings
set idf = d.idf
from in_idf d
where in_strings.att_id = d.att_id
and in_strings.gram = d.gram;


truncate table in_norms;


insert into in_norms(entity_id, att_id, norm)
     SELECT a.entity_id,a.att_id, sqrt(SUM((a.tf*b.idf)^2)) norm
       FROM in_strings a, in_idf b
      WHERE a.gram = b.gram
        and a.att_id = b.att_id
   GROUP BY a.entity_id,a.att_id;


update in_strings
set norm = n.norm
from in_norms n
where in_strings.entity_id = n.entity_id
and in_strings.att_id = n.att_id;

perform UC_entity(entity_id, $2) from distinct_ents where source_id = $1;

end
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sources(integer)  RETURNS void AS
$$
BEGIN

INSERT INTO in_fields (source_id, name, tag_code)
       SELECT source_id, name, tag_code
         FROM public.doit_fields
        WHERE source_id = $1;
end
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION start_this(integer) RETURNS void AS
$$

BEGIN

raise info  'truncating tables';

truncate table seen_strings;
truncate table seen_dists;

raise info 'truncating more tables';


truncate table in_fields ;
truncate table in_data ;
truncate table in_data1 ;
truncate table distinct_sources ;
truncate table global_attributes ;
truncate table attribute_clusters ;
truncate table distinct_entity;
truncate table distinct_ents;



truncate table cluster_table;

truncate table test_category;

truncate table test_matching;

truncate table test_entity;


insert into test_entity(entity_id) select distinct entity_id from public.doit_data where category_id = $1;

insert into test_matching(global_id, local_id) select d.global_entity_id, d.local_entity_id from public.goby_entity_result d,test_entity e
where d.local_entity_id = e.entity_id;

insert into test_category(source_id, entity_id,name, value) select d.source_id, d.entity_id, d.name, d.value from public.doit_data d
where exists(select e.local_id from test_matching e where e.local_id = d.entity_id);


INSERT INTO in_data (source_id, entity_id, name, value)
       SELECT source_id, entity_id, name, value
         FROM test_category
     where value is not NULL;


RAISE INFO 'in_data has the data now';


insert into in_data1(source_id, entity_id, name, value)
       select source_id,entity_id, name,value
       from in_data;

alter table in_data1 alter column value type text using substring(value from 1 for 100);

insert into distinct_entity(source_id, c)
select source_id, count(distinct entity_id)
from in_data1
group by source_id;

insert into distinct_ents(source_id, entity_id)
select source_id, entity_id
from in_data1
group by source_id, entity_id;


insert into distinct_sources
select source_id from distinct_entity
order by c desc;


perform sources(source_id) from distinct_sources;


INSERT INTO global_attributes (source_id, name)
       SELECT MIN(source_id), tag_code
         FROM in_fields
     GROUP BY tag_code;

  -- Create training attribute clusters
 INSERT INTO attribute_clusters (global_id, global_name, local_source_id,
                                  local_name, uncertainty, authority)
       SELECT g.id, g.name, t.source_id, t.name, 0.0, 1.0
         FROM global_attributes g, in_fields t
        WHERE g.name = t.tag_code;

perform UC_scores1(source_id, $1) from distinct_sources;

perform fpfn($1);

END
$$ LANGUAGE plpgsql;


   
drop sequence if exists sequence1;
drop sequence if exists counter;

create sequence sequence1 start 1 increment 1;
create sequence counter start 1 increment 1;

drop table if exists cluster_table;
create table cluster_table(entity_id integer, cid integer);

drop table if exists in_data;
create table in_data(source_id integer, entity_id integer, name text, value text);


drop table if exists in_fields;
create table in_fields(source_id integer, name text, tag_code text);


DROP TABLE IF EXISTS global_attributes CASCADE;
CREATE TABLE global_attributes (id serial, source_id integer, name text);

DROP TABLE IF EXISTS attribute_clusters CASCADE;
CREATE TABLE attribute_clusters (global_id integer,global_name text,local_source_id integer,local_name text, uncertainty float,authority float);

create or replace function tokenize (text) returns setof text as
$$
begin
return query
select tokid || '||' || trim(both '{}' from ts_lexize('english_stem', token)::text)
from (
select * from ts_parse('default', $1)
where tokid != 12
and length(token) < 2048
) t;
end
$$ language plpgsql;


create or replace function to_num (s text) returns numeric as
$$
import math

if (s is None):
   return s

try:
    n = float(s)
    if (math.isinf(n) or math.isnan(n)):
        return None
    else:
        return n
except ValueError:
    return None

$$ language plpythonu;



drop table if exists output;
create table output(category_id integer, entity integer, fp integer, fn integer);

drop table if exists att_ids;
drop table if exists UC_strings;
drop table if exists in_dist_sums;
drop table if exists temp_dists;
drop table if exists in_dists;
drop table if exists in_val_ngrams_yo;
drop table if exists denr;
drop table if exists numr;
drop table if exists in_strings;
drop table if exists in_idf;
drop table if exists in_norms;
drop table if exists results;
drop table if exists temp_a;
drop table if exists final;
drop table if exists temp_matches;
drop table if exists temp_strings;
drop table if exists distinct_entiti;
drop table if exists seen_idf;
drop table if exists seen_norms;
drop table if exists seen_dist_sums;
drop table if exists seen_temp_dists;
drop table if exists final_dists;
drop table if exists final_strings;
drop table if exists results_dist;
drop table if exists UC_dists;
drop table if exists dists;
drop table if exists dists1;
drop table if exists table_temp;
drop table if exists tableA;
drop table if exists tableB;
drop table if exists  temp_str;
drop table if exists temp_dist;
drop table if exists temp_score;
drop table if exists UC_mean;
drop table if exists distinct_ents;
drop table if exists temp_scores;
drop table if exists new_string;
drop table if exists new_dist;
drop table if exists temp_matches_temp;
drop table if exists table_temp1;
drop table if exists seen_strings cascade;
drop table if exists prefinal_strings;
drop table if exists seen_dists;
drop table if exists in_data1;
drop table if exists distinct_sources;
drop table if exists distinct_entity;
drop table if exists test_category;
drop table if exists test_matching;
drop table if exists test_entity;
drop table if exists matched_string;
drop table if exists just_testing;
drop table if exists just_testing_norm;
drop table if exists just1;
drop table if exists just_entity;

create table just_entity(entity_id integer);
create table just1(entity_id integer);
create table just_testing_norm(att_id integer, norm float);
create table matched_string(att_id integer, gram text);
create table test_entity(entity_id integer);
create table test_matching(global_id integer, local_id integer);
create table test_category(source_id integer, entity_id integer,name text, value text);
create table distinct_entity(source_id integer, c float);
create table distinct_sources(source_id integer);

create table in_data1(source_id integer, entity_id integer, name text, value text);
create table seen_strings(cid integer, att_id integer,gram text,tf float, idf float, norm float);
create table just_testing(cid integer, att_id integer,gram text,tf float, idf float, norm float);

CREATE INDEX cid_idx ON seen_strings(cid);

CREATE INDEX gram_idx ON seen_strings(att_id,gram);

create table prefinal_strings(cid integer, att_id integer,gram text, tf float, idf float,norm float);
create table seen_dists(cid integer, name text, value text,stdev float);

create index name_idx on seen_dists(name);

create table distinct_ents(source_id integer, entity_id integer);
create table UC_mean(mean float,count float);
create table att_ids(source_id integer, entity_id integer, att_id integer, value text);
create table UC_strings(att_id integer, weight float);
create table UC_dists(name text, weight float);
create table in_dist_sums(name text, n float, sm float, smsqr float);
create table temp_dists( name text,count float,  mean float, stdev float);
create table dists(entity_id integer, name text, value text);
create table dists1(entity_id integer, name text, value text);
create table in_dists(entity_id integer, name text, value text, stdev float);
create table in_val_ngrams_yo(source_id integer, entity_id integer, att_id integer, gram text);
create table denr(entity_id integer, att_id integer, b float);
create table numr(entity_id integer, att_id integer, gram text, a float);
create table in_strings(entity_id integer, att_id integer, gram text, tf float, idf float, norm float);
create table in_idf( att_id integer, gram text, idf float);
create table in_norms(entity_id integer, att_id integer, norm float);

create table results(cid integer, att_id integer, score float);
create table results_dist( cid integer, name text, score float);
create table temp_a( cid integer,name text, score float);
create table final(cid integer, score float);
create table final_dists(cid integer, score float);
create table final_strings(entity_id integer, cid integer, score float);
create table temp_matches( cid integer);
create table temp_strings(entity_id integer, cid integer, att_id integer, gram text);
create table distinct_entiti(entities float);
create table seen_idf(att_id integer, gram text, idf float);
create table seen_norms(cid integer, att_id integer, norm float);
create table seen_dist_sums(name text, n float, sm float, smsqr float);
create table seen_temp_dists( name text,count float,  mean float, stdev float);
create table table_temp(cid integer, att_id integer, gram text, tf float, idf float, norm float);
create table temp_dist(sum float);
create table new_string(att_id integer, gram text, tf float, idf float, norm float);
create table new_dist( name text, value text, stdev float);
create table temp_matches_temp( cid integer, score float);
create table table_temp1(cid integer, att_id integer, gram text, tf float, idf float, norm float);

