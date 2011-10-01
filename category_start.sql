-- algorithm starts at start_this() function below where a category id is passed and then entities that belong to a
-- particular source within this category are passed incrementally to the function entity_match() in entity.sql file.
   


CREATE or REPLACE function sources(integer) returns void as 
$$
begin

INSERT INTO in_fields (source_id, name, tag_code)
       SELECT source_id, name, tag_code
         FROM public.doit_fields 
        WHERE source_id = $1;
end
$$ LANGUAGE plpgsql;






-- a category_id is passed here and the data that belongs to that category is put into tables 

CREATE OR REPLACE FUNCTION start_this(integer) RETURNS void AS
$$
BEGIN

truncate table public.seen_strings;
truncate table prefinal_dists;
truncate table prefinal_strings;
truncate table tableB;
truncate table in_fields ;
truncate table in_data ;
truncate table in_data1 ;
truncate table distinct_sources ;
truncate table global_attributes ;
truncate table attribute_clusters ;
truncate table distinct_entity; 
truncate table cluster_table;
truncate table test_category;
truncate table test_matching;
truncate table test_entity;


----test category will have data from the entities present in the Goby match for the category passed above


INSERT INTO test_entity(entity_id) 
select distinct entity_id 
from public.doit_data 
where category_id = $1;

INSERT INTO test_matching(global_id, local_id) 
select d.entity_id, d.result_id 
from public.entity_result d,test_entity e 
where d.result_id = e.entity_id;

INSERT INTO test_category(source_id, entity_id,name, value) 
select d.source_id, d.entity_id, d.name, d.value 
from public.doit_data d 
where exists(select e.local_id from test_matching e where e.local_id = d.entity_id);


INSERT INTO in_data (source_id, entity_id, name, value)
SELECT source_id, entity_id, name, value
FROM test_category
where value is not null;


INSERT INTO in_data1(source_id, entity_id, name, value)
select source_id,entity_id, name,value
from in_data;

alter table in_data1 alter column value type text using substring(value from 1 for 130);


INSERT INTO distinct_entity(source_id, c)
select source_id, count(distinct entity_id) 
from in_data1
group by source_id;

INSERT INTO distinct_ents(source_id, entity_id)
select source_id, entity_id
from in_data1
group by source_id, entity_id;


INSERT INTO distinct_sources
select source_id from distinct_entity;


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

perform source(source_id) from distinct_sources;

END
$$ LANGUAGE plpgsql;



------here a source_id is passed one at a time from the category_id passed above

CREATE OR REPLACE FUNCTION source(integer) RETURNS void AS
$$
BEGIN

RAISE INFO 'NEW SOURCE_ID';

----- getting the global_id's for the incoming columns

INSERT INTO att_ids(source_id,entity_id,att_id,value)
select i.source_id, i.entity_id, g.global_id as att_id, i.value
from in_data1 i, attribute_clusters g
where i.source_id = g.local_source_id
and i.name = g.local_name
and i.source_id = $1
and to_num(i.value::text) is NULL
group by i.source_id,i.entity_id, g.global_id, i.value;

RAISE INFO 'ATT_IDS DONE';

----- uniqueness weight for string columns

INSERT INTO UC_strings(att_id, weight)
select att_id, ((count(distinct value)::float)/(count(value)::float)) * (log(15,(count(value)::numeric))) as weight
from att_ids
group by att_id;


INSERT INTO dists(entity_id, name, value)
select entity_id, name, value
from in_data1 
where to_num(value::text) is not NULL
and source_id = $1
group by entity_id, name, value;


------- uniqueness weight for numerical columns

INSERT INTO UC_dists(name, weight)
select name, ((count(distinct value)::float)/(count(value)::float)) *  (log(15,(count(value)::numeric))) as weight
 from dists
group by name;


------  ngrams and tf, idf and norms(normalization factor) for incoming columns

INSERT INTO  in_val_ngrams_yo(source_id , entity_id, att_id, gram)
SELECT source_id, entity_id, att_id, tokenize(value) gram
from att_ids
where to_num(value::text) is NULL
;

RAISE INFO 'NGRAMS DONE';

INSERT INTO denr(entity_id, att_id, b)
select entity_id, att_id, count(gram)
from in_val_ngrams_yo
group by entity_id, att_id;



INSERT INTO numr(entity_id, att_id, gram, a)
select entity_id, att_id, gram, count(gram) 
from in_val_ngrams_yo
group by entity_id,att_id,gram;


INSERT INTO in_strings(entity_id, att_id, gram, tf, idf, norm) 
select n.entity_id, n.att_id, n.gram, n.a/d.b as tf, NULL, NULL
from numr n, denr d
where d.entity_id = n.entity_id
and d.att_id = n.att_id
group by n.entity_id,n.att_id,n.gram, n.a, d.b;



INSERT INTO in_idf(att_id, gram,idf)
select y.att_id,y.gram,( d.c::float ) /(count(*)::float) as idf
from in_val_ngrams_yo y, distinct_entity d
where d.source_id =$1
group by y.att_id,y.gram, d.c;

update in_strings
set idf = d.idf
from in_idf d
where in_strings.att_id = d.att_id
and in_strings.gram = d.gram;


INSERT INTO in_norms(entity_id, att_id, norm)
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


-- entities present within a source is passed  incrementally into this function, distinct_ents has columns
-- source_id, entity_id present within a category, this function is in entity.sql file


perform entity_match(entity_id) from distinct_ents where source_id = $1;



truncate table UC_strings;
truncate table UC_dists;
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
truncate table temp_strings;
truncate table distinct_entiti;
truncate table tableA;
truncate table  temp_str;
truncate table temp_dist;
truncate table temp_score;

end
$$ LANGUAGE plpgsql;










