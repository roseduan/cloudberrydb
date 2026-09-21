create or replace function print_text_arrays(
    in array1 text[],
    in array2 text[]
)
returns text[]
language plpgsql volatile
as $$
begin
	return array1 || array2;
end;
$$;

create table outer_sublink as
select 'a' as col1, array[null] as col2
union all
select 'b' as col1, array[null] as col2
union all
select 'c' as col1, array['abc','def'] as col2
;

explain select * from outer_sublink where exists (select unnest(print_text_arrays(col2,col2)) order by 1) order by 1;
select * from outer_sublink where exists (select unnest(print_text_arrays(col2,col2)) order by 1) order by 1;
explain select * from outer_sublink where '{abc,def,abc,def}' in (select print_text_arrays(col2 ,col2)) order by 1;
select * from outer_sublink where '{abc,def,abc,def}' in (select print_text_arrays(col2 ,col2)) order by 1;
explain select (select print_text_arrays(col2, col2)) from outer_sublink order by 1;
select (select print_text_arrays(col2, col2)) from outer_sublink order by 1;
explain select (select unnest(print_text_arrays(col2, col2)) order by 1 limit 1) from outer_sublink order by 1;
select (select unnest(print_text_arrays(col2, col2)) order by 1 limit 1) from outer_sublink order by 1;

drop function print_text_arrays;
drop table outer_sublink;
