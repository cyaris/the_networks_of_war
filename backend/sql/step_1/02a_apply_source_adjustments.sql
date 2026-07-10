create or replace table source_file_versions as

select *
from (
    values
        ('country_codes', {country_codes_source_file}, {country_codes_source_version}, {country_codes_documentation}),
        ('extrastate_wars', {extrastate_wars_source_file}, {extrastate_wars_source_version}, {extrastate_wars_documentation}),
        ('interstate_mid_dyads', {interstate_mid_dyads_source_file}, {interstate_mid_dyads_source_version}, {interstate_mid_dyads_documentation}),
        ('interstate_war_dyads', {interstate_war_dyads_source_file}, {interstate_war_dyads_source_version}, {interstate_war_dyads_documentation}),
        ('interstate_wars', {interstate_wars_source_file}, {interstate_wars_source_version}, {interstate_wars_documentation}),
        ('intrastate_wars', {intrastate_wars_source_file}, {intrastate_wars_source_version}, {intrastate_wars_documentation}),
        ('war_types', {war_types_source_file}, {war_types_source_version}, {war_types_documentation})
) as t(source_key, source_file, source_version, documentation);

create or replace table source_interstate_mid_war_num_adjustments as

select *
from (
    values
        (
            'mid-4.02-disno-3582-wwii',
            'interstate_mid_dyads',
            '4.02',
            3582::double,
            139::double,
            'MID 4.02 war dyads are missing a directed interstate-war mapping; manual review assigns the dispute to World War II.'
        ),
        (
            'mid-4.02-disno-3583-wwii',
            'interstate_mid_dyads',
            '4.02',
            3583::double,
            139::double,
            'MID 4.02 war dyads are missing a directed interstate-war mapping; manual review assigns the dispute to World War II.'
        ),
        (
            'mid-4.02-disno-3585-wwii',
            'interstate_mid_dyads',
            '4.02',
            3585::double,
            139::double,
            'MID 4.02 war dyads are missing a directed interstate-war mapping; manual review assigns the dispute to World War II.'
        ),
        (
            'mid-4.02-disno-4182-south-lebanon',
            'interstate_mid_dyads',
            '4.02',
            4182::double,
            4182::double,
            'MID 4.02 includes Lebanon-Israel war dyads without a corresponding COW interstate war number; use the MID dispute number as a synthetic war id.'
        ),
        (
            'mid-4.02-disno-4339-africas-world-war',
            'interstate_mid_dyads',
            '4.02',
            4339::double,
            905::double,
            'MID 4.02 war dyads are missing a directed interstate-war mapping; manual review assigns the dispute to Africa''s World War.'
        )
) as t(adjustment_id, source_key, source_version, disno, war_num, rationale);

insert into source_interstate_war_dyads (war_num, disno)

select
    a.war_num,
    a.disno
from source_interstate_mid_war_num_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
where not exists (
    select 1
    from source_interstate_war_dyads c
    where
        c.disno = a.disno
        and c.war_num is not null
);

create or replace table source_interstate_war_metadata_adjustments as

select *
from (
    values
        (
            'mid-4.02-war-4182-name',
            'interstate_mid_dyads',
            '4.02',
            4182::double,
            'Israeli–Hezbollah Conflict (South Lebanon)',
            1::integer,
            'Synthetic war metadata for the MID 4.02 Lebanon-Israel war dyads assigned to synthetic war_num 4182.'
        )
) as t(adjustment_id, source_key, source_version, war_num, war_name, war_type, rationale);

insert into source_interstate_wars (war_num, war_name, war_type)

select
    a.war_num,
    a.war_name,
    a.war_type
from source_interstate_war_metadata_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
where not exists (
    select 1
    from source_interstate_wars c
    where c.war_num = a.war_num
);
