create or replace macro clean_int(value) as (
    try_cast(nullif(trim(value::varchar), '') as integer)
);

create or replace macro clean_number(value) as (
    try_cast(nullif(trim(value::varchar), '') as double)
);

create or replace macro clean_text(value) as (
    if(nullif(trim(value::varchar), '') in ('-7', '-8', '-9'), null, nullif(trim(value::varchar), ''))
);

create or replace macro clean_date_month(value) as (
    if(clean_int(value) between 1 and 12, clean_int(value), null)
);

create or replace macro clean_date_day(value) as (
    if(clean_int(value) between 1 and 31, clean_int(value), null)
);

create or replace macro clean_date_year(value) as (
    if(clean_int(value) > 0, clean_int(value), null)
);

create or replace macro clean_end_year(value) as (
    case
        when clean_int(value) = -7 then -7
        when clean_int(value) > 0 then clean_int(value)
        else null
    end
);

create or replace macro clean_war_reference(value) as (
    if(clean_int(value) > 0, clean_int(value), null)
);

create or replace macro clean_participant(value) as (
    case
        when clean_text(value) = 'al-Qaeda & Iraqi resistence' then 'al-Qaeda & Iraqi Resistance'
        when clean_text(value) = 'Albanians and Bosnians' then 'Albanians & Bosnians'
        when clean_text(value) = 'Asir and Yemen Rebels' then 'Asir & Yemen Rebels'
        when clean_text(value) = 'Baron von Ungern-Sternbergs White army' then 'Baron von Ungern-Sternberg''s White army'
        when clean_text(value) = 'Beni Aseer tribe' then 'Beni Aseer Tribe'
        when clean_text(value) = 'Bharatpuran rebels' then 'Bharatpuran Rebels'
        when clean_text(value) = 'Bosnia and Herzegovina' then 'Bosnia & Herzegovina'
        when clean_text(value) = 'Bosnia-Herzegovina and Bulgaria' then 'Bosnia-Herzegovina & Bulgaria'
        when clean_text(value) = 'Bosnian and Montenegrin Beys' then 'Bosnian & Montenegrin Beys'
        when clean_text(value) = 'Damascus and Aleppo' then 'Damascus & Aleppo'
        when clean_text(value) = 'EPLF and ELF' then 'EPLF & ELF'
        when clean_text(value) = 'FNL and FDD' then 'FNL & FDD'
        when clean_text(value) = 'GMD and CCP' then 'GMD & CCP'
        when clean_text(value) = 'Janissaries' then 'Janissaries'
        when clean_text(value) = 'Javanese rebels' then 'Javanese Rebels'
        when clean_text(value) = 'Kandyan rebels' then 'Kandyan Rebels'
        when clean_text(value) = 'Karens and Communists' then 'Karens & Communists'
        when clean_text(value) = 'KDP and IMK' then 'KDP & IMK'
        when clean_text(value) = 'Kumuliks and Allies' then 'Kumuliks & Allies'
        when clean_text(value) = 'Lebanese Maronites and Druze' then 'Lebanese Maronites & Druze'
        when clean_text(value) = 'Leftist Rebels and Drug Cartels' then 'Leftist Rebels & Drug Cartels'
        when clean_text(value) = 'Liberals and Radicals' then 'Liberals & Radicals'
        when clean_text(value) = 'Mahdist rebels' then 'Mahdist Rebels'
        when clean_text(value) = 'Montenegro and Hercegovina' then 'Montenegro & Hercegovina'
        when clean_text(value) = 'Monteneros and ERP' then 'Monteneros & ERP'
        when clean_text(value) = 'Ninjas and Cocoye militias' then 'Ninjas & Cocoye militias'
        when clean_text(value) = 'NPFL and INPFL' then 'NPFL & INPFL'
        when clean_text(value) = 'Palestinians and Bedouins' then 'Palestinians & Bedouins'
        when clean_text(value) = 'Pathan tribe' then 'Pathan Tribe'
        when clean_text(value) = 'Pathan tribes' then 'Pathan Tribes'
        when clean_text(value) = 'PUK and INC' then 'PUK & INC'
        when clean_text(value) = 'RCD and MLC' then 'RCD & MLC'
        when clean_text(value) = 'Rif tribes' then 'Rif Tribes'
        when clean_text(value) = 'Sanusi tribe' then 'Sanusi Tribe'
        when clean_text(value) = 'Sao Tome and Principe' then 'Sao Tome & Principe'
        when clean_text(value) = 'Shaanxi and Gansu Muslims' then 'Shaanxi & Gansu Muslims'
        when clean_text(value) = 'Shi''ites and Druzes' then 'Shi''ites & Druzes'
        when clean_text(value) = 'Shiites and SAIRI' then 'Shiites & SAIRI'
        when clean_text(value) = 'Sierra Leone rebels' then 'Sierra Leone Rebels'
        when clean_text(value) = 'SPLA-Nasir and Anya Nya II' then 'SPLA-Nasir & Anya Nya II'
        when clean_text(value) = 'St. Kitts and Nevis' then 'St. Kitts & Nevis'
        when clean_text(value) = 'St. Vincent and the Grenadines' then 'St. Vincent & the Grenadines'
        when clean_text(value) = 'Trinidad and Tobago' then 'Trinidad & Tobago'
        when clean_text(value) = 'Turkey/Ottoman Empire/Egypt' then 'Turkey, Ottoman Empire & Egypt'
        when clean_text(value) = 'United States' then 'United States of America'
        when clean_text(value) = 'Uyghur and Kirghiz' then 'Uyghur & Kirghiz'
        when clean_text(value) = 'Wadai sultanate' then 'Wadai Sultanate'
        when clean_text(value) = 'Waziri tribes' then 'Waziri Tribes'
        when clean_text(value) = 'Workers and Peasants' then 'Workers & Peasants'
        when clean_text(value) = 'Yunnan and Guizhou' then 'Yunnan & Guizhou'
        when clean_text(value) = 'Zailis and Jinden' then 'Zailis & Jinden'
        when clean_text(value) = 'Zhili and Fengtien Factions' then 'Zhili & Fengtien Factions'
        when clean_text(value) = 'Zulu tribe' then 'Zulu Tribe'
        else clean_text(value)
    end
);

create or replace macro cow_date(year_value, month_value, day_value, default_month, default_day) as (
    if(
        clean_int(year_value) > 0,
        make_date(
            clean_int(year_value),
            coalesce(clean_date_month(month_value), default_month),
            least(coalesce(clean_date_day(day_value), default_day), extract(day from last_day(make_date(clean_int(year_value), coalesce(clean_date_month(month_value), default_month), 1)))::integer)
        ),
        null
    )
);

create or replace macro cow_end_date(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7, make_date(extract(year from current_date)::integer, 12, 31), cow_date(year_value, month_value, day_value, 12, 31))
);

create or replace macro date_estimated(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7 or (clean_int(year_value) > 0 and (clean_date_month(month_value) is null or clean_date_day(day_value) is null)), 1, 0)
);

create or replace macro ongoing_war(year_value) as (
    if(clean_int(year_value) = -7, 1, 0)
);
