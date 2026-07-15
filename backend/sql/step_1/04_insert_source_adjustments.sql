insert into source_file_versions

values
    ('country_codes', {country_codes_source_file}, {country_codes_source_version}),
    ('extrastate_wars', {extrastate_wars_source_file}, {extrastate_wars_source_version}),
    ('interstate_mid_dyads', {interstate_mid_dyads_source_file}, {interstate_mid_dyads_source_version}),
    ('interstate_war_dyads', {interstate_war_dyads_source_file}, {interstate_war_dyads_source_version}),
    ('interstate_wars', {interstate_wars_source_file}, {interstate_wars_source_version}),
    ('intrastate_wars', {intrastate_wars_source_file}, {intrastate_wars_source_version});

insert into source_interstate_mid_war_num_adjustments

values
    ('interstate_mid_dyads', '4.03', 3582::double, 139::double),
    ('interstate_mid_dyads', '4.03', 3583::double, 139::double),
    ('interstate_mid_dyads', '4.03', 3585::double, 139::double),
    ('interstate_mid_dyads', '4.03', 4182::double, 4182::double),
    ('interstate_mid_dyads', '4.03', 4339::double, 905::double);

insert into source_interstate_war_metadata_adjustments

values
    ('interstate_mid_dyads', '4.03', 4182::double, 'Israeli–Hezbollah Conflict (South Lebanon)', 1::integer);

insert into source_participant_side_adjustments

values
    ('interstate_mid_dyads', '4.03', 4182::double, 660::integer, 'Lebanon', 1::integer),
    ('interstate_mid_dyads', '4.03', 4182::double, 666::integer, 'Israel', 2::integer);
