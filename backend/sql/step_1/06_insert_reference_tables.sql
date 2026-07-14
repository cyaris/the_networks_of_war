insert into war_types

values
    (1::integer, 'Inter-State War', null),
    (2::integer, 'Extra-State War', 'Colonial (conflict with colony)'),
    (3::integer, 'Extra-State War', 'Imperial (state vs non-state)'),
    (4::integer, 'Intra-State War', 'Civil War (for central control)'),
    (5::integer, 'Intra-State War', 'Civil War (over local issues)'),
    (6::integer, 'Intra-State War', 'Regional/Internal'),
    (7::integer, 'Intra-State War', 'Intercommunal'),
    (8::integer, 'Non-State War', 'In Non-State Territory'),
    (9::integer, 'Non-State War', 'Across State Borders');
