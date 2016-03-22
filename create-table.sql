CREATE TABLE records (
    activity_nr     INTEGER NOT NULL,
    reporting_id    CHARACTER VARYING(7) NOT NULL,
    estab_name      CHARACTER VARYING(255),
    site_address    CHARACTER VARYING(255),
    site_city       CHARACTER VARYING(30),
    site_state      CHARACTER VARYING(25),
    site_zip        CHARACTER VARYING(5),
    owner_type      CHARACTER VARYING(5),
    sic_code        CHARACTER VARYING(4),
    naics_code      CHARACTER VARYING(6),
    union_status    CHARACTER VARYING(4),
    nr_in_estab     INTEGER,
    open_date       CHARACTER VARYING(10)
)
