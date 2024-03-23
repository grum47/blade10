-- create schemas
create schema if not exists blade;
create schema if not exists blade_raw;
create schema if not exists blade_clean;
create schema if not exists blade_dwh;
create schema if not exists blade_datavault;
create schema if not exists blade_anchorn;

-- create tables
create table if not exists blade.areas (
    area_id int not null,
    parent_id int not null,
    name varchar(255)
);

create table if not exists blade.metro_lines (
    metro_line_id int not null,
    city_id int not null,
    name varchar(255),
    hex_color varchar(255) not null
);

create table if not exists blade.metro_station (
    metro_station_id int not null,
    metro_line_id int not null,
    order_station int not null,
    lat double precision not null,
    lon double precision not null,
    name varchar(255)
);





/*
create table if not exists blade.gender (
    gender_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employment (
    employment_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.experience (
    experience_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employer_active_vacancies_order (
    employer_active_vacancies_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.job_search_statuses_employer (
    job_search_statuses_employer_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.working_days (
    working_days_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.applicant_comment_access_type (
    applicant_comment_access_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.business_trip_readiness (
    business_trip_readiness_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_order (
    resume_search_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_fields (
    resume_search_fields_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.applicant_negotiation_status (
    applicant_negotiation_status_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.working_time_intervals (
    working_time_intervals_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.negotiations_state (
    negotiations_state_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_relocation (
    resume_search_relocation_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.applicant_comments_order (
    applicant_comments_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employer_hidden_vacancies_order (
    employer_hidden_vacancies_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.job_search_statuses_applicant (
    job_search_statuses_applicant_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.currency (
    currency_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_access_type (
    resume_access_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.education_level (
    education_level_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_moderation_note (
    resume_moderation_note_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_cluster (
    vacancy_cluster_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_search_fields (
    vacancy_search_fields_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employer_type (
    employer_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_type (
    vacancy_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_search_order (
    vacancy_search_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.working_time_modes (
    working_time_modes_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_status (
    resume_status_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_relation (
    vacancy_relation_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employer_relation (
    employer_relation_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.negotiations_participant_type (
    negotiations_participant_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.preferred_contact_type (
    preferred_contact_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.travel_time (
    travel_time_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_billing_type (
    vacancy_billing_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.relocation_type (
    relocation_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.messaging_status (
    messaging_status_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_hidden_fields (
    resume_hidden_fields_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.schedule (
    schedule_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_logic (
    resume_search_logic_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.employer_archived_vacancies_order (
    employer_archived_vacancies_order_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_not_prolonged_reason (
    vacancy_not_prolonged_reason_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.phone_call_status (
    phone_call_status_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.driver_license_types (
    driver_license_types_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.language_level (
    language_level_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_label (
    resume_search_label_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_search_experience_period (
    resume_search_experience_period_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.resume_contacts_site_type (
    resume_contacts_site_type_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.vacancy_label (
    vacancy_label_id varchar(255) not null,
    name varchar(255) not null
);


create table if not exists blade.negotiations_order (
    negotiations_order_id varchar(255) not null,
    name varchar(255) not null
);

*/