drop table vvo_sved_dohod_rashod;
create table vvo_sved_dohod_rashod (
ddate date,
inn varchar2(10),
title varchar2(512),
dohod number,
rashod number
);
commit;
select count(*) from vvo_sved_dohod_rashod;
