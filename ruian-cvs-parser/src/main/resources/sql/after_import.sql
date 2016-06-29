/*
create table vh_ruian_obce_ulice as
select obec_kod,obec_nazev,obecmomc_nazev,obecmop_nazev,cobce_kod,cobce_nazev,ulice_nazev,psc,okres_kod,okres_nazev 
from vh_ruian r 
group by obec_kod,obec_nazev,obecmomc_nazev,obecmop_nazev,cobce_kod,cobce_nazev,ulice_nazev,psc,okres_kod,okres_nazev;
*/


create table vh_ruian_cobce as
select obec_kod,obec_nazev,obecmomc_nazev,obecmop_nazev,cobce_kod,cobce_nazev,psc,okres_kod,okres_nazev 
from vh_ruian r 
group by obec_kod,obec_nazev,obecmomc_nazev,obecmop_nazev,cobce_kod,cobce_nazev,psc,okres_kod,okres_nazev;


create index vh_ruian_cobce_idx1 on vh_ruian_cobce(obec_nazev);
create index vh_ruian_cobce_idx2 on vh_ruian_cobce(cobce_nazev);
create index vh_ruian_cobce_idx3 on vh_ruian_cobce(obecmomc_nazev);



create table vh_ruian_sobec as
select o.obec_kod, null as cobce_kod, o.obec_nazev as sobec_nazev, 'okres: '||o.okres_nazev as sobec_desc from vh_ruian_cobce o
where exists (select 1 from vh_ruian_cobce o2 where (o.obec_nazev=o2.obec_nazev or o.obec_nazev=o2.cobce_nazev or o.obec_nazev=o2.obecmomc_nazev) and o.obec_kod!=o2.obec_kod)
union
select o.obec_kod, null as cobce_kod, o.obec_nazev as sobec_nazev, '' as sobec_desc from vh_ruian_cobce o
where not exists (select 1 from vh_ruian_cobce o2 where (o.obec_nazev=o2.obec_nazev or o.obec_nazev=o2.cobce_nazev or o.obec_nazev=o2.obecmomc_nazev) and o.obec_kod!=o2.obec_kod)
--praha
/*
union
select o.obec_kod, null as cobce_kod, o.obecmop_nazev as sobec_nazev, '' as sobec_desc from vh_ruian_cobce o
where o.obecmop_nazev is not null 
  and o.obecmomc_nazev!=o.obecmop_nazev 
union
--praha
union
select o.obec_kod, null as cobce_kod, o.obecmomc_nazev as sobec_nazev, '' as sobec_desc from vh_ruian_cobce o
where o.obecmomc_nazev is not null
  and o.obec_kod=554782
  and o.obecmomc_nazev like 'Praha %'
*/
--praha
union
select o.obec_kod, null as cobce_kod, substr(o.obecmomc_nazev,7) as sobec_nazev, 'Praha' as sobec_desc from vh_ruian_cobce o
where o.obecmomc_nazev is not null
  and o.obec_kod=554782
  and o.obecmomc_nazev like 'Praha-%'
--obce v praze
union
select o.obec_kod, null cobce_kod, o.cobce_nazev as sobec_nazev, 'Praha' as sobec_desc from vh_ruian_cobce o
where o.cobce_nazev!=o.obec_nazev 
  and o.cobce_nazev is not null
  and o.obec_kod=554782
--obce v praze
union
select o.obec_kod, null cobce_kod, substr(o.obecmomc_nazev,7) as sobec_nazev, 'Praha' as sobec_desc from vh_ruian_cobce o
where o.obecmomc_nazev!=o.obec_nazev 
  and o.obecmomc_nazev is not null
  and o.obecmomc_nazev like 'Praha-%'
  and o.obec_kod=554782
--obce mimo prahu
union
select o.obec_kod, o.cobce_kod, o.cobce_nazev as sobec_nazev, o.obec_nazev as sobec_desc from vh_ruian_cobce o
where o.cobce_nazev!=o.obec_nazev 
  and o.cobce_nazev is not null
  and o.cobce_nazev!='Město'
  and o.obec_kod!=554782
--obce mimo prahu
union
select o.obec_kod, null cobce_kod, o.obecmomc_nazev as sobec_nazev, o.obec_nazev as sobec_desc from vh_ruian_cobce o
where o.obecmomc_nazev!=o.obec_nazev 
  and o.obecmomc_nazev is not null
  and o.obecmomc_nazev!='Město'
  and o.obec_kod!=554782
;


alter table vh_ruian_sobec add (sobec_nazev_upper varchar as upper(sobec_nazev));
create index vh_ruian_sobec_idx1 on vh_ruian_sobec(sobec_nazev_upper);

alter table vh_ruian_sobec add (sobec_nazev_stripped varchar as translate(upper(sobec_nazev),'ĚÉŠČŘŔŽÝÁÄÍÚŮÜÓÔÖŤĹĽĎŇ','EESCRRZYAAIUUUOOOTLLDN'));
create index vh_ruian_sobec_idx2 on vh_ruian_sobec(sobec_nazev_stripped);


--tabulka kod obce - psc
create table vh_ruian_psc as
select obec_kod,psc
from vh_ruian r 
group by obec_kod,psc;

create index vh_ruian_psc_idx1 on vh_ruian_psc(obec_kod);
create index vh_ruian_psc_idx2 on vh_ruian_psc(psc);


--uklid nepotrebnych tabulek
drop table vh_ruian_cobce;
drop table vh_ruian;


  