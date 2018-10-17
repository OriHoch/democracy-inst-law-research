select
a.bill_id "מזהה הצעת החוק",
b."Name" "שם הצעת החוק",
a.status_desc "תיאור האירוע",
a.start_date "תאריך האירוע",
a.committee_name "שם הועדה"--,
-- a.item_name "שם הנושא בישיבה הקשור להצעת החוק"

from (

select i."ItemID" bill_id, s."StartDate" start_date, t."Desc" status_desc, 'מליאה' committee_name, i."Name" item_name
from plenum_kns_plmsessionitem i, plenum_kns_plenumsession s, bills_kns_bill b, knesset_kns_status t
where i."ItemTypeID" = 2 and i."StatusID" = t."StatusID" and i."ItemID" = b."BillID"
and i."PlenumSessionID" = s."PlenumSessionID" and b."SubTypeDesc" = 'ממשלתית'
-- and b."BillID" in (2065306,2065306,2065305,2065304)

union

select i."ItemID" bill_id, s."StartDate" start_date, t."Desc" status_desc, c."Name" committee_name, i."Name" item_name
from committees_kns_cmtsessionitem i, committees_kns_committeesession s, bills_kns_bill b, knesset_kns_status t, committees_kns_committee c
where i."ItemTypeID" = 2 and i."StatusID" = t."StatusID" and i."ItemID" = b."BillID" and i."CommitteeSessionID" = s."CommitteeSessionID"
and c."CommitteeID" = s."CommitteeID" and b."SubTypeDesc" = 'ממשלתית'
-- and b."BillID" in (2065306,2065306,2065305,2065304)

union

select b."BillID" bill_id, b."PublicationDate" start_date, 'פרסום החוק או התיקון' status_desc, '' mmittee_name,  '' item_name
from bills_kns_bill b
where b."SubTypeDesc" = 'ממשלתית' and b."PublicationDate" is not null
-- and b."BillID" in (2065306,2065306,2065305,2065304)

union

select i."ItemID" bill_id, v.vote_date start_date, concat('הצבעה - ', v.vote_item_dscr, ' ', v.sess_item_dscr) status_desc,
c."Name" committee_name, i."Name" item_name
from votes_view_vote_rslts_hdr_approved v, committees_kns_cmtsessionitem i,
committees_kns_committeesession s, committees_kns_committee c
where v.sess_item_id = i."ItemID" and i."ItemTypeID" = 2
and i."CommitteeSessionID" = s."CommitteeSessionID" and c."CommitteeID" = s."CommitteeID"
-- and i."ItemID" in (2065306,2065306,2065305,2065304)

) a,

(

select * from bills_kns_bill where "SubTypeDesc" = 'ממשלתית'
-- and "BillID" in (2065306,2065306,2065305,2065304)

) b

where a.bill_id = b."BillID"

order by a.bill_id desc, a.start_date
