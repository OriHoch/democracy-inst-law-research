select
i."ItemID" "מזהה הצעת החוק",
i."Name" "שם הנושא בישיבה הקשור להצעת החוק",
c."Name" "שם הועדה",
s."StartDate" "זמן תחילת הישיבה",
t."Desc" "סטטוס"


from committees_kns_cmtsessionitem i, committees_kns_committeesession s, bills_kns_bill b, knesset_kns_status t, committees_kns_committee c
where i."ItemTypeID" = 2 and i."StatusID" = t."StatusID" and i."ItemID" = b."BillID" and i."CommitteeSessionID" = s."CommitteeSessionID"
and c."CommitteeID" = s."CommitteeID"
and b."SubTypeDesc" = 'ממשלתית'
order by b."BillID" desc, s."StartDate" desc
