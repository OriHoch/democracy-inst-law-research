select
bills_kns_bill."BillID" "מזהה ייחודי להצעות חוק",
bills_kns_bill."Number" "המספר של הצעת החוק הממשלתית",
-- knesset_kns_status."StatusID",
knesset_kns_status."Desc" "סטטוס הצעת החוק הממשלתית",
bills_kns_bill."KnessetNum" "מספר הכנסת",
bills_kns_bill."IsContinuationBill" "האם הוחל על הצעת החוק דין רציפות",
bills_kns_bill."PostponementReasonDesc" "סיבת העצירה",
bills_kns_bill."PublicationSeriesFirstCall" "סדרת הפרסום לקריאה הראשונה",
committees_kns_committee."Name" "הועדה המטפלת",
bills_kns_bill."Name" "שם ההצעה",
billunion_main_bill."MainBillID" "מזהה ההצעה שאליה מוזגה ההצעה",
bills_kns_billsplit."SplitBillID" "מזהה ההצעה לפיצוול",
bills_kns_billsplit."Name" "שם מוצע לפיצול",
bill_documents.document_types "תיאור סוגי המסמכים הקשורים",
bill_names.change_desc "סוגי שינויים לשם ההצעה"

from
bills_kns_bill
left join knesset_kns_status on bills_kns_bill."StatusID" = knesset_kns_status."StatusID"
left join committees_kns_committee on committees_kns_committee."CommitteeID" = bills_kns_bill."CommitteeID"
left join bills_kns_billunion billunion_main_bill on bills_kns_bill."BillID" = billunion_main_bill."UnionBillID"
left join bills_kns_billsplit on bills_kns_billsplit."MainBillID" = bills_kns_bill."BillID"
left join (select "BillID", array_to_string(array_agg(distinct "GroupTypeDesc"), ' | ') document_types from bills_kns_documentbill group by "BillID") bill_documents
          on bill_documents."BillID" = bills_kns_bill."BillID"
left join (select "BillID", array_to_string(array_agg(distinct "NameHistoryTypeDesc"), ' | ') change_desc from bills_kns_billname group by "BillID") bill_names
          on bill_names."BillID" = bills_kns_bill."BillID"

where bills_kns_bill."SubTypeDesc" = 'ממשלתית'
-- not approved
and knesset_kns_status."StatusID" = 118
-- and bills_kns_bill."KnessetNum" = 20

order by bills_kns_bill."StatusID", bills_kns_bill."KnessetNum" desc, bills_kns_bill."BillID" desc
