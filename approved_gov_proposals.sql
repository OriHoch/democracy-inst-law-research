select
bills_kns_bill."BillID" "מזהה ייחודי להצעות חוק",
bills_kns_bill."Number" "המספר של הצעת החוק הממשלתית",
bills_kns_bill."KnessetNum" "מספר הכנסת",
bills_kns_bill."MagazineNumber" "מספר חוברת הקשורה להצעת החוק",
bills_kns_bill."PageNumber" "מספר עמוד הקשור להצעת החוק",
laws_kns_israel_law."IsraelLawID" "מזהה חוק האב",
laws_kns_law_binding."BindingTypeDesc" "סוג הקשר לחוק האב",
laws_kns_law_binding."MagazineNumber" "מספר חוברת הקשורה לחוק האב",
laws_kns_law_binding."PageNumber" "מספר עמוד הקשור לחוק האב",
laws_kns_law_binding."AmendmentTypeDesc" "תיאור סוג התיקון",
laws_kns_israel_law."Name" "שם חוק האב",
laws_kns_israel_law."IsBasicLaw" "האם חוק האב הוא חוק יסוד?",
laws_kns_israel_law."IsFavoriteLaw" "האם חוק האב הוא חוק מפתח?",
laws_kns_israel_law."IsBudgetLaw" "האם חוק האב הוא חוק תקציב?",
laws_kns_israel_law."PublicationDate" "תאריך הפרסום לראשונה של חוק האב",
laws_kns_israel_law."LatestPublicationDate" "תאריך הפרסום של התיקון האחרון לחוק האב",
laws_kns_israel_law."LawValidityDesc" "תיאור תוקף החוק של חוק האב",
laws_kns_israel_law."ValidityStartDate" "תאריך תחילת תוקף של חוק האב",
laws_kns_israel_law."ValidityStartDateNotes" "הערה לתחילת התוקף של חוק האב",
laws_kns_israel_law."ValidityFinishDate" "תאריך פקיעה של חוק האב",
laws_kns_israel_law."ValidityFinishDateNotes" "הערות לתאריך הפקיעה של חוק האב",
israel_law_ministries.govministry_names "המשרדים הממשלתיים המשוייכים לחוק האב",
law_clasifications.clasifications "סיווג חוק האב במאגר החקיקה",
committees_kns_committee."Name" "הועדה המטפלת בהצעת החוק",
bills_kns_bill."Name" "שם הצעת החוק",
bill_documents.document_types "תיאור סוגי המסמכים הקשורים להצעה",
bill_names.change_desc "סוגי שינויים לשם שבוצעו לשם ההצעה",
bills_kns_bill."SummaryLaw" "תקציר החוק"

from
bills_kns_bill
left join knesset_kns_status on bills_kns_bill."StatusID" = knesset_kns_status."StatusID"
left join committees_kns_committee on committees_kns_committee."CommitteeID" = bills_kns_bill."CommitteeID"
left join (select "BillID", array_to_string(array_agg(distinct "GroupTypeDesc"), ' | ') document_types from bills_kns_documentbill group by "BillID") bill_documents
          on bill_documents."BillID" = bills_kns_bill."BillID"
left join (select "BillID", array_to_string(array_agg(distinct "NameHistoryTypeDesc"), ' | ') change_desc from bills_kns_billname group by "BillID") bill_names
          on bill_names."BillID" = bills_kns_bill."BillID"
left join laws_kns_law_binding on laws_kns_law_binding."LawID" = bills_kns_bill."BillID"
left join laws_kns_israel_law on laws_kns_israel_law."IsraelLawID" = laws_kns_law_binding."IsraelLawID"
left join (select laws_kns_israel_law_ministry."IsraelLawID", array_to_string(array_agg(distinct knesset_kns_govministry."Name"), ' | ') govministry_names
           from laws_kns_israel_law_ministry left join knesset_kns_govministry on laws_kns_israel_law_ministry."GovMinistryID" = knesset_kns_govministry."GovMinistryID"
           group by laws_kns_israel_law_ministry."IsraelLawID") israel_law_ministries on laws_kns_israel_law."IsraelLawID" = israel_law_ministries."IsraelLawID"
left join (select "IsraelLawID", array_to_string(array_agg(distinct "ClassificiationDesc"), ' | ') clasifications from laws_kns_israel_law_classification group by "IsraelLawID") law_clasifications
          on law_clasifications."IsraelLawID" = laws_kns_israel_law."IsraelLawID"

where
bills_kns_bill."SubTypeDesc" = 'ממשלתית'
-- only bills = proposals
and laws_kns_law_binding."LawTypeID" = 2
-- approved
and knesset_kns_status."StatusID" = 118

order by bills_kns_bill."StatusID", bills_kns_bill."KnessetNum" desc, bills_kns_bill."BillID" desc