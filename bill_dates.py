from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging, datetime
from functools import lru_cache, partial
import re


@lru_cache(maxsize=None)
def warn_once(str):
    logging.warning(str)


def to_date(dt):
    if isinstance(dt, datetime.datetime):
        return dt.date()
    elif isinstance(dt, datetime.date):
        return dt
    else:
        raise Exception('invalid dt type: {}'.format(dt))


def init_knessetdates():
    return []

        
def load_knessetdate_row(knessetdates, knessetdate):
    if knessetdate['PlenumFinish'] is not None:
        kd = (to_date(knessetdate['PlenumStart']), to_date(knessetdate['PlenumFinish']))
        knessetdates.append(kd)


def calc_knessetdates_range(row, from_attr, to_attr, knessetdates):
    if not row[from_attr] or not row[to_attr]:
        return None, None
    fr = to_date(row[from_attr])
    to = to_date(row[to_attr])
    days_with_pagra = (to - fr).days
    if days_with_pagra < 0:
        negate = True
        to, fr = fr, to
    else:
        negate = False
    event_days = 0
    found_from = False
    for knessetdate_from, knessetdate_to in sorted(knessetdates, key=lambda r: r[0]):
        if found_from:
            if to <= knessetdate_to:
                if to >= knessetdate_from:
                    # event finished during this knessetdate
                    event_days += (to - knessetdate_from).days
                break
            else:
                # event finished after this knessetdate
                event_days += (knessetdate_to - knessetdate_from).days
        elif fr <= knessetdate_to:
            # event started before or during this knessetdate
            found_from = True
            if to > knessetdate_from:
                # event finished during or after this knessetdate
                if to <= knessetdate_to:
                    # event finished during this knessetdate
                    if fr >= knessetdate_from:
                        # event started during this knessetdate
                        event_days += (to - fr).days
                    else:
                        # event started before this knessetdate (during pagra)
                        event_days += (to - knessetdate_from).days
                else:
                    # event finished after this knessetdata
                    if fr >= knessetdate_from:
                        # event started during this knessetdate
                        event_days += (knessetdate_to - fr).days
                    else:
                        # event started before this knessetdate (during pagra)
                        event_days += (knessetdate_to - knessetdate_from).days
    if not found_from:
        logging.warning('did not find from for bill id {}: {}'.format(row['bill_id'], fr))
        event_days = 0
    elif negate:
        event_days = -1 * event_days
    days_without_pagra = event_days
    return days_with_pagra, days_without_pagra


MIN_SESSION_DATE = datetime.datetime(1945, 1, 1)


def get_session_date(row):
    if row['StartDate'] and row['StartDate'] > MIN_SESSION_DATE:
        return row['StartDate'].date()
    elif row['FinishDate'] and row['FinishDate'] > MIN_SESSION_DATE:
        return row['FinishDate'].date()
    else:
        m = re.findall('([0-9]+)/([0-9]+)/([0-9]+)', row['Name'])
        assert m, 'failed to find date for session {}'.format(row)
        assert len(m) == 1
        return datetime.date(*map(int, reversed(m[0])))


class BillDatesLoader(object):

    splits_fields = [{'name': 'bill_id', 'type': 'integer'},
                     {'name': 'split_to_bill_id', 'type': 'integer'},
                     {'name': 'split_date', 'type': 'date'},]

    unions_fields = [{'name': 'bill_id', 'type': 'integer'},
                     {'name': 'merged_with_bill_id', 'type': 'integer'},
                     {'name': 'merge_dates', 'type': 'array'},]

    dates_fields = [{'name': 'bill_id', 'type': 'integer'},
                    {'name': 'knesset_num', 'type': 'integer'},
                    {'name': 'sub_type', 'type': 'string'},
                    {'name': 'status', 'type': 'string'},
                    # {'name': 'first_plenum_discussion', 'type': 'date'},
                    {'name': 'ancestor_first_plenum_discussion', 'type': 'date'},
                    # {'name': 'first_preliminary_call', 'type': 'date'},
                    # {'name': 'last_preliminary_call', 'type': 'date'},
                    # {'name': 'ancestor_first_preliminary_call', 'type': 'date'},
                    # {'name': 'ancestor_last_preliminary_call', 'type': 'date'},
                    # {'name': 'first_call', 'type': 'date'},
                    # {'name': 'ancestor_first_call', 'type': 'date'},
                    # {'name': 'pass_to_committee_vote_dates', 'type': 'array'},
                    # {'name': 'ancestor_pass_to_committee_vote_dates', 'type': 'array'},
                    {'name': 'presented_to_plenum', 'type': 'date'},
                    {'name': 'merged_first_call', 'type': 'date'},
                    {'name': 'from_presented_to_first_call', 'type': 'integer'},
                    {'name': 'from_presented_to_first_call_without_pagra', 'type': 'integer'},
                    {'name': 'merged_last_call', 'type': 'date'},
                    # {'name': 'discussions_in_relevant_committee', 'type': 'array'},
                    # {'name': 'ancestor_discussions_in_relevant_committee', 'type': 'array'},
                    {'name': 'from_last_call_to_first_relevant_committee_discussion', 'type': 'integer'},
                    {'name': 'from_last_call_to_first_relevant_committee_discussion_without_pagra', 'type': 'integer'},
                    {'name': 'from_first_relevant_committee_discussions_to_last', 'type': 'integer'},
                    {'name': 'from_first_relevant_committee_discussions_to_last_without_pagra', 'type': 'integer'},
                    {'name': 'from_presented_to_plenum_to_publication', 'type': 'integer'},
                    {'name': 'from_presented_to_plenum_to_publication_without_pagra', 'type': 'integer'},
                    {'name': 'from_first_plenum_discussion_to_publication', 'type': 'integer'},
                    {'name': 'from_first_plenum_discussion_to_publication_without_pagra', 'type': 'integer'},
                    {'name': 'publication_date', 'type': 'date'},
                    {'name': 'dates_comments', 'type': 'string'},]

    DATE_RANGES = {'from_presented_to_first_call': ['presented_to_plenum', 'merged_first_call'],
                   'from_last_call_to_first_relevant_committee_discussion': ['merged_last_call', 'first_relevant_committee_discussion'],
                   'from_first_relevant_committee_discussions_to_last': ['first_relevant_committee_discussion', 'last_relevant_committee_discussion'],
                   'from_presented_to_plenum_to_publication': ['presented_to_plenum', 'publication_date'],
                   'from_first_plenum_discussion_to_publication': ['ancestor_first_plenum_discussion', 'publication_date']}

    def __init__(self):
        self.committee_session_dates = {}
        self.bill_session_first_event = {}
        self.split_status_ids = set()
        self.merge_status_ids = set()
        self.first_call_status_id = None
        self.preliminary_call_status_ids = set()
        self.session_dates = {}
        self.bill_split_dates = {}
        self.bill_merge_dates = {}
        self.bill_splits = {}
        self.bill_unions = {}
        self.bill_first_calls = {}
        self.all_bill_ids = set()
        self.bill_dates = {}
        self.bill_parents = {}
        self.bill_first_preliminary_calls = {}
        self.bill_last_preliminary_calls = {}
        self.bill_pass_to_committee_votes = {}
        self.all_statuses = {}
        self.bills = {}
        self.bill_pass_to_committee_session_ids = {}
        self.bill_discussions_in_committees = {}
        self.bill_discussions_in_relevant_committee = {}
        self.committee_session_committees = {}
        self.knessetdates = init_knessetdates()
        self.row_loaders = {'kns_committeesession': self.load_committee_session_row,
                            'kns_cmtsessionitem': self.load_committee_session_item_row,
                            'kns_status': self.load_status_row,
                            'kns_plenumsession': self.load_plenum_session_row,
                            'kns_plmsessionitem': self.load_plenum_session_item_row,
                            'kns_billsplit': self.load_bill_split_row,
                            'kns_billunion': self.load_bill_union_row,
                            'view_vote_rslts_hdr_approved': self.load_vote_row,
                            'kns_bill': self.load_bill_row,
                            'kns_knessetdates': self._load_knessetdate_row}

    def load_status_row(self, row):
        self.all_statuses[row['StatusID']] = row['Desc']
        if row['Desc']:
            if row['Desc'].strip() == 'לאישור פיצול במליאה':
                self.split_status_ids.add(row['StatusID'])
            elif row['Desc'].strip() == 'לאישור מיזוג בוועדת הכנסת':
                self.merge_status_ids.add(row['StatusID'])
            elif row['Desc'].strip() == 'הונחה על שולחן הכנסת לקריאה ראשונה':
                assert self.first_call_status_id is None
                self.first_call_status_id = row['StatusID']
            elif row['Desc'].strip() in ('הונחה על שולחן הכנסת לדיון מוקדם',
                                         'לדיון במליאה לקראת הקריאה הראשונה',
                                         'במליאה לדיון מוקדם'):
                self.preliminary_call_status_ids.add(row['StatusID'])

    def load_committee_session_row(self, row):
        session_date = get_session_date(row)
        if session_date:
            self.committee_session_dates[row['CommitteeSessionID']] = session_date
        self.committee_session_committees[row['CommitteeSessionID']] = row['CommitteeID']

    def load_committee_session_item_row(self, row):
        self.all_bill_ids.add(row['ItemID'])
        last_first_event = self.bill_session_first_event.setdefault(row['ItemID'], None)
        session_date = self.committee_session_dates.get(row['CommitteeSessionID'])
        if session_date:
            if not last_first_event or last_first_event > session_date:
                self.bill_session_first_event[row['ItemID']] = session_date
            if row['StatusID'] in self.merge_status_ids:
                merge_dates = self.bill_merge_dates.setdefault(row['ItemID'], [])
                merge_dates.append(session_date)
            committee_id = self.committee_session_committees.get(row['CommitteeSessionID'])
            if committee_id:
                bill_discussions = self.bill_discussions_in_committees.setdefault(row['ItemID'], {})
                bill_discussions.setdefault(committee_id, set()).add(session_date)

    def load_plenum_session_row(self, row):
        session_date = get_session_date(row)
        if session_date:
            self.session_dates[row['PlenumSessionID']] = session_date

    def load_plenum_session_item_row(self, row):
        self.all_bill_ids.add(row['ItemID'])
        last_first_event = self.bill_session_first_event.setdefault(row['ItemID'], None)
        session_date = self.session_dates.get(row['PlenumSessionID'])
        if session_date:
            if not last_first_event or last_first_event > session_date:
                self.bill_session_first_event[row['ItemID']] = session_date
            if row['StatusID'] in self.split_status_ids:
                split_dates = self.bill_split_dates.setdefault(row['ItemID'], [])
                split_dates.append(session_date)
            elif row['StatusID'] == self.first_call_status_id:
                first_call = self.bill_first_calls.get(row['ItemID'])
                if not first_call or first_call > session_date:
                    self.bill_first_calls[row['ItemID']] = session_date
                self.bill_pass_to_committee_session_ids.setdefault(row['ItemID'], []).append(row['PlenumSessionID'])
            elif row['StatusID'] in self.preliminary_call_status_ids:
                first_preliminary_call = self.bill_first_preliminary_calls.get(row['ItemID'])
                if not first_preliminary_call or first_preliminary_call > session_date:
                    self.bill_first_preliminary_calls[row['ItemID']] = session_date
                last_preliminary_call = self.bill_last_preliminary_calls.get(row['ItemID'])
                if not last_preliminary_call or last_preliminary_call < session_date:
                    self.bill_last_preliminary_calls[row['ItemID']] = session_date
                self.bill_pass_to_committee_session_ids.setdefault(row['ItemID'], []).append(row['PlenumSessionID'])

    def load_bill_split_row(self, row):
        self.all_bill_ids.add(row['MainBillID'])
        self.all_bill_ids.add(row['SplitBillID'])
        splits = self.bill_splits.setdefault(row['MainBillID'], [])
        split_dates = self.bill_split_dates.get(row['MainBillID'], [])
        split_to_bill_id = row['SplitBillID']
        split_to_first_event = self.bill_session_first_event.get(split_to_bill_id)
        if split_to_first_event:
            split_dates_after_first_event = [d for d in split_dates
                                             if d <= split_to_first_event]
        else:
            split_dates_after_first_event = []
        splits.append({'split_to_bill_id': row['SplitBillID'],
                       'split_to_first_event': split_to_first_event,
                       'split_dates': list(set(split_dates)),
                       'split_dates_after_first_event': list(set(split_dates_after_first_event))})

    def load_bill_union_row(self, row):
        self.all_bill_ids.add(row['MainBillID'])
        self.all_bill_ids.add(row['UnionBillID'])
        unions = self.bill_unions.setdefault(row['MainBillID'], [])
        merge_dates = self.bill_merge_dates.get(row['MainBillID'], [])
        merged_with_bill_id = row['UnionBillID']
        unions.append({'merged_with_bill_id': merged_with_bill_id,
                       'merge_dates': merge_dates})

    def load_vote_row(self, row):
        vote_date = row['vote_date']
        bill_id = row['sess_item_id']
        if 'להעביר את הצעת החוק לוועדה' in row['vote_item_dscr'].strip():
            vote_dates = self.bill_pass_to_committee_votes.setdefault(bill_id, set())
            vote_dates.add(vote_date)
        elif row['session_id'] in self.bill_pass_to_committee_session_ids.get(bill_id, []):
            vote_dates = self.bill_pass_to_committee_votes.setdefault(bill_id, set())
            vote_dates.add(vote_date)

    def load_bill_row(self, row):
        self.all_bill_ids.add(row['BillID'])
        self.bills[row['BillID']] = {'knesset_num': row['KnessetNum'],
                                     'status': self.all_statuses[row['StatusID']],
                                     'sub_type': row['SubTypeDesc'],
                                     'publication_date': row['PublicationDate'].date() if row['PublicationDate'] else None}
        committee_discussions = self.bill_discussions_in_committees.get(row['BillID'], {})
        relevant_committee_discussions = committee_discussions.get(row['CommitteeID'], set())
        self.bill_discussions_in_relevant_committee[row['BillID']] = relevant_committee_discussions

    def _load_knessetdate_row(self, knessetdate):
        load_knessetdate_row(self.knessetdates, knessetdate)

    def get_splits_resource(self):
        for bill_id, splits in self.bill_splits.items():
            for split in splits:
                if len(splits) == 2 and len(split['split_dates_after_first_event']) == 2:
                    other_splits = [s for s in splits if s['split_to_bill_id'] != split['split_to_bill_id']]
                    assert len(other_splits) == 1
                    other_split = other_splits[0]
                    assert len(other_split['split_dates_after_first_event']) == 1
                    other_split_date = other_split['split_dates_after_first_event'][0]
                    split['split_dates_after_first_event'] = [d for d in split['split_dates_after_first_event']
                                                              if d != other_split_date]
                split_dates = list(set(split['split_dates']))
                if len(split_dates) > 1:
                    split_dates = split['split_dates_after_first_event']
                if split_dates:
                    assert len(split_dates) < 2
                    split_date = split_dates[0]
                else:
                    split_date = None
                self.bill_parents.setdefault(split['split_to_bill_id'], set()).add(bill_id)
                yield {'bill_id': bill_id,
                       'split_to_bill_id': split['split_to_bill_id'],
                       'split_date': split_date}

    def get_unions_resource(self):
        for bill_id, merges in self.bill_unions.items():
            for merge in merges:
                merge_dates = list(set(merge['merge_dates']))
                self.bill_parents.setdefault(merge['merged_with_bill_id'], set()).add(bill_id)
                self.bill_parents.setdefault(bill_id, set()).add(merge['merged_with_bill_id'])
                yield {'bill_id': bill_id,
                       'merged_with_bill_id': merge['merged_with_bill_id'],
                       'merge_dates': list(map(str, merge_dates))}

    def find_first_attr(self, bill_id, attr, previous_parents=None, last=False):
        if not previous_parents:
            previous_parents = set()
        previous_parents.add(bill_id)
        first_attr = self.bill_dates.get(bill_id, {}).get(attr)
        for parent_bill_id in self.bill_parents.get(bill_id, []):
            if parent_bill_id not in previous_parents:
                previous_parents.add(parent_bill_id)
                parent_attr = self.find_first_attr(parent_bill_id, attr, previous_parents, last=last)
                if last:
                    if first_attr is None or (parent_attr and parent_attr > first_attr):
                        first_attr = parent_attr
                else:
                    if first_attr is None or (parent_attr and parent_attr < first_attr):
                        first_attr = parent_attr
        return first_attr

    def collect_pass_to_committee_votes(self, bill_id, previous_parents=None):
        if not previous_parents:
            previous_parents = set()
        previous_parents.add(bill_id)
        votes = self.bill_pass_to_committee_votes.get(bill_id, set())
        for parent_bill_id in self.bill_parents.get(bill_id, []):
            if parent_bill_id not in previous_parents:
                previous_parents.add(parent_bill_id)
                parent_votes = self.collect_pass_to_committee_votes(parent_bill_id, previous_parents)
                votes = votes.union(parent_votes)
        return votes

    def collect_discussions_in_relevant_committee(self, bill_id, previous_parents=None):
        if not previous_parents:
            previous_parents = set()
        previous_parents.add(bill_id)
        discussions = self.bill_discussions_in_relevant_committee.get(bill_id, set())
        for parent_bill_id in self.bill_parents.get(bill_id, []):
            if parent_bill_id not in previous_parents:
                previous_parents.add(parent_bill_id)
                parent_discussions = self.collect_discussions_in_relevant_committee(parent_bill_id, previous_parents)
                discussions = discussions.union(parent_discussions)
        return discussions

    def get_dates_resource(self):
        for bill_id, presented_to_plenum in self.bill_first_calls.items():
            self.bill_dates.setdefault(bill_id, {})['first_call'] = presented_to_plenum
        for bill_id, first_preliminary_call in self.bill_first_preliminary_calls.items():
            self.bill_dates.setdefault(bill_id, {})['first_preliminary_call'] = first_preliminary_call
        for bill_id, last_preliminary_call in self.bill_last_preliminary_calls.items():
            self.bill_dates.setdefault(bill_id, {})['last_preliminary_call'] = last_preliminary_call
        for bill_id, first_plenum_discussion in self.bill_session_first_event.items():
            self.bill_dates.setdefault(bill_id, {})['first_plenum_discussion'] = first_plenum_discussion
        for bill_id in self.all_bill_ids:
            bill_dates = self.bill_dates.get(bill_id, {})
            find_first_attr = partial(self.find_first_attr, bill_id)
            find_last_attr = partial(self.find_first_attr, bill_id, last=True)
            pass_to_committee_vote_dates = list(set(self.bill_pass_to_committee_votes.get(bill_id, [])))
            ancestor_pass_to_committee_votes_dates = list(set(self.collect_pass_to_committee_votes(bill_id)))
            row = {'bill_id': bill_id,
                   'first_call': bill_dates.get('first_call'),
                   'first_preliminary_call': bill_dates.get('first_preliminary_call'),
                   'last_preliminary_call': bill_dates.get('last_preliminary_call'),
                   'first_plenum_discussion': bill_dates.get('first_plenum_discussion'),
                   'ancestor_first_call': find_first_attr('first_call'),
                   'ancestor_first_preliminary_call': find_first_attr('first_preliminary_call'),
                   'ancestor_last_preliminary_call': find_last_attr('last_preliminary_call'),
                   'ancestor_first_plenum_discussion': find_first_attr('first_plenum_discussion'),}
            dates_comments = []
            presented_to_plenum = row['first_call']
            presented_to_plenum_comment = ''
            first_call, first_call_comment = None, ''
            last_call, last_call_comment = None, ''
            if row['ancestor_first_call'] and (not presented_to_plenum or row['ancestor_first_call'] < presented_to_plenum):
                presented_to_plenum, presented_to_plenum_comment = row['ancestor_first_call'], 'used ancestor first call'
            for vote_date in pass_to_committee_vote_dates:
                if not presented_to_plenum or vote_date < presented_to_plenum:
                    presented_to_plenum, presented_to_plenum_comment = vote_date, 'used pass to committee vote'
                if not last_call or last_call < vote_date:
                    last_call = vote_date
                if not first_call or first_call > vote_date:
                    first_call = vote_date
            for vote_date in ancestor_pass_to_committee_votes_dates:
                if not presented_to_plenum or vote_date < presented_to_plenum:
                    presented_to_plenum, presented_to_plenum_comment = vote_date, 'used ancestor pass to committee vote'
                if not last_call or last_call < vote_date:
                    last_call, last_call_comment = vote_date, 'used ancestor pass to committee vote'
                if not first_call or first_call > vote_date:
                    first_call, first_call_comment = vote_date, 'used ancestor pass to committee vote'
            if not presented_to_plenum:
                if row['last_preliminary_call']:
                    presented_to_plenum, presented_to_plenum_comment = row['last_preliminary_call'], 'used last preliminary call'
                if row['ancestor_last_preliminary_call'] and (not presented_to_plenum or row['ancestor_last_preliminary_call'] < presented_to_plenum):
                    presented_to_plenum, presented_to_plenum_comment = row['ancestor_last_preliminary_call'], 'used ancestor last preliminary call'
            if presented_to_plenum:
                row['presented_to_plenum'] = presented_to_plenum
                if presented_to_plenum_comment:
                    dates_comments.append('{} as presented to plenum'.format(presented_to_plenum_comment))
            else:
                row['presented_to_plenum'] = None
                dates_comments.append('failed to find presented to plenum')
            if not first_call:
                if presented_to_plenum:
                    first_call = presented_to_plenum
                    first_call_comment = 'used presented to plenum'
            if first_call:
                row['merged_first_call'] = first_call
                if first_call_comment:
                    dates_comments.append('{} as first call'.format(first_call_comment))
            else:
                row['merged_first_call'] = None
                dates_comments.append('failed to find first call')
            if not last_call:
                if first_call:
                    last_call = first_call
                    last_call_comment = 'used first call'
            if last_call:
                row['merged_last_call'] = last_call
                if last_call_comment:
                    dates_comments.append('{} as last call'.format(last_call_comment))
            else:
                row['merged_last_call'] = None
                dates_comments.append('failed to find last call')
            discussions_in_relevant_committee = self.bill_discussions_in_relevant_committee.get(bill_id, set())
            row['discussions_in_relevant_committee'] = list(map(str, list(discussions_in_relevant_committee)))
            ancestor_discussions_in_relevant_committee = self.collect_discussions_in_relevant_committee(bill_id)
            row['ancestor_discussions_in_relevant_committee'] = list(map(str, ancestor_discussions_in_relevant_committee))
            row['dates_comments'] = ', '.join(dates_comments)
            row['pass_to_committee_vote_dates'] = list(map(str, pass_to_committee_vote_dates))
            row['ancestor_pass_to_committee_vote_dates'] = list(map(str, ancestor_pass_to_committee_votes_dates))
            first_relevant_committee_discussion, last_relevant_committee_discussion = None, None
            for discussion in discussions_in_relevant_committee:
                if not first_relevant_committee_discussion or first_relevant_committee_discussion > discussion:
                    first_relevant_committee_discussion = discussion
                if not last_relevant_committee_discussion or last_relevant_committee_discussion < discussion:
                    last_relevant_committee_discussion = discussion
            row['first_relevant_committee_discussion'] = first_relevant_committee_discussion
            row['last_relevant_committee_discussion'] = last_relevant_committee_discussion
            row.update(**self.bills[bill_id])
            for field, from_to in self.DATE_RANGES.items():
                from_attr, to_attr = from_to
                row[field], row[field+'_without_pagra'] = calc_knessetdates_range(row, from_attr, to_attr,
                                                                                  self.knessetdates)
            row = {k: v for k, v in row.items()
                   if k in [f['name'] for f in self.dates_fields]}
            yield row


class BillDatesProcessor(object):

    def __init__(self):
        self.resource_idx = {}
        self.loader = BillDatesLoader()

    def get_resources(self, resources):
        for i, resource in enumerate(resources):
            name = self.resource_idx.get(i)
            if name:
                row_loader = self.loader.row_loaders[name]
                for row in resource:
                    row_loader(row)
            else:
                yield resource
        yield self.loader.get_splits_resource()
        yield self.loader.get_unions_resource()
        yield self.loader.get_dates_resource()

    def get_datapackage(self, datapackage):
        new_descriptors = []
        for i, descriptor in enumerate(datapackage['resources']):
            if descriptor['name'] in self.loader.row_loaders.keys():
                self.resource_idx[i] = descriptor['name']
            else:
                new_descriptors.append(descriptor)
        new_descriptors.append({PROP_STREAMING: True,
                              'name': 'bill_splits', 'path': 'bill_splits.csv',
                              'schema': {'fields': BillDatesLoader.splits_fields}})
        new_descriptors.append({PROP_STREAMING: True,
                              'name': 'bill_unions', 'path': 'bill_unions.csv',
                              'schema': {'fields': BillDatesLoader.unions_fields}})
        new_descriptors.append({PROP_STREAMING: True,
                              'name': 'bill_dates', 'path': 'bill_dates.csv',
                              'schema': {'fields': BillDatesLoader.dates_fields}})
        return dict(datapackage, resources=new_descriptors)

    def main(self):
        parameters, datapackage, resources = ingest()
        spew(self.get_datapackage(datapackage), self.get_resources(resources))


if __name__ == '__main__':
    BillDatesProcessor().main()
