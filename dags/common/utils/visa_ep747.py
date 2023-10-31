import csv
import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union, List, Optional, Tuple, Callable

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

REPORT_HEADER = 'REPORT_HEADER'
REPORT_FOOTER = 'REPORT_FOOTER'
RECORD_HEADER = 'RECORD_HEADER'
RECORD_SECTION = 'RECORD_SECTION'
EMPTY_RECORD = 'EMPTY_RECORD'
RECORD = 'RECORD'


@dataclass
class Line:
    line_number: int
    text: str

    @property
    def is_empty(self) -> bool:
        return not self.text.strip()

    def match(self, patterns: Union[str, List[str]]) -> bool:
        if isinstance(patterns, str):
            patterns = [patterns]
        return any(re.match(pattern, self.text) for pattern in patterns)

    @property
    def indent_size(self) -> int:
        return len(self.text) - len(self.text.lstrip())


def parse_str(s: str) -> str:
    return re.sub(r'\s+', ' ', s).strip()


def parse_int(s: str) -> Optional[int]:
    s = s.strip()
    if s:
        return int(s.replace(',', ''))


def parse_float(s: str) -> Optional[float]:
    s = s.strip()
    if s:
        return float(s.replace(',', ''))


def parse_date(s: str) -> Optional[date]:
    s = s.strip()
    if s:
        return datetime.strptime(s, '%d%b%y').date()


def parse_headers(s: str) -> dict:
    data = {}
    x = re.sub(r':\s+', ':', s.strip())  # "A:  B" -> "A:B
    x = re.split(r'\s{2,}', x)  # "A:B  CCC  D:E" -> ["A:B", "CCC", "D:E"]
    x = [i for i in x if re.match(r'.*:.*', i)]  # ["A:B", "CCC", "D:E"] -> ["A:B, "D:E"]
    for i in x:
        k, v = i.split(':')
        k = k.lower().replace(' ', '_')
        data[k] = v
    return data


def get_header_data(tag_lines: List[Tuple[str, Line]]) -> dict:
    data = {k: v for tag, line in tag_lines if tag == REPORT_HEADER for k, v in parse_headers(line.text).items()}
    del data['page']
    return data


def get_hierarchies(tag_lines: List[Tuple[str, Line]]) -> List[Optional[List[str]]]:
    results = []
    reversed_tag_lines = sorted(tag_lines, key=lambda i: i[1].line_number, reverse=True)
    for m, (tag, line) in enumerate(reversed_tag_lines):
        if tag == RECORD and m + 1 < len(reversed_tag_lines):
            line_hierarchies = []
            for n, (lookup_tag, lookup_line) in enumerate(reversed_tag_lines[m + 1:]):
                last_indent_size = line_hierarchies[-1].indent_size if line_hierarchies else line.indent_size
                if lookup_tag == RECORD_SECTION and lookup_line.indent_size < last_indent_size:
                    line_hierarchies.append(lookup_line)
            line_hierarchies = [parse_str(i.text) for i in line_hierarchies]
            line_hierarchies.reverse()
            results.append(line_hierarchies)
        else:
            results.append(None)
    results.reverse()

    for n, result in enumerate(results):
        if result is None:
            results[n] = [''] * 10
        else:
            results[n] = [result[i] if i + 1 <= len(result) else '' for i in range(10)]

    return results


def test_get_hierarchies():
    # Will lookup previous section.
    tag_lines = [
        (RECORD_SECTION, Line(1, 'A')),
        (RECORD, Line(2, ' B'))
    ]
    assert get_hierarchies(tag_lines=tag_lines)[1] == ['A'] + [''] * 9

    # Will skip sections that are not with smaller indent size.
    tag_lines = [
        (RECORD_SECTION, Line(1, 'A')),
        (RECORD, Line(2, 'B'))
    ]
    assert get_hierarchies(tag_lines=tag_lines)[1] == [''] * 10

    # Will lookup all previous sections if there are more than one.
    tag_lines = [
        (RECORD_SECTION, Line(1, 'A')),
        (RECORD_SECTION, Line(2, ' B')),
        (RECORD, Line(3, '  C'))
    ]
    assert get_hierarchies(tag_lines=tag_lines)[2] == ['A', 'B'] + [''] * 8

    # Will skip un-related sections based on indentation.
    tag_lines = [
        (RECORD_SECTION, Line(1, 'A')),
        (RECORD_SECTION, Line(2, ' B')),
        (RECORD, Line(3, '  C')),
        (RECORD_SECTION, Line(4, ' D')),
        (RECORD, Line(5, '  E'))
    ]
    assert get_hierarchies(tag_lines=tag_lines)[4] == ['A', 'D'] + [''] * 8


def get_vss_report_lines(lines: List[str], report_id: str) -> List[List[Line]]:
    lines = deepcopy(lines)
    lines = [Line(line_number=n, text=i) for n, i in enumerate(lines, start=1)]

    # Group lines by report.
    groups = dict()  # type: Dict[str,List[List[Line]]]
    report_lines = []  # type: List[Line]
    is_looking_for_new_report = False
    for line in lines:
        # Skip empty line.
        if line.is_empty:
            continue
        # Ignore header.
        if 'CAUTION:' in line.text:
            continue
        # Start of report.
        if is_looking_for_new_report and 'REPORT ID:' in line.text:
            report_lines = [line]
            is_looking_for_new_report = False
            continue
        # End of report.
        if not is_looking_for_new_report and re.match('.*END OF .* REPORT.*', line.text):
            report_lines.append(line)
            curr_report_id = re.split(r'\s{2,}', report_lines[0].text.strip())[1]
            groups[curr_report_id] = groups.get(curr_report_id, []) + [report_lines]
            is_looking_for_new_report = True
            continue
        # Report line.
        report_lines.append(line)

    return groups.get(report_id, [])


def vss_110_parse_func(lines: List[str]) -> List[dict]:
    results = []
    for lines in get_vss_report_lines(lines=lines, report_id='VSS-110'):
        tag_lines = []
        for line in lines:
            # REPORT_HEADER
            if line.match(r'\s?.*:.*'):
                tag_lines.append((REPORT_HEADER, line))
            # REPORT_FOOTER
            elif line.match('.*END OF VSS-110 REPORT.*'):
                tag_lines.append((REPORT_FOOTER, line))
            # RECORD_HEADER
            elif line.match([
                r'.*CREDIT.*DEBIT.*TOTAL',
                r'.*COUNT.*AMOUNT.*AMOUNT.*AMOUNT'
            ]):
                tag_lines.append((RECORD_HEADER, line))
            # RECORD_SECTION
            elif line.match(r'^\s*.{0,50}\s*$'):
                tag_lines.append((RECORD_SECTION, line))
            # EMPTY_RECORD
            elif line.match(r'.*NO DATA FOR THIS REPORT.*'):
                tag_lines.append((EMPTY_RECORD, line))
            # RECORD
            else:
                tag_lines.append((RECORD, line))
        header_data = get_header_data(tag_lines=tag_lines)
        hierarchies = get_hierarchies(tag_lines=tag_lines)
        for n, (tag, line) in enumerate(tag_lines):
            if tag == RECORD:
                results.append({
                    'line_number': line.line_number,
                    'reporting_for': header_data['reporting_for'],
                    'proc_date': parse_date(header_data['proc_date']),
                    'rollup_to': header_data['rollup_to'],
                    'report_date': parse_date(header_data['report_date']),
                    'funds_xfer_entity': header_data['funds_xfer_entity'],
                    'settlement_currency': header_data['settlement_currency'],
                    'funds_transfer_amount': header_data.get('funds_transfer_amount', ''),
                    'hierarchy_1': hierarchies[n][0],
                    'hierarchy_2': hierarchies[n][1],
                    'hierarchy_3': hierarchies[n][2],
                    'hierarchy_4': hierarchies[n][3],
                    'hierarchy_5': hierarchies[n][4],
                    'hierarchy_6': hierarchies[n][5],
                    'hierarchy_7': hierarchies[n][6],
                    'hierarchy_8': hierarchies[n][7],
                    'hierarchy_9': hierarchies[n][8],
                    'hierarchy_10': hierarchies[n][9],
                    'name': parse_str(line.text[:40]),
                    'count': parse_int(line.text[40:51]),
                    'credit_amount': parse_float(line.text[51:77]),
                    'debit_amount': parse_float(line.text[77:103]),
                    'total_amount': parse_str(line.text[103:])
                })
    return results


def vss_120_parse_func(lines: List[str]) -> List[dict]:
    results = []
    for lines in get_vss_report_lines(lines=lines, report_id='VSS-120'):
        tag_lines = []
        for line in lines:
            # REPORT_HEADER
            if line.match(r'\s?.*:.*'):
                tag_lines.append((REPORT_HEADER, line))
            # REPORT_FOOTER
            elif line.match(r'.*END OF VSS-120 REPORT.*'):
                tag_lines.append((REPORT_FOOTER, line))
            # RECORD_HEADER
            elif line.match([
                r'.*RATE.*COUNT.*CLEARING.*INTERCHANGE.*INTERCHANGE',
                r'.*TABLE.*AMOUNT.*VALUE.*VALUE',
                r'.*ID.*CREDITS.*DEBITS'
            ]):
                tag_lines.append((RECORD_HEADER, line))
            # REPORT_SECTION
            elif line.match(r'^\s*.{0,50}\s*$'):
                tag_lines.append((RECORD_SECTION, line))
            # EMPTY_RECORD
            elif line.match(r'.*NO DATA FOR THIS REPORT.*'):
                tag_lines.append((EMPTY_RECORD, line))
            # RECORD
            else:
                tag_lines.append((RECORD, line))
        header_data = get_header_data(tag_lines=tag_lines)
        hierarchies = get_hierarchies(tag_lines=tag_lines)
        for n, (tag, line) in enumerate(tag_lines):
            if tag == RECORD:
                results.append({
                    'line_number': line.line_number,
                    'reporting_for': header_data.get('reporting_for'),
                    'proc_date': parse_date(header_data.get('proc_date')),
                    'rollup_to': header_data.get('rollup_to'),
                    'report_date': parse_date(header_data.get('report_date')),
                    'funds_xfer_entity': header_data.get('funds_xfer_entity'),
                    'settlement_currency': header_data.get('settlement_currency'),
                    'clearing_currency': header_data.get('clearing_currency'),
                    'hierarchy_1': hierarchies[n][0],
                    'hierarchy_2': hierarchies[n][1],
                    'hierarchy_3': hierarchies[n][2],
                    'hierarchy_4': hierarchies[n][3],
                    'hierarchy_5': hierarchies[n][4],
                    'hierarchy_6': hierarchies[n][5],
                    'hierarchy_7': hierarchies[n][6],
                    'hierarchy_8': hierarchies[n][7],
                    'hierarchy_9': hierarchies[n][8],
                    'hierarchy_10': hierarchies[n][9],
                    'name': parse_str(line.text[:50]),
                    'count': parse_int(line.text[50:66]),
                    'clearing_amount': parse_str(line.text[66:89]),
                    'interchange_value_credits': parse_float(line.text[89:110]),
                    'interchange_value_debits': parse_float(line.text[110:])
                })
    return results


def vss_130_parse_func(lines: List[str]) -> List[dict]:
    results = []
    for lines in get_vss_report_lines(lines=lines, report_id='VSS-130'):
        tag_lines = []
        for line in lines:
            # REPORT_HEADER
            if line.match(r'\s?.*:.*'):
                tag_lines.append((REPORT_HEADER, line))
            # REPORT_FOOTER
            elif line.match(r'.*END OF VSS-130 REPORT.*'):
                tag_lines.append((REPORT_FOOTER, line))
            # RECORD_HEADER
            elif line.match([
                r'.*REIMBURSEMENT.*REIMBURSEMENT',
                r'.*COUNT.*INTERCHANGE.*FEE.*FEE',
                r'.*AMOUNT.*CREDITS.*DEBITS'
            ]):
                tag_lines.append((RECORD_HEADER, line))
            # REPORT_SECTION
            elif line.match(r'^\s*.{0,50}\s*$'):
                tag_lines.append((RECORD_SECTION, line))
            # EMPTY_RECORD
            elif line.match(r'.*NO DATA FOR THIS REPORT.*'):
                tag_lines.append((EMPTY_RECORD, line))
            # RECORD
            else:
                tag_lines.append((RECORD, line))
        header_data = get_header_data(tag_lines=tag_lines)
        hierarchies = get_hierarchies(tag_lines=tag_lines)
        for n, (tag, line) in enumerate(tag_lines):
            if tag == RECORD:
                results.append({
                    'line_number': line.line_number,
                    'reporting_for': header_data.get('reporting_for'),
                    'proc_date': parse_date(header_data.get('proc_date')),
                    'rollup_to': header_data.get('rollup_to'),
                    'report_date': parse_date(header_data.get('report_date')),
                    'funds_xfer_entity': header_data.get('funds_xfer_entity'),
                    'settlement_currency': header_data.get('settlement_currency'),
                    'hierarchy_1': hierarchies[n][0],
                    'hierarchy_2': hierarchies[n][1],
                    'hierarchy_3': hierarchies[n][2],
                    'hierarchy_4': hierarchies[n][3],
                    'hierarchy_5': hierarchies[n][4],
                    'hierarchy_6': hierarchies[n][5],
                    'hierarchy_7': hierarchies[n][6],
                    'hierarchy_8': hierarchies[n][7],
                    'hierarchy_9': hierarchies[n][8],
                    'hierarchy_10': hierarchies[n][9],
                    'name': parse_str(line.text[:50]),
                    'count': parse_int(line.text[50:66]),
                    'interchange_amount': parse_str(line.text[66:89]),
                    'reimbursement_fee_credits': parse_float(line.text[89:110]),
                    'reimbursement_fee_debits': parse_float(line.text[110:])
                })
    return results


def vss_140_parse_func(lines: List[str]) -> List[dict]:
    results = []
    for lines in get_vss_report_lines(lines=lines, report_id='VSS-140'):
        tag_lines = []
        for line in lines:
            # REPORT_HEADER
            if line.match(r'\s?.*:.*'):
                tag_lines.append((REPORT_HEADER, line))
            # REPORT_FOOTER
            elif line.match(r'.*END OF VSS-140 REPORT.*'):
                tag_lines.append((REPORT_FOOTER, line))
            # RECORD_HEADER
            elif line.match([
                r'.*COUNT.*INTERCHANGE.*VISA CHARGES.*VISA CHARGES',
                r'.*AMOUNT.*CREDITS.*DEBITS'
            ]):
                tag_lines.append((RECORD_HEADER, line))
            # REPORT_SECTION
            elif line.match(r'^\s*.{0,50}\s*$'):
                tag_lines.append((RECORD_SECTION, line))
            # EMPTY_RECORD
            elif line.match(r'.*NO DATA FOR THIS REPORT.*'):
                tag_lines.append((EMPTY_RECORD, line))
            # RECORD
            else:
                tag_lines.append((RECORD, line))
        header_data = get_header_data(tag_lines=tag_lines)
        hierarchies = get_hierarchies(tag_lines=tag_lines)
        for n, (tag, line) in enumerate(tag_lines):
            if tag == RECORD:
                results.append({
                    'line_number': line.line_number,
                    'reporting_for': header_data.get('reporting_for'),
                    'proc_date': parse_date(header_data.get('proc_date')),
                    'rollup_to': header_data.get('rollup_to'),
                    'report_date': parse_date(header_data.get('report_date')),
                    'funds_xfer_entity': header_data.get('funds_xfer_entity'),
                    'settlement_currency': header_data.get('settlement_currency'),
                    'hierarchy_1': hierarchies[n][0],
                    'hierarchy_2': hierarchies[n][1],
                    'hierarchy_3': hierarchies[n][2],
                    'hierarchy_4': hierarchies[n][3],
                    'hierarchy_5': hierarchies[n][4],
                    'hierarchy_6': hierarchies[n][5],
                    'hierarchy_7': hierarchies[n][6],
                    'hierarchy_8': hierarchies[n][7],
                    'hierarchy_9': hierarchies[n][8],
                    'hierarchy_10': hierarchies[n][9],
                    'name': parse_str(line.text[:50]),
                    'count': parse_int(line.text[50:66]),
                    'interchange_amount': parse_str(line.text[66:89]),
                    'visa_charges_credits': parse_float(line.text[89:110]),
                    'visa_charges_debits': parse_float(line.text[110:])
                })
    return results


@dataclass
class Meta:
    report_id: str
    parse_func: Callable
    redshift_table: str


metas = [
    Meta(report_id='VSS-110', parse_func=vss_110_parse_func, redshift_table='visa__vss_110_records'),
    Meta(report_id='VSS-120', parse_func=vss_120_parse_func, redshift_table='visa__vss_120_records'),
    Meta(report_id='VSS-130', parse_func=vss_130_parse_func, redshift_table='visa__vss_130_records'),
    Meta(report_id='VSS-140', parse_func=vss_140_parse_func, redshift_table='visa__vss_140_records')
]


@task(task_id='transform')
def transform_func(
        source_bucket: str,
        source_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        s3_key_to_filename_func: Callable
):
    s3_hook = S3Hook()
    s3_keys = s3_hook.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    s3_keys = [key for key in s3_keys if not key.endswith('/')]  # Skip folders.
    logger.info(f'Found {len(s3_keys)} keys.')
    for s3_key in s3_keys:
        logger.info(f'Processing \'{s3_key}\'.')
        with NamedTemporaryFile(mode='wb') as f_source:
            s3_obj = s3_hook.get_key(bucket_name=source_bucket, key=s3_key)
            s3_obj.download_fileobj(Fileobj=f_source)
            f_source.flush()
            with open(f_source.name, mode='r') as f:
                lines = f.readlines()
            created_at = datetime.now()
            for meta in metas:
                records = [{
                    'created_at': created_at,
                    'filename': s3_key_to_filename_func(source_bucket, s3_key),
                    **r
                } for r in meta.parse_func(lines=lines)]
                if not records:
                    continue
                with NamedTemporaryFile(mode='wb') as f_dest:
                    with open(f_dest.name, mode='w', encoding='utf8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=records[0].keys())
                        writer.writerows(records)
                    f_dest.flush()
                    path = Path(s3_key)
                    dest_fn = path.name.replace(path.suffix, '.csv')
                    s3_hook.load_file(filename=f_dest.name, bucket_name=dest_bucket,
                                      key=f"{dest_prefix}/{meta.redshift_table}/{dest_fn}")
