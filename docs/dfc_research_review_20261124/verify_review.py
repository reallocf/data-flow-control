from pathlib import Path
import csv
import hashlib
import json
import xml.etree.ElementTree as ET
from collections import Counter

root=Path(__file__).resolve().parent

p=json.loads(
    (root/'evidence/provenance.json').read_text(
        encoding='utf-8'
    )
)

for name,digest in p['file_sha256'].items():

    actual=hashlib.sha256(
        (root/name).read_bytes().replace(b'\r\n',b'\n')
    ).hexdigest()

    if actual!=digest:
        raise RuntimeError('Digest differs: '+name)

for path in (root/'figures').glob('*.svg'):
    ET.parse(path)

def rows(name):
    with (root/'evidence'/name).open(
        encoding='utf-8',newline=''
    ) as f:
        return list(csv.DictReader(f))

f=rows('family_review.csv')
c=rows('candidate_review.csv')
s=rows('section195_comparison.csv')

assert len(f)==17
assert sum(x['single_transaction_eligible']=='True' for x in f)==14

assert len(c)==57
assert Counter(x['historical_label'] for x in c)=={
    'EXACT':2,'PARTIAL':29,'INCORRECT':26
}

assert len(s)==6
assert all(x['citation_positions']=='10' for x in s)

assert [
    sum(
        int(x['complete_positions'])
        for x in s if x['run'].startswith(k)
    )
    for k in ['BROAD','WORKLIST']
]==[15,11]

assert len(rows('semantic_assessment.csv'))==60
assert len(rows('downstream_comparison.csv'))==6
assert len(rows('factual_pairs.csv'))==5

print('Review digests and data consistency: PASS')
