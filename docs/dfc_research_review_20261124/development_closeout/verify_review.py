import csv
import hashlib
import json
from pathlib import Path

P=Path(__file__).resolve().parent

def load(name):
    return json.loads(
        (P/name).read_text(encoding='utf-8-sig')
    )

def rows(name):
    with (P/name).open(
        encoding='utf-8-sig',newline=''
    ) as f:
        return list(csv.DictReader(f))

def check(ok,message):
    if not ok:
        raise RuntimeError(message)

manifest=load('evidence_manifest.json')

for name,expected in manifest.items():
    actual=hashlib.sha256(
        (P/name).read_bytes()
    ).hexdigest()
    check(actual==expected,'Digest differs: '+name)

r=rows('environment_representation.csv')

check(
    {x['condition']:int(x['semantic_score'])
     for x in r}=={'A':41,'B':54,'I':38},
    'Environment comparison differs'
)

c=load('confidence_summary.json')

check(
    c['assessed']==57
    and c['high']==36
    and c['high_on_exact']==2
    and c['high_on_nonexact']==34,
    'Confidence comparison differs'
)

r=rows('predicate_enforcement.csv')

check(
    len(r)==29
    and all(x['status']=='PASS' for x in r),
    'Predicate enforcement differs'
)

r=rows('conditional_legal_comparison.csv')

check(
    len(r)==23
    and sum(x['matches_reference']=='True'
            for x in r)==13
    and sum(x['false_admission']=='True'
            for x in r)==5
    and sum(x['false_rejection']=='True'
            for x in r)==5,
    'Conditional comparison differs'
)

r=rows('cross_model_comparison.csv')

check(
    {(x['benchmark'],x['group']):
        int(x['semantic_score'])
     for x in r}=={
        ('274','BASELINE'):55,
        ('274','ADDITIONAL'):38,
        ('195','BASELINE'):46,
        ('195','ADDITIONAL'):49
     },
    'Cross-model comparison differs'
)

r=rows('reference_condition_comparison.csv')

check(
    {x['condition']:(
        int(x['available_responses']),
        int(x['semantic_score']),
        int(x['maximum_score'])
     ) for x in r}=={
        'REF0':(3,36,102),
        'R':(1,11,34),
        'L':(2,25,68)
     },
    'Reference comparison differs'
)

status=load('reference_collection_status.json')

check(
    status['connection_closed_attempts']==2
    and status['confirmed_reference_tokens']==84345
    and not status['balanced_experiment_complete'],
    'Collection status differs'
)

mcp=load('mcp_transport_result.json')

check(
    mcp['status']=='PASS'
    and len(mcp['checks'])==4
    and all(mcp['checks'].values())
    and mcp['synthetic_lifespan'],
    'MCP evidence differs'
)

print('VERIFIED_FILES:',len(manifest))
print('REPORTED_TABLE_CHECKS: PASS')
print('INDEPENDENT_LEGAL_REVIEW: NOT_ESTABLISHED')
print('RUNTIME_REPRODUCTION: NOT_PERFORMED')
