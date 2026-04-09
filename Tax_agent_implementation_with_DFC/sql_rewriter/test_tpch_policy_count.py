"""Tests for TPC-H policy count rewriting."""

import pathlib

import pytest

from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

POLICY_COUNTS = [1, 10, 100, 1000]


def load_tpch_query(query_num: int) -> str:
    """Load a TPC-H query from the benchmarks directory."""
    benchmarks_dir = pathlib.Path(__file__).parent.parent / "benchmarks" / "tpch" / "queries"
    query_file = benchmarks_dir / f"q{query_num:02d}.sql"

    if not query_file.exists():
        raise FileNotFoundError(f"TPC-H query {query_num} not found at {query_file}")

    return query_file.read_text()


def build_policies(num_policies: int) -> list[DFCPolicy]:
    """Build distinct policies for TPC-H on lineitem."""
    templates = [
        "max(lineitem.l_quantity + {i}) >= {i_plus}",
        "min(lineitem.l_quantity + {i}) >= {i_plus}",
        "sum(lineitem.l_quantity + {i}) >= {i_plus}",
        "avg(lineitem.l_quantity + {i}) >= {i_plus}",
        "count(lineitem.l_quantity) >= 1 + 0 * {i}",
    ]
    policies = []
    for i in range(num_policies):
        template = templates[i % len(templates)]
        constraint = template.format(i=i, i_plus=i + 1)
        policies.append(
            DFCPolicy(
                sources=["lineitem"],
                constraint=constraint,
                on_fail=Resolution.REMOVE,
                description=f"q01_policy_{i + 1}",
            )
        )
    return policies


def _clear_policies(rewriter: SQLRewriter) -> None:
    existing = rewriter.get_dfc_policies()
    for old_policy in existing:
        rewriter.delete_policy(
            sources=old_policy.sources,
            constraint=old_policy.constraint,
            on_fail=old_policy.on_fail,
        )


@pytest.fixture
def tpch_rewriter():
    """Create a SQLRewriter instance with TPC-H data loaded."""
    rewriter = SQLRewriter()

    rewriter.execute("INSTALL tpch")
    rewriter.execute("LOAD tpch")
    rewriter.execute("CALL dbgen(sf=0.1)")

    yield rewriter

    rewriter.close()


EXPECTED_SQL_Q01 = {
    1: r"""SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS sum_disc_price,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE
  l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
  l_returnflag,
  l_linestatus
HAVING
  (
    MAX(lineitem.l_quantity + 0) >= 1
  )
ORDER BY
  l_returnflag,
  l_linestatus""",
    10: r"""SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS sum_disc_price,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE
  l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
  l_returnflag,
  l_linestatus
HAVING
  (
    AVG(lineitem.l_quantity + 8) >= 9
  )
  AND (
    MIN(lineitem.l_quantity + 6) >= 7
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 4
  )
  AND (
    SUM(lineitem.l_quantity + 2) >= 3
  )
  AND (
    MAX(lineitem.l_quantity + 0) >= 1
  )
  AND (
    MIN(lineitem.l_quantity + 1) >= 2
  )
  AND (
    AVG(lineitem.l_quantity + 3) >= 4
  )
  AND (
    MAX(lineitem.l_quantity + 5) >= 6
  )
  AND (
    SUM(lineitem.l_quantity + 7) >= 8
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 9
  )
ORDER BY
  l_returnflag,
  l_linestatus""",
    100: r"""SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS sum_disc_price,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE
  l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
  l_returnflag,
  l_linestatus
HAVING
  (
    AVG(lineitem.l_quantity + 98) >= 99
  )
  AND (
    MIN(lineitem.l_quantity + 96) >= 97
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 94
  )
  AND (
    SUM(lineitem.l_quantity + 92) >= 93
  )
  AND (
    MAX(lineitem.l_quantity + 90) >= 91
  )
  AND (
    AVG(lineitem.l_quantity + 88) >= 89
  )
  AND (
    MIN(lineitem.l_quantity + 86) >= 87
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 84
  )
  AND (
    SUM(lineitem.l_quantity + 82) >= 83
  )
  AND (
    MAX(lineitem.l_quantity + 80) >= 81
  )
  AND (
    AVG(lineitem.l_quantity + 78) >= 79
  )
  AND (
    MIN(lineitem.l_quantity + 76) >= 77
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 74
  )
  AND (
    SUM(lineitem.l_quantity + 72) >= 73
  )
  AND (
    MAX(lineitem.l_quantity + 70) >= 71
  )
  AND (
    AVG(lineitem.l_quantity + 68) >= 69
  )
  AND (
    MIN(lineitem.l_quantity + 66) >= 67
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 64
  )
  AND (
    SUM(lineitem.l_quantity + 62) >= 63
  )
  AND (
    MAX(lineitem.l_quantity + 60) >= 61
  )
  AND (
    AVG(lineitem.l_quantity + 58) >= 59
  )
  AND (
    MIN(lineitem.l_quantity + 56) >= 57
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 54
  )
  AND (
    SUM(lineitem.l_quantity + 52) >= 53
  )
  AND (
    MAX(lineitem.l_quantity + 50) >= 51
  )
  AND (
    AVG(lineitem.l_quantity + 48) >= 49
  )
  AND (
    MIN(lineitem.l_quantity + 46) >= 47
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 44
  )
  AND (
    SUM(lineitem.l_quantity + 42) >= 43
  )
  AND (
    MAX(lineitem.l_quantity + 40) >= 41
  )
  AND (
    AVG(lineitem.l_quantity + 38) >= 39
  )
  AND (
    MIN(lineitem.l_quantity + 36) >= 37
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 34
  )
  AND (
    SUM(lineitem.l_quantity + 32) >= 33
  )
  AND (
    MAX(lineitem.l_quantity + 30) >= 31
  )
  AND (
    AVG(lineitem.l_quantity + 28) >= 29
  )
  AND (
    MIN(lineitem.l_quantity + 26) >= 27
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 24
  )
  AND (
    SUM(lineitem.l_quantity + 22) >= 23
  )
  AND (
    MAX(lineitem.l_quantity + 20) >= 21
  )
  AND (
    AVG(lineitem.l_quantity + 18) >= 19
  )
  AND (
    MIN(lineitem.l_quantity + 16) >= 17
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 14
  )
  AND (
    SUM(lineitem.l_quantity + 12) >= 13
  )
  AND (
    MAX(lineitem.l_quantity + 10) >= 11
  )
  AND (
    AVG(lineitem.l_quantity + 8) >= 9
  )
  AND (
    MIN(lineitem.l_quantity + 6) >= 7
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 4
  )
  AND (
    SUM(lineitem.l_quantity + 2) >= 3
  )
  AND (
    MAX(lineitem.l_quantity + 0) >= 1
  )
  AND (
    MIN(lineitem.l_quantity + 1) >= 2
  )
  AND (
    AVG(lineitem.l_quantity + 3) >= 4
  )
  AND (
    MAX(lineitem.l_quantity + 5) >= 6
  )
  AND (
    SUM(lineitem.l_quantity + 7) >= 8
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 9
  )
  AND (
    MIN(lineitem.l_quantity + 11) >= 12
  )
  AND (
    AVG(lineitem.l_quantity + 13) >= 14
  )
  AND (
    MAX(lineitem.l_quantity + 15) >= 16
  )
  AND (
    SUM(lineitem.l_quantity + 17) >= 18
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 19
  )
  AND (
    MIN(lineitem.l_quantity + 21) >= 22
  )
  AND (
    AVG(lineitem.l_quantity + 23) >= 24
  )
  AND (
    MAX(lineitem.l_quantity + 25) >= 26
  )
  AND (
    SUM(lineitem.l_quantity + 27) >= 28
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 29
  )
  AND (
    MIN(lineitem.l_quantity + 31) >= 32
  )
  AND (
    AVG(lineitem.l_quantity + 33) >= 34
  )
  AND (
    MAX(lineitem.l_quantity + 35) >= 36
  )
  AND (
    SUM(lineitem.l_quantity + 37) >= 38
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 39
  )
  AND (
    MIN(lineitem.l_quantity + 41) >= 42
  )
  AND (
    AVG(lineitem.l_quantity + 43) >= 44
  )
  AND (
    MAX(lineitem.l_quantity + 45) >= 46
  )
  AND (
    SUM(lineitem.l_quantity + 47) >= 48
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 49
  )
  AND (
    MIN(lineitem.l_quantity + 51) >= 52
  )
  AND (
    AVG(lineitem.l_quantity + 53) >= 54
  )
  AND (
    MAX(lineitem.l_quantity + 55) >= 56
  )
  AND (
    SUM(lineitem.l_quantity + 57) >= 58
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 59
  )
  AND (
    MIN(lineitem.l_quantity + 61) >= 62
  )
  AND (
    AVG(lineitem.l_quantity + 63) >= 64
  )
  AND (
    MAX(lineitem.l_quantity + 65) >= 66
  )
  AND (
    SUM(lineitem.l_quantity + 67) >= 68
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 69
  )
  AND (
    MIN(lineitem.l_quantity + 71) >= 72
  )
  AND (
    AVG(lineitem.l_quantity + 73) >= 74
  )
  AND (
    MAX(lineitem.l_quantity + 75) >= 76
  )
  AND (
    SUM(lineitem.l_quantity + 77) >= 78
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 79
  )
  AND (
    MIN(lineitem.l_quantity + 81) >= 82
  )
  AND (
    AVG(lineitem.l_quantity + 83) >= 84
  )
  AND (
    MAX(lineitem.l_quantity + 85) >= 86
  )
  AND (
    SUM(lineitem.l_quantity + 87) >= 88
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 89
  )
  AND (
    MIN(lineitem.l_quantity + 91) >= 92
  )
  AND (
    AVG(lineitem.l_quantity + 93) >= 94
  )
  AND (
    MAX(lineitem.l_quantity + 95) >= 96
  )
  AND (
    SUM(lineitem.l_quantity + 97) >= 98
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 99
  )
ORDER BY
  l_returnflag,
  l_linestatus""",
    1000: r"""SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS sum_disc_price,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE
  l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
  l_returnflag,
  l_linestatus
HAVING
  (
    AVG(lineitem.l_quantity + 998) >= 999
  )
  AND (
    MIN(lineitem.l_quantity + 996) >= 997
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 994
  )
  AND (
    SUM(lineitem.l_quantity + 992) >= 993
  )
  AND (
    MAX(lineitem.l_quantity + 990) >= 991
  )
  AND (
    AVG(lineitem.l_quantity + 988) >= 989
  )
  AND (
    MIN(lineitem.l_quantity + 986) >= 987
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 984
  )
  AND (
    SUM(lineitem.l_quantity + 982) >= 983
  )
  AND (
    MAX(lineitem.l_quantity + 980) >= 981
  )
  AND (
    AVG(lineitem.l_quantity + 978) >= 979
  )
  AND (
    MIN(lineitem.l_quantity + 976) >= 977
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 974
  )
  AND (
    SUM(lineitem.l_quantity + 972) >= 973
  )
  AND (
    MAX(lineitem.l_quantity + 970) >= 971
  )
  AND (
    AVG(lineitem.l_quantity + 968) >= 969
  )
  AND (
    MIN(lineitem.l_quantity + 966) >= 967
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 964
  )
  AND (
    SUM(lineitem.l_quantity + 962) >= 963
  )
  AND (
    MAX(lineitem.l_quantity + 960) >= 961
  )
  AND (
    AVG(lineitem.l_quantity + 958) >= 959
  )
  AND (
    MIN(lineitem.l_quantity + 956) >= 957
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 954
  )
  AND (
    SUM(lineitem.l_quantity + 952) >= 953
  )
  AND (
    MAX(lineitem.l_quantity + 950) >= 951
  )
  AND (
    AVG(lineitem.l_quantity + 948) >= 949
  )
  AND (
    MIN(lineitem.l_quantity + 946) >= 947
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 944
  )
  AND (
    SUM(lineitem.l_quantity + 942) >= 943
  )
  AND (
    MAX(lineitem.l_quantity + 940) >= 941
  )
  AND (
    AVG(lineitem.l_quantity + 938) >= 939
  )
  AND (
    MIN(lineitem.l_quantity + 936) >= 937
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 934
  )
  AND (
    SUM(lineitem.l_quantity + 932) >= 933
  )
  AND (
    MAX(lineitem.l_quantity + 930) >= 931
  )
  AND (
    AVG(lineitem.l_quantity + 928) >= 929
  )
  AND (
    MIN(lineitem.l_quantity + 926) >= 927
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 924
  )
  AND (
    SUM(lineitem.l_quantity + 922) >= 923
  )
  AND (
    MAX(lineitem.l_quantity + 920) >= 921
  )
  AND (
    AVG(lineitem.l_quantity + 918) >= 919
  )
  AND (
    MIN(lineitem.l_quantity + 916) >= 917
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 914
  )
  AND (
    SUM(lineitem.l_quantity + 912) >= 913
  )
  AND (
    MAX(lineitem.l_quantity + 910) >= 911
  )
  AND (
    AVG(lineitem.l_quantity + 908) >= 909
  )
  AND (
    MIN(lineitem.l_quantity + 906) >= 907
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 904
  )
  AND (
    SUM(lineitem.l_quantity + 902) >= 903
  )
  AND (
    MAX(lineitem.l_quantity + 900) >= 901
  )
  AND (
    AVG(lineitem.l_quantity + 898) >= 899
  )
  AND (
    MIN(lineitem.l_quantity + 896) >= 897
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 894
  )
  AND (
    SUM(lineitem.l_quantity + 892) >= 893
  )
  AND (
    MAX(lineitem.l_quantity + 890) >= 891
  )
  AND (
    AVG(lineitem.l_quantity + 888) >= 889
  )
  AND (
    MIN(lineitem.l_quantity + 886) >= 887
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 884
  )
  AND (
    SUM(lineitem.l_quantity + 882) >= 883
  )
  AND (
    MAX(lineitem.l_quantity + 880) >= 881
  )
  AND (
    AVG(lineitem.l_quantity + 878) >= 879
  )
  AND (
    MIN(lineitem.l_quantity + 876) >= 877
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 874
  )
  AND (
    SUM(lineitem.l_quantity + 872) >= 873
  )
  AND (
    MAX(lineitem.l_quantity + 870) >= 871
  )
  AND (
    AVG(lineitem.l_quantity + 868) >= 869
  )
  AND (
    MIN(lineitem.l_quantity + 866) >= 867
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 864
  )
  AND (
    SUM(lineitem.l_quantity + 862) >= 863
  )
  AND (
    MAX(lineitem.l_quantity + 860) >= 861
  )
  AND (
    AVG(lineitem.l_quantity + 858) >= 859
  )
  AND (
    MIN(lineitem.l_quantity + 856) >= 857
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 854
  )
  AND (
    SUM(lineitem.l_quantity + 852) >= 853
  )
  AND (
    MAX(lineitem.l_quantity + 850) >= 851
  )
  AND (
    AVG(lineitem.l_quantity + 848) >= 849
  )
  AND (
    MIN(lineitem.l_quantity + 846) >= 847
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 844
  )
  AND (
    SUM(lineitem.l_quantity + 842) >= 843
  )
  AND (
    MAX(lineitem.l_quantity + 840) >= 841
  )
  AND (
    AVG(lineitem.l_quantity + 838) >= 839
  )
  AND (
    MIN(lineitem.l_quantity + 836) >= 837
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 834
  )
  AND (
    SUM(lineitem.l_quantity + 832) >= 833
  )
  AND (
    MAX(lineitem.l_quantity + 830) >= 831
  )
  AND (
    AVG(lineitem.l_quantity + 828) >= 829
  )
  AND (
    MIN(lineitem.l_quantity + 826) >= 827
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 824
  )
  AND (
    SUM(lineitem.l_quantity + 822) >= 823
  )
  AND (
    MAX(lineitem.l_quantity + 820) >= 821
  )
  AND (
    AVG(lineitem.l_quantity + 818) >= 819
  )
  AND (
    MIN(lineitem.l_quantity + 816) >= 817
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 814
  )
  AND (
    SUM(lineitem.l_quantity + 812) >= 813
  )
  AND (
    MAX(lineitem.l_quantity + 810) >= 811
  )
  AND (
    AVG(lineitem.l_quantity + 808) >= 809
  )
  AND (
    MIN(lineitem.l_quantity + 806) >= 807
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 804
  )
  AND (
    SUM(lineitem.l_quantity + 802) >= 803
  )
  AND (
    MAX(lineitem.l_quantity + 800) >= 801
  )
  AND (
    AVG(lineitem.l_quantity + 798) >= 799
  )
  AND (
    MIN(lineitem.l_quantity + 796) >= 797
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 794
  )
  AND (
    SUM(lineitem.l_quantity + 792) >= 793
  )
  AND (
    MAX(lineitem.l_quantity + 790) >= 791
  )
  AND (
    AVG(lineitem.l_quantity + 788) >= 789
  )
  AND (
    MIN(lineitem.l_quantity + 786) >= 787
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 784
  )
  AND (
    SUM(lineitem.l_quantity + 782) >= 783
  )
  AND (
    MAX(lineitem.l_quantity + 780) >= 781
  )
  AND (
    AVG(lineitem.l_quantity + 778) >= 779
  )
  AND (
    MIN(lineitem.l_quantity + 776) >= 777
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 774
  )
  AND (
    SUM(lineitem.l_quantity + 772) >= 773
  )
  AND (
    MAX(lineitem.l_quantity + 770) >= 771
  )
  AND (
    AVG(lineitem.l_quantity + 768) >= 769
  )
  AND (
    MIN(lineitem.l_quantity + 766) >= 767
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 764
  )
  AND (
    SUM(lineitem.l_quantity + 762) >= 763
  )
  AND (
    MAX(lineitem.l_quantity + 760) >= 761
  )
  AND (
    AVG(lineitem.l_quantity + 758) >= 759
  )
  AND (
    MIN(lineitem.l_quantity + 756) >= 757
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 754
  )
  AND (
    SUM(lineitem.l_quantity + 752) >= 753
  )
  AND (
    MAX(lineitem.l_quantity + 750) >= 751
  )
  AND (
    AVG(lineitem.l_quantity + 748) >= 749
  )
  AND (
    MIN(lineitem.l_quantity + 746) >= 747
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 744
  )
  AND (
    SUM(lineitem.l_quantity + 742) >= 743
  )
  AND (
    MAX(lineitem.l_quantity + 740) >= 741
  )
  AND (
    AVG(lineitem.l_quantity + 738) >= 739
  )
  AND (
    MIN(lineitem.l_quantity + 736) >= 737
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 734
  )
  AND (
    SUM(lineitem.l_quantity + 732) >= 733
  )
  AND (
    MAX(lineitem.l_quantity + 730) >= 731
  )
  AND (
    AVG(lineitem.l_quantity + 728) >= 729
  )
  AND (
    MIN(lineitem.l_quantity + 726) >= 727
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 724
  )
  AND (
    SUM(lineitem.l_quantity + 722) >= 723
  )
  AND (
    MAX(lineitem.l_quantity + 720) >= 721
  )
  AND (
    AVG(lineitem.l_quantity + 718) >= 719
  )
  AND (
    MIN(lineitem.l_quantity + 716) >= 717
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 714
  )
  AND (
    SUM(lineitem.l_quantity + 712) >= 713
  )
  AND (
    MAX(lineitem.l_quantity + 710) >= 711
  )
  AND (
    AVG(lineitem.l_quantity + 708) >= 709
  )
  AND (
    MIN(lineitem.l_quantity + 706) >= 707
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 704
  )
  AND (
    SUM(lineitem.l_quantity + 702) >= 703
  )
  AND (
    MAX(lineitem.l_quantity + 700) >= 701
  )
  AND (
    AVG(lineitem.l_quantity + 698) >= 699
  )
  AND (
    MIN(lineitem.l_quantity + 696) >= 697
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 694
  )
  AND (
    SUM(lineitem.l_quantity + 692) >= 693
  )
  AND (
    MAX(lineitem.l_quantity + 690) >= 691
  )
  AND (
    AVG(lineitem.l_quantity + 688) >= 689
  )
  AND (
    MIN(lineitem.l_quantity + 686) >= 687
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 684
  )
  AND (
    SUM(lineitem.l_quantity + 682) >= 683
  )
  AND (
    MAX(lineitem.l_quantity + 680) >= 681
  )
  AND (
    AVG(lineitem.l_quantity + 678) >= 679
  )
  AND (
    MIN(lineitem.l_quantity + 676) >= 677
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 674
  )
  AND (
    SUM(lineitem.l_quantity + 672) >= 673
  )
  AND (
    MAX(lineitem.l_quantity + 670) >= 671
  )
  AND (
    AVG(lineitem.l_quantity + 668) >= 669
  )
  AND (
    MIN(lineitem.l_quantity + 666) >= 667
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 664
  )
  AND (
    SUM(lineitem.l_quantity + 662) >= 663
  )
  AND (
    MAX(lineitem.l_quantity + 660) >= 661
  )
  AND (
    AVG(lineitem.l_quantity + 658) >= 659
  )
  AND (
    MIN(lineitem.l_quantity + 656) >= 657
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 654
  )
  AND (
    SUM(lineitem.l_quantity + 652) >= 653
  )
  AND (
    MAX(lineitem.l_quantity + 650) >= 651
  )
  AND (
    AVG(lineitem.l_quantity + 648) >= 649
  )
  AND (
    MIN(lineitem.l_quantity + 646) >= 647
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 644
  )
  AND (
    SUM(lineitem.l_quantity + 642) >= 643
  )
  AND (
    MAX(lineitem.l_quantity + 640) >= 641
  )
  AND (
    AVG(lineitem.l_quantity + 638) >= 639
  )
  AND (
    MIN(lineitem.l_quantity + 636) >= 637
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 634
  )
  AND (
    SUM(lineitem.l_quantity + 632) >= 633
  )
  AND (
    MAX(lineitem.l_quantity + 630) >= 631
  )
  AND (
    AVG(lineitem.l_quantity + 628) >= 629
  )
  AND (
    MIN(lineitem.l_quantity + 626) >= 627
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 624
  )
  AND (
    SUM(lineitem.l_quantity + 622) >= 623
  )
  AND (
    MAX(lineitem.l_quantity + 620) >= 621
  )
  AND (
    AVG(lineitem.l_quantity + 618) >= 619
  )
  AND (
    MIN(lineitem.l_quantity + 616) >= 617
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 614
  )
  AND (
    SUM(lineitem.l_quantity + 612) >= 613
  )
  AND (
    MAX(lineitem.l_quantity + 610) >= 611
  )
  AND (
    AVG(lineitem.l_quantity + 608) >= 609
  )
  AND (
    MIN(lineitem.l_quantity + 606) >= 607
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 604
  )
  AND (
    SUM(lineitem.l_quantity + 602) >= 603
  )
  AND (
    MAX(lineitem.l_quantity + 600) >= 601
  )
  AND (
    AVG(lineitem.l_quantity + 598) >= 599
  )
  AND (
    MIN(lineitem.l_quantity + 596) >= 597
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 594
  )
  AND (
    SUM(lineitem.l_quantity + 592) >= 593
  )
  AND (
    MAX(lineitem.l_quantity + 590) >= 591
  )
  AND (
    AVG(lineitem.l_quantity + 588) >= 589
  )
  AND (
    MIN(lineitem.l_quantity + 586) >= 587
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 584
  )
  AND (
    SUM(lineitem.l_quantity + 582) >= 583
  )
  AND (
    MAX(lineitem.l_quantity + 580) >= 581
  )
  AND (
    AVG(lineitem.l_quantity + 578) >= 579
  )
  AND (
    MIN(lineitem.l_quantity + 576) >= 577
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 574
  )
  AND (
    SUM(lineitem.l_quantity + 572) >= 573
  )
  AND (
    MAX(lineitem.l_quantity + 570) >= 571
  )
  AND (
    AVG(lineitem.l_quantity + 568) >= 569
  )
  AND (
    MIN(lineitem.l_quantity + 566) >= 567
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 564
  )
  AND (
    SUM(lineitem.l_quantity + 562) >= 563
  )
  AND (
    MAX(lineitem.l_quantity + 560) >= 561
  )
  AND (
    AVG(lineitem.l_quantity + 558) >= 559
  )
  AND (
    MIN(lineitem.l_quantity + 556) >= 557
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 554
  )
  AND (
    SUM(lineitem.l_quantity + 552) >= 553
  )
  AND (
    MAX(lineitem.l_quantity + 550) >= 551
  )
  AND (
    AVG(lineitem.l_quantity + 548) >= 549
  )
  AND (
    MIN(lineitem.l_quantity + 546) >= 547
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 544
  )
  AND (
    SUM(lineitem.l_quantity + 542) >= 543
  )
  AND (
    MAX(lineitem.l_quantity + 540) >= 541
  )
  AND (
    AVG(lineitem.l_quantity + 538) >= 539
  )
  AND (
    MIN(lineitem.l_quantity + 536) >= 537
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 534
  )
  AND (
    SUM(lineitem.l_quantity + 532) >= 533
  )
  AND (
    MAX(lineitem.l_quantity + 530) >= 531
  )
  AND (
    AVG(lineitem.l_quantity + 528) >= 529
  )
  AND (
    MIN(lineitem.l_quantity + 526) >= 527
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 524
  )
  AND (
    SUM(lineitem.l_quantity + 522) >= 523
  )
  AND (
    MAX(lineitem.l_quantity + 520) >= 521
  )
  AND (
    AVG(lineitem.l_quantity + 518) >= 519
  )
  AND (
    MIN(lineitem.l_quantity + 516) >= 517
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 514
  )
  AND (
    SUM(lineitem.l_quantity + 512) >= 513
  )
  AND (
    MAX(lineitem.l_quantity + 510) >= 511
  )
  AND (
    AVG(lineitem.l_quantity + 508) >= 509
  )
  AND (
    MIN(lineitem.l_quantity + 506) >= 507
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 504
  )
  AND (
    SUM(lineitem.l_quantity + 502) >= 503
  )
  AND (
    MAX(lineitem.l_quantity + 500) >= 501
  )
  AND (
    AVG(lineitem.l_quantity + 498) >= 499
  )
  AND (
    MIN(lineitem.l_quantity + 496) >= 497
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 494
  )
  AND (
    SUM(lineitem.l_quantity + 492) >= 493
  )
  AND (
    MAX(lineitem.l_quantity + 490) >= 491
  )
  AND (
    AVG(lineitem.l_quantity + 488) >= 489
  )
  AND (
    MIN(lineitem.l_quantity + 486) >= 487
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 484
  )
  AND (
    SUM(lineitem.l_quantity + 482) >= 483
  )
  AND (
    MAX(lineitem.l_quantity + 480) >= 481
  )
  AND (
    AVG(lineitem.l_quantity + 478) >= 479
  )
  AND (
    MIN(lineitem.l_quantity + 476) >= 477
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 474
  )
  AND (
    SUM(lineitem.l_quantity + 472) >= 473
  )
  AND (
    MAX(lineitem.l_quantity + 470) >= 471
  )
  AND (
    AVG(lineitem.l_quantity + 468) >= 469
  )
  AND (
    MIN(lineitem.l_quantity + 466) >= 467
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 464
  )
  AND (
    SUM(lineitem.l_quantity + 462) >= 463
  )
  AND (
    MAX(lineitem.l_quantity + 460) >= 461
  )
  AND (
    AVG(lineitem.l_quantity + 458) >= 459
  )
  AND (
    MIN(lineitem.l_quantity + 456) >= 457
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 454
  )
  AND (
    SUM(lineitem.l_quantity + 452) >= 453
  )
  AND (
    MAX(lineitem.l_quantity + 450) >= 451
  )
  AND (
    AVG(lineitem.l_quantity + 448) >= 449
  )
  AND (
    MIN(lineitem.l_quantity + 446) >= 447
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 444
  )
  AND (
    SUM(lineitem.l_quantity + 442) >= 443
  )
  AND (
    MAX(lineitem.l_quantity + 440) >= 441
  )
  AND (
    AVG(lineitem.l_quantity + 438) >= 439
  )
  AND (
    MIN(lineitem.l_quantity + 436) >= 437
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 434
  )
  AND (
    SUM(lineitem.l_quantity + 432) >= 433
  )
  AND (
    MAX(lineitem.l_quantity + 430) >= 431
  )
  AND (
    AVG(lineitem.l_quantity + 428) >= 429
  )
  AND (
    MIN(lineitem.l_quantity + 426) >= 427
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 424
  )
  AND (
    SUM(lineitem.l_quantity + 422) >= 423
  )
  AND (
    MAX(lineitem.l_quantity + 420) >= 421
  )
  AND (
    AVG(lineitem.l_quantity + 418) >= 419
  )
  AND (
    MIN(lineitem.l_quantity + 416) >= 417
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 414
  )
  AND (
    SUM(lineitem.l_quantity + 412) >= 413
  )
  AND (
    MAX(lineitem.l_quantity + 410) >= 411
  )
  AND (
    AVG(lineitem.l_quantity + 408) >= 409
  )
  AND (
    MIN(lineitem.l_quantity + 406) >= 407
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 404
  )
  AND (
    SUM(lineitem.l_quantity + 402) >= 403
  )
  AND (
    MAX(lineitem.l_quantity + 400) >= 401
  )
  AND (
    AVG(lineitem.l_quantity + 398) >= 399
  )
  AND (
    MIN(lineitem.l_quantity + 396) >= 397
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 394
  )
  AND (
    SUM(lineitem.l_quantity + 392) >= 393
  )
  AND (
    MAX(lineitem.l_quantity + 390) >= 391
  )
  AND (
    AVG(lineitem.l_quantity + 388) >= 389
  )
  AND (
    MIN(lineitem.l_quantity + 386) >= 387
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 384
  )
  AND (
    SUM(lineitem.l_quantity + 382) >= 383
  )
  AND (
    MAX(lineitem.l_quantity + 380) >= 381
  )
  AND (
    AVG(lineitem.l_quantity + 378) >= 379
  )
  AND (
    MIN(lineitem.l_quantity + 376) >= 377
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 374
  )
  AND (
    SUM(lineitem.l_quantity + 372) >= 373
  )
  AND (
    MAX(lineitem.l_quantity + 370) >= 371
  )
  AND (
    AVG(lineitem.l_quantity + 368) >= 369
  )
  AND (
    MIN(lineitem.l_quantity + 366) >= 367
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 364
  )
  AND (
    SUM(lineitem.l_quantity + 362) >= 363
  )
  AND (
    MAX(lineitem.l_quantity + 360) >= 361
  )
  AND (
    AVG(lineitem.l_quantity + 358) >= 359
  )
  AND (
    MIN(lineitem.l_quantity + 356) >= 357
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 354
  )
  AND (
    SUM(lineitem.l_quantity + 352) >= 353
  )
  AND (
    MAX(lineitem.l_quantity + 350) >= 351
  )
  AND (
    AVG(lineitem.l_quantity + 348) >= 349
  )
  AND (
    MIN(lineitem.l_quantity + 346) >= 347
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 344
  )
  AND (
    SUM(lineitem.l_quantity + 342) >= 343
  )
  AND (
    MAX(lineitem.l_quantity + 340) >= 341
  )
  AND (
    AVG(lineitem.l_quantity + 338) >= 339
  )
  AND (
    MIN(lineitem.l_quantity + 336) >= 337
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 334
  )
  AND (
    SUM(lineitem.l_quantity + 332) >= 333
  )
  AND (
    MAX(lineitem.l_quantity + 330) >= 331
  )
  AND (
    AVG(lineitem.l_quantity + 328) >= 329
  )
  AND (
    MIN(lineitem.l_quantity + 326) >= 327
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 324
  )
  AND (
    SUM(lineitem.l_quantity + 322) >= 323
  )
  AND (
    MAX(lineitem.l_quantity + 320) >= 321
  )
  AND (
    AVG(lineitem.l_quantity + 318) >= 319
  )
  AND (
    MIN(lineitem.l_quantity + 316) >= 317
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 314
  )
  AND (
    SUM(lineitem.l_quantity + 312) >= 313
  )
  AND (
    MAX(lineitem.l_quantity + 310) >= 311
  )
  AND (
    AVG(lineitem.l_quantity + 308) >= 309
  )
  AND (
    MIN(lineitem.l_quantity + 306) >= 307
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 304
  )
  AND (
    SUM(lineitem.l_quantity + 302) >= 303
  )
  AND (
    MAX(lineitem.l_quantity + 300) >= 301
  )
  AND (
    AVG(lineitem.l_quantity + 298) >= 299
  )
  AND (
    MIN(lineitem.l_quantity + 296) >= 297
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 294
  )
  AND (
    SUM(lineitem.l_quantity + 292) >= 293
  )
  AND (
    MAX(lineitem.l_quantity + 290) >= 291
  )
  AND (
    AVG(lineitem.l_quantity + 288) >= 289
  )
  AND (
    MIN(lineitem.l_quantity + 286) >= 287
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 284
  )
  AND (
    SUM(lineitem.l_quantity + 282) >= 283
  )
  AND (
    MAX(lineitem.l_quantity + 280) >= 281
  )
  AND (
    AVG(lineitem.l_quantity + 278) >= 279
  )
  AND (
    MIN(lineitem.l_quantity + 276) >= 277
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 274
  )
  AND (
    SUM(lineitem.l_quantity + 272) >= 273
  )
  AND (
    MAX(lineitem.l_quantity + 270) >= 271
  )
  AND (
    AVG(lineitem.l_quantity + 268) >= 269
  )
  AND (
    MIN(lineitem.l_quantity + 266) >= 267
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 264
  )
  AND (
    SUM(lineitem.l_quantity + 262) >= 263
  )
  AND (
    MAX(lineitem.l_quantity + 260) >= 261
  )
  AND (
    AVG(lineitem.l_quantity + 258) >= 259
  )
  AND (
    MIN(lineitem.l_quantity + 256) >= 257
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 254
  )
  AND (
    SUM(lineitem.l_quantity + 252) >= 253
  )
  AND (
    MAX(lineitem.l_quantity + 250) >= 251
  )
  AND (
    AVG(lineitem.l_quantity + 248) >= 249
  )
  AND (
    MIN(lineitem.l_quantity + 246) >= 247
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 244
  )
  AND (
    SUM(lineitem.l_quantity + 242) >= 243
  )
  AND (
    MAX(lineitem.l_quantity + 240) >= 241
  )
  AND (
    AVG(lineitem.l_quantity + 238) >= 239
  )
  AND (
    MIN(lineitem.l_quantity + 236) >= 237
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 234
  )
  AND (
    SUM(lineitem.l_quantity + 232) >= 233
  )
  AND (
    MAX(lineitem.l_quantity + 230) >= 231
  )
  AND (
    AVG(lineitem.l_quantity + 228) >= 229
  )
  AND (
    MIN(lineitem.l_quantity + 226) >= 227
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 224
  )
  AND (
    SUM(lineitem.l_quantity + 222) >= 223
  )
  AND (
    MAX(lineitem.l_quantity + 220) >= 221
  )
  AND (
    AVG(lineitem.l_quantity + 218) >= 219
  )
  AND (
    MIN(lineitem.l_quantity + 216) >= 217
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 214
  )
  AND (
    SUM(lineitem.l_quantity + 212) >= 213
  )
  AND (
    MAX(lineitem.l_quantity + 210) >= 211
  )
  AND (
    AVG(lineitem.l_quantity + 208) >= 209
  )
  AND (
    MIN(lineitem.l_quantity + 206) >= 207
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 204
  )
  AND (
    SUM(lineitem.l_quantity + 202) >= 203
  )
  AND (
    MAX(lineitem.l_quantity + 200) >= 201
  )
  AND (
    AVG(lineitem.l_quantity + 198) >= 199
  )
  AND (
    MIN(lineitem.l_quantity + 196) >= 197
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 194
  )
  AND (
    SUM(lineitem.l_quantity + 192) >= 193
  )
  AND (
    MAX(lineitem.l_quantity + 190) >= 191
  )
  AND (
    AVG(lineitem.l_quantity + 188) >= 189
  )
  AND (
    MIN(lineitem.l_quantity + 186) >= 187
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 184
  )
  AND (
    SUM(lineitem.l_quantity + 182) >= 183
  )
  AND (
    MAX(lineitem.l_quantity + 180) >= 181
  )
  AND (
    AVG(lineitem.l_quantity + 178) >= 179
  )
  AND (
    MIN(lineitem.l_quantity + 176) >= 177
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 174
  )
  AND (
    SUM(lineitem.l_quantity + 172) >= 173
  )
  AND (
    MAX(lineitem.l_quantity + 170) >= 171
  )
  AND (
    AVG(lineitem.l_quantity + 168) >= 169
  )
  AND (
    MIN(lineitem.l_quantity + 166) >= 167
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 164
  )
  AND (
    SUM(lineitem.l_quantity + 162) >= 163
  )
  AND (
    MAX(lineitem.l_quantity + 160) >= 161
  )
  AND (
    AVG(lineitem.l_quantity + 158) >= 159
  )
  AND (
    MIN(lineitem.l_quantity + 156) >= 157
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 154
  )
  AND (
    SUM(lineitem.l_quantity + 152) >= 153
  )
  AND (
    MAX(lineitem.l_quantity + 150) >= 151
  )
  AND (
    AVG(lineitem.l_quantity + 148) >= 149
  )
  AND (
    MIN(lineitem.l_quantity + 146) >= 147
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 144
  )
  AND (
    SUM(lineitem.l_quantity + 142) >= 143
  )
  AND (
    MAX(lineitem.l_quantity + 140) >= 141
  )
  AND (
    AVG(lineitem.l_quantity + 138) >= 139
  )
  AND (
    MIN(lineitem.l_quantity + 136) >= 137
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 134
  )
  AND (
    SUM(lineitem.l_quantity + 132) >= 133
  )
  AND (
    MAX(lineitem.l_quantity + 130) >= 131
  )
  AND (
    AVG(lineitem.l_quantity + 128) >= 129
  )
  AND (
    MIN(lineitem.l_quantity + 126) >= 127
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 124
  )
  AND (
    SUM(lineitem.l_quantity + 122) >= 123
  )
  AND (
    MAX(lineitem.l_quantity + 120) >= 121
  )
  AND (
    AVG(lineitem.l_quantity + 118) >= 119
  )
  AND (
    MIN(lineitem.l_quantity + 116) >= 117
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 114
  )
  AND (
    SUM(lineitem.l_quantity + 112) >= 113
  )
  AND (
    MAX(lineitem.l_quantity + 110) >= 111
  )
  AND (
    AVG(lineitem.l_quantity + 108) >= 109
  )
  AND (
    MIN(lineitem.l_quantity + 106) >= 107
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 104
  )
  AND (
    SUM(lineitem.l_quantity + 102) >= 103
  )
  AND (
    MAX(lineitem.l_quantity + 100) >= 101
  )
  AND (
    AVG(lineitem.l_quantity + 98) >= 99
  )
  AND (
    MIN(lineitem.l_quantity + 96) >= 97
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 94
  )
  AND (
    SUM(lineitem.l_quantity + 92) >= 93
  )
  AND (
    MAX(lineitem.l_quantity + 90) >= 91
  )
  AND (
    AVG(lineitem.l_quantity + 88) >= 89
  )
  AND (
    MIN(lineitem.l_quantity + 86) >= 87
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 84
  )
  AND (
    SUM(lineitem.l_quantity + 82) >= 83
  )
  AND (
    MAX(lineitem.l_quantity + 80) >= 81
  )
  AND (
    AVG(lineitem.l_quantity + 78) >= 79
  )
  AND (
    MIN(lineitem.l_quantity + 76) >= 77
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 74
  )
  AND (
    SUM(lineitem.l_quantity + 72) >= 73
  )
  AND (
    MAX(lineitem.l_quantity + 70) >= 71
  )
  AND (
    AVG(lineitem.l_quantity + 68) >= 69
  )
  AND (
    MIN(lineitem.l_quantity + 66) >= 67
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 64
  )
  AND (
    SUM(lineitem.l_quantity + 62) >= 63
  )
  AND (
    MAX(lineitem.l_quantity + 60) >= 61
  )
  AND (
    AVG(lineitem.l_quantity + 58) >= 59
  )
  AND (
    MIN(lineitem.l_quantity + 56) >= 57
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 54
  )
  AND (
    SUM(lineitem.l_quantity + 52) >= 53
  )
  AND (
    MAX(lineitem.l_quantity + 50) >= 51
  )
  AND (
    AVG(lineitem.l_quantity + 48) >= 49
  )
  AND (
    MIN(lineitem.l_quantity + 46) >= 47
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 44
  )
  AND (
    SUM(lineitem.l_quantity + 42) >= 43
  )
  AND (
    MAX(lineitem.l_quantity + 40) >= 41
  )
  AND (
    AVG(lineitem.l_quantity + 38) >= 39
  )
  AND (
    MIN(lineitem.l_quantity + 36) >= 37
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 34
  )
  AND (
    SUM(lineitem.l_quantity + 32) >= 33
  )
  AND (
    MAX(lineitem.l_quantity + 30) >= 31
  )
  AND (
    AVG(lineitem.l_quantity + 28) >= 29
  )
  AND (
    MIN(lineitem.l_quantity + 26) >= 27
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 24
  )
  AND (
    SUM(lineitem.l_quantity + 22) >= 23
  )
  AND (
    MAX(lineitem.l_quantity + 20) >= 21
  )
  AND (
    AVG(lineitem.l_quantity + 18) >= 19
  )
  AND (
    MIN(lineitem.l_quantity + 16) >= 17
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 14
  )
  AND (
    SUM(lineitem.l_quantity + 12) >= 13
  )
  AND (
    MAX(lineitem.l_quantity + 10) >= 11
  )
  AND (
    AVG(lineitem.l_quantity + 8) >= 9
  )
  AND (
    MIN(lineitem.l_quantity + 6) >= 7
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 4
  )
  AND (
    SUM(lineitem.l_quantity + 2) >= 3
  )
  AND (
    MAX(lineitem.l_quantity + 0) >= 1
  )
  AND (
    MIN(lineitem.l_quantity + 1) >= 2
  )
  AND (
    AVG(lineitem.l_quantity + 3) >= 4
  )
  AND (
    MAX(lineitem.l_quantity + 5) >= 6
  )
  AND (
    SUM(lineitem.l_quantity + 7) >= 8
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 9
  )
  AND (
    MIN(lineitem.l_quantity + 11) >= 12
  )
  AND (
    AVG(lineitem.l_quantity + 13) >= 14
  )
  AND (
    MAX(lineitem.l_quantity + 15) >= 16
  )
  AND (
    SUM(lineitem.l_quantity + 17) >= 18
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 19
  )
  AND (
    MIN(lineitem.l_quantity + 21) >= 22
  )
  AND (
    AVG(lineitem.l_quantity + 23) >= 24
  )
  AND (
    MAX(lineitem.l_quantity + 25) >= 26
  )
  AND (
    SUM(lineitem.l_quantity + 27) >= 28
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 29
  )
  AND (
    MIN(lineitem.l_quantity + 31) >= 32
  )
  AND (
    AVG(lineitem.l_quantity + 33) >= 34
  )
  AND (
    MAX(lineitem.l_quantity + 35) >= 36
  )
  AND (
    SUM(lineitem.l_quantity + 37) >= 38
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 39
  )
  AND (
    MIN(lineitem.l_quantity + 41) >= 42
  )
  AND (
    AVG(lineitem.l_quantity + 43) >= 44
  )
  AND (
    MAX(lineitem.l_quantity + 45) >= 46
  )
  AND (
    SUM(lineitem.l_quantity + 47) >= 48
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 49
  )
  AND (
    MIN(lineitem.l_quantity + 51) >= 52
  )
  AND (
    AVG(lineitem.l_quantity + 53) >= 54
  )
  AND (
    MAX(lineitem.l_quantity + 55) >= 56
  )
  AND (
    SUM(lineitem.l_quantity + 57) >= 58
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 59
  )
  AND (
    MIN(lineitem.l_quantity + 61) >= 62
  )
  AND (
    AVG(lineitem.l_quantity + 63) >= 64
  )
  AND (
    MAX(lineitem.l_quantity + 65) >= 66
  )
  AND (
    SUM(lineitem.l_quantity + 67) >= 68
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 69
  )
  AND (
    MIN(lineitem.l_quantity + 71) >= 72
  )
  AND (
    AVG(lineitem.l_quantity + 73) >= 74
  )
  AND (
    MAX(lineitem.l_quantity + 75) >= 76
  )
  AND (
    SUM(lineitem.l_quantity + 77) >= 78
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 79
  )
  AND (
    MIN(lineitem.l_quantity + 81) >= 82
  )
  AND (
    AVG(lineitem.l_quantity + 83) >= 84
  )
  AND (
    MAX(lineitem.l_quantity + 85) >= 86
  )
  AND (
    SUM(lineitem.l_quantity + 87) >= 88
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 89
  )
  AND (
    MIN(lineitem.l_quantity + 91) >= 92
  )
  AND (
    AVG(lineitem.l_quantity + 93) >= 94
  )
  AND (
    MAX(lineitem.l_quantity + 95) >= 96
  )
  AND (
    SUM(lineitem.l_quantity + 97) >= 98
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 99
  )
  AND (
    MIN(lineitem.l_quantity + 101) >= 102
  )
  AND (
    AVG(lineitem.l_quantity + 103) >= 104
  )
  AND (
    MAX(lineitem.l_quantity + 105) >= 106
  )
  AND (
    SUM(lineitem.l_quantity + 107) >= 108
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 109
  )
  AND (
    MIN(lineitem.l_quantity + 111) >= 112
  )
  AND (
    AVG(lineitem.l_quantity + 113) >= 114
  )
  AND (
    MAX(lineitem.l_quantity + 115) >= 116
  )
  AND (
    SUM(lineitem.l_quantity + 117) >= 118
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 119
  )
  AND (
    MIN(lineitem.l_quantity + 121) >= 122
  )
  AND (
    AVG(lineitem.l_quantity + 123) >= 124
  )
  AND (
    MAX(lineitem.l_quantity + 125) >= 126
  )
  AND (
    SUM(lineitem.l_quantity + 127) >= 128
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 129
  )
  AND (
    MIN(lineitem.l_quantity + 131) >= 132
  )
  AND (
    AVG(lineitem.l_quantity + 133) >= 134
  )
  AND (
    MAX(lineitem.l_quantity + 135) >= 136
  )
  AND (
    SUM(lineitem.l_quantity + 137) >= 138
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 139
  )
  AND (
    MIN(lineitem.l_quantity + 141) >= 142
  )
  AND (
    AVG(lineitem.l_quantity + 143) >= 144
  )
  AND (
    MAX(lineitem.l_quantity + 145) >= 146
  )
  AND (
    SUM(lineitem.l_quantity + 147) >= 148
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 149
  )
  AND (
    MIN(lineitem.l_quantity + 151) >= 152
  )
  AND (
    AVG(lineitem.l_quantity + 153) >= 154
  )
  AND (
    MAX(lineitem.l_quantity + 155) >= 156
  )
  AND (
    SUM(lineitem.l_quantity + 157) >= 158
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 159
  )
  AND (
    MIN(lineitem.l_quantity + 161) >= 162
  )
  AND (
    AVG(lineitem.l_quantity + 163) >= 164
  )
  AND (
    MAX(lineitem.l_quantity + 165) >= 166
  )
  AND (
    SUM(lineitem.l_quantity + 167) >= 168
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 169
  )
  AND (
    MIN(lineitem.l_quantity + 171) >= 172
  )
  AND (
    AVG(lineitem.l_quantity + 173) >= 174
  )
  AND (
    MAX(lineitem.l_quantity + 175) >= 176
  )
  AND (
    SUM(lineitem.l_quantity + 177) >= 178
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 179
  )
  AND (
    MIN(lineitem.l_quantity + 181) >= 182
  )
  AND (
    AVG(lineitem.l_quantity + 183) >= 184
  )
  AND (
    MAX(lineitem.l_quantity + 185) >= 186
  )
  AND (
    SUM(lineitem.l_quantity + 187) >= 188
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 189
  )
  AND (
    MIN(lineitem.l_quantity + 191) >= 192
  )
  AND (
    AVG(lineitem.l_quantity + 193) >= 194
  )
  AND (
    MAX(lineitem.l_quantity + 195) >= 196
  )
  AND (
    SUM(lineitem.l_quantity + 197) >= 198
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 199
  )
  AND (
    MIN(lineitem.l_quantity + 201) >= 202
  )
  AND (
    AVG(lineitem.l_quantity + 203) >= 204
  )
  AND (
    MAX(lineitem.l_quantity + 205) >= 206
  )
  AND (
    SUM(lineitem.l_quantity + 207) >= 208
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 209
  )
  AND (
    MIN(lineitem.l_quantity + 211) >= 212
  )
  AND (
    AVG(lineitem.l_quantity + 213) >= 214
  )
  AND (
    MAX(lineitem.l_quantity + 215) >= 216
  )
  AND (
    SUM(lineitem.l_quantity + 217) >= 218
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 219
  )
  AND (
    MIN(lineitem.l_quantity + 221) >= 222
  )
  AND (
    AVG(lineitem.l_quantity + 223) >= 224
  )
  AND (
    MAX(lineitem.l_quantity + 225) >= 226
  )
  AND (
    SUM(lineitem.l_quantity + 227) >= 228
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 229
  )
  AND (
    MIN(lineitem.l_quantity + 231) >= 232
  )
  AND (
    AVG(lineitem.l_quantity + 233) >= 234
  )
  AND (
    MAX(lineitem.l_quantity + 235) >= 236
  )
  AND (
    SUM(lineitem.l_quantity + 237) >= 238
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 239
  )
  AND (
    MIN(lineitem.l_quantity + 241) >= 242
  )
  AND (
    AVG(lineitem.l_quantity + 243) >= 244
  )
  AND (
    MAX(lineitem.l_quantity + 245) >= 246
  )
  AND (
    SUM(lineitem.l_quantity + 247) >= 248
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 249
  )
  AND (
    MIN(lineitem.l_quantity + 251) >= 252
  )
  AND (
    AVG(lineitem.l_quantity + 253) >= 254
  )
  AND (
    MAX(lineitem.l_quantity + 255) >= 256
  )
  AND (
    SUM(lineitem.l_quantity + 257) >= 258
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 259
  )
  AND (
    MIN(lineitem.l_quantity + 261) >= 262
  )
  AND (
    AVG(lineitem.l_quantity + 263) >= 264
  )
  AND (
    MAX(lineitem.l_quantity + 265) >= 266
  )
  AND (
    SUM(lineitem.l_quantity + 267) >= 268
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 269
  )
  AND (
    MIN(lineitem.l_quantity + 271) >= 272
  )
  AND (
    AVG(lineitem.l_quantity + 273) >= 274
  )
  AND (
    MAX(lineitem.l_quantity + 275) >= 276
  )
  AND (
    SUM(lineitem.l_quantity + 277) >= 278
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 279
  )
  AND (
    MIN(lineitem.l_quantity + 281) >= 282
  )
  AND (
    AVG(lineitem.l_quantity + 283) >= 284
  )
  AND (
    MAX(lineitem.l_quantity + 285) >= 286
  )
  AND (
    SUM(lineitem.l_quantity + 287) >= 288
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 289
  )
  AND (
    MIN(lineitem.l_quantity + 291) >= 292
  )
  AND (
    AVG(lineitem.l_quantity + 293) >= 294
  )
  AND (
    MAX(lineitem.l_quantity + 295) >= 296
  )
  AND (
    SUM(lineitem.l_quantity + 297) >= 298
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 299
  )
  AND (
    MIN(lineitem.l_quantity + 301) >= 302
  )
  AND (
    AVG(lineitem.l_quantity + 303) >= 304
  )
  AND (
    MAX(lineitem.l_quantity + 305) >= 306
  )
  AND (
    SUM(lineitem.l_quantity + 307) >= 308
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 309
  )
  AND (
    MIN(lineitem.l_quantity + 311) >= 312
  )
  AND (
    AVG(lineitem.l_quantity + 313) >= 314
  )
  AND (
    MAX(lineitem.l_quantity + 315) >= 316
  )
  AND (
    SUM(lineitem.l_quantity + 317) >= 318
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 319
  )
  AND (
    MIN(lineitem.l_quantity + 321) >= 322
  )
  AND (
    AVG(lineitem.l_quantity + 323) >= 324
  )
  AND (
    MAX(lineitem.l_quantity + 325) >= 326
  )
  AND (
    SUM(lineitem.l_quantity + 327) >= 328
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 329
  )
  AND (
    MIN(lineitem.l_quantity + 331) >= 332
  )
  AND (
    AVG(lineitem.l_quantity + 333) >= 334
  )
  AND (
    MAX(lineitem.l_quantity + 335) >= 336
  )
  AND (
    SUM(lineitem.l_quantity + 337) >= 338
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 339
  )
  AND (
    MIN(lineitem.l_quantity + 341) >= 342
  )
  AND (
    AVG(lineitem.l_quantity + 343) >= 344
  )
  AND (
    MAX(lineitem.l_quantity + 345) >= 346
  )
  AND (
    SUM(lineitem.l_quantity + 347) >= 348
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 349
  )
  AND (
    MIN(lineitem.l_quantity + 351) >= 352
  )
  AND (
    AVG(lineitem.l_quantity + 353) >= 354
  )
  AND (
    MAX(lineitem.l_quantity + 355) >= 356
  )
  AND (
    SUM(lineitem.l_quantity + 357) >= 358
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 359
  )
  AND (
    MIN(lineitem.l_quantity + 361) >= 362
  )
  AND (
    AVG(lineitem.l_quantity + 363) >= 364
  )
  AND (
    MAX(lineitem.l_quantity + 365) >= 366
  )
  AND (
    SUM(lineitem.l_quantity + 367) >= 368
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 369
  )
  AND (
    MIN(lineitem.l_quantity + 371) >= 372
  )
  AND (
    AVG(lineitem.l_quantity + 373) >= 374
  )
  AND (
    MAX(lineitem.l_quantity + 375) >= 376
  )
  AND (
    SUM(lineitem.l_quantity + 377) >= 378
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 379
  )
  AND (
    MIN(lineitem.l_quantity + 381) >= 382
  )
  AND (
    AVG(lineitem.l_quantity + 383) >= 384
  )
  AND (
    MAX(lineitem.l_quantity + 385) >= 386
  )
  AND (
    SUM(lineitem.l_quantity + 387) >= 388
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 389
  )
  AND (
    MIN(lineitem.l_quantity + 391) >= 392
  )
  AND (
    AVG(lineitem.l_quantity + 393) >= 394
  )
  AND (
    MAX(lineitem.l_quantity + 395) >= 396
  )
  AND (
    SUM(lineitem.l_quantity + 397) >= 398
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 399
  )
  AND (
    MIN(lineitem.l_quantity + 401) >= 402
  )
  AND (
    AVG(lineitem.l_quantity + 403) >= 404
  )
  AND (
    MAX(lineitem.l_quantity + 405) >= 406
  )
  AND (
    SUM(lineitem.l_quantity + 407) >= 408
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 409
  )
  AND (
    MIN(lineitem.l_quantity + 411) >= 412
  )
  AND (
    AVG(lineitem.l_quantity + 413) >= 414
  )
  AND (
    MAX(lineitem.l_quantity + 415) >= 416
  )
  AND (
    SUM(lineitem.l_quantity + 417) >= 418
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 419
  )
  AND (
    MIN(lineitem.l_quantity + 421) >= 422
  )
  AND (
    AVG(lineitem.l_quantity + 423) >= 424
  )
  AND (
    MAX(lineitem.l_quantity + 425) >= 426
  )
  AND (
    SUM(lineitem.l_quantity + 427) >= 428
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 429
  )
  AND (
    MIN(lineitem.l_quantity + 431) >= 432
  )
  AND (
    AVG(lineitem.l_quantity + 433) >= 434
  )
  AND (
    MAX(lineitem.l_quantity + 435) >= 436
  )
  AND (
    SUM(lineitem.l_quantity + 437) >= 438
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 439
  )
  AND (
    MIN(lineitem.l_quantity + 441) >= 442
  )
  AND (
    AVG(lineitem.l_quantity + 443) >= 444
  )
  AND (
    MAX(lineitem.l_quantity + 445) >= 446
  )
  AND (
    SUM(lineitem.l_quantity + 447) >= 448
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 449
  )
  AND (
    MIN(lineitem.l_quantity + 451) >= 452
  )
  AND (
    AVG(lineitem.l_quantity + 453) >= 454
  )
  AND (
    MAX(lineitem.l_quantity + 455) >= 456
  )
  AND (
    SUM(lineitem.l_quantity + 457) >= 458
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 459
  )
  AND (
    MIN(lineitem.l_quantity + 461) >= 462
  )
  AND (
    AVG(lineitem.l_quantity + 463) >= 464
  )
  AND (
    MAX(lineitem.l_quantity + 465) >= 466
  )
  AND (
    SUM(lineitem.l_quantity + 467) >= 468
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 469
  )
  AND (
    MIN(lineitem.l_quantity + 471) >= 472
  )
  AND (
    AVG(lineitem.l_quantity + 473) >= 474
  )
  AND (
    MAX(lineitem.l_quantity + 475) >= 476
  )
  AND (
    SUM(lineitem.l_quantity + 477) >= 478
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 479
  )
  AND (
    MIN(lineitem.l_quantity + 481) >= 482
  )
  AND (
    AVG(lineitem.l_quantity + 483) >= 484
  )
  AND (
    MAX(lineitem.l_quantity + 485) >= 486
  )
  AND (
    SUM(lineitem.l_quantity + 487) >= 488
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 489
  )
  AND (
    MIN(lineitem.l_quantity + 491) >= 492
  )
  AND (
    AVG(lineitem.l_quantity + 493) >= 494
  )
  AND (
    MAX(lineitem.l_quantity + 495) >= 496
  )
  AND (
    SUM(lineitem.l_quantity + 497) >= 498
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 499
  )
  AND (
    MIN(lineitem.l_quantity + 501) >= 502
  )
  AND (
    AVG(lineitem.l_quantity + 503) >= 504
  )
  AND (
    MAX(lineitem.l_quantity + 505) >= 506
  )
  AND (
    SUM(lineitem.l_quantity + 507) >= 508
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 509
  )
  AND (
    MIN(lineitem.l_quantity + 511) >= 512
  )
  AND (
    AVG(lineitem.l_quantity + 513) >= 514
  )
  AND (
    MAX(lineitem.l_quantity + 515) >= 516
  )
  AND (
    SUM(lineitem.l_quantity + 517) >= 518
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 519
  )
  AND (
    MIN(lineitem.l_quantity + 521) >= 522
  )
  AND (
    AVG(lineitem.l_quantity + 523) >= 524
  )
  AND (
    MAX(lineitem.l_quantity + 525) >= 526
  )
  AND (
    SUM(lineitem.l_quantity + 527) >= 528
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 529
  )
  AND (
    MIN(lineitem.l_quantity + 531) >= 532
  )
  AND (
    AVG(lineitem.l_quantity + 533) >= 534
  )
  AND (
    MAX(lineitem.l_quantity + 535) >= 536
  )
  AND (
    SUM(lineitem.l_quantity + 537) >= 538
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 539
  )
  AND (
    MIN(lineitem.l_quantity + 541) >= 542
  )
  AND (
    AVG(lineitem.l_quantity + 543) >= 544
  )
  AND (
    MAX(lineitem.l_quantity + 545) >= 546
  )
  AND (
    SUM(lineitem.l_quantity + 547) >= 548
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 549
  )
  AND (
    MIN(lineitem.l_quantity + 551) >= 552
  )
  AND (
    AVG(lineitem.l_quantity + 553) >= 554
  )
  AND (
    MAX(lineitem.l_quantity + 555) >= 556
  )
  AND (
    SUM(lineitem.l_quantity + 557) >= 558
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 559
  )
  AND (
    MIN(lineitem.l_quantity + 561) >= 562
  )
  AND (
    AVG(lineitem.l_quantity + 563) >= 564
  )
  AND (
    MAX(lineitem.l_quantity + 565) >= 566
  )
  AND (
    SUM(lineitem.l_quantity + 567) >= 568
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 569
  )
  AND (
    MIN(lineitem.l_quantity + 571) >= 572
  )
  AND (
    AVG(lineitem.l_quantity + 573) >= 574
  )
  AND (
    MAX(lineitem.l_quantity + 575) >= 576
  )
  AND (
    SUM(lineitem.l_quantity + 577) >= 578
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 579
  )
  AND (
    MIN(lineitem.l_quantity + 581) >= 582
  )
  AND (
    AVG(lineitem.l_quantity + 583) >= 584
  )
  AND (
    MAX(lineitem.l_quantity + 585) >= 586
  )
  AND (
    SUM(lineitem.l_quantity + 587) >= 588
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 589
  )
  AND (
    MIN(lineitem.l_quantity + 591) >= 592
  )
  AND (
    AVG(lineitem.l_quantity + 593) >= 594
  )
  AND (
    MAX(lineitem.l_quantity + 595) >= 596
  )
  AND (
    SUM(lineitem.l_quantity + 597) >= 598
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 599
  )
  AND (
    MIN(lineitem.l_quantity + 601) >= 602
  )
  AND (
    AVG(lineitem.l_quantity + 603) >= 604
  )
  AND (
    MAX(lineitem.l_quantity + 605) >= 606
  )
  AND (
    SUM(lineitem.l_quantity + 607) >= 608
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 609
  )
  AND (
    MIN(lineitem.l_quantity + 611) >= 612
  )
  AND (
    AVG(lineitem.l_quantity + 613) >= 614
  )
  AND (
    MAX(lineitem.l_quantity + 615) >= 616
  )
  AND (
    SUM(lineitem.l_quantity + 617) >= 618
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 619
  )
  AND (
    MIN(lineitem.l_quantity + 621) >= 622
  )
  AND (
    AVG(lineitem.l_quantity + 623) >= 624
  )
  AND (
    MAX(lineitem.l_quantity + 625) >= 626
  )
  AND (
    SUM(lineitem.l_quantity + 627) >= 628
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 629
  )
  AND (
    MIN(lineitem.l_quantity + 631) >= 632
  )
  AND (
    AVG(lineitem.l_quantity + 633) >= 634
  )
  AND (
    MAX(lineitem.l_quantity + 635) >= 636
  )
  AND (
    SUM(lineitem.l_quantity + 637) >= 638
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 639
  )
  AND (
    MIN(lineitem.l_quantity + 641) >= 642
  )
  AND (
    AVG(lineitem.l_quantity + 643) >= 644
  )
  AND (
    MAX(lineitem.l_quantity + 645) >= 646
  )
  AND (
    SUM(lineitem.l_quantity + 647) >= 648
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 649
  )
  AND (
    MIN(lineitem.l_quantity + 651) >= 652
  )
  AND (
    AVG(lineitem.l_quantity + 653) >= 654
  )
  AND (
    MAX(lineitem.l_quantity + 655) >= 656
  )
  AND (
    SUM(lineitem.l_quantity + 657) >= 658
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 659
  )
  AND (
    MIN(lineitem.l_quantity + 661) >= 662
  )
  AND (
    AVG(lineitem.l_quantity + 663) >= 664
  )
  AND (
    MAX(lineitem.l_quantity + 665) >= 666
  )
  AND (
    SUM(lineitem.l_quantity + 667) >= 668
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 669
  )
  AND (
    MIN(lineitem.l_quantity + 671) >= 672
  )
  AND (
    AVG(lineitem.l_quantity + 673) >= 674
  )
  AND (
    MAX(lineitem.l_quantity + 675) >= 676
  )
  AND (
    SUM(lineitem.l_quantity + 677) >= 678
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 679
  )
  AND (
    MIN(lineitem.l_quantity + 681) >= 682
  )
  AND (
    AVG(lineitem.l_quantity + 683) >= 684
  )
  AND (
    MAX(lineitem.l_quantity + 685) >= 686
  )
  AND (
    SUM(lineitem.l_quantity + 687) >= 688
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 689
  )
  AND (
    MIN(lineitem.l_quantity + 691) >= 692
  )
  AND (
    AVG(lineitem.l_quantity + 693) >= 694
  )
  AND (
    MAX(lineitem.l_quantity + 695) >= 696
  )
  AND (
    SUM(lineitem.l_quantity + 697) >= 698
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 699
  )
  AND (
    MIN(lineitem.l_quantity + 701) >= 702
  )
  AND (
    AVG(lineitem.l_quantity + 703) >= 704
  )
  AND (
    MAX(lineitem.l_quantity + 705) >= 706
  )
  AND (
    SUM(lineitem.l_quantity + 707) >= 708
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 709
  )
  AND (
    MIN(lineitem.l_quantity + 711) >= 712
  )
  AND (
    AVG(lineitem.l_quantity + 713) >= 714
  )
  AND (
    MAX(lineitem.l_quantity + 715) >= 716
  )
  AND (
    SUM(lineitem.l_quantity + 717) >= 718
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 719
  )
  AND (
    MIN(lineitem.l_quantity + 721) >= 722
  )
  AND (
    AVG(lineitem.l_quantity + 723) >= 724
  )
  AND (
    MAX(lineitem.l_quantity + 725) >= 726
  )
  AND (
    SUM(lineitem.l_quantity + 727) >= 728
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 729
  )
  AND (
    MIN(lineitem.l_quantity + 731) >= 732
  )
  AND (
    AVG(lineitem.l_quantity + 733) >= 734
  )
  AND (
    MAX(lineitem.l_quantity + 735) >= 736
  )
  AND (
    SUM(lineitem.l_quantity + 737) >= 738
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 739
  )
  AND (
    MIN(lineitem.l_quantity + 741) >= 742
  )
  AND (
    AVG(lineitem.l_quantity + 743) >= 744
  )
  AND (
    MAX(lineitem.l_quantity + 745) >= 746
  )
  AND (
    SUM(lineitem.l_quantity + 747) >= 748
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 749
  )
  AND (
    MIN(lineitem.l_quantity + 751) >= 752
  )
  AND (
    AVG(lineitem.l_quantity + 753) >= 754
  )
  AND (
    MAX(lineitem.l_quantity + 755) >= 756
  )
  AND (
    SUM(lineitem.l_quantity + 757) >= 758
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 759
  )
  AND (
    MIN(lineitem.l_quantity + 761) >= 762
  )
  AND (
    AVG(lineitem.l_quantity + 763) >= 764
  )
  AND (
    MAX(lineitem.l_quantity + 765) >= 766
  )
  AND (
    SUM(lineitem.l_quantity + 767) >= 768
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 769
  )
  AND (
    MIN(lineitem.l_quantity + 771) >= 772
  )
  AND (
    AVG(lineitem.l_quantity + 773) >= 774
  )
  AND (
    MAX(lineitem.l_quantity + 775) >= 776
  )
  AND (
    SUM(lineitem.l_quantity + 777) >= 778
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 779
  )
  AND (
    MIN(lineitem.l_quantity + 781) >= 782
  )
  AND (
    AVG(lineitem.l_quantity + 783) >= 784
  )
  AND (
    MAX(lineitem.l_quantity + 785) >= 786
  )
  AND (
    SUM(lineitem.l_quantity + 787) >= 788
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 789
  )
  AND (
    MIN(lineitem.l_quantity + 791) >= 792
  )
  AND (
    AVG(lineitem.l_quantity + 793) >= 794
  )
  AND (
    MAX(lineitem.l_quantity + 795) >= 796
  )
  AND (
    SUM(lineitem.l_quantity + 797) >= 798
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 799
  )
  AND (
    MIN(lineitem.l_quantity + 801) >= 802
  )
  AND (
    AVG(lineitem.l_quantity + 803) >= 804
  )
  AND (
    MAX(lineitem.l_quantity + 805) >= 806
  )
  AND (
    SUM(lineitem.l_quantity + 807) >= 808
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 809
  )
  AND (
    MIN(lineitem.l_quantity + 811) >= 812
  )
  AND (
    AVG(lineitem.l_quantity + 813) >= 814
  )
  AND (
    MAX(lineitem.l_quantity + 815) >= 816
  )
  AND (
    SUM(lineitem.l_quantity + 817) >= 818
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 819
  )
  AND (
    MIN(lineitem.l_quantity + 821) >= 822
  )
  AND (
    AVG(lineitem.l_quantity + 823) >= 824
  )
  AND (
    MAX(lineitem.l_quantity + 825) >= 826
  )
  AND (
    SUM(lineitem.l_quantity + 827) >= 828
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 829
  )
  AND (
    MIN(lineitem.l_quantity + 831) >= 832
  )
  AND (
    AVG(lineitem.l_quantity + 833) >= 834
  )
  AND (
    MAX(lineitem.l_quantity + 835) >= 836
  )
  AND (
    SUM(lineitem.l_quantity + 837) >= 838
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 839
  )
  AND (
    MIN(lineitem.l_quantity + 841) >= 842
  )
  AND (
    AVG(lineitem.l_quantity + 843) >= 844
  )
  AND (
    MAX(lineitem.l_quantity + 845) >= 846
  )
  AND (
    SUM(lineitem.l_quantity + 847) >= 848
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 849
  )
  AND (
    MIN(lineitem.l_quantity + 851) >= 852
  )
  AND (
    AVG(lineitem.l_quantity + 853) >= 854
  )
  AND (
    MAX(lineitem.l_quantity + 855) >= 856
  )
  AND (
    SUM(lineitem.l_quantity + 857) >= 858
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 859
  )
  AND (
    MIN(lineitem.l_quantity + 861) >= 862
  )
  AND (
    AVG(lineitem.l_quantity + 863) >= 864
  )
  AND (
    MAX(lineitem.l_quantity + 865) >= 866
  )
  AND (
    SUM(lineitem.l_quantity + 867) >= 868
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 869
  )
  AND (
    MIN(lineitem.l_quantity + 871) >= 872
  )
  AND (
    AVG(lineitem.l_quantity + 873) >= 874
  )
  AND (
    MAX(lineitem.l_quantity + 875) >= 876
  )
  AND (
    SUM(lineitem.l_quantity + 877) >= 878
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 879
  )
  AND (
    MIN(lineitem.l_quantity + 881) >= 882
  )
  AND (
    AVG(lineitem.l_quantity + 883) >= 884
  )
  AND (
    MAX(lineitem.l_quantity + 885) >= 886
  )
  AND (
    SUM(lineitem.l_quantity + 887) >= 888
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 889
  )
  AND (
    MIN(lineitem.l_quantity + 891) >= 892
  )
  AND (
    AVG(lineitem.l_quantity + 893) >= 894
  )
  AND (
    MAX(lineitem.l_quantity + 895) >= 896
  )
  AND (
    SUM(lineitem.l_quantity + 897) >= 898
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 899
  )
  AND (
    MIN(lineitem.l_quantity + 901) >= 902
  )
  AND (
    AVG(lineitem.l_quantity + 903) >= 904
  )
  AND (
    MAX(lineitem.l_quantity + 905) >= 906
  )
  AND (
    SUM(lineitem.l_quantity + 907) >= 908
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 909
  )
  AND (
    MIN(lineitem.l_quantity + 911) >= 912
  )
  AND (
    AVG(lineitem.l_quantity + 913) >= 914
  )
  AND (
    MAX(lineitem.l_quantity + 915) >= 916
  )
  AND (
    SUM(lineitem.l_quantity + 917) >= 918
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 919
  )
  AND (
    MIN(lineitem.l_quantity + 921) >= 922
  )
  AND (
    AVG(lineitem.l_quantity + 923) >= 924
  )
  AND (
    MAX(lineitem.l_quantity + 925) >= 926
  )
  AND (
    SUM(lineitem.l_quantity + 927) >= 928
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 929
  )
  AND (
    MIN(lineitem.l_quantity + 931) >= 932
  )
  AND (
    AVG(lineitem.l_quantity + 933) >= 934
  )
  AND (
    MAX(lineitem.l_quantity + 935) >= 936
  )
  AND (
    SUM(lineitem.l_quantity + 937) >= 938
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 939
  )
  AND (
    MIN(lineitem.l_quantity + 941) >= 942
  )
  AND (
    AVG(lineitem.l_quantity + 943) >= 944
  )
  AND (
    MAX(lineitem.l_quantity + 945) >= 946
  )
  AND (
    SUM(lineitem.l_quantity + 947) >= 948
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 949
  )
  AND (
    MIN(lineitem.l_quantity + 951) >= 952
  )
  AND (
    AVG(lineitem.l_quantity + 953) >= 954
  )
  AND (
    MAX(lineitem.l_quantity + 955) >= 956
  )
  AND (
    SUM(lineitem.l_quantity + 957) >= 958
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 959
  )
  AND (
    MIN(lineitem.l_quantity + 961) >= 962
  )
  AND (
    AVG(lineitem.l_quantity + 963) >= 964
  )
  AND (
    MAX(lineitem.l_quantity + 965) >= 966
  )
  AND (
    SUM(lineitem.l_quantity + 967) >= 968
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 969
  )
  AND (
    MIN(lineitem.l_quantity + 971) >= 972
  )
  AND (
    AVG(lineitem.l_quantity + 973) >= 974
  )
  AND (
    MAX(lineitem.l_quantity + 975) >= 976
  )
  AND (
    SUM(lineitem.l_quantity + 977) >= 978
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 979
  )
  AND (
    MIN(lineitem.l_quantity + 981) >= 982
  )
  AND (
    AVG(lineitem.l_quantity + 983) >= 984
  )
  AND (
    MAX(lineitem.l_quantity + 985) >= 986
  )
  AND (
    SUM(lineitem.l_quantity + 987) >= 988
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 989
  )
  AND (
    MIN(lineitem.l_quantity + 991) >= 992
  )
  AND (
    AVG(lineitem.l_quantity + 993) >= 994
  )
  AND (
    MAX(lineitem.l_quantity + 995) >= 996
  )
  AND (
    SUM(lineitem.l_quantity + 997) >= 998
  )
  AND (
    COUNT(lineitem.l_quantity) >= 1 + 0 * 999
  )
ORDER BY
  l_returnflag,
  l_linestatus""",
}

EXPECTED_SQL_Q03 = {
    1: r"""WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    MAX(l_quantity + 0) AS dfc
  FROM customer, orders, lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
  ORDER BY
    revenue DESC,
    o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1""",
    10: r"""WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    MAX(l_quantity + 0) AS dfc
  FROM customer, orders, lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
  ORDER BY
    revenue DESC,
    o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1""",
    100: r"""WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    MAX(l_quantity + 0) AS dfc
  FROM customer, orders, lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
  ORDER BY
    revenue DESC,
    o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1""",
    1000: r"""WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    MAX(l_quantity + 0) AS dfc
  FROM customer, orders, lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
  ORDER BY
    revenue DESC,
    o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1""",
}


@pytest.mark.parametrize("policy_count", POLICY_COUNTS)
def test_tpch_q01_policy_counts_rewrite_sql(tpch_rewriter, policy_count):
    """Ensure policy count scaling is reflected in rewrite SQL for TPC-H Q01."""
    query = load_tpch_query(1)

    _clear_policies(tpch_rewriter)
    for policy in build_policies(policy_count):
        tpch_rewriter.register_policy(policy)

    transformed = tpch_rewriter.transform_query(query)
    expected = EXPECTED_SQL_Q01[policy_count]

    assert transformed == expected, (
        "Transformed SQL does not match expected.\n"
        f"Policy count: {policy_count}\n"
        f"Expected SQL:\n{expected}\n\n"
        f"Actual SQL:\n{transformed}"
    )


@pytest.mark.parametrize("policy_count", POLICY_COUNTS)
def test_tpch_q03_policy_counts_rewrite_sql(tpch_rewriter, policy_count):
    """Ensure policy count scaling is reflected in rewrite SQL for TPC-H Q03."""
    query = load_tpch_query(3)

    _clear_policies(tpch_rewriter)
    for policy in build_policies(policy_count):
        tpch_rewriter.register_policy(policy)

    transformed = tpch_rewriter.transform_query(query)
    expected = EXPECTED_SQL_Q03[policy_count]

    assert transformed == expected, (
        "Transformed SQL does not match expected.\n"
        f"Policy count: {policy_count}\n"
        f"Expected SQL:\n{expected}\n\n"
        f"Actual SQL:\n{transformed}"
    )

