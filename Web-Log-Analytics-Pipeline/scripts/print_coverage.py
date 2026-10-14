import csv

with open('target/site/jacoco/jacoco.csv', mode='r') as f:
    reader = list(csv.DictReader(f))

pkg_stats = {}
total_line_miss = 0
total_line_cov = 0
total_branch_miss = 0
total_branch_cov = 0

for r in reader:
    pkg = r['PACKAGE']
    lm, lc = int(r['LINE_MISSED']), int(r['LINE_COVERED'])
    bm, bc = int(r['BRANCH_MISSED']), int(r['BRANCH_COVERED'])
    total_line_miss += lm
    total_line_cov += lc
    total_branch_miss += bm
    total_branch_cov += bc

    if pkg not in pkg_stats:
        pkg_stats[pkg] = {'lm': 0, 'lc': 0, 'bm': 0, 'bc': 0}
    pkg_stats[pkg]['lm'] += lm
    pkg_stats[pkg]['lc'] += lc
    pkg_stats[pkg]['bm'] += bm
    pkg_stats[pkg]['bc'] += bc

print("=== JaCoCo Coverage Breakdown ===")
for pkg in sorted(pkg_stats.keys()):
    st = pkg_stats[pkg]
    l_tot = st['lm'] + st['lc']
    l_pct = (st['lc'] / l_tot * 100.0) if l_tot > 0 else 0
    b_tot = st['bm'] + st['bc']
    b_pct = (st['bc'] / b_tot * 100.0) if b_tot > 0 else 0
    print(f"Package {pkg}:")
    print(f"  Line Coverage:   {l_pct:.2f}% ({st['lc']}/{l_tot} lines)")
    print(f"  Branch Coverage: {b_pct:.2f}% ({st['bc']}/{b_tot} branches)")

tot_lines = total_line_miss + total_line_cov
tot_pct = (total_line_cov / tot_lines * 100.0) if tot_lines > 0 else 0
b_tot = total_branch_miss + total_branch_cov
b_pct = (total_branch_cov / b_tot * 100.0) if b_tot > 0 else 0
print("---------------------------------")
print(f"Overall Line Coverage:   {tot_pct:.2f}% ({total_line_cov}/{tot_lines} lines)")
print(f"Overall Branch Coverage: {b_pct:.2f}% ({total_branch_cov}/{b_tot} branches)")
