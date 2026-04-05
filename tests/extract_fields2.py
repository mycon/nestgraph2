"""
Extract HvacControlTrait and FanControlTrait field definitions from
the compiled Nest web app bundle.

Two strategies:
1. Path-map registry (p()(d, A.KEY, {path:..., trait:...}))
2. Raw string search for all hvacState / fanState sub-field names
3. Protobuf deserializer switch/case blocks
"""
import re
from collections import defaultdict

content = open('tests/main-36606966c8417ddf91d0.js', encoding='utf-8', errors='replace').read()
print(f"Bundle size: {len(content):,} chars\n")

# ── Strategy 1: path registry ──────────────────────────────────────────────
# Pattern: p()(d,A.FIELD_KEY,{path:...,trait:..._e.trait.hvac.XxxTrait...})
# or variations with spaces and different quote styles

found = []
# Find all occurrences after A. up to matching trait
for m in re.finditer(r'A\.(\w+),\{path:(.*?)trait:(?:\w+)\.trait\.hvac\.(HvacControlTrait|FanControlTrait)', content, re.DOTALL):
    key = m.group(1)
    path_raw = m.group(2).strip().rstrip(',')
    trait = m.group(3)
    found.append((trait, key, path_raw))

by_trait = defaultdict(list)
for trait, key, path in found:
    by_trait[trait].append((key, path))

for trait in ['HvacControlTrait', 'FanControlTrait']:
    entries = by_trait.get(trait, [])
    print(f"=== {trait} ({len(entries)} entries) ===")
    for key, path in sorted(entries):
        print(f"  {key:<50} {path}")
    print()

# ── Strategy 2: hvacState sub-fields ──────────────────────────────────────
print("=== hvacState sub-field names ===")
sub_fields = set()
for m in re.finditer(r'"hvacState"\s*,\s*"(\w+)"', content):
    sub_fields.add(m.group(1))
for m in re.finditer(r"'hvacState'\s*,\s*'(\w+)'", content):
    sub_fields.add(m.group(1))
for f in sorted(sub_fields):
    print(f"  {f}")
print()

# ── Strategy 3: FanControlTrait currentSpeed context ──────────────────────
print("=== FanControlTrait context ===")
for m in re.finditer(r'currentSpeed', content):
    idx = m.start()
    print(repr(content[max(0, idx-200):idx+300]))
    print()

# ── Strategy 4: look for protobuf reader switch/case blocks ────────────────
print("=== Proto deserializer blocks (getFieldNumber / case) ===")
# google-protobuf compiled: reader.getFieldNumber()
for m in re.finditer(r'getFieldNumber\(\)', content):
    idx = m.start()
    block = content[max(0, idx-200):idx+600]
    if 'hvac' in block.lower() or 'fan' in block.lower():
        print(f"--- char {idx} ---")
        print(block)
        print()
