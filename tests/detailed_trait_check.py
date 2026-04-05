#!/usr/bin/env python3
"""
Detailed extraction of trait definitions we're not sure about.
"""
import re

with open('tests/js_dump/sdk-421f7ba4a82b319d562a.js', encoding='utf-8', errors='replace') as f:
    content = f.read()

def extract_deserializer(trait_name):
    """Extract and format the deserializer for a trait."""
    pattern = re.compile(
        re.escape(trait_name) + r'\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{',
        re.DOTALL
    )
    m = pattern.search(content)
    if m:
        start = m.end()
        depth = 1
        i = start
        while i < len(content) and depth > 0:
            if content[i] == '{': depth += 1
            elif content[i] == '}': depth -= 1
            i += 1
        return content[m.start():i]
    return None

print("=" * 80)
print("DETAILED TRAIT ANALYSIS")
print("=" * 80)
print()

# TemperatureTrait
print("TemperatureTrait.deserializeBinaryFromReader:")
print("─" * 80)
deserializer = extract_deserializer('TemperatureTrait')
if deserializer:
    # Extract just the meaningful parts
    matches = re.findall(r'case\s+(\d+):(.{0,150})', deserializer)
    for field_num, code_snippet in matches:
        code_short = code_snippet[:100].replace('\n', ' ')
        print(f"  field {field_num}: {code_short}...")
else:
    print("  NOT FOUND")
print()

# SmokeTrait
print("SmokeTrait.deserializeBinaryFromReader:")
print("─" * 80)
deserializer = extract_deserializer('SmokeTrait')
if deserializer:
    matches = re.findall(r'case\s+(\d+):(.{0,150})', deserializer)
    for field_num, code_snippet in matches:
        code_short = code_snippet[:100].replace('\n', ' ')
        print(f"  field {field_num}: {code_short}...")
else:
    print("  NOT FOUND")
print()

# CarbonMonoxideTrait
print("CarbonMonoxideTrait.deserializeBinaryFromReader:")
print("─" * 80)
deserializer = extract_deserializer('CarbonMonoxideTrait')
if deserializer:
    matches = re.findall(r'case\s+(\d+):(.{0,150})', deserializer)
    for field_num, code_snippet in matches:
        code_short = code_snippet[:100].replace('\n', ' ')
        print(f"  field {field_num}: {code_short}...")
else:
    print("  NOT FOUND")
print()

# HumidityTrait
print("HumidityTrait.deserializeBinaryFromReader:")
print("─" * 80)
deserializer = extract_deserializer('HumidityTrait')
if deserializer:
    matches = re.findall(r'case\s+(\d+):(.{0,150})', deserializer)
    for field_num, code_snippet in matches:
        code_short = code_snippet[:100].replace('\n', ' ')
        print(f"  field {field_num}: {code_short}...")
else:
    print("  NOT FOUND")
print()

# Now check what our decoder actually does
print("=" * 80)
print("OUR DECODER LOGIC")
print("=" * 80)
print()

with open('src/grpc_web.py', encoding='utf-8') as f:
    code = f.read()

for fn_search in ['_read_temperature_trait', '_read_humidity_trait', '_read_smoke_trait', '_read_carbon_monoxide_trait']:
    pattern = re.compile(f'{fn_search}.*?(?=\ndef |$)', re.DOTALL)
    m = pattern.search(code)
    if m:
        func_text = m.group()
        # Count what fields it checks
        field_checks = re.findall(r'[fo]f\s*==\s*(\d+)', func_text)
        print(f"{fn_search}:")
        if field_checks:
            print(f"  Checks fields: {sorted(set(field_checks))}")
        else:
            print(f"  No explicit field checks found")
        if 'for' in func_text and 'decode_proto_fields' in func_text:
            print(f"  Uses decode_proto_fields iteration")
