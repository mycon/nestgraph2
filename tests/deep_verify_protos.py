#!/usr/bin/env python3
"""
Deep verification: Extract sub-message definitions to verify field mappings.
"""
import re

with open('tests/js_dump/sdk-421f7ba4a82b319d562a.js', encoding='utf-8', errors='replace') as f:
    content = f.read()

print("=" * 80)
print("DEEP PROTO VERIFICATION - Sub-message field mappings")
print("=" * 80)
print()

# HvacState (nested in HvacControlTrait)
print("HvacControlTrait.HvacState fields:")
print("─" * 80)

pattern = re.compile(
    r"HvacState\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{",
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
    func = content[m.start():i]
    
    # Extract all case statements
    cases = re.findall(r'case\s+(\d+):', func)
    print(f"  Fields with case statements: {', '.join(sorted(set(cases)))}")
    print()

# TemperatureSample (used by TemperatureTrait)
print("TemperatureTrait nested structures:")
print("─" * 80)

pattern = re.compile(
    r"TemperatureTrait\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{",
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
    func = content[m.start():i]
    
    cases = re.findall(r'case\s+(\d+):(?:.*?)(?=break|default)', func, re.DOTALL)
    for i, case_body in enumerate(cases, 1):
        header = f"  field {i}:"
        if 'readMessage' in case_body:
            # Try to extract message type name
            msg_match = re.search(r'proto\.nest\.[\w.]+', case_body)
            if msg_match:
                print(f"{header} sub-message ({msg_match.group()})")
            else:
                print(f"{header} sub-message")
        elif 'readFloat' in case_body:
            print(f"{header} float")
        else:
            print(f"{header} (other type)")
    print()

# EcoModeStateTrait to check its enum structure
print("EcoModeStateTrait fields:")
print("─" * 80)

pattern = re.compile(
    r"EcoModeStateTrait\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{",
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
    func = content[m.start():i]
    
    cases = re.findall(r'case\s+(\d+):\s*([^}]+?)(?:break|default)', func, re.DOTALL)
    for field_num, case_body in cases:
        if 'readEnum' in case_body:
            print(f"  field {field_num}: enum")
        elif 'readMessage' in case_body:
            print(f"  field {field_num}: sub-message")
        else:
            print(f"  field {field_num}: (other)")
    
    # Look for enum definitions
    enum_pattern = re.compile(r"EcoModeStateTrait\.[\w]+\s*=\s*\{[^}]+\}")
    for enum_match in enum_pattern.finditer(content):
        enum_text = enum_match.group()
        if 'eco' in enum_text.lower() or 'state' in enum_text.lower():
            print()
            print(f"  Enum: {enum_text[:100]}...")
    print()

# SmokeTrait (fields 3-6 are unusual)
print("SmokeTrait fields (unusual: starts at field 3):")
print("─" * 80)

pattern = re.compile(
    r"SmokeTrait\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{",
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
    func = content[m.start():i]
    print(f"  Deserializer (first 300 chars):")
    print(f"    {func[:300]}...")
    
    cases = re.findall(r'case\s+(\d+):.*?break', func, re.DOTALL)
    print(f"\n  Has case statements for fields: {', '.join(cases)}")
    print()

print("=" * 80)
print("COMPARISON WITH CURRENT DECODERS")
print("=" * 80)
print()

with open('src/grpc_web.py', encoding='utf-8') as f:
    grpc_code = f.read()

# Check _read_hvac_control_trait
print("_read_hvac_control_trait implementation:")
pattern = re.compile(r'def _read_hvac_control_trait\(data: bytes\)(.*?)(?=\ndef |\Z)', re.DOTALL)
m = pattern.search(grpc_code)
if m:
    impl = m.group(1)
    if '_HVAC_STATE_FIELDS' in impl:
        match = re.search(r'_HVAC_STATE_FIELDS = \{([^}]+)\}', impl)
        if match:
            fields = match.group(1)
            field_nums = re.findall(r'(\d+):', fields)
            print(f"  Checks fields: {', '.join(sorted(set(field_nums)))}")
            if '1' in field_nums and '13' in field_nums:
                print("  ✓ Covers HvacState fields 1-13")
            else:
                print("  ✗ May not cover all HvacState fields")
print()

# Check _read_temperature_trait
print("_read_temperature_trait implementation:")
pattern = re.compile(r'def _read_temperature_trait\(data: bytes\)(.*?)(?=\ndef |\Z)', re.DOTALL)
m = pattern.search(grpc_code)
if m:
    impl = m.group(1)
    if 'field 1' in impl or 'f == 1' in impl:
        print("  ✓ Reads field 1 (TemperatureSample)")
        if 'FloatValue' in impl:
            print("  ✓ Knows about FloatValue sub-message")
print()

print("=" * 80)
