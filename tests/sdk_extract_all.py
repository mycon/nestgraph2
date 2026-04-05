#!/usr/bin/env python3
"""
Extract structured field definitions for all actively-decoded traits from the SDK.
Outputs a machine-readable comparison with current .proto files.
"""
import re, sys

sys.stdout.reconfigure(encoding='utf-8')

SDK = 'tests/js_dump/sdk-421f7ba4a82b319d562a.js'
with open(SDK, encoding='utf-8', errors='replace') as f:
    content = f.read()

# All top-level and nested traits to extract
TRAITS = [
    # sensor
    'TemperatureTrait', 'TemperatureTrait.TemperatureSample',
    'HumidityTrait', 'HumidityTrait.HumiditySample',
    'CarbonMonoxideTrait', 'CarbonMonoxideTrait.CoSample',
    'SmokeTrait', 'SmokeTrait.SmokeSample',
    'BatteryVoltageTrait', 'BatteryVoltageTrait.BatteryVoltageSample',
    # hvac
    'HvacControlTrait', 'HvacControlTrait.HvacState',
    'FanControlTrait',
    'EcoModeStateTrait',
    'EcoModeTrait',
    'TargetTemperatureSettingsTrait',
    'SetPointScheduleSettingsTrait.TemperatureSetPoint',
]

ENUMS = [
    'EcoModeStateTrait.EcoMode',
    'EcoModeStateTrait.EcoModeChangeReason',
    'FanControlTrait.FanSpeedSetting',
    'FanControlTrait.FanSpeedState',
    'FanControlTrait.VentilationState',
    'FanControlTrait.VentilationAlert',
    'HvacControlTrait.HvacStateChangeReason',
]


def extract_deser(name):
    """Return list of (field_num, type_description) from deserializeBinaryFromReader."""
    pat = re.compile(
        re.escape(name) + r'\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{',
        re.DOTALL
    )
    m = pat.search(content)
    if not m:
        return None
    start = m.end()
    depth = 1
    i = start
    while i < len(content) and depth > 0:
        if content[i] == '{': depth += 1
        elif content[i] == '}': depth -= 1
        i += 1
    func = content[m.start():i]

    fields = []
    # Each case block
    case_pat = re.compile(r'case\s+(\d+):(.*?)(?=case\s+\d+:|default:|$)', re.DOTALL)
    for cm in case_pat.finditer(func):
        fnum = int(cm.group(1))
        body = cm.group(2)
        if 'readMessage' in body:
            # Extract message type
            mt = re.search(r'new proto\.([\w.]+)', body)
            if mt:
                short = mt.group(1).split('.')[-1]
                fields.append((fnum, f'message ({short})'))
            else:
                # Google proto type
                gt = re.search(r'new ([a-z]\.[\w.]+)', body)
                if gt:
                    fields.append((fnum, f'message ({gt.group(1)})'))
                else:
                    fields.append((fnum, 'message'))
        elif 'readEnum' in body:
            fields.append((fnum, 'enum'))
        elif 'readBool' in body:
            fields.append((fnum, 'bool'))
        elif 'readString' in body:
            fields.append((fnum, 'string'))
        elif 'readFloat' in body:
            fields.append((fnum, 'float'))
        elif 'readInt32' in body or 'readSint32' in body:
            fields.append((fnum, 'int32'))
        elif 'readInt64' in body:
            fields.append((fnum, 'int64'))
        elif 'readUint32' in body:
            fields.append((fnum, 'uint32'))
        elif 'readBytes' in body:
            fields.append((fnum, 'bytes'))
        else:
            fields.append((fnum, '(unknown)'))
    return fields


def extract_enum(name):
    """Return {value_name: int} for a JS enum object."""
    pat = re.compile(re.escape(name) + r'\s*=\s*\{([^}]+)\}')
    m = pat.search(content)
    if not m:
        return None
    body = m.group(1)
    pairs = re.findall(r'(\w+)\s*:\s*(\d+)', body)
    return {k: int(v) for k, v in pairs}


print("=" * 80)
print("SDK FIELD EXTRACTION — sdk-421f7ba4a82b319d562a.js")
print("=" * 80)
print()
for name in TRAITS:
    fields = extract_deser(f'proto.nest.trait.sensor.{name}') or \
             extract_deser(f'proto.nest.trait.hvac.{name}') or \
             extract_deser(f'proto.{name}')
    prefix = name.split('.')[0]
    if fields is None:
        print(f"  {name}: NOT FOUND in SDK")
    else:
        print(f"  {name}:")
        for fnum, ftype in sorted(fields):
            print(f"    field {fnum:2d}: {ftype}")
    print()

print()
print("=" * 80)
print("SDK ENUM VALUES")
print("=" * 80)
print()
for name in ENUMS:
    vals = extract_enum(f'proto.nest.trait.sensor.{name}') or \
           extract_enum(f'proto.nest.trait.hvac.{name}') or \
           extract_enum(f'proto.{name}')
    if vals is None:
        print(f"  {name}: NOT FOUND")
    else:
        print(f"  {name}:")
        for k, v in sorted(vals.items(), key=lambda x: x[1]):
            print(f"    {v}: {k}")
    print()
