
import sys
import os
sys.path.append(os.getcwd())

def parse_currency(val):
    if not val:
        return 0.0
    val = str(val).replace('$', '').replace(',', '').replace('(', '-').replace(')', '').strip()
    if not val:
        return 0.0
    try:
        return float(val)
    except:
        return 0.0

def debug():
    path = "data/imports/manual_history.txt"
    file_sum = 0.0
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        
    print("Debugging first 20 lines:")
    for i, line in enumerate(lines[:20]):
        if i < 2: 
            print(f"Line {i}: {line.strip()}")
            continue
            
        parts = line.split('\t')
        if len(parts) < 10: continue
        
        val = 0.0
        msg = f"Line {i}: Len={len(parts)}"
        
        # Try finding Profit col
        # Let's print parts around 19, 20, 21
        portion = []
        for idx in range(18, min(len(parts), 24)):
            portion.append(f"[{idx}]='{parts[idx]}'")
        
        msg += " | " + " ".join(portion)
        
        if len(parts) > 20:
             val = parse_currency(parts[20])
             msg += f" | Parsed(20)={val}"
        
        print(msg)
        file_sum += val
        
    print(f"\nTotal Sum so far: {file_sum}")

if __name__ == "__main__":
    debug()
