from pathlib import Path

path = Path('src/main.rs')
text = path.read_text()
old = '127.0.0.1:8765'
count = text.count(old)
if count < 1:
    raise SystemExit('expected at least one canonical 8765 reference in src/main.rs')
text = text.replace(old, '127.0.0.1:8766')
path.write_text(text)
print(f'replaced {count} daemon port reference(s)')
