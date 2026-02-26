"""
Klod Proxy — установка.
Запусти: python install.py
"""
import os
import subprocess
import sys

DIR = os.path.dirname(os.path.abspath(__file__))


def step(msg):
    print(f"\n  → {msg}")


def run(cmd, **kw):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kw)


def main():
    print()
    print("  ╭──────────────────────────╮")
    print("  │   Klod Proxy Installer   │")
    print("  ╰──────────────────────────╯")

    # 1. Зависимости
    step("Installing dependencies...")
    r = run(f'"{sys.executable}" -m pip install aiohttp rich readchar --quiet')
    if r.returncode == 0:
        print("    OK")
    else:
        print(f"    pip error: {r.stderr.strip()}")
        return

    # 2. Инициализация БД
    step("Initializing database...")
    sys.path.insert(0, DIR)
    import proxy
    proxy.init_db()
    print("    OK")

    # 3. Создание klod.bat
    step("Creating klod.bat...")
    bat_path = os.path.join(DIR, "klod.bat")
    with open(bat_path, "w", encoding="utf-8") as f:
        f.write('@echo off\n')
        f.write('chcp 65001 >nul 2>&1\n')
        f.write(f'"{sys.executable}" "{os.path.join(DIR, "proxy.py")}" %*\n')
    print(f"    {bat_path}")

    # 4. Добавление в PATH
    step("Adding to PATH...")
    r = run(f'powershell -Command "[Environment]::GetEnvironmentVariable(\'Path\', \'User\')"')
    user_path = r.stdout.strip()
    if DIR.lower() in user_path.lower():
        print("    Already in PATH")
    else:
        r = run(f'''powershell -Command "$p = [Environment]::GetEnvironmentVariable('Path', 'User'); [Environment]::SetEnvironmentVariable('Path', \\"$p;{DIR}\\", 'User')"''')
        if r.returncode == 0:
            print("    Added to user PATH")
        else:
            print(f"    Failed: {r.stderr.strip()}")
            print(f"    Manually add to PATH: {DIR}")

    # 5. Готово
    print()
    print("  ✓ Done!")
    print()
    print("  Usage:")
    print("    klod          — start proxy (after reopening terminal)")
    print("    python proxy.py  — start proxy now")
    print()
    print("  First run: go to Providers and add your API endpoint.")
    print()


if __name__ == "__main__":
    main()
