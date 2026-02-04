from pathlib import Path
import yaml


def main() -> None:
    path = Path(__file__).resolve().parents[1] / "config" / "customs.yml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    under3_100 = cfg["util_tables_under3"]["100-2000"]["hp"][:5]
    over3_100 = cfg["util_tables_3_5"]["100-2000"]["hp"][:5]
    under3_2000 = cfg["util_tables_under3"]["2000-3000"]["hp"][:5]
    over3_2000 = cfg["util_tables_3_5"]["2000-3000"]["hp"][:5]
    print("[100-2000 hp] under_3:", under3_100)
    print("[100-2000 hp] 3_5:", over3_100)
    print("[2000-3000 hp] under_3:", under3_2000)
    print("[2000-3000 hp] 3_5:", over3_2000)


if __name__ == "__main__":
    main()
