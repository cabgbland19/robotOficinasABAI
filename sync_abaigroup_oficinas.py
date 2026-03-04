import os
import re
import time
import logging
from dataclasses import dataclass
from pathlib import Path
from collections import Counter

import requests
from bs4 import BeautifulSoup
import pandas as pd


SOURCE_URL = "https://www.abaigroup.com/donde-encontrarnos/"


def setup_logging() -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("oficinas_sync")
    logger.setLevel(log_level)
    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(log_dir / "oficinas_sync.log", encoding="utf-8")
    fh.setLevel(log_level)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False
    return logger


log = setup_logging()


@dataclass(frozen=True)
class Oficina:
    pais: str
    ciudad: str
    oficina: str


# -----------------------
# Helpers
# -----------------------
EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+", re.IGNORECASE)

FOOTER_STOP_RE = re.compile(
    r"(©\s*\d{4})|"
    r"(uso legal)|"
    r"(pol[ií]tica de privacidad)|"
    r"(pol[ií]tica de cookies)|"
    r"(gestionar el consentimiento)|"
    r"(almacenamiento o acceso t[eé]cnico)|"
    r"(cookiedatabase\.org)|"
    r"(siempre activo)|"
    r"(preferencias)|"
    r"(estad[ií]sticas)|"
    r"(marketing)|"
    r"(canal seguridad de la informaci[oó]n)",
    re.IGNORECASE,
)

STOP_INLINE = {"close", "close menu", "oficinas", "sede central"}

def _norm(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _strip_md_hashes(s: str) -> str:
    # por si vienen "### Oficinas en Colombia" o "#### Bogotá"
    return re.sub(r"^\s*#+\s*", "", s).strip()

def _is_country_header(line: str) -> bool:
    clean = _strip_md_hashes(line)
    return clean.lower().startswith("oficinas en ")

def _extract_country(line: str) -> str:
    clean = _strip_md_hashes(line)
    return _norm(clean[len("oficinas en "):])

def _is_stop_line(line: str) -> bool:
    low = _strip_md_hashes(line).lower()
    return (
        low in STOP_INLINE
        or low.startswith("otras ciudades")
        or low.startswith("tlf:")
        or low.startswith("tel:")
    )

def _looks_like_city(line: str) -> bool:
    clean = _strip_md_hashes(line)
    low = clean.lower()

    # nunca tratar header de país como ciudad
    if low.startswith("oficinas en "):
        return False

    # cortar/evitar footer
    if FOOTER_STOP_RE.search(clean):
        return False

    # evitar cosas tipo CONTACTO/LINKS/etc
    if low in {"contacto", "links", "talento"}:
        return False

    # ciudad suele ser corta y sin números
    if any(ch.isdigit() for ch in clean):
        return False

    if EMAIL_RE.search(clean):
        return False

    # evitar párrafos largos
    if len(clean) > 60:
        return False

    return True


def scrape_abaigroup_oficinas() -> list[Oficina]:
    """
    Scrape basado en texto plano + heurísticas, pero:
      - corta al llegar footer/cookies
    """
    log.info("GET %s", SOURCE_URL)
    r = requests.get(
        SOURCE_URL,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; oficinas-scraper/1.0)"},
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    lines = [_norm(x) for x in soup.get_text("\n").splitlines()]
    lines = [x for x in lines if x]

    oficinas: list[Oficina] = []
    pais_actual: str | None = None
    ciudad_actual: str | None = None
    buffer_dir: list[str] = []

    in_offices_section = False  
    seen_any_office = False

    def flush_address():
        nonlocal buffer_dir, seen_any_office
        if pais_actual and ciudad_actual and buffer_dir:
            addr = _norm(" ".join(buffer_dir))
            if addr and not EMAIL_RE.search(addr) and not addr.lower().startswith(("tlf:", "tel:")):
                oficinas.append(Oficina(pais_actual, ciudad_actual, addr))
                seen_any_office = True
        buffer_dir = []

    for raw in lines:
        clean = _strip_md_hashes(raw)
        low = clean.lower()

        # 1) detectar país
        if _is_country_header(clean):
            in_offices_section = True
            flush_address()
            pais_actual = _extract_country(clean)
            ciudad_actual = None
            log.info("Detectado país: %s", pais_actual)
            continue

        # 2) antes de entrar a oficinas, ignorar todo (para no cortar por 'Contacto' del bloque superior)
        if not in_offices_section:
            continue

        # 3) cortar al llegar footer/cookies (solo ya estando en oficinas)
        if FOOTER_STOP_RE.search(clean) or low in {"contacto", "links", "talento"}:
            flush_address()
            log.info("Corte por footer/cookies: '%s'", clean)
            break

        # 4) stop-lines internas
        if _is_stop_line(clean):
            flush_address()
            ciudad_actual = None
            # no reseteamos pais_actual, porque entre modales puede aparecer "Close"
            continue

        # 5) saltar emails / telefonos sueltos
        if EMAIL_RE.search(clean) or low.startswith(("tlf:", "tel:", "+")):
            continue

        # 6) detectar ciudad
        if _looks_like_city(clean):
            flush_address()
            ciudad_actual = clean
            continue

        # 7) dirección
        if ciudad_actual:
            bullet = clean.lstrip()
            if bullet.startswith(("*", "•", "-")):
                # si hay bullets, tratarlos como parte de dirección
                buffer_dir.append(bullet.lstrip("*•- ").strip())
            else:
                buffer_dir.append(clean)

    flush_address()

    seen = set()
    out: list[Oficina] = []
    for o in oficinas:
        key = (o.pais.lower(), o.ciudad.lower(), o.oficina.lower())
        if key not in seen:
            seen.add(key)
            out.append(o)

    log.info("Scrape OK. Total oficinas: %s", len(out))
    counts = Counter([o.pais for o in out])
    log.info("Resumen por país: %s", dict(counts))
    return out


# -----------------------
# API
# -----------------------
def api_login(base_url: str, username: str, password: str) -> str:
    url = f"{base_url.rstrip('/')}/auth/token"
    log.info("Login API: %s", url)
    r = requests.post(url, data={"username": username, "password": password}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def api_get_all(base_url: str, token: str) -> list[dict]:
    url = f"{base_url.rstrip('/')}/oficinas"
    headers = {"Authorization": f"Bearer {token}"}

    all_rows: list[dict] = []
    offset = 0
    limit = 500

    while True:
        r = requests.get(url, headers=headers, params={"limit": limit, "offset": offset}, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += limit

    log.info("GET /oficinas total: %s", len(all_rows))
    return all_rows


def api_post_oficina(base_url: str, token: str, o: Oficina) -> bool:
    """
    True si creó.
    False si ya existe (409).
    """
    url = f"{base_url.rstrip('/')}/oficinas"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"pais": o.pais, "ciudad": o.ciudad, "oficina": o.oficina}

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        return False

    raise RuntimeError(f"POST /oficinas failed {r.status_code}: {r.text}")


def main():
    base_url = os.getenv("API_BASE_URL")  # https://tu-app.onrender.com
    username = os.getenv("API_USERNAME")
    password = os.getenv("API_PASSWORD")
    out_xlsx = os.getenv("OUT_XLSX", "oficinas.xlsx")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    t0 = time.time()

    scraped = scrape_abaigroup_oficinas()
    log.info("Oficinas extraídas: %s", len(scraped))

    # Si no tienes API_* igual exporta el scrape a Excel
    if not base_url or not username or not password:
        log.warning("Faltan API_BASE_URL / API_USERNAME / API_PASSWORD. Exportaré solo el scrape a Excel.")
        df = pd.DataFrame([o.__dict__ for o in scraped])[["pais", "ciudad", "oficina"]]
        df.to_excel(out_xlsx, index=False)
        log.info("Excel generado (solo scrape): %s (rows=%s)", out_xlsx, len(df))
        return

    token = api_login(base_url, username, password)

    #contra lo que ya existe (por si la API no tiene unique constraint)
    existing = api_get_all(base_url, token)
    existing_keys = {(x["pais"].lower(), x["ciudad"].lower(), x["oficina"].lower()) for x in existing}

    created = 0
    skipped = 0

    log.info("Insertando oficinas en API... (DRY_RUN=%s)", dry_run)
    for idx, o in enumerate(scraped, start=1):
        key = (o.pais.lower(), o.ciudad.lower(), o.oficina.lower())
        if key in existing_keys:
            skipped += 1
            log.debug("SKIP exists [%s/%s] %s | %s | %s", idx, len(scraped), o.pais, o.ciudad, o.oficina)
            continue

        if dry_run:
            created += 1
            existing_keys.add(key)
            log.debug("DRY created [%s/%s] %s | %s | %s", idx, len(scraped), o.pais, o.ciudad, o.oficina)
            continue

        ok = api_post_oficina(base_url, token, o)
        if ok:
            created += 1
            existing_keys.add(key)
            log.debug("CREATED [%s/%s] %s | %s | %s", idx, len(scraped), o.pais, o.ciudad, o.oficina)
        else:
            skipped += 1
            log.debug("SKIPPED 409 [%s/%s] %s | %s | %s", idx, len(scraped), o.pais, o.ciudad, o.oficina)

    final_rows = api_get_all(base_url, token)
    df = pd.DataFrame(final_rows)[["id", "pais", "ciudad", "oficina"]]
    df.to_excel(out_xlsx, index=False)

    log.info("Excel generado: %s (rows=%s)", out_xlsx, len(df))
    log.info("Resumen: creadas=%s, existentes=%s, tiempo=%.2fs", created, skipped, time.time() - t0)


if __name__ == "__main__":
    main()