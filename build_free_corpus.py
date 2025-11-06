import argparse, csv, hashlib, os, re, time, json
from pathlib import Path
from urllib.parse import urlparse, unquote

import fitz  # PyMuPDF
import pandas as pd
import requests
from tqdm import tqdm

# ----------------- RUTAS DEL REPO -----------------
REPO_ROOT = Path(__file__).resolve().parent
TEXT_DIR   = REPO_ROOT / "texts"
META_CSV   = REPO_ROOT / "index_texts.csv"
DOCS_DIR   = REPO_ROOT / "docs"
DOCS_JSON  = DOCS_DIR   / "docs_index.json"

# ----------------- UTILIDADES -----------------
def safe_filename(url: str) -> str:
    """Nombre de archivo seguro a partir de la URL."""
    name = unquote(Path(urlparse(url).path).name) or "documento.pdf"
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    return name

def date_parts(iso_date):
    """Devuelve (YYYY,MM,DD) o None. Acepta 'YYYY-MM-DD', 'YYYY/MM/DD', etc."""
    if pd.isna(iso_date): 
        return None
    s = str(iso_date).strip().replace("/", "-")
    parts = s.split("-")
    try:
        y = parts[0].zfill(4)
        m = (parts[1].zfill(2) if len(parts) > 1 else "01")
        d = (parts[2].zfill(2) if len(parts) > 2 else "01")
        int(y); int(m); int(d)
        return y, m, d
    except Exception:
        return None

def format_date(val):
    """Normaliza la fecha a YYYY-MM-DD (sin hora). Si falla, devuelve el valor como string."""
    try:
        d = pd.to_datetime(val, dayfirst=True, errors="coerce")
        return d.date().isoformat() if pd.notna(d) else str(val)
    except Exception:
        return str(val)

def normalize_text(s: str) -> str:
    """Limpia artefactos comunes de boletines: soft-hyphen, zero-width, guiones de final de línea,
    letras separadas, signos sueltos '>' y '<', espacios múltiples, etc."""
    if not s:
        return s
    # Invisibles y BOM/soft hyphen
    s = (s.replace('\u00AD','')   # soft hyphen
           .replace('\u200B','')  # zero width space
           .replace('\u200C','')  # zero width non-joiner
           .replace('\u200D','')  # zero width joiner
           .replace('\ufeff','')  # BOM
           .replace('\r',''))

    # Une palabras cortadas por guión al final de línea
    s = re.sub(r'-\s*\n\s*', '', s)

    # Quita marcas sueltas tipo "ark>" o "os>" (residuales)
    s = re.sub(r'([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{2,})>', r'\1', s)
    s = s.replace('<', '').replace('>', '')

    # Normaliza saltos de línea múltiples
    s = re.sub(r'\n{2,}', '\n', s)

    # Une secuencias tipo "t r a b a j o" (3+ letras separadas por espacios)
    def _join_letters(m):
        return m.group(0).replace(' ', '')
    s = re.sub(r'(?:(?<=\b)|(?<=\W))((?:[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]\s){2,}[A-Za-zÁÉÍÓÚÜÑáéíóúüñ])(?=\b|\W)', _join_letters, s)

    # Colapsa múltiples espacios/tabs
    s = re.sub(r'[ \t]{2,}', ' ', s)
    return s

def fetch_pdf_bytes(url, timeout=90, retries=3, sleep=2):
    """Descarga PDF (bytes) con reintentos simples."""
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            time.sleep(sleep*(i+1))
    raise RuntimeError(f"Descarga fallida: {url} -> {last}")

def extract_text_from_pdf_bytes(b: bytes, max_pages=0) -> str:
    """Extrae texto con PyMuPDF. max_pages=0 => todas las páginas."""
    text = []
    with fitz.open(stream=b, filetype="pdf") as doc:
        pages = range(len(doc)) if max_pages in (0, None) else range(min(max_pages, len(doc)))
        for i in pages:
            text.append(doc.load_page(i).get_text("text"))
    return "\n".join(text)

def truncate(s, n=20000):
    return s if len(s) <= n else s[:n] + " …"

# ----------------- PROGRAMA PRINCIPAL -----------------
def main():
    ap = argparse.ArgumentParser(description="Corpus gratuito: extrae texto e índices (sin guardar PDFs).")
    ap.add_argument("--excel", default="Libro para GITHUB.xlsx", help="Ruta al Excel fuente")
    ap.add_argument("--sheet", default=0, help="Nombre o índice de hoja (por defecto 0)")
    ap.add_argument("--col-date", default="fecha de publicación", help="Nombre de la columna de fecha")
    ap.add_argument("--col-url",  default="documento (.pdf)",     help="Nombre de la columna URL PDF")
    ap.add_argument("--limit", type=int, default=0, help="Límite de filas a procesar (0=todas)")
    ap.add_argument("--sleep", type=float, default=0.5, help="Pausa entre descargas (seg)")
    ap.add_argument("--resume", action="store_true", help="Reanudar usando index_texts.csv (salta lo ya procesado)")
    ap.add_argument("--rewrite", action="store_true", help="Re-escribir .txt aunque exista")
    ap.add_argument("--max-pages", type=int, default=0, help="0=todas; limitar páginas por PDF acelera y aligera")
    ap.add_argument("--max-chars", type=int, default=1200, help="Longitud máx. por documento en el índice JSON")
    args = ap.parse_args()

    # Asegura carpetas
    REPO_ROOT.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # Lee Excel
    df = pd.read_excel(args.excel, sheet_name=args.sheet)
    if args.col_url not in df.columns:
        raise SystemExit(f"No encuentro la columna URL '{args.col_url}'. Columnas: {list(df.columns)}")

    # Conjunto de URLs ya procesadas (si resume)
    done = set()
    if args.resume and META_CSV.exists():
        with META_CSV.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["pdf_url"])

    # Abrir CSV de metadatos (append)
    meta_exists = META_CSV.exists()
    meta_f = META_CSV.open("a", newline="", encoding="utf-8")
    meta_w = csv.DictWriter(meta_f, fieldnames=["date","pdf_url","text_relpath","chars","sha256_bytes"])
    if not meta_exists:
        meta_w.writeheader()

    # Cargar JSON existente (para no perder entradas si re-ejecutas)
    docs = []
    if DOCS_JSON.exists():
        try:
            docs = json.loads(DOCS_JSON.read_text(encoding="utf-8"))
        except Exception:
            docs = []

    # Iteración
    rows  = df.to_dict("records")
    total = len(rows) if args.limit == 0 else min(args.limit, len(rows))
    pbar  = tqdm(rows[:total], desc="Extrayendo texto", unit="pdf")

    for r in pbar:
        url = str(r.get(args.col_url, "")).strip()
        if not url or url.lower().startswith("nan"):
            continue
        if args.resume and url in done:
            continue

        # Estructura por fecha (YYYY/MM/DD)
        d_raw = r.get(args.col_date, "")
        d_fmt = format_date(d_raw)
        ymd   = date_parts(d_fmt)
        base_dir = TEXT_DIR
        if ymd:
            base_dir = base_dir / ymd[0] / ymd[1] / ymd[2]
        base_dir = Path(str(base_dir).strip("/\\"))
        base_dir.mkdir(parents=True, exist_ok=True)

        # Rutas .txt
        filename_pdf = safe_filename(url)
        base_name    = filename_pdf[:-4] if filename_pdf.lower().endswith(".pdf") else filename_pdf
        txt_path     = base_dir / f"{base_name}.txt"

        try:
            # Descarga y hash
            pdf_bytes = fetch_pdf_bytes(url)
            sha = hashlib.sha256(pdf_bytes).hexdigest()

            # Extrae/escribe texto (siempre si --rewrite; si no, solo si falta)
            if args.rewrite or (not txt_path.exists() or txt_path.stat().st_size == 0):
                text = extract_text_from_pdf_bytes(pdf_bytes, max_pages=args.max_pages)
                text = normalize_text(text)
                txt_path.write_text(text, encoding="utf-8", errors="ignore")

            # Metadatos CSV
            text_len = txt_path.stat().st_size
            rel_text = txt_path.relative_to(REPO_ROOT).as_posix()
            meta_w.writerow({
                "date": d_fmt,
                "pdf_url": url,
                "text_relpath": rel_text,
                "chars": text_len,
                "sha256_bytes": sha
            })
            meta_f.flush()

            # Entrada para JSON (recortada para web)
            try:
                content = txt_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                content = ""
            docs.append({
                "id": hashlib.md5(url.encode("utf-8")).hexdigest(),
                "date": d_fmt,
                "pdf_url": url,
                "text_relpath": rel_text,
                "snippet": truncate(content.replace("\n", " "), args.max_chars)
            })

            time.sleep(args.sleep)

        except Exception as e:
            print(f"[ERROR] {url} -> {e}")

    meta_f.close()

    # Guardar índice JSON para la web
    DOCS_JSON.parent.mkdir(parents=True, exist_ok=True)
    DOCS_JSON.write_text(json.dumps(docs, ensure_ascii=False), encoding="utf-8")

    print(f"\n✓ Listo.")
    print(f"  Textos:        {TEXT_DIR}")
    print(f"  Índice CSV:    {META_CSV}")
    print(f"  Índice JSON:   {DOCS_JSON}")
    print("  Consejo: si pesa mucho, ejecuta con --max-pages 6 --max-chars 1200 para aligerar.")

if __name__ == "__main__":
    main()
