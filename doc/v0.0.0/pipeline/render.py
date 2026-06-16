#!/usr/bin/env python3
"""render.py - PDF -> page PNGs via pdftoppm (poppler). Stdlib + poppler only."""
import glob, os, shutil, subprocess


def render(pdf, out_dir, dpi=150, max_pages=100):
    os.makedirs(out_dir, exist_ok=True)
    if not shutil.which("pdftoppm"):
        raise RuntimeError("pdftoppm not installed (conda install -c conda-forge poppler)")
    prefix = os.path.join(out_dir, "page")
    proc = subprocess.run(
        ["pdftoppm", "-png", "-r", str(dpi), "-l", str(max_pages), pdf, prefix],
        capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        raise RuntimeError("pdftoppm rc=%d: %s" % (proc.returncode, (proc.stderr or proc.stdout or "")[:300]))
    return sorted(glob.glob(prefix + "-*.png"))
