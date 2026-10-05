#!/usr/bin/env python3
"""
Verify every code snippet the documentation pulls out of the C# sources.

Why this exists
---------------
DocFX resolves `[!code-csharp[](File.cs#Region)]` at build time, which is what
makes "the examples are real, compiling, tested code" true. But when the region
does not exist, DocFX emits an *empty* <pre><code> block and reports
`0 warning(s)`. So renaming or deleting a #region silently blanks a code block
on the website and nothing fails. (`<code source=...>` inside an XML doc comment
does warn -- `CodeNotFound` -- but only when the whole *file* is missing, not
when the region is.)

This script closes that gap. It is intended to run in CI, before `docfx build`.

Checks
------
  1. Every snippet reference in docfx/**/*.md points at a file that exists.
  2. Every `#Region` fragment names a #region that actually exists there.
  3. Every `#Lstart-Lend` fragment is within the file's line count.
  4. Same for `<code source="..." region="..."/>` in XML doc comments in **/*.cs.
  5. Reports regions defined in files the docs use, which nothing references
     (dead examples) -- informational, does not fail the build.

Exit status: 0 if everything resolves, 1 otherwise.
"""

from __future__ import annotations

import os
import re
import sys
from collections import defaultdict

# docfx/ -- this script lives there; the repo root is its parent.
DOCFX_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DOCFX_DIR)

# [!code-csharp[Label](path/to/File.cs#Fragment "optional title")]
SNIPPET_RE = re.compile(
    r"\[!code-(?P<lang>[\w-]+)"
    r"\[(?P<label>[^\]]*)\]"
    r"\(\s*(?P<path>[^)#\s\"]+)"
    r"(?:#(?P<frag>[^)\s\"]+))?"
    r"(?:\s+\"[^\"]*\")?\s*\)\]"
)

# <code source="File.cs" region="Name" .../> inside /// XML doc comments
XMLDOC_RE = re.compile(
    r"<code\b[^>]*?\bsource\s*=\s*\"(?P<path>[^\"]+)\""
    r"(?:[^>]*?\bregion\s*=\s*\"(?P<region>[^\"]+)\")?",
    re.IGNORECASE,
)

REGION_RE = re.compile(r"^\s*#region\s+(?P<name>\S.*?)\s*$")
LINE_RANGE_RE = re.compile(r"^L(?P<start>\d+)(?:-L(?P<end>\d+))?$")

SKIP_DIRS = {"_site", "api", "obj", "bin", ".git", "node_modules", "scratch", "out"}


def walk(root: str, suffix: str):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(suffix):
                yield os.path.join(dirpath, name)


_region_cache: dict[str, tuple[set[str], int]] = {}


def regions_of(path: str) -> tuple[set[str], int]:
    """Return (set of #region names, line count) for a source file."""
    if path not in _region_cache:
        try:
            with open(path, encoding="utf-8-sig") as handle:
                lines = handle.read().splitlines()
        except OSError:
            _region_cache[path] = (set(), 0)
        else:
            names = set()
            for line in lines:
                match = REGION_RE.match(line)
                if match:
                    names.add(match.group("name"))
            _region_cache[path] = (names, len(lines))
    return _region_cache[path]


def rel(path: str) -> str:
    return os.path.relpath(path, REPO_ROOT)


def main() -> int:
    errors: list[str] = []
    # source file -> set of region names the docs actually use
    used: dict[str, set[str]] = defaultdict(set)

    # --- 1. Markdown snippet references -----------------------------------
    for md in walk(DOCFX_DIR, ".md"):
        with open(md, encoding="utf-8-sig") as handle:
            text = handle.read()

        # Snippet syntax shown *inside* a fenced block is documentation about
        # the syntax, not a reference to resolve. Track fences and skip them.
        fence: str | None = None

        for line_no, line in enumerate(text.splitlines(), 1):
            stripped = line.lstrip()
            if fence is not None:
                if stripped.startswith(fence):
                    fence = None
                continue
            if stripped.startswith("```") or stripped.startswith("~~~"):
                marker = stripped[0] * 3
                # An opening fence may carry an info string; a closing one may not.
                fence = marker
                continue

            for m in SNIPPET_RE.finditer(line):
                where = f"{rel(md)}:{line_no}"
                target = os.path.normpath(
                    os.path.join(os.path.dirname(md), m.group("path"))
                )

                if not os.path.isfile(target):
                    errors.append(f"{where}: no such file: {m.group('path')}")
                    continue

                frag = m.group("frag")
                if frag is None:
                    used[target]  # noqa: B018 - mark file as doc-referenced
                    continue

                names, n_lines = regions_of(target)
                lr = LINE_RANGE_RE.match(frag)
                if lr:
                    start = int(lr.group("start"))
                    end = int(lr.group("end") or lr.group("start"))
                    if start < 1 or end > n_lines or start > end:
                        errors.append(
                            f"{where}: line range #{frag} is outside "
                            f"{m.group('path')} (1-{n_lines})"
                        )
                elif frag in names:
                    used[target].add(frag)
                else:
                    close = sorted(
                        n for n in names if n.lower().startswith(frag[:3].lower())
                    )
                    hint = f"  did you mean: {', '.join(close)}" if close else ""
                    errors.append(
                        f"{where}: no '#region {frag}' in {m.group('path')}."
                        f" DocFX would render an EMPTY code block here.{hint}"
                    )

    # --- 2. <code source=.. region=..> in XML doc comments -----------------
    for cs in walk(REPO_ROOT, ".cs"):
        with open(cs, encoding="utf-8-sig") as handle:
            text = handle.read()
        if "<code" not in text:
            continue

        for line_no, line in enumerate(text.splitlines(), 1):
            if "///" not in line:
                continue
            for m in XMLDOC_RE.finditer(line):
                where = f"{rel(cs)}:{line_no}"
                # DocFX resolves this relative to the .cs file's own directory
                # and strips any leading "../" segments.
                raw = m.group("path").replace("\\", "/")
                cleaned = "/".join(p for p in raw.split("/") if p not in ("..", "."))
                if raw != cleaned:
                    errors.append(
                        f"{where}: <code source=\"{raw}\"> uses '..', which DocFX "
                        f"strips. A doc comment cannot reach outside its own "
                        f"directory -- use a docfx/apidoc/ overwrite file instead."
                    )
                    continue

                target = os.path.normpath(os.path.join(os.path.dirname(cs), cleaned))
                if not os.path.isfile(target):
                    errors.append(f"{where}: no such file: {raw}")
                    continue

                region = m.group("region")
                if region is None:
                    continue
                names, _ = regions_of(target)
                if region in names:
                    used[target].add(region)
                else:
                    errors.append(
                        f"{where}: no '#region {region}' in {raw}"
                    )

    # --- 3. Informational: regions nothing references ----------------------
    orphans: list[str] = []
    for target, used_names in sorted(used.items()):
        names, _ = regions_of(target)
        for name in sorted(names - used_names):
            orphans.append(f"  {rel(target)}: #region {name}")

    if orphans:
        print("Regions defined but not referenced by any documentation:")
        print("\n".join(orphans))
        print()

    n_refs = sum(len(v) for v in used.values())
    if errors:
        print(f"FAIL: {len(errors)} broken documentation snippet reference(s):\n")
        for e in errors:
            print(f"  {e}")
        print()
        return 1

    print(f"OK: {n_refs} documentation snippet reference(s) resolve "
          f"across {len(used)} source file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
