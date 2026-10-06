# DocFX bug: breadcrumbs break when a table of contents points at anchors

Status: **investigated, not implemented.** Nothing in this repository has been changed.
The fix below was verified in a scratch build on 2026-10-06 against **DocFX 2.81.0** and
then reverted. A copy of the fix script is parked at
`docfx/scratch/breadcrumb-fix-main.js` (that directory is git-ignored).

This only bites if we adopt an **anchor-based API table of contents** — a hand-curated
`toc.yml` whose member entries are `uid`s that resolve to fragments on the type page
(`Mallard.DuckDbConnection.html#Mallard_DuckDbConnection_ExecuteValue_`) rather than to
separate member pages. We have not adopted it. The attraction is that it lets the sidebar
be grouped by task ("Connecting", "Querying", "Appending") and show method *verbs*, while
keeping each type on one page and the page count at 55 instead of 319. This bug is the
main thing standing in the way, so it is written up here before the context is lost.

---

## 1. Symptom

With a curated TOC, the breadcrumb on a type page lists every sibling member:

```
API / Connecting / DuckDbConnection / BeginTransaction / PrepareStatement / ExecuteNonQuery
```

and it is **inconsistent** between pages — which is worse than it merely being long:

| page | breadcrumb |
|---|---|
| `DuckDbConnection` (has curated member entries) | `API / Connecting / `**`DuckDbConnection`** |
| `DuckDbTransaction` (no curated member entries) | `API / Connecting` |

A type keeps its own name in the trail only when it happens to have member entries
underneath it. There is no way to read the page's identity off the breadcrumb reliably.

---

## 2. Root cause

Two separate defects compound. Both are in the `modern` template's client-side
TypeScript — the breadcrumb is **not** rendered by Mustache. `layout/_master.tmpl` emits
only an empty `<nav id="breadcrumb"></nav>` and `public/docfx.min.js` fills it in.

### 2.1 `isSameURL` compares the path and ignores the fragment

`helper.ts`:

```ts
/**
 * Determines if two URLs should be considered the same.
 */
export function isSameURL(a: { pathname: string }, b: { pathname: string }): boolean {
  return normalizeUrlPath(a) === normalizeUrlPath(b)

  function normalizeUrlPath(url: { pathname: string }): string {
    return url.pathname
      .replace(/\/index\.html$/gi, '/')
      .replace(/\.html$/gi, '')
      .replace(/\/$/gi, '')
      .toLowerCase()
  }
}
```

Note the parameter type: it structurally accepts only `{ pathname }`, so the fragment is
not merely unused, it is unreachable. Every TOC node pointing at
`…/Mallard.DuckDbConnection.html#anything` is therefore "the current page", and all of
them are marked active.

### 2.2 `activeNodes.slice(0, -1)` removes the wrong node

`toc.ts`, inside `renderToc()`:

```ts
const activeNodes = []
items.forEach(initTocNodes)
…
return activeNodes.slice(0, -1)

function initTocNodes(node: TocNode): boolean {
  let active
  if (node.href) {
    const url = new URL(node.href, tocUrl)
    node.href = url.href
    active = isExternalHref(url) ? false : isSameURL(url, window.location)
    if (active) {
      if (node.items) { node.expanded = true }
      selectedNodes.push(node)
    }
  }
  if (node.items) {
    for (const child of node.items) {
      if (initTocNodes(child)) { active = true; node.expanded = true }
    }
  }
  if (active) { activeNodes.unshift(node); return true }
  return false
}
```

The `slice(0, -1)` is meant to drop the current page, leaving its ancestors. Because the
walk `unshift`s as the recursion unwinds, the last element is the *deepest, last-visited*
active node. When anchor siblings exist that is one of the anchors, so the page node
survives; when they do not, the page node is dropped. Hence the inconsistency in §1.

The breadcrumb the user sees is `[...navBreadcrumb, ...tocBreadcrumb]`, assembled in
`docfx.ts`.

---

## 3. What the DOM actually contains

Dumped post-JavaScript with
`chromium --headless --virtual-time-budget=9000 --dump-dom <url>`, on the
`DuckDbConnection` page with a curated TOC:

```
API              -> …/api/Mallard.DuckDbConnection.html                       (no fragment)
Connecting       -> ""                                                        (group node)
DuckDbConnection -> …/api/Mallard.DuckDbConnection.html                       (no fragment)
BeginTransaction -> …/api/Mallard.DuckDbConnection.html#Mallard_…_BeginTransaction_
PrepareStatement -> …/api/Mallard.DuckDbConnection.html#Mallard_…_PrepareStatement_
ExecuteNonQuery  -> …/api/Mallard.DuckDbConnection.html#Mallard_…_ExecuteNonQuery_
```

Two traps live in that listing, and both cost time if rediscovered from scratch:

- **`API` also points at the current page.** The root `toc.yml` has `href: api/`, and a
  folder reference resolves to the first page of that folder's TOC — which, with our
  curated ordering, is `DuckDbConnection`. So "drop every entry whose href is the current
  page" deletes the legitimate `API` ancestor. The **fragment** is the only sound
  discriminator.
- **Group nodes carry `href=""`.** `new URL("", location.href)` resolves to the current
  page, so an empty href must be excluded explicitly or the group label gets eaten too.

---

## 4. The fix to carry in-tree

Goes in `docfx/template/public/main.js` — which we do not currently have; the `modern`
template ships a stub and ours would override it. `main.js` is a supported extension
point: `docfx.min.js` does `await import("./main.js").then(i => i.default)`, and the
`start()` hook runs before the breadcrumb is rendered, so a `MutationObserver` catches it.

```js
export default {
  start: () => {
    const nav = document.getElementById("breadcrumb");
    if (!nav) return;

    const resolve = (a) => {
      const raw = a?.getAttribute("href");
      if (!raw) return null;                       // group nodes carry no href
      try { return new URL(raw, window.location.href); } catch { return null; }
    };
    const onThisPage = (u) => u && u.pathname === window.location.pathname;

    const prune = () => {
      let items = [...nav.querySelectorAll(".breadcrumb-item")];
      for (const li of items) {                    // 1. anchors into this page
        const u = resolve(li.querySelector("a"));
        if (u && u.hash && onThisPage(u)) li.remove();
      }
      items = [...nav.querySelectorAll(".breadcrumb-item")];
      if (items.length > 1) {                      // 2. the page node docfx missed
        const last = items[items.length - 1];
        const u = resolve(last.querySelector("a"));
        if (u && !u.hash && onThisPage(u)) last.remove();
      }
    };

    new MutationObserver(prune).observe(nav, { childList: true, subtree: true });
    prune();
  },
};
```

Pass 1 fixes §2.1, pass 2 fixes §2.2. Verified results:

| page | before | after |
|---|---|---|
| type with curated members | `API / Connecting / DuckDbConnection / BeginTransaction / PrepareStatement / ExecuteNonQuery` | `API / Connecting` |
| type without members | `API / Connecting` | `API / Connecting` |
| another group | `API / Appending` | `API / Appending` |
| ordinary article | `Examples` | `Examples` |

**It is a verified no-op on the stock generated TOC.** With the fix installed and
`api/toc.yml` left as DocFX generates it, breadcrumbs are identical to baseline
(`API / Mallard`, `API`, `Examples`). So it can be landed before we commit to a curated
TOC, and it does no harm if we never do.

The resulting trail is ancestors-only, which matches DocFX's convention elsewhere and
avoids repeating the `<h1>`. To keep the page's own name instead, delete pass 2 — but
then the §1 inconsistency returns.

---

## 5. Upstream fix, for the pull request

Blast radius is small: **`isSameURL` has exactly one call site**, `toc.ts:70`.

The honest fix is to make TOC activity fragment-aware rather than to post-process the
DOM. Sketch:

1. Widen `isSameURL` to take the hash into account, or add a sibling helper
   (`isSameDocument` for path-only, `isSameURL` for path+hash) and use the right one at
   each site. The current signature `{ pathname: string }` has to change either way.
2. At `toc.ts:70`, treat a node as the *selected* node only on an exact match
   (path **and** hash), and as an *ancestor* when its path matches but its hash does not.
   Anchor siblings then stop being active.
3. Replace `activeNodes.slice(0, -1)` with an explicit "drop the selected node",
   independent of visit order. The current code relies on the selected node being last,
   which is exactly the assumption that anchors violate.

Worth checking while in there, because it is driven by the same `active` flag:

- sidebar highlighting (`li.active`) — with anchors it cannot currently distinguish which
  member is being viewed (see §7);
- `renderNextArticle(items, selectedNodes[0])` — prev/next links, also fed by the same walk.

Source locations. The source map calls them `../src/toc.ts`, `../src/helper.ts`,
`../src/nav.ts`, `../src/docfx.ts`, relative to `templates/modern/public/`. In the
`dotnet/docfx` repo these are almost certainly `templates/modern/src/*.ts` — **confirm
before filing**, that path is inferred from the map, not read from the repo.

To read the original TypeScript without cloning anything:

```
docfx template export modern -o /tmp/tmpl
cd /tmp/tmpl/modern/public
# docfx.min.js.map and chunk-*.min.js.map carry full sourcesContent
python3 -c "import json; m=json.load(open('docfx.min.js.map')); \
  print(dict(zip(m['sources'],m['sourcesContent']))['../src/toc.ts'])"
```

`helper.ts` lives in a shared chunk, not in `docfx.min.js.map`; at 2.81.0 that was
`chunk-LSENCIDT.min.js.map`, but the hash will change between releases, so grep the maps
rather than hard-coding it.

---

## 6. Approaches that do not work

- **CSS only.** `#breadcrumb .breadcrumb-item:has(> a[href*="#"]) { display: none }` does
  trim the anchor entries, and is tempting as a one-liner. It cannot fix §2.2, because by
  then DocFX has already decided which node to drop. Result:
  `API / Connecting / DuckDbConnection` on one page and `API / Connecting` on the next.
- **"Remove any breadcrumb entry that points at the current page."** Deletes the `API`
  ancestor, for the reason in §3.
- **`_disableBreadcrumb: true`.** Removes the breadcrumb altogether. With an anchor TOC
  the breadcrumb is the only on-page indication of where you are in the curated
  hierarchy, so this throws away the thing we were trying to gain.
- **Changing the curated TOC.** The hrefs *have* to carry fragments; that is the whole
  point of the anchor approach.

---

## 7. Hazards

**A broken or missing `public/main.js` silently kills all page JavaScript.** `docfx.min.js`
does `await import("./main.js")`; if that 404s or throws, the promise rejects and the
top-level `.catch(console.error)` swallows it. Search, the sidebar TOC, the breadcrumb and
the theme toggle all stop working, with nothing but a console message. This was hit by
accident while producing a control run — the breadcrumb simply came back empty and it
looked like a timing problem.

Once we own `template/public/main.js`, that file becomes load-bearing for the whole site.
A smoke check is cheap: load a built page in headless Chromium and assert
`window.docfx.ready === true`. That flag is set at the end of the init chain for exactly
this purpose. It would fit alongside `docfx/check-snippets.py` in the docs workflow.

**`:has()` and `MutationObserver`** are both fine in current browsers; no polyfill needed.

---

## 8. Open question: sidebar active-state

Not fixed, and not fixable from `main.js` alone. The sidebar marks `li.active` from the
same path-only comparison, so with an anchor TOC it cannot tell which member you are
looking at — either all siblings highlight or none do. Doing it properly means tracking
`location.hash` plus scroll position, which is roughly what the right-hand "In this
article" rail already does.

If we go the anchor route this is the next thing to look at. It is a strictly cosmetic
regression — navigation works — but it undercuts the "see where you are at a glance"
motivation.

---

## References

- DocFX 2.81.0, `modern` template.
- `docfx/scratch/breadcrumb-fix-main.js` — the script from §4 (git-ignored).
- `docfx/scratch/preview/` — screenshots, including `curated-anchor-toc.png`
  (the broken breadcrumb) and `separatepages-*.png` (the alternative, where each member
  is its own page and the breadcrumb is correct because the bug does not apply).
- Related config, if the separate-pages route is preferred instead:
  `memberLayout: SeparatePages` and `namespaceLayout: Nested` under `metadata` in
  `docfx/docfx.json`.
