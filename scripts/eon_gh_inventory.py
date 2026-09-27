#!/usr/bin/env python3
"""eon_gh_inventory.py — missing-tool add (D2558): repo audit for EON-ARCH.
Read-only. Needs GH_TOKEN env. Prints repo, size KB, pushed date, open issues."""
import json, os, urllib.request
TOK = os.environ.get("GH_TOKEN", "")
H = {"Authorization": f"Bearer {TOK}", "Accept": "application/vnd.github+json"}
def get(url):
    req = urllib.request.Request(url, headers=H)
    return json.load(urllib.request.urlopen(req, timeout=20))
repos = get("https://api.github.com/users/didicola/repos?per_page=100&type=all")
for r in repos:
    n = r.get("name")
    try:
        iss = get(f"https://api.github.com/repos/didicola/{n}/issues?state=open&per_page=100")
        no = len(iss) if isinstance(iss, list) else "?"
    except Exception:
        no = "?"
    print(f"{n:22s} {r.get('size'):>8d}KB pushed:{(r.get('pushed_at') or '')[:10]} open_issues:{no}")
