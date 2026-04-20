from __future__ import annotations

import json
import math
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Tuple


RUNOFF_KEYS = [
    "lag",
    "wum",
    "wlm",
    "wdm",
    "k",
    "c",
    "b",
    "imp",
    "sm",
    "ex",
    "kss",
    "kg",
    "kkss",
    "kkg",
    "cs",
    "area",
]

STATE_KEYS = ["wu", "wl", "wd", "fr", "s", "qrss0", "qrg0", "qs0"]


def _f(v: Any) -> float:
    return float(str(v).strip())


def _close(a: float, b: float, tol: float = 1e-6) -> bool:
    return math.isclose(float(a), float(b), rel_tol=tol, abs_tol=tol)


def _parse_xml_units(xml_path: Path) -> Dict[str, Dict[str, Any]]:
    root = ET.fromstring(xml_path.read_text(encoding="utf-8"))
    out: Dict[str, Dict[str, Any]] = {}
    for unit in root.findall(".//unit"):
        uid = (unit.findtext("unitId") or "").strip()
        if not uid:
            continue
        g = unit.find("unitGModel")
        c = unit.find("unitCModel")
        if g is None:
            continue
        params = {
            "lag": _f(g.findtext("lag", "0")),
            "wum": _f(g.findtext("wum", "0")),
            "wlm": _f(g.findtext("wlm", "0")),
            "wdm": _f(g.findtext("wdm", "0")),
            "k": _f(g.findtext("k", "0")),
            "c": _f(g.findtext("c", "0")),
            "b": _f(g.findtext("b", "0")),
            "imp": _f(g.findtext("imp", "0")),
            "sm": _f(g.findtext("sm", "0")),
            "ex": _f(g.findtext("ex", "0")),
            "kss": _f(g.findtext("kss", "0")),
            "kg": _f(g.findtext("kg", "0")),
            "kkss": _f(g.findtext("kkss", "0")),
            "kkg": _f(g.findtext("kkg", "0")),
            "cs": _f(g.findtext("cs", "0")),
            "area": _f(g.findtext("area", "0")),
        }
        state = {
            "wu": _f(g.findtext("wu0", "0")),
            "wl": _f(g.findtext("wl0", "0")),
            "wd": _f(g.findtext("wd0", "0")),
            "fr": _f(g.findtext("fr0", "0")),
            "s": _f(g.findtext("s0", "0")),
            "qrss0": _f(g.findtext("qrss0", "0")),
            "qrg0": _f(g.findtext("qrg0", "0")),
            "qs0": _f(g.findtext("qrs0", "0")),
        }
        routing = {
            "k_hours": _f(c.findtext("ke", "0")) if c is not None else 0.0,
            "x": _f(c.findtext("xe", "0")) if c is not None else 0.0,
            "n_segments": int(float(c.findtext("n", "1"))) if c is not None else 1,
        }
        evap = unit.find("evapStation")
        evap_arr: List[float] = []
        if evap is not None:
            arr = (evap.findtext("evapArr") or "").strip()
            if arr:
                evap_arr = [_f(x) for x in arr.split(",") if str(x).strip()]
        pre = []
        for ps in unit.findall("preStation"):
            sid = (ps.findtext("preStaSenid") or "").strip()
            wt = _f(ps.findtext("preStaWeight", "0"))
            if sid:
                pre.append((sid, wt))
        pre.sort()
        out[uid] = {
            "params": params,
            "state": state,
            "routing": routing,
            "evap_arr": evap_arr,
            "prestations": pre,
        }
    return out


def _parse_json_scheme(json_path: Path, time_type: str = "Hour", step_size: int = 1) -> Dict[str, Any]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    scheme = None
    for s in data.get("schemes", []):
        if str(s.get("time_type")) == time_type and int(s.get("step_size", -1)) == int(step_size):
            scheme = s
            break
    if scheme is None:
        raise ValueError("target scheme not found in json")

    bind_map: Dict[str, Dict[str, Any]] = {}
    bindings = scheme.get("catchment_forcing_bindings", scheme.get("forcing_bindings", [])) or []
    for b in bindings:
        cid = str(b.get("catchment_id", "")).strip()
        if not cid:
            continue
        bind_map[cid] = b

    c_map: Dict[str, Dict[str, Any]] = {}
    for c in scheme.get("catchments", []) or []:
        cid = str(c.get("id", "")).strip()
        if not cid:
            continue
        rm = ((c.get("runoff_model") or {}).get("params") or {})
        st = ((c.get("runoff_model") or {}).get("state") or {})
        rt = ((c.get("routing_model") or {}).get("params") or {})
        bind = bind_map.get(cid, {})
        evap_arr: List[float] = []
        pre = []
        for var in bind.get("variables", []) or []:
            if str(var.get("kind", "")).strip() == "potential_evapotranspiration":
                evap_arr = [_f(x) for x in (var.get("monthly_values") or [])]
            if str(var.get("kind", "")).strip() == "precipitation":
                for stn in var.get("stations", []) or []:
                    sid = str(stn.get("id", "")).strip()
                    wt = _f(stn.get("weight", "0"))
                    if sid:
                        pre.append((sid, wt))
        pre.sort()
        c_map[cid] = {
            "params": {k: _f(rm.get(k, 0.0)) for k in RUNOFF_KEYS},
            "state": {k: _f(st.get(k, 0.0)) for k in STATE_KEYS},
            "routing": {
                "k_hours": _f(rt.get("k_hours", 0.0)),
                "x": _f(rt.get("x", 0.0)),
                "n_segments": int(rt.get("n_segments", 1)),
            },
            "evap_arr": evap_arr,
            "prestations": pre,
        }
    return c_map


def compare(xml_path: Path, json_path: Path) -> List[str]:
    xml_units = _parse_xml_units(xml_path)
    js_units = _parse_json_scheme(json_path)
    diffs: List[str] = []
    for cid, x in sorted(xml_units.items()):
        j = js_units.get(cid)
        if j is None:
            diffs.append(f"[MISSING] catchment {cid} exists in XML but not JSON")
            continue
        for k in RUNOFF_KEYS:
            if not _close(x["params"][k], j["params"][k]):
                diffs.append(f"[PARAM] {cid}.{k}: xml={x['params'][k]} json={j['params'][k]}")
        for k in STATE_KEYS:
            if not _close(x["state"][k], j["state"][k]):
                diffs.append(f"[STATE] {cid}.{k}: xml={x['state'][k]} json={j['state'][k]}")
        for k in ("k_hours", "x"):
            if not _close(x["routing"][k], j["routing"][k]):
                diffs.append(f"[ROUTING] {cid}.{k}: xml={x['routing'][k]} json={j['routing'][k]}")
        if int(x["routing"]["n_segments"]) != int(j["routing"]["n_segments"]):
            diffs.append(
                f"[ROUTING] {cid}.n_segments: xml={x['routing']['n_segments']} json={j['routing']['n_segments']}"
            )
        if len(x["evap_arr"]) != len(j["evap_arr"]) or any(
            not _close(a, b) for a, b in zip(x["evap_arr"], j["evap_arr"])
        ):
            diffs.append(f"[EVAP] {cid}.monthly_values mismatch: xml={x['evap_arr']} json={j['evap_arr']}")
        if len(x["prestations"]) != len(j["prestations"]):
            diffs.append(
                f"[RAIN_ST] {cid}.station_count: xml={len(x['prestations'])} json={len(j['prestations'])}"
            )
        else:
            for (xs, xw), (jsid, jw) in zip(x["prestations"], j["prestations"]):
                if xs != jsid or (not _close(xw, jw)):
                    diffs.append(f"[RAIN_ST] {cid}: xml=({xs},{xw}) json=({jsid},{jw})")

    for cid in sorted(js_units.keys()):
        if cid not in xml_units:
            diffs.append(f"[EXTRA] catchment {cid} exists in JSON but not XML")
    return diffs


if __name__ == "__main__":
    xml = Path(r"d:\floodForecastSystem\HFSchemeConf.xml")
    js = Path(r"d:\floodForecastSystem\hydro_project\configs\forecastSchemeConf.json")
    res = compare(xml, js)
    print(f"Compared XML={xml.name} vs JSON={js.name}")
    if not res:
        print("No differences found in catchment params/state/routing/evap/station weights.")
    else:
        print(f"Differences: {len(res)}")
        for line in res:
            print(line)
