"""⑥ 품질 관문 (배치 1천 개마다, 설계 7절) + 사람 검수.

캘린더 지름길은 주문 유형별로 비교한 차이의 가중 평균이다 (일정이 의도를 늘린 쪽이 +).

1단계 (비교가 끝나면): 자동 지표를 계산하고 사람 검수 파일을 만든다 → 상태 reviewing
  - 검수 30개: 두 라벨 일치 50% · 갈림 30% · W 유형 20%
  - review.people(U · Y · S · J)에게 나눠서 review/todo/<배치>_<이름>.csv
2단계 (검수 파일이 review/done/에 다 모이면): 틀림 비율을 더해 최종 판정 → passed / failed

지표와 기준은 config/pipeline.yaml의 gate. 보고서는 배치 폴더의 report.json · report.md.
"""

from __future__ import annotations

import csv
import re
from collections import Counter

from spira import batches as B
from spira import config
from spira.checks import _read_csv

REVIEW = config.ROOT / "review"
FIELDS = ["번호", "review_id", "물건", "가구", "도착 요일", "캘린더", "정답 의도", "의도 문장",
          "판정 (O/X)", "맞는 의도 id (X일 때)", "메모"]


def _rate(n, d):
    return None if d == 0 else n / d


def metrics(batch: str) -> dict:
    cfg = B.pipeline()["gate"]
    comp = B.read(batch, "compared.json")
    cs = comp["cases"]
    n = len(cs)
    none_ratio = _rate(sum(1 for c in cs if not c["final"]), n)

    # 캘린더 지름길: 관련 없는 일정이 붙은 주문이 일정 없는 주문보다 의도를 더 많이 받는지.
    # 주문 유형(A · B · W …)마다 따로 비교하고 두 묶음 크기로 가중 평균한다.
    # 유형 구성이 두 묶음에서 달라서 생기는 차이는 지름길이 아니므로 섞지 않는다.
    unrel = [c for c in cs if c["has_calendar"] and not c["has_related"]]
    nocal = [c for c in cs if not c["has_calendar"]]
    diff_sum = weight = 0
    by_type = {}
    for t in sorted({c["type"] for c in cs}):
        u = [c for c in unrel if c["type"] == t]
        o = [c for c in nocal if c["type"] == t]
        if not u or not o:
            continue
        d = _rate(sum(1 for c in u if c["final"]), len(u)) - _rate(sum(1 for c in o if c["final"]), len(o))
        by_type[t] = {"diff": d, "unrelated": len(u), "no_calendar": len(o)}
        diff_sum += d * (len(u) + len(o))
        weight += len(u) + len(o)
    shortcut = None if weight == 0 else diff_sum / weight

    agreement = _rate(sum(1 for c in cs if c["agree"]), n)
    bs = [c for c in cs if c["type"] == "B"]
    b_recall = _rate(sum(1 for c in bs if c["made_intents"][0] in c["final"]), len(bs))
    ws = [c for c in cs if c["type"] == "W"]
    w_seen = _rate(sum(1 for c in ws if c["made_intents"][0] in set(c["label_1"]) | set(c["label_2"])
                       | set(c["alternatives"])), len(ws))

    weights = config.load_list("intents")["frequency_weight"]
    intents = B.intents()
    total_w = sum(weights[i["frequency"]] for i in intents)
    counts = Counter(x for c in cs for x in c["final"] if x != "unclassified")
    total = sum(counts.values())
    low = [i["id"] for i in intents
           if counts[i["id"]] < cfg["per_intent_min_ratio"] * total * weights[i["frequency"]] / total_w]

    checks = comp["checks"]
    check_wrong = sum(len(g["wrong"]) for g in checks.values())
    check_total = sum(g["total"] for g in checks.values())

    pairs = Counter()
    for c in cs:
        if not c["agree"]:
            pairs[" ↔ ".join(sorted(["+".join(c["label_1"]) or "none", "+".join(c["label_2"]) or "none"]))] += 1

    def between(x, lo_hi):
        return None if x is None else lo_hi[0] <= x <= lo_hi[1]

    checks_ok = None if check_total == 0 else check_wrong == 0
    return {
        "cases": n,
        "items": {
            "none_ratio": {"value": none_ratio, "target": cfg["none_ratio"], "ok": between(none_ratio, cfg["none_ratio"])},
            "calendar_shortcut": {"value": shortcut, "target": [0, cfg["calendar_shortcut_max"]],
                                  "ok": None if shortcut is None else shortcut <= cfg["calendar_shortcut_max"],
                                  "by_type": by_type},
            "label_agreement": {"value": agreement, "target": cfg["label_agreement"],
                                "ok": between(agreement, cfg["label_agreement"])},
            "b_recall": {"value": b_recall, "target": [cfg["b_recall_min"], 1], "ok": between(b_recall, [cfg["b_recall_min"], 1])},
            "w_seen": {"value": w_seen, "target": cfg["w_seen"], "ok": between(w_seen, cfg["w_seen"])},
            "per_intent": {"value": len(low), "low": low, "ok": not low},
            "checks": {"value": check_wrong, "total": check_total, "ok": checks_ok},
        },
        "unseen_intent": sum(1 for c in cs if c["unseen_intent"]),
        "unclassified": sum(1 for c in cs if "unclassified" in c["final"]),
        "frequent_splits": pairs.most_common(10),
        "intent_counts": dict(counts.most_common()),
    }


def _hh(c):
    return "(숨김)" if c["household_hidden"] else ", ".join(c["household_types"] or [])


def _cal(c):
    out = []
    for e in c["calendar"]:
        d = e["days_from_arrival"]
        when = f"{d}일 뒤" if d >= 0 else f"{-d}일 전"
        out.append(f"도착 {when} {e['weekday']}요일: {e['tag']}")
    return " / ".join(out) or "없음"


def make_review(batch: str) -> list:
    cfg = B.pipeline()["review"]
    rng = B.rng_for(batch, "review")
    comp = B.read(batch, "compared.json")
    cases = {c["case_ref"]: c for c in B.read(batch, "cases.json")}
    cs = comp["cases"]
    n = cfg["per_batch"]
    groups = {
        "agree": [c for c in cs if c["agree"] and c["type"] != "W"],
        "disagree": [c for c in cs if not c["agree"]],
        "weak": [c for c in cs if c["type"] == "W"],
    }
    picked, seen = [], set()
    for g, share in cfg["mix"].items():
        pool = [c for c in groups[g] if c["case_ref"] not in seen]
        for c in rng.sample(pool, min(len(pool), round(n * share))):
            picked.append(c)
            seen.add(c["case_ref"])
    rest = [c for c in cs if c["case_ref"] not in seen]   # 한쪽이 모자라면 나머지로 채운다
    picked += rng.sample(rest, max(0, n - len(picked)))
    rng.shuffle(picked)
    names = {i["id"]: i["name"] for i in B.intents()}
    people = cfg["people"]
    written = []
    REVIEW.joinpath("todo").mkdir(parents=True, exist_ok=True)
    for k, person in enumerate(people):
        mine = picked[k::len(people)]
        p = REVIEW / "todo" / f"{batch}_{person}.csv"
        with p.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            for i, c in enumerate(mine, 1):
                full = cases[c["case_ref"]]
                final = " + ".join(f"{x} ({names.get(x, x)})" for x in c["final"]) or "none (의도 없음)"
                text = " / ".join(v["intent_text"] for v in c["chosen"].values())
                w.writerow([i, c["case_ref"], ", ".join(full["items"]), _hh(full), full["arrival_weekday"],
                            _cal(full), final, text, "", "", ""])
        written.append(p)
    B.write(batch, "review_plan.json", {"people": people, "case_refs": [c["case_ref"] for c in picked]})
    written.append(B.bdir(batch) / "review_plan.json")
    return written


def review_done(batch: str) -> bool:
    plan = B.read(batch, "review_plan.json")
    return bool(plan) and all((REVIEW / "done" / f"{batch}_{p}.csv").exists() for p in plan["people"])


def review_result(batch: str) -> dict:
    plan = B.read(batch, "review_plan.json")
    judged = wrong = 0
    fixes = []
    for p in plan["people"]:
        for row in _read_csv(REVIEW / "done" / f"{batch}_{p}.csv"):
            v = next((row[k] for k in row if k and k.startswith("판정")), "").strip().upper()
            if v not in ("O", "X"):
                continue
            judged += 1
            if v == "X":
                wrong += 1
                fixes.append({"review_id": row.get("review_id"), "by": p,
                              "correct": next((row[k] for k in row if k and k.startswith("맞는 의도")), "").strip()})
    return {"judged": judged, "wrong": wrong, "wrong_ratio": _rate(wrong, judged), "fixes": fixes}


def review_corrections(batch: str) -> dict[str, list[str] | None]:
    """사람 검수에서 X를 받은 사례 → 고친 의도 목록.
    '맞는 의도 id' 칸을 쉼표 · + · 공백으로 나눠 읽는다. none이면 의도 없음.
    칸이 비었거나 모르는 id가 있으면 None (그 사례는 적재하지 않는다)."""
    plan = B.read(batch, "review_plan.json")
    known = {i["id"] for i in B.intents()} | {"none"}
    out: dict[str, list[str] | None] = {}
    for p in (plan or {}).get("people", []):
        for row in _read_csv(REVIEW / "done" / f"{batch}_{p}.csv"):
            v = next((row[k] for k in row if k and k.startswith("판정")), "").strip().upper()
            ref = (row.get("review_id") or "").strip()
            if v != "X" or not ref:
                continue
            raw = next((row[k] for k in row if k and k.startswith("맞는 의도")), "")
            ids = [x for x in re.split(r"[\s,+/]+", raw.strip().lower()) if x]
            out[ref] = sorted(set(ids)) if ids and all(x in known for x in ids) else None
    return out


def add_review(batch: str) -> list:
    """이미 판정이 난 배치에 사람 검수를 붙인다: 검수 파일을 만들고 reviewing으로 되돌린다.
    검수가 다 모이면 자동 지표 + 사람 검수로 다시 판정한다."""
    rep = B.read(batch, "report.json")
    rep.update(review=None, verdict=None)
    rep.pop("judged_at", None)
    written = [B.write(batch, "report.json", rep)]
    written += make_review(batch)
    written.append(_write_md(batch))
    B.set_state(batch, "reviewing", note="사람 검수 추가")
    written.append(B.bdir(batch) / "state.json")
    return written


def review_on() -> bool:
    return B.pipeline()["review"]["per_batch"] > 0


def step1(batch: str) -> list:
    m = metrics(batch)
    written = [B.write(batch, "report.json", {"metrics": m, "review": None, "verdict": None})]
    if not review_on():   # 사람 검수 없음: 자동 지표로 바로 판정
        return written + step2(batch)
    written += make_review(batch)
    written.append(_write_md(batch))
    B.set_state(batch, "reviewing")
    written.append(B.bdir(batch) / "state.json")
    return written


def step2(batch: str) -> list:
    cfg = B.pipeline()["gate"]
    rep = B.read(batch, "report.json")
    oks = [it["ok"] for it in rep["metrics"]["items"].values()]
    review = None
    if review_on():
        rv = review_result(batch)
        review_ok = rv["wrong_ratio"] is not None and rv["wrong_ratio"] <= cfg["review_wrong_max"]
        oks.append(review_ok)
        review = rv | {"ok": review_ok}
    verdict = "passed" if all(x is not False for x in oks) else "failed"
    rep.update(review=review, verdict=verdict, judged_at=B.now_iso())
    written = [B.write(batch, "report.json", rep), _write_md(batch)]
    B.set_state(batch, verdict)
    written.append(B.bdir(batch) / "state.json")
    return written


LABELS = {"none_ratio": "의도 없음 비율", "calendar_shortcut": "캘린더 지름길", "label_agreement": "두 라벨 일치",
          "b_recall": "B 유형 다시 읽힘", "w_seen": "W 유형 읽힘", "per_intent": "기대의 절반 미만인 의도 수",
          "checks": "확인용 사례 틀림"}


def _fmt(v):
    return "—" if v is None else (f"{v:.1%}" if isinstance(v, float) else str(v))


def _write_md(batch: str):
    rep = B.read(batch, "report.json")
    m = rep["metrics"]
    lines = [f"# 품질 관문 · {batch}", "", f"판정: **{rep.get('verdict') or '사람 검수 기다리는 중'}**", "",
             "| 지표 | 값 | 기준 | 통과 |", "| --- | --- | --- | --- |"]
    for k, it in m["items"].items():
        tgt = it.get("target")
        tgt_s = "—" if tgt is None else f"{_fmt(tgt[0])} ~ {_fmt(tgt[1])}"
        ok = {True: "✅", False: "❌", None: "—"}[it["ok"]]
        lines.append(f"| {LABELS[k]} | {_fmt(it['value'])} | {tgt_s} | {ok} |")
    rv = rep.get("review")
    if rv:
        lines.append(f"| 사람 검수 틀림 | {_fmt(rv['wrong_ratio'])} ({rv['wrong']}/{rv['judged']}) | ~ 20% | "
                     f"{'✅' if rv['ok'] else '❌'} |")
    lines += ["", f"- 주문에 안 보이는 의도: {m['unseen_intent']}건 · unclassified: {m['unclassified']}건"]
    if m["items"]["per_intent"]["low"]:
        lines.append(f"- 모자란 의도: {', '.join(m['items']['per_intent']['low'])}")
    if m["frequent_splits"]:
        lines += ["", "자주 갈리는 의도 쌍", ""] + [f"- {p}: {n}건" for p, n in m["frequent_splits"]]
    p = B.bdir(batch) / "report.md"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p
