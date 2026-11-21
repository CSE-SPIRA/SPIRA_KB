"""라벨 작업 묶음 맞추기 (spira fix-labels).

라벨 작업의 사례 묶음은 처음 만들 때 고정 시드로 나눈 묶음과 같아야 한다.
처음 묶음과 다른 작업을 찾아서
- 그 작업의 결과를 discarded/로 옮기고
- 처음 묶음으로 작업을 다시 대기열에 넣고
- 그 배치의 비교 · 보고서 · 사람 검수 파일을 지운다 (바뀐 비교 결과로 검수를 새로 받는다)
- 배치를 labeling으로 되돌린다. 라벨이 다 모이면 자동 진행이 다시 비교 · 검수 파일 · 판정을 한다
통과(passed)했거나 적재(loaded)된 배치는 건드리지 않는다 (DB에 이미 들어간 줄과 섞이지 않게).
"""

from __future__ import annotations

import shutil

from spira import batches as B
from spira import config
from spira.queue import DATA
from spira.steps.assemble_orders import make_label_jobs, original_chunks


def wrong_jobs(batch: str) -> list[str]:
    lm = B.read(batch, "label_map.json")
    if not lm:
        return []
    cases = B.read(batch, "cases.json")
    chunks = original_chunks(batch, cases)
    bad = []
    for job_id, j in lm["jobs"].items():
        n = int(job_id.rsplit("_", 1)[1])
        if j["cases"] != [c["case_ref"] for c in chunks[n - 1]]:
            bad.append(job_id)
    return bad


LOCKED = {"passed", "loaded"}


def blocker(batch: str) -> str | None:
    """고치면 안 되는 이유. 없으면 None."""
    cycle, _, _ = B.parse(batch)
    bad = wrong_jobs(batch)
    if not bad:
        return None
    if B.state(batch) in LOCKED:
        return f"{batch}: 이미 {B.state(batch)} 상태라 건드리지 않습니다"
    busy = [j for j in bad if (DATA / cycle / "running" / f"{j}.json").exists()]
    if busy:
        return f"{batch}: 지금 누가 하고 있는 작업이 있습니다 ({', '.join(busy)}). 끝난 뒤 다시 실행하세요"
    return None


def fix(batch: str) -> tuple[list[str], list]:
    """(다시 넣은 작업, 바뀐 파일). blocker()가 None인 배치에만 부른다."""
    cycle, _, _ = B.parse(batch)
    bad = wrong_jobs(batch)
    if not bad:
        return [], []
    if (why := blocker(batch)):
        raise RuntimeError(why)
    written = []
    ddir = DATA / cycle / "discarded"
    ddir.mkdir(parents=True, exist_ok=True)
    for job_id in bad:
        for kind in ("results", "failed"):
            src = DATA / cycle / kind / f"{job_id}.json"
            if src.exists():
                dst = ddir / f"{job_id}.wrongcases.json"
                shutil.move(src, dst)
                written += [src, dst]
        err = DATA / cycle / "failed" / f"{job_id}.error.txt"
        if err.exists():
            err.unlink()
            written.append(err)
    for name in ("compared.json", "report.json", "report.md", "review_plan.json"):
        f = B.bdir(batch) / name
        if f.exists():
            f.unlink()
            written.append(f)
    for f in (config.ROOT / "review").glob(f"*/{batch}_*.csv"):
        f.unlink()
        written.append(f)
    written += make_label_jobs(batch, B.read(batch, "cases.json"), only=bad)
    if B.state(batch) != "labeling":
        B.set_state(batch, "labeling", note=f"라벨 작업 묶음 바로잡음: {', '.join(bad)}")
        written.append(B.bdir(batch) / "state.json")
    return bad, written
