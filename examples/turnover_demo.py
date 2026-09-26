"""端到端联调演示：宁波女子公开赛末轮 → 男子挑战赛资格赛的转换日。

运行：python3 examples/turnover_demo.py
使用内存外的临时事件文件，结束时打印三个时点的交接清单。
"""

from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tournament_turnaround import (  # noqa: E402
    CommitmentState,
    DrawStage,
    JsonlEventStore,
    ResourceKind,
    TimeWindow,
    TurnaroundHub,
    VenueConfig,
    build_handover,
    render_text,
)

TZ = timezone(timedelta(hours=8))


def t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 27, hour, minute, tzinfo=TZ)


def main() -> None:
    path = Path(tempfile.mkdtemp()) / "turnover.jsonl"
    clock = [t(7, 0)]
    hub = TurnaroundHub(JsonlEventStore(path), clock=lambda: clock[0])
    hub.restore()

    # ---- 两个赛事值班团队与场地/设备 ----
    hub.register_tournament("WTO", "宁波女子公开赛", ["张值班长"], priority=1)
    hub.register_tournament("MCH", "男子挑战赛", ["李值班长"], priority=2)
    hub.register_resource("C1", ResourceKind.COURT, "中心球场")
    hub.register_resource("C2", ResourceKind.COURT, "二号球场")
    hub.register_resource("ROOF", ResourceKind.EQUIPMENT, "可开启屋盖")
    hub.set_equipment_status("ROOF", True, "屋盖收合正常")

    women_cfg = VenueConfig("女子赛配置", electronic_lines=True, hvac_zone="看台A区", player_channel="球员通道A")
    men_cfg = VenueConfig("男挑赛配置", electronic_lines=False, hvac_zone="全看台分区", player_channel="球员通道B")

    # ---- 名单：男挑资格赛 + 一名候补 ----
    hub.confirm_entry("m-q1", "MCH", "陈强", stage=DrawStage.QUALIFYING, occupies_slot="Q1")
    hub.confirm_entry("m-q2", "MCH", "赵磊", stage=DrawStage.QUALIFYING, occupies_slot="Q2")
    hub.confirm_entry("m-alt", "MCH", "替补孙宁", stage=DrawStage.QUALIFYING)
    hub.set_draw_stage("MCH", DrawStage.QUALIFYING)

    # ---- 排程建议（带确认截止） ----
    final_cmt = hub.schedule_match(
        "wom-final", "WTO", "女单末轮", "C1", TimeWindow(t(10), t(13)),
        config=women_cfg, player_ids=["王欣", "李婷"], requires_equipment=["ROOF"],
        deadline=t(8, 30),
    )
    change_cmt = hub.prepare_config_change(
        "cfg-c1", "MCH", "C1", women_cfg, men_cfg, TimeWindow(t(13), t(14)),
        prepared_by="场务周组长", deadline=t(9),
    )
    pre_cmt = hub.schedule_match(
        "q-pre", "MCH", "资格赛预选", "C2", TimeWindow(t(11, 30), t(13, 30)),
        deadline=t(9),
    )
    q1_cmt = hub.schedule_match(
        "q1", "MCH", "资格赛第1轮", "C1", TimeWindow(t(14), t(16)),
        depends_on=["q-pre"], config=men_cfg, player_ids=["陈强", "赵磊"],
        deadline=t(13, 30),
    )
    hub.publish_shift(
        "shift-teardown", "MCH", "拆换班组甲", TimeWindow(t(13), t(14)),
        role="电子司线/空调拆换", proposed_by="场务周组长", deadline=t(9),
    )
    hub.assign_security_zone(
        "zone-tunnel", "MCH", TimeWindow(t(13), t(16)),
        note="通道B切换安保",
        proposed_by="安保王队长", deadline=t(10),
    )

    # ---- 值班长确认 ----
    clock[0] = t(8, 0)
    hub.confirm_commitment(final_cmt.commitment_id, "张值班长")
    hub.confirm_commitment(change_cmt.commitment_id, "李值班长")
    hub.confirm_commitment(pre_cmt.commitment_id, "李值班长")
    shift_cmt = next(c for c in hub.commitments.values() if c.purpose.startswith("班次 shift-teardown"))
    zone_cmt = next(c for c in hub.commitments.values() if c.purpose.startswith("安保分区 zone-tunnel"))
    hub.confirm_commitment(shift_cmt.commitment_id, "李值班长")
    hub.confirm_commitment(zone_cmt.commitment_id, "李值班长")

    print(render_text(build_handover(hub, now=t(8, 30))))
    print("\n" + "#" * 60 + "\n")

    # ---- 比赛日推进：女网末轮进行，资格赛预选完赛 ----
    clock[0] = t(10, 0)
    hub.start_match("wom-final")
    clock[0] = t(11, 30)
    hub.start_match("q-pre")
    clock[0] = t(12, 55)
    hub.complete_match("q-pre", "6-1 6-2", "陈强")

    # ---- 13:00 拆换、检查、交还，14:00 资格赛第1轮确认开赛 ----
    clock[0] = t(13, 0)
    hub.complete_match("wom-final", "7-6(5) 6-4", "王欣")  # 末轮准点结束
    hub.lock_player_movement("wom-final", "通道A→颁奖区")
    hub.check_config("cfg-c1", "李值班长", True, "电子司线已拆除、空调全分区运行、通道B标识就位")
    hub.release_venue("C1", "张值班长")
    hub.confirm_commitment(q1_cmt.commitment_id, "李值班长")

    print(render_text(build_handover(hub, now=t(13, 45))))


if __name__ == "__main__":
    main()
