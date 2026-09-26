# 连续赛事场地转换中枢

宁波网球中心女子公开赛收官日衔接男子挑战赛资格赛的运行中枢。参赛名单与退赛递补、
签表阶段、比赛依赖、训练申请、场地配置、屋盖与设备维护、人员班次、安保分区统一建模为
**带版本的时间承诺**，排程建议只有经各赛事值班长确认后才占用资源。

## 核心规则

- **先建议、后确认**：任何排程都先落为 `PROPOSED` 提案（带 `confirm_by` 截止时间），
  赛事值班长确认后才产生 `RESOURCE_HELD` 占用；值班长按赛事授权，不能跨赛事确认。
- **并发确认只有一方成功**：确认提案与资源持有在同一原子批次提交，资源流版本构成互斥锁。
  两个团队同时抢同一时段，一方成功，落败方自动进入该资源的候补队列。
- **候补顺序可解释**：按优先级（数字小者优先）、同优先级按进入候补的先后；
  资源释放或设备恢复后自动推进队首中第一个可服务者，其余留队不乱序。
- **降雨/超时只改未来**：`revise_for_disruption` 仅重排尚未开赛的比赛及其未开赛后继依赖链；
  已开赛/已完赛比赛、完赛成绩、球员实际动线只追加不改写。旧承诺标记 `SUPERSEDED`
  并释放占用，新承诺仍需值班长确认，不能借重排直接占场。
- **名单变更按当时签表规则**：签表未锁定时退赛由排名最高候补升入直接名单；
  锁定后退赛以幸运失败者身份递补；资格赛槽位只能由资格赛比赛结果填入，
  任何路径都不得手工指派或让一名球员重复占两个槽位。
- **重启沿用原截止时间**：所有状态（含截止时间、待确认安排、占用、签表事实）都由事件流折叠，
  重启重放 JSONL 事件日志即可恢复，截止时间口径不变。
- **交接清单**：随时可生成每块场地"何时从哪种配置转为下一种配置、谁完成检查"的时间线，
  以及仍受前序结果、设备状态、签表空槽、未确认承诺阻塞的比赛及具体原因。

## 事件溯源结构

| 文件 | 职责 |
| --- | --- |
| `contracts/domain.schema.json` | 领域事件信封、8 类聚合、28 类事件的登记枚举 |
| `src/tournament_turnaround/events.py` | `Event`、内存/JSONL 事件存储、流版本与乐观并发、原子批写 |
| `src/tournament_turnaround/state.py` | 事件折叠出的中枢状态：名单、签表槽位、比赛依赖、提案、候补、设备、配置转换 |
| `src/tournament_turnaround/hub.py` | 命令层：登记、提案/确认/驳回/失效、候补推进、退赛递补、降雨重排、检查 |
| `src/tournament_turnaround/handover.py` | 交接清单只读投影（配置时间线、待确认承诺、阻塞比赛、候补顺序、球员动线） |
| `src/tournament_turnaround/contracts.py` | 不依赖第三方包的事件信封基础校验器 |
| `data/sample.json` | 中文联调样例（值班长确认时间承诺） |
| `tests/` | 23 项测试：契约边界、并发互斥、候补顺序、递补规则、不可变性、重启截止时间、交接清单 |

### 聚合与事件

聚合：`tournament`、`roster`、`draw`、`match`、`venue_resource`、`proposal`、
`player`、`turnaround_plan`（均按赛事/资源/比赛标识分流，流内版本严格递增）。

关键事件链：

```
PROPOSAL_SUBMITTED ──值班长确认──▶ PROPOSAL_CONFIRMED + RESOURCE_HELD
        │                              │
        ├─ 冲突/落败 ─▶ WAITLIST_JOINED ┴─ 释放 ─▶ VENUE_RELEASED ─▶ WAITLIST_ADVANCED
        ├─ 超时 ─────▶ PROPOSAL_EXPIRED（截止时间来自事件本身，重启不变）
        └─ 重排 ─────▶ 旧: VENUE_RELEASED + PROPOSAL_SUPERSEDED；新: PROPOSAL_SUBMITTED + SCHEDULE_REVISED

ENTRY_CONFIRMED ─退赛─▶ ENTRY_WITHDRAWN (+SLOT_VACATED) ─按签表阶段─▶ ALTERNATE_PROMOTED + SLOT_FILLED
MATCH_STARTED ─▶ MATCH_COMPLETED（资格赛完赛是 QUALIFIER 槽位被填入的唯一路径）
```

### 持久化与重启

`JsonlEventStore` 每行是一次原子提交（一个事件批），`fsync` 后返回；
用同一文件路径重新构造 `TurnaroundHub(JsonlEventStore(path))` 即完成重放恢复，
没有第二份状态存储。

## 最小用法

```python
from datetime import datetime, timedelta, timezone
from tournament_turnaround import TurnaroundHub, build_handover

hub = TurnaroundHub()
hub.register_tournament("ATP-CHQ", "男子挑战赛资格赛", ["atp-duty-1"])
hub.register_resource("court-1", "court", "中心场", "ATP 资格赛配置")
hub.set_match("atp-q1", "ATP-CHQ", "QUALIFYING", "QR1",
              resource_id="court-1", requires_equipment=["电子司线"])

start = datetime(2026, 9, 27, 19, tzinfo=timezone(timedelta(hours=8)))
prop = hub.submit_proposal(
    "MATCH", "ATP-CHQ", "court-1",
    start, start + timedelta(hours=1, minutes=30),
    "资格赛 QR1", confirm_by=start - timedelta(minutes=30),
    match_id="atp-q1", requires_equipment=["电子司线"])
hub.confirm_proposal(prop, "atp-duty-1")          # 确认后才占用 court-1
hub.revise_for_disruption("atp-q1", "RAIN", start + timedelta(hours=3))  # 只能改未开赛链

print(build_handover(hub.state, datetime.now(timezone.utc)).to_dict())
```

## 测试

```bash
python3 -m unittest discover -s tests
```

覆盖要点：两团队并发确认同一时段仅一方成功并进候补、释放后按优先级推进、
锁定前后退赛递补差异、资格赛槽位不可手工填入、完赛成绩与球员动线不可改写、
后继链整体顺延而已完赛前序不动、重启后沿用原截止时间失效、交接清单 JSON 可序列化。

## 编译检查

```bash
python3 -m compileall -q src tests
```
