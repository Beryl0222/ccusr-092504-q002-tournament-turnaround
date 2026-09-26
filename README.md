# 连续赛事场地转换中枢

面向宁波网球中心"女子公开赛结束当天衔接男子挑战赛资格赛"的转换调度内核：参赛名单、退赛递补、签表阶段、比赛依赖、训练申请、场地配置、屋盖设备、人员班次、安保分区全部形成**带版本的时间承诺**；排程建议只有经各赛事值班长确认后才占用资源，重启后沿事件流完整恢复。

## 核心规则

- **两阶段占用**：排程建议（PROPOSED）不硬占用；值班长确认（CONFIRMED）才正式占用。两个团队并发确认同一时段时，聚合版本乐观锁保证只有一方成功，失利方按"窗口开始时间 → 赛事优先级 → 到达序号"进入可解释的候补队列，资源释放后依序推进。
- **确认截止**：建议可带 deadline；超截止未确认（含服务重启恢复时）自动释放并推进候补，重启沿用事件中记录的原截止时间，不会因重启重新计时。
- **重排冻结**：降雨/超时只能重排以指定比赛为根的依赖链上**尚未开赛**的比赛；已结束成绩、进行中比赛、已锁定的球员实际动线不可改写。重排自动终止旧承诺、推进原场地候补，且不得强占其他赛事已确认的场地。
- **递补按当时签表规则**：资格赛阶段的退赛只能由资格赛候补递补，正赛阶段同理——不得借递补越过资格赛；已占有签位者不得重复占位。无合格候补时空缺签位进入交接清单。
- **配置转换即场地占用**：电子司线拆换、空调分区切换、球员通道转换登记为该球场 COURT 资源上的承诺（与比赛同一资源命名空间）。末轮若被重排进原拆换窗，会被已确认的转换作业挡住，从机制上杜绝"同一片场地同时被两项赛事占用"；作业未检查前可用 `delay_config_change` 改期。

## 目录

- `contracts/domain.schema.json`：领域事件信封与已登记的事件/聚合类型。
- `data/sample.json`：中文联调样例。
- `src/tournament_turnaround/`
  - `contracts.py`：不依赖第三方包的信封校验器。
  - `events.py`：事件类型登记与事件构造（强制时区、正整数版本）。
  - `store.py`：仅追加 JSONL 存储，按聚合版本的乐观并发追加，支持重放恢复。
  - `model.py`：资源、承诺、名单、签表阶段、比赛、场地配置等值对象。
  - `rules.py`：区间重叠、候补排序、递补资格、依赖链与可重排/阻塞判定（纯函数）。
  - `hub.py`：`TurnaroundHub` 调度中枢（鉴权、两阶段占用、候补、截止、递补、重排、重启恢复）。
  - `handover.py`：交接清单——每块场地何时从哪种配置转为下一种、谁完成检查、哪些比赛仍受前序结果/设备/配置阻塞、待确认建议与候补顺序。
- `examples/turnover_demo.py`：女子公开赛末轮 → 男挑资格赛的端到端演示。
- `tests/`：契约校验、并发仲裁、候补顺序、截止重启、递补规则、重排冻结与端到端交接清单。

## 快速体验

```bash
python3 examples/turnover_demo.py
```

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```

## 典型流程

```python
hub.register_tournament("WTO", "宁波女子公开赛", ["张值班长"], priority=1)
hub.register_resource("C1", ResourceKind.COURT, "中心球场")

proposal = hub.schedule_match(
    "wom-final", "WTO", "女单末轮", "C1",
    TimeWindow(t(10), t(13)), deadline=t(8, 30),
)
hub.confirm_commitment(proposal.commitment_id, "张值班长")  # 确认后才占用

change = hub.prepare_config_change(
    "cfg-c1", "MCH", "C1", women_cfg, men_cfg, TimeWindow(t(13), t(14)),
    prepared_by="场务周组长",
)
hub.confirm_commitment(change.commitment_id, "李值班长")
hub.check_config("cfg-c1", "李值班长", True, "电子司线已拆换、空调分区切换")

print(render_text(build_handover(hub)))
```

事件以 JSONL 仅追加落盘；新进程用同一存储构造 `TurnaroundHub` 并调用 `restore()` 即可恢复全部承诺、截止与候补状态。
