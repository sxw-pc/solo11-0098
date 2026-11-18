# 样本链采录台

离线实验室扫码日志的保管链核验。原始事件与来源哈希留在 SQLite，结论关联证据 ID。样本和人员均为虚构代号，不连接外部服务。

## 运行

`python -m samplechain demo` 一条命令初始化、导入、判定并输出报告路径。`python -m samplechain --help` 查看独立命令；`python -m pytest -q` 测试。运行期只用 Python 3.11 标准库。

## 数据与规则

Batch → Sample，Source → Event，Event → Issue，Sample → Decision。采集、移交、接收、入柜、取出、检测需按顺序；封签开封和复封成对，离柜时间超过 30 分钟、温度超 2–8 度、过期或作废阻断检测。扫描设备序号断号及冲突保留问题。报告包含全部输入行、来源哈希、问题和规则版本。

当前单机实现按事件时间排序；无法在缺失外部锚点时判断设备时钟准确性。人工争议处理属于后续业务，不自动推断。

安装检查：在仓库目录运行 `python -m pip install . --no-build-isolation`；本地开发也可直接运行模块命令。报告同时生成 JSON 和 Markdown，`inspect` 子命令可追踪单个对象。

审计台账：Run 由规则版本和来源摘要确定，Evidence 关联原始对象与来源，Decision 引用 Run；重复核验保持同一输入的报告稳定。

原始来源字节保存在 SQLite BLOB 中，JSON 证据包以 Base64 表示字节；修改输入文件不会覆盖已采录证据。

## 纯水供给闭环（独立管路，不判样本）

`samplechain/water_loop.py` 在同库内操作独立的纯水管路与储罐，输出真实合格水量、阀回执与膜寿命，不修改样本可检测性或既有 decision。冻结入口 `register` 仍只写 `water_case`；所有控制表（`water_snapshot`、`water_telemetry`、`water_plan`、`water_command`、`water_meter`、`water_ledger`、`water_delivery`、`water_notice` 等）在首次建快照时惰性创建。

台账规则：体积用整数毫升，电导以 µS/cm×1000 的整数毫单位计，溶质质量 = 电导毫单位×毫升；混水按质量守恒做塞流核算，绝不对两个探头读数直接平均，也不把未观测水质当零。直供膜总容量 8L、每时间窗 4L，冲洗与交付都消耗膜容量并跨窗合并核算；公共管道初始 1L、10 µS/cm 滞留液必须先隔离排空并经 PIPE-1 复测合格才能走直供路径。水质过限或来源冲突只能改向排水/待核，已交付事实不回滚——异常以召回单（`retest_delivery`）和补水单（`makeup_delivery`）处理。

两条预授权路线在执行前冻结、同一案件不可混用，对同一冻结输入产出不同的真实水源/膜寿命/备用库存：

- `direct_first`：t0 直供 1L 冲洗 + 3L 交 W1，t1 直供 2L 交 W2；膜累计 6L、储罐保留 3L。
- `reserve_first`：储罐 3L 交 W1，t1 直供 1L 冲洗后 2L 交 W2；膜累计 3L、储罐余 0L。

版本一致性：传感器校准版本、阀固件版本、订单版本强制绑定同一快照；旧快照的迟到传感行不能覆盖新命令。模拟 PLC（`SimPLC`）提供 nonce/状态查询、ACK 丢失对账、断联、流量计晚到、直供/储罐阀互锁与膜中途失效注入；两工人抢同一订单只有一个 `claim` 成功。

`python -m samplechain water-demo --workdir <目录> --route direct_first|reserve_first` 一条命令跑完冻结→快照→采录→计划→PLC 执行→JSON/Markdown 报告。分步命令：`water-snapshot`、`water-telemetry`、`water-plan`、`water-claim`、`water-run`（可带 `--fault drop_ack,delay_meter`、`--membrane-fail-after-ml`）、`water-naive`（演示只看合格探头的简单补丁如何被拦截排水）、`water-recall`、`water-makeup`、`water-report`。报告含每单合格/待核升数、流入/冲洗/出水/储量的体积与质量守恒、电导证据（含拒收行）、膜状态、命令及 ACK，回执沿 source SHA→水质→阀动作→流量计→订单形成可回放因果链。
