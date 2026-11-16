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
