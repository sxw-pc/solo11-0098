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

## 供应商目录抓取与发布

`catalog-register` 把签约合同与供应商网页快照（A 分页 HTML、B JSON-LD）连每页 SHA 冻结进 `catalog_case`，起点不含任何抽取结果。`catalog-extract` 解析两种格式、校验分页链/跨页修订号/页面 SHA/重复与乱序响应，并按合同 `unit_equivalence` 把报价归一到每测试单位（A 冷藏 R 200 分/次、B 180 分/次）；任一校验失败整批待核，已确认目录不会被改成半张目录。

`catalog-candidate --strategy strict_gtin|signed_alias_merge` 生成候选版本与新增/变更/下架差异：`strict_gtin` 以 GTIN 为商品身份分列两个冷藏 R 产品；`signed_alias_merge` 在核验 MOCK-LAB 签署的 GTIN 别名（签名、有效期、与网页制造商/储温一致）后合并为一个规范产品并推荐最低归一单价。室温 R-RT 在两条路线下都单列；身份策略写入版本后不可在同一版本内更换，别名凭证缺失/过期/冲突即隔离并保留上一已发布版本。

`catalog-publish --nonce` 先落可恢复 nonce 意图再提交模拟 LIMS（`samplechain/lims.py`，独立文件模拟远端）：版本栅栏拒绝旧版本覆盖与跳号，nonce 幂等防重复商品。`catalog-recover` 核验远端最终状态：补登丢失/部分 ACK、采纳本地回滚后仍存在于 LIMS 的版本，更正只能以新版本接续。`catalog-report` 输出 JSON/Markdown 审计链：网页 SHA → 分页链 → 抽取字段 → 身份凭证 → 单位换算 → 版本差异 → LIMS 回执。
