# realtimedw_jd098

**分析实时数仓架构师·线上分布漂移的检测的数值分布并标出离群点。**

`realtimedw_jd098` —— 抽取数值做分布分析，用中位数绝对偏差识别实时数仓架构师·线上分布漂移的检测上的离群点

- 零依赖，Python 3.10+ 标准库即可运行
- 单文件，`realtimedw_jd098.py` 拷走就能用
- 不联网、不写文件

## 对应的岗位

- 目标岗位：**百川智能 · 中级实时数仓架构师**
- 技能主线：Flink、Kafka、Spark
- 领域标签：数据平台
- 研究方向：实时数仓架构师·线上分布漂移的检测

工具里的规则表就是照着上面这条岗位要求建的——每条规则都挂在技能线或领域标签上，命中时能指回具体要求。

<!-- ipos:install -->
## 安装

零依赖，Python 3.10+ 即可：

```bash
pip install bryantliak-realtimedw_jd098
```

装完直接用命令名调用：

```bash
realtimedw_jd098 --help
```

也可以不用安装，直接把 `realtimedw_jd098.py` 拷走用——它只依赖标准库。

## 为什么需要它

用中位数绝对偏差而不是标准差，离群点本身不会把阈值带偏

它遵守下面几条自己定的规矩：

1. **只报告能定位到具体位置的结论**，不做语义猜测。
2. **规则表公开**，每条命中都能对应到岗位要求的一条明示规则。
3. **输入为空或格式不对时明确报错**，不静默返回空结果。

## 用法

```bash
python realtimedw_jd098.py --file input.txt
python realtimedw_jd098.py --text "示例内容"
python realtimedw_jd098.py --file input.txt --json
```

加 `--json` 可以拿到机器可读的结果，便于接到流水线里。

## 已知局限

- 分箱是等宽或等频，样本太少时结论不稳。
- 不做显著性检验。

## 跑测试

```bash
python test_realtimedw_jd098.py
```

测试用独立的断言与临时数据，不污染工作目录。

## 来源与参考

- Python 标准库文档：<https://docs.python.org/3/>
- GitHub 仓库：<https://github.com/bryantliak/realtimedw_jd098>

---

## 关于作者

bryantliak 做实时数仓架构师·线上分布漂移的检测相关的问题，方向对标百川智能的中级实时数仓架构师岗位。这个工具先替你回答一个具体问题，再谈别的。

- 个人主页：<https://bryantliak.github.io>
- 仓库：<https://github.com/bryantliak/realtimedw_jd098>

## 许可

MIT，见 [LICENSE](LICENSE)。
