# LaTeX 论文工程说明

## 工程结构

```
latex/
├── main.tex                  # 主入口文件（文档类、宏包、正文组织）
├── chapters/
│   ├── 00_abstract.tex       # 中英文摘要
│   ├── 01_introduction.tex   # 第1章 绪论
│   ├── 02_related_work.tex   # 第2章 相关技术与理论基础
│   ├── 03_system_design.tex  # 第3章 系统需求分析与总体设计
│   ├── 04_implementation.tex # 第4章 系统详细设计与实现
│   ├── 05_evaluation.tex     # 第5章 系统测试与实验分析
│   └── 06_conclusion.tex     # 第6章 总结与展望（含致谢）
├── figures/                  # 图片目录（放 .pdf/.png/.jpg）
├── references.bib            # BibTeX 参考文献库（25 条）
└── README.md                 # 本文件
```

---

## 文档类说明

本工程使用 **CTeX 宏集** `ctexrep` 文档类：

```latex
\documentclass[12pt, a4paper, UTF8, zihao=-4, linespread=1.5, openany]{ctexrep}
```

- `ctexrep`：对应标准 LaTeX `report` 类，支持 `\chapter`，适合毕业论文
- `zihao=-4`：正文小四号（12 pt）
- `linespread=1.5`：1.5 倍行距
- 参考文献格式：`gbt7714-numerical`（GB/T 7714-2015 数字引用）

---

## 编译方式

### 推荐：XeLaTeX（完整四步）

```bash
xelatex main.tex
bibtex main
xelatex main.tex
xelatex main.tex
```

> XeLaTeX 原生支持 Unicode 与系统字体，是 CTeX 宏集的首选编译引擎。

### 使用 latexmk 自动化（推荐）

安装 Perl + latexmk 后，一条命令完成全部编译：

```bash
latexmk -xelatex main.tex
```

清除中间文件：

```bash
latexmk -c
```

### VS Code 配置（LaTeX Workshop 插件）

在 `.vscode/settings.json` 中添加：

```json
{
  "latex-workshop.latex.tools": [
    {
      "name": "xelatex",
      "command": "xelatex",
      "args": ["-synctex=1", "-interaction=nonstopmode", "-file-line-error", "%DOC%"]
    },
    {
      "name": "bibtex",
      "command": "bibtex",
      "args": ["%DOCFILE%"]
    }
  ],
  "latex-workshop.latex.recipes": [
    {
      "name": "xelatex -> bibtex -> xelatex*2",
      "tools": ["xelatex", "bibtex", "xelatex", "xelatex"]
    }
  ],
  "latex-workshop.latex.recipe.default": "xelatex -> bibtex -> xelatex*2"
}
```

---

## 环境安装

### Windows（推荐 TeX Live 2024+）

1. 下载 TeX Live 安装器：https://tug.org/texlive/
2. 完整安装（约 7 GB），确保包含 `ctex`、`gbt7714`、`natbib` 宏包
3. 验证安装：`xelatex --version`

### 检查必要宏包

```bash
kpsewhich ctexrep.cls        # CTeX 文档类
kpsewhich gbt7714.sty        # GB/T 7714 参考文献格式（若缺失见下方说明）
```

若 `gbt7714` 不可用，在 `main.tex` 中将参考文献格式改为备选：

```latex
\bibliographystyle{unsrtnat}   % 替代方案（不符合国标，仅调试用）
```

---

## 写作规范说明

| 元素 | LaTeX 写法 |
|------|-----------|
| 行内代码 | `\code{变量名}` 或 `\texttt{...}` |
| 文件路径 | `\filepath{/opt/pipeline/jobs/}` |
| 数学公式 | `equation` 环境，编号用 `\label{eq:xxx}` |
| 三线表 | `booktabs` 宏包：`\toprule` / `\midrule` / `\bottomrule` |
| 代码块 | `lstlisting` 环境，指定 `language=Python` 或 `language=bash` |
| 图片 | `figure` 环境，图片存放于 `figures/` 目录 |
| 引用文献 | `\cite{key}` 或 `\citep{key}` |
| 章节交叉引用 | `\label{sec:xxx}` + `\ref{sec:xxx}` |

---

## 内容迁移进度

章节文件中的 `% TODO:` 注释标记了尚待从 Markdown 版本迁移的段落。
迁移顺序建议：第2章（技术原理）→ 第3章（设计）→ 第4章（实现）→ 其余章节。
