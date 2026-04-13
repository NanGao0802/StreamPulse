# 第五层：展示层（frontend/）

> 对应架构层级：**展示层**  
> 核心技术：Vue 3 · Vite · Apache ECharts 5 · 原生CSS（Glassmorphism）  
> 核心文件：`frontend/src/App.vue`、`frontend/src/api/dashboard.js`

---

## 待学习内容清单

- [x] Vue 3 组合式 API（`setup`、`ref`、`computed`、`watch`）与选项式API的区别
- [x] `onMounted` / `onBeforeUnmount` 生命周期钩子的作用
- [x] `nextTick()` 在图表渲染时为什么必要
- [x] ECharts 图表的初始化流程（`init` → `setOption`）
- [x] `DataZoom` 组件如何实现图表缩放
- [x] `VITE_API_BASE` 环境变量如何注入到前端代码
- [x] `dashboard.js` 中 `buildUrl()` 和 `request()` 的封装逻辑
- [x] 情感标签（positive/negative/neutral）如何映射为中文显示和颜色样式
- [x] Glassmorphism 毛玻璃风格的 CSS 实现原理
- [x] 响应式布局：Grid 在不同屏幕宽度下如何自适应

---

## 目录结构

```
frontend/
├── src/
│   ├── App.vue           # 主应用组件（单页应用，全部UI都在这里）
│   ├── api/
│   │   └── dashboard.js  # API请求封装层
│   ├── main.js           # 应用入口
│   └── main.css          # 全局样式
├── public/
│   └── favicon.ico
├── .env.development      # 开发环境变量（VITE_API_BASE）
├── index.html
├── vite.config.js
└── package.json
```

---

## 页面功能区块

### 顶部控制栏（hero区）
- **话题选择**：下拉框，自动加载所有可用话题（`getTopics()`）
- **分析粒度切换**：按天 / 按小时（`segmented` 按钮组）
- **时间区间选择**：日期桶 / 小时桶下拉框
- **刷新数据**按钮：触发 `refreshAll()`

### KPI 指标卡（6个）

| 卡片 | 数据来源 |
|------|---------|
| 当前话题 | `selectedTopic`（本地状态） |
| 累计评论数 | `summary.total_comment_count` |
| 累计热度 | `summary.total_heat_sum` |
| 最新日评论数 | `summary.latest_daily_comment_count` |
| 最新小时负面占比 | `summary.latest_hourly_negative_ratio` |
| 累计告警数 | `summary.alert_count` |

### 趋势图表区（ECharts）

| 图表 | 数据 | 颜色 |
|------|------|------|
| 评论数 & 负面评论数 | `comment_count` + `negative_count` | 蓝色 + 红色 |
| 负面占比趋势 | `negative_ratio × 100` | 紫色 |
| 热度趋势（全宽） | `heat_sum` | 天蓝色 |

### 评论明细表
- **按天模式**：当日 Top30（按热度降序，Silver层读取）
- **按小时模式**：该小时全量（最多2000条）
- 情感标签颜色：正向=绿色 / 负向=红色 / 中性=灰色

### 告警记录面板
- 展示当前话题最近30条告警
- 每条含：触发时间、统计小时、评论数、负面占比、Top3负面评论样例

---

## API 调用层（dashboard.js）

```javascript
const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:18000"

// 封装的4个导出函数
export function getTopics()           // GET /api/topics
export function getDashboard(params)  // GET /api/dashboard
export function getComments(params)   // GET /api/comments
export function getAlerts(params)     // GET /api/alerts
```

---

## 关键 Vue 状态

```javascript
const selectedTopic = ref("")        // 当前选中话题
const granularity = ref("daily")     // 分析粒度：daily/hourly
const selectedBucket = ref("")       // 当前选中时间桶
const dashboard = ref({...})        // 趋势数据（meta/summary/daily/hourly/alerts）
const comments = ref({...})         // 评论明细数据
const alerts = ref({...})           // 告警记录数据
```

**计算属性：**
```javascript
const summary = computed(() => dashboard.value.summary)
const trendRows = computed(() =>
    granularity.value === "daily"
        ? dashboard.value.daily.data
        : dashboard.value.hourly.data
)
```

---

## 数据刷新流程

```
refreshAll()
  ├── loadTopics()      → getTopics()      → 填充话题下拉框
  ├── loadDashboard()   → getDashboard()   → 更新趋势图数据
  ├── loadAlerts()      → getAlerts()      → 更新告警面板
  ├── loadComments()    → getComments()    → 更新评论明细表
  └── nextTick() → renderCharts()          → 重新渲染ECharts图表
```

---

## 样式关键点

### Glassmorphism 毛玻璃效果

```css
background: rgba(255, 255, 255, 0.78);
border: 1px solid rgba(148, 163, 184, 0.14);
backdrop-filter: blur(18px);
box-shadow: 0 18px 40px rgba(15, 23, 42, 0.07);
```

### 情感标签颜色

```css
.emotion-tag.positive { background: linear-gradient(135deg, #16a34a, #22c55e); }
.emotion-tag.neutral  { background: linear-gradient(135deg, #64748b, #94a3b8); }
.emotion-tag.negative { background: linear-gradient(135deg, #dc2626, #ef4444); }
```

---

## 本地开发

```powershell
cd frontend
npm install
npm run dev    # 默认访问 http://localhost:5173
```

API地址配置：`frontend/.env.development`
```
VITE_API_BASE=http://127.0.0.1:18000
```

---

## 1. Vue 3 组合式 API（Composition API）

本项目 `App.vue` 使用 Vue 3 的组合式 API（`<script setup>` 语法糖），与 Vue 2 的选项式 API 对比：

```javascript
// ❌ Vue 2 选项式 API（Options API）
export default {
    data() { return { selectedTopic: "" } },
    computed: { summary() { ... } },
    methods: { loadDashboard() { ... } },
    mounted() { this.loadDashboard() }
}

// ✅ Vue 3 组合式 API（本项目用法）
import { ref, computed, onMounted } from "vue"

const selectedTopic = ref("")        // 响应式变量（等价于 data）
const summary = computed(() => ...)  // 计算属性
function loadDashboard() { ... }     // 方法（直接定义函数）
onMounted(() => { loadDashboard() }) // 生命周期钩子
```

**组合式 API 的优势：** 逻辑可以按功能聚合（不必分散在 data/methods/computed），便于封装和复用。

---

## 2. 响应式状态（ref）

```javascript
// 基础状态
const selectedTopic  = ref("")          // 当前选中话题
const granularity    = ref("daily")     // 粒度："daily" 或 "hourly"
const selectedBucket = ref("")          // 选中的时间桶（日期或小时）
const loading        = ref(false)       // 加载中状态
const errorMsg       = ref("")          // 错误信息

// 数据状态
const dashboard = ref({                 // /api/dashboard 响应
    meta: { topic: "", available_topics: [], hourly_range: {}, daily_range: {} },
    summary: { total_comment_count: 0, total_heat_sum: 0, alert_count: 0, ... },
    daily:   { rows: 0, data: [] },
    hourly:  { rows: 0, data: [] },
    alerts:  { rows: 0, data: [] }
})
const comments = ref({ rows: 0, data: [] })
const alerts   = ref({ rows: 0, data: [] })
```

`ref()` 将普通值包装成响应式对象。模板中访问 `selectedTopic` 会自动解包（不需要写 `.value`），JS 代码中需要写 `selectedTopic.value`。

---

## 3. 计算属性（computed）

```javascript
// 当前汇总数据
const summary = computed(() => dashboard.value.summary)

// 当前粒度对应的趋势数据行（daily或hourly切换）
const trendRows = computed(() =>
    granularity.value === "daily"
        ? dashboard.value.daily.data
        : dashboard.value.hourly.data
)

// 时间桶选项（下拉框选项列表）
const bucketOptions = computed(() => {
    if (granularity.value === "daily") {
        // 从 trendRows 提取所有 stat_date，去重排序
        return [...new Set(trendRows.value.map(r => r.stat_date))].sort().reverse()
    } else {
        return [...new Set(trendRows.value.map(r => r.stat_hour))].sort().reverse()
    }
})
```

计算属性会自动追踪依赖的 `ref`，依赖变化时自动重新计算，不需要手动更新。

---

## 4. 生命周期钩子

```javascript
// onMounted：组件挂载后执行（DOM已就绪）
onMounted(async () => {
    await loadTopics()      // 加载话题列表
    await loadDashboard()   // 加载仪表盘数据
    await nextTick()        // 等待DOM更新
    renderCharts()          // 初始化 ECharts 图表
    window.addEventListener("resize", handleResize)  // 监听窗口大小变化
})

// onBeforeUnmount：组件卸载前执行（清理资源）
onBeforeUnmount(() => {
    window.removeEventListener("resize", handleResize)  // 移除事件监听
    chartInstances.forEach(chart => chart.dispose())    // 销毁 ECharts 实例，防止内存泄漏
})
```

---

## 5. nextTick() 的作用

```javascript
// 问题场景：
selectedTopic.value = "新话题"      // 修改状态
renderCharts()                      // ❌ 此时DOM还没更新，ECharts找不到容器

// 正确做法：
selectedTopic.value = "新话题"
await nextTick()                    // ✅ 等Vue将数据变更同步到DOM
renderCharts()                      // ✅ DOM已更新，可以安全操作
```

Vue 的 DOM 更新是**异步**的（批量异步更新队列），`nextTick()` 返回一个 Promise，在下一次 DOM 更新刷新后 resolve。

---

## 6. API 调用层（dashboard.js）

```javascript
// frontend/src/api/dashboard.js
const API_BASE = (import.meta.env.VITE_API_BASE || "http://127.0.0.1:18000").replace(/\/$/, "")

// 构建完整 URL（含查询参数）
function buildUrl(path, params = {}) {
    const url = new URL(`${API_BASE}${path}`)
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== "") {
            url.searchParams.append(key, value)
        }
    })
    return url.toString()
}

// 统一请求函数：GET + JSON解析 + 错误处理
async function request(path, params = {}) {
    const url = buildUrl(path, params)
    console.log("[API REQUEST]", url)         // 调试日志，答辩时可在浏览器控制台看到

    const res = await fetch(url, { method: "GET" })
    const text = await res.text()

    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`)

    try {
        return JSON.parse(text)
    } catch (e) {
        throw new Error(`JSON parse error: ${text}`)
    }
}

// 4个导出函数
export function getTopics()          { return request("/api/topics") }
export function getDashboard(params) { return request("/api/dashboard", params) }
export function getComments(params)  { return request("/api/comments", params) }
export function getAlerts(params)    { return request("/api/alerts", params) }
```

**`import.meta.env.VITE_API_BASE`：** Vite 的环境变量注入机制，构建时将 `.env.development` 中的 `VITE_API_BASE=http://127.0.0.1:18000` 替换进代码，打包后变成硬编码字符串。

---

## 7. 数据刷新完整流程

```javascript
async function refreshAll() {
    loading.value = true
    errorMsg.value = ""
    try {
        await loadTopics()    // 1. 获取话题列表 → 填充下拉框
        await loadDashboard() // 2. 获取仪表盘数据 → 更新KPI卡 + 趋势数据
        await loadAlerts()    // 3. 获取告警记录 → 更新告警面板
        await loadComments()  // 4. 获取评论明细 → 更新评论表格
        await nextTick()
        renderCharts()        // 5. 重绘全部ECharts图表
    } catch (e) {
        errorMsg.value = e.message
    } finally {
        loading.value = false
    }
}

async function loadDashboard() {
    const result = await getDashboard({
        topic:        selectedTopic.value,
        daily_limit:  365,
        hourly_limit: 720,
        alerts_limit: 20,
    })
    dashboard.value = result

    // 自动设置默认选中的时间桶（最新日期/小时）
    const opts = bucketOptions.value
    if (opts.length > 0 && !selectedBucket.value) {
        selectedBucket.value = opts[0]
    }
}
```

---

## 8. ECharts 图表渲染

```javascript
function renderCharts() {
    renderCommentTrendChart()   // 图表1：评论数 + 负面评论数（双Y轴）
    renderNegRatioChart()       // 图表2：负面占比折线
    renderHeatTrendChart()      // 图表3：热度趋势（全宽）
}

function renderCommentTrendChart() {
    const el = document.getElementById("chart-comment-trend")
    if (!el) return

    // 初始化或复用已有实例
    if (!commentChart) {
        commentChart = echarts.init(el)
    }

    const rows = trendRows.value
    const xAxis = rows.map(r => granularity.value === "daily" ? r.stat_date : r.stat_hour)

    commentChart.setOption({
        tooltip: { trigger: "axis" },
        legend: { data: ["评论数", "负面评论数"] },
        dataZoom: [{ type: "inside" }, { type: "slider" }],  // 支持缩放和平移
        xAxis: { type: "category", data: xAxis },
        yAxis: [
            { type: "value", name: "评论数" },
            { type: "value", name: "负面占比", axisLabel: { formatter: "{value}%" } },
        ],
        series: [
            {
                name: "评论数",
                type: "bar",
                data: rows.map(r => r.comment_count),
                itemStyle: { color: "#3b82f6" },
            },
            {
                name: "负面评论数",
                type: "line",
                yAxisIndex: 1,
                data: rows.map(r => Math.round(r.negative_ratio * 100)),
                itemStyle: { color: "#ef4444" },
            },
        ],
    })
}

// 窗口大小变化时自动调整图表尺寸
function handleResize() {
    [commentChart, negRatioChart, heatChart].forEach(c => c && c.resize())
}
```

---

## 9. 情感标签映射

```javascript
// 情感标签 → 中文显示
function formatSentiment(label) {
    const map = {
        "positive": "正向",
        "negative": "负向",
        "neutral":  "中性",
    }
    return map[label] || label
}
```

```css
/* 情感标签颜色（CSS class） */
.emotion-tag.positive {
    background: linear-gradient(135deg, #16a34a, #22c55e);   /* 绿色渐变 */
    color: white;
}
.emotion-tag.neutral {
    background: linear-gradient(135deg, #64748b, #94a3b8);   /* 灰色渐变 */
    color: white;
}
.emotion-tag.negative {
    background: linear-gradient(135deg, #dc2626, #ef4444);   /* 红色渐变 */
    color: white;
}
```

模板中的用法：

```html
<span :class="['emotion-tag', comment.sentiment_label]">
    {{ formatSentiment(comment.sentiment_label) }}
</span>
```

---

## 10. Glassmorphism 毛玻璃效果 CSS

```css
/* 卡片/面板的毛玻璃效果 */
.card {
    background: rgba(255, 255, 255, 0.78);       /* 半透明白色背景 */
    border: 1px solid rgba(148, 163, 184, 0.14); /* 极淡边框 */
    backdrop-filter: blur(18px);                 /* 背景模糊（毛玻璃核心） */
    -webkit-backdrop-filter: blur(18px);          /* Safari兼容 */
    box-shadow: 0 18px 40px rgba(15, 23, 42, 0.07);  /* 柔和阴影 */
    border-radius: 16px;
}

/* 整体背景：渐变色 */
body {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
}
```

**毛玻璃效果实现原理：**
1. `background: rgba(...)` 让卡片背景半透明
2. `backdrop-filter: blur(18px)` 对卡片**背后**的内容做模糊处理
3. 两者组合产生"透过磨砂玻璃看背景"的视觉效果

---

## 11. 格式化工具函数

```javascript
// 负面占比：0.382 → "38.2%"
function formatPercent(val) {
    if (val === null || val === undefined) return "-"
    return (val * 100).toFixed(1) + "%"
}

// 大数字格式化：12345 → "1.2万"
function formatNumber(val) {
    if (!val && val !== 0) return "-"
    if (val >= 10000) return (val / 10000).toFixed(1) + "万"
    return val.toLocaleString()
}

// 热度格式化：保留2位小数
function formatHeat(val) {
    if (val === null || val === undefined) return "-"
    return Number(val).toFixed(2)
}

// 时间格式化：去掉多余的 .000 等
function formatTime(val) {
    if (!val) return "-"
    return String(val).replace(/\.000$/, "")
}
```

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
