<template>
  <div class="page">
    <div class="page-bg"></div>

    <header class="hero">
      <div class="hero-left">
        <div class="hero-tag">Structured Streaming · Multi-Topic Monitor</div>
        <h1>多话题舆情监测系统</h1>
        <p class="hero-desc">
          面向社交媒体评论流的实时舆情监测平台，支持多话题切换、趋势分析、情绪分类、
          热度评估、评论明细查看与邮件告警联动。
        </p>

        <div class="hero-pills">
          <span class="pill">Kafka</span>
          <span class="pill">Spark Structured Streaming</span>
          <span class="pill">Delta Lake</span>
          <span class="pill">StructBERT</span>
          <span class="pill">FastAPI + Vue</span>
        </div>
      </div>

      <div class="hero-right">
        <div class="control-card">
          <div class="control-grid">
            <div class="field">
              <label>选择话题</label>
              <select v-model="selectedTopic" @change="handleTopicChange">
                <option value="">请选择话题</option>
                <option v-for="item in topicOptions" :key="item.topic" :value="item.topic">
                  {{ item.topic }}（{{ item.total_comment_count }}条）
                </option>
              </select>
            </div>

            <div class="field">
              <label>分析粒度</label>
              <div class="segmented">
                <button
                  :class="{ active: granularity === 'daily' }"
                  @click="switchGranularity('daily')"
                >
                  按天
                </button>
                <button
                  :class="{ active: granularity === 'hourly' }"
                  @click="switchGranularity('hourly')"
                >
                  按小时
                </button>
              </div>
            </div>

            <div class="field">
              <label>{{ granularity === "daily" ? "选择日期" : "选择小时" }}</label>
              <select v-model="selectedBucket" @change="loadComments">
                <option value="">请选择</option>
                <option v-for="item in bucketOptions" :key="item" :value="item">
                  {{ item }}
                </option>
              </select>
            </div>

            <div class="field">
              <label>&nbsp;</label>
              <button class="refresh-btn" @click="refreshAll">刷新数据</button>
            </div>
          </div>

          <div v-if="loading" class="status loading">正在加载数据...</div>
          <div v-if="errorMsg" class="status error">加载失败：{{ errorMsg }}</div>
        </div>
      </div>
    </header>

    <section class="kpi-grid">
      <div class="kpi-card kpi-topic">
        <div class="kpi-title">当前话题</div>
        <div class="kpi-value topic-text">{{ selectedTopic || "-" }}</div>
        <div class="kpi-desc">当前正在分析的舆情主题</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-title">累计评论数</div>
        <div class="kpi-value">{{ summary.total_comment_count ?? 0 }}</div>
        <div class="kpi-desc">话题累计评论总量</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-title">累计热度</div>
        <div class="kpi-value">{{ formatNumber(summary.total_heat_sum ?? 0) }}</div>
        <div class="kpi-desc">点赞、转发与情绪综合计算</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-title">最新日评论数</div>
        <div class="kpi-value">{{ summary.latest_daily_comment_count ?? 0 }}</div>
        <div class="kpi-desc">日期：{{ formatDay(summary.latest_date) }}</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-title">最新小时负面占比</div>
        <div class="kpi-value">{{ formatPercent(summary.latest_hourly_negative_ratio) }}</div>
        <div class="kpi-desc">小时：{{ formatHour(summary.latest_hour) }}</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-title">累计告警数</div>
        <div class="kpi-value">{{ summary.alert_count ?? 0 }}</div>
        <div class="kpi-desc">已触发的预警事件总数</div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>{{ granularity === "daily" ? "按天趋势分析" : "按小时趋势分析" }}</h2>
          <p>
            {{
              granularity === "daily"
                ? "查看最近一年内每天的评论数、负面评论数、负面占比和热度趋势"
                : "查看小时维度下的评论数、负面评论数、负面占比和热度趋势"
            }}
          </p>
        </div>
        <div class="panel-meta">{{ trendRows.length }} 个时间点</div>
      </div>

      <div class="chart-grid">
        <div class="chart-card">
          <div class="chart-title">评论数与负面评论数</div>
          <div ref="countChartRef" class="chart-box"></div>
        </div>

        <div class="chart-card">
          <div class="chart-title">负面占比趋势</div>
          <div ref="ratioChartRef" class="chart-box"></div>
        </div>

        <div class="chart-card full-span">
          <div class="chart-title">热度趋势</div>
          <div ref="heatChartRef" class="chart-box heat-chart"></div>
        </div>
      </div>
    </section>

    <section class="content-grid">
      <section class="panel comments-panel">
        <div class="panel-head">
          <div>
            <h2>{{ granularity === "daily" ? "每日 Top30 评论" : "每小时全部评论" }}</h2>
            <p>
              {{
                granularity === "daily"
                  ? `当前日期：${selectedBucket || "-"}`
                  : `当前小时：${selectedBucket || "-"}`
              }}
            </p>
          </div>
          <div class="panel-meta">{{ comments.rows || 0 }} 条</div>
        </div>

        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>发布时间</th>
                <th>情绪</th>
                <th>情绪分数</th>
                <th>热度</th>
                <th>点赞</th>
                <th>转发</th>
                <th>评论内容</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in comments.data" :key="row.event_id">
                <td>{{ formatTime(row.publish_time) }}</td>
                <td>
                  <span :class="['emotion-tag', row.sentiment_label]">
                    {{ emotionText(row.sentiment_label) }}
                  </span>
                </td>
                <td>{{ formatScore(row.sentiment_score) }}</td>
                <td>{{ formatNumber(row.heat_score) }}</td>
                <td>{{ row.like_count ?? 0 }}</td>
                <td>{{ row.repost_count ?? 0 }}</td>
                <td class="text-cell">{{ row.text }}</td>
              </tr>
              <tr v-if="!comments.data.length">
                <td colspan="7" class="empty-cell">暂无评论数据</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="panel alerts-panel">
        <div class="panel-head">
          <div>
            <h2>告警记录</h2>
            <p>展示当前话题最近触发的负面预警事件</p>
          </div>
          <div class="panel-meta">{{ alerts.rows || 0 }} 条</div>
        </div>

        <div class="alert-list">
          <div v-for="item in alerts.data" :key="item.alert_key" class="alert-card">
            <div class="alert-top">
              <div class="alert-time">{{ formatTime(item.created_at) }}</div>
              <div class="alert-badge">预警</div>
            </div>

            <div class="alert-topic">{{ item.topic }}</div>
            <div class="alert-meta">
              统计小时：{{ item.stat_hour }}
            </div>
            <div class="alert-meta">
              评论数：{{ item.comment_count }}
            </div>
            <div class="alert-meta">
              负面评论数：{{ item.negative_count }}
            </div>
            <div class="alert-meta">
              负面占比：{{ formatPercent(item.negative_ratio) }}
            </div>

            <div
              v-if="item.top_negative_comments && item.top_negative_comments.length"
              class="samples"
            >
              <div class="samples-title">热度最高的负面评论样例</div>
              <div
                v-for="(sample, idx) in item.top_negative_comments"
                :key="idx"
                class="sample-item"
              >
                <div class="sample-meta">
                  {{ idx + 1 }}. {{ sample.publish_time }} ｜ 热度 {{ sample.heat_score }}
                </div>
                <div class="sample-text">{{ sample.text }}</div>
              </div>
            </div>
          </div>

          <div v-if="!alerts.data.length" class="empty-alert">
            当前话题暂无告警记录
          </div>
        </div>
      </section>
    </section>
  </div>
</template>

<script setup>
import * as echarts from "echarts";
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { getAlerts, getComments, getDashboard, getTopics } from "./api/dashboard";

const loading = ref(false);
const errorMsg = ref("");

const topicOptions = ref([]);
const selectedTopic = ref("");
const granularity = ref("daily");
const selectedBucket = ref("");

const dashboard = ref({
  meta: {},
  summary: {},
  daily: { data: [] },
  hourly: { data: [] },
  alerts: { data: [] },
});

const comments = ref({
  rows: 0,
  data: [],
  bucket_options: [],
  selected_bucket: "",
});

const alerts = ref({
  rows: 0,
  data: [],
});

const countChartRef = ref(null);
const ratioChartRef = ref(null);
const heatChartRef = ref(null);

let countChart = null;
let ratioChart = null;
let heatChart = null;

const summary = computed(() => dashboard.value.summary || {});
const trendRows = computed(() =>
  granularity.value === "daily"
    ? (dashboard.value.daily?.data || [])
    : (dashboard.value.hourly?.data || [])
);
const bucketOptions = computed(() => comments.value.bucket_options || []);

const xLabels = computed(() =>
  trendRows.value.map((item) =>
    granularity.value === "daily"
      ? String(item.stat_date || "").slice(0, 10)
      : String(item.stat_hour || "").slice(5, 16)
  )
);

const commentCountData = computed(() =>
  trendRows.value.map((item) => Number(item.comment_count || 0))
);

const negativeCountData = computed(() =>
  trendRows.value.map((item) => Number(item.negative_count || 0))
);

const negativeRatioData = computed(() =>
  trendRows.value.map((item) => Number(((Number(item.negative_ratio || 0)) * 100).toFixed(2)))
);

const heatSumData = computed(() =>
  trendRows.value.map((item) => Number(item.heat_sum || 0))
);

function formatPercent(value) {
  const num = Number(value || 0);
  return `${(num * 100).toFixed(2)}%`;
}

function formatNumber(value) {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return "0";
  return num.toLocaleString();
}

function formatScore(value) {
  return Number(value || 0).toFixed(4);
}

function formatTime(value) {
  if (!value) return "-";
  return String(value).slice(0, 19);
}

function formatDay(value) {
  if (!value) return "-";
  return String(value).slice(0, 10);
}

function formatHour(value) {
  if (!value) return "-";
  return String(value).slice(0, 16);
}

function emotionText(label) {
  if (label === "positive") return "正向";
  if (label === "negative") return "负向";
  return "中性";
}

function baseOption() {
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 48, right: 28, top: 40, bottom: 68 },
    dataZoom: [
      { type: "inside" },
      { type: "slider", height: 18, bottom: 12 },
    ],
    xAxis: {
      type: "category",
      boundaryGap: false,
      axisLabel: {
        color: "#64748b",
        rotate: 30,
      },
      axisLine: {
        lineStyle: { color: "#cbd5e1" },
      },
      data: xLabels.value,
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#64748b" },
      splitLine: {
        lineStyle: { color: "rgba(148,163,184,0.18)" },
      },
    },
  };
}

function countOption() {
  const option = baseOption();
  option.color = ["#2563eb", "#ef4444"];
  option.legend = {
    top: 0,
    data: ["评论数", "负面评论数"],
    textStyle: { color: "#475569" },
  };
  option.series = [
    {
      name: "评论数",
      type: "line",
      smooth: true,
      symbol: "none",
      areaStyle: { opacity: 0.1 },
      lineStyle: { width: 3 },
      data: commentCountData.value,
    },
    {
      name: "负面评论数",
      type: "line",
      smooth: true,
      symbol: "none",
      areaStyle: { opacity: 0.06 },
      lineStyle: { width: 3 },
      data: negativeCountData.value,
    },
  ];
  return option;
}

function ratioOption() {
  const option = baseOption();
  option.color = ["#7c3aed"];
  option.legend = {
    top: 0,
    data: ["负面占比(%)"],
    textStyle: { color: "#475569" },
  };
  option.yAxis.name = "%";
  option.series = [
    {
      name: "负面占比(%)",
      type: "line",
      smooth: true,
      symbol: "none",
      areaStyle: { opacity: 0.08 },
      lineStyle: { width: 3 },
      data: negativeRatioData.value,
    },
  ];
  return option;
}

function heatOption() {
  const option = baseOption();
  option.color = ["#0ea5e9"];
  option.legend = {
    top: 0,
    data: ["热度"],
    textStyle: { color: "#475569" },
  };
  option.series = [
    {
      name: "热度",
      type: "line",
      smooth: true,
      symbol: "none",
      areaStyle: { opacity: 0.08 },
      lineStyle: { width: 3 },
      data: heatSumData.value,
    },
  ];
  return option;
}

function renderCharts() {
  if (!countChartRef.value || !ratioChartRef.value || !heatChartRef.value) return;

  if (!countChart) countChart = echarts.init(countChartRef.value);
  if (!ratioChart) ratioChart = echarts.init(ratioChartRef.value);
  if (!heatChart) heatChart = echarts.init(heatChartRef.value);

  countChart.setOption(countOption(), true);
  ratioChart.setOption(ratioOption(), true);
  heatChart.setOption(heatOption(), true);
}

function resizeCharts() {
  if (countChart) countChart.resize();
  if (ratioChart) ratioChart.resize();
  if (heatChart) heatChart.resize();
}

async function loadTopics() {
  const data = await getTopics();
  topicOptions.value = data?.data || [];

  if (!selectedTopic.value && topicOptions.value.length > 0) {
    selectedTopic.value = topicOptions.value[0].topic;
  }
}

async function loadDashboard() {
  if (!selectedTopic.value) return;

  const data = await getDashboard({
    topic: selectedTopic.value,
    daily_limit: 365,
    hourly_limit: 720,
    alerts_limit: 30,
  });

  dashboard.value = data;
}

async function loadComments() {
  if (!selectedTopic.value) return;

  const params = {
    topic: selectedTopic.value,
    view: granularity.value,
  };

  if (granularity.value === "daily") {
    if (selectedBucket.value) params.date = selectedBucket.value;
    params.limit = 30;
  } else {
    if (selectedBucket.value) params.hour = selectedBucket.value;
    params.limit = 2000;
  }

  const data = await getComments(params);
  comments.value = data;

  if (comments.value.selected_bucket) {
    selectedBucket.value = comments.value.selected_bucket;
  } else if (!selectedBucket.value && comments.value.bucket_options?.length) {
    selectedBucket.value = comments.value.bucket_options[0];
  }
}

async function loadAlerts() {
  if (!selectedTopic.value) return;

  const data = await getAlerts({
    topic: selectedTopic.value,
    limit: 30,
  });

  alerts.value = data;
}

async function refreshAll() {
  loading.value = true;
  errorMsg.value = "";

  try {
    await loadTopics();
    await loadDashboard();
    await loadAlerts();

    selectedBucket.value = "";
    await loadComments();

    await nextTick();
    renderCharts();
  } catch (err) {
    console.error(err);
    errorMsg.value = String(err?.message || err || "未知错误");
  } finally {
    loading.value = false;
  }
}

async function handleTopicChange() {
  selectedBucket.value = "";
  await refreshAll();
}

async function switchGranularity(value) {
  granularity.value = value;
  selectedBucket.value = "";
  await loadComments();
  await nextTick();
  renderCharts();
}

watch(trendRows, async () => {
  await nextTick();
  renderCharts();
}, { deep: true });

onMounted(async () => {
  window.addEventListener("resize", resizeCharts);
  await refreshAll();
});

onBeforeUnmount(() => {
  window.removeEventListener("resize", resizeCharts);
  if (countChart) countChart.dispose();
  if (ratioChart) ratioChart.dispose();
  if (heatChart) heatChart.dispose();
});
</script>

<style scoped>
.page {
  position: relative;
  min-height: 100vh;
  padding: 26px;
  color: #0f172a;
  background:
    radial-gradient(circle at top left, rgba(37, 99, 235, 0.14), transparent 24%),
    radial-gradient(circle at top right, rgba(124, 58, 237, 0.12), transparent 24%),
    linear-gradient(180deg, #f8fbff 0%, #eef4ff 48%, #f7f9fc 100%);
}
.page-bg {
  position: absolute;
  inset: 0;
  pointer-events: none;
}
.hero {
  position: relative;
  display: grid;
  grid-template-columns: 1.2fr 1fr;
  gap: 22px;
  margin-bottom: 22px;
}
.hero-left,
.control-card,
.kpi-card,
.panel {
  background: rgba(255,255,255,0.78);
  border: 1px solid rgba(148, 163, 184, 0.14);
  box-shadow: 0 18px 40px rgba(15, 23, 42, 0.07);
  backdrop-filter: blur(18px);
}
.hero-left {
  border-radius: 28px;
  padding: 28px;
}
.hero-right {
  display: flex;
}
.control-card {
  width: 100%;
  border-radius: 28px;
  padding: 22px;
}
.hero-tag {
  display: inline-flex;
  align-items: center;
  padding: 7px 14px;
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.06);
  color: #334155;
  font-size: 12px;
  font-weight: 700;
  margin-bottom: 18px;
}
.hero-left h1 {
  margin: 0 0 14px;
  font-size: 44px;
  line-height: 1.08;
  letter-spacing: -0.03em;
}
.hero-desc {
  margin: 0;
  color: #475569;
  font-size: 15px;
  line-height: 1.8;
}
.hero-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 18px;
}
.pill {
  padding: 7px 12px;
  border-radius: 999px;
  background: rgba(37, 99, 235, 0.08);
  color: #2563eb;
  font-size: 12px;
  font-weight: 700;
}
.control-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(180px, 1fr));
  gap: 14px;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.field label {
  font-size: 13px;
  color: #475569;
  font-weight: 700;
}
.field select,
.refresh-btn {
  height: 42px;
  border-radius: 14px;
  border: 1px solid rgba(148, 163, 184, 0.25);
  background: rgba(255,255,255,0.96);
  padding: 0 14px;
  font-size: 14px;
  color: #0f172a;
}
.segmented {
  display: flex;
  gap: 8px;
  background: rgba(15, 23, 42, 0.05);
  border-radius: 14px;
  padding: 4px;
}
.segmented button {
  flex: 1;
  height: 34px;
  border: none;
  border-radius: 10px;
  background: transparent;
  color: #475569;
  font-weight: 700;
  cursor: pointer;
}
.segmented button.active {
  background: #0f172a;
  color: white;
}
.refresh-btn {
  background: linear-gradient(135deg, #2563eb, #0ea5e9);
  color: white;
  border: none;
  font-weight: 800;
  cursor: pointer;
  box-shadow: 0 10px 20px rgba(37, 99, 235, 0.22);
}
.status {
  margin-top: 14px;
  padding: 12px 14px;
  border-radius: 14px;
  font-size: 13px;
}
.status.loading {
  background: #eff6ff;
  color: #2563eb;
}
.status.error {
  background: #fef2f2;
  color: #dc2626;
}
.kpi-grid {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 16px;
  margin-bottom: 24px;
}
.kpi-card {
  border-radius: 22px;
  padding: 20px;
}
.kpi-title {
  color: #64748b;
  font-size: 14px;
  margin-bottom: 14px;
}
.kpi-value {
  font-size: 34px;
  line-height: 1;
  font-weight: 800;
  letter-spacing: -0.03em;
  margin-bottom: 12px;
}
.topic-text {
  font-size: 20px;
  line-height: 1.35;
}
.kpi-desc {
  color: #94a3b8;
  font-size: 13px;
  line-height: 1.6;
}
.panel {
  border-radius: 24px;
  padding: 22px;
  margin-bottom: 22px;
}
.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 18px;
}
.panel-head h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 800;
}
.panel-head p {
  margin: 6px 0 0;
  color: #64748b;
  font-size: 13px;
}
.panel-meta {
  padding: 6px 12px;
  border-radius: 999px;
  background: rgba(37, 99, 235, 0.08);
  color: #2563eb;
  font-size: 12px;
  font-weight: 700;
}
.chart-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}
.chart-card {
  border: 1px solid rgba(148, 163, 184, 0.12);
  border-radius: 20px;
  padding: 16px;
  background: rgba(251,253,255,0.9);
}
.chart-title {
  font-size: 15px;
  font-weight: 800;
  margin-bottom: 10px;
}
.chart-box {
  width: 100%;
  height: 320px;
}
.full-span {
  grid-column: 1 / -1;
}
.heat-chart {
  height: 360px;
}
.content-grid {
  display: grid;
  grid-template-columns: 1.55fr 1fr;
  gap: 20px;
}
.table-wrap {
  overflow: auto;
  max-height: 820px;
}
.data-table {
  width: 100%;
  border-collapse: collapse;
}
.data-table thead th {
  position: sticky;
  top: 0;
  background: rgba(255,255,255,0.96);
  z-index: 1;
}
.data-table th,
.data-table td {
  border-bottom: 1px solid rgba(148, 163, 184, 0.14);
  padding: 13px 10px;
  text-align: left;
  vertical-align: top;
  font-size: 14px;
}
.text-cell {
  min-width: 420px;
  white-space: pre-wrap;
  line-height: 1.7;
  color: #1e293b;
}
.emotion-tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 58px;
  padding: 6px 10px;
  border-radius: 999px;
  color: white;
  font-size: 12px;
  font-weight: 800;
}
.emotion-tag.positive {
  background: linear-gradient(135deg, #16a34a, #22c55e);
}
.emotion-tag.neutral {
  background: linear-gradient(135deg, #64748b, #94a3b8);
}
.emotion-tag.negative {
  background: linear-gradient(135deg, #dc2626, #ef4444);
}
.empty-cell,
.empty-alert {
  text-align: center;
  color: #64748b;
  padding: 26px;
}
.alert-list {
  display: flex;
  flex-direction: column;
  gap: 14px;
  max-height: 820px;
  overflow: auto;
}
.alert-card {
  border: 1px solid rgba(248, 113, 113, 0.18);
  border-radius: 18px;
  padding: 16px;
  background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,250,250,0.92));
  box-shadow: 0 10px 24px rgba(239, 68, 68, 0.06);
}
.alert-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}
.alert-time {
  color: #64748b;
  font-size: 12px;
}
.alert-badge {
  padding: 5px 10px;
  border-radius: 999px;
  background: rgba(239,68,68,0.12);
  color: #dc2626;
  font-size: 12px;
  font-weight: 800;
}
.alert-topic {
  font-size: 16px;
  font-weight: 800;
  margin-bottom: 8px;
}
.alert-meta {
  color: #475569;
  font-size: 13px;
  line-height: 1.8;
}
.samples {
  margin-top: 14px;
  padding-top: 12px;
  border-top: 1px dashed rgba(148, 163, 184, 0.3);
}
.samples-title {
  font-size: 13px;
  font-weight: 800;
  margin-bottom: 10px;
  color: #334155;
}
.sample-item + .sample-item {
  margin-top: 10px;
}
.sample-meta {
  color: #64748b;
  font-size: 12px;
  margin-bottom: 4px;
}
.sample-text {
  color: #1e293b;
  font-size: 13px;
  line-height: 1.65;
}

@media (max-width: 1600px) {
  .kpi-grid {
    grid-template-columns: repeat(3, 1fr);
  }
}
@media (max-width: 1200px) {
  .hero,
  .content-grid,
  .chart-grid {
    grid-template-columns: 1fr;
  }
  .full-span {
    grid-column: auto;
  }
}
@media (max-width: 900px) {
  .control-grid {
    grid-template-columns: 1fr;
  }
}
@media (max-width: 700px) {
  .page {
    padding: 16px;
  }
  .kpi-grid {
    grid-template-columns: 1fr;
  }
  .hero-left h1 {
    font-size: 32px;
  }
}
</style>
