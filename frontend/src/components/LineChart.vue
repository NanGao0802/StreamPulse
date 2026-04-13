<template>
  <div class="chart-shell">
    <div class="chart-head">
      <div>
        <div class="chart-title">{{ title }}</div>
        <div v-if="subtitle" class="chart-subtitle">{{ subtitle }}</div>
      </div>
    </div>
    <div ref="chartRef" class="chart-box"></div>
  </div>
</template>

<script setup>
import * as echarts from "echarts";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

const props = defineProps({
  title: { type: String, default: "" },
  subtitle: { type: String, default: "" },
  option: { type: Object, default: () => ({}) },
});

const chartRef = ref(null);
let chartInstance = null;

function renderChart() {
  if (!chartRef.value) return;
  if (!chartInstance) chartInstance = echarts.init(chartRef.value);
  chartInstance.setOption(props.option || {}, true);
}

function handleResize() {
  if (chartInstance) chartInstance.resize();
}

watch(
  () => props.option,
  () => renderChart(),
  { deep: true }
);

onMounted(() => {
  renderChart();
  window.addEventListener("resize", handleResize);
});

onBeforeUnmount(() => {
  window.removeEventListener("resize", handleResize);
  if (chartInstance) {
    chartInstance.dispose();
    chartInstance = null;
  }
});
</script>

<style scoped>
.chart-shell {
  width: 100%;
}
.chart-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}
.chart-title {
  font-size: 17px;
  font-weight: 700;
  color: #0f172a;
}
.chart-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: #64748b;
}
.chart-box {
  width: 100%;
  height: 360px;
}
</style>
