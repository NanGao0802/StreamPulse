<template>
  <div class="stat-card">
    <div class="stat-top">
      <div class="stat-title">{{ title }}</div>
      <div v-if="badge" class="stat-badge">{{ badge }}</div>
    </div>
    <div class="stat-value">{{ value }}</div>
    <div class="stat-desc">{{ desc }}</div>
  </div>
</template>

<script setup>
defineProps({
  title: { type: String, default: "" },
  value: { type: [String, Number], default: "" },
  desc: { type: String, default: "" },
  badge: { type: String, default: "" },
});
</script>

<style scoped>
.stat-card {
  background: rgba(255, 255, 255, 0.78);
  backdrop-filter: blur(14px);
  border: 1px solid rgba(148, 163, 184, 0.12);
  border-radius: 20px;
  padding: 20px;
  min-height: 132px;
  box-shadow:
    0 10px 30px rgba(15, 23, 42, 0.08),
    inset 0 1px 0 rgba(255, 255, 255, 0.5);
}
.stat-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 14px;
}
.stat-title {
  color: #475569;
  font-size: 14px;
}
.stat-badge {
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(37, 99, 235, 0.08);
  color: #2563eb;
  font-size: 12px;
  font-weight: 600;
}
.stat-value {
  font-size: 34px;
  line-height: 1;
  font-weight: 800;
  color: #0f172a;
  letter-spacing: -0.02em;
  margin-bottom: 12px;
}
.stat-desc {
  color: #64748b;
  font-size: 13px;
  line-height: 1.5;
}
</style>
